//! Optional same-user effect requests. No HID, scripts, or global-settings writes.
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

pub fn new_session_id() -> String {
    format!(
        "{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(not(unix))]
pub async fn start(_: &tokio_util::task::TaskTracker) {}

#[cfg(unix)]
pub use unix::start;

#[cfg(unix)]
mod unix {
    use crate::{
        SESSIONS,
        effects::{self, Effect, EffectKind, FRAME_INTERVAL, Preferences},
    };
    use serde_json::{Value, json};
    use std::{
        collections::HashMap,
        fs, io,
        io::Read,
        os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
        path::PathBuf,
        sync::Arc,
        time::Duration,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt, BufReader},
        net::{UnixListener, UnixStream},
        time::{Instant, sleep, timeout},
    };
    use tokio_util::{sync::CancellationToken, task::TaskTracker};

    const MAX_FRAME: usize = 1024;
    const MAX_SESSIONS: usize = 32;
    const IDLE: Duration = Duration::from_secs(15);
    const WRITE_TIMEOUT: Duration = Duration::from_millis(500);

    struct Endpoint {
        listener: UnixListener,
        path: PathBuf,
        device: u64,
        inode: u64,
    }

    impl Endpoint {
        fn bind(home: PathBuf, uid: u32) -> io::Result<Self> {
            if !home.is_absolute() {
                return Err(io::Error::other("home must be absolute"));
            }
            let directory = home.join(".opendeck-m18");
            match fs::DirBuilder::new().mode(0o700).create(&directory) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
            let metadata = fs::symlink_metadata(&directory)?;
            if !metadata.is_dir() || metadata.uid() != uid || metadata.mode() & 0o777 != 0o700 {
                return Err(io::Error::other(
                    "effect directory must be owned by this user with mode 0700",
                ));
            }
            let path = directory.join("led.sock");
            // Never unlink a preexisting socket: it may belong to another live process.
            let listener = UnixListener::bind(&path)?;
            let metadata = fs::symlink_metadata(&path)?;
            let endpoint = Self {
                listener,
                path,
                device: metadata.dev(),
                inode: metadata.ino(),
            };
            fs::set_permissions(&endpoint.path, fs::Permissions::from_mode(0o600))?;
            Ok(endpoint)
        }
    }

    impl Drop for Endpoint {
        fn drop(&mut self) {
            if fs::symlink_metadata(&self.path)
                .is_ok_and(|metadata| metadata.dev() == self.device && metadata.ino() == self.inode)
            {
                let _ = fs::remove_file(&self.path);
            }
        }
    }

    fn preferences(value: &Value) -> Result<Option<Preferences>, &'static str> {
        let object = value.as_object().ok_or("invalid configuration")?;
        if object
            .keys()
            .any(|key| !["enabled", "reduced_motion", "brightness_cap"].contains(&key.as_str()))
        {
            return Err("unknown configuration field");
        }
        let boolean = |name| -> Result<bool, &'static str> {
            value
                .get(name)
                .map_or(Ok(false), |v| v.as_bool().ok_or("invalid preference"))
        };
        let enabled = boolean("enabled")?;
        let reduced_motion = boolean("reduced_motion")?;
        let cap = value
            .get("brightness_cap")
            .map_or(Ok(32), |v| v.as_u64().ok_or("invalid brightness"))?;
        if cap > 64 {
            return Err("brightness cap exceeds 64");
        }
        Ok(enabled.then_some(Preferences {
            reduced_motion,
            brightness_cap: cap as u8,
        }))
    }

    fn load_preferences(uid: u32) -> Result<Option<Preferences>, &'static str> {
        let path = std::env::current_exe()
            .map_err(|_| "executable path unavailable")?
            .parent()
            .ok_or("plugin directory unavailable")?
            .join("led-effects.json");
        let file = match fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(_) => {
                return Err(
                    "cannot open led-effects.json (use an owned regular file, not a symlink)",
                );
            }
        };
        let metadata = file
            .metadata()
            .map_err(|_| "cannot inspect led-effects.json")?;
        if !metadata.is_file()
            || metadata.uid() != uid
            || metadata.mode() & 0o022 != 0
            || metadata.len() > 4096
        {
            return Err(
                "unsafe led-effects.json (use an owned regular file, not writable by others, at most 4096 bytes)",
            );
        }
        let mut bytes = Vec::new();
        file.take(4097)
            .read_to_end(&mut bytes)
            .map_err(|_| "cannot read led-effects.json")?;
        if bytes.len() > 4096 {
            return Err("oversized led-effects.json");
        }
        preferences(
            &serde_json::from_slice::<Value>(&bytes).map_err(|_| "invalid led-effects.json")?,
        )
    }

    pub async fn start(tracker: &TaskTracker) {
        // SAFETY: geteuid has no preconditions and changes no process state.
        let uid = unsafe { libc::geteuid() };
        let preferences = match load_preferences(uid) {
            Ok(Some(preferences)) => preferences,
            Ok(None) => return,
            Err(error) => {
                log::error!("LED effects disabled: {}", error);
                return;
            }
        };
        let endpoint = match std::env::var_os("HOME")
            .ok_or_else(|| io::Error::other("HOME unavailable"))
            .and_then(|home| Endpoint::bind(PathBuf::from(home), uid))
        {
            Ok(endpoint) => endpoint,
            Err(_) => {
                log::error!(
                    "LED effects disabled: cannot bind private ~/.opendeck-m18/led.sock; inspect ownership/mode or an existing socket with OpenDeck closed"
                );
                return;
            }
        };
        let token = Arc::new(CancellationToken::new());
        SESSIONS
            .write()
            .await
            .insert_task("_led_effects".into(), token.clone());
        tracker.spawn(async move {
            log::info!("LED effect interface v1 enabled (same-user Unix socket)");
            loop {
                let stream = tokio::select! {
                    biased;
                    _ = token.cancelled() => break,
                    result = endpoint.listener.accept() => match result {
                        Ok((stream, _)) => stream,
                        Err(_) => break,
                    },
                };
                if !stream
                    .peer_cred()
                    .is_ok_and(|credentials| credentials.uid() == uid)
                {
                    continue;
                }
                // One controlling connection. Additional clients cannot preempt it;
                // their protocol deadlines must treat an unanswered handshake as unavailable.
                tokio::select! {
                    biased;
                    _ = token.cancelled() => break,
                    _ = connection(stream, preferences) => {},
                }
            }
        });
    }

    struct ConnectionLease(CancellationToken);
    impl Drop for ConnectionLease {
        fn drop(&mut self) {
            self.0.cancel();
        }
    }

    #[derive(Debug)]
    struct Target {
        device: String,
        session: String,
        sequence: u64,
    }

    #[derive(Debug)]
    enum Request {
        Hello,
        Effect(Target, EffectKind, Duration),
        Cancel(Target),
    }

    fn parse(bytes: &[u8]) -> Result<Request, &'static str> {
        if bytes.len() > MAX_FRAME {
            return Err("frame-too-large");
        }
        let value: Value = serde_json::from_slice(bytes).map_err(|_| "invalid-json")?;
        let object = value.as_object().ok_or("invalid-request")?;
        if value["version"].as_u64() != Some(1) {
            return Err("unsupported-version");
        }
        let op = value["op"].as_str().ok_or("invalid-operation")?;
        let allowed: &[&str] = match op {
            "hello" => &["version", "op"],
            "cancel" => &["version", "op", "device", "session", "sequence"],
            "effect" => &[
                "version",
                "op",
                "device",
                "session",
                "sequence",
                "effect",
                "duration_ms",
            ],
            _ => return Err("invalid-operation"),
        };
        if object.keys().any(|key| !allowed.contains(&key.as_str())) {
            return Err("unknown-field");
        }
        if op == "hello" {
            return Ok(Request::Hello);
        }
        let identifier = |name| {
            value[name]
                .as_str()
                .filter(|s| !s.is_empty() && s.len() <= 128)
                .map(str::to_owned)
                .ok_or("invalid-target")
        };
        let target = Target {
            device: identifier("device")?,
            session: identifier("session")?,
            sequence: value["sequence"]
                .as_u64()
                .filter(|n| (1..=9_007_199_254_740_991).contains(n))
                .ok_or("invalid-sequence")?,
        };
        if op == "cancel" {
            return Ok(Request::Cancel(target));
        }
        let kind = EffectKind::parse(value["effect"].as_str().ok_or("invalid-effect")?)
            .ok_or("invalid-effect")?;
        let millis = value["duration_ms"]
            .as_u64()
            .filter(|n| (125..=kind.max_ms()).contains(n))
            .ok_or("invalid-duration")?;
        Ok(Request::Effect(target, kind, Duration::from_millis(millis)))
    }

    fn error(code: &str) -> Value {
        json!({"ok": false, "version": 1, "error": code})
    }

    async fn apply(
        request: Request,
        owner: u64,
        lease: &CancellationToken,
        preferences: Preferences,
        sequences: &mut HashMap<(String, String), u64>,
    ) -> Value {
        let (target, operation) = match request {
            Request::Hello => {
                let sessions = SESSIONS.read().await;
                let devices: Vec<_> = sessions
                    .published_outputs()
                    .take(MAX_SESSIONS)
                    .map(|(id, output)| json!({"device": id, "session": output.effect_session}))
                    .collect();
                return json!({"ok": true, "version": 1, "effects": ["start", "ready", "stop-mode", "stop-pending", "failure"],
                    "max_fps": 8, "brightness_cap": preferences.brightness_cap,
                    "reduced_motion": preferences.reduced_motion, "devices": devices});
            }
            Request::Cancel(target) => (target, None),
            Request::Effect(target, kind, duration) => (target, Some((kind, duration))),
        };
        let output = SESSIONS
            .read()
            .await
            .published_outputs()
            .find(|(id, _)| **id == target.device)
            .map(|(_, output)| output.clone());
        dispatch(
            target,
            operation,
            output.as_deref(),
            owner,
            lease,
            preferences,
            sequences,
        )
    }

    fn dispatch(
        target: Target,
        operation: Option<(EffectKind, Duration)>,
        output: Option<&crate::device::DeviceOutput>,
        owner: u64,
        lease: &CancellationToken,
        preferences: Preferences,
        sequences: &mut HashMap<(String, String), u64>,
    ) -> Value {
        let Some(output) = output.filter(|output| {
            output.id == target.device
                && output.effect_session == target.session
                && !output.token.is_cancelled()
        }) else {
            return error("stale-or-unknown-session");
        };
        let key = (target.device, target.session);
        if sequences
            .get(&key)
            .is_some_and(|previous| target.sequence <= *previous)
        {
            return error("stale-sequence");
        }
        if sequences.len() >= MAX_SESSIONS && !sequences.contains_key(&key) {
            return error("session-limit");
        }
        sequences.insert(key, target.sequence);
        if let Some((kind, duration)) = operation {
            let effect = Effect {
                kind,
                duration,
                owner,
                cancelled: lease.clone(),
                started: Instant::now(),
                preferences,
            };
            if !effects::submit(&output.effects, effect) {
                return error("higher-priority-effect");
            }
        } else {
            effects::cancel(&output.effects, owner);
        }
        json!({"ok": true, "version": 1, "status": "queued", "sequence": target.sequence})
    }

    async fn read_frame(reader: &mut (impl tokio::io::AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(128);
        for _ in 0..MAX_FRAME {
            let byte = reader.read_u8().await?;
            bytes.push(byte);
            if byte == b'\n' {
                return Ok(bytes);
            }
        }
        Err(io::Error::other("frame-too-large"))
    }

    async fn connection(stream: UnixStream, preferences: Preferences) {
        let owner = super::NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let lease = ConnectionLease(CancellationToken::new());
        let mut sequences = HashMap::new();
        let (read, mut write) = stream.into_split();
        let mut read = BufReader::new(read);
        let mut greeted = false;
        loop {
            let bytes = match timeout(IDLE, read_frame(&mut read)).await {
                Ok(Ok(bytes)) => bytes,
                _ => break,
            };
            let reply = match parse(&bytes) {
                Ok(Request::Hello) => {
                    greeted = true;
                    apply(Request::Hello, owner, &lease.0, preferences, &mut sequences).await
                }
                Ok(request) if greeted => {
                    apply(request, owner, &lease.0, preferences, &mut sequences).await
                }
                Ok(_) => error("hello-required"),
                Err(code) => error(code),
            };
            let mut encoded = reply.to_string().into_bytes();
            encoded.push(b'\n');
            if !matches!(
                timeout(WRITE_TIMEOUT, write.write_all(&encoded)).await,
                Ok(Ok(()))
            ) {
                break;
            }
            sleep(FRAME_INTERVAL).await; // Bound parser/output-request work to 8 messages/s.
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn preferences_are_opt_in_strict_and_bounded() {
            assert!(preferences(&json!({})).unwrap().is_none());
            assert!(preferences(&json!({"enabled": false})).unwrap().is_none());
            assert_eq!(
                preferences(&json!({"enabled": true}))
                    .unwrap()
                    .unwrap()
                    .brightness_cap,
                32
            );
            for invalid in [
                json!({"enabled": "yes"}),
                json!({"enabled": true, "brightness_cap": 65}),
                json!({"command": "anything"}),
            ] {
                assert!(preferences(&invalid).is_err());
            }
        }

        #[test]
        fn parser_rejects_unbounded_unknown_and_raw_control_requests() {
            assert!(matches!(
                parse(br#"{"version":1,"op":"hello"}"#),
                Ok(Request::Hello)
            ));
            let valid = json!({"version":1,"op":"effect","device":"18-test","session":"generation-1","sequence":1,"effect":"stop-mode","duration_ms":10000});
            assert!(parse(valid.to_string().as_bytes()).is_ok());
            for (field, value) in [
                ("version", json!(2)),
                ("duration_ms", json!(10001)),
                ("sequence", json!(0)),
                ("effect", json!("hid")),
                ("device", json!("")),
                ("command", json!("exec")),
            ] {
                let mut bad = valid.clone();
                bad[field] = value;
                assert!(parse(bad.to_string().as_bytes()).is_err(), "{field}");
            }
            assert!(parse(&vec![b' '; MAX_FRAME + 1]).is_err());
        }

        #[tokio::test]
        async fn dispatch_binds_session_sequence_priority_and_owner_without_persisting_frames() {
            let _test_guard = crate::palette::TEST_LOCK.lock().await;
            use tokio::sync::{RwLock, mpsc};
            let id = "18-protocol-test".to_string();
            let (sender, _receiver) = mpsc::channel(1);
            let device_token = Arc::new(CancellationToken::new());
            let output = crate::device::DeviceOutput::new(
                id.clone(),
                device_token.clone(),
                Arc::new(RwLock::new(())),
                sender,
            );
            let base = [[3, 4, 5]; 24];
            crate::palette::PALETTES.lock().await.select(&id, base);
            let preferences = Preferences {
                reduced_motion: true,
                brightness_cap: 32,
            };
            let lease = CancellationToken::new();
            let mut sequences = HashMap::new();
            let target = |sequence| Target {
                device: id.clone(),
                session: output.effect_session.clone(),
                sequence,
            };
            let stop = Some((EffectKind::StopMode, Duration::from_secs(10)));
            assert_eq!(
                dispatch(
                    target(1),
                    stop,
                    Some(&output),
                    7,
                    &lease,
                    preferences,
                    &mut sequences
                )["status"],
                "queued"
            );
            assert_eq!(
                dispatch(
                    target(1),
                    None,
                    Some(&output),
                    7,
                    &lease,
                    preferences,
                    &mut sequences
                )["error"],
                "stale-sequence"
            );
            assert_eq!(
                dispatch(
                    target(2),
                    Some((EffectKind::Ready, Duration::from_secs(1))),
                    Some(&output),
                    7,
                    &lease,
                    preferences,
                    &mut sequences
                )["error"],
                "higher-priority-effect"
            );
            assert_eq!(
                output.effects.borrow().as_ref().unwrap().kind,
                EffectKind::StopMode
            );
            dispatch(
                target(3),
                None,
                Some(&output),
                8,
                &lease,
                preferences,
                &mut sequences,
            );
            assert!(output.effects.borrow().is_some()); // A stale other owner cannot cancel.
            dispatch(
                target(4),
                None,
                Some(&output),
                7,
                &lease,
                preferences,
                &mut sequences,
            );
            assert!(output.effects.borrow().is_none());
            let mut stale = target(5);
            stale.session = "prior-generation".into();
            assert_eq!(
                dispatch(
                    stale,
                    stop,
                    Some(&output),
                    7,
                    &lease,
                    preferences,
                    &mut sequences
                )["error"],
                "stale-or-unknown-session"
            );
            device_token.cancel();
            assert_eq!(
                dispatch(
                    target(5),
                    stop,
                    Some(&output),
                    7,
                    &lease,
                    preferences,
                    &mut sequences
                )["error"],
                "stale-or-unknown-session"
            );
            assert_eq!(crate::palette::PALETTES.lock().await.get(&id), Some(base));
        }

        #[tokio::test]
        async fn framing_is_bounded_and_connection_loss_revokes_effect_lease() {
            let (client, server) = UnixStream::pair().unwrap();
            let task = tokio::spawn(connection(
                server,
                Preferences {
                    reduced_motion: true,
                    brightness_cap: 32,
                },
            ));
            let (read, mut write) = client.into_split();
            write
                .write_all(b"{\"version\":1,\"op\":\"hello\"}\n")
                .await
                .unwrap();
            let mut read = BufReader::new(read);
            let reply = read_frame(&mut read).await.unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&reply).unwrap()["max_fps"],
                8
            );
            drop(read);
            drop(write);
            timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap();
            let token = CancellationToken::new();
            {
                let _lease = ConnectionLease(token.clone());
            }
            assert!(token.is_cancelled());
            let mut oversized = &vec![b'x'; MAX_FRAME + 1][..];
            assert!(read_frame(&mut oversized).await.is_err());
        }

        #[tokio::test]
        async fn endpoint_refuses_unsafe_directory_and_existing_socket() {
            // SAFETY: read-only process uid lookup.
            let uid = unsafe { libc::geteuid() };
            let root = PathBuf::from("/tmp").join(super::super::new_session_id());
            fs::create_dir(&root).unwrap();
            let endpoint = Endpoint::bind(root.clone(), uid).unwrap();
            assert_eq!(
                fs::symlink_metadata(&endpoint.path).unwrap().mode() & 0o777,
                0o600
            );
            assert!(Endpoint::bind(root.clone(), uid).is_err());
            drop(endpoint);
            let directory = root.join(".opendeck-m18");
            fs::set_permissions(&directory, fs::Permissions::from_mode(0o755)).unwrap();
            assert!(Endpoint::bind(root.clone(), uid).is_err());
            fs::remove_dir_all(root).unwrap();
        }
    }
}
