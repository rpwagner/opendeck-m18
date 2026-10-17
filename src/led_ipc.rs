//! Optional same-user access to temporary LED palettes. No application semantics.
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::palette::LedPalette;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub const UPDATE_INTERVAL: Duration = Duration::from_millis(125);
#[cfg(unix)]
const MAX_TTL_MS: u64 = 10000;
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

#[derive(Clone, Debug)]
pub struct PaletteOverride {
    pub colors: LedPalette,
    pub expires: Instant,
    pub connection: CancellationToken,
}

impl PaletteOverride {
    pub fn palette(&self, base: LedPalette, now: Instant) -> LedPalette {
        if self.connection.is_cancelled() || now >= self.expires {
            base
        } else {
            self.colors
        }
    }
}

#[cfg(not(unix))]
pub async fn start(_: &tokio_util::task::TaskTracker) {}

#[cfg(unix)]
pub use unix::start;

#[cfg(unix)]
mod unix {
    use super::*;
    use crate::{SESSIONS, palette::parse_palette};
    use serde_json::{Value, json};
    use std::{
        fs, io,
        io::Read,
        os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
        path::PathBuf,
        sync::Arc,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt, BufReader},
        net::{UnixListener, UnixStream},
        time::{sleep, timeout},
    };
    use tokio_util::task::TaskTracker;

    const MAX_FRAME: usize = 1024;
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
                    "control directory must be owned by this user with mode 0700",
                ));
            }
            let path = directory.join("led.sock");
            // An existing socket may belong to another live process; never unlink it.
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

    fn enabled(value: &Value) -> Result<bool, &'static str> {
        let object = value.as_object().ok_or("invalid configuration")?;
        if object.keys().any(|key| key != "enabled") {
            return Err("unknown configuration field");
        }
        value
            .get("enabled")
            .map_or(Ok(false), |v| v.as_bool().ok_or("enabled must be boolean"))
    }

    fn load_enabled(uid: u32) -> Result<bool, &'static str> {
        let path = std::env::current_exe()
            .map_err(|_| "executable path unavailable")?
            .parent()
            .ok_or("plugin directory unavailable")?
            .join("led-control.json");
        let file = match fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(_) => {
                return Err(
                    "cannot open led-control.json (use an owned regular file, not a symlink)",
                );
            }
        };
        let metadata = file
            .metadata()
            .map_err(|_| "cannot inspect led-control.json")?;
        if !metadata.is_file()
            || metadata.uid() != uid
            || metadata.mode() & 0o022 != 0
            || metadata.len() > 4096
        {
            return Err(
                "unsafe led-control.json (use an owned regular file, not writable by others, at most 4096 bytes)",
            );
        }
        let mut bytes = Vec::new();
        file.take(4097)
            .read_to_end(&mut bytes)
            .map_err(|_| "cannot read led-control.json")?;
        if bytes.len() > 4096 {
            return Err("oversized led-control.json");
        }
        enabled(&serde_json::from_slice::<Value>(&bytes).map_err(|_| "invalid led-control.json")?)
    }

    pub async fn start(tracker: &TaskTracker) {
        // SAFETY: geteuid is a read-only process identity lookup.
        let uid = unsafe { libc::geteuid() };
        match load_enabled(uid) {
            Ok(true) => {}
            Ok(false) => return,
            Err(error) => {
                log::error!("External LED control disabled: {}", error);
                return;
            }
        }
        let endpoint = match std::env::var_os("HOME")
            .ok_or_else(|| io::Error::other("HOME unavailable"))
            .and_then(|home| Endpoint::bind(PathBuf::from(home), uid))
        {
            Ok(endpoint) => endpoint,
            Err(_) => {
                log::error!(
                    "External LED control disabled: cannot bind private ~/.opendeck-m18/led.sock; inspect ownership/mode or an existing socket with OpenDeck closed"
                );
                return;
            }
        };
        let token = Arc::new(CancellationToken::new());
        SESSIONS
            .write()
            .await
            .insert_task("_led_control".into(), token.clone());
        tracker.spawn(async move {
            log::info!("External LED palette control v1 enabled");
            loop {
                let stream = tokio::select! {
                    biased;
                    _ = token.cancelled() => break,
                    result = endpoint.listener.accept() => match result { Ok((stream, _)) => stream, Err(_) => break },
                };
                if !stream.peer_cred().is_ok_and(|credentials| credentials.uid() == uid) { continue; }
                // A single controlling stream gives requests an unambiguous order.
                tokio::select! {
                    biased;
                    _ = token.cancelled() => break,
                    _ = connection(stream) => {},
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
    }

    #[derive(Debug)]
    enum Request {
        Hello,
        Set(Target, LedPalette, Duration),
        Clear(Target),
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
            "clear" => &["version", "op", "device", "session"],
            "set" => &["version", "op", "device", "session", "ledColors", "ttl_ms"],
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
        };
        if op == "clear" {
            return Ok(Request::Clear(target));
        }
        let colors = parse_palette(&value).ok_or("invalid-palette")?;
        let millis = value["ttl_ms"]
            .as_u64()
            .filter(|n| (125..=MAX_TTL_MS).contains(n))
            .ok_or("invalid-ttl")?;
        Ok(Request::Set(target, colors, Duration::from_millis(millis)))
    }

    fn error(code: &str) -> Value {
        json!({"ok":false,"version":1,"error":code})
    }

    async fn apply(request: Request, lease: &CancellationToken) -> Value {
        let (target, palette) = match request {
            Request::Hello => {
                let sessions = SESSIONS.read().await;
                let devices: Vec<_> = sessions
                    .published_outputs()
                    .take(32)
                    .map(|(id, output)| json!({"device":id,"session":output.session_id}))
                    .collect();
                return json!({"ok":true,"version":1,"operations":["set","clear"],"led_count":24,
                    "max_hz":8,"max_ttl_ms":MAX_TTL_MS,"devices":devices});
            }
            Request::Clear(target) => (target, None),
            Request::Set(target, colors, ttl) => (target, Some((colors, ttl))),
        };
        let output = SESSIONS
            .read()
            .await
            .published_outputs()
            .find(|(id, _)| **id == target.device)
            .map(|(_, output)| output.clone());
        dispatch(target, palette, output.as_deref(), lease)
    }

    fn dispatch(
        target: Target,
        palette: Option<(LedPalette, Duration)>,
        output: Option<&crate::device::DeviceOutput>,
        lease: &CancellationToken,
    ) -> Value {
        let Some(output) = output.filter(|output| {
            output.id == target.device
                && output.session_id == target.session
                && !output.token.is_cancelled()
        }) else {
            return error("stale-or-unknown-session");
        };
        output
            .external_palette
            .send_replace(palette.map(|(colors, ttl)| PaletteOverride {
                colors,
                expires: Instant::now() + ttl,
                connection: lease.clone(),
            }));
        json!({"ok":true,"version":1,"status":"queued"})
    }

    async fn read_frame(reader: &mut (impl tokio::io::AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(384);
        for _ in 0..MAX_FRAME {
            let byte = reader.read_u8().await?;
            bytes.push(byte);
            if byte == b'\n' {
                return Ok(bytes);
            }
        }
        Err(io::Error::other("frame-too-large"))
    }

    async fn connection(stream: UnixStream) {
        let lease = ConnectionLease(CancellationToken::new());
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
                    apply(Request::Hello, &lease.0).await
                }
                Ok(request) if greeted => apply(request, &lease.0).await,
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
            sleep(UPDATE_INTERVAL).await;
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn opt_in_and_protocol_are_strict_and_reuse_existing_palette_format() {
            assert!(!enabled(&json!({})).unwrap());
            assert!(enabled(&json!({"enabled":true})).unwrap());
            assert!(enabled(&json!({"enabled":"yes"})).is_err());
            assert!(enabled(&json!({"enabled":true,"animation":"anything"})).is_err());
            assert!(matches!(
                parse(br#"{"version":1,"op":"hello"}"#),
                Ok(Request::Hello)
            ));
            let valid = json!({"version":1,"op":"set","device":"18-test","session":"generation-1","ttl_ms":1000,"ledColors":vec!["#123456";24]});
            assert!(matches!(
                parse(valid.to_string().as_bytes()),
                Ok(Request::Set(_, _, _))
            ));
            for (field, value) in [
                ("version", json!(2)),
                ("ttl_ms", json!(10001)),
                ("device", json!("")),
                ("ledColors", json!(["#123456"])),
                ("op", json!("execute")),
                ("command", json!("anything")),
            ] {
                let mut bad = valid.clone();
                bad[field] = value;
                assert!(parse(bad.to_string().as_bytes()).is_err(), "{field}");
            }
            let mut bad = valid.clone();
            bad["ledColors"][0] = json!("#XYZ123");
            assert!(parse(bad.to_string().as_bytes()).is_err());
            assert!(parse(&vec![b' '; MAX_FRAME + 1]).is_err());
        }

        #[tokio::test]
        async fn external_palettes_and_clear_bind_to_current_device_without_persisting() {
            let _test_guard = crate::palette::TEST_LOCK.lock().await;
            let id = "18-protocol-test".to_string();
            let (sender, _receiver) = tokio::sync::mpsc::channel(1);
            let token = Arc::new(CancellationToken::new());
            let output = crate::device::DeviceOutput::new(
                id.clone(),
                token.clone(),
                Arc::new(tokio::sync::RwLock::new(())),
                sender,
            );
            let base = [[3, 4, 5]; 24];
            crate::palette::PALETTES.lock().await.select(&id, base);
            let lease = CancellationToken::new();
            let target = || Target {
                device: id.clone(),
                session: output.session_id.clone(),
            };
            let colors = std::array::from_fn(|i| [i as u8, 120, 255]);
            assert_eq!(
                dispatch(
                    target(),
                    Some((colors, Duration::from_secs(1))),
                    Some(&output),
                    &lease
                )["status"],
                "queued"
            );
            assert_eq!(
                output.external_palette.borrow().as_ref().unwrap().colors,
                colors
            );
            let mut stale = target();
            stale.session = "old-generation".into();
            assert_eq!(
                dispatch(stale, None, Some(&output), &lease)["error"],
                "stale-or-unknown-session"
            );
            assert!(output.external_palette.borrow().is_some());
            dispatch(target(), None, Some(&output), &lease);
            assert!(output.external_palette.borrow().is_none());
            token.cancel();
            assert_eq!(
                dispatch(
                    target(),
                    Some((colors, Duration::from_secs(1))),
                    Some(&output),
                    &lease
                )["error"],
                "stale-or-unknown-session"
            );
            assert_eq!(crate::palette::PALETTES.lock().await.get(&id), Some(base));
        }

        #[tokio::test]
        async fn framing_and_disconnect_are_bounded() {
            let (client, server) = UnixStream::pair().unwrap();
            let task = tokio::spawn(connection(server));
            let (read, mut write) = client.into_split();
            write
                .write_all(b"{\"version\":1,\"op\":\"hello\"}\n")
                .await
                .unwrap();
            let mut read = BufReader::new(read);
            let reply = read_frame(&mut read).await.unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&reply).unwrap()["led_count"],
                24
            );
            drop(read);
            drop(write);
            timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap();
            let token = CancellationToken::new();
            {
                let _guard = ConnectionLease(token.clone());
            }
            assert!(token.is_cancelled());
            let mut oversized = &vec![b'x'; MAX_FRAME + 1][..];
            assert!(read_frame(&mut oversized).await.is_err());
        }

        #[tokio::test]
        async fn endpoint_refuses_unsafe_directory_and_existing_socket() {
            // SAFETY: read-only process uid lookup.
            let uid = unsafe { libc::geteuid() };
            let root = PathBuf::from("/tmp").join(new_session_id());
            fs::create_dir(&root).unwrap();
            let endpoint = Endpoint::bind(root.clone(), uid).unwrap();
            assert_eq!(
                fs::symlink_metadata(&endpoint.path).unwrap().mode() & 0o777,
                0o600
            );
            assert!(Endpoint::bind(root.clone(), uid).is_err());
            drop(endpoint);
            fs::set_permissions(
                root.join(".opendeck-m18"),
                fs::Permissions::from_mode(0o755),
            )
            .unwrap();
            assert!(Endpoint::bind(root.clone(), uid).is_err());
            fs::remove_dir_all(root).unwrap();
        }
    }
}
