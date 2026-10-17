use std::{future::Future, sync::Arc, time::Duration};

use data_url::DataUrl;
use image::{DynamicImage, load_from_memory_with_format};
use mirajazz::{device::Device, error::MirajazzError, types::DeviceInput};
use openaction::{device_plugin, global_events::SetImageEvent};
use tokio::{
    sync::{RwLock, mpsc, watch},
    time::{Instant, MissedTickBehavior, interval, sleep_until},
};
use tokio_util::sync::CancellationToken;

use crate::{
    SESSIONS,
    effects::{Effect, FRAME_INTERVAL},
    inputs::{ButtonEvent, ButtonSession, opendeck_to_device},
    mappings::{
        COL_COUNT, CandidateDevice, ENCODER_COUNT, KEY_COUNT, Kind, ROW_COUNT,
        get_image_format_for_key,
    },
    palette::LedPalette,
    session::{Removal, SessionMatch, SessionRegistry},
};

pub enum DeviceCommand {
    SetImage { position: u8, image: DynamicImage },
    ClearImage(u8),
    ClearAll,
    SetBrightness(u8),
    SetLedColors(LedPalette),
}

pub struct DeviceOutput {
    pub id: String,
    pub token: Arc<CancellationToken>,
    input_gate: Arc<RwLock<()>>,
    sender: mpsc::Sender<DeviceCommand>,
    #[cfg_attr(not(unix), allow(dead_code))]
    pub effect_session: String,
    #[cfg_attr(not(unix), allow(dead_code))]
    pub effects: watch::Sender<Option<Effect>>,
}

impl DeviceOutput {
    pub(crate) fn new(
        id: String,
        token: Arc<CancellationToken>,
        input_gate: Arc<RwLock<()>>,
        sender: mpsc::Sender<DeviceCommand>,
    ) -> Self {
        Self {
            id,
            token,
            input_gate,
            sender,
            effect_session: crate::led_ipc::new_session_id(),
            effects: watch::channel(None).0,
        }
    }

    pub async fn send(&self, command: DeviceCommand) -> Result<(), ()> {
        self.sender.send(command).await.map_err(|_| ())
    }
}

/// Initializes a device and listens for events
pub async fn device_task(candidate: CandidateDevice, token: Arc<CancellationToken>) {
    log::debug!("Running device task for {:?}", candidate);

    let device = async {
        let device = connect(&candidate).await?;

        // Initialization is deliberately not cancellation-selectable. If the USB
        // handle fails during one of these writes, the write future must resolve
        // before the device is discarded.
        device.set_brightness(50).await?;
        device.clear_all_button_images().await?;
        device.flush().await?;

        Ok(device)
    }
    .await;

    let device: Device = match device {
        Ok(device) => device,
        Err(err) => {
            handle_error(&candidate.id, &token, err).await;
            log::error!(
                "Had error during device init, finishing device task: {:?}",
                candidate
            );
            return;
        }
    };

    let device = Arc::new(device);
    let (sender, receiver) = mpsc::channel(128);
    let input_session = ButtonSession::new();
    let output = Arc::new(DeviceOutput::new(
        candidate.id.clone(),
        token.clone(),
        input_session.gate(),
        sender,
    ));
    let effect_receiver = output.effects.subscribe();

    match publish_device_if_current(&candidate, &device, &output, &token).await {
        PublicationOutcome::Published => {}
        PublicationOutcome::Stale => {
            log::debug!("Discarding cancelled connection for {}", candidate.id);
            device.shutdown().await.ok();
            return;
        }
        PublicationOutcome::RegistrationFailed(error) => {
            log::error!("Unable to register device {}: {}", candidate.id, error);
            device.shutdown().await.ok();
            return;
        }
    }

    let mut output_task = tokio::spawn(device_output_task(
        candidate.id.clone(),
        candidate.kind.clone(),
        device.clone(),
        receiver,
        effect_receiver,
        token.clone(),
    ));

    let input_task = tokio::spawn(device_events_task(
        candidate.clone(),
        device.clone(),
        token.clone(),
        input_session,
    ));

    let output_finished = tokio::select! {
        result = &mut output_task => Some(result),
        _ = token.cancelled() => None,
    };

    let output_result = match output_finished {
        Some(result) => result,
        None => output_task.await,
    };

    match output_result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            handle_error(&candidate.id, &token, err).await;
        }
        Err(err) => {
            log::error!("Output worker for {} panicked: {}", candidate.id, err);
            disconnect_session(&candidate.id, &token).await;
        }
    }

    disconnect_session(&candidate.id, &token).await;

    log::debug!("Shutting down owned device {:?}", candidate);
    // This task always owns this exact handle. The output worker has fully
    // stopped, so shutdown cannot overlap with another write on it.
    device.shutdown().await.ok();

    // A pending HID read must not be cancelled and then reused. Detaching lets a
    // physical disconnect complete it normally; it owns no output path.
    drop(input_task);

    log::debug!("Device task finished for {:?}", candidate);
}

#[derive(Debug, Eq, PartialEq)]
enum PublicationOutcome {
    Published,
    Stale,
    RegistrationFailed(String),
}

async fn publish_with<D, O, Register, RegisterFuture, Deregister, DeregisterFuture>(
    sessions: &RwLock<SessionRegistry<D, O>>,
    id: &str,
    device: D,
    output: O,
    token: &Arc<CancellationToken>,
    register: Register,
    deregister: Deregister,
) -> PublicationOutcome
where
    Register: FnOnce() -> RegisterFuture,
    RegisterFuture: Future<Output = Result<(), String>>,
    Deregister: FnOnce(String) -> DeregisterFuture,
    DeregisterFuture: Future<Output = Result<(), String>>,
{
    if sessions
        .write()
        .await
        .begin_registration(id, token, device, output)
        .is_err()
    {
        sessions
            .write()
            .await
            .begin_removal(id, SessionMatch::Token(token));
        return PublicationOutcome::Stale;
    }

    if let Err(error) = register().await {
        sessions.write().await.discard_registration(id, token);
        return PublicationOutcome::RegistrationFailed(error);
    }

    if sessions.write().await.finish_registration(id, token) {
        return PublicationOutcome::Published;
    }

    if let Err(error) = deregister(id.to_owned()).await {
        log::error!(
            "Unable to roll back stale OpenDeck registration for {}: {}",
            id,
            error
        );
    }
    sessions.write().await.discard_registration(id, token);
    PublicationOutcome::Stale
}

async fn register_opendeck_device(candidate: &CandidateDevice) -> Result<(), String> {
    crate::require_opendeck_connection()?;

    device_plugin::register_device(
        candidate.id.clone(),
        candidate.kind.human_name(),
        ROW_COUNT as u8,
        COL_COUNT as u8,
        ENCODER_COUNT as u8,
        0,
    )
    .await
    .map_err(|error| error.to_string())
}

async fn publish_device_if_current(
    candidate: &CandidateDevice,
    device: &Arc<Device>,
    output: &Arc<DeviceOutput>,
    token: &Arc<CancellationToken>,
) -> PublicationOutcome {
    log::info!("Registering device {}", candidate.id);
    publish_with(
        &SESSIONS,
        &candidate.id,
        device.clone(),
        output.clone(),
        token,
        || register_opendeck_device(candidate),
        deregister_opendeck_device,
    )
    .await
}

async fn deregister_opendeck_device(id: String) -> Result<(), String> {
    crate::require_opendeck_connection()?;
    device_plugin::unregister_device(id)
        .await
        .map_err(|error| error.to_string())
}

async fn disconnect_matching_with<D, O, Drain, F, Fut>(
    sessions: &RwLock<SessionRegistry<D, O>>,
    id: &str,
    expected: SessionMatch<'_>,
    input_gate: Drain,
    deregister: F,
) -> bool
where
    Drain: FnOnce(&O) -> Option<Arc<RwLock<()>>>,
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let Some(removal) = sessions.write().await.begin_removal(id, expected) else {
        log::debug!("Ignoring stale disconnect from replaced device {}", id);
        return false;
    };
    let Removal::Ready(removed) = removal else {
        return true;
    };

    let input_gate = removed.output.as_ref().and_then(input_gate);
    let _input_guard = match input_gate {
        Some(gate) => Some(gate.write_owned().await),
        None => None,
    };

    if removed.device.is_some() {
        log::info!("Deregistering device {}", id);
        if let Err(error) = deregister(id.to_owned()).await {
            log::error!("Unable to deregister device {}: {}", id, error);
        }
    }

    if removed.cleanup_pending {
        sessions.write().await.finish_cleanup(id, &removed.token);
    }
    drop(removed.output);
    true
}

async fn disconnect_matching(id: &str, expected: SessionMatch<'_>) -> bool {
    disconnect_matching_with(
        &SESSIONS,
        id,
        expected,
        |output| Some(output.input_gate.clone()),
        deregister_opendeck_device,
    )
    .await
}

async fn disconnect_session(id: &str, expected_token: &Arc<CancellationToken>) -> bool {
    disconnect_matching(id, SessionMatch::Token(expected_token)).await
}

pub async fn disconnect_generation(id: &str, generation: u64) -> bool {
    disconnect_matching(id, SessionMatch::Generation(generation)).await
}

fn is_nonfatal_error(err: &MirajazzError) -> bool {
    matches!(err, MirajazzError::ImageError(_) | MirajazzError::BadData)
}

fn schedule_flush(flush_deadline: &mut Option<Instant>) {
    flush_deadline.get_or_insert_with(|| Instant::now() + Duration::from_millis(50));
}

/// Handles a device error. Image conversion errors are nonfatal; HID and
/// protocol errors discard this connection generation so the watcher can reopen it.
pub async fn handle_error(
    id: &str,
    expected_token: &Arc<CancellationToken>,
    err: MirajazzError,
) -> bool {
    log::error!("Device {} error: {}", id, err);

    if is_nonfatal_error(&err) {
        return true;
    }

    disconnect_session(id, expected_token).await;
    false
}

pub async fn connect(candidate: &CandidateDevice) -> Result<Device, MirajazzError> {
    let result = Device::connect(
        &candidate.dev,
        candidate.kind.protocol_version(),
        KEY_COUNT,
        ENCODER_COUNT,
    )
    .await;

    match result {
        Ok(device) => Ok(device),
        Err(e) => {
            log::error!("Error while connecting to device: {e}");
            Err(e)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum InputEvent {
    KeyDown(String, u8),
    KeyUp(String, u8),
}

async fn send_opendeck_input(event: InputEvent) -> Result<(), String> {
    crate::require_opendeck_connection()?;

    let result = match event {
        InputEvent::KeyDown(id, key) => device_plugin::key_down(id, key).await,
        InputEvent::KeyUp(id, key) => device_plugin::key_up(id, key).await,
    };

    result.map_err(|error| error.to_string())
}

/// Handles events from device to OpenDeck
async fn device_events_task(
    candidate: CandidateDevice,
    device: Arc<Device>,
    token: Arc<CancellationToken>,
    mut session: ButtonSession,
) -> Result<(), MirajazzError> {
    log::debug!("Connecting to {} for incoming events", candidate.id);
    let reader = device.get_reader(|_, _| Ok(DeviceInput::NoData));
    let mut sink = OpenDeckKeyEventSink;

    log::debug!("Connected to {} for incoming events", candidate.id);
    log::debug!("Reader is ready for {}", candidate.id);

    loop {
        log::debug!("Reading updates...");

        let report = match reader.raw_read_data(512).await {
            Ok(report) => report,
            Err(e) => {
                if !handle_error(&candidate.id, &token, e).await {
                    break;
                }
                continue;
            }
        };

        match process_session_report(&candidate.id, &token, &mut session, &report, &mut sink).await
        {
            Ok(ReportStatus::Current) => {}
            Ok(ReportStatus::Stale) => break,
            Err(e) => {
                if !handle_error(&candidate.id, &token, e).await {
                    break;
                }
            }
        }
    }

    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
enum ReportStatus {
    Current,
    Stale,
}

trait KeyEventSink {
    async fn emit(&mut self, id: &str, event: ButtonEvent) -> Result<(), String>;
}

struct OpenDeckKeyEventSink;

impl KeyEventSink for OpenDeckKeyEventSink {
    async fn emit(&mut self, id: &str, event: ButtonEvent) -> Result<(), String> {
        let event = match event {
            ButtonEvent::Down(position) => InputEvent::KeyDown(id.to_owned(), position),
            ButtonEvent::Up(position) => InputEvent::KeyUp(id.to_owned(), position),
        };
        send_opendeck_input(event).await
    }
}

async fn process_session_report<S: KeyEventSink>(
    id: &str,
    token: &Arc<CancellationToken>,
    session: &mut ButtonSession,
    report: &[u8],
    sink: &mut S,
) -> Result<ReportStatus, MirajazzError> {
    let is_current = { !token.is_cancelled() && SESSIONS.read().await.is_current(id, token) };

    if !is_current {
        return Ok(ReportStatus::Stale);
    }

    // Disconnect takes this session-owned gate before a replacement can be
    // published, so an old reader cannot emit under the replacement's id.
    let input_gate = session.gate();
    let input_guard = input_gate.read().await;
    if token.is_cancelled() {
        return Ok(ReportStatus::Stale);
    }

    let Some(event) = session.process_report(report)? else {
        return Ok(ReportStatus::Current);
    };

    log::debug!("New update: {:#?}", event);
    let result = sink.emit(id, event).await;
    drop(input_guard);

    if let Err(error) = result {
        log::error!("Unable to deliver input event for device {}: {}", id, error);
        disconnect_session(id, token).await;
        return Ok(ReportStatus::Stale);
    }

    Ok(ReportStatus::Current)
}

enum OutputAction {
    Command(Option<DeviceCommand>),
    Flush,
    KeepAlive,
    Leds,
    Cancel,
}

enum OutputStep {
    Continue,
    ScheduleFlush,
    ClearFlush,
    Stop,
}

trait OutputDevice: Send + Sync {
    async fn set_button_image(
        &self,
        key: u8,
        format: mirajazz::types::ImageFormat,
        image: DynamicImage,
    ) -> Result<(), MirajazzError>;
    async fn clear_button_image(&self, key: u8) -> Result<(), MirajazzError>;
    async fn clear_all_button_images(&self) -> Result<(), MirajazzError>;
    async fn set_brightness(&self, brightness: u8) -> Result<(), MirajazzError>;
    async fn set_led_colors(&self, colors: &[[u8; 3]]) -> Result<(), MirajazzError>;
    async fn flush(&self) -> Result<(), MirajazzError>;
    async fn keep_alive(&self) -> Result<(), MirajazzError>;
}

impl OutputDevice for Device {
    async fn set_button_image(
        &self,
        key: u8,
        format: mirajazz::types::ImageFormat,
        image: DynamicImage,
    ) -> Result<(), MirajazzError> {
        Device::set_button_image(self, key, format, image).await
    }

    async fn clear_button_image(&self, key: u8) -> Result<(), MirajazzError> {
        Device::clear_button_image(self, key).await
    }

    async fn clear_all_button_images(&self) -> Result<(), MirajazzError> {
        Device::clear_all_button_images(self).await
    }

    async fn set_brightness(&self, brightness: u8) -> Result<(), MirajazzError> {
        Device::set_brightness(self, brightness).await
    }

    async fn set_led_colors(&self, colors: &[[u8; 3]]) -> Result<(), MirajazzError> {
        Device::set_led_colors(self, colors).await
    }

    async fn flush(&self) -> Result<(), MirajazzError> {
        Device::flush(self).await
    }

    async fn keep_alive(&self) -> Result<(), MirajazzError> {
        Device::keep_alive(self).await
    }
}

/// Owns all active-session HID writes for one device. The select only chooses
/// the next action; each HID future is awaited afterward, where cancellation
/// cannot drop it halfway through an overlapped Windows write.
async fn device_output_task<D: OutputDevice + 'static>(
    id: String,
    kind: Kind,
    device: Arc<D>,
    mut receiver: mpsc::Receiver<DeviceCommand>,
    effects: watch::Receiver<Option<Effect>>,
    token: Arc<CancellationToken>,
) -> Result<(), MirajazzError> {
    let mut keepalive = interval(Duration::from_secs(10));
    // Restore before consuming queued updates, so a newer selection always wins.
    let saved = crate::palette::PALETTES.lock().await.get(&id);
    if let Some(colors) = saved {
        log::info!("Restoring LED palette for {}", id);
        device.set_led_colors(&colors).await?;
    }
    let mut base = saved.unwrap_or(crate::palette::DEFAULT_PALETTE);
    let mut last_leds = saved;
    let mut leds_owned = saved.is_some();
    let mut next_led = Instant::now() + FRAME_INTERVAL;
    keepalive.set_missed_tick_behavior(MissedTickBehavior::Skip);
    keepalive.tick().await;

    let mut flush_deadline: Option<Instant> = None;

    loop {
        let flush_at =
            flush_deadline.unwrap_or_else(|| Instant::now() + Duration::from_secs(24 * 60 * 60));

        let action = tokio::select! {
            biased;
            _ = token.cancelled() => OutputAction::Cancel,
            _ = sleep_until(flush_at), if flush_deadline.is_some() => OutputAction::Flush,
            _ = sleep_until(next_led) => OutputAction::Leds,
            command = receiver.recv() => OutputAction::Command(command),
            _ = keepalive.tick() => OutputAction::KeepAlive,
        };

        let result = match action {
            OutputAction::Command(Some(DeviceCommand::SetImage { position, image })) => {
                log::debug!("Setting image for button {}", position);
                device
                    .set_button_image(
                        opendeck_to_device(position),
                        get_image_format_for_key(&kind, position),
                        image,
                    )
                    .await
                    .map(|_| OutputStep::ScheduleFlush)
            }
            OutputAction::Command(Some(DeviceCommand::ClearImage(position))) => device
                .clear_button_image(opendeck_to_device(position))
                .await
                .map(|_| OutputStep::ScheduleFlush),
            OutputAction::Command(Some(DeviceCommand::ClearAll)) => {
                match device.clear_all_button_images().await {
                    Ok(()) => device.flush().await.map(|_| OutputStep::ClearFlush),
                    Err(err) => Err(err),
                }
            }
            OutputAction::Command(Some(DeviceCommand::SetBrightness(brightness))) => device
                .set_brightness(brightness)
                .await
                .map(|_| OutputStep::Continue),
            OutputAction::Command(Some(DeviceCommand::SetLedColors(colors))) => {
                base = colors;
                leds_owned = true;
                Ok(OutputStep::Continue)
            }
            OutputAction::Leds => {
                // Read the palette owner at render time: a manual selection made
                // during an effect must be the palette restored afterward.
                base = crate::palette::PALETTES
                    .lock()
                    .await
                    .get(&id)
                    .unwrap_or(base);
                let current = effects.borrow().clone();
                leds_owned |= current.is_some();
                let frame = current
                    .as_ref()
                    .map_or(base, |effect| effect.frame(&base, Instant::now()));
                let result = if leds_owned && Some(frame) != last_leds {
                    device.set_led_colors(&frame).await.map(|_| {
                        last_leds = Some(frame);
                        OutputStep::Continue
                    })
                } else {
                    Ok(OutputStep::Continue)
                };
                // Schedule after each write; never catch up with a burst of frames.
                next_led = Instant::now() + FRAME_INTERVAL;
                result
            }
            OutputAction::Command(None) | OutputAction::Cancel => {
                base = crate::palette::PALETTES
                    .lock()
                    .await
                    .get(&id)
                    .unwrap_or(base);
                if leds_owned && last_leds != Some(base) {
                    device.set_led_colors(&base).await?;
                }
                Ok(OutputStep::Stop)
            }
            OutputAction::Flush => {
                log::debug!("Flushing pending updates for {}", id);
                device.flush().await.map(|_| OutputStep::ClearFlush)
            }
            OutputAction::KeepAlive => {
                log::debug!("Sending keepalive to {}", id);
                device.keep_alive().await.map(|_| OutputStep::Continue)
            }
        };

        match result {
            Ok(OutputStep::Continue) => {}
            Ok(OutputStep::ScheduleFlush) => {
                // Anchor the batch to its first update. A continuous image stream
                // can no longer postpone flushing forever.
                schedule_flush(&mut flush_deadline);
            }
            Ok(OutputStep::ClearFlush) => flush_deadline = None,
            Ok(OutputStep::Stop) => return Ok(()),
            Err(err) if is_nonfatal_error(&err) => {
                log::error!("Device {} nonfatal output error: {}", id, err);
            }
            Err(err) => {
                return Err(err);
            }
        }
    }
}

/// Handles different combinations of "set image" event, including clearing the specific buttons and whole device
pub fn command_for_set_image(evt: SetImageEvent) -> Result<Option<DeviceCommand>, MirajazzError> {
    match (evt.position, evt.image) {
        (Some(position), Some(image)) => {
            let url = DataUrl::process(image.as_str()).unwrap();
            let (body, _fragment) = url.decode_to_vec().unwrap();

            if url.mime_type().subtype != "jpeg" {
                log::error!("Incorrect mime type: {}", url.mime_type());
                return Ok(None);
            }

            let image = load_from_memory_with_format(body.as_slice(), image::ImageFormat::Jpeg)?;
            Ok(Some(DeviceCommand::SetImage { position, image }))
        }
        (Some(position), None) => Ok(Some(DeviceCommand::ClearImage(position))),
        (None, None) => Ok(Some(DeviceCommand::ClearAll)),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod led_tests {
    use super::*;
    use crate::effects::{self, EffectKind, Preferences};
    use std::sync::{
        Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Default)]
    struct RecordingDevice {
        leds: StdMutex<Vec<(Instant, LedPalette)>>,
        images: AtomicUsize,
        flushes: AtomicUsize,
    }

    impl OutputDevice for RecordingDevice {
        async fn set_button_image(
            &self,
            _: u8,
            _: mirajazz::types::ImageFormat,
            _: DynamicImage,
        ) -> Result<(), MirajazzError> {
            self.images.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        async fn clear_button_image(&self, _: u8) -> Result<(), MirajazzError> {
            Ok(())
        }
        async fn clear_all_button_images(&self) -> Result<(), MirajazzError> {
            Ok(())
        }
        async fn set_brightness(&self, _: u8) -> Result<(), MirajazzError> {
            Ok(())
        }
        async fn set_led_colors(&self, colors: &[[u8; 3]]) -> Result<(), MirajazzError> {
            self.leds
                .lock()
                .unwrap()
                .push((Instant::now(), colors.try_into().unwrap()));
            Ok(())
        }
        async fn flush(&self) -> Result<(), MirajazzError> {
            self.flushes.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        async fn keep_alive(&self) -> Result<(), MirajazzError> {
            Ok(())
        }
    }

    async fn tick() {
        tokio::time::sleep(FRAME_INTERVAL + Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
    }

    fn effect(kind: EffectKind, owner: u64, cancelled: CancellationToken) -> Effect {
        Effect {
            kind,
            owner,
            cancelled,
            started: Instant::now(),
            duration: Duration::from_millis(kind.max_ms()),
            preferences: Preferences {
                reduced_motion: false,
                brightness_cap: 32,
            },
        }
    }

    #[tokio::test(start_paused = true)]
    async fn output_worker_preserves_new_manual_palette_through_stop_and_client_loss() {
        let _test_guard = crate::palette::TEST_LOCK.lock().await;
        let id = crate::led_ipc::new_session_id();
        let original = [[1, 2, 3]; 24];
        crate::palette::PALETTES.lock().await.select(&id, original);
        let (sender, receiver) = mpsc::channel(128);
        let (effects, effect_receiver) = watch::channel(None);
        let token = Arc::new(CancellationToken::new());
        let connection = CancellationToken::new();
        let device = Arc::new(RecordingDevice::default());
        let worker = tokio::spawn(device_output_task(
            id.clone(),
            Kind::VsdInsideM18,
            device.clone(),
            receiver,
            effect_receiver,
            token.clone(),
        ));
        tokio::task::yield_now().await;
        effects::submit(
            &effects,
            effect(EffectKind::StopMode, 1, connection.clone()),
        );
        tick().await;
        let changed = [[12, 13, 14]; 24];
        crate::palette::PALETTES.lock().await.select(&id, changed);
        sender
            .send(DeviceCommand::SetLedColors(changed))
            .await
            .unwrap();
        tokio::task::yield_now().await;
        // Stop remains red while the manual selection updates only the resting base.
        tick().await;
        let frame = device.leds.lock().unwrap().last().unwrap().1;
        assert_eq!(frame[0], [32, 0, 0]);
        assert_eq!(frame[23], changed[23]);
        assert!(!effects::submit(
            &effects,
            effect(EffectKind::Ready, 1, connection.clone())
        ));
        connection.cancel();
        tick().await;
        assert_eq!(device.leds.lock().unwrap().last().unwrap().1, changed);
        assert_eq!(
            crate::palette::PALETTES.lock().await.get(&id),
            Some(changed)
        );
        let writes = device.leds.lock().unwrap().len();
        for _ in 0..8 {
            tick().await;
        }
        assert_eq!(device.leds.lock().unwrap().len(), writes); // No idle writes.
        token.cancel();
        worker.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn coalesced_frames_are_rate_limited_and_do_not_block_lcd_flushes() {
        let _test_guard = crate::palette::TEST_LOCK.lock().await;
        let id = crate::led_ipc::new_session_id();
        let (sender, receiver) = mpsc::channel(128);
        let (effects, effect_receiver) = watch::channel(None);
        let token = Arc::new(CancellationToken::new());
        let device = Arc::new(RecordingDevice::default());
        let worker = tokio::spawn(device_output_task(
            id,
            Kind::VsdInsideM18,
            device.clone(),
            receiver,
            effect_receiver,
            token.clone(),
        ));
        tokio::task::yield_now().await;
        for _ in 0..100 {
            effects::submit(
                &effects,
                effect(EffectKind::Start, 1, CancellationToken::new()),
            );
        }
        sender
            .send(DeviceCommand::SetImage {
                position: 0,
                image: DynamicImage::new_rgb8(64, 64),
            })
            .await
            .unwrap();
        tokio::task::yield_now().await;
        for _ in 0..8 {
            tick().await;
        }
        let writes = device.leds.lock().unwrap().clone();
        assert_eq!(writes.len(), 8);
        assert!(
            writes
                .windows(2)
                .all(|pair| pair[1].0 - pair[0].0 >= FRAME_INTERVAL)
        );
        assert_eq!(device.images.load(Ordering::Relaxed), 1);
        assert!(device.flushes.load(Ordering::Relaxed) >= 1);
        token.cancel();
        worker.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn disabled_interface_does_not_change_unsaved_hardware_palette() {
        let _test_guard = crate::palette::TEST_LOCK.lock().await;
        let (sender, receiver) = mpsc::channel(128);
        let (_effects, effect_receiver) = watch::channel(None);
        let token = Arc::new(CancellationToken::new());
        let device = Arc::new(RecordingDevice::default());
        let worker = tokio::spawn(device_output_task(
            crate::led_ipc::new_session_id(),
            Kind::VsdInsideM18,
            device.clone(),
            receiver,
            effect_receiver,
            token.clone(),
        ));
        tokio::task::yield_now().await;
        tick().await;
        assert!(device.leds.lock().unwrap().is_empty());
        let manual = [[18, 20, 22]; 24];
        sender
            .send(DeviceCommand::SetLedColors(manual))
            .await
            .unwrap();
        tokio::task::yield_now().await;
        tick().await;
        sender
            .send(DeviceCommand::SetLedColors(manual))
            .await
            .unwrap();
        tokio::task::yield_now().await;
        tick().await;
        assert_eq!(device.leds.lock().unwrap().len(), 1);
        assert_eq!(device.leds.lock().unwrap()[0].1, manual);
        token.cancel();
        worker.await.unwrap().unwrap();
    }
}
