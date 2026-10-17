#![cfg_attr(not(unix), allow(dead_code))]
//! Transient palettes only. The existing device output worker owns all timing/HID.
use std::time::Duration;

use tokio::{sync::watch, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::palette::LedPalette;

pub const FRAME_INTERVAL: Duration = Duration::from_millis(125);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EffectKind {
    Start,
    Ready,
    StopMode,
    StopPending,
    Failure,
}

impl EffectKind {
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "start" => Self::Start,
            "ready" => Self::Ready,
            "stop-mode" => Self::StopMode,
            "stop-pending" => Self::StopPending,
            "failure" => Self::Failure,
            _ => return None,
        })
    }

    pub fn max_ms(self) -> u64 {
        match self {
            Self::Start | Self::StopPending => 8000,
            Self::Ready | Self::Failure => 1000,
            Self::StopMode => 10000,
        }
    }

    fn priority(self) -> u8 {
        match self {
            Self::StopMode => 3,
            Self::StopPending => 2,
            _ => 1,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Preferences {
    pub reduced_motion: bool,
    pub brightness_cap: u8,
}

#[derive(Clone, Debug)]
pub struct Effect {
    pub kind: EffectKind,
    pub owner: u64,
    pub cancelled: CancellationToken,
    pub started: Instant,
    pub duration: Duration,
    pub preferences: Preferences,
}

impl Effect {
    pub fn active(&self, now: Instant) -> bool {
        !self.cancelled.is_cancelled() && now < self.started + self.duration
    }

    pub fn frame(&self, base: &LedPalette, now: Instant) -> LedPalette {
        if !self.active(now) {
            return *base;
        }
        let mut palette = *base;
        let millis = now.saturating_duration_since(self.started).as_millis() as usize;
        let cap = self.preferences.brightness_cap;
        let amber = [cap, cap / 2, 0];
        let red = [cap, 0, 0];
        let green = [cap / 4, cap, cap / 2];
        for (index, color) in palette.iter_mut().take(22).enumerate() {
            *color = if self.preferences.reduced_motion {
                match self.kind {
                    EffectKind::Start | EffectKind::Failure => amber,
                    EffectKind::Ready => green,
                    EffectKind::StopMode | EffectKind::StopPending => red,
                }
            } else {
                match self.kind {
                    EffectKind::Start => {
                        // Wire indices 0..21 follow the perimeter; inner 22/23 are untouched.
                        let distance = (index + 22 - (millis / 125) % 22) % 22;
                        let divisor = if distance < 3 { 1 << distance } else { 12 };
                        amber.map(|channel| channel / divisor)
                    }
                    EffectKind::Ready => {
                        let progress = millis as f32 / self.duration.as_millis() as f32;
                        let alpha = (1.0 - (2.0 * progress - 1.0).abs()).clamp(0.0, 1.0);
                        std::array::from_fn(|channel| {
                            (base[index][channel] as f32 * (1.0 - alpha)
                                + green[channel] as f32 * alpha) as u8
                        })
                    }
                    EffectKind::StopMode | EffectKind::StopPending => red,
                    // Two slow, low-intensity pulses, then restore the current base.
                    EffectKind::Failure => {
                        if millis % 500 < 250 {
                            amber
                        } else {
                            base[index]
                        }
                    }
                }
            };
        }
        palette
    }
}

/// Coalesce at the owner, before the worker samples. A lower-priority request
/// cannot replace an armed Stop indication, even between output ticks.
pub fn submit(sender: &watch::Sender<Option<Effect>>, effect: Effect) -> bool {
    sender.send_if_modified(|current| {
        if current.as_ref().is_some_and(|old| {
            old.active(effect.started) && old.kind.priority() > effect.kind.priority()
        }) {
            return false;
        }
        *current = Some(effect);
        true
    })
}

pub fn cancel(sender: &watch::Sender<Option<Effect>>, owner: u64) {
    sender.send_if_modified(|current| {
        if current.as_ref().is_some_and(|effect| effect.owner == owner) {
            *current = None;
            true
        } else {
            false
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn effect(kind: EffectKind, now: Instant) -> Effect {
        Effect {
            kind,
            owner: 1,
            cancelled: CancellationToken::new(),
            started: now,
            duration: Duration::from_millis(kind.max_ms()),
            preferences: Preferences {
                reduced_motion: false,
                brightness_cap: 32,
            },
        }
    }

    #[test]
    fn travelling_start_preserves_inner_leds_and_expires() {
        let now = Instant::now();
        let base = [[7, 8, 9]; 24];
        let start = effect(EffectKind::Start, now);
        let first = start.frame(&base, now);
        let next = start.frame(&base, now + FRAME_INTERVAL);
        assert_eq!(first[0], [32, 16, 0]);
        assert_eq!(next[1], [32, 16, 0]);
        assert_eq!(&next[22..], &base[22..]);
        assert_eq!(start.frame(&base, now + start.duration), base);
        start.cancelled.cancel();
        assert_eq!(start.frame(&base, now), base);
    }

    #[test]
    fn stop_priority_and_cancel_never_resume_an_old_effect() {
        let now = Instant::now();
        let (sender, receiver) = watch::channel(None);
        assert!(submit(&sender, effect(EffectKind::Start, now)));
        assert!(submit(&sender, effect(EffectKind::StopMode, now)));
        assert!(!submit(&sender, effect(EffectKind::Ready, now)));
        cancel(&sender, 2); // Another client's stale cancellation is powerless.
        assert_eq!(
            receiver.borrow().as_ref().unwrap().kind,
            EffectKind::StopMode
        );
        cancel(&sender, 1);
        assert!(receiver.borrow().is_none());
    }

    #[test]
    fn expired_or_disconnected_stop_does_not_block_new_owner() {
        let now = Instant::now();
        let (sender, _) = watch::channel(None);
        let stop = effect(EffectKind::StopMode, now);
        let connection = stop.cancelled.clone();
        assert!(submit(&sender, stop));
        connection.cancel();
        let mut next = effect(EffectKind::Start, now);
        next.owner = 2;
        assert!(submit(&sender, next));
        assert!(submit(
            &sender,
            effect(EffectKind::Ready, now + Duration::from_secs(9))
        ));
    }

    #[test]
    fn ready_bloom_returns_to_latest_palette_and_motion_can_be_static() {
        let now = Instant::now();
        let mut ready = effect(EffectKind::Ready, now);
        let base = [[0; 3]; 24];
        assert_eq!(
            ready.frame(&base, now + Duration::from_millis(500))[0],
            [8, 32, 16]
        );
        let changed = [[10, 20, 30]; 24];
        assert_eq!(ready.frame(&changed, now + Duration::from_secs(1)), changed);
        ready.preferences.reduced_motion = true;
        assert_eq!(
            ready.frame(&base, now),
            ready.frame(&base, now + FRAME_INTERVAL)
        );
        ready.preferences.brightness_cap = 0;
        assert_eq!(ready.frame(&base, now), base);
    }
}
