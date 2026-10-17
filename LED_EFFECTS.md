# Optional local LED effects (protocol v1)

This fork adds a generic, opt-in interface inside the existing M18 plugin process.
It is a device presentation capability. It does not launch or stop workbenches,
observe application readiness, run commands, open another HID handle, or require
OpenDeck/OpenAction changes. The normal OpenDeck plugin identity, assignments,
Set LED Colors action, and saved `ledPalettes` remain compatible.

## Enable and recover

With OpenDeck closed, place `led-effects.json` **beside the installed M18 plugin
executable**, owned by your account and not writable by other users:

```json
{"enabled": true, "reduced_motion": false, "brightness_cap": 32}
```

The file is not included in the release artifact. Missing file, `enabled: false`,
or Windows means no effect endpoint. `reduced_motion: true` makes each temporary
cue static; set `enabled: false` to disable all temporary cues. `brightness_cap`
is an RGB channel ceiling for the effect colors, 0–64 out of 255, default 32.
It does not change LCD brightness or rewrite your selected resting palette.
Unknown fields, malformed/oversized configuration, a symlink, unsafe ownership,
or writable-by-others permissions disable the extension with an actionable log.
Configuration is read at plugin start, not watched or rewritten.

On macOS/Linux, the plugin creates `~/.opendeck-m18/` with mode 0700 and serves
`~/.opendeck-m18/led.sock` with mode 0600. An existing directory must already be
owned by the effective user with exactly mode 0700. Connecting peers must have
the same effective UID. This is a same-user boundary, **not isolation between
applications running as that user**. No TCP listener or new daemon is added.
The parent path must fit the platform's Unix-socket path limit.

A socket already at that path is never removed automatically. If binding fails,
normal device support continues without the effect interface. After a crash,
close OpenDeck and verify that no M18 plugin process is running before removing
the stale `led.sock` and restarting. Never delete another live process's socket.
On normal shutdown the plugin removes only the socket inode it created. It
retains the private directory. Disable/remove the opt-in JSON and restart to
return to ordinary device behavior. Older artifacts ignore this unbundled file;
keep the prior tagged plugin archive for normal OpenDeck reinstall/recovery.

## Framing and capabilities

Use one persistent Unix stream connection. Send one UTF-8 JSON object per line,
**at most 1024 bytes including the newline**, and wait for each newline-delimited
JSON reply before sending the next request. Send a hello before requesting an
effect. Unknown fields/operations/versions and invalid values are rejected;
malformed/oversized/truncated frames and transport timeouts may close the stream.
Replies contain bounded error codes, never an echo of raw request data.

```json
{"version":1,"op":"hello"}
```

The response includes `ok`, `version`, `effects`, `max_fps: 8`, configured
`brightness_cap`/`reduced_motion`, and up to 32 `devices`, each with `device` and
`session`. Only fully published, non-cancelled device generations appear. A
session identifier changes on every device connection and process restart. It
is an equality token, not an authentication secret. Do not cache one across a
reconnect. Empty devices means no published device; there is no default target.

There is **one controlling connection**. Other connections cannot preempt it;
use a short client handshake deadline and treat an unanswered hello as
unavailable. The kernel accept backlog is not another active controller. A
connection has a 15-second complete-frame deadline and a 500-ms write deadline;
closing it cancels its effects. Send hello periodically if keeping it idle, or
close it when no longer needed. Message processing is capped at eight per second.
At most 32 device/session sequence records are retained per connection.

## Requests and effects

Copy the exact `device` and `session` from hello. `sequence` must strictly
increase per device/session on this connection, from 1 through 2^53−1. Even a
priority-rejected valid request consumes its sequence. Replayed requests are
rejected; they do not renew a lease or cancel a newer effect.

```json
{"version":1,"op":"effect","device":"18-EXAMPLE","session":"COPY-FROM-HELLO","sequence":1,"effect":"start","duration_ms":2000}
{"version":1,"op":"cancel","device":"18-EXAMPLE","session":"COPY-FROM-HELLO","sequence":2}
```

An `ok: true, status: queued` response means the latest request is accepted for
the current device worker. It does **not** confirm a hardware write, application
readiness, or a successful lifecycle action. Stale/unknown sessions fail closed.
There is no palette array, raw HID, file path, script, URL, or command operation.

| Effect | Display on perimeter LEDs 1–22 | Maximum lease |
| --- | --- | --- |
| `start` | Low amber traveling light; static amber in reduced motion | 8000 ms |
| `ready` | One gentle mint/green bloom toward the current resting palette; static green in reduced motion | 1000 ms |
| `stop-mode` | Steady subdued red | 10000 ms |
| `stop-pending` | Steady subdued red | 8000 ms |
| `failure` | Two slow amber pulses; static amber in reduced motion | 1000 ms |

Minimum lease is 125 ms. Inner LEDs 23–24 retain their current manual palette
colors. The perimeter order reuses the existing inspector/wire numbering.
Effects expire automatically; a client may renew only while the actual condition
still holds. `stop-mode` outranks `stop-pending`, which outranks the other cues.
Lower priority requests are rejected while a higher priority lease is active.
To intentionally leave Stop mode, cancel it before requesting a lower priority
cue. A replacement/cancel discards the old cue; it never resumes in the background.

The existing output worker samples a single coalesced effect slot and serializes
LED, LCD, flush, brightness, and keepalive operations. It schedules the next LED
sample at least 125 ms after a sample/write completes: no frame catch-up burst,
and no redundant write for an unchanged palette. Set LED Colors updates the
**resting** palette immediately in its existing owner, without overriding active
Stop feedback. On expiration/cancel/client loss, the next sample restores the
latest selected palette, including changes made during an effect. Temporary
frames never update global settings. A final best-effort restoration during
normal device-worker shutdown is deliberately immediate and may fall outside
the ordinary eight-updates-per-second ceiling. A failed HID write discards the
connection through the existing device error path; no hardware success is claimed.

If no saved palette exists, effects restore the plugin's existing DEFAULT_PALETTE.
With effects disabled and no manual selection, no new startup/idle LED write is
introduced. With a selected palette, normal reconnect restoration remains.
Manual palette requests are now coalesced into the same <=8-Hz LED scheduler,
adding up to one 125-ms scheduling interval before a normal write.

## Client responsibilities

The caller owns application identity and fresh status evidence. Request `start`
only during a real pending activation; request `ready` once only after fresh,
positive readiness evidence, never merely process exit or a running process.
Red LEDs provide no stop eligibility or authority. Cancel on page/profile or
selection change, error/slow observation, operation completion, timeout, device
loss, and client shutdown. Guard asynchronous callbacks with the caller's own
context/identity generation; an increasing device sequence alone cannot detect
that a callback refers to an old workbench selection. On reconnect negotiate a
new session and reevaluate current intent; never replay an old queued effect.
Treat a missing/older endpoint, timeout or protocol error as optional lighting
unavailable, and leave ordinary actions/LCD feedback functional.

The device plugin cannot observe a client's page or workbench transitions.
Its independent protection is bounded leases, current device sessions, sequence
rejection, priority, and automatic connection-loss cancellation. Lifecycle-to-cue
integration is separate work in the consumer; this PR does not claim it exists.

## Validation and release

Use the existing Rust toolchain and release packaging. `libc` is now a direct
Unix-only dependency for the effective UID lookup; it was already in Cargo.lock.
Tokio's Unix sockets, peer credentials, timers, cancellation, and watch channel,
serde_json, existing palette store, and existing device output worker supply the
rest. The missing inter-plugin effect transport requires the small custom
protocol; there is no new framework, client dependency, USB implementation,
Python wheel content, or unrelated package/image release. GPL-3.0 notices remain
intact; distribute modified device-plugin source with its normal license duties.
No release is created by opening this PR; the existing release workflow remains
manual and independently versions this plugin.

Run `cargo fmt --all -- --check`, `cargo test --locked`, and
`cargo clippy --locked --all-targets -- -D warnings`. The PR's native check
workflow covers Linux, macOS and Windows (where IPC is disabled).
Tests use recorded output devices/paused time and Unix socket pairs, never a real
M18 or Runtime/provider request. They exercise frame/priority/expiry/cancellation,
latest-palette restoration, rate/coalescing and LCD flush behavior, strict parser
bounds and current-session visibility. Native filesystem socket/permission checks
must run on a host that permits AF_UNIX pathname binding.

Before calling the device behavior accepted, evaluate a tagged artifact on the
Mac/M18: brightness, physical traveling order, bloom/pulse pacing, real command
rate and latency under LCD load, static/disabled modes, manual palette changes,
Stop expiration, application restart, unplug/replug, concurrent clients, and
crash/stale-socket recovery. Record those separately from offline test evidence.
