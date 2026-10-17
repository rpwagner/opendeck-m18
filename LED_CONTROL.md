# External LED palette control

This opt-in interface lets local applications set the M18's 24 RGB LEDs through
the existing plugin process and serialized device output worker. It provides
capability discovery, a temporary palette override, and clearing that override.
Clients choose colors and send successive palettes if they need animation.

## Configuration

With OpenDeck closed, put an owned, regular `led-control.json` beside the installed
plugin executable, with no write permissions for other users:

```json
{"enabled":true}
```

A missing file or `enabled:false` disables the endpoint. Windows continues to use
the existing action without this Unix interface. Configuration is read only at
plugin start; invalid/unknown fields, symlinks, unsafe permissions or files larger
than 4096 bytes disable external control with a log message. The file is not
included in release packages.

On macOS/Linux the plugin creates `~/.opendeck-m18/` with mode 0700 and
`~/.opendeck-m18/led.sock` with mode 0600. An existing directory must already have
that mode and belong to the effective user. Peer credentials must identify the
same effective UID. Applications running as that user share this boundary.
There is no TCP listener, separate daemon or additional HID owner.

An existing socket is never unlinked automatically. If a crash leaves one behind,
close OpenDeck, verify the plugin is no longer running, remove only that stale
socket, and restart. Normal shutdown removes only the socket inode created by
this process. The private directory is retained. Disable/remove the opt-in file
and restart to return to ordinary action-only operation. Preserve a known-good
plugin archive for recovery; existing saved palettes and assignments are unchanged.

## Protocol v1

Open a persistent Unix stream connection. Send one UTF-8 JSON object per line,
at most 1024 bytes including the newline, and wait for its newline-delimited
reply. Start with:

```json
{"version":1,"op":"hello"}
```

The reply reports `version`, `operations:["set","clear"]`, `led_count:24`,
`max_hz:8`, `max_ttl_ms:10000`, and up to 32 connected `devices`. Each device has
`device` and `session` strings. Only fully published, non-cancelled devices appear.
Use those exact strings in subsequent requests; the session changes on reconnect
and process restart. It is an equality token, not an authentication secret.

| Operation | Additional required fields | Result |
| --- | --- | --- |
| `set` | `device`, `session`, `ledColors`, `ttl_ms` | Replace the current temporary palette |
| `clear` | `device`, `session` | Restore the latest saved/manual resting palette |

`ledColors` is exactly 24 `#RRGGBB` strings, using the same format, ordering and
parser as the existing Set LED Colors action. All 24 values are controlled;
there are no reserved colors or LED positions. `ttl_ms` is an integer from 125
to 10000. A later set replaces the previous palette and deadline. Clients select
brightness, animation timing and any presentation policy.

For example, construct a set request with:

```python
request = {
    "version": 1, "op": "set",
    "device": device["device"], "session": device["session"],
    "ledColors": ["#102030"] * 24, "ttl_ms": 1000,
}
```

Clear it with:

```python
request = {
    "version": 1, "op": "clear",
    "device": device["device"], "session": device["session"],
}
```

`{"ok":true,"version":1,"status":"queued"}` acknowledges acceptance for the
current worker, not a confirmed hardware write. Error replies contain bounded
codes such as `invalid-palette`, `invalid-ttl` or `stale-or-unknown-session`.
Unknown fields, operations and versions are rejected. Oversized/truncated frames,
15-second complete-frame timeouts, or 500-ms reply-write timeouts close the stream.
A hello may keep an otherwise idle connection open.

There is one controlling connection, with requests applied in stream order.
Another client cannot preempt it; use a bounded handshake timeout to detect an
occupied/unavailable endpoint. Reconnect, discover a fresh session, and send only
current intent. The API maintains no priority system or application state.

## Palette ownership and limits

Overrides never write OpenDeck global settings. Expiry, clear, connection loss
and normal device-worker shutdown restore the **latest** manual palette, including
manual selections made while an override was active. A manual selection updates
the resting palette without ending the current override. Without a saved palette,
the existing DEFAULT_PALETTE is the fallback.

The worker coalesces overrides into one latest-value slot, serializes them with
LCD/brightness/keepalive writes, and samples at most every 125 ms. Unchanged
palettes are not resent; delayed work does not produce catch-up bursts. Manual
LED requests use that same scheduler, adding up to a 125-ms scheduling interval.
Normal shutdown may immediately restore the resting palette outside that rate
ceiling. No new unsaved startup/idle palette write occurs without external control.

## Build and validation

Use the existing release packaging and GPL-3.0 license. The only added dependency
is Unix-only `libc` for effective UID lookup, already present in Cargo.lock.
Existing Tokio sockets, peer credentials, cancellation, watch channels and timers,
serde_json, the palette parser/store and device worker supply the implementation.
No other application or package is required.

Run `cargo fmt --all -- --check`, `cargo test --locked`, and
`cargo clippy --locked --all-targets -- -D warnings`. Native CI covers macOS,
Linux and Windows. Tests use recorded output devices, paused clocks and Unix
sockets; physical device rate/latency, RGB ordering, restart, unplug/replug,
concurrent clients and crash recovery still need hardware evaluation.
