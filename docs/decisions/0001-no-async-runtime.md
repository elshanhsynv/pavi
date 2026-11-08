# 0001. No async runtime

- Status: accepted
- Date: 2026-11-08 (recorded after the fact)

## Context
All work is local file I/O plus CPU-bound decoding. egui redraws on its own frame loop and only needs a way to pick up finished results.

## Decision
Use a fixed pool of `std::thread` workers (one per CPU) fed by a bounded `sync_channel`, with cooperative cancellation through `Arc<AtomicBool>`. The UI polls results with `try_recv()` each frame. No tokio, async-std or rayon.

## Consequences
- Simple to reason about and debug; no executor, no `Send + 'static` futures.
- Back-pressure is explicit: a full queue drops the request and it is retried on a later frame.
- Long work must check cancellation itself, at least once per batch.
- `cargo deny` bans `tokio` and `async-std` so this isn't changed by accident.
