# 0015. Remote requests wait on a helper thread, so a cancel ends the wait

- Status: accepted
- Date: 2026-11-19
- Amends: [0008](0008-remote-files.md) (a request in flight can now be interrupted)

## Context
Remote reads (ADR 0008) are blocking `ureq` requests made on the worker that runs the read. Cancellation was checked between batches, so a request already sent ran until it answered or timed out: up to 15 s to connect and 30 s for an answer, times three attempts, or a 300 s body. Cancelling the open of a slow or silent server, or a scan over a slow link, left the task and its worker busy for that long, and closing the tab didn't help. An async client would solve this but is ruled out (ADR 0001), and `ureq` has no way to abort a call from another thread.

## Decision
- The runtime runs each task inside `parquet_reader::interruptible(flags, …)` with the task's cancellation flag and the runtime's shutdown flag. The flags are kept in a thread-local, so `parquet_reader` needs no runtime types (dependencies still point down).
- Inside it, every remote request (the probe at open, a range, a download) runs on a short-lived helper thread (`pavi-http`). The worker waits for the answer and checks the flags every `INTERRUPT_POLL` (50 ms). When one is set, the worker returns `Cancelled` at once and marks the request abandoned.
- The helper checks that mark before each attempt and between 64 KB blocks of the body. An abandoned request therefore stops at its next block, or when its own timeouts end it, and its result is dropped. At most one helper per worker is waited on; abandoned ones end within their timeouts.
- Outside `interruptible` (the CLI, tests, the app's own threads), requests run on the calling thread as before.
- A download reports progress through a shared byte count and stops when its checkpoint asks, on either thread.

## Consequences
- Cancelling an open or a read of a remote file ends the task within about 50 ms, whatever the server does. The worker is free for the next task.
- A request costs one thread start, which is small next to a network round trip.
- An abandoned request can keep its connection open until its timeout. That is a few idle threads at most, and the range cache only ever sees completed answers.
