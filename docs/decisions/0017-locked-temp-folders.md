# 0017. Temporary folders are locked while in use; left-over ones are removed at start

- Status: accepted
- Date: 2026-11-22
- Amends: [0005](0005-external-sort.md) (left-over spill folders were removed after a day), [0007](0007-other-formats.md), [0011](0011-cli-sql.md)

## Context
Sorts that spill (ADR 0005), opened CSV/JSON/Arrow files (ADR 0007), downloads and prepared join tables (ADR 0009) each use a folder in the system temp directory, deleted when the work ends. A process that is killed instead (Ctrl+C on a CLI sort, a crash, Task Manager) runs no destructors, so its folder stays. A large spilled sort leaves up to 32 GiB. PAVI removed such folders only once they were a day old, and only when another folder of the same kind was made. Until then the disk space was gone.

PAVI can't install a Ctrl+C handler without `unsafe` or a new dependency, and the standard library can't ask whether another process is still alive. It can lock a file: `File::try_lock`, stable since Rust 1.89. The operating system releases a lock when its process ends, however it ends.

## Decision
- Every temporary folder (`spill::TempFolder`) holds a `.pavi-lock` file, locked for as long as the folder is in use and unlocked just before the folder is deleted.
- A folder in the temp directory whose name starts with `pavi-`, whose `.pavi-lock` can be locked (its process has ended), and that hasn't changed for a minute is left over. It is removed with its lock file last, so a removal cut short is finished next time. The minute covers the moment between creating a folder and locking it.
- This cleanup (`parquet_reader::remove_leftover_temp_folders`) runs on a `pavi-cleanup` thread when the app starts and when the CLI starts; the CLI waits for it only after its output is written. It also runs before each new temporary folder is made.
- Folders without a lock file (from an older PAVI, or on a file system without locks) keep the old rule: removed after a day by the next folder of the same kind.

## Consequences
- A sort stopped with Ctrl+C gives its disk space back at the next `pavi-cli` command or app start, a minute after it stopped. In a test, a killed sort left 692 MB; the next quick CLI command removed it (406 ms in all, its answer printed first) and the app removed one within 5 s of starting.
- Normal runs only list the temp directory: a CLI `--count` still takes about 17 ms.
- A folder in use is never removed: its lock is held, by a handle that no other process and no other handle of the same process can lock past.
- The CLI's own partial output (`PATH.pavi-cli-<pid>.tmp`, next to `-o PATH`) is not a temporary folder and is not covered.
