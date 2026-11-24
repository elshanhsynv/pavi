# 0018. Files that change on disk are noticed, refused for reads and reloaded on request

- Status: accepted
- Date: 2026-11-24

## Context
A source reads each file's footer once, when it opens, and reopens the file by path for every read. When another program rewrites the file while PAVI shows it (a pipeline writing its output again, a notebook saving over it), the next read used the old footer against the new bytes. That ended in a decode error, a malformed-data message or, worse, plausible rows from the wrong offsets. Nothing told the user the file had changed. Reload (Ctrl+R) existed, but it also dropped the tab's filter and sort.

## Decision
- **Stamp at open.** Each local Parquet file's size and modification time are taken from the handle its footer is read through (`change::FileStamp`). A folder dataset also records the modification time of every folder it searched, taken just before the folder is listed (`dataset::Listing`). For a CSV, JSON or Arrow file (ADR 0007) the original's size and time are taken before it is read; PAVI's temporary copy is not checked.
- **Refuse stale reads.** Every read opens the file through `SourceFile::open`, which compares the open handle's size and time with the stamp and fails with `parquet_reader::FileChanged` when they differ. It costs one `fstat` per opened file, next to the `open` it already makes. The app and CLI show the error; the app does not ask for that page again until the file is reloaded.
- **Check in the background.** `ParquetSource::check_changed` compares the stamps with the disk: one `stat` per file and per searched folder. A folder's time changes when an entry is added, removed or renamed in it, so new and deleted files are noticed without listing the folder again. The app runs it on the worker pool (`RuntimeHandle::submit_change_check`) for the open tab every 2 seconds (`watch::CHANGE_CHECK_INTERVAL`), one check at a time.
- **Say so, and reload on request.** Once a check finds a change, a bar above the grid names what changed and offers Reload; checks stop until the file is opened again. Reload (the bar, the file menu, the top bar or Ctrl+R) opens the file again in the same tab and brings back its filter and sort, the way a restart does. It does not reload by itself: the user may be in the middle of reading rows the new file no longer has.
- **Not checked:** remote objects (ADR 0008), which would need a request every interval; temporary tables PAVI wrote itself; parked tabs, until they are shown again.

## Consequences
- A rewritten file can no longer produce rows from the wrong place. Pages already in memory stay on screen until Reload, consistent with the file as it was opened.
- A file rewritten with the same size and the same modification time is not noticed. File systems keep the time to 100 ns (NTFS) or 1 ns (ext4, APFS), so this needs a writer that restores the old time on purpose.
- A writer that creates temporary files in a dataset folder (`_temporary/`, `.part` files) makes the folder look changed even if the finished files are the same; Reload then reads the same rows again.
- Idle, the app wakes every 2 seconds while a file is open, for a check that costs a few `stat` calls (up to one per file and folder of a dataset: at most 4,096 files and the folders searched).
- Reload of a SQL result shows the file's rows; the query stays in the SQL editor and its history, to run again.
