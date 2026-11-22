# 0016. Counts trust statistics that prove a whole row group matches; the CLI counts on threads

- Status: accepted
- Date: 2026-11-22
- Amends: [0011](0011-cli-sql.md) (only `--sql` used more than the main thread)

## Context
A filter's count (`count_matches_by_row_group`, ADR 0012, and the CLI's `--count --filter`) skipped row groups whose footer statistics prove that *no* row matches, but read every other row group in full, on one thread. On the billion-row sample, `--count --filter "value_int > 5000000000"` took 2 min 45 s. Half of its row groups lie entirely above the bound, so the statistics already give their count, and the other half are ruled out.

Until now a stored null count of zero was not trusted for pruning (null tests never prune). Older readers turned a missing null count into 0. The parquet crate PAVI uses (60) keeps "missing" apart: `null_count_opt()` is `None` when the writer recorded nothing.

## Decision
- **A row group counts in full, unread, when its statistics prove every row passes** (`FilterExpr::must_match`, `Comparison::must_match_statistics`). Proof needs all of:
  - a recorded null count of 0 (a null fails every comparison; `IS NOT NULL` needs only this);
  - exact min and max, as the footer marks them (truncated text proves nothing);
  - statistics in the current format, not the deprecated signed-order fields;
  - for floats, a recorded NaN count of 0, since min and max leave NaN out;
  - for `AND`, every part proven; for `OR`, one part.
  `contains`, `LIKE`, `IS NULL`, bloom filters and the page index never prove a full match. Folder partition columns prove it from their folder value.
- **The CLI's `--count` with a filter counts on threads.** Row groups are cut into chunks of consecutive groups of about 1,048,576 rows (`COUNT_CHUNK_ROWS`). Up to 16 threads (`MAX_COUNT_THREADS`, at most the machine's parallelism) take chunks one at a time and add up their counts (`count_matches_in_row_groups`). The main thread only shows progress. The first error stops the other threads.
- The app's count stays on one worker of the pool, so it never takes the grid's workers. It gets the statistics proof.

## Consequences
- A filter on a sorted or clustered column counts in about the time of reading its footer plus the row groups the bound cuts through. A filter the statistics can't settle counts up to about as many times faster as there are cores.
- A writer that records a wrong null count of 0, or wrong exact bounds, now gives a wrong count, not only a slower scan. PAVI already relies on these statistics to rule row groups out, so this widens the trust in footers but doesn't create it.
- The CLI's `--count` holds one reader and one 4,096-row batch of the filter's columns per thread, so up to 16 of each.
