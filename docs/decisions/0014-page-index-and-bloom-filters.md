# 0014. Filters read the page index and bloom filters, only where a test asks for them

- Status: accepted
- Date: 2026-11-19

## Context
Filters skipped whole row groups by their footer min/max, and nothing finer. A lookup in a column that is sorted within each row group (`user_id == 123` in the billion-row sample, where `user_id` repeats 0…999,999 in each group of a million rows) passes every row group's min/max, so all billion rows were decoded to find a thousand: 15.4 s. A lookup of one value in a column that isn't sorted can't be pruned by min/max at all.

Parquet has two finer structures for this. The **page index** (column index and offset index, written by default by arrow-rs, pyarrow and polars) keeps each data page's min, max and first row. A **bloom filter** per column chunk answers "is this value surely absent?". Both live outside the footer, so reading them costs extra I/O. Loading them at open (`ArrowReaderOptions::with_page_index`) would make the footer of a large file much bigger in memory and slow every open, including the opens that never filter.

## Decision
The footer is still loaded without them. A filtered scan reads them **per row group, only for the columns its filter tests, while it scans** (`page_prune.rs`):

- **Page index.** For each row group of a scan run, `run_reader` reads the column and offset index of each tested column (one chunk's index ≤ `MAX_PAGE_INDEX_BYTES`, 4 MB; a larger one isn't read). Each page's min and max become a `Statistics` value and go through the same test that prunes row groups (`Comparison::might_match_statistics`), so a page and a row group are judged by one rule. A page of nulls passes no comparison. `AND` intersects the rows its parts allow, `OR` unites them, and a test that can't be judged (`IS NULL`, `LIKE`, a nested or partition column, no page index) allows every row. The surviving row ranges become the reader's `RowSelection`, together with the existing skip to `start_row`.
- **Row numbers.** Pruned pages leave gaps, so the rows a reader decodes are no longer consecutive source rows. `RowMap` keeps the selected ranges (one per surviving stretch of pages) and maps each decoded row back to its row in the file, for a filtered window's row numbers and resume point, the per-row-group match count (ADR 0012), and the position every checkpoint reports.
- **Bloom filters.** `row_group_might_match` asks a column chunk's bloom filter about each `==` test, after the min/max test and only when the footer records the filter's length (≤ `MAX_BLOOM_FILTER_BYTES`, 16 MB). The value is looked up as the column stores it (`Comparison::bloom_key`): ints, `Date32`, text and binary. Floats, decimals and timestamps are left out because their stored form can differ from what PAVI reads. A row group is skipped when an `AND` has a part the bloom filter rules out, or when it rules out every part of an `OR`. The file is opened only when a bloom filter must be read.

Sorted and aggregated scans with a filter use the same pruned reader. Paging without a filter is unchanged.

## Consequences
- A lookup in a clustered or sorted column decodes one page per row group: `user_id == 123` on the billion-row sample takes 2.1 s instead of 15.4 s. The time left is decoding one ~125,000-row page in each of the 1,000 row groups; files with smaller pages gain more.
- A lookup of an absent value in a file with bloom filters skips every row group whose bloom filter rules it out. Many writers (polars, pyarrow by default) don't write bloom filters; for those files nothing changes.
- Pruning never changes results: a missing, oversized or unreadable page index or bloom filter means the rows are read and tested, as before.
- Each filtered scan reads a few small byte ranges per row group (about 110 ms over the 1,000 row groups of the billion-row sample). Remote files make one range request per index.
