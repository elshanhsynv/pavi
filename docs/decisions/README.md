# Architecture decisions (ADRs)

One short file per decision that would be expensive to reverse or easy to "improve" by accident.
Before changing something an ADR covers, write a new ADR that supersedes it.

Name files `NNNN-short-title.md` and use this template:

```markdown
# NNNN. Title

- Status: accepted | superseded by NNNN
- Date: YYYY-MM-DD

## Context
What problem or force made a decision necessary.

## Decision
What we do, in one or two sentences.

## Consequences
What gets easier, what gets harder, what we must keep doing because of this.
```

0001–0004 were reconstructed after the fact from the code and docs. **Owner: confirm or correct the
"Context" and "Decision" of each**; until then treat their reasons as informed guesses, not intent.

| ADR | Decision |
|---|---|
| [0001](0001-no-async-runtime.md) | Std threads and bounded channels, no async runtime |
| [0002](0002-bounded-memory.md) | Every buffer has an explicit cap |
| [0003](0003-windowed-grid.md) | The grid shows huge files through a 500,000-row window |
| [0004](0004-custom-query-engine.md) | A small custom query engine instead of DataFusion/DuckDB/Polars |
| [0005](0005-external-sort.md) | Sorts larger than memory spill to temporary files (amends 0002) |
