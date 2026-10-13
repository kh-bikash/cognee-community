# cognee-community-connector-biorxiv

A bioRxiv / medRxiv data-source connector for [cognee](https://github.com/topoteretes/cognee):
sync preprints into memory, so an agent can answer "what appeared in my field this week, and
has any of it been published since?".

Preprint servers sit between a paper index and nothing at all: bioRxiv and medRxiv carry
life-science and clinical work months before it reaches a journal, and a large share never gets
there. A journal index cannot answer that question; this can.

It exposes a `dlt` resource you hand to `cognee.remember(...)`, reusing cognee's existing DLT
ingestion path (`resolve_dlt_sources` → `ingest_dlt_source` → `orphan_cleanup`) so you get
**incremental re-sync** (upsert by DOI, `merge` write disposition, a posting-date cursor) and
**forget-on-delete** (preprints that leave the synced window are emitted as hard-deletes and
purged from memory on the next sync) with no core changes.

**No credentials.** The API is public: no account, no token, no approval.

## Install

```bash
uv pip install cognee-community-connector-biorxiv
# or, from this monorepo:
cd packages/connector/biorxiv && uv sync --all-extras
```

## Usage

```python
import cognee
from cognee_community_connector_biorxiv import biorxiv_source

await cognee.remember(
    biorxiv_source(
        server="biorxiv",  # or "medrxiv"
        posted_since="2026-10-01",
        category="neuroscience",
    ),
    dataset_name="preprints",
    primary_key="id",
    write_disposition="merge",  # incremental upsert by DOI
    max_rows_per_table=0,  # unlimited: orphan-cleanup sees the whole corpus
)

answer = await cognee.search(
    query_text="What new findings were posted in neuroscience, and which are published?",
    query_type=cognee.SearchType.GRAPH_COMPLETION,
    datasets=["preprints"],
)
```

`posted_since` is required. The archives hold hundreds of thousands of preprints, so an
unbounded sync is never what a caller means.

## What gets ingested

One document per preprint, keyed by DOI, as a header of structured facts followed by the
abstract:

```
DOI: 10.64898/2026.09.28.755022
Server: biorxiv
Category: neuroscience
Posted: 2026-10-01
Version: 2
Type: new results
Authors: Harvey, D. Y.; Stoll, H.; Hamilton, R. H.
Corresponding author: Denise Y Harvey (University of Pennsylvania)
Published in a journal: no
License: cc_no

Abstract:
...
```

### Versions are the thing to get right

The details endpoint returns one entry **per version**, so a revised preprint appears several
times in a window as version 1, 2, 3. Keying on DOI and keeping the highest version means
memory holds the current preprint instead of one row per revision. A live interval showed
`count_new_papers` (432) well below `total` (552) for the same window, which is exactly that
gap between new papers and revisions of existing ones.

State tracks `doi@version` rather than the DOI alone, so a preprint whose version has not moved
is skipped without re-rendering, while a new version re-emits the row and merges over the old
one.

### The `published` field

Each entry carries the DOI of the journal article if the preprint was eventually published, and
the string `"NA"` (not null) when it was not. The connector reads `"NA"` as unpublished, which
matters: taken literally it would record a journal DOI of `NA` and render a link to it.

That field changing on an existing record long after posting is a real reason to re-sync, and
it is what makes "which of the preprints we tracked actually made it into a journal?"
answerable.

### Node sets

Preprints are grouped into cognee node sets by category, server and publication state
(`category:neuroscience`, `server:biorxiv`, `state:published`), so you can ask about one field
without a dataset per category. Pass `include_node_sets=False` to skip it.

## Incremental sync and forget-on-delete

There is no "modified since" filter and no id-only projection, so the walk that would detect
deletions costs the same as the walk that fetches content. Doing both separately would double
every run for no benefit, so the two modes differ:

- **`detect_deletions=True`** (default) walks the whole configured window once and uses it for
  both purposes: rows for DOIs whose version is new to the corpus, tombstones for DOIs that have
  left the window.
- **`detect_deletions=False`** walks only from the stored cursor forward. Cheaper on a long
  window, but it cannot see a deletion.

- **The cursor day is re-read on purpose.** Posting dates have day granularity, so excluding
  the date the previous run stopped on would permanently drop a preprint posted later that same
  day. Unchanged versions are skipped by the `doi@version` check, so the re-read costs
  bandwidth rather than work.
- **Safety.** If the window returns zero preprints while DOIs were previously known, the run
  skips deletion and preserves state. A transient failure must not be read as "the window
  emptied" and purge your dataset.

### An honest limit

Preprint servers are close to append-only: a withdrawn preprint normally stays with a
withdrawal notice rather than disappearing. The sweep covers the configured window rather than
the whole archive, which is what makes it affordable, and that is the scope of the
forget-on-delete guarantee.

`max_rows_per_table=0` matters: cognee reads at most 50 rows per table by default, and
orphan-cleanup can only forget what it read back.

## Options

| argument | default | meaning |
| --- | --- | --- |
| `posted_since` | required | Earliest posting date, `YYYY-MM-DD` |
| `server` | `"biorxiv"` | `"biorxiv"` or `"medrxiv"` |
| `posted_until` | today | Latest posting date, `YYYY-MM-DD` |
| `category` | all | Subject category, matched case-insensitively, client-side |
| `detect_deletions` | `True` | Walk the whole window so departed preprints are forgotten |
| `include_node_sets` | `True` | Group by category, server and publication state |
| `user_agent` | cognee UA | Override the User-Agent sent to the API |

Category narrowing happens client-side because the details endpoint takes no category
parameter.

## Example

`examples/example.py` runs two syncs so you can watch the incremental path, and needs no
credentials:

```bash
uv run python examples/example.py
```

## Tests

The API is fully mocked, so no network access is needed:

```bash
uv run pytest tests/ -v
```

The fake models the parts of the contract the connector depends on, each checked against the
live API first: the cursor as a path segment advancing by the returned page size, the
`"no posts found"` status once the cursor passes the end, one entry per version, and
`published` holding the string `"NA"` rather than null.
