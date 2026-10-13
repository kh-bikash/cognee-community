# cognee-community-connector-clinicaltrials

A ClinicalTrials.gov data-source connector for [cognee](https://github.com/topoteretes/cognee):
sync the public trial registry into memory, so you can ask the registry questions.

A paper tells you what was published. The trial registry tells you what is being attempted
right now, by whom, on which population, with what endpoints, and whether it stopped early.
Most of that is long free text rather than metadata.

It exposes a `dlt` resource you hand to `cognee.remember(...)`, reusing cognee's existing DLT
ingestion path (`resolve_dlt_sources` → `ingest_dlt_source` → `orphan_cleanup`) so you get
**incremental re-sync** (upsert by NCT id, `merge` write disposition, a `LastUpdatePostDate`
cursor pushed down to the API) and **forget-on-delete** (studies that leave the synced scope
are emitted as hard-deletes and purged from memory on the next sync) with no core changes.

**No credentials.** API v2 is public: no account, no token, no approval. You can run the
example below right now.

## Install

```bash
uv pip install cognee-community-connector-clinicaltrials
# or, from this monorepo:
cd packages/connector/clinicaltrials && uv sync --all-extras
```

## Usage

```python
import cognee
from cognee_community_connector_clinicaltrials import clinicaltrials_source

await cognee.remember(
    clinicaltrials_source(
        condition="type 2 diabetes",
        statuses=["RECRUITING"],
    ),
    dataset_name="trials",
    primary_key="id",
    write_disposition="merge",  # incremental upsert by NCT id
    max_rows_per_table=0,  # unlimited: orphan-cleanup sees the whole corpus
)

answer = await cognee.search(
    query_text="Which sponsors are running phase 3 trials, and what are their endpoints?",
    query_type=cognee.SearchType.GRAPH_COMPLETION,
    datasets=["trials"],
)
```

A scope is required. Syncing the whole registry (around half a million studies) is never what
a caller means, and an accidental full sync is expensive for both sides, so
`clinicaltrials_source()` with no scope raises rather than doing it.

## What gets ingested

One document per **study**. The free text is spread across several modules and none of them
stands alone: eligibility criteria mean little without knowing the intervention they gate. They
are rendered into one document with section headings.

```
NCT ID: NCT04512963
Status: COMPLETED
Phase: PHASE1
Study type: INTERVENTIONAL
Enrollment: 64 (ACTUAL)
Lead sponsor: PRG Science & Technology Co., Ltd.
Conditions: Hutchinson-Gilford Progeria Syndrome, Werner Syndrome
Start date: 2020-08-24
Last update posted: 2021-09-22

Brief summary:
...

Detailed description:
...

Interventions:
- Progerinin (DRUG): single and multiple ascending doses

Outcome measures:
- Primary: Safety and tolerability Time frame: 28 days

Eligibility:
Participants (sex: ALL, minimum age: 19 Years, healthy volunteers: True)
Inclusion criteria: ...
```

**Kept:** titles, conditions, keywords, interventions, brief summary, detailed description,
eligibility criteria and outcome measures, plus the structured facts in the header.

**Skipped:** purely administrative and structural fields (ID aliases, oversight flags,
IPD-sharing statements, contact phone numbers) and the `derivedSection` browse-condition tree,
which is generated taxonomy rather than anything a person wrote. Locations collapse to a site
count and country list, since the full list is addresses and phone numbers.

The registry's free text carries Markdown-style backslash escapes (`\&`, `\<`, `\[`). A survey
of 214 live description fields found them only ever in front of punctuation, never as
meaningful backslashes, so they are removed. Left in place they corrupt the entities cognify
extracts, turning one organization name into text with a stray backslash in it.

### Node sets

Studies are grouped into cognee node sets by condition, lead sponsor and overall status
(`condition:Werner Syndrome`, `sponsor:Acme Pharma`, `status:RECRUITING`), so you can ask about
one therapeutic area without creating a dataset per condition. Pass
`include_node_sets=False` to skip it.

## Incremental sync and forget-on-delete

- **Cursor.** `LastUpdatePostDate`, pushed down to the API as
  `filter.advanced=AREA[LastUpdatePostDate]RANGE[<cursor>,MAX]` and persisted in dlt's resource
  state. The other connectors in this repo have to list everything and compare timestamps
  locally; here the delta is computed server-side, so a run where nothing changed is one small
  request plus the id sweep.
- **The cursor day is re-read on purpose.** The range filter is inclusive and the cursor has
  day granularity, so each run re-fetches the studies posted on the cursor date. Excluding them
  would permanently drop a study updated later on the same day as the previous run. The
  re-fetched rows merge onto themselves by NCT id, so the cost is a little bandwidth.
- **Deletions.** The registry has no deletion feed, so each run sweeps the ids in scope
  (`fields=NCTId`, up to 1000 per request) and compares them against the previous run's ids.
  Studies that vanished are emitted with `_deleted=True`, dlt removes them on `merge`, and
  `orphan_cleanup` purges them from the graph, vector and relational stores.
- **Studies new to the corpus are caught.** A study last updated before the cursor cannot come
  back from the date filter. The sweep notices it is unknown and fetches it by id, so widening
  a scope does not silently skip history.
- **Safety.** If the sweep returns zero studies while ids were previously known, the run skips
  deletion and preserves state. A transient listing failure must not be read as "the scope
  emptied" and purge your dataset.

### What "deleted" means here

The registry rarely removes records. A stopped trial usually becomes `WITHDRAWN` or
`TERMINATED`, which is a status change, not a deletion, and stays in memory: "this trial was
terminated" is exactly the kind of fact worth remembering. What drives a tombstone is a study
leaving the synced scope, either withdrawn from the registry or no longer matching your
condition, sponsor or status filter.

`max_rows_per_table=0` matters: cognee reads at most 50 rows per table by default, and
orphan-cleanup can only forget what it read back.

## Options

| argument | default | meaning |
| --- | --- | --- |
| `condition` | none | Condition to search, e.g. `"type 2 diabetes"`. Accepts the registry's syntax, so `"diabetes OR obesity"` works. A relevance search, not an exact match. |
| `sponsor` | none | Sponsor to search. Also a relevance search, so near matches come back too. |
| `statuses` | all | Exact `overallStatus` values, e.g. `["RECRUITING", "COMPLETED"]`. |
| `query_term` | none | Raw query term, for scopes the options above cannot express. |
| `page_size` | `100` | Studies per request, capped at 1000 by the API. |
| `detect_deletions` | `True` | Sweep scope ids each run so departed studies are forgotten. |
| `include_node_sets` | `True` | Group by condition, sponsor and status. |
| `user_agent` | cognee UA | Override the User-Agent sent to the registry. |

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
live API first: opaque `nextPageToken` paging, the inclusive
`AREA[LastUpdatePostDate]RANGE[...]` filter, `filter.ids` lookups, and the `fields=NCTId`
projection.
