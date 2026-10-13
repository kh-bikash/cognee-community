# cognee-community-connector-federalregister

A Federal Register data-source connector for [cognee](https://github.com/topoteretes/cognee):
sync US regulatory documents into memory, so an agent can answer "what changed for us this
month, and which agency changed it?".

Every US rule, proposed rule and agency notice is published in the Federal Register, and for a
lot of companies that is the feed deciding what they have to do next quarter. Today that means
reading a daily list by hand.

It exposes a `dlt` resource you hand to `cognee.remember(...)`, reusing cognee's existing DLT
ingestion path (`resolve_dlt_sources` → `ingest_dlt_source` → `orphan_cleanup`) so you get
**incremental re-sync** (upsert by document number, `merge` write disposition, a
`publication_date` cursor pushed down to the API) and **forget-on-delete** (documents that
leave the synced scope are emitted as hard-deletes and purged from memory on the next sync)
with no core changes.

**No credentials.** The API is public: no account, no token, no approval.

## Install

```bash
uv pip install cognee-community-connector-federalregister
# or, from this monorepo:
cd packages/connector/federalregister && uv sync --all-extras
```

## Usage

```python
import cognee
from cognee_community_connector_federalregister import federalregister_source

await cognee.remember(
    federalregister_source(
        published_since="2026-10-01",
        agencies=["environmental-protection-agency"],
        document_types=["RULE", "PRORULE"],
    ),
    dataset_name="regulations",
    primary_key="id",
    write_disposition="merge",  # incremental upsert by document number
    max_rows_per_table=0,  # unlimited: orphan-cleanup sees the whole corpus
)

answer = await cognee.search(
    query_text="Which agencies tightened emissions requirements, and when do they take effect?",
    query_type=cognee.SearchType.GRAPH_COMPLETION,
    datasets=["regulations"],
)
```

`published_since` is required. The Federal Register goes back to 1994 and holds millions of
documents, so an unbounded sync is never what a caller means.

Agency values are the slugs that appear in agency URLs, such as
`environmental-protection-agency`. Document types are the filter codes `RULE`, `PRORULE`,
`NOTICE` and `PRESDOCU`. Note the asymmetry: the filter takes `RULE` while the returned `type`
field is the display name `Rule`.

## What gets ingested

One document per Federal Register document, keyed by `document_number`, which is the stable
citation id. Each is a header of structured facts followed by the agency's abstract:

```
Document number: 2026-20082
Type: Rule
Action: Final rule.
Agencies: State Department
Citation: 91 FR 62313
Published: 2026-10-01
Effective: 2026-10-01
Dockets: Public Notice: 13131

Abstract:
The Department of State is amending the International Traffic in Arms Regulations ...
```

### An honest limit on full text

The API returns an `abstract`, which is the agency's own summary and is the genuinely useful
text. The full rule body is only available as a separate text URL per document, so ingesting it
costs one extra request per document and brings a lot of procedural boilerplate (addresses,
docket procedure, signature blocks) along with the substance.

The default is therefore the abstract plus the structured facts. Pass `include_full_text=True`
to fetch bodies as well. A text URL that fails is logged and skipped rather than aborting the
run, so the document still lands with its abstract.

`abstract` is null for presidential documents. Those render from title, type and signing
president instead of being skipped.

### Node sets

Documents are grouped into cognee node sets by agency and type (`agency:State Department`,
`type:Proposed Rule`), so "what did EPA publish?" works without a dataset per agency. Pass
`include_node_sets=False` to skip it.

## Incremental sync and forget-on-delete

- **Cursor.** `publication_date`, pushed down as `conditions[publication_date][gte]` and
  persisted in dlt's resource state. The delta is computed server-side, so nothing has to list
  everything and compare timestamps locally.
- **The cursor day is re-read on purpose.** The `gte` condition is inclusive and the cursor has
  day granularity, so each run re-reads documents published on the cursor date. Excluding them
  would permanently drop a document published later on the same day as the previous run. The
  re-read rows merge onto themselves by document number.
- **Pagination follows `next_page_url`.** The response carries that link with a
  `search_after_cursor` already in it. Incrementing `page` alone runs into a server-side page
  ceiling on large result sets.
- **Deletions.** Each run sweeps the document numbers in scope with a `fields[]=document_number`
  projection and compares them against the previous run. Documents that vanished are emitted
  with `_deleted=True`, dlt removes them on `merge`, and `orphan_cleanup` purges them from the
  graph, vector and relational stores. The Federal Register is close to append-only, so in
  practice this catches documents leaving your configured scope rather than true deletions.
- **Documents new to the corpus are caught.** A document published before the cursor cannot
  come back from the date condition. The sweep notices it is unknown and fetches it from the
  per-document endpoint, so widening an agency list does not silently skip history.
- **Safety.** If the sweep returns zero documents while numbers were previously known, the run
  skips deletion and preserves state. A transient listing failure must not be read as "the
  scope emptied" and purge your dataset.

`max_rows_per_table=0` matters: cognee reads at most 50 rows per table by default, and
orphan-cleanup can only forget what it read back.

## Options

| argument | default | meaning |
| --- | --- | --- |
| `published_since` | required | Earliest publication date, `YYYY-MM-DD` |
| `agencies` | all | Agency slugs, e.g. `["environmental-protection-agency"]` |
| `document_types` | all | `RULE`, `PRORULE`, `NOTICE`, `PRESDOCU` |
| `term` | none | Full-text search term |
| `per_page` | `100` | Documents per request, capped at 1000 by the API |
| `detect_deletions` | `True` | Sweep scope ids so departed documents are forgotten |
| `include_node_sets` | `True` | Group by agency and type |
| `include_full_text` | `False` | Also fetch each document's full text |
| `user_agent` | cognee UA | Override the User-Agent sent to the API |

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
live API first: the `conditions[publication_date][gte]` condition, `next_page_url` cursor
pagination, the `fields[]` projection, the per-document endpoint, and the plain-text full-text
URL.
