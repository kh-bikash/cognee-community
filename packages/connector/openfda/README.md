# cognee-community-connector-openfda

An openFDA data-source connector for [cognee](https://github.com/topoteretes/cognee):
sync FDA drug labels and drug recalls into memory.

A label says what a drug was approved for, what it must warn about and what goes wrong in
practice. A recall says what went wrong with a specific lot and why. The
`warnings_and_cautions`, `contraindications`, `adverse_reactions` and `boxed_warning` sections
are long prose that a team is otherwise expected to read by hand.

It exposes a `dlt` resource you hand to `cognee.remember(...)`, reusing cognee's existing DLT
ingestion path (`resolve_dlt_sources` → `ingest_dlt_source` → `orphan_cleanup`) so you get
**incremental re-sync** (upsert by `set_id`, `merge` write disposition, a date cursor) and
**forget-on-delete** (records that leave the synced scope are emitted as hard-deletes and
purged from memory on the next sync) with no core changes.

**No credentials.** An API key is optional and only raises rate limits.

> **openFDA's own disclaimer applies.** The API attaches it to every response: the data is
> unvalidated and must not be relied on for decisions about medical care. The connector carries
> that disclaimer into each document rather than dropping it, because an agent answering from
> drug text should not lose the caveat a reader needs.

## Install

```bash
uv pip install cognee-community-connector-openfda
# or, from this monorepo:
cd packages/connector/openfda && uv sync --all-extras
```

## Usage

```python
import cognee
from cognee_community_connector_openfda import openfda_source

await cognee.remember(
    openfda_source(
        dataset="label",  # or "enforcement" for recalls
        generic_name="metformin",
        effective_since="20250101",
    ),
    dataset_name="drug_labels",
    primary_key="id",
    write_disposition="merge",  # incremental upsert by set id
    max_rows_per_table=0,  # unlimited: orphan-cleanup sees the whole corpus
)

answer = await cognee.search(
    query_text="What are the contraindications and serious warnings for this drug?",
    query_type=cognee.SearchType.GRAPH_COMPLETION,
    datasets=["drug_labels"],
)
```

A scope is required. The label endpoint alone holds hundreds of thousands of records, so
`openfda_source()` with no `brand_name`, `generic_name`, `manufacturer` or `search` raises.

## Datasets

| `dataset` | endpoint | identity | cursor |
| --- | --- | --- | --- |
| `"label"` | `/drug/label.json` | `set_id` | `effective_time` |
| `"enforcement"` | `/drug/enforcement.json` | `recall_number` | `report_date` |

### Labels are versioned, so the key is `set_id`

Every label revision gets a fresh `id`, while `set_id` stays with the product across revisions
and a `version` counter increments. A live label was at version 25.

Keying on `id` would add a row per revision, so memory would slowly fill with superseded labels
that all look current. Keying on `set_id` and keeping the highest version means one row per
product, replaced when a new revision is published. State tracks `set_id@version`, so a product
whose label has not been revised is skipped without re-rendering.

Enforcement records are not versioned, so their `recall_number` is used directly.

## What gets ingested

One document per record, as a header of identity facts followed by the prose sections.

```
Set ID: 000dc81d-ab91-450c-8eae-8eb74e72296f
Label version: 25
Brand name: ADVIL
Generic name: IBUPROFEN
Manufacturer: Pfizer
Pharmacologic class: Nonsteroidal Anti-inflammatory Drug [EPC]
Effective: 2026-07-06

Indications and usage:
...

Warnings and cautions:
...

openFDA disclaimer:
Do not rely on openFDA to make decisions regarding medical care. ...
```

**Labels keep** the prose sections: boxed warning, indications, dosage, contraindications,
warnings, adverse reactions, interactions, use in specific populations, pregnancy, paediatric
and geriatric use, overdosage, description, clinical pharmacology, mechanism of action,
pharmacokinetics, nonclinical toxicology, clinical studies, how supplied, storage.

**Labels drop** the `*_table` variants. Around a third of the 42 label fields are tables holding
HTML that duplicates the prose field beside them, and that markup would only add noise to
entity extraction.

**Recalls keep** the reason for recall, product description, affected lots and distribution
pattern, plus classification, status and recalling firm. The firm's street address and postal
code are contact data, so only its city, state and country are kept, and `"N/A"` placeholders
are not rendered as facts.

Nearly every openFDA text field is a single-element list, so values are flattened: without
that, documents would contain `['For the relief of pain.']`.

### Node sets

Labels are grouped by generic name, manufacturer and pharmacologic class; recalls by firm,
classification and status. Pass `include_node_sets=False` to skip it.

## The pagination ceiling

`limit` caps at 1000 and `skip` is rejected above 25000, so a scope holding more than 25000
records cannot be walked by paging alone. The documented escape hatch does not work in
practice: `search_after` answers `search_after has 1 value(s) but sort has 2`, and neither a
comma-joined pair nor repeated parameters are accepted.

So a window reporting more than the ceiling is **split in half** and each half walked
separately, which stays entirely within documented parameters. A single day that still exceeds
the ceiling cannot be split further; that is logged as a warning naming the window rather than
silently returning a truncated slice of it.

## Incremental sync and forget-on-delete

openFDA has no field projection, so the walk that would detect deletions costs the same as the
walk that fetches content. Doing both separately would double every run, so the two modes
differ:

- **`detect_deletions=True`** (default) walks the whole configured window once and uses it for
  both purposes: rows for ids whose version is new to the corpus, tombstones for ids that have
  left the window.
- **`detect_deletions=False`** walks only from the stored cursor forward. Cheaper, but it
  cannot see a deletion.

- **The cursor day is re-read on purpose.** The search range is inclusive and the cursor has day
  granularity, so excluding the cursor date would permanently drop a record dated later on the
  same day as the previous run. Unchanged records are skipped by the identity check, so the
  re-read costs bandwidth rather than work.
- **A search with no matches returns 404,** not an empty result set, so that is read as "no
  matches" rather than surfacing as an error.
- **Safety.** If the window returns zero records while ids were previously known, the run skips
  deletion and preserves state. A transient failure must not be read as "the scope emptied" and
  purge your dataset.

`max_rows_per_table=0` matters: cognee reads at most 50 rows per table by default, and
orphan-cleanup can only forget what it read back.

## Options

| argument | default | meaning |
| --- | --- | --- |
| `dataset` | `"label"` | `"label"` or `"enforcement"` |
| `brand_name` | none | Brand name to match, e.g. `"ADVIL"` |
| `generic_name` | none | Generic name to match, e.g. `"metformin"` |
| `manufacturer` | none | Manufacturer name to match |
| `search` | none | Raw openFDA search expression |
| `effective_since` | `"20200101"` | Earliest cursor date, `YYYYMMDD` |
| `effective_until` | today | Latest cursor date, `YYYYMMDD` |
| `page_size` | `100` | Records per request, capped at 1000 |
| `detect_deletions` | `True` | Walk the whole window so departed records are forgotten |
| `include_node_sets` | `True` | Group records into node sets |
| `api_key` | none | Optional openFDA key; raises rate limits only |
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
live API first: `search` with an inclusive `field:[A TO B]` range, `limit`/`skip` paging, the
rejection of `skip` above 25000, `meta.results.total`, the per-response `meta.disclaimer`,
single-element list fields, and the separate identity of `id` and `set_id` on a versioned
label.
