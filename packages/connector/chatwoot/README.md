# cognee-community-connector-chatwoot

A Chatwoot data-source connector for [cognee](https://github.com/topoteretes/cognee):
sync your support inbox into memory, so you can "ask my support inbox".

Resolved conversations are where a lot of undocumented knowledge lives: the workaround an
agent explained once, the reason a customer churned, the bug that turned out to be a config
mistake. This connector turns each conversation into a document cognee can reason over.

It exposes a `dlt` resource you hand to `cognee.remember(...)`, reusing cognee's existing DLT
ingestion path (`resolve_dlt_sources` → `ingest_dlt_source` → `orphan_cleanup`) so you get
**incremental re-sync** (upsert by conversation id, `merge` write disposition,
`last_activity_at` cursor) and **forget-on-delete** (conversations deleted in Chatwoot are
emitted as hard-deletes and purged from memory on the next sync) with no core changes.

## Install

```bash
uv pip install cognee-community-connector-chatwoot
# or, from this monorepo:
cd packages/connector/chatwoot && uv sync --all-extras
```

## Setup

1. **API token:** Chatwoot → *Profile Settings* → *Access Token*. The connector only issues
   `GET` requests, so a read-only agent account is enough.
2. **Account id:** the number in your Chatwoot URL: `/app/accounts/<account_id>/dashboard`.
3. **Base URL:** `https://app.chatwoot.com` for Chatwoot Cloud, or your self-hosted origin.

```bash
export CHATWOOT_URL="https://app.chatwoot.com"
export CHATWOOT_ACCOUNT_ID="1"
export CHATWOOT_API_TOKEN="…"
```

On Chatwoot older than 4.19.0 the `Authorization: Bearer` scheme does not exist yet, so pass
`legacy_auth_header=True` to send the token as `api_access_token` instead.

## Usage

```python
import cognee
from cognee_community_connector_chatwoot import chatwoot_source

await cognee.remember(
    chatwoot_source(
        base_url="https://app.chatwoot.com",
        account_id=1,
        api_token="...",
        labels=["billing"],  # optional: only billing conversations
    ),
    dataset_name="support_inbox",
    primary_key="id",
    write_disposition="merge",  # incremental upsert by conversation id
    max_rows_per_table=0,  # unlimited: orphan-cleanup sees the whole corpus
)

answer = await cognee.search(
    query_text="Why do customers ask for refunds on duplicate charges?",
    query_type=cognee.SearchType.GRAPH_COMPLETION,
    datasets=["support_inbox"],
)
```

## What gets ingested

One document per **conversation**, not per message. A single support message is rarely
meaningful on its own ("yes, that worked"); the resolution only exists in the thread. Each
document is a header block (status, inbox, labels, contact, timestamps) followed by a
speaker-labelled transcript:

```
Status: resolved
Inbox: Billing
Labels: billing, refund
Contact: Jane Doe <jane@example.com>
Opened: 2026-10-01T09:14:00Z
Last activity: 2026-10-01T09:41:00Z

Transcript:
Customer (Jane Doe): My invoice shows the same charge twice.
Agent (Sam): That's the webhook retry creating a second charge. Refunded it.
Internal note (Sam): Root cause is the retry not checking idempotency keys.
```

Message filtering is deliberate, because a support inbox carries a lot of machine
chatter:

- **Kept:** incoming (customer) and outgoing (agent) messages.
- **Dropped:** activity messages ("Conversation was marked resolved") and templates/canned
  replies. They are status noise, not knowledge.
- **Internal notes** (`private: true`) are kept by default and labelled as internal, because
  that is often where the real diagnosis is written. Pass `include_private_notes=False` to
  leave them out.
- **Attachments** are noted inline (`[attachment: image …]`) but never downloaded.

> **Note on `status`**
> `GET /conversations` defaults to `status=open` in Chatwoot's API. A connector that omits the
> parameter silently ingests only unresolved conversations and misses every resolved one, the
> exact subset with the most knowledge in it. This connector always sends `status` explicitly
> and defaults it to `all`.

### Node sets

Conversations are grouped into cognee node sets by inbox (`inbox:Billing`) and by label
(`label:refund`), so you can ask about one inbox without creating a dataset per inbox. Pass
`include_node_sets=False` to skip it. cognee namespaces these names under the source tag, so
they cannot collide with cognee's own node sets.

## Incremental sync and forget-on-delete

- **Cursor:** `last_activity_at`, which Chatwoot bumps on every new message. It is persisted
  in dlt's resource state, so re-running `remember` with the same dataset only re-renders
  conversations that moved. A conversation that is new to the corpus is ingested even if it is
  older than the cursor, so widening a label filter or being granted a new inbox does not
  silently skip history.
- **Deletions:** Chatwoot has no deletion feed, so each run compares the ids it just listed
  against the ids it saw last run. Conversations that vanished are emitted with `_deleted=True`,
  dlt removes them on `merge`, and `orphan_cleanup` purges them from the graph, vector and
  relational stores.
- **Safety:** if the sweep returns zero conversations while ids were previously known, the run
  skips deletion and preserves state. A transient listing failure must not be read as "the
  inbox was wiped" and purge your dataset.

`max_rows_per_table=0` matters: cognee reads at most 50 rows per table by default, and
orphan-cleanup can only forget what it read back.

## Options

| argument | default | meaning |
| --- | --- | --- |
| `base_url` | required | Instance origin, e.g. `https://app.chatwoot.com` |
| `account_id` | required | Account id from the Chatwoot URL |
| `api_token` | `$CHATWOOT_API_TOKEN` | User API token |
| `status` | `"all"` | `all`, `open`, `resolved`, `pending`, `snoozed` |
| `inbox_ids` | all readable | Restrict to these inbox ids |
| `labels` | all | Restrict to conversations with these labels |
| `include_private_notes` | `True` | Ingest internal agent notes |
| `include_node_sets` | `True` | Group by inbox and label |
| `legacy_auth_header` | `False` | Use `api_access_token` (Chatwoot < 4.19.0) |

## Example

`examples/example.py` runs two syncs against a live instance so you can watch the incremental
path work:

```bash
uv run python examples/example.py
```

## Tests

The Chatwoot API is fully mocked, so no credentials or network access are needed:

```bash
uv run pytest tests/ -v
```
