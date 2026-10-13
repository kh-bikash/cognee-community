"""Chatwoot connector demo: "ask my support inbox".

Pull Chatwoot conversations into cognee memory, incrementally, with
forget-on-delete.

This example is built on cognee's DLT ingestion subsystem: ``chatwoot_source``
returns a ``dlt`` resource that you hand straight to ``cognee.remember``. The
first run backfills the inbox; re-running ``remember`` syncs only conversations
that have moved since (via ``last_activity_at``), and conversations you delete
in Chatwoot are forgotten from memory on the next sync.

────────────────────────────────────────────────────────────────────────────
One-time setup
────────────────────────────────────────────────────────────────────────────
1. Install the connector:

       uv pip install cognee-community-connector-chatwoot

2. Create an access token in Chatwoot under Profile Settings → Access Token.
   The connector only issues GET requests, so a read-only agent is enough.

3. Export your connection details. The account id is the number in your
   Chatwoot URL (``/app/accounts/<account_id>/dashboard``):

       export CHATWOOT_URL="https://app.chatwoot.com"
       export CHATWOOT_ACCOUNT_ID="1"
       export CHATWOOT_API_TOKEN="…"
       # optional: export CHATWOOT_LABELS="billing,refund"

4. Set your LLM key (``LLM_API_KEY``) in ``.env`` like any other cognee example.

Run it:

    uv run python examples/example.py
"""

import asyncio
import os

import cognee

from cognee_community_connector_chatwoot import chatwoot_source

# Keep the inbox in its own dataset so it is easy to inspect and forget.
DATASET_NAME = "chatwoot_support"

# Routing kwargs shared by every remember() call below. ``max_rows_per_table=0``
# disables cognee's per-table read cap so orphan-cleanup (forget-on-delete)
# compares against the *entire* synced corpus, not a 50-row window.
CHATWOOT_REMEMBER_KWARGS = {
    "primary_key": "id",
    "write_disposition": "merge",
    "max_rows_per_table": 0,
}


async def main():
    base_url = os.environ.get("CHATWOOT_URL")
    account_id = os.environ.get("CHATWOOT_ACCOUNT_ID")
    api_token = os.environ.get("CHATWOOT_API_TOKEN")
    labels = os.environ.get("CHATWOOT_LABELS")

    if not all([base_url, account_id, api_token]):
        print(
            "Set CHATWOOT_URL, CHATWOOT_ACCOUNT_ID and CHATWOOT_API_TOKEN.\n"
            "See the setup steps in this file's docstring, then re-run."
        )
        return

    # Start from a clean slate so the demo is reproducible.
    await cognee.prune.prune_data()
    await cognee.prune.prune_system(metadata=True)

    def build_source():
        return chatwoot_source(
            base_url=base_url,
            account_id=account_id,
            api_token=api_token,
            labels=labels.split(",") if labels else None,
        )

    # ── First sync: full backfill ──────────────────────────────────────────
    print("\n=== Chatwoot sync #1 (backfill) ===")
    result = await cognee.remember(
        build_source(), dataset_name=DATASET_NAME, **CHATWOOT_REMEMBER_KWARGS
    )
    print(result)

    answer = await cognee.search(
        query_text="What are customers most often contacting support about?",
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        datasets=[DATASET_NAME],
    )
    print("Themes:", answer)

    answer = await cognee.search(
        query_text="Which problems did we resolve, and how?",
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        datasets=[DATASET_NAME],
    )
    print("Resolutions:", answer)

    # ── Second sync: incremental delta + forget-on-delete ──────────────────
    # Re-running with the SAME dataset reuses the persisted cursor: only
    # conversations active since sync #1 are fetched, and anything deleted in
    # Chatwoot is removed from memory by orphan_cleanup.
    #
    # To watch it work: reply to one conversation and delete another in
    # Chatwoot, then run this file again.
    print("\n=== Chatwoot sync #2 (incremental) ===")
    result = await cognee.remember(
        build_source(), dataset_name=DATASET_NAME, **CHATWOOT_REMEMBER_KWARGS
    )
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
