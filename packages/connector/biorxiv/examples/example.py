"""bioRxiv / medRxiv connector demo: ask the preprint servers.

Pull preprints into cognee memory, incrementally, with forget-on-delete.

This example is built on cognee's DLT ingestion subsystem: ``biorxiv_source``
returns a ``dlt`` resource that you hand straight to ``cognee.remember``. The
first run backfills the window; re-running ``remember`` emits only preprints
whose version has moved since, so revisions replace their earlier text instead
of piling up next to it.

────────────────────────────────────────────────────────────────────────────
Setup
────────────────────────────────────────────────────────────────────────────
1. Install the connector:

       uv pip install cognee-community-connector-biorxiv

2. That is it for the API. It is public, so there is no account, no token and
   no approval step.

3. Set your LLM key (``LLM_API_KEY``) in ``.env`` like any other cognee
   example, so the graph can be built and questions answered.

Optionally narrow the scope before running:

       export BIORXIV_SERVER="medrxiv"
       export BIORXIV_SINCE="2026-10-01"
       export BIORXIV_CATEGORY="health informatics"

Run it:

    uv run python examples/example.py
"""

import asyncio
import os
from datetime import date, timedelta

import cognee

from cognee_community_connector_biorxiv import biorxiv_source

# Keep the preprints in their own dataset so they are easy to inspect and forget.
DATASET_NAME = "preprints"

# A deliberately small default window, so the demo finishes quickly. A single
# day of bioRxiv is a few hundred entries.
DEFAULT_DAYS_BACK = 1

# Routing kwargs shared by every remember() call below. ``max_rows_per_table=0``
# disables cognee's per-table read cap so orphan-cleanup (forget-on-delete)
# compares against the *entire* synced corpus, not a 50-row window.
BIORXIV_REMEMBER_KWARGS = {
    "primary_key": "id",
    "write_disposition": "merge",
    "max_rows_per_table": 0,
}


async def main():
    server = os.environ.get("BIORXIV_SERVER", "biorxiv")
    posted_since = os.environ.get("BIORXIV_SINCE") or str(
        date.today() - timedelta(days=DEFAULT_DAYS_BACK)
    )
    category = os.environ.get("BIORXIV_CATEGORY")

    # Start from a clean slate so the demo is reproducible.
    await cognee.prune.prune_data()
    await cognee.prune.prune_system(metadata=True)

    def build_source():
        return biorxiv_source(
            server=server,
            posted_since=posted_since,
            category=category,
        )

    # ── First sync: backfill the window ────────────────────────────────────
    print(f"\n=== {server} sync #1 (posted since {posted_since}) ===")
    result = await cognee.remember(
        build_source(), dataset_name=DATASET_NAME, **BIORXIV_REMEMBER_KWARGS
    )
    print(result)

    answer = await cognee.search(
        query_text="What are the main findings in these preprints?",
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        datasets=[DATASET_NAME],
    )
    print("Findings:", answer)

    answer = await cognee.search(
        query_text="Which institutions are the corresponding authors from?",
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        datasets=[DATASET_NAME],
    )
    print("Institutions:", answer)

    answer = await cognee.search(
        query_text="Which of these preprints have already been published in a journal?",
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        datasets=[DATASET_NAME],
    )
    print("Published:", answer)

    # ── Second sync: incremental delta + forget-on-delete ──────────────────
    # Re-running with the SAME dataset reuses the persisted state: preprints
    # whose version has not moved are skipped, a new version replaces its
    # earlier text, and anything that left the window is removed from memory by
    # orphan_cleanup.
    print(f"\n=== {server} sync #2 (incremental) ===")
    result = await cognee.remember(
        build_source(), dataset_name=DATASET_NAME, **BIORXIV_REMEMBER_KWARGS
    )
    print(result)
    print(
        "\nNo versions moved in between, so sync #2 emitted nothing even though "
        "it re-read the window to check for deletions."
    )


if __name__ == "__main__":
    asyncio.run(main())
