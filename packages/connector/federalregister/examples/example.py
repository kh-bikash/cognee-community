"""Federal Register connector demo: ask the regulatory record.

Pull Federal Register documents into cognee memory, incrementally, with
forget-on-delete.

This example is built on cognee's DLT ingestion subsystem:
``federalregister_source`` returns a ``dlt`` resource that you hand straight to
``cognee.remember``. The first run backfills the window; re-running
``remember`` asks the API only for documents published since the last run.

────────────────────────────────────────────────────────────────────────────
Setup
────────────────────────────────────────────────────────────────────────────
1. Install the connector:

       uv pip install cognee-community-connector-federalregister

2. That is it for the API. It is public, so there is no account, no token and
   no approval step.

3. Set your LLM key (``LLM_API_KEY``) in ``.env`` like any other cognee
   example, so the graph can be built and questions answered.

Optionally narrow the scope before running:

       export FR_PUBLISHED_SINCE="2026-10-01"
       export FR_AGENCIES="environmental-protection-agency"

Run it:

    uv run python examples/example.py
"""

import asyncio
import os
from datetime import date, timedelta

import cognee

from cognee_community_connector_federalregister import federalregister_source

# Keep the regulatory slice in its own dataset so it is easy to inspect and forget.
DATASET_NAME = "federal_register"

# A deliberately small default window, so the demo finishes quickly.
DEFAULT_DAYS_BACK = 3

# Routing kwargs shared by every remember() call below. ``max_rows_per_table=0``
# disables cognee's per-table read cap so orphan-cleanup (forget-on-delete)
# compares against the *entire* synced corpus, not a 50-row window.
FR_REMEMBER_KWARGS = {
    "primary_key": "id",
    "write_disposition": "merge",
    "max_rows_per_table": 0,
}


async def main():
    published_since = os.environ.get("FR_PUBLISHED_SINCE") or str(
        date.today() - timedelta(days=DEFAULT_DAYS_BACK)
    )
    agencies = os.environ.get("FR_AGENCIES")
    document_types = ["RULE", "PRORULE"]

    # Start from a clean slate so the demo is reproducible.
    await cognee.prune.prune_data()
    await cognee.prune.prune_system(metadata=True)

    def build_source():
        return federalregister_source(
            published_since=published_since,
            agencies=agencies.split(",") if agencies else None,
            document_types=document_types,
        )

    # ── First sync: backfill the window ────────────────────────────────────
    print(f"\n=== Federal Register sync #1 (rules published since {published_since}) ===")
    result = await cognee.remember(build_source(), dataset_name=DATASET_NAME, **FR_REMEMBER_KWARGS)
    print(result)

    answer = await cognee.search(
        query_text="Which agencies published rules, and what do the rules change?",
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        datasets=[DATASET_NAME],
    )
    print("Agencies and changes:", answer)

    answer = await cognee.search(
        query_text="Are there any proposed rules with a comment deadline I should know about?",
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        datasets=[DATASET_NAME],
    )
    print("Comment deadlines:", answer)

    # ── Second sync: incremental delta + forget-on-delete ──────────────────
    # Re-running with the SAME dataset reuses the persisted cursor: the API is
    # asked only for documents published since sync #1, and anything that left
    # the scope is removed from memory by orphan_cleanup.
    print("\n=== Federal Register sync #2 (incremental) ===")
    result = await cognee.remember(build_source(), dataset_name=DATASET_NAME, **FR_REMEMBER_KWARGS)
    print(result)
    print(
        "\nNothing new was published in between, so sync #2 re-read only the "
        "cursor day and swept the scope's document numbers."
    )


if __name__ == "__main__":
    asyncio.run(main())
