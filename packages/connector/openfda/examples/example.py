"""openFDA connector demo: ask the drug labels.

Pull FDA drug labels (or drug recalls) into cognee memory, incrementally, with
forget-on-delete.

This example is built on cognee's DLT ingestion subsystem: ``openfda_source``
returns a ``dlt`` resource that you hand straight to ``cognee.remember``. The
first run backfills the scope; re-running ``remember`` emits only records whose
label version has moved since, so a new revision replaces its predecessor
instead of piling up next to it.

Note openFDA's own disclaimer, which the connector carries into every document:
the data is unvalidated and must not be relied on for decisions about medical
care.

────────────────────────────────────────────────────────────────────────────
Setup
────────────────────────────────────────────────────────────────────────────
1. Install the connector:

       uv pip install cognee-community-connector-openfda

2. That is it for the API. An API key is optional and only raises rate limits,
   so no account is needed.

3. Set your LLM key (``LLM_API_KEY``) in ``.env`` like any other cognee
   example, so the graph can be built and questions answered.

Optionally narrow the scope before running:

       export OPENFDA_GENERIC="metformin"
       export OPENFDA_SINCE="20250101"
       export OPENFDA_DATASET="label"      # or "enforcement"
       export OPENFDA_API_KEY="..."        # optional

Run it:

    uv run python examples/example.py
"""

import asyncio
import os

import cognee

from cognee_community_connector_openfda import openfda_source

# Keep the labels in their own dataset so they are easy to inspect and forget.
DATASET_NAME = "openfda"

# A deliberately narrow default scope, so the demo finishes quickly.
DEFAULT_GENERIC = "metformin"
DEFAULT_SINCE = "20250101"

# Routing kwargs shared by every remember() call below. ``max_rows_per_table=0``
# disables cognee's per-table read cap so orphan-cleanup (forget-on-delete)
# compares against the *entire* synced corpus, not a 50-row window.
OPENFDA_REMEMBER_KWARGS = {
    "primary_key": "id",
    "write_disposition": "merge",
    "max_rows_per_table": 0,
}


async def main():
    dataset = os.environ.get("OPENFDA_DATASET", "label")
    generic_name = os.environ.get("OPENFDA_GENERIC", DEFAULT_GENERIC)
    effective_since = os.environ.get("OPENFDA_SINCE", DEFAULT_SINCE)
    api_key = os.environ.get("OPENFDA_API_KEY")

    # Start from a clean slate so the demo is reproducible.
    await cognee.prune.prune_data()
    await cognee.prune.prune_system(metadata=True)

    def build_source():
        return openfda_source(
            dataset=dataset,
            generic_name=generic_name,
            effective_since=effective_since,
            api_key=api_key,
        )

    # ── First sync: backfill the scope ─────────────────────────────────────
    print(f"\n=== openFDA sync #1 ({dataset}: {generic_name!r} since {effective_since}) ===")
    result = await cognee.remember(
        build_source(), dataset_name=DATASET_NAME, **OPENFDA_REMEMBER_KWARGS
    )
    print(result)

    answer = await cognee.search(
        query_text="What is this drug indicated for, and who should not take it?",
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        datasets=[DATASET_NAME],
    )
    print("Indications and contraindications:", answer)

    answer = await cognee.search(
        query_text="What are the most serious warnings and adverse reactions?",
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        datasets=[DATASET_NAME],
    )
    print("Warnings:", answer)

    answer = await cognee.search(
        query_text="Which manufacturers produce it, and do their labels differ?",
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        datasets=[DATASET_NAME],
    )
    print("Manufacturers:", answer)

    # ── Second sync: incremental delta + forget-on-delete ──────────────────
    # Re-running with the SAME dataset reuses the persisted state: products
    # whose label version has not moved are skipped, a new revision replaces
    # the old label, and anything that left the scope is removed from memory by
    # orphan_cleanup.
    print("\n=== openFDA sync #2 (incremental) ===")
    result = await cognee.remember(
        build_source(), dataset_name=DATASET_NAME, **OPENFDA_REMEMBER_KWARGS
    )
    print(result)
    print(
        "\nNo label versions moved in between, so sync #2 emitted nothing even "
        "though it re-read the window to check for deletions."
    )


if __name__ == "__main__":
    asyncio.run(main())
