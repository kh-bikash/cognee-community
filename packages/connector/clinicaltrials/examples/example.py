"""ClinicalTrials.gov connector demo: ask the trial registry.

Pull studies from the public ClinicalTrials.gov API v2 into cognee memory,
incrementally, with forget-on-delete.

This example is built on cognee's DLT ingestion subsystem:
``clinicaltrials_source`` returns a ``dlt`` resource that you hand straight to
``cognee.remember``. The first run backfills the scope; re-running ``remember``
syncs only studies whose ``LastUpdatePostDate`` has moved since, and studies
that leave the scope are forgotten from memory on the next sync.

────────────────────────────────────────────────────────────────────────────
Setup
────────────────────────────────────────────────────────────────────────────
1. Install the connector:

       uv pip install cognee-community-connector-clinicaltrials

2. That is it for the registry. API v2 is public, so there is no account, no
   token and no approval step.

3. Set your LLM key (``LLM_API_KEY``) in ``.env`` like any other cognee
   example, so the graph can be built and questions answered.

Optionally narrow the scope before running:

       export CTGOV_CONDITION="progeria"

Run it:

    uv run python examples/example.py
"""

import asyncio
import os

import cognee

from cognee_community_connector_clinicaltrials import clinicaltrials_source

# Keep the registry slice in its own dataset so it is easy to inspect and forget.
DATASET_NAME = "clinical_trials"

# A deliberately small scope, so the demo finishes quickly. "progeria" is about
# a dozen studies; "type 2 diabetes" is tens of thousands.
DEFAULT_CONDITION = "progeria"

# Routing kwargs shared by every remember() call below. ``max_rows_per_table=0``
# disables cognee's per-table read cap so orphan-cleanup (forget-on-delete)
# compares against the *entire* synced corpus, not a 50-row window.
CTGOV_REMEMBER_KWARGS = {
    "primary_key": "id",
    "write_disposition": "merge",
    "max_rows_per_table": 0,
}


async def main():
    condition = os.environ.get("CTGOV_CONDITION", DEFAULT_CONDITION)

    # Start from a clean slate so the demo is reproducible.
    await cognee.prune.prune_data()
    await cognee.prune.prune_system(metadata=True)

    def build_source():
        return clinicaltrials_source(condition=condition)

    # ── First sync: full backfill of the scope ─────────────────────────────
    print(f"\n=== ClinicalTrials.gov sync #1 (backfill: {condition!r}) ===")
    result = await cognee.remember(
        build_source(), dataset_name=DATASET_NAME, **CTGOV_REMEMBER_KWARGS
    )
    print(result)

    answer = await cognee.search(
        query_text=f"What is being investigated in {condition} trials?",
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        datasets=[DATASET_NAME],
    )
    print("Investigations:", answer)

    answer = await cognee.search(
        query_text="Which sponsors are involved, and which interventions are they testing?",
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        datasets=[DATASET_NAME],
    )
    print("Sponsors and interventions:", answer)

    answer = await cognee.search(
        query_text="Which of these studies were completed, and what did they measure?",
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        datasets=[DATASET_NAME],
    )
    print("Completed studies:", answer)

    # ── Second sync: incremental delta + forget-on-delete ──────────────────
    # Re-running with the SAME dataset reuses the persisted cursor: the API is
    # asked only for studies updated since sync #1, and anything that left the
    # scope is removed from memory by orphan_cleanup.
    print("\n=== ClinicalTrials.gov sync #2 (incremental) ===")
    result = await cognee.remember(
        build_source(), dataset_name=DATASET_NAME, **CTGOV_REMEMBER_KWARGS
    )
    print(result)
    print(
        "\nNothing changed upstream, so sync #2 re-read only the cursor day "
        "and swept the scope's ids."
    )


if __name__ == "__main__":
    asyncio.run(main())
