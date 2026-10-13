"""Unit tests for the ClinicalTrials.gov connector.

The API v2 is fully mocked via ``FakeCTSession``: no ``requests`` traffic and no
network access are required, so these run in CI. The fake models the parts of
the contract the connector depends on, each of which was checked against the
live API first: opaque ``nextPageToken`` paging, the inclusive
``AREA[LastUpdatePostDate]RANGE[...]`` filter, ``filter.ids`` lookups, and the
``fields=NCTId`` projection.

Coverage:

  - scope options map onto their API parameters
  - a scope is required, so the registry is never synced whole by accident
  - token pagination walks pages, stops without a token, and stops on a repeat
  - the first run sends no date filter; later runs push the cursor server-side
  - the cursor day is re-fetched (inclusive range), so same-day edits are kept
  - a run with no changes emits nothing
  - studies that left the scope become hard-delete markers
  - an empty sweep does not mass-delete, and preserves state
  - a study new to the corpus below the cursor is backfilled by id
  - detect_deletions=False skips the sweep entirely
  - the text modules render, and derivedSection is ignored
  - node sets come from conditions, sponsor and status, deterministically
  - 429 is retried honouring Retry-After; 400 propagates; non-JSON raises
  - the dlt resource is wired with merge + id PK + hard_delete + document mode
  - a real dlt merge removes the marked row (end-to-end forget-on-delete)
"""

import re

import pytest

from cognee_community_connector_clinicaltrials.clinicaltrials import (
    _content,
    _node_sets,
    _scope_params,
    clinicaltrials_source,
    sync_studies,
)


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------
def _study(
    nct_id,
    *,
    last_update="2026-10-01",
    status="RECRUITING",
    title="A study of something",
    conditions=None,
    sponsor="Acme Pharma",
    brief="Brief summary text.",
    detailed=None,
    interventions=None,
    outcomes=None,
    criteria=None,
    locations=None,
    phases=None,
):
    """Build a study in the shape API v2 returns."""
    protocol = {
        "identificationModule": {"nctId": nct_id, "briefTitle": title},
        "statusModule": {
            "overallStatus": status,
            "lastUpdatePostDateStruct": {"date": last_update, "type": "ACTUAL"},
            "startDateStruct": {"date": "2025-01-15"},
        },
        "sponsorCollaboratorsModule": {"leadSponsor": {"name": sponsor}},
        "conditionsModule": {"conditions": conditions or ["Type 2 Diabetes"]},
        "designModule": {
            "studyType": "INTERVENTIONAL",
            "phases": phases or ["PHASE2"],
            "enrollmentInfo": {"count": 120, "type": "ESTIMATED"},
        },
        "descriptionModule": {"briefSummary": brief},
    }
    if detailed:
        protocol["descriptionModule"]["detailedDescription"] = detailed
    if interventions:
        protocol["armsInterventionsModule"] = {"interventions": interventions}
    if outcomes:
        protocol["outcomesModule"] = outcomes
    if criteria:
        protocol["eligibilityModule"] = criteria
    if locations:
        protocol["contactsLocationsModule"] = {"locations": locations}

    return {
        "protocolSection": protocol,
        # Generated taxonomy, which the connector must ignore.
        "derivedSection": {"conditionBrowseModule": {"meshes": [{"term": "SHOULD NOT APPEAR"}]}},
        "hasResults": False,
    }


def _last_update(study):
    return study["protocolSection"]["statusModule"]["lastUpdatePostDateStruct"]["date"]


def _id_of(study):
    return study["protocolSection"]["identificationModule"]["nctId"]


class _Resp:
    def __init__(self, payload, text=None):
        self._payload = payload
        self.text = text or ""

    def raise_for_status(self):
        pass

    def json(self):
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


_RANGE_RE = re.compile(r"RANGE\[([0-9-]+),MAX\]")


class FakeCTSession:
    """Minimal stand-in for a ``requests`` session hitting the studies endpoint."""

    def __init__(self, studies, page_size=None):
        self.studies = list(studies)
        self.forced_page_size = page_size
        self.calls = []

    def get(self, url, params=None):
        params = dict(params or {})
        self.calls.append(params)

        selected = self.studies

        if "filter.ids" in params:
            wanted = set(params["filter.ids"].split(","))
            selected = [s for s in selected if _id_of(s) in wanted]
        else:
            if params.get("filter.overallStatus"):
                allowed = set(params["filter.overallStatus"].split(","))
                selected = [
                    s
                    for s in selected
                    if s["protocolSection"]["statusModule"]["overallStatus"] in allowed
                ]
            advanced = params.get("filter.advanced")
            if advanced:
                match = _RANGE_RE.search(advanced)
                assert match, f"unexpected advanced filter: {advanced!r}"
                since = match.group(1)
                # The live API's RANGE is inclusive at both ends.
                selected = [s for s in selected if _last_update(s) >= since]

        page_size = self.forced_page_size or int(params.get("pageSize", 10))
        # Tokens are opaque to the connector; this fake encodes an offset in
        # them. A token it did not mint (as a broken server would send) reads
        # as the start of the list rather than crashing.
        try:
            offset = int(params.get("pageToken", 0))
        except (TypeError, ValueError):
            offset = 0
        window = selected[offset : offset + page_size]

        payload = {"studies": [self._project(s, params) for s in window]}
        if offset + page_size < len(selected):
            payload["nextPageToken"] = str(offset + page_size)
        return _Resp(payload)

    @staticmethod
    def _project(study, params):
        """Honour the fields projection the deletion sweep relies on."""
        if params.get("fields") == "NCTId":
            return {
                "protocolSection": {
                    "identificationModule": {"nctId": _id_of(study)},
                }
            }
        return study


def _sync(session, state=None, **kwargs):
    """Run a sync pass and return (rows, state). Defaults to a real scope."""
    state = {} if state is None else state
    kwargs.setdefault("condition", "diabetes")
    rows = list(sync_studies(session, state, **kwargs))
    return rows, state


# ---------------------------------------------------------------------------
# Scope
# ---------------------------------------------------------------------------
def test_scope_options_map_onto_api_parameters():
    params = _scope_params("diabetes", "Acme", ["RECRUITING", "COMPLETED"], "insulin")
    assert params == {
        "query.cond": "diabetes",
        "query.spons": "Acme",
        "filter.overallStatus": "RECRUITING,COMPLETED",
        "query.term": "insulin",
    }
    assert _scope_params(None, None, None, None) == {}


def test_source_requires_a_scope():
    pytest.importorskip("dlt")
    with pytest.raises(ValueError, match="requires a scope"):
        clinicaltrials_source(session=object())


def test_status_filter_is_applied():
    session = FakeCTSession(
        [
            _study("NCT01", status="RECRUITING"),
            _study("NCT02", status="COMPLETED"),
        ]
    )
    rows, _ = _sync(session, statuses=["RECRUITING"])
    assert [r["id"] for r in rows] == ["NCT01"]


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------
def test_token_pagination_walks_every_page():
    session = FakeCTSession([_study(f"NCT{i:02d}") for i in range(1, 8)], page_size=3)
    rows, state = _sync(session)

    assert len([r for r in rows if not r.get("_deleted")]) == 7
    assert len(state["known_ids"]) == 7


def test_pagination_stops_on_a_repeated_token():
    class StuckSession(FakeCTSession):
        def get(self, url, params=None):
            response = super().get(url, params)
            # Always hand back the same token, as a broken server would.
            response._payload["nextPageToken"] = "same"
            return response

    session = StuckSession([_study("NCT01"), _study("NCT02")], page_size=1)
    rows, _ = _sync(session)

    # The walk stops on the second sight of the token instead of looping, and
    # the study repeated across those pages is emitted once.
    assert [r["id"] for r in rows] == ["NCT01"]


# ---------------------------------------------------------------------------
# Incremental sync
# ---------------------------------------------------------------------------
def test_first_run_sends_no_date_filter_and_records_the_cursor():
    session = FakeCTSession(
        [_study("NCT01", last_update="2026-09-01"), _study("NCT02", last_update="2026-10-05")]
    )
    rows, state = _sync(session)

    assert {r["id"] for r in rows} == {"NCT01", "NCT02"}
    assert state["last_update_posted"] == "2026-10-05"
    delta_call = session.calls[0]
    assert "filter.advanced" not in delta_call
    assert delta_call["query.cond"] == "diabetes"


def test_later_runs_push_the_cursor_to_the_api():
    session = FakeCTSession(
        [_study("NCT01", last_update="2026-09-01"), _study("NCT02", last_update="2026-10-05")]
    )
    state = {"known_ids": ["NCT01", "NCT02"], "last_update_posted": "2026-10-02"}
    rows, state = _sync(session, state=state)

    assert [r["id"] for r in rows] == ["NCT02"]
    assert "AREA[LastUpdatePostDate]RANGE[2026-10-02,MAX]" in session.calls[0]["filter.advanced"]


def test_cursor_day_is_refetched_so_same_day_edits_are_not_lost():
    # A study updated on the cursor date itself must still come back, because
    # the previous run may have read it earlier that same day.
    session = FakeCTSession([_study("NCT01", last_update="2026-10-02")])
    state = {"known_ids": ["NCT01"], "last_update_posted": "2026-10-02"}
    rows, _ = _sync(session, state=state)

    assert [r["id"] for r in rows] == ["NCT01"]


def test_run_with_no_changes_emits_nothing():
    session = FakeCTSession([_study("NCT01", last_update="2026-09-01")])
    state = {"known_ids": ["NCT01"], "last_update_posted": "2026-10-02"}
    rows, state = _sync(session, state=state)

    assert rows == []
    assert state["known_ids"] == ["NCT01"]
    assert state["last_update_posted"] == "2026-10-02"


def test_study_new_to_the_corpus_below_the_cursor_is_backfilled_by_id():
    # NCT02 predates the cursor, so the server-side date filter cannot return
    # it. The scope sweep notices it is unknown and it gets fetched by id.
    session = FakeCTSession(
        [_study("NCT01", last_update="2026-10-05"), _study("NCT02", last_update="2026-01-01")]
    )
    state = {"known_ids": ["NCT01"], "last_update_posted": "2026-10-02"}
    rows, state = _sync(session, state=state)

    assert {r["id"] for r in rows} == {"NCT01", "NCT02"}
    assert any("filter.ids" in call for call in session.calls)
    assert state["known_ids"] == ["NCT01", "NCT02"]


# ---------------------------------------------------------------------------
# Forget-on-delete
# ---------------------------------------------------------------------------
def test_study_that_left_the_scope_emits_a_hard_delete_marker():
    session = FakeCTSession([_study("NCT01", last_update="2026-09-01")])
    state = {"known_ids": ["NCT01", "NCT02"], "last_update_posted": "2026-10-02"}
    rows, state = _sync(session, state=state)

    assert rows == [{"id": "NCT02", "_deleted": True}]
    assert state["known_ids"] == ["NCT01"]


def test_empty_sweep_does_not_mass_delete_and_preserves_state():
    session = FakeCTSession([])
    state = {"known_ids": ["NCT01", "NCT02"], "last_update_posted": "2026-10-02"}
    rows, state = _sync(session, state=state)

    assert rows == []
    assert state["known_ids"] == ["NCT01", "NCT02"]


def test_detect_deletions_off_skips_the_sweep():
    session = FakeCTSession([_study("NCT01")])
    rows, state = _sync(session, detect_deletions=False)

    assert [r["id"] for r in rows] == ["NCT01"]
    assert not any(call.get("fields") == "NCTId" for call in session.calls)
    assert state["known_ids"] == ["NCT01"]


def test_live_rows_carry_a_false_delete_marker():
    session = FakeCTSession([_study("NCT01")])
    rows, _ = _sync(session)
    assert rows[0]["_deleted"] is False


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def test_content_renders_the_text_modules_and_skips_generated_taxonomy():
    study = _study(
        "NCT01",
        brief="Testing whether drug X lowers HbA1c.",
        detailed="Participants receive drug X or placebo for 24 weeks.",
        interventions=[
            {"type": "DRUG", "name": "Drug X", "description": "10mg once daily"},
            {"type": "OTHER", "name": "Placebo"},
        ],
        outcomes={
            "primaryOutcomes": [
                {
                    "measure": "Change in HbA1c",
                    "description": "From baseline to week 24",
                    "timeFrame": "24 weeks",
                }
            ],
            "secondaryOutcomes": [{"measure": "Body weight change"}],
        },
        criteria={
            "eligibilityCriteria": "Inclusion: adults with T2D. Exclusion: pregnancy.",
            "sex": "ALL",
            "minimumAge": "18 Years",
            "healthyVolunteers": False,
        },
        locations=[
            {"country": "United States", "city": "Boston"},
            {"country": "Germany", "city": "Berlin"},
        ],
    )
    text = _content(study)

    assert "NCT ID: NCT01" in text
    assert "Status: RECRUITING" in text
    assert "Phase: PHASE2" in text
    assert "Lead sponsor: Acme Pharma" in text
    assert "Conditions: Type 2 Diabetes" in text
    assert "Enrollment: 120 (ESTIMATED)" in text
    assert "Last update posted: 2026-10-01" in text
    assert "Testing whether drug X lowers HbA1c." in text
    assert "Participants receive drug X or placebo" in text
    assert "- Drug X (DRUG): 10mg once daily" in text
    assert "- Placebo (OTHER)" in text
    assert "- Primary: Change in HbA1c" in text
    assert "Time frame: 24 weeks" in text
    assert "- Secondary: Body weight change" in text
    assert "minimum age: 18 Years" in text
    assert "Inclusion: adults with T2D." in text
    assert "2 site(s) in Germany, United States" in text
    # derivedSection is generated taxonomy, not knowledge.
    assert "SHOULD NOT APPEAR" not in text


def test_markdown_escapes_in_registry_text_are_undone():
    # Live records carry backslash-escaped punctuation in their free text. Left
    # in, "PRG Science \\& Technology Co." extracts as a mangled entity name.
    study = _study(
        "NCT01",
        sponsor=r"PRG Science \& Technology Co., Ltd.",
        brief=r"Doses \> 10mg were excluded \[per protocol\].",
        detailed=r"Developed by PRG S\&T for rare aging diseases.",
    )
    text = _content(study)

    assert "PRG Science & Technology Co., Ltd." in text
    assert "Doses > 10mg were excluded [per protocol]." in text
    assert "PRG S&T" in text
    assert "\\" not in text
    # The node set must carry the clean name too, so grouping by sponsor works.
    assert "sponsor:PRG Science & Technology Co., Ltd." in _node_sets(study)


def test_row_carries_the_document_contract():
    session = FakeCTSession([_study("NCT01", title="A trial of X")])
    rows, _ = _sync(session)
    row = rows[0]

    assert row["id"] == "NCT01"
    assert row["title"] == "A trial of X"
    assert row["url"] == "https://clinicaltrials.gov/study/NCT01"
    assert "Brief summary" in row["content"]


def test_title_falls_back_to_the_official_title():
    study = _study("NCT01")
    del study["protocolSection"]["identificationModule"]["briefTitle"]
    study["protocolSection"]["identificationModule"]["officialTitle"] = "The official long title"

    session = FakeCTSession([study])
    rows, _ = _sync(session)
    assert rows[0]["title"] == "The official long title"


def test_node_sets_cover_conditions_sponsor_and_status_in_stable_order():
    study = _study("NCT01", conditions=["Obesity", "Type 2 Diabetes"], status="COMPLETED")
    assert _node_sets(study) == [
        "condition:Obesity",
        "condition:Type 2 Diabetes",
        "sponsor:Acme Pharma",
        "status:COMPLETED",
    ]


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------
def test_rate_limited_request_is_retried_honouring_retry_after(monkeypatch):
    requests = pytest.importorskip("requests")

    slept = []
    monkeypatch.setattr(
        "cognee_community_connector_clinicaltrials.clinicaltrials.time.sleep",
        lambda s: slept.append(s),
    )

    class RateLimitedOnce(FakeCTSession):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.hit = False

        def get(self, url, params=None):
            if not self.hit:
                self.hit = True
                response = requests.Response()
                response.status_code = 429
                response.headers["Retry-After"] = "3"
                raise requests.exceptions.HTTPError(response=response)
            return super().get(url, params)

    session = RateLimitedOnce([_study("NCT01")])
    rows, _ = _sync(session)

    assert [r["id"] for r in rows] == ["NCT01"]
    assert slept == [3.0]


def test_bad_request_is_not_retried(monkeypatch):
    requests = pytest.importorskip("requests")
    monkeypatch.setattr(
        "cognee_community_connector_clinicaltrials.clinicaltrials.time.sleep", lambda s: None
    )

    class BadRequest(FakeCTSession):
        def get(self, url, params=None):
            response = requests.Response()
            response.status_code = 400
            raise requests.exceptions.HTTPError(response=response)

    with pytest.raises(requests.exceptions.HTTPError):
        _sync(BadRequest([]))


def test_non_json_body_raises_a_clear_error():
    class HtmlSession(FakeCTSession):
        def get(self, url, params=None):
            return _Resp(None, text="<html>maintenance</html>")

    with pytest.raises(ValueError, match="non-JSON body"):
        _sync(HtmlSession([]))


def test_a_user_agent_is_always_sent():
    pytest.importorskip("requests")
    from cognee_community_connector_clinicaltrials.clinicaltrials import _make_session

    assert "cognee" in _make_session().headers["User-Agent"]
    assert _make_session("my-agent/1.0").headers["User-Agent"] == "my-agent/1.0"


# ---------------------------------------------------------------------------
# dlt wiring
# ---------------------------------------------------------------------------
def test_source_is_configured_for_merge_hard_delete_and_document_mode():
    pytest.importorskip("dlt")
    from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR

    resource = clinicaltrials_source(condition="diabetes", session=FakeCTSession([]))
    schema = resource.compute_table_schema()

    write_disposition = schema["write_disposition"]
    if isinstance(write_disposition, dict):
        write_disposition = write_disposition.get("disposition")
    assert write_disposition == "merge"

    columns = schema["columns"]
    assert columns["id"].get("primary_key") is True
    assert columns["_deleted"].get("hard_delete") is True
    assert columns["cognee_node_set"].get("data_type") == "json"

    assert getattr(resource, DOCUMENT_SOURCE_ATTR) == "clinicaltrials"


# ---------------------------------------------------------------------------
# End-to-end: a real dlt merge acts on the hard-delete marker
# ---------------------------------------------------------------------------
def test_forget_on_delete_end_to_end_through_a_real_dlt_merge(tmp_path):
    dlt = pytest.importorskip("dlt")
    pytest.importorskip("duckdb")

    pipeline = dlt.pipeline(
        pipeline_name="test_clinicaltrials_e2e",
        destination=dlt.destinations.duckdb(str(tmp_path / "ct.duckdb")),
        dataset_name="trials",
    )

    # Sync #1: two studies land in the destination.
    session1 = FakeCTSession([_study("NCT01"), _study("NCT02")])
    pipeline.run(clinicaltrials_source(condition="diabetes", session=session1))
    with pipeline.sql_client() as client:
        assert client.execute_sql("SELECT count(*) FROM clinicaltrials_studies")[0][0] == 2

    # Sync #2: NCT02 has left the scope. The connector emits a hard-delete
    # marker for it; dlt's merge removes it from the destination.
    session2 = FakeCTSession([_study("NCT01")])
    pipeline.run(clinicaltrials_source(condition="diabetes", session=session2))
    with pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id FROM clinicaltrials_studies")
    assert [r[0] for r in rows] == ["NCT01"]
