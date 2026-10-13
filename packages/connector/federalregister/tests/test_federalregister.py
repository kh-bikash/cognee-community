"""Unit tests for the Federal Register connector.

The API is fully mocked via ``FakeFRSession``: no ``requests`` traffic and no
network access are required, so these run in CI. The fake models the parts of
the contract the connector depends on, each checked against the live API first:
the ``conditions[publication_date][gte]`` condition, ``next_page_url`` cursor
pagination, the ``fields[]`` projection, the per-document endpoint, and the
plain-text full-text URL.

Coverage:

  - published_since is required, so the archive is never synced unbounded
  - scope options map onto their condition parameters
  - cursor pagination follows next_page_url rather than incrementing page
  - the first run starts at published_since; later runs start at the cursor
  - the cursor day is re-read (inclusive gte), so same-day publications survive
  - a run with no new documents emits nothing
  - documents that left the scope become hard-delete markers
  - an empty sweep does not mass-delete, and preserves state
  - a document new to the corpus below the cursor is backfilled by number
  - detect_deletions=False skips the sweep entirely
  - presidential documents with a null abstract still render
  - full text is opt-in, and a failing text URL degrades instead of aborting
  - node sets come from agencies and type, deterministically ordered
  - 429 is retried honouring Retry-After; 404 propagates; non-JSON raises
  - the dlt resource is wired with merge + id PK + hard_delete + document mode
  - a real dlt merge removes the marked row (end-to-end forget-on-delete)
"""

import pytest

from cognee_community_connector_federalregister.federalregister import (
    _content,
    _node_sets,
    _query_params,
    federalregister_source,
    sync_documents,
)

DOCUMENTS_URL = "https://www.federalregister.gov/api/v1/documents.json"


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------
def _document(
    number,
    *,
    publication_date="2026-10-01",
    title="A rule about something",
    abstract="The agency is amending its regulations.",
    doc_type="Rule",
    agencies=("State Department",),
    action="Final rule.",
    citation="91 FR 62313",
    effective_on="2026-10-01",
    comments_close_on=None,
    dockets=("Public Notice: 13131",),
    president=None,
):
    """Build a document in the shape the API returns."""
    return {
        "document_number": number,
        "title": title,
        "abstract": abstract,
        "publication_date": publication_date,
        "type": doc_type,
        "html_url": f"https://www.federalregister.gov/documents/{number}",
        "agencies": [{"name": name, "raw_name": name.upper()} for name in agencies],
        "docket_ids": list(dockets),
        "effective_on": effective_on,
        "comments_close_on": comments_close_on,
        "action": action,
        "citation": citation,
        "president": president,
        "raw_text_url": f"https://www.federalregister.gov/full_text/{number}.txt",
    }


class _Resp:
    def __init__(self, payload, text=""):
        self._payload = payload
        self.text = text

    def raise_for_status(self):
        pass

    def json(self):
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


class FakeFRSession:
    """Minimal stand-in for a ``requests`` session hitting the API."""

    def __init__(self, documents, per_page=None, full_text="FULL TEXT BODY"):
        self.documents = list(documents)
        self.forced_per_page = per_page
        self.full_text = full_text
        self.calls = []

    def get(self, url, params=None):
        params = dict(params or {})
        self.calls.append((url, params))

        if url.endswith(".txt"):
            return _Resp(None, text=self.full_text)

        # Per-document endpoint used to backfill by number.
        if "/documents/" in url and url.endswith(".json"):
            number = url.rsplit("/", 1)[1][: -len(".json")]
            for document in self.documents:
                if document["document_number"] == number:
                    return _Resp(document)
            raise AssertionError(f"unknown document {number}")

        # The next-page URL carries its own state; this fake encodes the offset
        # and the gte condition in it.
        if "next_page_offset=" in url:
            offset = int(url.split("next_page_offset=")[1].split("&")[0])
            since = url.split("gte=")[1].split("&")[0]
            return self._page(since, offset, params)

        since = params.get("conditions[publication_date][gte]", "")
        return self._page(since, 0, params)

    def _page(self, since, offset, params):
        selected = [d for d in self.documents if d["publication_date"] >= since]
        selected.sort(key=lambda d: (d["publication_date"], d["document_number"]))
        per_page = self.forced_per_page or int(params.get("per_page", 20) or 20)
        window = selected[offset : offset + per_page]

        payload = {
            "count": len(selected),
            "results": [self._project(d, params) for d in window],
        }
        if offset + per_page < len(selected):
            payload["next_page_url"] = (
                f"{DOCUMENTS_URL}?next_page_offset={offset + per_page}&gte={since}"
            )
        return _Resp(payload)

    @staticmethod
    def _project(document, params):
        """Honour the fields projection the deletion sweep relies on."""
        fields = params.get("fields[]")
        if fields == ["document_number"]:
            return {"document_number": document["document_number"]}
        return document


def _sync(session, state=None, **kwargs):
    """Run a sync pass and return (rows, state)."""
    state = {} if state is None else state
    kwargs.setdefault("published_since", "2026-10-01")
    rows = list(sync_documents(session, state, **kwargs))
    return rows, state


# ---------------------------------------------------------------------------
# Scope
# ---------------------------------------------------------------------------
def test_published_since_is_required():
    pytest.importorskip("dlt")
    with pytest.raises(ValueError, match="published_since"):
        federalregister_source(published_since="", session=object())


def test_scope_options_map_onto_condition_parameters():
    params = _query_params(
        published_since="2026-10-01",
        agencies=["environmental-protection-agency"],
        document_types=["RULE", "PRORULE"],
        term="emissions",
        fields=("document_number",),
        per_page=50,
    )
    assert params["conditions[publication_date][gte]"] == "2026-10-01"
    assert params["conditions[agencies][]"] == ["environmental-protection-agency"]
    assert params["conditions[type][]"] == ["RULE", "PRORULE"]
    assert params["conditions[term]"] == "emissions"
    assert params["fields[]"] == ["document_number"]
    assert params["per_page"] == 50
    # order=oldest keeps the newest publication date at the end of the walk,
    # which is where the cursor is read from.
    assert params["order"] == "oldest"


def test_per_page_is_capped_at_the_api_limit():
    params = _query_params(
        published_since="2026-10-01",
        agencies=None,
        document_types=None,
        term=None,
        fields=("document_number",),
        per_page=5000,
    )
    assert params["per_page"] == 1000


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------
def test_pagination_follows_next_page_url():
    session = FakeFRSession(
        [_document(f"2026-{i:05d}", publication_date="2026-10-01") for i in range(1, 8)],
        per_page=3,
    )
    rows, state = _sync(session)

    assert len([r for r in rows if not r.get("_deleted")]) == 7
    assert len(state["known_ids"]) == 7
    # The follow-up requests went to the next-page URL, not to page=N.
    assert any("next_page_offset=" in url for url, _ in session.calls)


# ---------------------------------------------------------------------------
# Incremental sync
# ---------------------------------------------------------------------------
def test_first_run_starts_at_published_since_and_records_the_cursor():
    session = FakeFRSession(
        [
            _document("2026-00001", publication_date="2026-10-01"),
            _document("2026-00002", publication_date="2026-10-05"),
        ]
    )
    rows, state = _sync(session)

    assert {r["id"] for r in rows} == {"2026-00001", "2026-00002"}
    assert state["last_publication_date"] == "2026-10-05"
    _, first_params = session.calls[0]
    assert first_params["conditions[publication_date][gte]"] == "2026-10-01"


def test_later_runs_start_at_the_cursor():
    session = FakeFRSession(
        [
            _document("2026-00001", publication_date="2026-10-01"),
            _document("2026-00002", publication_date="2026-10-05"),
        ]
    )
    state = {
        "known_ids": ["2026-00001", "2026-00002"],
        "last_publication_date": "2026-10-03",
    }
    rows, state = _sync(session, state=state)

    assert [r["id"] for r in rows] == ["2026-00002"]
    assert session.calls[0][1]["conditions[publication_date][gte]"] == "2026-10-03"


def test_cursor_day_is_reread_so_same_day_publications_are_not_lost():
    session = FakeFRSession([_document("2026-00001", publication_date="2026-10-03")])
    state = {"known_ids": ["2026-00001"], "last_publication_date": "2026-10-03"}
    rows, _ = _sync(session, state=state)

    assert [r["id"] for r in rows] == ["2026-00001"]


def test_run_with_no_new_documents_emits_nothing():
    session = FakeFRSession([_document("2026-00001", publication_date="2026-10-01")])
    state = {"known_ids": ["2026-00001"], "last_publication_date": "2026-10-05"}
    rows, state = _sync(session, state=state)

    assert rows == []
    assert state["known_ids"] == ["2026-00001"]
    assert state["last_publication_date"] == "2026-10-05"


def test_document_new_to_the_corpus_below_the_cursor_is_backfilled():
    # 2026-00002 predates the cursor, so the date condition cannot return it.
    # The scope sweep notices it is unknown and it gets fetched by number.
    session = FakeFRSession(
        [
            _document("2026-00001", publication_date="2026-10-06"),
            _document("2026-00002", publication_date="2026-10-02"),
        ]
    )
    state = {"known_ids": ["2026-00001"], "last_publication_date": "2026-10-05"}
    rows, state = _sync(session, state=state)

    assert {r["id"] for r in rows} == {"2026-00001", "2026-00002"}
    assert any(url.endswith("/documents/2026-00002.json") for url, _ in session.calls)
    assert state["known_ids"] == ["2026-00001", "2026-00002"]


# ---------------------------------------------------------------------------
# Forget-on-delete
# ---------------------------------------------------------------------------
def test_document_that_left_the_scope_emits_a_hard_delete_marker():
    session = FakeFRSession([_document("2026-00001", publication_date="2026-10-01")])
    state = {
        "known_ids": ["2026-00001", "2026-00002"],
        "last_publication_date": "2026-10-05",
    }
    rows, state = _sync(session, state=state)

    assert rows == [{"id": "2026-00002", "_deleted": True}]
    assert state["known_ids"] == ["2026-00001"]


def test_empty_sweep_does_not_mass_delete_and_preserves_state():
    session = FakeFRSession([])
    state = {
        "known_ids": ["2026-00001", "2026-00002"],
        "last_publication_date": "2026-10-05",
    }
    rows, state = _sync(session, state=state)

    assert rows == []
    assert state["known_ids"] == ["2026-00001", "2026-00002"]


def test_detect_deletions_off_skips_the_sweep():
    session = FakeFRSession([_document("2026-00001")])
    rows, state = _sync(session, detect_deletions=False)

    assert [r["id"] for r in rows] == ["2026-00001"]
    assert not any(params.get("fields[]") == ["document_number"] for _, params in session.calls)
    assert state["known_ids"] == ["2026-00001"]


def test_live_rows_carry_a_false_delete_marker():
    session = FakeFRSession([_document("2026-00001")])
    rows, _ = _sync(session)
    assert rows[0]["_deleted"] is False


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def test_content_renders_the_header_and_abstract():
    document = _document(
        "2026-00001",
        title="Emissions standards",
        abstract="EPA is tightening limits on particulate matter.",
        agencies=("Environmental Protection Agency",),
        comments_close_on="2026-11-30",
    )
    text = _content(document)

    assert "Document number: 2026-00001" in text
    assert "Type: Rule" in text
    assert "Action: Final rule." in text
    assert "Agencies: Environmental Protection Agency" in text
    assert "Citation: 91 FR 62313" in text
    assert "Published: 2026-10-01" in text
    assert "Effective: 2026-10-01" in text
    assert "Comments close: 2026-11-30" in text
    assert "Dockets: Public Notice: 13131" in text
    assert "EPA is tightening limits on particulate matter." in text


def test_agency_rules_do_not_name_the_sitting_president():
    # The API returns the sitting president on every document, including
    # technical agency rules. Naming them on an EPA rule reads as authorship.
    document = _document(
        "2026-20849",
        doc_type="Rule",
        agencies=("Environmental Protection Agency",),
        president={"identifier": "donald-trump", "name": "Donald Trump"},
    )
    assert "President:" not in _content(document)


def test_presidential_document_without_an_abstract_still_renders():
    # The two most recent documents in a live query were presidential
    # documents, both with a null abstract.
    document = _document(
        "2026-20972",
        title="Columbus Day, 2026",
        abstract=None,
        doc_type="Presidential Document",
        agencies=("Executive Office of the President",),
        action=None,
        citation=None,
        effective_on=None,
        dockets=(),
        president={"identifier": "donald-trump", "name": "Donald Trump"},
    )
    text = _content(document)

    assert "Type: Presidential Document" in text
    assert "President: Donald Trump" in text
    assert "Abstract:" not in text
    assert text.strip()


def test_full_text_is_opt_in():
    session = FakeFRSession([_document("2026-00001")])
    rows, _ = _sync(session)
    assert "FULL TEXT BODY" not in rows[0]["content"]
    assert not any(url.endswith(".txt") for url, _ in session.calls)

    session = FakeFRSession([_document("2026-00001")])
    rows, _ = _sync(session, include_full_text=True)
    assert "Full text:\nFULL TEXT BODY" in rows[0]["content"]


def test_failing_full_text_url_degrades_instead_of_aborting(monkeypatch):
    requests = pytest.importorskip("requests")
    monkeypatch.setattr(
        "cognee_community_connector_federalregister.federalregister.time.sleep", lambda s: None
    )

    class NoFullText(FakeFRSession):
        def get(self, url, params=None):
            if url.endswith(".txt"):
                response = requests.Response()
                response.status_code = 404
                raise requests.exceptions.HTTPError(response=response)
            return super().get(url, params)

    session = NoFullText([_document("2026-00001")])
    rows, _ = _sync(session, include_full_text=True)

    # The abstract still made it in; only the full text was lost.
    assert "The agency is amending its regulations." in rows[0]["content"]
    assert "Full text:" not in rows[0]["content"]


def test_node_sets_cover_agencies_and_type_in_stable_order():
    document = _document(
        "2026-00001",
        agencies=("State Department", "Commerce Department"),
        doc_type="Proposed Rule",
    )
    assert _node_sets(document) == [
        "agency:Commerce Department",
        "agency:State Department",
        "type:Proposed Rule",
    ]


def test_row_carries_the_document_contract():
    session = FakeFRSession([_document("2026-00001", title="Emissions standards")])
    rows, _ = _sync(session)
    row = rows[0]

    assert row["id"] == "2026-00001"
    assert row["title"] == "Emissions standards"
    assert row["url"] == "https://www.federalregister.gov/documents/2026-00001"


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------
def test_rate_limited_request_is_retried_honouring_retry_after(monkeypatch):
    requests = pytest.importorskip("requests")

    slept = []
    monkeypatch.setattr(
        "cognee_community_connector_federalregister.federalregister.time.sleep",
        lambda s: slept.append(s),
    )

    class RateLimitedOnce(FakeFRSession):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.hit = False

        def get(self, url, params=None):
            if not self.hit:
                self.hit = True
                response = requests.Response()
                response.status_code = 429
                response.headers["Retry-After"] = "4"
                raise requests.exceptions.HTTPError(response=response)
            return super().get(url, params)

    session = RateLimitedOnce([_document("2026-00001")])
    rows, _ = _sync(session)

    assert [r["id"] for r in rows] == ["2026-00001"]
    assert slept == [4.0]


def test_not_found_is_not_retried(monkeypatch):
    requests = pytest.importorskip("requests")
    monkeypatch.setattr(
        "cognee_community_connector_federalregister.federalregister.time.sleep", lambda s: None
    )

    class NotFound(FakeFRSession):
        def get(self, url, params=None):
            response = requests.Response()
            response.status_code = 404
            raise requests.exceptions.HTTPError(response=response)

    with pytest.raises(requests.exceptions.HTTPError):
        _sync(NotFound([]))


def test_non_json_body_raises_a_clear_error():
    class HtmlSession(FakeFRSession):
        def get(self, url, params=None):
            return _Resp(None, text="<html>maintenance</html>")

    with pytest.raises(ValueError, match="non-JSON body"):
        _sync(HtmlSession([]))


def test_a_user_agent_is_always_sent():
    pytest.importorskip("requests")
    from cognee_community_connector_federalregister.federalregister import _make_session

    assert "cognee" in _make_session().headers["User-Agent"]
    assert _make_session("my-agent/1.0").headers["User-Agent"] == "my-agent/1.0"


# ---------------------------------------------------------------------------
# dlt wiring
# ---------------------------------------------------------------------------
def test_source_is_configured_for_merge_hard_delete_and_document_mode():
    pytest.importorskip("dlt")
    from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR

    resource = federalregister_source(published_since="2026-10-01", session=FakeFRSession([]))
    schema = resource.compute_table_schema()

    write_disposition = schema["write_disposition"]
    if isinstance(write_disposition, dict):
        write_disposition = write_disposition.get("disposition")
    assert write_disposition == "merge"

    columns = schema["columns"]
    assert columns["id"].get("primary_key") is True
    assert columns["_deleted"].get("hard_delete") is True
    assert columns["cognee_node_set"].get("data_type") == "json"

    assert getattr(resource, DOCUMENT_SOURCE_ATTR) == "federalregister"


# ---------------------------------------------------------------------------
# End-to-end: a real dlt merge acts on the hard-delete marker
# ---------------------------------------------------------------------------
def test_forget_on_delete_end_to_end_through_a_real_dlt_merge(tmp_path):
    dlt = pytest.importorskip("dlt")
    pytest.importorskip("duckdb")

    pipeline = dlt.pipeline(
        pipeline_name="test_federalregister_e2e",
        destination=dlt.destinations.duckdb(str(tmp_path / "fr.duckdb")),
        dataset_name="regulations",
    )

    session1 = FakeFRSession([_document("2026-00001"), _document("2026-00002")])
    pipeline.run(federalregister_source(published_since="2026-10-01", session=session1))
    with pipeline.sql_client() as client:
        assert client.execute_sql("SELECT count(*) FROM federalregister_documents")[0][0] == 2

    # 2026-00002 has left the scope; its tombstone removes it on merge.
    session2 = FakeFRSession([_document("2026-00001")])
    pipeline.run(federalregister_source(published_since="2026-10-01", session=session2))
    with pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id FROM federalregister_documents")
    assert [r[0] for r in rows] == ["2026-00001"]
