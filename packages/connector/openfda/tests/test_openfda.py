"""Unit tests for the openFDA connector.

The API is fully mocked via ``FakeFDASession``: no ``requests`` traffic and no
network access are required, so these run in CI. The fake models the parts of
the contract the connector depends on, each checked against the live API first:
``search`` with an inclusive ``field:[A TO B]`` range, ``limit``/``skip``
paging, the rejection of ``skip`` above 25000, ``meta.results.total``, the
per-response ``meta.disclaimer``, single-element list fields, and the separate
identity of ``id`` and ``set_id`` on a versioned label.

Coverage:

  - a scope is required, and the dataset name is validated
  - scope terms map onto the shared openfda identity fields
  - limit/skip paging walks a window and stops on a short page
  - an oversized window is bisected rather than truncated at the skip ceiling
  - a single day over the ceiling is reported rather than silently truncated
  - labels collapse by set_id to the highest version, not by id
  - an unchanged label version is not re-emitted; a new revision is
  - *_table fields are dropped, and list fields are flattened
  - the openFDA disclaimer is carried into every document
  - recall records render their own header and keep only coarse firm location
  - records that left the scope become hard-delete markers
  - an empty window does not mass-delete, and preserves state
  - detect_deletions=False walks from the cursor instead of the whole window
  - 429 is retried honouring Retry-After; 404 reads as "no matches"; 400 raises
  - the dlt resource is wired with merge + id PK + hard_delete + document mode
  - a real dlt merge removes the marked row (end-to-end forget-on-delete)
"""

import re

import pytest

from cognee_community_connector_openfda.openfda import (
    _DATASETS,
    _content,
    _node_sets,
    _scope_terms,
    _title,
    openfda_source,
    sync_records,
)

LABEL = _DATASETS["label"]
ENFORCEMENT = _DATASETS["enforcement"]

DISCLAIMER = "Do not rely on openFDA to make decisions regarding medical care."


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------
def _label(
    set_id,
    *,
    version=1,
    effective_time="20260701",
    brand="ADVIL",
    generic="IBUPROFEN",
    manufacturer="Pfizer",
    indications="For the relief of pain.",
    warnings="May cause stomach bleeding.",
    extra=None,
):
    """Build a drug label in the shape the API returns."""
    record = {
        # `id` changes with every revision; `set_id` is the stable product key.
        "id": f"{set_id}-rev{version}",
        "set_id": set_id,
        "version": str(version),
        "effective_time": effective_time,
        "indications_and_usage": [indications],
        "warnings_and_cautions": [warnings],
        # Table variants duplicate the prose in HTML and must be dropped.
        "dosage_and_administration": ["Take one tablet."],
        "dosage_and_administration_table": ["<table>SHOULD NOT APPEAR</table>"],
        "openfda": {
            "brand_name": [brand],
            "generic_name": [generic],
            "manufacturer_name": [manufacturer],
            "product_type": ["HUMAN OTC DRUG"],
            "route": ["ORAL"],
            "pharm_class_epc": ["Nonsteroidal Anti-inflammatory Drug [EPC]"],
        },
    }
    if extra:
        record.update(extra)
    return record


def _recall(
    recall_number,
    *,
    report_date="20260211",
    firm="Unichem Pharmaceuticals USA Inc.",
    reason="cGMP Deviations: impurity above specification.",
    product="Bisoprolol Fumarate tablets, 2.5 mg",
    classification="Class II",
    status="Ongoing",
):
    """Build an enforcement (recall) record in the shape the API returns."""
    return {
        "recall_number": recall_number,
        "report_date": report_date,
        "recall_initiation_date": "20260121",
        "center_classification_date": "20260130",
        "recalling_firm": firm,
        "reason_for_recall": reason,
        "product_description": product,
        "classification": classification,
        "status": status,
        "voluntary_mandated": "Voluntary: Firm initiated",
        "initial_firm_notification": "Letter",
        "code_info": "Lot # GBHL24005A",
        "distribution_pattern": "US Nationwide and PR.",
        "product_quantity": "N/A",
        "city": "East Brunswick",
        "state": "NJ",
        "country": "United States",
        # Contact data, which must not reach the document.
        "address_1": "1 Tower Center Blvd Ste 2200",
        "postal_code": "08816-1145",
        "openfda": {"brand_name": ["BISOPROLOL FUMARATE"]},
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


_RANGE_RE = re.compile(r"(\w+):\[(\d{8}) TO (\d{8})\]")


class FakeFDASession:
    """Minimal stand-in for a ``requests`` session hitting openFDA."""

    def __init__(self, records, cursor_field="effective_time", disclaimer=DISCLAIMER):
        self.records = list(records)
        self.cursor_field = cursor_field
        self.disclaimer = disclaimer
        self.calls = []

    def get(self, url, params=None):
        params = dict(params or {})
        self.calls.append(params)

        search = params.get("search", "")
        match = _RANGE_RE.search(search)
        assert match, f"every request must carry a date window: {search!r}"
        _, since, until = match.groups()

        selected = [r for r in self.records if since <= str(r.get(self.cursor_field, "")) <= until]
        selected.sort(key=lambda r: (str(r.get(self.cursor_field, "")), str(r.get("id", ""))))

        limit = int(params.get("limit", 1))
        skip = int(params.get("skip", 0))
        # The live API rejects skip above the ceiling with a 400.
        assert skip <= 25_000, "the connector must not page past the skip ceiling"

        window = selected[skip : skip + limit]
        return _Resp(
            {
                "meta": {
                    "disclaimer": self.disclaimer,
                    "results": {"skip": skip, "limit": limit, "total": len(selected)},
                },
                "results": window,
            }
        )


def _sync(session, state=None, **kwargs):
    """Run a sync pass and return (rows, state)."""
    state = {} if state is None else state
    kwargs.setdefault("generic_name", "ibuprofen")
    kwargs.setdefault("effective_since", "20260101")
    kwargs.setdefault("effective_until", "20261231")
    rows = list(sync_records(session, state, **kwargs))
    return rows, state


# ---------------------------------------------------------------------------
# Scope / validation
# ---------------------------------------------------------------------------
def test_a_scope_is_required():
    pytest.importorskip("dlt")
    with pytest.raises(ValueError, match="requires a scope"):
        openfda_source(session=object())


def test_dataset_name_is_validated():
    pytest.importorskip("dlt")
    with pytest.raises(ValueError, match="dataset must be one of"):
        openfda_source(dataset="device", generic_name="x", session=object())


def test_scope_terms_use_the_shared_identity_fields():
    terms = _scope_terms("ADVIL", "ibuprofen", "Pfizer", "boxed_warning:*")
    assert terms == [
        'openfda.brand_name:"ADVIL"',
        'openfda.generic_name:"ibuprofen"',
        'openfda.manufacturer_name:"Pfizer"',
        "(boxed_warning:*)",
    ]


# ---------------------------------------------------------------------------
# Paging and the skip ceiling
# ---------------------------------------------------------------------------
def test_paging_walks_a_window_and_stops_on_a_short_page():
    session = FakeFDASession([_label(f"set{i:03d}") for i in range(1, 8)])
    rows, state = _sync(session, page_size=3)

    assert len([r for r in rows if not r.get("_deleted")]) == 7
    assert len(state["known"]) == 7


def test_oversized_window_is_bisected_rather_than_truncated():
    class HugeWindow(FakeFDASession):
        """Reports more than the ceiling until the window is narrow enough."""

        def get(self, url, params=None):
            response = super().get(url, params)
            match = _RANGE_RE.search(params.get("search", ""))
            since, until = match.group(2), match.group(3)
            # Pretend any window wider than ~60 days is oversized.
            if int(until) - int(since) > 60:
                response._payload["meta"]["results"]["total"] = 30_000
                response._payload["results"] = []
            return response

    session = HugeWindow([_label("set001", effective_time="20260715")])
    rows, _ = _sync(session, page_size=10)

    windows = [
        _RANGE_RE.search(call["search"]).groups()[1:]
        for call in session.calls
        if _RANGE_RE.search(call.get("search", ""))
    ]
    # The full year was split into narrower windows rather than paged past the
    # ceiling, and the record in one of those halves still came through.
    assert any(int(until) - int(since) <= 60 for since, until in windows)
    assert [r["id"] for r in rows] == ["set001"]


def test_single_day_over_the_ceiling_is_reported_not_truncated_silently(caplog):
    class OneHugeDay(FakeFDASession):
        def get(self, url, params=None):
            response = super().get(url, params)
            response._payload["meta"]["results"]["total"] = 40_000
            response._payload["results"] = []
            return response

    session = OneHugeDay([])
    with caplog.at_level("WARNING"):
        rows, _ = _sync(session, effective_since="20260701", effective_until="20260701")

    assert rows == []
    assert any("skip ceiling" in message for message in caplog.messages)


# ---------------------------------------------------------------------------
# Label versioning
# ---------------------------------------------------------------------------
def test_labels_collapse_by_set_id_to_the_highest_version():
    # Every revision has its own `id`; keying on that would keep all three.
    session = FakeFDASession(
        [
            _label("set001", version=1, indications="First revision."),
            _label("set001", version=25, indications="Current revision."),
            _label("set001", version=12, indications="Middle revision."),
        ]
    )
    rows, state = _sync(session)

    assert len(rows) == 1
    assert rows[0]["id"] == "set001"
    assert "Current revision." in rows[0]["content"]
    assert "Label version: 25" in rows[0]["content"]
    assert state["known"] == ["set001@25"]


def test_unchanged_label_version_is_not_re_emitted():
    session = FakeFDASession([_label("set001", version=3)])
    state = {"known": ["set001@3"], "last_cursor": "20260701"}
    rows, state = _sync(session, state=state)

    assert rows == []
    assert state["known"] == ["set001@3"]


def test_new_label_revision_re_emits_the_row():
    session = FakeFDASession([_label("set001", version=4, indications="Revised text.")])
    state = {"known": ["set001@3"], "last_cursor": "20260701"}
    rows, state = _sync(session, state=state)

    assert [r["id"] for r in rows] == ["set001"]
    assert "Revised text." in rows[0]["content"]
    assert state["known"] == ["set001@4"]


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def test_label_renders_prose_and_drops_table_fields():
    text = _content(LABEL, _label("set001"))

    assert "Set ID: set001" in text
    assert "Brand name: ADVIL" in text
    assert "Generic name: IBUPROFEN" in text
    assert "Manufacturer: Pfizer" in text
    assert "Pharmacologic class: Nonsteroidal Anti-inflammatory Drug [EPC]" in text
    assert "Effective: 2026-07-01" in text
    assert "Indications and usage:\nFor the relief of pain." in text
    assert "Warnings and cautions:\nMay cause stomach bleeding." in text
    assert "Dosage and administration:\nTake one tablet." in text
    # The *_table variant duplicates the prose as HTML.
    assert "SHOULD NOT APPEAR" not in text
    assert "<table>" not in text


def test_list_fields_are_flattened_rather_than_rendered_as_lists():
    # Nearly every openFDA text field is a single-element list, so a naive
    # str() would put "['For the relief of pain.']" into the document.
    text = _content(LABEL, _label("set001"))
    assert "['For the relief of pain.']" not in text
    assert "[" not in text.split("Pharmacologic class")[0]


def test_multi_valued_identity_fields_are_comma_joined():
    # Real values from a live label: a combination product has two active
    # substances and two brand names. Space-joining makes them read as one.
    record = _label(
        "set001",
        extra={
            "openfda": {
                "brand_name": ["INVOKAMET", "INVOKAMET XR"],
                "generic_name": ["CANAGLIFLOZIN AND METFORMIN HYDROCHLORIDE"],
                "substance_name": ["CANAGLIFLOZIN", "METFORMIN HYDROCHLORIDE"],
                "application_number": ["NDA204353", "NDA205879"],
            }
        },
    )
    text = _content(LABEL, record)

    assert "Brand name: INVOKAMET, INVOKAMET XR" in text
    assert "Active substance: CANAGLIFLOZIN, METFORMIN HYDROCHLORIDE" in text
    assert "Application number: NDA204353, NDA205879" in text


def test_the_disclaimer_is_carried_into_every_document():
    session = FakeFDASession([_label("set001")])
    rows, _ = _sync(session)

    assert "openFDA disclaimer:" in rows[0]["content"]
    assert "Do not rely on openFDA" in rows[0]["content"]


def test_recall_renders_its_own_header_and_hides_contact_data():
    text = _content(ENFORCEMENT, _recall("D-0301-2026"))

    assert "Recall number: D-0301-2026" in text
    assert "Classification: Class II" in text
    assert "Status: Ongoing" in text
    assert "Recalling firm: Unichem Pharmaceuticals USA Inc." in text
    assert "Firm location: East Brunswick, NJ, United States" in text
    assert "Reported: 2026-02-11" in text
    assert "Reason for recall:\ncGMP Deviations: impurity above specification." in text
    assert "Affected lots:\nLot # GBHL24005A" in text
    # Street address and postal code are contact data, not knowledge.
    assert "Tower Center" not in text
    assert "08816" not in text
    # "N/A" placeholders should not be rendered as facts.
    assert "Quantity: N/A" not in text


def test_recalls_sync_with_their_own_identity_and_cursor():
    session = FakeFDASession(
        [_recall("D-0301-2026"), _recall("D-0302-2026")], cursor_field="report_date"
    )
    rows, state = _sync(session, dataset="enforcement")

    assert {r["id"] for r in rows} == {"D-0301-2026", "D-0302-2026"}
    # Enforcement records are not versioned, so identity is the bare number.
    assert state["known"] == ["D-0301-2026", "D-0302-2026"]
    assert state["last_cursor"] == "20260211"


def test_titles_name_the_product():
    assert _title(LABEL, _label("set001")) == "ADVIL (IBUPROFEN) label"
    assert _title(ENFORCEMENT, _recall("D-1")).startswith("Recall D-1: Bisoprolol")


def test_node_sets_differ_per_dataset_and_are_stable():
    assert _node_sets(LABEL, _label("set001")) == [
        "class:Nonsteroidal Anti-inflammatory Drug [EPC]",
        "generic:IBUPROFEN",
        "manufacturer:Pfizer",
    ]
    assert _node_sets(ENFORCEMENT, _recall("D-1")) == [
        "classification:Class II",
        "firm:Unichem Pharmaceuticals USA Inc.",
        "status:Ongoing",
    ]


def test_row_carries_the_document_contract():
    session = FakeFDASession([_label("set001")])
    rows, _ = _sync(session)
    row = rows[0]

    assert row["id"] == "set001"
    assert row["title"] == "ADVIL (IBUPROFEN) label"
    assert row["url"].endswith("setid=set001")
    assert row["_deleted"] is False


# ---------------------------------------------------------------------------
# Forget-on-delete
# ---------------------------------------------------------------------------
def test_record_that_left_the_scope_emits_a_hard_delete_marker():
    session = FakeFDASession([_label("set001")])
    state = {"known": ["set001@1", "setGONE@1"], "last_cursor": "20260101"}
    rows, state = _sync(session, state=state)

    assert rows == [{"id": "setGONE", "_deleted": True}]
    assert state["known"] == ["set001@1"]


def test_empty_window_does_not_mass_delete_and_preserves_state():
    session = FakeFDASession([])
    state = {"known": ["set001@1", "set002@1"], "last_cursor": "20260101"}
    rows, state = _sync(session, state=state)

    assert rows == []
    assert state["known"] == ["set001@1", "set002@1"]


def test_detect_deletions_off_walks_from_the_cursor():
    session = FakeFDASession(
        [
            _label("setOLD", effective_time="20260201"),
            _label("setNEW", effective_time="20260901"),
        ]
    )
    state = {"known": ["setOLD@1"], "last_cursor": "20260801"}
    rows, state = _sync(session, state=state, detect_deletions=False)

    # The walk started at the cursor, so the older label was never read and
    # must not be mistaken for deleted.
    assert [r["id"] for r in rows] == ["setNEW"]
    assert "setOLD@1" in state["known"]
    assert any("20260801 TO" in call["search"] for call in session.calls)


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------
def test_rate_limited_request_is_retried_honouring_retry_after(monkeypatch):
    requests = pytest.importorskip("requests")

    slept = []
    monkeypatch.setattr(
        "cognee_community_connector_openfda.openfda.time.sleep", lambda s: slept.append(s)
    )

    class RateLimitedOnce(FakeFDASession):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.hit = False

        def get(self, url, params=None):
            if not self.hit:
                self.hit = True
                response = requests.Response()
                response.status_code = 429
                response.headers["Retry-After"] = "5"
                raise requests.exceptions.HTTPError(response=response)
            return super().get(url, params)

    session = RateLimitedOnce([_label("set001")])
    rows, _ = _sync(session)

    assert [r["id"] for r in rows] == ["set001"]
    assert slept == [5.0]


def test_not_found_reads_as_no_matches(monkeypatch):
    # openFDA answers a search with no matches with a 404 rather than an empty
    # result set, so it must not surface as an error.
    requests = pytest.importorskip("requests")
    monkeypatch.setattr("cognee_community_connector_openfda.openfda.time.sleep", lambda s: None)

    class NoMatches(FakeFDASession):
        def get(self, url, params=None):
            response = requests.Response()
            response.status_code = 404
            raise requests.exceptions.HTTPError(response=response)

    rows, state = _sync(NoMatches([]))
    assert rows == []
    assert state["known"] == []


def test_bad_request_is_raised(monkeypatch):
    requests = pytest.importorskip("requests")
    monkeypatch.setattr("cognee_community_connector_openfda.openfda.time.sleep", lambda s: None)

    class BadRequest(FakeFDASession):
        def get(self, url, params=None):
            response = requests.Response()
            response.status_code = 400
            raise requests.exceptions.HTTPError(response=response)

    with pytest.raises(requests.exceptions.HTTPError):
        _sync(BadRequest([]))


def test_non_json_body_raises_a_clear_error():
    class HtmlSession(FakeFDASession):
        def get(self, url, params=None):
            return _Resp(None, text="<html>maintenance</html>")

    with pytest.raises(ValueError, match="non-JSON body"):
        _sync(HtmlSession([]))


def test_api_key_is_passed_through_when_given():
    session = FakeFDASession([_label("set001")])
    _sync(session, api_key="secret-key")
    assert all(call.get("api_key") == "secret-key" for call in session.calls)

    session = FakeFDASession([_label("set001")])
    _sync(session)
    assert all("api_key" not in call for call in session.calls)


def test_a_user_agent_is_always_sent():
    pytest.importorskip("requests")
    from cognee_community_connector_openfda.openfda import _make_session

    assert "cognee" in _make_session().headers["User-Agent"]
    assert _make_session("my-agent/1.0").headers["User-Agent"] == "my-agent/1.0"


# ---------------------------------------------------------------------------
# dlt wiring
# ---------------------------------------------------------------------------
def test_source_is_configured_for_merge_hard_delete_and_document_mode():
    pytest.importorskip("dlt")
    from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR

    resource = openfda_source(generic_name="ibuprofen", session=FakeFDASession([]))
    schema = resource.compute_table_schema()

    write_disposition = schema["write_disposition"]
    if isinstance(write_disposition, dict):
        write_disposition = write_disposition.get("disposition")
    assert write_disposition == "merge"

    columns = schema["columns"]
    assert columns["id"].get("primary_key") is True
    assert columns["_deleted"].get("hard_delete") is True
    assert columns["cognee_node_set"].get("data_type") == "json"

    assert getattr(resource, DOCUMENT_SOURCE_ATTR) == "openfda"


# ---------------------------------------------------------------------------
# End-to-end: a real dlt merge acts on the hard-delete marker
# ---------------------------------------------------------------------------
def test_forget_on_delete_end_to_end_through_a_real_dlt_merge(tmp_path):
    dlt = pytest.importorskip("dlt")
    pytest.importorskip("duckdb")

    pipeline = dlt.pipeline(
        pipeline_name="test_openfda_e2e",
        destination=dlt.destinations.duckdb(str(tmp_path / "fda.duckdb")),
        dataset_name="labels",
    )

    session1 = FakeFDASession([_label("set001"), _label("set002")])
    pipeline.run(
        openfda_source(
            generic_name="ibuprofen",
            effective_since="20260101",
            effective_until="20261231",
            session=session1,
        )
    )
    with pipeline.sql_client() as client:
        assert client.execute_sql("SELECT count(*) FROM openfda_records")[0][0] == 2

    # set002 has left the scope; its tombstone removes it on merge.
    session2 = FakeFDASession([_label("set001")])
    pipeline.run(
        openfda_source(
            generic_name="ibuprofen",
            effective_since="20260101",
            effective_until="20261231",
            session=session2,
        )
    )
    with pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id FROM openfda_records")
    assert [r[0] for r in rows] == ["set001"]
