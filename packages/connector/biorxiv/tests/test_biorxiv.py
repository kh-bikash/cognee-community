"""Unit tests for the bioRxiv / medRxiv connector.

The details API is fully mocked via ``FakeBioRxivSession``: no ``requests``
traffic and no network access are required, so these run in CI. The fake models
the parts of the contract the connector depends on, each checked against the
live API first: the cursor as a path segment advancing by the returned page
size, the ``"no posts found"`` status once the cursor passes the end, one entry
per version, and ``published`` holding the string ``"NA"`` rather than null.

Coverage:

  - posted_since is required, and the server name is validated
  - path-cursor paging walks the window and stops on "no posts found"
  - several versions of one DOI collapse to the highest version
  - an unchanged version is not re-emitted on the next run
  - a new version re-emits the row so merge replaces the old one
  - published "NA" is read as unpublished, not as a journal DOI
  - the published transition shows up as a changed row
  - category narrowing happens client-side
  - preprints that left the window become hard-delete markers
  - an empty window does not mass-delete, and preserves state
  - detect_deletions=False walks from the cursor instead of the whole window
  - node sets come from category, server and publication state
  - 429 is retried honouring Retry-After; 404 propagates; non-JSON raises
  - the dlt resource is wired with merge + id PK + hard_delete + document mode
  - a real dlt merge removes the marked row (end-to-end forget-on-delete)
"""

import pytest

from cognee_community_connector_biorxiv.biorxiv import (
    _content,
    _node_sets,
    _published_doi,
    biorxiv_source,
    sync_preprints,
)


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------
def _entry(
    doi,
    *,
    version=1,
    date="2026-10-01",
    title="A preprint about something",
    abstract="We show that the thing happens.",
    category="neuroscience",
    server="biorxiv",
    published="NA",
    authors="Harvey, D. Y.; Stoll, H.",
    corresponding="Denise Y Harvey",
    institution="University of Pennsylvania",
    entry_type="new results",
    license_name="cc_no",
):
    """Build a version entry in the shape the details endpoint returns."""
    return {
        "doi": doi,
        "title": title,
        "authors": authors,
        "author_corresponding": corresponding,
        "author_corresponding_institution": institution,
        "date": date,
        "version": str(version),
        "type": entry_type,
        "license": license_name,
        "category": category,
        "abstract": abstract,
        "published": published,
        "server": server,
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


class FakeBioRxivSession:
    """Minimal stand-in for a ``requests`` session hitting the details API."""

    def __init__(self, entries, page_size=30):
        self.entries = list(entries)
        self.page_size = page_size
        self.calls = []

    def get(self, url):
        self.calls.append(url)
        # .../details/{server}/{since}/{until}/{cursor}
        parts = url.rstrip("/").split("/")
        cursor = int(parts[-1])
        until = parts[-2]
        since = parts[-3]
        server = parts[-4]

        selected = [
            e for e in self.entries if e["server"] == server and since <= e["date"] <= until
        ]
        selected.sort(key=lambda e: (e["date"], e["doi"], int(e["version"])))
        window = selected[cursor : cursor + self.page_size]

        if not window:
            return _Resp({"messages": [{"status": "no posts found"}], "collection": []})
        return _Resp(
            {
                "messages": [
                    {
                        "status": "ok",
                        "category": "all",
                        "interval": f"{since}:{until}",
                        "cursor": str(cursor),
                        "count": len(window),
                        "total": str(len(selected)),
                    }
                ],
                "collection": window,
            }
        )


def _sync(session, state=None, **kwargs):
    """Run a sync pass and return (rows, state)."""
    state = {} if state is None else state
    kwargs.setdefault("posted_since", "2026-10-01")
    kwargs.setdefault("posted_until", "2026-10-31")
    rows = list(sync_preprints(session, state, **kwargs))
    return rows, state


# ---------------------------------------------------------------------------
# Scope / validation
# ---------------------------------------------------------------------------
def test_posted_since_is_required():
    pytest.importorskip("dlt")
    with pytest.raises(ValueError, match="posted_since"):
        biorxiv_source(posted_since="", session=object())


def test_server_name_is_validated():
    pytest.importorskip("dlt")
    with pytest.raises(ValueError, match="server must be one of"):
        biorxiv_source(posted_since="2026-10-01", server="arxiv", session=object())


def test_medrxiv_is_supported():
    session = FakeBioRxivSession([_entry("10.1/a", server="medrxiv")])
    rows, _ = _sync(session, server="medrxiv")
    assert [r["id"] for r in rows] == ["10.1/a"]


# ---------------------------------------------------------------------------
# Paging
# ---------------------------------------------------------------------------
def test_path_cursor_paging_walks_the_window():
    session = FakeBioRxivSession([_entry(f"10.1/p{i:03d}") for i in range(1, 8)], page_size=3)
    rows, state = _sync(session)

    assert len([r for r in rows if not r.get("_deleted")]) == 7
    assert len(state["known"]) == 7
    # The cursor is a path segment, and it advanced by the page size.
    assert session.calls[0].endswith("/0")
    assert any(url.endswith("/3") for url in session.calls)


def test_paging_stops_on_no_posts_found():
    session = FakeBioRxivSession([_entry("10.1/a")], page_size=1)
    rows, _ = _sync(session)

    assert len(rows) == 1
    # The last call returned the "no posts found" sentinel rather than looping.
    assert session.calls[-1].endswith("/1")


# ---------------------------------------------------------------------------
# Versions
# ---------------------------------------------------------------------------
def test_several_versions_collapse_to_the_highest():
    session = FakeBioRxivSession(
        [
            _entry("10.1/a", version=1, abstract="First draft."),
            _entry("10.1/a", version=3, abstract="Third draft."),
            _entry("10.1/a", version=2, abstract="Second draft."),
        ]
    )
    rows, state = _sync(session)

    assert len(rows) == 1
    assert "Third draft." in rows[0]["content"]
    assert "Version: 3" in rows[0]["content"]
    assert state["known"] == ["10.1/a@3"]


def test_unchanged_version_is_not_re_emitted():
    session = FakeBioRxivSession([_entry("10.1/a", version=2)])
    state = {"known": ["10.1/a@2"], "last_posted_date": "2026-10-01"}
    rows, state = _sync(session, state=state)

    assert rows == []
    assert state["known"] == ["10.1/a@2"]


def test_new_version_re_emits_the_row():
    session = FakeBioRxivSession([_entry("10.1/a", version=3, abstract="Revised.")])
    state = {"known": ["10.1/a@2"], "last_posted_date": "2026-10-01"}
    rows, state = _sync(session, state=state)

    assert [r["id"] for r in rows] == ["10.1/a"]
    assert "Revised." in rows[0]["content"]
    assert state["known"] == ["10.1/a@3"]


# ---------------------------------------------------------------------------
# The published field
# ---------------------------------------------------------------------------
def test_published_na_is_read_as_unpublished():
    # The API uses the string "NA", not null. Reading it naively would record a
    # journal DOI of "NA" and render a link to it.
    assert _published_doi(_entry("10.1/a", published="NA")) == ""
    assert _published_doi(_entry("10.1/a", published="")) == ""
    assert _published_doi(_entry("10.1/a", published="10.1038/s41586-026-1")) == (
        "10.1038/s41586-026-1"
    )

    text = _content(_entry("10.1/a", published="NA"))
    assert "Published in a journal: no" in text
    assert "NA" not in text


def test_publication_shows_up_as_a_changed_row():
    # A preprint that gets published gains a journal DOI on a new version, which
    # is a real reason to re-sync rather than treat the archive as append-only.
    session = FakeBioRxivSession([_entry("10.1/a", version=2, published="10.1038/s41586-026-1")])
    state = {"known": ["10.1/a@1"], "last_posted_date": "2026-10-01"}
    rows, _ = _sync(session, state=state)

    assert "Published in a journal: yes, DOI 10.1038/s41586-026-1" in rows[0]["content"]
    assert "state:published" in rows[0]["cognee_node_set"]


# ---------------------------------------------------------------------------
# Category narrowing
# ---------------------------------------------------------------------------
def test_category_is_narrowed_client_side():
    session = FakeBioRxivSession(
        [
            _entry("10.1/a", category="neuroscience"),
            _entry("10.1/b", category="genomics"),
        ]
    )
    rows, _ = _sync(session, category="Genomics")

    # Matching is case-insensitive, since the caller writes the label by hand.
    assert [r["id"] for r in rows] == ["10.1/b"]


# ---------------------------------------------------------------------------
# Forget-on-delete
# ---------------------------------------------------------------------------
def test_preprint_that_left_the_window_emits_a_hard_delete_marker():
    session = FakeBioRxivSession([_entry("10.1/a")])
    state = {"known": ["10.1/a@1", "10.1/gone@1"], "last_posted_date": "2026-10-01"}
    rows, state = _sync(session, state=state)

    assert rows == [{"id": "10.1/gone", "_deleted": True}]
    assert state["known"] == ["10.1/a@1"]


def test_empty_window_does_not_mass_delete_and_preserves_state():
    session = FakeBioRxivSession([])
    state = {"known": ["10.1/a@1", "10.1/b@1"], "last_posted_date": "2026-10-01"}
    rows, state = _sync(session, state=state)

    assert rows == []
    assert state["known"] == ["10.1/a@1", "10.1/b@1"]


def test_detect_deletions_off_walks_from_the_cursor():
    session = FakeBioRxivSession(
        [
            _entry("10.1/old", date="2026-10-02"),
            _entry("10.1/new", date="2026-10-20"),
        ]
    )
    state = {"known": ["10.1/old@1"], "last_posted_date": "2026-10-10"}
    rows, state = _sync(session, state=state, detect_deletions=False)

    # The walk started at the cursor, so the older preprint was never listed
    # and must not be mistaken for deleted.
    assert [r["id"] for r in rows] == ["10.1/new"]
    assert "10.1/old@1" in state["known"]
    assert any("/2026-10-10/" in url for url in session.calls)


def test_live_rows_carry_a_false_delete_marker():
    session = FakeBioRxivSession([_entry("10.1/a")])
    rows, _ = _sync(session)
    assert rows[0]["_deleted"] is False


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def test_content_renders_the_header_and_abstract():
    text = _content(
        _entry(
            "10.64898/2026.09.28.755022",
            version=2,
            title="The Role of the Left Inferior Frontal Gyrus",
            abstract="We tested whether stimulation improves resolution.",
            category="neuroscience",
        )
    )

    assert "DOI: 10.64898/2026.09.28.755022" in text
    assert "Server: biorxiv" in text
    assert "Category: neuroscience" in text
    assert "Posted: 2026-10-01" in text
    assert "Version: 2" in text
    assert "Type: new results" in text
    assert "Authors: Harvey, D. Y.; Stoll, H." in text
    assert "Corresponding author: Denise Y Harvey (University of Pennsylvania)" in text
    assert "We tested whether stimulation improves resolution." in text


def test_named_character_placeholders_are_decoded():
    # Abstracts carry special characters as {name} placeholders. A survey of 191
    # live abstracts found them in 22, and "{beta}-amyloid" is not the entity
    # "beta-amyloid" as far as extraction is concerned.
    entry = _entry(
        "10.1/a",
        title="{beta}-amyloid at 37{degrees}C",
        abstract="We measured 2{middle dot}08 million cells, {rho} = 0.4, CO{subscript 2} "
        "and a {superscript 2} exponent, cost {pound}5.",
    )
    text = _content(entry)

    assert "2·08 million" in text
    assert "ρ = 0.4" in text
    assert "CO₂" in text
    assert "² exponent" in text
    assert "£5" in text
    assert "{" not in text

    # Titles are a separate row field, not part of the rendered body, so they
    # are checked where they actually land.
    session = FakeBioRxivSession([entry])
    rows, _ = _sync(session)
    assert rows[0]["title"] == "β-amyloid at 37°C"


def test_unknown_placeholders_are_left_alone():
    # Braces also appear legitimately in formulae, so only known names decode.
    text = _content(_entry("10.1/a", abstract="The set {x_i} and {not_a_real_name} stay."))
    assert "{x_i}" in text
    assert "{not_a_real_name}" in text


def test_row_carries_the_document_contract():
    session = FakeBioRxivSession([_entry("10.1/a", title="A preprint")])
    rows, _ = _sync(session)
    row = rows[0]

    assert row["id"] == "10.1/a"
    assert row["title"] == "A preprint"
    assert row["url"] == "https://doi.org/10.1/a"


def test_node_sets_cover_category_server_and_state_in_stable_order():
    assert _node_sets(_entry("10.1/a", category="genomics", server="medrxiv")) == [
        "category:genomics",
        "server:medrxiv",
        "state:preprint",
    ]


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------
def test_rate_limited_request_is_retried_honouring_retry_after(monkeypatch):
    requests = pytest.importorskip("requests")

    slept = []
    monkeypatch.setattr(
        "cognee_community_connector_biorxiv.biorxiv.time.sleep", lambda s: slept.append(s)
    )

    class RateLimitedOnce(FakeBioRxivSession):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.hit = False

        def get(self, url):
            if not self.hit:
                self.hit = True
                response = requests.Response()
                response.status_code = 429
                response.headers["Retry-After"] = "2"
                raise requests.exceptions.HTTPError(response=response)
            return super().get(url)

    session = RateLimitedOnce([_entry("10.1/a")])
    rows, _ = _sync(session)

    assert [r["id"] for r in rows] == ["10.1/a"]
    assert slept == [2.0]


def test_not_found_is_not_retried(monkeypatch):
    requests = pytest.importorskip("requests")
    monkeypatch.setattr("cognee_community_connector_biorxiv.biorxiv.time.sleep", lambda s: None)

    class NotFound(FakeBioRxivSession):
        def get(self, url):
            response = requests.Response()
            response.status_code = 404
            raise requests.exceptions.HTTPError(response=response)

    with pytest.raises(requests.exceptions.HTTPError):
        _sync(NotFound([]))


def test_non_json_body_raises_a_clear_error():
    class HtmlSession(FakeBioRxivSession):
        def get(self, url):
            return _Resp(None, text="<html>maintenance</html>")

    with pytest.raises(ValueError, match="non-JSON body"):
        _sync(HtmlSession([]))


def test_a_user_agent_is_always_sent():
    pytest.importorskip("requests")
    from cognee_community_connector_biorxiv.biorxiv import _make_session

    assert "cognee" in _make_session().headers["User-Agent"]
    assert _make_session("my-agent/1.0").headers["User-Agent"] == "my-agent/1.0"


# ---------------------------------------------------------------------------
# dlt wiring
# ---------------------------------------------------------------------------
def test_source_is_configured_for_merge_hard_delete_and_document_mode():
    pytest.importorskip("dlt")
    from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR

    resource = biorxiv_source(posted_since="2026-10-01", session=FakeBioRxivSession([]))
    schema = resource.compute_table_schema()

    write_disposition = schema["write_disposition"]
    if isinstance(write_disposition, dict):
        write_disposition = write_disposition.get("disposition")
    assert write_disposition == "merge"

    columns = schema["columns"]
    assert columns["id"].get("primary_key") is True
    assert columns["_deleted"].get("hard_delete") is True
    assert columns["cognee_node_set"].get("data_type") == "json"

    assert getattr(resource, DOCUMENT_SOURCE_ATTR) == "biorxiv"


# ---------------------------------------------------------------------------
# End-to-end: a real dlt merge acts on the hard-delete marker
# ---------------------------------------------------------------------------
def test_forget_on_delete_end_to_end_through_a_real_dlt_merge(tmp_path):
    dlt = pytest.importorskip("dlt")
    pytest.importorskip("duckdb")

    pipeline = dlt.pipeline(
        pipeline_name="test_biorxiv_e2e",
        destination=dlt.destinations.duckdb(str(tmp_path / "bx.duckdb")),
        dataset_name="preprints",
    )

    session1 = FakeBioRxivSession([_entry("10.1/a"), _entry("10.1/b")])
    pipeline.run(
        biorxiv_source(posted_since="2026-10-01", posted_until="2026-10-31", session=session1)
    )
    with pipeline.sql_client() as client:
        assert client.execute_sql("SELECT count(*) FROM biorxiv_preprints")[0][0] == 2

    # 10.1/b has left the window; its tombstone removes it on merge.
    session2 = FakeBioRxivSession([_entry("10.1/a")])
    pipeline.run(
        biorxiv_source(posted_since="2026-10-01", posted_until="2026-10-31", session=session2)
    )
    with pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id FROM biorxiv_preprints")
    assert [r[0] for r in rows] == ["10.1/a"]
