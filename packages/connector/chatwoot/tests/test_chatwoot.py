"""Unit tests for the Chatwoot connector.

The Chatwoot application API is fully mocked via ``FakeChatwootSession``: no
``requests`` traffic and no live credentials are required, so these run in CI.
Coverage:

  - activity and template messages are dropped; customer/agent/private notes render
  - private notes are labelled as internal, and can be excluded
  - a message with only an attachment still appears in the transcript
  - ``status=all`` is always sent (Chatwoot's default of ``open`` would hide
    resolved conversations, which is where most of the knowledge is)
  - label filters are pushed down as a repeated array param
  - conversation pagination walks pages, and stops if a server ignores ``page``
  - message pagination walks the ``after`` cursor across page boundaries
  - full backfill yields every conversation and records the cursor + id set
  - incremental re-sync yields ONLY conversations active since the cursor
  - conversations that vanish from the sweep become hard-delete markers
  - an empty sweep does not mass-delete, and preserves state
  - a conversation new to the corpus is ingested even below the cursor
  - overlapping inbox scopes do not emit a conversation twice
  - node sets are derived from inbox + labels, deterministically ordered
  - rate-limit (429) responses are retried honouring Retry-After; 401 propagates
  - the dlt resource is wired with merge + id PK + hard_delete + document mode
  - a real dlt merge removes the marked row (end-to-end forget-on-delete)

The end-to-end "deletion removes it from memory" guarantee comes from cognee's
existing ``orphan_cleanup`` path; here we prove the connector emits the markers
that drive it, and that dlt acts on them.
"""

import pytest

from cognee_community_connector_chatwoot.chatwoot import (
    _activity_at,
    _conversation_title,
    _node_sets,
    _render_transcript,
    _speaker,
    chatwoot_source,
    sync_conversations,
)

BASE_URL = "https://support.example.com"
ACCOUNT_ID = 1

# Chatwoot message_type values.
INCOMING = 0
OUTGOING = 1
ACTIVITY = 2
TEMPLATE = 3


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------
def _conversation(
    conversation_id,
    *,
    last_activity_at,
    status="resolved",
    inbox_id=1,
    labels=None,
    contact_name="Jane Doe",
    contact_email="jane@example.com",
    created_at=1_700_000_000,
    mail_subject=None,
):
    """Build a conversation as the list endpoint returns it."""
    conversation = {
        "id": conversation_id,
        "status": status,
        "inbox_id": inbox_id,
        "labels": labels or [],
        "created_at": created_at,
        "last_activity_at": last_activity_at,
        "meta": {"sender": {"name": contact_name, "email": contact_email}},
    }
    if mail_subject:
        conversation["additional_attributes"] = {"mail_subject": mail_subject}
    return conversation


def _message(
    message_id,
    *,
    content="",
    message_type=INCOMING,
    private=False,
    sender_name="Jane Doe",
    attachments=None,
):
    """Build a message as the messages endpoint returns it."""
    return {
        "id": message_id,
        "content": content,
        "message_type": message_type,
        "private": private,
        "created_at": 1_700_000_000 + message_id,
        "sender": {"name": sender_name},
        "attachments": attachments or [],
    }


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class FakeChatwootSession:
    """Minimal stand-in for a ``requests`` session hitting Chatwoot."""

    def __init__(self, conversations, messages=None, inboxes=None, page_size=25):
        self.conversations = conversations
        self.messages = messages or {}
        self.inboxes = inboxes if inboxes is not None else [{"id": 1, "name": "Billing"}]
        self.page_size = page_size
        self.calls = []

    def get(self, url, params=None):
        params = params or {}
        self.calls.append((url, params))

        if url.endswith("/inboxes"):
            return _Resp({"payload": self.inboxes})

        if url.endswith("/messages"):
            conversation_id = url.split("/conversations/")[1].split("/messages")[0]
            messages = self.messages.get(str(conversation_id), [])
            after = params.get("after")
            if after is not None:
                messages = [m for m in messages if m["id"] > after]
            return _Resp({"payload": messages[:100]})

        if url.endswith("/conversations"):
            # Chatwoot defaults to status=open; the connector must always be
            # explicit, so the fake refuses to guess.
            assert "status" in params, "the connector must send an explicit status"
            selected = self.conversations
            if params.get("inbox_id") is not None:
                selected = [c for c in selected if c["inbox_id"] == params["inbox_id"]]
            if params.get("labels[]"):
                wanted = params["labels[]"]
                assert isinstance(wanted, list), f"labels must be a list, got {wanted!r}"
                selected = [c for c in selected if set(c["labels"]) & set(wanted)]
            if params.get("status") not in (None, "all"):
                selected = [c for c in selected if c["status"] == params["status"]]

            page = int(params.get("page", 1))
            start = (page - 1) * self.page_size
            window = selected[start : start + self.page_size]
            return _Resp({"data": {"meta": {"all_count": len(selected)}, "payload": window}})

        raise AssertionError(f"unexpected URL: {url}")


def _sync(session, state=None, **kwargs):
    """Run a sync pass and return (rows, state)."""
    state = {} if state is None else state
    rows = list(sync_conversations(session, BASE_URL, ACCOUNT_ID, state, **kwargs))
    return rows, state


# ---------------------------------------------------------------------------
# Rendering / filtering
# ---------------------------------------------------------------------------
def test_activity_and_template_messages_are_dropped():
    transcript = _render_transcript(
        [
            _message(1, content="My invoice is doubled", message_type=INCOMING),
            _message(2, content="Conversation was marked resolved", message_type=ACTIVITY),
            _message(3, content="Thanks for contacting us!", message_type=TEMPLATE),
            _message(4, content="Refunded the duplicate", message_type=OUTGOING, sender_name="Sam"),
        ],
        include_private_notes=True,
    )

    assert "My invoice is doubled" in transcript
    assert "Refunded the duplicate" in transcript
    assert "marked resolved" not in transcript
    assert "Thanks for contacting us" not in transcript


def test_private_notes_are_labelled_internal_and_can_be_excluded():
    messages = [
        _message(1, content="Customer asked about billing", message_type=INCOMING),
        _message(
            2,
            content="Root cause was the webhook retry",
            message_type=OUTGOING,
            private=True,
            sender_name="Sam",
        ),
    ]

    included = _render_transcript(messages, include_private_notes=True)
    assert "Internal note (Sam): Root cause was the webhook retry" in included

    excluded = _render_transcript(messages, include_private_notes=False)
    assert "webhook retry" not in excluded
    assert "Customer asked about billing" in excluded


def test_speaker_labels_use_message_type_not_sender():
    assert _speaker(_message(1, message_type=INCOMING, sender_name="Jane")) == "Customer (Jane)"
    assert _speaker(_message(2, message_type=OUTGOING, sender_name="Sam")) == "Agent (Sam)"
    assert _speaker(_message(3, message_type=OUTGOING, private=True, sender_name="Sam")) == (
        "Internal note (Sam)"
    )
    # Some channels omit the sender entirely; the role must still be stated.
    assert _speaker({"message_type": INCOMING}) == "Customer"


def test_attachment_only_message_still_appears():
    transcript = _render_transcript(
        [
            _message(
                1,
                content="",
                message_type=INCOMING,
                attachments=[{"file_type": "image", "data_url": "https://cdn/x.png"}],
            )
        ],
        include_private_notes=True,
    )
    assert "[attachment: image https://cdn/x.png]" in transcript


def test_email_subject_is_preferred_as_the_title():
    with_subject = _conversation(7, last_activity_at=10, mail_subject="Duplicate charge")
    assert _conversation_title(with_subject, "Jane Doe") == "Chatwoot #7: Duplicate charge"

    without_subject = _conversation(7, last_activity_at=10)
    assert _conversation_title(without_subject, "Jane Doe") == (
        "Chatwoot conversation #7 with Jane Doe"
    )


def test_activity_at_falls_back_when_last_activity_is_missing():
    assert _activity_at({"last_activity_at": 50, "timestamp": 10}) == 50
    assert _activity_at({"timestamp": 10}) == 10
    assert _activity_at({"created_at": 5}) == 5
    assert _activity_at({}) == 0.0


def test_node_sets_cover_inbox_and_labels_in_stable_order():
    conversation = _conversation(1, last_activity_at=10, labels=["refund", "billing"])
    assert _node_sets(conversation, {"1": "Billing"}) == [
        "inbox:Billing",
        "label:billing",
        "label:refund",
    ]
    # No inbox name resolved and no labels → nothing to group by.
    assert _node_sets(_conversation(1, last_activity_at=10, labels=[]), {}) is None


# ---------------------------------------------------------------------------
# Request shaping
# ---------------------------------------------------------------------------
def test_status_all_is_sent_so_resolved_conversations_are_not_missed():
    session = FakeChatwootSession(
        [
            _conversation(1, last_activity_at=10, status="open"),
            _conversation(2, last_activity_at=20, status="resolved"),
        ],
        messages={"1": [_message(1, content="a")], "2": [_message(2, content="b")]},
    )
    rows, _ = _sync(session)

    assert {row["id"] for row in rows} == {"1", "2"}
    list_calls = [params for url, params in session.calls if url.endswith("/conversations")]
    assert all(params["status"] == "all" for params in list_calls)


def test_label_filter_is_pushed_down_as_a_repeated_array_param():
    session = FakeChatwootSession(
        [
            _conversation(1, last_activity_at=10, labels=["billing"]),
            _conversation(2, last_activity_at=20, labels=["shipping"]),
        ],
        messages={"1": [_message(1, content="a")], "2": [_message(2, content="b")]},
    )
    rows, _ = _sync(session, labels=["billing"])

    assert [row["id"] for row in rows] == ["1"]


def test_conversation_pagination_walks_every_page():
    conversations = [_conversation(i, last_activity_at=i) for i in range(1, 8)]
    session = FakeChatwootSession(
        conversations,
        messages={str(i): [_message(i, content=f"m{i}")] for i in range(1, 8)},
        page_size=3,
    )
    rows, state = _sync(session)

    assert len([r for r in rows if not r.get("_deleted")]) == 7
    assert len(state["known_ids"]) == 7


def test_pagination_stops_when_the_server_ignores_the_page_param():
    class StuckSession(FakeChatwootSession):
        def get(self, url, params=None):
            params = dict(params or {})
            if url.endswith("/conversations"):
                params["page"] = 1  # always serve page 1
            return super().get(url, params)

    session = StuckSession(
        [_conversation(i, last_activity_at=i) for i in range(1, 4)],
        messages={str(i): [_message(i, content="m")] for i in range(1, 4)},
        page_size=3,
    )
    rows, _ = _sync(session)

    # Three conversations, each yielded once, and no infinite loop.
    assert len(rows) == 3


def test_message_pagination_walks_the_after_cursor_past_a_full_page():
    # 150 messages forces a second request: `after` returns at most 100.
    messages = [_message(i, content=f"line {i}") for i in range(1, 151)]
    session = FakeChatwootSession([_conversation(1, last_activity_at=10)], messages={"1": messages})
    rows, _ = _sync(session)

    assert "line 1" in rows[0]["content"]
    assert "line 150" in rows[0]["content"]
    message_calls = [p for url, p in session.calls if url.endswith("/messages")]
    assert len(message_calls) >= 2


# ---------------------------------------------------------------------------
# Incremental sync
# ---------------------------------------------------------------------------
def test_backfill_yields_all_conversations_and_records_cursor_and_ids():
    session = FakeChatwootSession(
        [_conversation(1, last_activity_at=100), _conversation(2, last_activity_at=200)],
        messages={"1": [_message(1, content="a")], "2": [_message(2, content="b")]},
    )
    rows, state = _sync(session)

    assert {row["id"] for row in rows} == {"1", "2"}
    assert state["last_activity_at"] == 200
    assert state["known_ids"] == ["1", "2"]
    assert rows[0]["url"] == f"{BASE_URL}/app/accounts/{ACCOUNT_ID}/conversations/1"


def test_incremental_yields_only_conversations_active_since_the_cursor():
    session = FakeChatwootSession(
        [_conversation(1, last_activity_at=100), _conversation(2, last_activity_at=300)],
        messages={"1": [_message(1, content="a")], "2": [_message(2, content="b")]},
    )
    state = {"known_ids": ["1", "2"], "last_activity_at": 200}
    rows, state = _sync(session, state=state)

    assert [row["id"] for row in rows] == ["2"]
    assert state["last_activity_at"] == 300


def test_incremental_with_no_changes_is_a_noop():
    session = FakeChatwootSession(
        [_conversation(1, last_activity_at=100)], messages={"1": [_message(1, content="a")]}
    )
    state = {"known_ids": ["1"], "last_activity_at": 100}
    rows, state = _sync(session, state=state)

    assert rows == []
    assert state["known_ids"] == ["1"]
    assert state["last_activity_at"] == 100


def test_conversation_new_to_the_corpus_is_ingested_even_below_the_cursor():
    session = FakeChatwootSession(
        [_conversation(1, last_activity_at=100), _conversation(2, last_activity_at=50)],
        messages={"1": [_message(1, content="a")], "2": [_message(2, content="b")]},
    )
    # The cursor is past conversation 2, but 2 was never ingested before.
    state = {"known_ids": ["1"], "last_activity_at": 100}
    rows, _ = _sync(session, state=state)

    assert [row["id"] for row in rows] == ["2"]


def test_overlapping_inbox_scopes_do_not_duplicate_a_conversation():
    session = FakeChatwootSession(
        [_conversation(1, last_activity_at=10, inbox_id=1)],
        messages={"1": [_message(1, content="a")]},
        inboxes=[{"id": 1, "name": "Billing"}],
    )
    rows, _ = _sync(session, inbox_ids=[1, 1])

    assert [row["id"] for row in rows] == ["1"]


# ---------------------------------------------------------------------------
# Forget-on-delete
# ---------------------------------------------------------------------------
def test_deleted_conversation_emits_a_hard_delete_marker():
    session = FakeChatwootSession(
        [_conversation(1, last_activity_at=100)], messages={"1": [_message(1, content="a")]}
    )
    state = {"known_ids": ["1", "2"], "last_activity_at": 100}
    rows, state = _sync(session, state=state)

    assert rows == [{"id": "2", "_deleted": True}]
    assert state["known_ids"] == ["1"]


def test_empty_sweep_does_not_mass_delete_and_preserves_state():
    session = FakeChatwootSession([], messages={})
    state = {"known_ids": ["1", "2", "3"], "last_activity_at": 100}
    rows, state = _sync(session, state=state)

    assert rows == []  # no tombstones from an empty sweep
    assert state["known_ids"] == ["1", "2", "3"]  # id set preserved


def test_live_rows_carry_a_false_delete_marker():
    session = FakeChatwootSession(
        [_conversation(1, last_activity_at=10)], messages={"1": [_message(1, content="a")]}
    )
    rows, _ = _sync(session)

    assert rows[0]["_deleted"] is False


# ---------------------------------------------------------------------------
# Transport: retries and hard failures
# ---------------------------------------------------------------------------
def test_rate_limited_request_is_retried_honouring_retry_after(monkeypatch):
    requests = pytest.importorskip("requests")

    slept = []
    monkeypatch.setattr(
        "cognee_community_connector_chatwoot.chatwoot.time.sleep", lambda s: slept.append(s)
    )

    class RateLimitedOnce(FakeChatwootSession):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.rate_limited = False

        def get(self, url, params=None):
            if url.endswith("/conversations") and not self.rate_limited:
                self.rate_limited = True
                response = requests.Response()
                response.status_code = 429
                response.headers["Retry-After"] = "7"
                raise requests.exceptions.HTTPError(response=response)
            return super().get(url, params)

    session = RateLimitedOnce(
        [_conversation(1, last_activity_at=10)], messages={"1": [_message(1, content="a")]}
    )
    rows, _ = _sync(session)

    assert [row["id"] for row in rows] == ["1"]
    assert slept == [7.0]


def test_unauthorized_request_is_not_retried(monkeypatch):
    requests = pytest.importorskip("requests")
    monkeypatch.setattr("cognee_community_connector_chatwoot.chatwoot.time.sleep", lambda s: None)

    class Unauthorized(FakeChatwootSession):
        def get(self, url, params=None):
            if url.endswith("/conversations"):
                response = requests.Response()
                response.status_code = 401
                raise requests.exceptions.HTTPError(response=response)
            return super().get(url, params)

    with pytest.raises(requests.exceptions.HTTPError):
        _sync(Unauthorized([], messages={}))


def test_inbox_listing_failure_degrades_instead_of_aborting(monkeypatch):
    requests = pytest.importorskip("requests")
    monkeypatch.setattr("cognee_community_connector_chatwoot.chatwoot.time.sleep", lambda s: None)

    class NoInboxes(FakeChatwootSession):
        def get(self, url, params=None):
            if url.endswith("/inboxes"):
                response = requests.Response()
                response.status_code = 403
                raise requests.exceptions.HTTPError(response=response)
            return super().get(url, params)

    session = NoInboxes(
        [_conversation(1, last_activity_at=10, labels=["billing"])],
        messages={"1": [_message(1, content="a")]},
    )
    rows, _ = _sync(session)

    # The sync still produces the conversation; only inbox grouping is lost.
    assert [row["id"] for row in rows] == ["1"]
    assert rows[0]["cognee_node_set"] == ["label:billing"]


# ---------------------------------------------------------------------------
# dlt wiring
# ---------------------------------------------------------------------------
def test_chatwoot_source_is_configured_for_merge_hard_delete_and_document_mode():
    pytest.importorskip("dlt")
    from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR

    resource = chatwoot_source(
        base_url=BASE_URL, account_id=ACCOUNT_ID, session=FakeChatwootSession([], messages={})
    )
    schema = resource.compute_table_schema()

    write_disposition = schema["write_disposition"]
    if isinstance(write_disposition, dict):
        write_disposition = write_disposition.get("disposition")
    assert write_disposition == "merge"

    columns = schema["columns"]
    assert columns["id"].get("primary_key") is True
    assert columns["_deleted"].get("hard_delete") is True
    assert columns["cognee_node_set"].get("data_type") == "json"

    # Document mode: transcripts go through cognify entity extraction.
    assert getattr(resource, DOCUMENT_SOURCE_ATTR) == "chatwoot"


def test_chatwoot_source_requires_a_token_or_session():
    pytest.importorskip("dlt")
    with pytest.raises(ValueError, match="api_token"):
        chatwoot_source(base_url=BASE_URL, account_id=ACCOUNT_ID)


def test_chatwoot_source_requires_an_account_id():
    pytest.importorskip("dlt")
    with pytest.raises(ValueError, match="account_id"):
        chatwoot_source(base_url=BASE_URL, account_id="", session=object())


def test_token_is_sent_as_bearer_by_default_and_legacy_header_on_request():
    pytest.importorskip("requests")
    from cognee_community_connector_chatwoot.chatwoot import _make_session

    assert _make_session("tok").headers["Authorization"] == "Bearer tok"
    assert _make_session("tok", legacy_auth_header=True).headers["api_access_token"] == "tok"


# ---------------------------------------------------------------------------
# End-to-end: a real dlt merge acts on the hard-delete marker
# ---------------------------------------------------------------------------
def test_forget_on_delete_end_to_end_through_a_real_dlt_merge(tmp_path):
    dlt = pytest.importorskip("dlt")
    pytest.importorskip("duckdb")

    pipeline = dlt.pipeline(
        pipeline_name="test_chatwoot_e2e",
        destination=dlt.destinations.duckdb(str(tmp_path / "chatwoot.duckdb")),
        dataset_name="support",
    )

    # Sync #1: two live conversations land in the destination.
    session1 = FakeChatwootSession(
        [_conversation(1, last_activity_at=100), _conversation(2, last_activity_at=200)],
        messages={"1": [_message(1, content="a")], "2": [_message(2, content="b")]},
    )
    pipeline.run(chatwoot_source(base_url=BASE_URL, account_id=ACCOUNT_ID, session=session1))
    with pipeline.sql_client() as client:
        assert client.execute_sql("SELECT count(*) FROM chatwoot_conversations")[0][0] == 2

    # Sync #2: conversation 2 is deleted upstream. The connector emits a
    # hard-delete marker for it; dlt's merge removes it from the destination.
    session2 = FakeChatwootSession(
        [_conversation(1, last_activity_at=100)], messages={"1": [_message(1, content="a")]}
    )
    pipeline.run(chatwoot_source(base_url=BASE_URL, account_id=ACCOUNT_ID, session=session2))
    with pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id FROM chatwoot_conversations")
    assert [r[0] for r in rows] == ["1"]  # 2 forgotten, 1 retained
