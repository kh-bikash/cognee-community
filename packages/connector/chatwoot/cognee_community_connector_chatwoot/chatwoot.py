"""Chatwoot connector for cognee: a ``dlt`` source that turns a support inbox into memory.

Pull Chatwoot conversations into cognee, incrementally and with
forget-on-deletion, so you can "ask my support inbox". Like the sibling Confluence and
Google Drive connectors this builds entirely on the existing DLT ingestion
subsystem; the source produced here is handed directly to
:func:`cognee.remember`::

    import cognee
    from cognee_community_connector_chatwoot import chatwoot_source

    await cognee.remember(
        chatwoot_source(
            base_url="https://app.chatwoot.com",
            account_id=1,
            api_token="…",
        ),
        dataset_name="support_inbox",
        primary_key="id",
        write_disposition="merge",   # incremental upsert by conversation id
        max_rows_per_table=0,        # 0 = no row cap (see note below)
    )

Design
------
* **Auth:** a Chatwoot user API token from *Profile Settings*. Sent as
  ``Authorization: Bearer <token>`` (Chatwoot 4.19.0+); older self-hosted
  instances take the legacy ``api_access_token`` header via
  ``legacy_auth_header=True``. Access is read-only, since the connector only issues
  ``GET`` requests.
* **Document granularity:** one document per *conversation*, not per message.
  A lone support message is rarely meaningful ("yes, that worked"); the
  resolution only exists in the thread. Messages are rendered into a single
  speaker-labelled transcript so entity extraction can attribute who said what.
* **Document mode:** the source declares ``cognee_document_source =
  "chatwoot"``, so ``resolve_dlt_sources`` tags each row
  ``system_metadata["source"] = "chatwoot"`` and the transcript flows through
  the standard cognify entity-extraction pipeline, which is the right
  treatment for prose, instead of the deterministic dlt-row schema-context
  path.
* **Primary key:** the conversation ``id``. Combined with
  ``write_disposition="merge"`` this gives idempotent upserts.
* **Incremental cursor:** the conversation's ``last_activity_at`` (a Unix
  timestamp that Chatwoot bumps on every new message). Each run lists the
  current conversations and re-renders only those active since the highest
  timestamp seen so far; the cursor is persisted in dlt's per-resource state,
  so re-running ``remember`` resumes where it left off and re-embeds only the
  delta.
* **Forget-on-delete:** Chatwoot has no deletion feed, so each run compares
  the ids it just listed against the ids seen on the previous run (also kept in
  resource state). Conversations that vanished are emitted with the ``_deleted``
  hard-delete marker; dlt removes those rows on ``merge`` and cognee's existing
  ``orphan_cleanup`` then purges them from the graph + vector + relational
  stores. No parallel cleanup path.

What counts as knowledge
------------------------
``message_type`` is filtered deliberately, because a support inbox carries a
lot of
machine chatter that would dilute the graph. Incoming (0) and outgoing (1)
messages are the actual conversation and are kept; activity messages (2, e.g.
"Conversation was marked resolved") and templates (3, canned replies) are
dropped.

Private messages (``private: true``) are internal agent notes and *are*
ingested, since that is frequently where the real diagnosis is written down,
but
they are labelled as internal notes in the transcript so an answer can be
attributed to a note rather than to something the customer was told.

.. important::
   ``GET /api/v1/accounts/{id}/conversations`` defaults to ``status=open``. A
   connector that omits the parameter silently ingests only unresolved
   conversations and misses every resolved one, the exact subset with the most
   knowledge in it. This connector always sends an explicit ``status``, and
   defaults it to ``all``.

.. note::
   cognee's ``ingest_dlt_source`` reads at most ``max_rows_per_table`` rows
   from the dlt destination (default 50). For a real inbox pass
   ``max_rows_per_table=0`` (unlimited) so orphan-cleanup compares against the
   *whole* synced corpus rather than a truncated window.

.. note::
   Conversations are grouped into cognee node sets by inbox name and by label
   (``include_node_sets=True``), which makes "what do billing tickets say
   about refunds?" answerable without a separate dataset per inbox. cognee
   namespaces these names under the source tag, so they cannot collide with
   cognee's own node sets.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from typing import Any

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion.dlt_utils import (
    DOCUMENT_SOURCE_ATTR,
    NODE_SET_COLUMN,
    NODE_SET_COLUMN_HINT,
)

logger = get_logger("chatwoot_connector")

# Chatwoot's application API is versioned under this prefix on the instance.
_API_BASE = "/api/v1/accounts"

_SOURCE_NAME = "chatwoot"
_TABLE_NAME = "chatwoot_conversations"

# message_type values (Chatwoot's enum, not an HTTP status).
_MESSAGE_INCOMING = 0
_MESSAGE_OUTGOING = 1
# 2 = activity (status changes, assignment churn), 3 = template (canned replies).
# Both are machine chatter rather than knowledge, so they are dropped.
_KNOWLEDGE_MESSAGE_TYPES = frozenset({_MESSAGE_INCOMING, _MESSAGE_OUTGOING})

# `after=<id>` returns up to 100 messages ascending; `before` returns only 20,
# so the forward cursor is the cheaper way to walk a whole thread.
_MESSAGE_PAGE_LIMIT = 100

# Retry budget for rate-limited / transient Chatwoot responses.
_MAX_RETRIES = 5

# Hard ceiling on conversation list pages, so a server that ignores `page`
# cannot spin forever. 400 pages x 25 per page is ~10k conversations; the
# no-new-ids check below normally stops long before this.
_MAX_LIST_PAGES = 400

_EXTRA_HINT = (
    'The Chatwoot connector requires "requests". Install the package:\n'
    "    uv pip install cognee-community-connector-chatwoot"
)


# ---------------------------------------------------------------------------
# Auth / HTTP helpers
# ---------------------------------------------------------------------------
def _make_session(api_token: str, legacy_auth_header: bool = False) -> Any:
    """Build a ``requests`` session authenticated against Chatwoot.

    ``requests`` is imported lazily so importing this module never hard-fails
    when the optional dependency is missing.
    """
    try:
        import requests
    except ImportError as exc:  # pragma: no cover - depends on optional extra
        raise ImportError(_EXTRA_HINT) from exc

    session = requests.Session()
    # Bearer is the current scheme (Chatwoot >= 4.19.0). The api_access_token
    # header still works and is the only option on older self-hosted installs,
    # so it stays reachable rather than silently 401-ing those users.
    if legacy_auth_header:
        session.headers.update({"api_access_token": api_token})
    else:
        session.headers.update({"Authorization": f"Bearer {api_token}"})
    session.headers.update({"Accept": "application/json"})
    return session


def _api_get(session: Any, base_url: str, path: str, params: dict | None = None) -> Any:
    """GET a Chatwoot API path and return parsed JSON, retrying transient errors.

    Chatwoot rate-limits (429) and self-hosted instances behind a proxy return
    the usual 5xx blips; neither should abort a long inbox sync. Permanent
    errors (401/403/404) and an exhausted retry budget propagate so the caller
    decides.
    """
    url = f"{base_url}{path}"
    for attempt in range(_MAX_RETRIES):
        try:
            response = session.get(url, params=params or {})
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            if attempt == _MAX_RETRIES - 1 or not _is_transient(exc):
                raise
            delay = _retry_after(_response_headers(exc), attempt)
            logger.warning(
                "Chatwoot: %s, retrying in %.1fs (%d/%d).",
                exc,
                delay,
                attempt + 1,
                _MAX_RETRIES,
            )
            time.sleep(delay)


def _response_headers(exc: Exception) -> Any:
    """Headers from a ``requests`` HTTPError's response, when there is one."""
    response = getattr(exc, "response", None)
    return getattr(response, "headers", None)


def _status_code(exc: Exception) -> int | None:
    """HTTP status carried by a ``requests`` exception, when there is one."""
    response = getattr(exc, "response", None)
    return getattr(response, "status_code", None)


def _is_transient(exc: Exception) -> bool:
    """True for rate-limit / server / timeout / network errors worth retrying."""
    import requests

    if isinstance(exc, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
        return True
    if isinstance(exc, requests.exceptions.HTTPError):
        return _status_code(exc) in (429, 500, 502, 503, 504)
    return False


def _retry_after(headers: Any, attempt: int) -> float:
    """Seconds to wait before retrying: the Retry-After header, else backoff."""
    header = None
    if headers is not None:
        header = headers.get("Retry-After") or headers.get("retry-after")
    try:
        return float(header)
    except (TypeError, ValueError):
        return float(2**attempt)


# ---------------------------------------------------------------------------
# Chatwoot API reads
# ---------------------------------------------------------------------------
def _iter_conversations(
    session: Any,
    base_url: str,
    account_id: int | str,
    *,
    status: str = "all",
    inbox_id: int | None = None,
    labels: list[str] | None = None,
) -> Iterator[dict]:
    """Yield conversation objects across Chatwoot's page-numbered pagination.

    Stops on an empty payload, and also when a page introduces no conversation
    id it has not already yielded: a server that ignores ``page`` would
    otherwise return page 1 forever.
    """
    seen: set[str] = set()
    page = 1
    while page <= _MAX_LIST_PAGES:
        params: dict[str, Any] = {"status": status, "page": page}
        if inbox_id is not None:
            params["inbox_id"] = inbox_id
        if labels:
            # `labels` is an array query param: requests repeats a list value
            # (labels[]=a&labels[]=b). A comma-joined string would be read as
            # one literal label and match nothing.
            params["labels[]"] = list(labels)

        payload = _conversation_payload(
            _api_get(session, base_url, f"{_API_BASE}/{account_id}/conversations", params)
        )
        if not payload:
            return

        fresh = 0
        for conversation in payload:
            conversation_id = str(conversation.get("id") or "")
            if not conversation_id or conversation_id in seen:
                continue
            seen.add(conversation_id)
            fresh += 1
            yield conversation

        if not fresh:
            logger.warning(
                "Chatwoot: page %d returned no new conversation ids; stopping pagination.", page
            )
            return
        page += 1


def _conversation_payload(response: Any) -> list[dict]:
    """Pull the conversation array out of a list response.

    ``GET /conversations`` nests it as ``data.payload``, while the
    ``conversations/filter`` endpoint returns a top-level ``payload``. Both
    shapes are accepted so a caller can swap in a filtered listing.
    """
    if not isinstance(response, dict):
        return []
    data = response.get("data")
    if isinstance(data, dict) and isinstance(data.get("payload"), list):
        return data["payload"]
    if isinstance(response.get("payload"), list):
        return response["payload"]
    return []


def _iter_messages(
    session: Any, base_url: str, account_id: int | str, conversation_id: str
) -> Iterator[dict]:
    """Yield a conversation's messages oldest-first, walking the ``after`` cursor.

    The cursor starts at 0 rather than being omitted: with neither ``after`` nor
    ``before``, Chatwoot returns the *most recent* slice of the thread, so a
    first call without a cursor would hand back the tail and the follow-up
    ``after=<highest>`` would come back empty, quietly truncating every long
    conversation to its last page. ``after=0`` starts at the oldest message
    (ids are positive) and also raises the page size from 20 to 100.
    """
    after: int = 0
    while True:
        params = {"after": after}
        response = _api_get(
            session,
            base_url,
            f"{_API_BASE}/{account_id}/conversations/{conversation_id}/messages",
            params,
        )
        messages = response.get("payload") if isinstance(response, dict) else None
        if not messages:
            return

        highest = after
        for message in messages:
            yield message
            message_id = message.get("id")
            if isinstance(message_id, int) and message_id > highest:
                highest = message_id

        # A short page is the end of the thread. Guard on a non-advancing
        # cursor too, so a malformed response cannot loop forever.
        if len(messages) < _MESSAGE_PAGE_LIMIT or highest == after:
            return
        after = highest


def _inbox_names(session: Any, base_url: str, account_id: int | str) -> dict[str, str]:
    """Map inbox id -> inbox name, for node-set grouping and transcript headers.

    One extra request per sync. A failure here is not fatal: grouping by inbox
    is a convenience, so the sync degrades to labels-only rather than aborting.
    """
    try:
        response = _api_get(session, base_url, f"{_API_BASE}/{account_id}/inboxes")
    except Exception as exc:
        logger.warning("Chatwoot: could not list inboxes (%s); skipping inbox names.", exc)
        return {}

    payload = response.get("payload") if isinstance(response, dict) else None
    if not isinstance(payload, list):
        return {}
    return {
        str(inbox.get("id")): str(inbox.get("name") or "")
        for inbox in payload
        if isinstance(inbox, dict) and inbox.get("id") is not None
    }


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def _sender_name(message: dict) -> str:
    """Best available display name for a message's sender."""
    sender = message.get("sender")
    if isinstance(sender, dict):
        name = sender.get("name") or sender.get("available_name") or sender.get("email")
        if name:
            return str(name)
    return ""


def _speaker(message: dict) -> str:
    """Label a message with its role, so the transcript is attributable.

    Role comes from ``message_type`` (incoming = the customer, outgoing = the
    team) rather than from ``sender``, which is absent on some channels.
    """
    name = _sender_name(message)
    if message.get("private"):
        return f"Internal note ({name})" if name else "Internal note"
    if message.get("message_type") == _MESSAGE_INCOMING:
        return f"Customer ({name})" if name else "Customer"
    return f"Agent ({name})" if name else "Agent"


def _attachment_notes(message: dict) -> list[str]:
    """Describe a message's attachments without downloading them."""
    attachments = message.get("attachments")
    if not isinstance(attachments, list):
        return []
    notes = []
    for attachment in attachments:
        if not isinstance(attachment, dict):
            continue
        file_type = attachment.get("file_type") or "file"
        url = attachment.get("data_url") or attachment.get("thumb_url") or ""
        notes.append(f"[attachment: {file_type}{f' {url}' if url else ''}]")
    return notes


def _is_knowledge(message: dict, include_private_notes: bool) -> bool:
    """Whether a message carries knowledge worth putting in the graph."""
    if message.get("message_type") not in _KNOWLEDGE_MESSAGE_TYPES:
        return False
    if message.get("private"):
        return include_private_notes
    return True


def _render_transcript(messages: list[dict], include_private_notes: bool) -> str:
    """Render messages into a speaker-labelled transcript.

    A message with no text but an attachment is still rendered, so "they sent
    the log file" does not vanish from the thread.
    """
    lines: list[str] = []
    for message in messages:
        if not _is_knowledge(message, include_private_notes):
            continue
        text = str(message.get("content") or "").strip()
        parts = [text] if text else []
        parts.extend(_attachment_notes(message))
        if not parts:
            continue
        lines.append(f"{_speaker(message)}: {' '.join(parts)}")
    return "\n".join(lines)


def _format_timestamp(value: Any) -> str:
    """Format a Chatwoot Unix timestamp as UTC ISO-8601, or "" when unusable."""
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(value)))
    except (TypeError, ValueError, OSError, OverflowError):
        return ""


def _contact(conversation: dict) -> dict:
    """The conversation's contact (sender), from its ``meta`` block."""
    meta = conversation.get("meta")
    if isinstance(meta, dict) and isinstance(meta.get("sender"), dict):
        return meta["sender"]
    return {}


def _labels(conversation: dict) -> list[str]:
    """The conversation's labels, as a list of clean strings."""
    labels = conversation.get("labels")
    if not isinstance(labels, list):
        return []
    return [str(label).strip() for label in labels if str(label).strip()]


def _mail_subject(conversation: dict) -> str:
    """Subject line for email-channel conversations, when Chatwoot kept one."""
    extra = conversation.get("additional_attributes")
    if isinstance(extra, dict):
        subject = extra.get("mail_subject")
        if subject:
            return str(subject).strip()
    return ""


def _conversation_title(conversation: dict, contact_name: str) -> str:
    """Human title for the document.

    Email conversations have a real subject, so it is preferred; chat and
    social channels do not, and fall back to the contact's name.
    """
    conversation_id = conversation.get("id")
    subject = _mail_subject(conversation)
    if subject:
        return f"Chatwoot #{conversation_id}: {subject}"
    if contact_name:
        return f"Chatwoot conversation #{conversation_id} with {contact_name}"
    return f"Chatwoot conversation #{conversation_id}"


def _node_sets(conversation: dict, inbox_names: dict[str, str]) -> list[str] | None:
    """Node-set names for a conversation: its inbox and each of its labels.

    cognee namespaces these under the source tag, so they cannot collide with
    cognee's own node sets.
    """
    names: list[str] = []
    inbox_name = inbox_names.get(str(conversation.get("inbox_id") or ""))
    if inbox_name:
        names.append(f"inbox:{inbox_name}")
    names.extend(f"label:{label}" for label in _labels(conversation))
    # Deduplicate while keeping a deterministic order, so an unchanged
    # conversation keeps a stable content hash across runs.
    return sorted(set(names)) or None


def _conversation_to_row(
    conversation: dict,
    transcript: str,
    base_url: str,
    account_id: int | str,
    inbox_names: dict[str, str],
    include_node_sets: bool,
) -> dict[str, Any]:
    """Flatten a conversation + its rendered transcript into a document row.

    Document-mode row contract: ``{id, title, content, url}``.
    ``resolve_dlt_sources`` turns ``title``/``content`` into the document text
    and keeps ``url``/``id`` as provenance.
    """
    contact = _contact(conversation)
    contact_name = str(contact.get("name") or "").strip()
    contact_email = str(contact.get("email") or "").strip()
    conversation_id = str(conversation.get("id"))

    header: list[str] = []
    if conversation.get("status"):
        header.append(f"Status: {conversation['status']}")
    inbox_name = inbox_names.get(str(conversation.get("inbox_id") or ""))
    if inbox_name:
        header.append(f"Inbox: {inbox_name}")
    labels = _labels(conversation)
    if labels:
        header.append(f"Labels: {', '.join(labels)}")
    if contact_name or contact_email:
        who = contact_name or "Unknown"
        header.append(f"Contact: {who}{f' <{contact_email}>' if contact_email else ''}")
    opened = _format_timestamp(conversation.get("created_at"))
    if opened:
        header.append(f"Opened: {opened}")
    last_activity = _format_timestamp(conversation.get("last_activity_at"))
    if last_activity:
        header.append(f"Last activity: {last_activity}")

    content = "\n".join(header)
    if transcript:
        content = f"{content}\n\nTranscript:\n{transcript}" if content else transcript

    row: dict[str, Any] = {
        "id": conversation_id,
        "title": _conversation_title(conversation, contact_name),
        "url": f"{base_url}/app/accounts/{account_id}/conversations/{conversation_id}",
        "content": content,
        # Hard-delete marker (always False for live conversations). Vanished
        # conversations are emitted separately with _deleted=True.
        "_deleted": False,
    }
    if include_node_sets:
        node_sets = _node_sets(conversation, inbox_names)
        if node_sets:
            row[NODE_SET_COLUMN] = node_sets
    return row


def _deleted_row(conversation_id: str) -> dict[str, Any]:
    """Build a minimal row that instructs dlt to hard-delete a conversation."""
    return {"id": str(conversation_id), "_deleted": True}


# ---------------------------------------------------------------------------
# Sync (pure given a session + state dict, so it is unit-testable)
# ---------------------------------------------------------------------------
def sync_conversations(
    session: Any,
    base_url: str,
    account_id: int | str,
    state: dict,
    *,
    status: str = "all",
    inbox_ids: list[int] | None = None,
    labels: list[str] | None = None,
    include_private_notes: bool = True,
    include_node_sets: bool = True,
) -> Iterator[dict[str, Any]]:
    """Yield conversations active since the last run, plus hard-delete markers.

    One listing pass enumerates the *current* conversations (cheap, since the list
    endpoint does not return full transcripts): that set drives deletion
    detection, while conversations whose ``last_activity_at`` is newer than the
    stored cursor have their messages fetched and rendered. The cursor
    (``last_activity_at``) and the id set (``known_ids``) are advanced in
    ``state`` so the next run is a no-op when nothing changed.
    """
    known_ids: set[str] = set(state.get("known_ids", []))
    last_activity_at: float = float(state.get("last_activity_at") or 0)
    newest_activity = last_activity_at
    current_ids: set[str] = set()
    changed = 0

    # Fetched regardless of include_node_sets: the transcript header names the
    # inbox too, and it is a single request per sync.
    inbox_names = _inbox_names(session, base_url, account_id)
    # A single `inbox_id` is all the API takes, so several inboxes mean several
    # listing passes; `None` lets Chatwoot return every inbox the token can see.
    scopes: list[int | None] = list(inbox_ids) if inbox_ids else [None]

    for inbox_id in scopes:
        for conversation in _iter_conversations(
            session,
            base_url,
            account_id,
            status=status,
            inbox_id=inbox_id,
            labels=labels,
        ):
            conversation_id = str(conversation.get("id"))
            if conversation_id in current_ids:
                # Overlapping inbox scopes can list the same conversation twice.
                continue
            current_ids.add(conversation_id)

            activity = _activity_at(conversation)
            # Skip only conversations we have already ingested that have not
            # moved. One new to the corpus is rendered regardless of its
            # timestamp, so conversations that predate the cursor (a
            # newly-granted inbox, a widened label filter, a tie exactly at the
            # cursor boundary) are not lost.
            if conversation_id in known_ids and activity <= last_activity_at:
                continue
            newest_activity = max(newest_activity, activity)

            messages = list(_iter_messages(session, base_url, account_id, conversation_id))
            transcript = _render_transcript(messages, include_private_notes)
            yield _conversation_to_row(
                conversation,
                transcript,
                base_url,
                account_id,
                inbox_names,
                include_node_sets,
            )
            changed += 1

    # Deletion detection relies on the sweep enumerating every current
    # conversation. An empty sweep while conversations were previously known
    # almost always means a transient/failed listing (network blip, a token
    # that lost inbox access, a momentary empty page) rather than a genuine
    # wipe. Treating it as "all deleted" would purge the whole dataset and
    # overwrite known_ids with [], making the loss permanent. Skip deletion and
    # preserve state in that case.
    if known_ids and not current_ids:
        logger.warning(
            "Chatwoot: conversation sweep returned 0 conversations but %d were known; "
            "skipping deletion this run to avoid a mass forget-on-delete on a transient sweep.",
            len(known_ids),
        )
        state["last_activity_at"] = newest_activity
        logger.info("Chatwoot: %d changed conversation(s), 0 deletion(s).", changed)
        return

    deleted = known_ids - current_ids
    for conversation_id in sorted(deleted):
        yield _deleted_row(conversation_id)

    state["known_ids"] = sorted(current_ids)
    state["last_activity_at"] = newest_activity
    logger.info("Chatwoot: %d changed conversation(s), %d deletion(s).", changed, len(deleted))


def _activity_at(conversation: dict) -> float:
    """The conversation's incremental cursor value.

    ``last_activity_at`` is the field Chatwoot bumps on every new message.
    ``timestamp`` and ``created_at`` are accepted as fallbacks so a response
    that omits it still sorts sensibly instead of collapsing to 0.
    """
    for key in ("last_activity_at", "timestamp", "created_at"):
        try:
            value = conversation.get(key)
            if value is not None:
                return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------
def chatwoot_source(
    *,
    base_url: str,
    account_id: int | str,
    api_token: str | None = None,
    status: str = "all",
    inbox_ids: list[int] | None = None,
    labels: list[str] | None = None,
    include_private_notes: bool = True,
    include_node_sets: bool = True,
    legacy_auth_header: bool = False,
    session: Any = None,
):
    """Return a ``dlt`` resource that yields Chatwoot conversations for ``remember``.

    Args:
        base_url: Instance base URL, e.g. ``https://app.chatwoot.com`` or your
            self-hosted origin.
        account_id: Chatwoot account id (the ``/app/accounts/<id>/`` segment).
        api_token: User API token from Profile Settings. Falls back to
            ``CHATWOOT_API_TOKEN``.
        status: Conversation status filter: ``"all"`` (default), ``"open"``,
            ``"resolved"``, ``"pending"`` or ``"snoozed"``. Chatwoot's own
            default is ``"open"``, which would hide resolved threads, so this
            connector always sends the value explicitly.
        inbox_ids: Restrict to these inbox ids. ``None`` syncs every inbox the
            token can read.
        labels: Restrict to conversations carrying these labels.
        include_private_notes: Ingest internal agent notes (``private: true``).
        include_node_sets: Group conversations into node sets by inbox and
            label.
        legacy_auth_header: Send the token as ``api_access_token`` instead of
            ``Authorization: Bearer``. Needed for Chatwoot below 4.19.0.
        session: Pre-built ``requests`` session. Mainly an injection point for
            tests; when omitted one is built from ``api_token``.

    Returns:
        A ``dlt`` resource (``chatwoot_conversations``) configured with
        ``primary_key="id"``, ``write_disposition="merge"`` and an ``_deleted``
        hard-delete column, tagged for cognee's document path. Hand it to
        ``cognee.remember(...)``.
    """
    try:
        import dlt
    except ImportError as exc:
        raise ImportError(
            'The Chatwoot connector requires the dlt extra: pip install "cognee[dlt]".'
        ) from exc

    base_url = base_url.rstrip("/")
    resolved_token = api_token or os.environ.get("CHATWOOT_API_TOKEN")
    if session is None and not resolved_token:
        raise ValueError(
            "chatwoot_source requires an api_token (or CHATWOOT_API_TOKEN), or an injected session."
        )
    if account_id is None or str(account_id).strip() == "":
        raise ValueError("chatwoot_source requires an account_id.")

    @dlt.resource(
        name=_TABLE_NAME,
        primary_key="id",
        write_disposition="merge",
        # _deleted is a boolean hard-delete marker: rows where it is True are
        # removed from the dlt destination on merge, which propagates the
        # deletion through cognee's orphan_cleanup. The node-set column needs
        # its json hint so dlt stores the list on the row instead of
        # normalizing it into a child table.
        columns={
            "_deleted": {"data_type": "bool", "hard_delete": True},
            **NODE_SET_COLUMN_HINT,
        },
    )
    def chatwoot_conversations():
        client = session or _make_session(resolved_token, legacy_auth_header)
        resource_state = dlt.current.resource_state()
        yield from sync_conversations(
            client,
            base_url,
            account_id,
            resource_state,
            status=status,
            inbox_ids=inbox_ids,
            labels=labels,
            include_private_notes=include_private_notes,
            include_node_sets=include_node_sets,
        )

    resource = chatwoot_conversations()
    # Opt into the document ingestion path (conversation -> transcript document
    # -> cognify entity extraction). resolve_dlt_sources reads this marker; it
    # never imports this connector. Document mode supports the incremental
    # model used here: write_disposition="merge" plus the _deleted hard-delete
    # tombstones emitted by the sweep above.
    setattr(resource, DOCUMENT_SOURCE_ATTR, _SOURCE_NAME)
    return resource
