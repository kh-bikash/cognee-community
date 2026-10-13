"""Federal Register connector for cognee: a ``dlt`` source that turns the US
regulatory record into memory.

Pull Federal Register documents (rules, proposed rules, notices and
presidential documents) into cognee, incrementally and with forget-on-deletion,
so a compliance or policy team can ask "what changed for us this month, and
which agency changed it?". Like the sibling Confluence and Google Drive
connectors this builds entirely on the existing DLT ingestion subsystem; the
source produced here is handed directly to :func:`cognee.remember`::

    import cognee
    from cognee_community_connector_federalregister import federalregister_source

    await cognee.remember(
        federalregister_source(
            published_since="2026-10-01",
            agencies=["environmental-protection-agency"],
        ),
        dataset_name="regulations",
        primary_key="id",
        write_disposition="merge",   # incremental upsert by document number
        max_rows_per_table=0,        # 0 = no row cap (see note below)
    )

Design
------
* **Auth:** none. The API is public, with no account, token or approval. A
  descriptive User-Agent is sent as a courtesy. Access is read-only.
* **Scope:** a ``published_since`` date is required, narrowed optionally by
  agency, document type or search term. The Federal Register goes back to 1994
  and holds millions of documents, so an unbounded sync is never what a caller
  means.
* **Document granularity:** one document per Federal Register document, keyed
  by its ``document_number``, which is the stable citation id.
* **Document mode:** the source declares ``cognee_document_source =
  "federalregister"``, so ``resolve_dlt_sources`` tags each row
  ``system_metadata["source"] = "federalregister"`` and the document flows
  through the standard cognify entity-extraction pipeline, which is the right
  treatment for prose, instead of the deterministic dlt-row schema-context
  path.
* **Incremental cursor:** ``publication_date``, pushed down to the API as
  ``conditions[publication_date][gte]``. The delta is computed server-side, so
  unlike the wiki and mailbox connectors nothing has to list everything and
  compare timestamps locally.
* **Forget-on-delete:** the API has no deletion feed, so each run sweeps the
  document numbers in scope and compares them against the numbers seen on the
  previous run (kept in resource state). Documents that vanished are emitted
  with the ``_deleted`` hard-delete marker; dlt removes those rows on ``merge``
  and cognee's existing ``orphan_cleanup`` then purges them from the graph +
  vector + relational stores. No parallel cleanup path.

What gets ingested, and an honest limit
---------------------------------------
The API returns an ``abstract``, which is the agency's own summary of the
document and is the genuinely useful text. The full rule body is only available
as a separate text URL per document, so ingesting it costs one extra request
per document and brings a large amount of procedural boilerplate (addresses,
docket procedure, signature blocks) along with the substance. The default is
therefore the abstract plus the structured facts a reader needs, and full text
is opt-in through ``include_full_text=True`` rather than pretending the abstract
is the whole rule.

``abstract`` is null for presidential documents. Those are rendered from their
title, type and president rather than skipped.

.. note::
   cognee's ``ingest_dlt_source`` reads at most ``max_rows_per_table`` rows
   from the dlt destination (default 50). For a real scope pass
   ``max_rows_per_table=0`` (unlimited) so orphan-cleanup compares against the
   *whole* synced corpus rather than a truncated window.

.. note::
   The cursor has day granularity and the ``gte`` condition is inclusive, so
   each run re-reads the documents published on the cursor date itself. That is
   deliberate: excluding them would permanently drop a document published later
   on the same day as the previous run. The re-read rows merge onto themselves
   by document number, so the only cost is a little bandwidth.

.. note::
   Deletion detection needs one id-only sweep of the scope per run. The
   Federal Register is close to append-only (a published document stays
   published), so the sweep mostly exists to catch a document leaving the
   configured scope. It can be turned off with ``detect_deletions=False``.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion.dlt_utils import (
    DOCUMENT_SOURCE_ATTR,
    NODE_SET_COLUMN,
    NODE_SET_COLUMN_HINT,
)

logger = get_logger("federalregister_connector")

_API_URL = "https://www.federalregister.gov/api/v1/documents.json"

_SOURCE_NAME = "federalregister"
_TABLE_NAME = "federalregister_documents"

# The API caps per_page at 1000.
_MAX_PER_PAGE = 1000

# Retry budget for rate-limited / transient responses.
_MAX_RETRIES = 5

# Hard ceiling on pages walked, so a server that keeps handing back a next-page
# link cannot spin forever.
_MAX_PAGES = 2000

# Fields fetched for a document that is being ingested.
_FULL_FIELDS = (
    "document_number",
    "title",
    "abstract",
    "publication_date",
    "type",
    "html_url",
    "agencies",
    "docket_ids",
    "effective_on",
    "comments_close_on",
    "action",
    "citation",
    "president",
    "raw_text_url",
)

# Fields fetched for the deletion sweep, which only needs identity.
_ID_FIELDS = ("document_number",)

_DEFAULT_USER_AGENT = (
    "cognee-community-connector-federalregister (+https://github.com/topoteretes/cognee-community)"
)

_EXTRA_HINT = (
    'The Federal Register connector requires "requests". Install the package:\n'
    "    uv pip install cognee-community-connector-federalregister"
)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def _make_session(user_agent: str | None = None) -> Any:
    """Build a ``requests`` session for the public API.

    ``requests`` is imported lazily so importing this module never hard-fails
    when the optional dependency is missing.
    """
    try:
        import requests
    except ImportError as exc:  # pragma: no cover - depends on optional extra
        raise ImportError(_EXTRA_HINT) from exc

    session = requests.Session()
    session.headers.update(
        {"Accept": "application/json", "User-Agent": user_agent or _DEFAULT_USER_AGENT}
    )
    return session


def _request(session: Any, url: str, params: dict | None = None) -> Any:
    """GET a URL, retrying rate-limit and transient errors."""
    for attempt in range(_MAX_RETRIES):
        try:
            response = session.get(url, params=params or {})
            response.raise_for_status()
            return response
        except Exception as exc:
            if attempt == _MAX_RETRIES - 1 or not _is_transient(exc):
                raise
            delay = _retry_after(_response_headers(exc), attempt)
            logger.warning(
                "Federal Register: %s, retrying in %.1fs (%d/%d).",
                exc,
                delay,
                attempt + 1,
                _MAX_RETRIES,
            )
            time.sleep(delay)
    raise RuntimeError("unreachable: retry loop exhausted without returning or raising")


def _api_get(session: Any, url: str, params: dict | None = None) -> dict:
    """GET a JSON endpoint and return the parsed body.

    The body is only parsed after ``raise_for_status``, and a non-JSON success
    body raises a clear error rather than a bare decoding failure.
    """
    response = _request(session, url, params)
    try:
        return response.json()
    except ValueError as exc:
        raise ValueError(
            f"Federal Register returned a non-JSON body: {response.text[:200]!r}"
        ) from exc


def _get_text(session: Any, url: str) -> str:
    """GET a plain-text document body (used only for opt-in full text)."""
    return _request(session, url).text


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
# Query building / paging
# ---------------------------------------------------------------------------
def _query_params(
    *,
    published_since: str,
    agencies: list[str] | None,
    document_types: list[str] | None,
    term: str | None,
    fields: tuple[str, ...],
    per_page: int,
) -> dict[str, Any]:
    """Build the documents query.

    ``conditions[...]`` keys are passed through as repeated array params where
    the API expects arrays, which ``requests`` produces from a list value.
    ``order=oldest`` makes the walk deterministic and keeps the newest
    publication date at the end, where the cursor is read from.
    """
    params: dict[str, Any] = {
        "per_page": max(1, min(per_page, _MAX_PER_PAGE)),
        "order": "oldest",
        "conditions[publication_date][gte]": published_since,
        "fields[]": list(fields),
    }
    if agencies:
        params["conditions[agencies][]"] = list(agencies)
    if document_types:
        params["conditions[type][]"] = list(document_types)
    if term:
        params["conditions[term]"] = term
    return params


def _iter_documents(session: Any, params: dict) -> Iterator[dict]:
    """Yield documents across the API's cursor pagination.

    The response carries ``next_page_url``, which already holds the
    ``search_after_cursor``. Following that link is the reliable walk:
    incrementing ``page`` alone runs into a server-side page ceiling on large
    result sets.
    """
    payload = _api_get(session, _API_URL, params)
    for page in range(_MAX_PAGES):
        results = payload.get("results")
        if not results:
            return
        yield from results

        next_url = payload.get("next_page_url")
        if not next_url:
            return
        # The next-page URL already carries every condition and the cursor, so
        # no params are re-sent with it.
        payload = _api_get(session, next_url)
        if page == _MAX_PAGES - 1:
            logger.warning("Federal Register: stopped after %d pages.", _MAX_PAGES)


def _iter_scope_ids(session: Any, scope: dict) -> Iterator[str]:
    """Yield the document numbers currently in scope, requesting identity only."""
    for document in _iter_documents(session, scope):
        number = _clean(document.get("document_number"))
        if number:
            yield number


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def _clean(value: Any) -> str:
    """Collapse a field to a stripped string."""
    return str(value).strip() if value is not None else ""


def _agency_names(document: dict) -> list[str]:
    """The publishing agencies' display names."""
    agencies = document.get("agencies")
    if not isinstance(agencies, list):
        return []
    names = []
    for agency in agencies:
        if not isinstance(agency, dict):
            continue
        name = _clean(agency.get("name")) or _clean(agency.get("raw_name"))
        if name:
            names.append(name)
    return names


def _president_name(document: dict) -> str:
    """The signing president's name, for presidential documents."""
    president = document.get("president")
    if isinstance(president, dict):
        return _clean(president.get("name"))
    return ""


def _is_presidential(document: dict) -> bool:
    """Whether this is a presidential document rather than an agency one."""
    return "presidential" in _clean(document.get("type")).lower()


def _header_lines(document: dict) -> list[str]:
    """The document's structured facts, as labelled lines above the prose."""
    lines = [f"Document number: {_clean(document.get('document_number'))}"]

    if document.get("type"):
        lines.append(f"Type: {_clean(document['type'])}")
    if document.get("action"):
        lines.append(f"Action: {_clean(document['action'])}")

    agencies = _agency_names(document)
    if agencies:
        lines.append(f"Agencies: {', '.join(agencies)}")

    # The API returns the sitting president on every document, including
    # technical agency rules. Naming them there reads as authorship of a rule
    # the agency wrote, so the line is kept for presidential documents only,
    # where the president really is the author.
    if _is_presidential(document):
        president = _president_name(document)
        if president:
            lines.append(f"President: {president}")

    if document.get("citation"):
        lines.append(f"Citation: {_clean(document['citation'])}")
    if document.get("publication_date"):
        lines.append(f"Published: {_clean(document['publication_date'])}")
    if document.get("effective_on"):
        lines.append(f"Effective: {_clean(document['effective_on'])}")
    if document.get("comments_close_on"):
        lines.append(f"Comments close: {_clean(document['comments_close_on'])}")

    dockets = document.get("docket_ids")
    if isinstance(dockets, list) and dockets:
        lines.append(f"Dockets: {', '.join(_clean(d) for d in dockets if _clean(d))}")

    return lines


def _content(document: dict, full_text: str = "") -> str:
    """Render a document into the text cognify will read.

    Presidential documents carry no abstract, so the header carries them: the
    title, type and signing president are the substance there.
    """
    sections = ["\n".join(_header_lines(document))]

    abstract = _clean(document.get("abstract"))
    if abstract:
        sections.append(f"Abstract:\n{abstract}")

    if full_text:
        sections.append(f"Full text:\n{full_text}")

    return "\n\n".join(section for section in sections if section.strip())


def _node_sets(document: dict) -> list[str] | None:
    """Node-set names for a document: its agencies and its type.

    cognee namespaces these under the source tag, so they cannot collide with
    cognee's own node sets.
    """
    names = [f"agency:{name}" for name in _agency_names(document)]
    document_type = _clean(document.get("type"))
    if document_type:
        names.append(f"type:{document_type}")
    # Deterministic order, so an unchanged document keeps a stable content hash.
    return sorted(set(names)) or None


def _document_to_row(document: dict, full_text: str, include_node_sets: bool) -> dict[str, Any]:
    """Flatten a document into a document-mode row.

    Row contract: ``{id, title, content, url}``. ``resolve_dlt_sources`` turns
    ``title``/``content`` into the document text and keeps ``url``/``id`` as
    provenance.
    """
    row: dict[str, Any] = {
        "id": _clean(document.get("document_number")),
        "title": _clean(document.get("title")),
        "url": _clean(document.get("html_url")),
        "content": _content(document, full_text),
        # Hard-delete marker (always False for live documents). Documents that
        # left the scope are emitted separately with _deleted=True.
        "_deleted": False,
    }
    if include_node_sets:
        node_sets = _node_sets(document)
        if node_sets:
            row[NODE_SET_COLUMN] = node_sets
    return row


def _deleted_row(document_number: str) -> dict[str, Any]:
    """Build a minimal row that instructs dlt to hard-delete a document."""
    return {"id": str(document_number), "_deleted": True}


# ---------------------------------------------------------------------------
# Sync (pure given a session + state dict, so it is unit-testable)
# ---------------------------------------------------------------------------
def sync_documents(
    session: Any,
    state: dict,
    *,
    published_since: str,
    agencies: list[str] | None = None,
    document_types: list[str] | None = None,
    term: str | None = None,
    per_page: int = 100,
    detect_deletions: bool = True,
    include_node_sets: bool = True,
    include_full_text: bool = False,
) -> Iterator[dict[str, Any]]:
    """Yield documents published since the last run, plus hard-delete markers.

    The delta is computed by the API: the publication-date condition starts at
    the stored cursor rather than at ``published_since`` once a run has
    completed. A separate id-only sweep of the whole scope drives deletion
    detection and also catches documents that are new to the corpus but were
    published before the cursor, which a date filter alone cannot return (a
    widened agency list, a loosened search term).
    """
    known_ids: set[str] = set(state.get("known_ids", []))
    cursor: str = str(state.get("last_publication_date") or "")
    # ISO dates compare lexicographically, so the later of the two is the max.
    delta_since = max(cursor, published_since) if cursor else published_since
    newest = cursor

    def scope(fields: tuple[str, ...], since: str) -> dict[str, Any]:
        return _query_params(
            published_since=since,
            agencies=agencies,
            document_types=document_types,
            term=term,
            fields=fields,
            per_page=per_page,
        )

    # --- Changed documents, filtered server-side ---------------------------
    emitted: set[str] = set()
    for document in _iter_documents(session, scope(_FULL_FIELDS, delta_since)):
        number = _clean(document.get("document_number"))
        if not number or number in emitted:
            continue
        emitted.add(number)
        newest = max(newest, _clean(document.get("publication_date")))
        yield _document_to_row(
            document, _full_text_for(session, document, include_full_text), include_node_sets
        )

    if not detect_deletions:
        state["known_ids"] = sorted(known_ids | emitted)
        state["last_publication_date"] = newest
        logger.info(
            "Federal Register: %d changed document(s), deletion detection off.", len(emitted)
        )
        return

    # --- Scope sweep: deletions, plus documents new to the corpus ----------
    current_ids = set(_iter_scope_ids(session, scope(_ID_FIELDS, published_since)))

    # An empty sweep while documents were previously known almost always means
    # a transient/failed listing rather than a scope that genuinely emptied.
    # Treating it as "all deleted" would purge the whole dataset and overwrite
    # known_ids with [], making the loss permanent. Skip deletion and preserve
    # state in that case.
    if known_ids and not current_ids:
        logger.warning(
            "Federal Register: scope sweep returned 0 documents but %d were known; "
            "skipping deletion this run to avoid a mass forget-on-delete on a "
            "transient sweep.",
            len(known_ids),
        )
        state["last_publication_date"] = newest
        logger.info("Federal Register: %d changed document(s), 0 deletion(s).", len(emitted))
        return

    backfill = sorted(current_ids - known_ids - emitted)
    for document in _iter_by_numbers(session, backfill, per_page):
        number = _clean(document.get("document_number"))
        if not number or number in emitted:
            continue
        emitted.add(number)
        newest = max(newest, _clean(document.get("publication_date")))
        yield _document_to_row(
            document, _full_text_for(session, document, include_full_text), include_node_sets
        )

    deleted = known_ids - current_ids
    for number in sorted(deleted):
        yield _deleted_row(number)

    state["known_ids"] = sorted(current_ids | emitted)
    state["last_publication_date"] = newest
    logger.info(
        "Federal Register: %d document(s) emitted (%d backfilled), %d deletion(s).",
        len(emitted),
        len(backfill),
        len(deleted),
    )


def _full_text_for(session: Any, document: dict, include_full_text: bool) -> str:
    """Fetch a document's full text when the caller opted in.

    A missing or failing text URL is not fatal: the abstract and the structured
    facts are still worth ingesting, so the document degrades rather than
    aborting the run.
    """
    if not include_full_text:
        return ""
    url = _clean(document.get("raw_text_url"))
    if not url:
        return ""
    try:
        return _get_text(session, url).strip()
    except Exception as exc:
        logger.warning(
            "Federal Register: could not fetch full text for %s (%s); using the abstract.",
            _clean(document.get("document_number")),
            exc,
        )
        return ""


def _iter_by_numbers(session: Any, numbers: list[str], per_page: int) -> Iterator[dict]:
    """Fetch specific documents by document number.

    The documents endpoint has no id filter, so these are read one at a time
    from the per-document endpoint. Only documents new to the corpus take this
    path, so it is normally empty.
    """
    for number in numbers:
        url = f"https://www.federalregister.gov/api/v1/documents/{number}.json"
        try:
            payload = _api_get(session, url, {"fields[]": list(_FULL_FIELDS)})
        except Exception as exc:
            logger.warning("Federal Register: could not fetch document %s (%s).", number, exc)
            continue
        if isinstance(payload, dict) and payload.get("document_number"):
            yield payload


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------
def federalregister_source(
    *,
    published_since: str,
    agencies: list[str] | None = None,
    document_types: list[str] | None = None,
    term: str | None = None,
    per_page: int = 100,
    detect_deletions: bool = True,
    include_node_sets: bool = True,
    include_full_text: bool = False,
    user_agent: str | None = None,
    session: Any = None,
):
    """Return a ``dlt`` resource that yields Federal Register documents.

    Args:
        published_since: Earliest publication date to sync, ``YYYY-MM-DD``.
            Required: the Federal Register goes back to 1994, so an unbounded
            sync is never what a caller means.
        agencies: Agency slugs to restrict to, as they appear in agency URLs,
            e.g. ``["environmental-protection-agency"]``.
        document_types: Document type codes to restrict to: ``"RULE"``,
            ``"PRORULE"``, ``"NOTICE"`` or ``"PRESDOCU"``. Note the filter
            takes the code while the returned ``type`` field is a display name
            such as ``"Rule"``.
        term: Full-text search term.
        per_page: Documents per request, capped at 1000 by the API.
        detect_deletions: Sweep the scope's document numbers each run so
            documents that left it are forgotten.
        include_node_sets: Group documents into node sets by agency and type.
        include_full_text: Also fetch each document's full text, which costs
            one extra request per document and includes procedural boilerplate.
        user_agent: Override the User-Agent sent to the API.
        session: Pre-built ``requests`` session. Mainly an injection point for
            tests; when omitted one is built here.

    Returns:
        A ``dlt`` resource (``federalregister_documents``) configured with
        ``primary_key="id"``, ``write_disposition="merge"`` and an ``_deleted``
        hard-delete column, tagged for cognee's document path. Hand it to
        ``cognee.remember(...)``.
    """
    try:
        import dlt
    except ImportError as exc:
        raise ImportError(
            'The Federal Register connector requires the dlt extra: pip install "cognee[dlt]".'
        ) from exc

    if not _clean(published_since):
        raise ValueError(
            "federalregister_source requires published_since (YYYY-MM-DD): the "
            "Federal Register is too large to sync unbounded."
        )

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
    def federalregister_documents():
        client = session or _make_session(user_agent)
        resource_state = dlt.current.resource_state()
        yield from sync_documents(
            client,
            resource_state,
            published_since=published_since,
            agencies=agencies,
            document_types=document_types,
            term=term,
            per_page=per_page,
            detect_deletions=detect_deletions,
            include_node_sets=include_node_sets,
            include_full_text=include_full_text,
        )

    resource = federalregister_documents()
    # Opt into the document ingestion path (document -> text -> cognify entity
    # extraction). resolve_dlt_sources reads this marker; it never imports this
    # connector. Document mode supports the incremental model used here:
    # write_disposition="merge" plus the _deleted hard-delete tombstones
    # emitted by the scope sweep.
    setattr(resource, DOCUMENT_SOURCE_ATTR, _SOURCE_NAME)
    return resource
