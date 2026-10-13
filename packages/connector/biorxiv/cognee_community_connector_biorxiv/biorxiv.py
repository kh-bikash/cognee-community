"""bioRxiv / medRxiv connector for cognee: a ``dlt`` source that turns preprint
servers into memory.

Pull preprints from bioRxiv or medRxiv into cognee, incrementally and with
forget-on-deletion, so a research team can ask "what appeared in my field this
week, and has any of it been published since?". Like the sibling Confluence and
Google Drive connectors this builds entirely on the existing DLT ingestion
subsystem; the source produced here is handed directly to
:func:`cognee.remember`::

    import cognee
    from cognee_community_connector_biorxiv import biorxiv_source

    await cognee.remember(
        biorxiv_source(server="biorxiv", posted_since="2026-10-01"),
        dataset_name="preprints",
        primary_key="id",
        write_disposition="merge",   # incremental upsert by DOI
        max_rows_per_table=0,        # 0 = no row cap (see note below)
    )

Design
------
* **Auth:** none. The API is public, with no account, token or approval. A
  descriptive User-Agent is sent as a courtesy. Access is read-only.
* **Servers:** ``biorxiv`` and ``medrxiv`` share one API shape, so either can
  be selected.
* **Scope:** a ``posted_since`` date is required, optionally bounded by
  ``posted_until`` and narrowed to a subject category.
* **Document granularity:** one document per preprint, keyed by DOI.
* **Document mode:** the source declares ``cognee_document_source =
  "biorxiv"``, so ``resolve_dlt_sources`` tags each row
  ``system_metadata["source"] = "biorxiv"`` and the abstract flows through the
  standard cognify entity-extraction pipeline, which is the right treatment for
  prose, instead of the deterministic dlt-row schema-context path.
* **Forget-on-delete:** ``write_disposition="merge"`` with an ``_deleted``
  hard-delete marker; dlt removes those rows on merge and cognee's existing
  ``orphan_cleanup`` then purges them from the graph + vector + relational
  stores. No parallel cleanup path.

Versions are the thing to get right
-----------------------------------
The details endpoint returns one entry per *version*, so a revised preprint
appears several times in a window as version 1, 2, 3. Keying on DOI and keeping
the highest version means memory holds the current preprint instead of one row
per revision. A live interval showed ``count_new_papers`` (432) well below
``total`` (552) for the same window, which is exactly that gap between new
papers and revisions of existing ones.

State therefore tracks ``doi@version`` rather than the DOI alone. A preprint
whose version has not moved is skipped without re-rendering, while a new
version re-emits the row and merges over the old one.

The ``published`` field
-----------------------
Each entry carries the DOI of the journal article if the preprint was
eventually published, and the string ``"NA"`` (not null) when it was not. That
makes "which of the preprints we tracked actually made it into a journal?"
answerable, and it is a field that changes on an existing record long after
posting, which is a real reason to re-sync rather than treat the archive as
append-only.

How the two sync modes differ, and why
--------------------------------------
There is no "modified since" filter and no id-only projection, so the walk that
would detect deletions is the same cost as the walk that fetches content. Doing
both separately would double every run for no benefit, so:

* ``detect_deletions=True`` (default) walks the whole configured window once and
  uses it for both purposes: rows are emitted for DOIs whose version is new to
  the corpus, and tombstones for DOIs that have left the window.
* ``detect_deletions=False`` walks only from the stored cursor forward, which is
  cheaper on a long window but cannot see a deletion.

An honest limit: preprint servers are close to append-only, since a withdrawn
preprint normally stays with a withdrawal notice rather than disappearing. The
sweep covers the configured window rather than the whole archive, which is what
makes it affordable, and that is the scope of the forget-on-delete guarantee.

.. note::
   cognee's ``ingest_dlt_source`` reads at most ``max_rows_per_table`` rows
   from the dlt destination (default 50). For a real window pass
   ``max_rows_per_table=0`` (unlimited) so orphan-cleanup compares against the
   *whole* synced corpus rather than a truncated window.

.. note::
   The cursor has day granularity, so each run re-reads the posting date it
   stopped on. Excluding it would permanently drop a preprint posted later on
   the same day as the previous run. Unchanged versions are skipped by the
   ``doi@version`` check, so the re-read costs bandwidth rather than work.
"""

from __future__ import annotations

import re
import time
from collections.abc import Iterator
from datetime import date
from typing import Any

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion.dlt_utils import (
    DOCUMENT_SOURCE_ATTR,
    NODE_SET_COLUMN,
    NODE_SET_COLUMN_HINT,
)

logger = get_logger("biorxiv_connector")

_API_BASE = "https://api.biorxiv.org/details"

_SOURCE_NAME = "biorxiv"
_TABLE_NAME = "biorxiv_preprints"

_SERVERS = ("biorxiv", "medrxiv")

# The API returns this when the cursor has passed the end of the window.
_NO_POSTS = "no posts found"

# The `published` field holds this string, not null, when a preprint has not
# been published in a journal.
_NOT_PUBLISHED = "NA"

# Retry budget for rate-limited / transient responses.
_MAX_RETRIES = 5

# Hard ceiling on pages walked, so a server that never signals the end cannot
# spin forever. At 30 entries per page this covers 300k entries.
_MAX_PAGES = 10_000

_DEFAULT_USER_AGENT = (
    "cognee-community-connector-biorxiv (+https://github.com/topoteretes/cognee-community)"
)

_EXTRA_HINT = (
    'The bioRxiv connector requires "requests". Install the package:\n'
    "    uv pip install cognee-community-connector-biorxiv"
)

# Named character placeholders the servers emit instead of the characters (see
# _clean). Greek letters dominate; "middle dot" is also used as a decimal point
# in some journals' house style, so it decodes to the character it names rather
# than being reinterpreted as a full stop.
_PLACEHOLDERS = {
    "middle dot": "·",
    "degrees": "°",
    "pound": "£",
    "euro": "€",
    "plusmn": "±",
    "times": "×",
    "le": "≤",
    "ge": "≥",
    "alpha": "α",
    "beta": "β",
    "gamma": "γ",
    "delta": "δ",
    "epsilon": "ε",
    "zeta": "ζ",
    "eta": "η",
    "theta": "θ",
    "iota": "ι",
    "kappa": "κ",
    "lambda": "λ",
    "mu": "μ",
    "nu": "ν",
    "xi": "ξ",
    "pi": "π",
    "rho": "ρ",
    "sigma": "σ",
    "tau": "τ",
    "upsilon": "υ",
    "phi": "φ",
    "chi": "χ",
    "psi": "ψ",
    "omega": "ω",
    "Alpha": "Α",
    "Beta": "Β",
    "Gamma": "Γ",
    "Delta": "Δ",
    "Theta": "Θ",
    "Lambda": "Λ",
    "Pi": "Π",
    "Sigma": "Σ",
    "Phi": "Φ",
    "Psi": "Ψ",
    "Omega": "Ω",
}

_SUPERSCRIPTS = "⁰¹²³⁴⁵⁶⁷⁸⁹"
_SUBSCRIPTS = "₀₁₂₃₄₅₆₇₈₉"

_PLACEHOLDER_RE = re.compile(r"\{([a-zA-Z][a-zA-Z0-9 _-]{0,24})\}")


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


def _api_get(session: Any, url: str) -> dict:
    """GET a details URL and return parsed JSON, retrying transient errors.

    The body is only parsed after ``raise_for_status``, and a non-JSON success
    body raises a clear error rather than a bare decoding failure.
    """
    for attempt in range(_MAX_RETRIES):
        try:
            response = session.get(url)
            response.raise_for_status()
            try:
                return response.json()
            except ValueError as exc:
                raise ValueError(
                    f"bioRxiv returned a non-JSON body: {response.text[:200]!r}"
                ) from exc
        except Exception as exc:
            if attempt == _MAX_RETRIES - 1 or not _is_transient(exc):
                raise
            delay = _retry_after(_response_headers(exc), attempt)
            logger.warning(
                "bioRxiv: %s, retrying in %.1fs (%d/%d).",
                exc,
                delay,
                attempt + 1,
                _MAX_RETRIES,
            )
            time.sleep(delay)
    raise RuntimeError("unreachable: retry loop exhausted without returning or raising")


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
# Paging
# ---------------------------------------------------------------------------
def _iter_entries(session: Any, server: str, since: str, until: str) -> Iterator[dict]:
    """Yield every version entry in a posting-date window.

    The cursor is the last path segment, not a query parameter, and it advances
    by the number of entries the previous page returned. The window ends when a
    page comes back empty or reports "no posts found", which is what the API
    answers with once the cursor has passed the end.
    """
    cursor = 0
    for _ in range(_MAX_PAGES):
        payload = _api_get(session, f"{_API_BASE}/{server}/{since}/{until}/{cursor}")
        status = _clean((payload.get("messages") or [{}])[0].get("status")).lower()
        collection = payload.get("collection")

        if status == _NO_POSTS or not collection:
            return

        yield from collection

        # The cursor advances by the page size the server actually used, which
        # is not a value the client gets to choose.
        advanced = len(collection)
        if advanced <= 0:
            return
        cursor += advanced
    logger.warning("bioRxiv: stopped after %d pages.", _MAX_PAGES)


# ---------------------------------------------------------------------------
# Field access / rendering
# ---------------------------------------------------------------------------
def _clean(value: Any) -> str:
    """Strip a field and decode the servers' ``{name}`` character placeholders.

    Abstracts carry special characters as named placeholders rather than as the
    characters themselves: ``{beta}-amyloid``, ``2{middle dot}08``,
    ``37{degrees}C``. A survey of 191 live abstracts found placeholders in 22 of
    them, so this is common rather than exotic, and left alone it damages what
    cognify extracts: "{beta}-amyloid" is not the entity "beta-amyloid".

    Only known names are decoded. An unrecognised ``{...}`` is left exactly as
    it is, since braces also appear legitimately in formulae and code.
    """
    if value is None:
        return ""
    return _PLACEHOLDER_RE.sub(_decode_placeholder, str(value)).strip()


def _decode_placeholder(match: Any) -> str:
    """Replace one ``{name}`` placeholder, or leave it untouched if unknown."""
    name = match.group(1)
    if name in _PLACEHOLDERS:
        return _PLACEHOLDERS[name]

    # "superscript 2" / "subscript 3" and their digits.
    parts = name.split()
    if len(parts) == 2 and parts[1].isdigit() and len(parts[1]) == 1:
        table = _SUPERSCRIPTS if parts[0] == "superscript" else None
        if parts[0] == "subscript":
            table = _SUBSCRIPTS
        if table is not None:
            return table[int(parts[1])]
    return match.group(0)


def _version(entry: dict) -> int:
    """The entry's version number, defaulting to 1 when unparseable."""
    try:
        return int(_clean(entry.get("version")) or 1)
    except ValueError:
        return 1


def _published_doi(entry: dict) -> str:
    """The journal DOI if this preprint was published, else "".

    The API uses the string "NA" rather than null for unpublished preprints, so
    a naive read would record a journal DOI of "NA" and render a link to it.
    """
    published = _clean(entry.get("published"))
    if not published or published.upper() == _NOT_PUBLISHED:
        return ""
    return published


def _is_published(entry: dict) -> bool:
    """Whether this preprint has an associated journal article."""
    return bool(_published_doi(entry))


def _header_lines(entry: dict) -> list[str]:
    """The preprint's structured facts, as labelled lines above the abstract."""
    lines = [f"DOI: {_clean(entry.get('doi'))}"]

    if entry.get("server"):
        lines.append(f"Server: {_clean(entry['server'])}")
    if entry.get("category"):
        lines.append(f"Category: {_clean(entry['category'])}")
    if entry.get("date"):
        lines.append(f"Posted: {_clean(entry['date'])}")
    lines.append(f"Version: {_version(entry)}")
    if entry.get("type"):
        lines.append(f"Type: {_clean(entry['type'])}")

    authors = _clean(entry.get("authors"))
    if authors:
        lines.append(f"Authors: {authors}")

    corresponding = _clean(entry.get("author_corresponding"))
    institution = _clean(entry.get("author_corresponding_institution"))
    if corresponding or institution:
        suffix = f" ({institution})" if institution else ""
        lines.append(f"Corresponding author: {corresponding}{suffix}")

    published = _published_doi(entry)
    lines.append(
        f"Published in a journal: yes, DOI {published}"
        if published
        else "Published in a journal: no"
    )

    if entry.get("license"):
        lines.append(f"License: {_clean(entry['license'])}")

    return lines


def _content(entry: dict) -> str:
    """Render a preprint into the text cognify will read."""
    sections = ["\n".join(_header_lines(entry))]
    abstract = _clean(entry.get("abstract"))
    if abstract:
        sections.append(f"Abstract:\n{abstract}")
    return "\n\n".join(section for section in sections if section.strip())


def _node_sets(entry: dict) -> list[str] | None:
    """Node-set names for a preprint: category, server and publication state.

    cognee namespaces these under the source tag, so they cannot collide with
    cognee's own node sets.
    """
    names = []
    category = _clean(entry.get("category"))
    if category:
        names.append(f"category:{category}")
    server = _clean(entry.get("server"))
    if server:
        names.append(f"server:{server.lower()}")
    names.append("state:published" if _is_published(entry) else "state:preprint")
    # Deterministic order, so an unchanged preprint keeps a stable content hash.
    return sorted(set(names)) or None


def _entry_to_row(entry: dict, include_node_sets: bool) -> dict[str, Any]:
    """Flatten a preprint into a document-mode row.

    Row contract: ``{id, title, content, url}``. ``resolve_dlt_sources`` turns
    ``title``/``content`` into the document text and keeps ``url``/``id`` as
    provenance.
    """
    doi = _clean(entry.get("doi"))
    row: dict[str, Any] = {
        "id": doi,
        "title": _clean(entry.get("title")),
        "url": f"https://doi.org/{doi}" if doi else "",
        "content": _content(entry),
        # Hard-delete marker (always False for live preprints). Preprints that
        # left the window are emitted separately with _deleted=True.
        "_deleted": False,
    }
    if include_node_sets:
        node_sets = _node_sets(entry)
        if node_sets:
            row[NODE_SET_COLUMN] = node_sets
    return row


def _deleted_row(doi: str) -> dict[str, Any]:
    """Build a minimal row that instructs dlt to hard-delete a preprint."""
    return {"id": str(doi), "_deleted": True}


def _version_key(doi: str, version: int) -> str:
    """The identity stored in state: a DOI at a specific version."""
    return f"{doi}@{version}"


# ---------------------------------------------------------------------------
# Sync (pure given a session + state dict, so it is unit-testable)
# ---------------------------------------------------------------------------
def sync_preprints(
    session: Any,
    state: dict,
    *,
    server: str = "biorxiv",
    posted_since: str,
    posted_until: str | None = None,
    category: str | None = None,
    detect_deletions: bool = True,
    include_node_sets: bool = True,
) -> Iterator[dict[str, Any]]:
    """Yield preprints new or revised since the last run, plus hard-delete markers.

    With ``detect_deletions`` on, the configured window is walked once and used
    both to emit rows and to find DOIs that have left it. With it off, only the
    window from the stored cursor forward is walked, which is cheaper but
    cannot see a deletion.
    """
    until = posted_until or str(date.today())
    known: set[str] = set(state.get("known", []))
    known_dois = {key.rsplit("@", 1)[0] for key in known}
    cursor: str = str(state.get("last_posted_date") or "")
    newest = cursor

    # The full window is needed to see deletions; otherwise start at the cursor.
    # ISO dates compare lexicographically, so max() picks the later date.
    since = posted_since if detect_deletions else max(cursor, posted_since) or posted_since

    # Collapse the per-version entries to the highest version per DOI.
    latest: dict[str, dict] = {}
    for entry in _iter_entries(session, server, since, until):
        doi = _clean(entry.get("doi"))
        if not doi:
            continue
        if category and _clean(entry.get("category")).lower() != category.lower():
            # The details endpoint has no category parameter, so this narrowing
            # happens here rather than server-side.
            continue
        if doi not in latest or _version(entry) > _version(latest[doi]):
            latest[doi] = entry
        newest = max(newest, _clean(entry.get("date")))

    emitted = 0
    fresh_keys: set[str] = set()
    for doi, entry in sorted(latest.items()):
        key = _version_key(doi, _version(entry))
        fresh_keys.add(key)
        if key in known:
            # Same DOI at the same version: nothing has changed.
            continue
        emitted += 1
        yield _entry_to_row(entry, include_node_sets)

    if not detect_deletions:
        state["known"] = sorted(known | fresh_keys)
        state["last_posted_date"] = newest
        logger.info("bioRxiv: %d preprint(s) emitted, deletion detection off.", emitted)
        return

    # An empty window while preprints were previously known almost always means
    # a transient/failed listing rather than a window that genuinely emptied.
    # Treating it as "all deleted" would purge the whole dataset and overwrite
    # the id set with [], making the loss permanent. Skip deletion and preserve
    # state in that case.
    if known and not latest:
        logger.warning(
            "bioRxiv: window returned 0 preprints but %d were known; skipping "
            "deletion this run to avoid a mass forget-on-delete on a transient walk.",
            len(known_dois),
        )
        state["last_posted_date"] = newest
        logger.info("bioRxiv: %d preprint(s) emitted, 0 deletion(s).", emitted)
        return

    deleted = known_dois - set(latest)
    for doi in sorted(deleted):
        yield _deleted_row(doi)

    state["known"] = sorted(fresh_keys)
    state["last_posted_date"] = newest
    logger.info("bioRxiv: %d preprint(s) emitted, %d deletion(s).", emitted, len(deleted))


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------
def biorxiv_source(
    *,
    posted_since: str,
    server: str = "biorxiv",
    posted_until: str | None = None,
    category: str | None = None,
    detect_deletions: bool = True,
    include_node_sets: bool = True,
    user_agent: str | None = None,
    session: Any = None,
):
    """Return a ``dlt`` resource that yields bioRxiv or medRxiv preprints.

    Args:
        posted_since: Earliest posting date to sync, ``YYYY-MM-DD``. Required,
            because the archives hold hundreds of thousands of preprints.
        server: ``"biorxiv"`` or ``"medrxiv"``.
        posted_until: Latest posting date, ``YYYY-MM-DD``. Defaults to today.
        category: Subject category to keep, e.g. ``"health informatics"``.
            Applied client-side, since the details endpoint has no category
            parameter.
        detect_deletions: Walk the whole window each run so preprints that left
            it are forgotten. Turning this off walks only from the cursor
            forward, which is cheaper but cannot see a deletion.
        include_node_sets: Group preprints into node sets by category, server
            and publication state.
        user_agent: Override the User-Agent sent to the API.
        session: Pre-built ``requests`` session. Mainly an injection point for
            tests; when omitted one is built here.

    Returns:
        A ``dlt`` resource (``biorxiv_preprints``) configured with
        ``primary_key="id"``, ``write_disposition="merge"`` and an ``_deleted``
        hard-delete column, tagged for cognee's document path. Hand it to
        ``cognee.remember(...)``.
    """
    try:
        import dlt
    except ImportError as exc:
        raise ImportError(
            'The bioRxiv connector requires the dlt extra: pip install "cognee[dlt]".'
        ) from exc

    if server not in _SERVERS:
        raise ValueError(f"server must be one of {_SERVERS}, got {server!r}.")
    if not _clean(posted_since):
        raise ValueError(
            "biorxiv_source requires posted_since (YYYY-MM-DD): the archive is "
            "too large to sync unbounded."
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
    def biorxiv_preprints():
        client = session or _make_session(user_agent)
        resource_state = dlt.current.resource_state()
        yield from sync_preprints(
            client,
            resource_state,
            server=server,
            posted_since=posted_since,
            posted_until=posted_until,
            category=category,
            detect_deletions=detect_deletions,
            include_node_sets=include_node_sets,
        )

    resource = biorxiv_preprints()
    # Opt into the document ingestion path (preprint -> text -> cognify entity
    # extraction). resolve_dlt_sources reads this marker; it never imports this
    # connector. Document mode supports the incremental model used here:
    # write_disposition="merge" plus the _deleted hard-delete tombstones.
    setattr(resource, DOCUMENT_SOURCE_ATTR, _SOURCE_NAME)
    return resource
