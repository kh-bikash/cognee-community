"""ClinicalTrials.gov connector for cognee: a ``dlt`` source that turns the trial
registry into memory.

Pull studies from the public ClinicalTrials.gov API v2 into cognee,
incrementally and with forget-on-deletion, so you can "ask the trial registry".
Like the sibling Confluence and Google Drive connectors this builds entirely on
the existing DLT ingestion subsystem; the source produced here is handed
directly to :func:`cognee.remember`::

    import cognee
    from cognee_community_connector_clinicaltrials import clinicaltrials_source

    await cognee.remember(
        clinicaltrials_source(condition="type 2 diabetes"),
        dataset_name="trials",
        primary_key="id",
        write_disposition="merge",   # incremental upsert by NCT id
        max_rows_per_table=0,        # 0 = no row cap (see note below)
    )

Design
------
* **Auth:** none. API v2 is public, with no account, token or approval. A
  descriptive User-Agent is sent as a courtesy so the registry can identify
  the client. Access is read-only, ``GET`` only.
* **Scope:** studies are selected by condition, sponsor, status or a raw query
  term, so a user syncs their therapeutic area instead of all of the registry.
  Each option maps to one API parameter, so nothing has to compose query
  expressions.
* **Document granularity:** one document per study. The free text is spread
  across several modules (description, eligibility, outcomes, interventions)
  and none of them stands alone: eligibility criteria mean little without the
  intervention they gate. They are rendered into one document with section
  headings.
* **Document mode:** the source declares ``cognee_document_source =
  "clinicaltrials"``, so ``resolve_dlt_sources`` tags each row
  ``system_metadata["source"] = "clinicaltrials"`` and the study flows through
  the standard cognify entity-extraction pipeline, which is the right treatment
  for prose, instead of the deterministic dlt-row schema-context path.
* **Primary key:** the NCT id. Combined with ``write_disposition="merge"`` this
  gives idempotent upserts.
* **Incremental cursor:** ``LastUpdatePostDate``, pushed down to the API as
  ``filter.advanced=AREA[LastUpdatePostDate]RANGE[<cursor>,MAX]``. Unlike the
  wiki and mailbox connectors, which must list everything and compare
  timestamps locally, the delta is computed server-side here, so a run where
  nothing changed costs one small request.
* **Forget-on-delete:** the registry has no deletion feed, so each run sweeps
  the ids currently in scope and compares them against the ids seen on the
  previous run (kept in resource state). Studies that vanished are emitted with
  the ``_deleted`` hard-delete marker; dlt removes those rows on ``merge`` and
  cognee's existing ``orphan_cleanup`` then purges them from the graph + vector
  + relational stores. No parallel cleanup path.

What counts as "deleted" here
-----------------------------
The registry rarely removes records. A stopped trial usually becomes
``WITHDRAWN`` or ``TERMINATED``, which is a status change, not a deletion, and
must stay in memory: "this trial was terminated" is exactly the sort of fact
worth remembering. What does drive a tombstone is a study leaving the synced
scope, either because the record was withdrawn from the registry or because it
no longer matches the configured condition, sponsor or status. That is the same
semantics as narrowing a label filter on a support inbox.

What counts as knowledge
------------------------
The text-bearing modules are the knowledge: titles, conditions, interventions,
brief summary, detailed description, eligibility criteria and outcome measures.
The connector skips purely administrative and structural fields (ID aliases,
oversight flags, IPD-sharing statements, contact phone numbers) and the
``derivedSection`` browse-condition tree, which is generated taxonomy rather
than anything a person wrote.

.. note::
   cognee's ``ingest_dlt_source`` reads at most ``max_rows_per_table`` rows
   from the dlt destination (default 50). For a real scope pass
   ``max_rows_per_table=0`` (unlimited) so orphan-cleanup compares against the
   *whole* synced corpus rather than a truncated window.

.. note::
   The cursor has day granularity, and the API's range filter is inclusive, so
   each run re-fetches the studies posted on the cursor date itself. That is
   deliberate: excluding them would silently drop any study updated later on
   the same day as the previous run. The re-fetched rows merge onto themselves
   by NCT id, so the only cost is a little bandwidth.

.. note::
   Deletion detection needs one id-only sweep of the scope per run
   (``fields=NCTId``, up to 1000 ids per request). For a very wide scope that
   is the dominant cost of an otherwise cheap incremental run, so it can be
   turned off with ``detect_deletions=False``.
"""

from __future__ import annotations

import re
import time
from collections.abc import Iterator
from typing import Any

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion.dlt_utils import (
    DOCUMENT_SOURCE_ATTR,
    NODE_SET_COLUMN,
    NODE_SET_COLUMN_HINT,
)

logger = get_logger("clinicaltrials_connector")

_API_URL = "https://clinicaltrials.gov/api/v2/studies"
_STUDY_URL = "https://clinicaltrials.gov/study"

_SOURCE_NAME = "clinicaltrials"
_TABLE_NAME = "clinicaltrials_studies"

# The API caps pageSize at 1000.
_MAX_PAGE_SIZE = 1000
# filter.ids takes a comma-joined list; keep batches well inside URL limits.
_ID_BATCH = 50

# Retry budget for rate-limited / transient responses.
_MAX_RETRIES = 5

# Hard ceiling on pages walked, so a server that keeps handing back tokens
# cannot spin forever. At 1000 studies per page this covers the whole registry.
_MAX_PAGES = 1000

_DEFAULT_USER_AGENT = (
    "cognee-community-connector-clinicaltrials (+https://github.com/topoteretes/cognee-community)"
)

_EXTRA_HINT = (
    'The ClinicalTrials.gov connector requires "requests". Install the package:\n'
    "    uv pip install cognee-community-connector-clinicaltrials"
)

# Punctuation the registry backslash-escapes in free text (see _clean).
_ESCAPED_PUNCT_RE = re.compile(r"\\([&<>\[\]~*^.])")


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


def _api_get(session: Any, params: dict) -> dict:
    """GET the studies endpoint and return parsed JSON, retrying transient errors.

    The registry rate-limits bursts and sits behind a CDN, so 429 and 5xx
    responses are retried. An unknown query parameter is answered with a 400
    and a *plain text* body (not JSON), so the response is only parsed after
    ``raise_for_status``, and a non-JSON success body raises a clear error
    rather than a bare ``JSONDecodeError``.
    """
    for attempt in range(_MAX_RETRIES):
        try:
            response = session.get(_API_URL, params=params)
            response.raise_for_status()
            try:
                return response.json()
            except ValueError as exc:
                raise ValueError(
                    f"ClinicalTrials.gov returned a non-JSON body: {response.text[:200]!r}"
                ) from exc
        except Exception as exc:
            if attempt == _MAX_RETRIES - 1 or not _is_transient(exc):
                raise
            delay = _retry_after(_response_headers(exc), attempt)
            logger.warning(
                "ClinicalTrials.gov: %s, retrying in %.1fs (%d/%d).",
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
# Query building / paging
# ---------------------------------------------------------------------------
def _scope_params(
    condition: str | None,
    sponsor: str | None,
    statuses: list[str] | None,
    query_term: str | None,
) -> dict[str, Any]:
    """Map the scope options onto API parameters.

    Each option is its own parameter and the API ANDs them together, so the
    connector never has to build a query expression. ``condition`` and
    ``sponsor`` are relevance searches rather than exact matches (a sponsor
    search for one company also returns near matches), while
    ``filter.overallStatus`` is an exact enum filter.
    """
    params: dict[str, Any] = {}
    if condition:
        params["query.cond"] = condition
    if sponsor:
        params["query.spons"] = sponsor
    if statuses:
        params["filter.overallStatus"] = ",".join(statuses)
    if query_term:
        params["query.term"] = query_term
    return params


def _iter_pages(session: Any, params: dict) -> Iterator[dict]:
    """Yield studies across the API's opaque ``nextPageToken`` pagination.

    There is no total to loop against, so the walk ends when a page carries no
    token. A repeated token would otherwise loop forever, so that is treated as
    the end too.
    """
    token: str | None = None
    seen_tokens: set[str] = set()
    for page in range(_MAX_PAGES):
        request = dict(params)
        if token:
            request["pageToken"] = token
        payload = _api_get(session, request)

        studies = payload.get("studies")
        if not studies:
            return
        yield from studies

        token = payload.get("nextPageToken")
        if not token:
            return
        if token in seen_tokens:
            logger.warning("ClinicalTrials.gov: repeated page token on page %d, stopping.", page)
            return
        seen_tokens.add(token)


def _iter_scope_ids(session: Any, scope: dict, page_size: int) -> Iterator[str]:
    """Yield the NCT ids currently in scope, requesting ids only.

    The ``fields`` projection keeps this sweep small: it is the pass that drives
    forget-on-delete, so it has to enumerate the whole scope even on an
    otherwise incremental run.
    """
    params = dict(scope)
    params.update({"fields": "NCTId", "pageSize": page_size})
    for study in _iter_pages(session, params):
        nct_id = _nct_id(study)
        if nct_id:
            yield nct_id


# ---------------------------------------------------------------------------
# Field access
# ---------------------------------------------------------------------------
def _module(study: dict, name: str) -> dict:
    """Return a protocol-section module, or an empty dict."""
    protocol = study.get("protocolSection")
    if not isinstance(protocol, dict):
        return {}
    value = protocol.get(name)
    return value if isinstance(value, dict) else {}


def _nct_id(study: dict) -> str:
    """The study's NCT id, which is its primary key."""
    return str(_module(study, "identificationModule").get("nctId") or "")


def _last_update_posted(study: dict) -> str:
    """The study's ``LastUpdatePostDate`` as ``YYYY-MM-DD``, or "".

    ISO dates sort lexicographically, so the cursor can be compared as a string
    without parsing.
    """
    struct = _module(study, "statusModule").get("lastUpdatePostDateStruct")
    if isinstance(struct, dict):
        return str(struct.get("date") or "")
    return ""


def _date(struct: Any) -> str:
    """Pull ``date`` out of one of the API's date structs."""
    if isinstance(struct, dict):
        return str(struct.get("date") or "")
    return ""


def _clean(value: Any) -> str:
    """Strip a text field and undo the registry's Markdown-style escaping.

    Free-text fields come back with backslashes in front of punctuation
    (``\\&``, ``\\<``, ``\\[``, and so on). A survey of 214 live description
    fields found escapes only ever on this punctuation set, never as meaningful
    backslashes in prose. Left in place they corrupt the entities cognify
    extracts: "PRG Science \\& Technology Co., Ltd." should be read as one
    organization name, not as text containing a stray backslash.
    """
    if value is None:
        return ""
    return _ESCAPED_PUNCT_RE.sub(r"\1", str(value)).strip()


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def _title(study: dict) -> str:
    """Human title: the brief title, falling back to the official one."""
    identification = _module(study, "identificationModule")
    brief = _clean(identification.get("briefTitle"))
    if brief:
        return brief
    official = _clean(identification.get("officialTitle"))
    return official or _nct_id(study)


def _header_lines(study: dict) -> list[str]:
    """The study's structured facts, as labelled lines above the prose."""
    status = _module(study, "statusModule")
    design = _module(study, "designModule")
    sponsors = _module(study, "sponsorCollaboratorsModule")
    conditions = _module(study, "conditionsModule")

    lines = [f"NCT ID: {_nct_id(study)}"]
    if status.get("overallStatus"):
        lines.append(f"Status: {status['overallStatus']}")

    phases = design.get("phases")
    if isinstance(phases, list) and phases:
        lines.append(f"Phase: {', '.join(str(p) for p in phases)}")
    if design.get("studyType"):
        lines.append(f"Study type: {design['studyType']}")

    enrollment = design.get("enrollmentInfo")
    if isinstance(enrollment, dict) and enrollment.get("count") is not None:
        kind = enrollment.get("type")
        lines.append(f"Enrollment: {enrollment['count']}{f' ({kind})' if kind else ''}")

    lead = sponsors.get("leadSponsor")
    if isinstance(lead, dict) and lead.get("name"):
        lines.append(f"Lead sponsor: {_clean(lead['name'])}")

    condition_list = _conditions(study)
    if condition_list:
        lines.append(f"Conditions: {', '.join(condition_list)}")
    keywords = conditions.get("keywords")
    if isinstance(keywords, list) and keywords:
        lines.append(f"Keywords: {', '.join(str(k) for k in keywords)}")

    start = _date(status.get("startDateStruct"))
    if start:
        lines.append(f"Start date: {start}")
    completion = _date(status.get("completionDateStruct"))
    if completion:
        lines.append(f"Completion date: {completion}")
    updated = _last_update_posted(study)
    if updated:
        lines.append(f"Last update posted: {updated}")

    return lines


def _interventions(study: dict) -> list[str]:
    """Rendered interventions: what is actually being given to participants."""
    interventions = _module(study, "armsInterventionsModule").get("interventions")
    if not isinstance(interventions, list):
        return []
    rendered = []
    for intervention in interventions:
        if not isinstance(intervention, dict):
            continue
        name = _clean(intervention.get("name"))
        if not name:
            continue
        kind = _clean(intervention.get("type"))
        description = _clean(intervention.get("description"))
        line = f"- {name}{f' ({kind})' if kind else ''}"
        rendered.append(f"{line}: {description}" if description else line)
    return rendered


def _outcomes(study: dict) -> list[str]:
    """Rendered outcome measures, which say what the trial is actually testing."""
    outcomes_module = _module(study, "outcomesModule")
    rendered = []
    for key, label in (("primaryOutcomes", "Primary"), ("secondaryOutcomes", "Secondary")):
        outcomes = outcomes_module.get(key)
        if not isinstance(outcomes, list):
            continue
        for outcome in outcomes:
            if not isinstance(outcome, dict):
                continue
            measure = _clean(outcome.get("measure"))
            if not measure:
                continue
            parts = [f"- {label}: {measure}"]
            description = _clean(outcome.get("description"))
            if description:
                parts.append(description)
            time_frame = _clean(outcome.get("timeFrame"))
            if time_frame:
                parts.append(f"Time frame: {time_frame}")
            rendered.append(" ".join(parts))
    return rendered


def _eligibility(study: dict) -> list[str]:
    """Rendered eligibility: who can join, and the criteria text itself."""
    eligibility = _module(study, "eligibilityModule")
    lines = []

    who = []
    if eligibility.get("sex"):
        who.append(f"sex: {eligibility['sex']}")
    if eligibility.get("minimumAge"):
        who.append(f"minimum age: {eligibility['minimumAge']}")
    if eligibility.get("maximumAge"):
        who.append(f"maximum age: {eligibility['maximumAge']}")
    if eligibility.get("healthyVolunteers") is not None:
        who.append(f"healthy volunteers: {bool(eligibility['healthyVolunteers'])}")
    if who:
        lines.append("Participants (" + ", ".join(who) + ")")

    criteria = _clean(eligibility.get("eligibilityCriteria"))
    if criteria:
        lines.append(criteria)
    return lines


def _locations(study: dict) -> str:
    """A one-line location summary.

    The full location list is mostly addresses and phone numbers, which is
    contact data rather than knowledge, so only the countries and a site count
    are kept.
    """
    locations = _module(study, "contactsLocationsModule").get("locations")
    if not isinstance(locations, list) or not locations:
        return ""
    countries = sorted(
        {
            _clean(location.get("country"))
            for location in locations
            if isinstance(location, dict) and location.get("country")
        }
    )
    summary = f"{len(locations)} site(s)"
    return f"{summary} in {', '.join(countries)}" if countries else summary


def _content(study: dict) -> str:
    """Render a study into the document text cognify will read."""
    description = _module(study, "descriptionModule")
    sections: list[str] = ["\n".join(_header_lines(study))]

    brief = _clean(description.get("briefSummary"))
    if brief:
        sections.append(f"Brief summary:\n{brief}")

    detailed = _clean(description.get("detailedDescription"))
    if detailed:
        sections.append(f"Detailed description:\n{detailed}")

    interventions = _interventions(study)
    if interventions:
        sections.append("Interventions:\n" + "\n".join(interventions))

    outcomes = _outcomes(study)
    if outcomes:
        sections.append("Outcome measures:\n" + "\n".join(outcomes))

    eligibility = _eligibility(study)
    if eligibility:
        sections.append("Eligibility:\n" + "\n".join(eligibility))

    locations = _locations(study)
    if locations:
        sections.append(f"Locations: {locations}")

    return "\n\n".join(section for section in sections if section.strip())


def _conditions(study: dict) -> list[str]:
    """The study's condition list, cleaned."""
    conditions = _module(study, "conditionsModule").get("conditions")
    if not isinstance(conditions, list):
        return []
    return [_clean(condition) for condition in conditions if _clean(condition)]


def _node_sets(study: dict) -> list[str] | None:
    """Node-set names for a study: its conditions, lead sponsor and status.

    cognee namespaces these under the source tag, so they cannot collide with
    cognee's own node sets.
    """
    names = [f"condition:{condition}" for condition in _conditions(study)]

    lead = _module(study, "sponsorCollaboratorsModule").get("leadSponsor")
    if isinstance(lead, dict) and lead.get("name"):
        names.append(f"sponsor:{_clean(lead['name'])}")

    status = _module(study, "statusModule").get("overallStatus")
    if status:
        names.append(f"status:{status}")

    # Deterministic order, so an unchanged study keeps a stable content hash.
    return sorted(set(names)) or None


def _study_to_row(study: dict, include_node_sets: bool) -> dict[str, Any]:
    """Flatten a study into a document row.

    Document-mode row contract: ``{id, title, content, url}``.
    ``resolve_dlt_sources`` turns ``title``/``content`` into the document text
    and keeps ``url``/``id`` as provenance.
    """
    nct_id = _nct_id(study)
    row: dict[str, Any] = {
        "id": nct_id,
        "title": _title(study),
        "url": f"{_STUDY_URL}/{nct_id}",
        "content": _content(study),
        # Hard-delete marker (always False for live studies). Studies that left
        # the scope are emitted separately with _deleted=True.
        "_deleted": False,
    }
    if include_node_sets:
        node_sets = _node_sets(study)
        if node_sets:
            row[NODE_SET_COLUMN] = node_sets
    return row


def _deleted_row(nct_id: str) -> dict[str, Any]:
    """Build a minimal row that instructs dlt to hard-delete a study."""
    return {"id": str(nct_id), "_deleted": True}


# ---------------------------------------------------------------------------
# Sync (pure given a session + state dict, so it is unit-testable)
# ---------------------------------------------------------------------------
def sync_studies(
    session: Any,
    state: dict,
    *,
    condition: str | None = None,
    sponsor: str | None = None,
    statuses: list[str] | None = None,
    query_term: str | None = None,
    page_size: int = 100,
    detect_deletions: bool = True,
    include_node_sets: bool = True,
) -> Iterator[dict[str, Any]]:
    """Yield studies updated since the last run, plus hard-delete markers.

    The delta is computed by the API: only studies whose ``LastUpdatePostDate``
    is at or after the stored cursor are fetched. A separate id-only sweep of
    the scope drives deletion detection and also catches studies that are new
    to the corpus but were last updated before the cursor, which the date
    filter alone would miss (a widened scope, a newly added condition).
    """
    page_size = max(1, min(page_size, _MAX_PAGE_SIZE))
    known_ids: set[str] = set(state.get("known_ids", []))
    cursor: str = str(state.get("last_update_posted") or "")
    newest = cursor
    scope = _scope_params(condition, sponsor, statuses, query_term)

    # --- Changed studies, filtered server-side -----------------------------
    delta_params = dict(scope)
    delta_params["pageSize"] = page_size
    if cursor:
        # RANGE is inclusive, so the cursor day is re-fetched. That is
        # deliberate: a study updated later on the same day as the previous run
        # would otherwise be skipped forever.
        delta_params["filter.advanced"] = f"AREA[LastUpdatePostDate]RANGE[{cursor},MAX]"

    emitted: set[str] = set()
    for study in _iter_pages(session, delta_params):
        nct_id = _nct_id(study)
        if not nct_id or nct_id in emitted:
            continue
        emitted.add(nct_id)
        newest = max(newest, _last_update_posted(study))
        yield _study_to_row(study, include_node_sets)

    if not detect_deletions:
        state["known_ids"] = sorted(known_ids | emitted)
        state["last_update_posted"] = newest
        logger.info(
            "ClinicalTrials.gov: %d changed study(ies), deletion detection off.", len(emitted)
        )
        return

    # --- Scope sweep: deletions, plus studies new to the corpus ------------
    current_ids = set(_iter_scope_ids(session, scope, page_size))

    # An empty sweep while studies were previously known almost always means a
    # transient/failed listing rather than a scope that genuinely emptied.
    # Treating it as "all deleted" would purge the whole dataset and overwrite
    # known_ids with [], making the loss permanent. Skip deletion and preserve
    # state in that case.
    if known_ids and not current_ids:
        logger.warning(
            "ClinicalTrials.gov: scope sweep returned 0 studies but %d were known; "
            "skipping deletion this run to avoid a mass forget-on-delete on a "
            "transient sweep.",
            len(known_ids),
        )
        state["last_update_posted"] = newest
        logger.info("ClinicalTrials.gov: %d changed study(ies), 0 deletion(s).", len(emitted))
        return

    backfill = sorted(current_ids - known_ids - emitted)
    for study in _iter_by_ids(session, backfill, page_size):
        nct_id = _nct_id(study)
        if not nct_id or nct_id in emitted:
            continue
        emitted.add(nct_id)
        newest = max(newest, _last_update_posted(study))
        yield _study_to_row(study, include_node_sets)

    deleted = known_ids - current_ids
    for nct_id in sorted(deleted):
        yield _deleted_row(nct_id)

    state["known_ids"] = sorted(current_ids | emitted)
    state["last_update_posted"] = newest
    logger.info(
        "ClinicalTrials.gov: %d study(ies) emitted (%d backfilled), %d deletion(s).",
        len(emitted),
        len(backfill),
        len(deleted),
    )


def _iter_by_ids(session: Any, nct_ids: list[str], page_size: int) -> Iterator[dict]:
    """Fetch specific studies by NCT id, in batches."""
    for start in range(0, len(nct_ids), _ID_BATCH):
        batch = nct_ids[start : start + _ID_BATCH]
        if not batch:
            continue
        params = {"filter.ids": ",".join(batch), "pageSize": min(page_size, _MAX_PAGE_SIZE)}
        yield from _iter_pages(session, params)


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------
def clinicaltrials_source(
    *,
    condition: str | None = None,
    sponsor: str | None = None,
    statuses: list[str] | None = None,
    query_term: str | None = None,
    page_size: int = 100,
    detect_deletions: bool = True,
    include_node_sets: bool = True,
    user_agent: str | None = None,
    session: Any = None,
):
    """Return a ``dlt`` resource that yields ClinicalTrials.gov studies.

    At least one scope option is required: syncing the entire registry (about
    half a million studies) is never what a caller means, and the accidental
    full sync is expensive for both sides.

    Args:
        condition: Condition or disease to search for, e.g. ``"type 2
            diabetes"``. Accepts the registry's search syntax, so
            ``"diabetes OR obesity"`` works. This is a relevance search, not an
            exact match.
        sponsor: Sponsor to search for. Also a relevance search, so near
            matches to the name come back too.
        statuses: Exact ``overallStatus`` values to keep, e.g.
            ``["RECRUITING", "COMPLETED"]``.
        query_term: Raw query term passed through to the registry, for scopes
            the options above cannot express.
        page_size: Studies per request, capped at 1000 by the API.
        detect_deletions: Sweep the scope's ids each run so studies that left
            it are forgotten. Costs one id-only pass over the scope.
        include_node_sets: Group studies into node sets by condition, sponsor
            and status.
        user_agent: Override the User-Agent sent to the registry.
        session: Pre-built ``requests`` session. Mainly an injection point for
            tests; when omitted one is built here.

    Returns:
        A ``dlt`` resource (``clinicaltrials_studies``) configured with
        ``primary_key="id"``, ``write_disposition="merge"`` and an ``_deleted``
        hard-delete column, tagged for cognee's document path. Hand it to
        ``cognee.remember(...)``.
    """
    try:
        import dlt
    except ImportError as exc:
        raise ImportError(
            'The ClinicalTrials.gov connector requires the dlt extra: pip install "cognee[dlt]".'
        ) from exc

    if not any([condition, sponsor, statuses, query_term]):
        raise ValueError(
            "clinicaltrials_source requires a scope: pass condition, sponsor, "
            "statuses or query_term."
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
    def clinicaltrials_studies():
        client = session or _make_session(user_agent)
        resource_state = dlt.current.resource_state()
        yield from sync_studies(
            client,
            resource_state,
            condition=condition,
            sponsor=sponsor,
            statuses=statuses,
            query_term=query_term,
            page_size=page_size,
            detect_deletions=detect_deletions,
            include_node_sets=include_node_sets,
        )

    resource = clinicaltrials_studies()
    # Opt into the document ingestion path (study -> text document -> cognify
    # entity extraction). resolve_dlt_sources reads this marker; it never
    # imports this connector. Document mode supports the incremental model used
    # here: write_disposition="merge" plus the _deleted hard-delete tombstones
    # emitted by the scope sweep.
    setattr(resource, DOCUMENT_SOURCE_ATTR, _SOURCE_NAME)
    return resource
