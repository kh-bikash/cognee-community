"""openFDA connector for cognee: a ``dlt`` source that turns FDA drug labels and
recalls into memory.

Pull drug labels or drug recall (enforcement) records from the public openFDA
API into cognee, incrementally and with forget-on-deletion. A label says what a
drug was approved for, what it must warn about and what goes wrong in practice;
a recall says what went wrong with a specific lot and why. Both are long prose
that a team is otherwise expected to read by hand. Like the sibling Confluence
and Google Drive connectors this builds entirely on the existing DLT ingestion
subsystem; the source produced here is handed directly to
:func:`cognee.remember`::

    import cognee
    from cognee_community_connector_openfda import openfda_source

    await cognee.remember(
        openfda_source(dataset="label", generic_name="metformin"),
        dataset_name="drug_labels",
        primary_key="id",
        write_disposition="merge",   # incremental upsert by set id
        max_rows_per_table=0,        # 0 = no row cap (see note below)
    )

Design
------
* **Auth:** none. An API key is optional and only raises rate limits, so one is
  passed through when given but never required. Access is read-only.
* **Datasets:** ``"label"`` (``/drug/label.json``) and ``"enforcement"``
  (``/drug/enforcement.json``). They have different identity, cursor and text
  fields, so each is described once in ``_DATASETS`` and the sync is shared.
* **Scope:** brand name, generic name, manufacturer or a raw search expression,
  bounded by a date window on the dataset's cursor field.
* **Document mode:** the source declares ``cognee_document_source =
  "openfda"``, so ``resolve_dlt_sources`` tags each row
  ``system_metadata["source"] = "openfda"`` and the record flows through the
  standard cognify entity-extraction pipeline, which is the right treatment for
  prose, instead of the deterministic dlt-row schema-context path.
* **Forget-on-delete:** ``write_disposition="merge"`` with an ``_deleted``
  hard-delete marker; dlt removes those rows on merge and cognee's existing
  ``orphan_cleanup`` then purges them from the graph + vector + relational
  stores. No parallel cleanup path.

Labels are versioned, so the primary key is ``set_id``
-----------------------------------------------------
Every label revision gets a fresh ``id``, while ``set_id`` stays with the
product across revisions and a ``version`` counter increments (a live label was
at version 25). Keying on ``id`` would add a row per revision, so memory would
fill with superseded labels that all look current. Keying on ``set_id`` and
keeping the highest version means one row per product, replaced when a new
revision is published. Enforcement records are not versioned, so their
``recall_number`` is used directly.

The pagination ceiling, and why the window is bisected
------------------------------------------------------
``limit`` caps at 1000 and ``skip`` is rejected above 25000, so a scope holding
more than 25000 records cannot be walked by paging alone. The documented escape
hatch does not work in practice: ``search_after`` answers
``search_after has 1 value(s) but sort has 2``, and neither a comma-joined pair
nor repeated parameters are accepted. Instead, a window reporting more than the
ceiling is split in half and each half walked separately, which stays entirely
within documented parameters. A single day that exceeds the ceiling cannot be
split further, and is reported rather than silently truncated.

The disclaimer travels with the data
------------------------------------
Every openFDA response carries a disclaimer saying the data is unvalidated and
must not be relied on for decisions about medical care. Feeding bare drug text
into an agent's memory while dropping that on the floor would strip exactly the
caveat a reader needs, so it is carried into each document.

What counts as knowledge
------------------------
For labels, the prose sections are the knowledge: indications, contraindications,
warnings, adverse reactions, dosage, interactions, clinical pharmacology and the
rest. Around a third of the 42 label fields are ``*_table`` variants holding
HTML tables that duplicate the prose field beside them in markup, so they are
dropped. For enforcement, the reason for recall and the product description are
the knowledge; the recalling firm's street address and postal code are contact
data, so only its city, state and country are kept.

.. note::
   cognee's ``ingest_dlt_source`` reads at most ``max_rows_per_table`` rows
   from the dlt destination (default 50). For a real scope pass
   ``max_rows_per_table=0`` (unlimited) so orphan-cleanup compares against the
   *whole* synced corpus rather than a truncated window.

.. note::
   openFDA has no field projection, so the walk that would detect deletions
   costs the same as the walk that fetches content. ``detect_deletions=True``
   therefore walks the configured window once and uses it for both purposes,
   while ``detect_deletions=False`` walks only from the stored cursor forward,
   which is cheaper but cannot see a deletion.

.. note::
   The cursor has day granularity and the search range is inclusive, so each
   run re-reads the cursor day. Excluding it would permanently drop a record
   dated later on the same day as the previous run. Unchanged records are
   skipped by the identity check, so the re-read costs bandwidth, not work.
"""

from __future__ import annotations

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

logger = get_logger("openfda_connector")

_SOURCE_NAME = "openfda"
_TABLE_NAME = "openfda_records"

# The API caps limit at 1000 and rejects skip above 25000.
_MAX_LIMIT = 1000
_SKIP_CEILING = 25_000

# Retry budget for rate-limited / transient responses.
_MAX_RETRIES = 5

# Hard ceiling on recursion depth when bisecting an oversized window.
_MAX_BISECT_DEPTH = 24

_DEFAULT_USER_AGENT = (
    "cognee-community-connector-openfda (+https://github.com/topoteretes/cognee-community)"
)

_EXTRA_HINT = (
    'The openFDA connector requires "requests". Install the package:\n'
    "    uv pip install cognee-community-connector-openfda"
)

# Label sections that are prose worth extracting, in reading order. The
# matching *_table fields are deliberately absent: they hold HTML tables that
# duplicate these fields in markup.
_LABEL_SECTIONS = (
    ("boxed_warning", "Boxed warning"),
    ("indications_and_usage", "Indications and usage"),
    ("dosage_and_administration", "Dosage and administration"),
    ("dosage_forms_and_strengths", "Dosage forms and strengths"),
    ("contraindications", "Contraindications"),
    ("warnings_and_cautions", "Warnings and cautions"),
    ("adverse_reactions", "Adverse reactions"),
    ("drug_interactions", "Drug interactions"),
    ("use_in_specific_populations", "Use in specific populations"),
    ("pregnancy", "Pregnancy"),
    ("pediatric_use", "Pediatric use"),
    ("geriatric_use", "Geriatric use"),
    ("overdosage", "Overdosage"),
    ("description", "Description"),
    ("clinical_pharmacology", "Clinical pharmacology"),
    ("mechanism_of_action", "Mechanism of action"),
    ("pharmacodynamics", "Pharmacodynamics"),
    ("pharmacokinetics", "Pharmacokinetics"),
    ("nonclinical_toxicology", "Nonclinical toxicology"),
    ("clinical_studies", "Clinical studies"),
    ("how_supplied", "How supplied"),
    ("storage_and_handling", "Storage and handling"),
    ("information_for_patients", "Information for patients"),
)

_ENFORCEMENT_SECTIONS = (
    ("reason_for_recall", "Reason for recall"),
    ("product_description", "Product description"),
    ("code_info", "Affected lots"),
    ("distribution_pattern", "Distribution pattern"),
)


class _Dataset:
    """How one openFDA endpoint names its identity, cursor and text."""

    def __init__(self, name, endpoint, id_field, cursor_field, versioned, sections, url_prefix):
        self.name = name
        self.endpoint = endpoint
        self.id_field = id_field
        self.cursor_field = cursor_field
        self.versioned = versioned
        self.sections = sections
        self.url_prefix = url_prefix


_DATASETS = {
    "label": _Dataset(
        name="label",
        endpoint="https://api.fda.gov/drug/label.json",
        id_field="set_id",
        cursor_field="effective_time",
        versioned=True,
        sections=_LABEL_SECTIONS,
        url_prefix="https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?setid=",
    ),
    "enforcement": _Dataset(
        name="enforcement",
        endpoint="https://api.fda.gov/drug/enforcement.json",
        id_field="recall_number",
        cursor_field="report_date",
        versioned=False,
        sections=_ENFORCEMENT_SECTIONS,
        url_prefix="",
    ),
}


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


def _api_get(session: Any, endpoint: str, params: dict) -> dict:
    """GET an openFDA endpoint and return parsed JSON, retrying transient errors.

    A rejected parameter comes back as a 400 with a JSON error body, and a
    missing match as a 404, so neither is retried. The body is only parsed after
    ``raise_for_status``, and a non-JSON success body raises a clear error.
    """
    for attempt in range(_MAX_RETRIES):
        try:
            response = session.get(endpoint, params=params)
            response.raise_for_status()
            try:
                return response.json()
            except ValueError as exc:
                raise ValueError(
                    f"openFDA returned a non-JSON body: {response.text[:200]!r}"
                ) from exc
        except Exception as exc:
            if attempt == _MAX_RETRIES - 1 or not _is_transient(exc):
                raise
            delay = _retry_after(_response_headers(exc), attempt)
            logger.warning(
                "openFDA: %s, retrying in %.1fs (%d/%d).",
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


def _is_not_found(exc: Exception) -> bool:
    """True when openFDA answers "no matches" with a 404."""
    import requests

    return isinstance(exc, requests.exceptions.HTTPError) and _status_code(exc) == 404


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
# Search building / paging
# ---------------------------------------------------------------------------
def _scope_terms(
    brand_name: str | None,
    generic_name: str | None,
    manufacturer: str | None,
    search: str | None,
) -> list[str]:
    """Build the search terms that define the scope.

    Brand, generic and manufacturer all live under the shared ``openfda``
    identity block, which both datasets carry, so one spelling works for both.
    """
    terms = []
    if brand_name:
        terms.append(f'openfda.brand_name:"{brand_name}"')
    if generic_name:
        terms.append(f'openfda.generic_name:"{generic_name}"')
    if manufacturer:
        terms.append(f'openfda.manufacturer_name:"{manufacturer}"')
    if search:
        terms.append(f"({search})")
    return terms


def _search_expression(dataset: _Dataset, terms: list[str], since: str, until: str) -> str:
    """Join the scope terms with the dataset's date window."""
    window = f"{dataset.cursor_field}:[{since} TO {until}]"
    return " AND ".join([*terms, window])


def _request_params(search: str, limit: int, skip: int, api_key: str | None) -> dict:
    """Build one page request."""
    params: dict[str, Any] = {"search": search, "limit": limit, "skip": skip}
    if api_key:
        params["api_key"] = api_key
    return params


def _total_for(session: Any, dataset: _Dataset, search: str, api_key: str | None) -> int:
    """How many records match a search, or 0 when openFDA answers 404."""
    try:
        payload = _api_get(session, dataset.endpoint, _request_params(search, 1, 0, api_key))
    except Exception as exc:
        if _is_not_found(exc):
            return 0
        raise
    results = (payload.get("meta") or {}).get("results") or {}
    try:
        return int(results.get("total") or 0)
    except (TypeError, ValueError):
        return 0


def _iter_window(
    session: Any,
    dataset: _Dataset,
    terms: list[str],
    since: str,
    until: str,
    page_size: int,
    api_key: str | None,
    depth: int = 0,
) -> Iterator[dict]:
    """Yield every record in a date window, bisecting when it exceeds the ceiling.

    ``skip`` is rejected above 25000, so a window holding more than that cannot
    be paged to the end. Splitting the window and walking each half keeps every
    request inside documented parameters.
    """
    search = _search_expression(dataset, terms, since, until)
    total = _total_for(session, dataset, search, api_key)
    if total <= 0:
        return

    if total > _SKIP_CEILING:
        midpoint = _midpoint(since, until)
        if midpoint is None or depth >= _MAX_BISECT_DEPTH:
            # A single day over the ceiling cannot be split further. Report it
            # rather than silently returning a truncated slice of it.
            logger.warning(
                "openFDA: %s to %s holds %d records, above the %d skip ceiling, and "
                "cannot be split further; only the first %d will be read. Narrow the "
                "scope to see the rest.",
                since,
                until,
                total,
                _SKIP_CEILING,
                _SKIP_CEILING,
            )
        else:
            yield from _iter_window(
                session, dataset, terms, since, midpoint, page_size, api_key, depth + 1
            )
            yield from _iter_window(
                session,
                dataset,
                terms,
                _next_day(midpoint),
                until,
                page_size,
                api_key,
                depth + 1,
            )
            return

    limit = max(1, min(page_size, _MAX_LIMIT))
    skip = 0
    while skip <= _SKIP_CEILING:
        try:
            payload = _api_get(
                session, dataset.endpoint, _request_params(search, limit, skip, api_key)
            )
        except Exception as exc:
            if _is_not_found(exc):
                return
            raise
        results = payload.get("results")
        if not results:
            return

        disclaimer = _clean((payload.get("meta") or {}).get("disclaimer"))
        for record in results:
            # The disclaimer is per response, so it is attached to each record
            # as it is read rather than fetched again later.
            record.setdefault("_openfda_disclaimer", disclaimer)
            yield record

        if len(results) < limit:
            return
        skip += limit


def _midpoint(since: str, until: str) -> str | None:
    """The date halfway through a ``YYYYMMDD`` window, or None if it is one day."""
    start, end = _to_date(since), _to_date(until)
    if start is None or end is None or start >= end:
        return None
    middle = start + (end - start) / 2
    if middle >= end:
        return None
    return middle.strftime("%Y%m%d")


def _next_day(day: str) -> str:
    """The day after a ``YYYYMMDD`` date, so bisected halves do not overlap."""
    from datetime import timedelta

    parsed = _to_date(day)
    if parsed is None:
        return day
    return (parsed + timedelta(days=1)).strftime("%Y%m%d")


def _to_date(value: str) -> Any:
    """Parse a ``YYYYMMDD`` string, or None when it is not one."""
    from datetime import datetime

    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Field access
# ---------------------------------------------------------------------------
def _clean(value: Any) -> str:
    """Collapse a field to a stripped string, joining openFDA's list fields.

    Most text fields come back as single-element lists, so a naive ``str()``
    would render ``['Do not use ...']`` into the document.
    """
    if value is None:
        return ""
    if isinstance(value, list):
        return " ".join(_clean(item) for item in value if _clean(item)).strip()
    return str(value).strip()


def _clean_terms(value: Any) -> str:
    """Join a multi-valued identity field with commas rather than spaces.

    ``_clean`` joins with a space, which is right for prose split across list
    items. It is wrong for identity fields that hold several distinct values: a
    product with two active substances renders as "CANAGLIFLOZIN METFORMIN
    HYDROCHLORIDE", which reads as one substance, and two brand names collapse
    into "INVOKAMET INVOKAMET XR". Both were real values from a live label.
    """
    if isinstance(value, list):
        terms = [_clean(item) for item in value]
        return ", ".join(dict.fromkeys(term for term in terms if term))
    return _clean(value)


def _openfda_block(record: dict) -> dict:
    """The shared ``openfda`` identity block, or an empty dict."""
    block = record.get("openfda")
    return block if isinstance(block, dict) else {}


def _first(record: dict, key: str) -> str:
    """The first value of an ``openfda`` list field."""
    values = _openfda_block(record).get(key)
    if isinstance(values, list) and values:
        return _clean(values[0])
    return _clean(values)


def _record_id(dataset: _Dataset, record: dict) -> str:
    """The record's stable identity for this dataset."""
    return _clean(record.get(dataset.id_field))


def _cursor_value(dataset: _Dataset, record: dict) -> str:
    """The record's cursor value (``YYYYMMDD``)."""
    return _clean(record.get(dataset.cursor_field))


def _version(record: dict) -> int:
    """A label's revision number, defaulting to 1 when absent or unparseable."""
    try:
        return int(_clean(record.get("version")) or 1)
    except ValueError:
        return 1


def _identity_key(dataset: _Dataset, record: dict) -> str:
    """What state stores: the id, plus the version for versioned datasets."""
    record_id = _record_id(dataset, record)
    if not dataset.versioned:
        return record_id
    return f"{record_id}@{_version(record)}"


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def _title(dataset: _Dataset, record: dict) -> str:
    """A human title for the record."""
    if dataset.name == "enforcement":
        firm = _clean(record.get("recalling_firm"))
        product = _clean(record.get("product_description"))[:80]
        number = _record_id(dataset, record)
        if firm and product:
            return f"Recall {number}: {product} ({firm})"
        return f"Recall {number}"

    brand = _first(record, "brand_name")
    generic = _first(record, "generic_name")
    if brand and generic and brand.lower() != generic.lower():
        return f"{brand} ({generic}) label"
    return f"{brand or generic or _record_id(dataset, record)} label"


def _label_header(record: dict) -> list[str]:
    """Identity lines for a drug label."""
    lines = [f"Set ID: {_clean(record.get('set_id'))}", f"Label version: {_version(record)}"]
    for key, label in (
        ("brand_name", "Brand name"),
        ("generic_name", "Generic name"),
        ("manufacturer_name", "Manufacturer"),
        ("product_type", "Product type"),
        ("route", "Route"),
        ("substance_name", "Active substance"),
        ("pharm_class_epc", "Pharmacologic class"),
        ("application_number", "Application number"),
    ):
        value = _clean_terms(_openfda_block(record).get(key))
        if value:
            lines.append(f"{label}: {value}")
    effective = _clean(record.get("effective_time"))
    if effective:
        lines.append(f"Effective: {_iso_date(effective)}")
    return lines


def _enforcement_header(record: dict) -> list[str]:
    """Identity lines for a recall record."""
    lines = [f"Recall number: {_clean(record.get('recall_number'))}"]
    for key, label in (
        ("classification", "Classification"),
        ("status", "Status"),
        ("recalling_firm", "Recalling firm"),
        ("voluntary_mandated", "Initiated by"),
        ("initial_firm_notification", "Notified via"),
        ("product_quantity", "Quantity"),
    ):
        value = _clean(record.get(key))
        if value and value.upper() != "N/A":
            lines.append(f"{label}: {value}")

    # Street address and postal code are contact data rather than knowledge, so
    # only the coarse location is kept.
    place = ", ".join(
        part
        for part in (
            _clean(record.get("city")),
            _clean(record.get("state")),
            _clean(record.get("country")),
        )
        if part and part.upper() != "N/A"
    )
    if place:
        lines.append(f"Firm location: {place}")

    for key, label in (
        ("recall_initiation_date", "Recall initiated"),
        ("center_classification_date", "Classified"),
        ("report_date", "Reported"),
    ):
        value = _clean(record.get(key))
        if value:
            lines.append(f"{label}: {_iso_date(value)}")
    return lines


def _iso_date(compact: str) -> str:
    """Render a ``YYYYMMDD`` value as ``YYYY-MM-DD``, or pass it through."""
    parsed = _to_date(compact)
    return parsed.isoformat() if parsed else compact


def _content(dataset: _Dataset, record: dict) -> str:
    """Render a record into the text cognify will read."""
    header = _label_header(record) if dataset.name == "label" else _enforcement_header(record)
    sections = ["\n".join(header)]

    for key, label in dataset.sections:
        text = _clean(record.get(key))
        if text:
            sections.append(f"{label}:\n{text}")

    disclaimer = _clean(record.get("_openfda_disclaimer"))
    if disclaimer:
        # Carried deliberately: openFDA attaches this to every response, and an
        # agent answering from drug text should not lose the caveat.
        sections.append(f"openFDA disclaimer:\n{disclaimer}")

    return "\n\n".join(section for section in sections if section.strip())


def _node_sets(dataset: _Dataset, record: dict) -> list[str] | None:
    """Node-set names for a record.

    cognee namespaces these under the source tag, so they cannot collide with
    cognee's own node sets.
    """
    names = []
    if dataset.name == "label":
        for key, prefix in (
            ("generic_name", "generic"),
            ("manufacturer_name", "manufacturer"),
            ("pharm_class_epc", "class"),
        ):
            for value in _openfda_block(record).get(key) or []:
                cleaned = _clean(value)
                if cleaned:
                    names.append(f"{prefix}:{cleaned}")
    else:
        firm = _clean(record.get("recalling_firm"))
        if firm:
            names.append(f"firm:{firm}")
        classification = _clean(record.get("classification"))
        if classification:
            names.append(f"classification:{classification}")
        status = _clean(record.get("status"))
        if status:
            names.append(f"status:{status}")
    # Deterministic order, so an unchanged record keeps a stable content hash.
    return sorted(set(names)) or None


def _record_to_row(dataset: _Dataset, record: dict, include_node_sets: bool) -> dict[str, Any]:
    """Flatten a record into a document-mode row.

    Row contract: ``{id, title, content, url}``. ``resolve_dlt_sources`` turns
    ``title``/``content`` into the document text and keeps ``url``/``id`` as
    provenance.
    """
    record_id = _record_id(dataset, record)
    row: dict[str, Any] = {
        "id": record_id,
        "title": _title(dataset, record),
        "url": f"{dataset.url_prefix}{record_id}" if dataset.url_prefix else "",
        "content": _content(dataset, record),
        # Hard-delete marker (always False for live records). Records that left
        # the scope are emitted separately with _deleted=True.
        "_deleted": False,
    }
    if include_node_sets:
        node_sets = _node_sets(dataset, record)
        if node_sets:
            row[NODE_SET_COLUMN] = node_sets
    return row


def _deleted_row(record_id: str) -> dict[str, Any]:
    """Build a minimal row that instructs dlt to hard-delete a record."""
    return {"id": str(record_id), "_deleted": True}


# ---------------------------------------------------------------------------
# Sync (pure given a session + state dict, so it is unit-testable)
# ---------------------------------------------------------------------------
def sync_records(
    session: Any,
    state: dict,
    *,
    dataset: str = "label",
    brand_name: str | None = None,
    generic_name: str | None = None,
    manufacturer: str | None = None,
    search: str | None = None,
    effective_since: str = "20200101",
    effective_until: str | None = None,
    page_size: int = 100,
    detect_deletions: bool = True,
    include_node_sets: bool = True,
    api_key: str | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield records new or revised since the last run, plus hard-delete markers.

    With ``detect_deletions`` on, the configured window is walked once and used
    both to emit rows and to find ids that have left it. With it off, only the
    window from the stored cursor forward is walked, which is cheaper but
    cannot see a deletion.
    """
    descriptor = _DATASETS[dataset]
    until = effective_until or date.today().strftime("%Y%m%d")
    known: set[str] = set(state.get("known", []))
    known_ids = {key.rsplit("@", 1)[0] if descriptor.versioned else key for key in known}
    cursor: str = str(state.get("last_cursor") or "")
    newest = cursor

    # The full window is needed to see deletions; otherwise start at the cursor.
    # Compact YYYYMMDD dates compare lexicographically.
    since = effective_since if detect_deletions else max(cursor, effective_since)

    terms = _scope_terms(brand_name, generic_name, manufacturer, search)

    # Collapse to one record per id, keeping the highest version where versions
    # exist, so a product with 25 label revisions yields its current label.
    latest: dict[str, dict] = {}
    for record in _iter_window(session, descriptor, terms, since, until, page_size, api_key):
        record_id = _record_id(descriptor, record)
        if not record_id:
            continue
        if (
            record_id not in latest
            or not descriptor.versioned
            or _version(record) > _version(latest[record_id])
        ):
            latest[record_id] = record
        newest = max(newest, _cursor_value(descriptor, record))

    emitted = 0
    fresh_keys: set[str] = set()
    # Sorted by id so the emission order is deterministic across runs.
    for _, record in sorted(latest.items()):
        key = _identity_key(descriptor, record)
        fresh_keys.add(key)
        if key in known:
            # Same id at the same version: nothing has changed.
            continue
        emitted += 1
        yield _record_to_row(descriptor, record, include_node_sets)

    if not detect_deletions:
        state["known"] = sorted(known | fresh_keys)
        state["last_cursor"] = newest
        logger.info(
            "openFDA (%s): %d record(s) emitted, deletion detection off.",
            descriptor.name,
            emitted,
        )
        return

    # An empty window while records were previously known almost always means a
    # transient/failed read rather than a scope that genuinely emptied. Treating
    # it as "all deleted" would purge the whole dataset and overwrite the id set
    # with [], making the loss permanent. Skip deletion and preserve state.
    if known and not latest:
        logger.warning(
            "openFDA (%s): window returned 0 records but %d were known; skipping "
            "deletion this run to avoid a mass forget-on-delete on a transient read.",
            descriptor.name,
            len(known_ids),
        )
        state["last_cursor"] = newest
        logger.info("openFDA (%s): %d record(s) emitted, 0 deletion(s).", descriptor.name, emitted)
        return

    deleted = known_ids - set(latest)
    for record_id in sorted(deleted):
        yield _deleted_row(record_id)

    state["known"] = sorted(fresh_keys)
    state["last_cursor"] = newest
    logger.info(
        "openFDA (%s): %d record(s) emitted, %d deletion(s).",
        descriptor.name,
        emitted,
        len(deleted),
    )


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------
def openfda_source(
    *,
    dataset: str = "label",
    brand_name: str | None = None,
    generic_name: str | None = None,
    manufacturer: str | None = None,
    search: str | None = None,
    effective_since: str = "20200101",
    effective_until: str | None = None,
    page_size: int = 100,
    detect_deletions: bool = True,
    include_node_sets: bool = True,
    api_key: str | None = None,
    user_agent: str | None = None,
    session: Any = None,
):
    """Return a ``dlt`` resource that yields openFDA drug labels or recalls.

    At least one scope term is required. The label endpoint alone holds
    hundreds of thousands of records, and an accidental full sync is expensive
    for both sides.

    Args:
        dataset: ``"label"`` for drug labels or ``"enforcement"`` for recalls.
        brand_name: Brand name to match, e.g. ``"ADVIL"``.
        generic_name: Generic name to match, e.g. ``"metformin"``.
        manufacturer: Manufacturer name to match.
        search: Raw openFDA search expression, for scopes the options above
            cannot express.
        effective_since: Earliest date on the dataset's cursor field, as
            ``YYYYMMDD`` (``effective_time`` for labels, ``report_date`` for
            recalls).
        effective_until: Latest date, ``YYYYMMDD``. Defaults to today.
        page_size: Records per request, capped at 1000 by the API.
        detect_deletions: Walk the whole window each run so records that left
            it are forgotten. Turning this off walks only from the cursor
            forward, which is cheaper but cannot see a deletion.
        include_node_sets: Group records into node sets.
        api_key: Optional openFDA API key. It only raises rate limits; the
            connector works without one.
        user_agent: Override the User-Agent sent to the API.
        session: Pre-built ``requests`` session. Mainly an injection point for
            tests; when omitted one is built here.

    Returns:
        A ``dlt`` resource (``openfda_records``) configured with
        ``primary_key="id"``, ``write_disposition="merge"`` and an ``_deleted``
        hard-delete column, tagged for cognee's document path. Hand it to
        ``cognee.remember(...)``.
    """
    try:
        import dlt
    except ImportError as exc:
        raise ImportError(
            'The openFDA connector requires the dlt extra: pip install "cognee[dlt]".'
        ) from exc

    if dataset not in _DATASETS:
        raise ValueError(f"dataset must be one of {tuple(_DATASETS)}, got {dataset!r}.")
    if not any([brand_name, generic_name, manufacturer, search]):
        raise ValueError(
            "openfda_source requires a scope: pass brand_name, generic_name, "
            "manufacturer or search."
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
    def openfda_records():
        client = session or _make_session(user_agent)
        resource_state = dlt.current.resource_state()
        yield from sync_records(
            client,
            resource_state,
            dataset=dataset,
            brand_name=brand_name,
            generic_name=generic_name,
            manufacturer=manufacturer,
            search=search,
            effective_since=effective_since,
            effective_until=effective_until,
            page_size=page_size,
            detect_deletions=detect_deletions,
            include_node_sets=include_node_sets,
            api_key=api_key,
        )

    resource = openfda_records()
    # Opt into the document ingestion path (record -> text -> cognify entity
    # extraction). resolve_dlt_sources reads this marker; it never imports this
    # connector. Document mode supports the incremental model used here:
    # write_disposition="merge" plus the _deleted hard-delete tombstones.
    setattr(resource, DOCUMENT_SOURCE_ATTR, _SOURCE_NAME)
    return resource
