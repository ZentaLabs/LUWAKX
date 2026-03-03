"""Review Flag Collector – in-memory buffer of tags requiring manual review.

During anonymization, `DicomProcessor` encounters situations where a tag could not
be processed automatically as intended (e.g. VR mismatch, LLM found no PHI and kept
the original value).  This module collects structured records of those situations
in memory and returns them – series by series – as plain row dicts via
``flush_series()``.  The caller (``MetadataExporter``) is responsible for
persisting those rows to disk, following the same pattern used for UID mappings
and DICOM metadata.

CSV columns (see ``ReviewFlagCollector.CSV_COLUMNS``):

    anonymized_patient_id   – anonymized patient identifier
    anonymized_study_uid    – anonymized study UID
    anonymized_series_uid   – anonymized series UID
    instance_uid            – SOPInstanceUID of the flagged instance, or ``*`` when every
                              instance in the series has the *same* original value for the tag
    tag_group               – DICOM tag group  (4-char hex, e.g. "0008")
    tag_element             – DICOM tag element (4-char hex, e.g. "103E")
    attribute_name          – human-readable attribute name (keyword when available)
    keyword                 – pydicom keyword (e.g. "SeriesDescription")
    vr                      – Value Representation as stored in the file (e.g. "LO")
    vm                      – Value Multiplicity (int count for this instance)
    reason                  – machine-readable flag reason (see constants below)
    original_value          – raw value before anonymization
    keep                    – 1 = original value was kept, 0 = tag was removed / replaced
    value                   – the actual output value (empty string when removed)
    override_keep           – *user-fillable* (leave blank; luwak never writes this)
    override_value          – *user-fillable* (leave blank; luwak never writes this)
"""

from typing import Any, Dict, List, Tuple


class ReviewFlagCollector:
    """In-memory accumulator of flagged-tag rows for one pipeline worker.

    Lifetime:
        One instance is created per ``DicomProcessor`` (i.e. per pipeline worker).
        All I/O is handled externally by ``MetadataExporter``; this class only
        accumulates, deduplicates, and collapses rows in memory.

    Typical usage::

        collector.set_series_context(patient_id, study_uid, series_uid)
        # … custom functions call collector.add_flag(…) …
        rows = collector.flush_series()   # returns collapsed rows, clears buffer
        exporter.append_series_review_flags(review_flags_file, rows)

    Attributes:
        REASON_VR_MISMATCH:
            A recipe instruction (e.g. ``func:generate_hmacuid``) was applied to a tag
            with a VR that is incompatible with the operation.  The tag was *not* modified;
            its original value was preserved.
        REASON_LLM_VERIFIED_CLEAN:
            The LLM/PHI-detector inspected the tag value and found no PHI.  The original
            value was kept.  Manual verification is still recommended.
        REASON_VR_FORMAT_INVALID:
            deid / pydicom emitted a warning indicating that a stored value does not conform
            to the declared VR format (e.g. ``IS`` field containing letters).  The value may
            or may not have been modified downstream.
        REASON_SQ_REPLACE_NEEDS_REVIEW:
            A tag with VR=SQ was marked for ``replace`` in the recipe template but no
            automatic replacement logic is available (the Final CTP Script column does not
            specify ``@remove()`` or ``removed``).  The original sequence value is kept
            unchanged; manual review is required to determine whether the content contains PHI.
    """

    # ── Reason codes ──────────────────────────────────────────────────────────
    REASON_VR_MISMATCH              = "VR_MISMATCH_OPERATION"
    REASON_LLM_VERIFIED_CLEAN       = "LLM_VERIFIED_CLEAN"
    REASON_VR_FORMAT_INVALID        = "VR_FORMAT_INVALID"
    REASON_SQ_REPLACE_NEEDS_REVIEW  = "SQ_REPLACE_NEEDS_REVIEW"

    # ── CSV schema ────────────────────────────────────────────────────────────
    CSV_COLUMNS: List[str] = [
        "anonymized_patient_id",
        "anonymized_study_uid",
        "anonymized_series_uid",
        "instance_uid",
        "tag_group",
        "tag_element",
        "attribute_name",
        "keyword",
        "vr",
        "vm",
        "reason",
        "original_value",
        "keep",
        "value",
        "override_keep",
        "override_value",
    ]

    # ─────────────────────────────────────────────────────────────────────────

    def __init__(self) -> None:
        """Initialise an empty collector.  No file paths or I/O involved."""
        # Per-series accumulation buffer.
        # Key:   (tag_group: str, tag_element: str, reason: str)
        # Value: list of per-instance dicts
        self._flags: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}

        # Series identification filled by set_series_context()
        self._series_ctx: Dict[str, str] = {
            "anonymized_patient_id": "",
            "anonymized_study_uid":  "",
            "anonymized_series_uid": "",
        }

    # ── Series lifecycle ──────────────────────────────────────────────────────

    def set_series_context(
        self,
        anonymized_patient_id: str,
        anonymized_study_uid: str,
        anonymized_series_uid: str,
    ) -> None:
        """Set series identification used for all subsequent ``add_flag`` calls.

        Must be called once at the start of each series, *before* any ``add_flag``
        call.  Clears the internal buffer.
        """
        self._series_ctx = {
            "anonymized_patient_id": anonymized_patient_id or "",
            "anonymized_study_uid":  anonymized_study_uid  or "",
            "anonymized_series_uid": anonymized_series_uid or "",
        }
        self._flags.clear()

    def add_flag(
        self,
        tag_group:       str,
        tag_element:     str,
        attribute_name:  str,
        keyword:         str,
        vr:              str,
        vm:              str,
        reason:          str,
        sop_instance_uid: str,
        original_value:  str,
        keep:            int,
        output_value:    str,
    ) -> None:
        """Record a flagged tag occurrence for the current series.

        Should be called once per *file* (instance) where the flag condition is
        triggered.  Multiple calls for the same ``(tag_group, tag_element, reason)``
        key are accumulated; ``flush_series`` will collapse them to ``instance_uid='*'``
        when all instances share the same original value.

        Args:
            tag_group:        4-char hex group string (e.g. ``"0008"``).
            tag_element:      4-char hex element string (e.g. ``"103E"``).
            attribute_name:   Human-readable name (use keyword if no better name exists).
            keyword:          pydicom keyword (e.g. ``"SeriesDescription"``).
            vr:               Value Representation string (e.g. ``"LO"``).
            vm:               Value Multiplicity as string (e.g. ``"1"``).
            reason:           One of the ``REASON_*`` class constants.
            sop_instance_uid: ``SOPInstanceUID`` of the DICOM instance, or ``"*"``
                              when not available.
            original_value:   Raw value before anonymization.
            keep:             1 if the original value was kept; 0 if removed/replaced.
            output_value:     Actual output value (empty string when removed).
        """
        key = (tag_group, tag_element, reason)
        entry: Dict[str, Any] = {
            "attribute_name":   attribute_name,
            "keyword":          keyword,
            "vr":               vr,
            "vm":               vm,
            "sop_instance_uid": sop_instance_uid or "*",
            "original_value":   str(original_value),
            "keep":             keep,
            "output_value":     str(output_value),
        }
        if key not in self._flags:
            self._flags[key] = []
        self._flags[key].append(entry)

    # ── Flush ─────────────────────────────────────────────────────────────────

    def flush_series(self) -> List[Dict[str, Any]]:
        """Collapse instance-level entries and return CSV rows for the current series.

        Collapse rule: if all entries for a ``(tag, reason)`` key share the same
        ``original_value``, a single row is emitted with ``instance_uid='*'``.
        Otherwise one row is emitted per distinct ``(sop_instance_uid, original_value)``
        pair (duplicates within the same instance are deduplicated).

        Clears the internal buffer after building the row list.

        Returns:
            List[Dict[str, Any]]: Rows ready to be written to the review-flags CSV.
            Keys match ``CSV_COLUMNS``.  Empty list if nothing was buffered.
        """
        if not self._flags:
            return []

        rows: List[Dict[str, Any]] = []

        for (tag_group, tag_element, reason), entries in self._flags.items():
            meta = entries[0]
            unique_original_values = {e["original_value"] for e in entries}

            if len(unique_original_values) == 1:
                # All instances have the same value – collapse.
                rows.append({
                    **self._series_ctx,
                    "instance_uid":  "*",
                    "tag_group":     tag_group,
                    "tag_element":   tag_element,
                    "attribute_name": meta["attribute_name"],
                    "keyword":       meta["keyword"],
                    "vr":            meta["vr"],
                    "vm":            meta["vm"],
                    "reason":        reason,
                    "original_value": meta["original_value"],
                    "keep":          meta["keep"],
                    "value":         meta["output_value"],
                    "override_keep":  "",
                    "override_value": "",
                })
            else:
                # Different values per instance – one row per unique (uid, value) pair.
                seen: set = set()
                for entry in entries:
                    dedup_key = (entry["sop_instance_uid"], entry["original_value"])
                    if dedup_key in seen:
                        continue
                    seen.add(dedup_key)
                    rows.append({
                        **self._series_ctx,
                        "instance_uid":  entry["sop_instance_uid"],
                        "tag_group":     tag_group,
                        "tag_element":   tag_element,
                        "attribute_name": entry["attribute_name"],
                        "keyword":       entry["keyword"],
                        "vr":            entry["vr"],
                        "vm":            entry["vm"],
                        "reason":        reason,
                        "original_value": entry["original_value"],
                        "keep":          entry["keep"],
                        "value":         entry["output_value"],
                        "override_keep":  "",
                        "override_value": "",
                    })

        self._flags.clear()
        return rows

    def get_pending_keywords_by_reason(self, reason: str) -> set:
        """Return the set of keywords buffered (not yet flushed) for the given reason.

        Call this *before* ``flush_series()`` to produce a summary log message from
        the same data that will be written to the CSV, avoiding duplicate state.

        Args:
            reason: One of the ``REASON_*`` class constants.

        Returns:
            Set[str]: pydicom keyword strings (e.g. ``"SeriesDescription"``) for all
            buffered flags that match *reason*.  Empty set if none.
        """
        return {
            entries[0]["keyword"]
            for (_, _, r), entries in self._flags.items()
            if r == reason and entries
        }

    def is_first_flag(self, tag_group: str, tag_element: str, reason: str) -> bool:
        """Return True if no flag has been recorded yet for this (tag, reason) pair.

        Use this as a deduplication guard to emit a logger warning only on the
        *first* occurrence within a series while still calling ``add_flag()`` for
        every instance.  Must be called *before* the corresponding ``add_flag()``
        call so the buffer is still empty for that key.

        Args:
            tag_group:   4-char hex group string (e.g. ``"0008"``).
            tag_element: 4-char hex element string (e.g. ``"103E"``).
            reason:      One of the ``REASON_*`` class constants.

        Returns:
            bool: True if the ``(tag_group, tag_element, reason)`` key is not yet
            in the buffer.
        """
        return (tag_group, tag_element, reason) not in self._flags
