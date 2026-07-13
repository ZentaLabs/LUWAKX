"""Deface priority election and series ordering.

This module provides the DefacePriorityElector class which is responsible for:

1. **Mixed CT+PET group** - when CT and PET series share the same
   (patient, study, FrameOfReferenceUID), each PET is paired with the CT
   whose ``AcquisitionDateTime`` is closest to its own.  The ML model runs
   on that CT, and the resulting mask is projected onto the paired PET.
   Multiple PETs can be paired with the same CT.

2. **CT-only group** - each CT series runs the ML model independently;
   no mask is shared between CT series.

3. **Ordering guarantee** - the returned list always places *all* elected
   primary CT series *before* their dependent PET series, ensuring the mask is
   persisted to ``DefaceMaskDatabase`` before any dependent PET attempts to
   retrieve it.

This concern is intentionally separated from DicomSeriesFactory (construction)
and from DefaceService (defacing execution).
"""

from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from .deface_mask_database import DefaceMaskDatabase
from ..dicom.dicom_series import DicomSeries


class DefacePriorityElector:
    """Elects primary deface candidates and orders series accordingly.

    A series is a *primary candidate* when its modality is in
    ``best_modalities`` and it has a non-empty ``frame_of_reference_uid``.

    Within a (patient, study, FrameOfReferenceUID) group the election strategy
    depends on which modalities are present:

    * **Mixed CT+PET group**: each PET is individually paired with the CT
      whose ``AcquisitionDateTime`` is closest to that PET's own.  All
      elected CTs are marked ``is_primary_deface_candidate = True``.
    * **CT-only group**: no election takes place; every CT runs the ML model
      independently with no mask sharing or DB persistence.

    Attributes:
        best_modalities: Upper-cased set of modalities eligible for primary
            election (source modalities, e.g. ``{"CT"}``).
        logger: Logger instance.
    """

    def __init__(
        self,
        best_modalities: List[str],
        logger,
        deface_mask_db: Optional[DefaceMaskDatabase] = None,
    ) -> None:
        """Initialise DefacePriorityElector.

        Args:
            best_modalities: List of modality strings for which primary
                election should run (only  ``["CT"]`` currently implemented).
            logger: Logger instance used for info/debug output.
            deface_mask_db: Optional :class:`DefaceMaskDatabase` instance.  When
                provided, PET/CT pairings are written to the
                ``deface_series_pairing`` table immediately after election so
                that the decisions survive process restarts.
        """
        self.best_modalities: Set[str] = {m.upper() for m in best_modalities}
        self.logger = logger
        self.deface_mask_db: Optional[DefaceMaskDatabase] = deface_mask_db

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def elect_and_sort(self, all_series: List[DicomSeries]) -> List[DicomSeries]:
        """Mark primary deface candidates, pair secondary series, and reorder.

        Processing guarantees after this call:
        * ``is_primary_deface_candidate`` is ``True`` only on CTs elected to
          serve a paired PET series (mixed groups).
        * Every secondary (e.g. PET) series has its ``primary_ct_series``
          attribute set to the CT that is closest in time to it.
        * For every mixed group all elected primaries appear *before* their
          paired secondary series in the returned list.

        Args:
            all_series: Flat list of :class:`DicomSeries` objects as produced
                by :class:`DicomSeriesFactory`.

        Returns:
            Reordered list with the same series objects (no copies are made).
        """
        if not self.best_modalities:
            return list(all_series)

        # Pass 1: partition into FOR-groups and no-FOR remainder
        # for_groups[group_key][modality] = list of series
        for_groups: Dict[Tuple, Dict[str, List[DicomSeries]]] = defaultdict(
            lambda: defaultdict(list)
        )
        no_for_series: List[DicomSeries] = []

        for s in all_series:
            if s.frame_of_reference_uid:
                group_key = (
                    s.original_patient_id,
                    s.original_patient_name,
                    s.original_patient_birthdate,
                    s.original_study_uid,
                    s.frame_of_reference_uid,
                )
                for_groups[group_key][(s.modality or '').upper()].append(s)
            else:
                no_for_series.append(s)

        # Pass 2: elect within each group, link secondaries
        # group_order captures the 4-tuple (primary_or_None, secondaries, other_primaries, other_mods)
        # in group-discovery order.
        result_groups: List[Tuple] = []

        for group_key, mod_groups in for_groups.items():
            primary_candidates: List[DicomSeries] = []
            secondary_series: List[DicomSeries] = []

            for mod, series_list in mod_groups.items():
                if mod in self.best_modalities:
                    primary_candidates.extend(series_list)
                else:
                    secondary_series.extend(series_list)

            _, _, _, study_uid, for_uid = group_key

            if primary_candidates and secondary_series:
                # Mixed group: per-PET AcquisitionDateTime-based election
                # For each secondary (PET) series independently elect the CT
                # that is closest in acquisition time to that PET.  A CT can be
                # chosen for more than one PET; all distinct elected CTs are
                # marked as primary deface candidates.  Each elected CT records
                # the UID(s) of the PET(s) it was elected for so that the mask
                # database can store and retrieve masks per (CT, PET) pair.
                elected_cts: Dict[str, DicomSeries] = {}  # series_uid -> series

                for sec in secondary_series:
                    ct = self._closest_ct_for_pet(primary_candidates, sec)
                    sec.primary_ct_series = ct
                    uid = ct.original_series_uid
                    elected_cts[uid] = ct
                    self.logger.info(
                        f"PET/CT pairing patient={sec.anonymized_patient_id!r} "
                        f"study={sec.anonymized_study_uid!r}: "
                        f"PET series {sec.anonymized_series_uid!r} "
                        f"with CT series {ct.anonymized_series_uid!r} "
                    )
                    # Safety net: warn if even the best-covering CT does not
                    # reach the PET's head band.  The projected mask cannot cover
                    # the face, so the PET output must be checked manually (the
                    # face may be left exposed).
                    coverage = self._head_coverage(ct, sec)
                    if coverage is not None and coverage < self._MIN_HEAD_COVERAGE:
                        self.logger.warning(
                            f"Elected CT series {ct.anonymized_series_uid!r} covers only "
                            f"{coverage:.0%} of the head region of PET series "
                            f"{sec.anonymized_series_uid!r} (no CT in the group fully covers "
                            f"the head). The projected face mask may miss the face - verify "
                            f"this PET output manually."
                        )
                    if self.deface_mask_db is not None:
                        self.deface_mask_db.upsert_pairing(
                            study_instance_uid     = sec.original_study_uid,
                            frame_of_reference_uid = for_uid,
                            pet_series_uid         = sec.original_series_uid,
                            ct_series_uid          = ct.original_series_uid,
                        )

                for ct in elected_cts.values():
                    ct.is_primary_deface_candidate = True

                non_elected_cts = [s for s in primary_candidates
                                   if s.original_series_uid not in elected_cts]
                result_groups.append(
                    (list(elected_cts.values()), secondary_series, non_elected_cts, [])
                )

            elif primary_candidates:
                # CT-only group: no election, each CT runs ML independently
                result_groups.append(([], [], primary_candidates, secondary_series))

            else:
                # Secondary-only group (e.g. standalone PET, no CT)
                result_groups.append(([], [], [], secondary_series))

        # Pass 3: rebuild ordered list
        # elected_primaries is always a list (possibly empty).  All elected
        # primaries appear before secondaries and non-elected candidates so that
        # mask DB entries are written before any dependent PET series reads them.
        result: List[DicomSeries] = []
        for elected_primaries, secondaries, other_primaries, other_mods in result_groups:
            result.extend(elected_primaries)
            result.extend(secondaries)
            result.extend(other_primaries)
            result.extend(other_mods)

        result.extend(no_for_series)
        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _closest_ct_for_pet(
        self,
        primary_candidates: List[DicomSeries],
        pet: DicomSeries,
    ) -> DicomSeries:
        """Return the best CT candidate to pair with ``pet``.

        Candidates are ranked by ``(head coverage, time, reformat penalty)``:

        1. **Head coverage (primary).** The CT must actually cover the PET's
           head region (the superior end of the PET, where the face is).  A
           limited-FOV diagnostic CT - e.g. a thorax-only scan - does not
           contain the head, so a "face" mask segmented on it lands on the
           shoulders and leaves the PET face exposed.  Candidates are sorted by
           how much of the PET's head band they cover (most first).  This is a
           spatial test and does not depend on acquisition timing, which is
           unreliable for whole-body PET (its per-bed ``AcquisitionTime`` spans
           the whole scan, and ``AcquisitionDateTime`` is often absent).
        2. **Time distance.** Among CTs that cover the head equally, the one
           whose ``AcquisitionDateTime`` is closest to this PET's (the
           co-acquired CT).
        3. **Reformat penalty (final tie-break).** When coverage and time are
           equal - e.g. an axial CT and its ``cor``/``sag`` MPR reformats, which
           share an acquisition time - a native axial acquisition is preferred
           over an MPR reformat / localizer.

        When z-extents are unavailable (e.g. a stale metadata cache), coverage
        is treated as neutral and the decision falls back to reformat + time.

        Args:
            primary_candidates: Non-empty list of primary-modality series (CT).
            pet: The individual secondary-modality series (PET) to match.

        Returns:
            The elected primary :class:`DicomSeries`.
        """
        pet_dt = self._parse_dicom_datetime(pet.acquisition_datetime)

        def sort_key(candidate: DicomSeries) -> Tuple[float, float, int]:
            cov = self._head_coverage(candidate, pet)
            # More coverage sorts first; None (unknown) -> 0.0, i.e. neutral.
            coverage_rank = -round(cov, 2) if cov is not None else 0.0
            cdt = self._parse_dicom_datetime(candidate.acquisition_datetime)
            if pet_dt is None or cdt is None:
                time_score = float('inf')
            else:
                time_score = abs((cdt - pet_dt).total_seconds())
            return (coverage_rank, time_score, self._ct_reformat_penalty(candidate))

        return min(primary_candidates, key=sort_key)

    # Fraction of the PET head band a CT must cover to be considered a valid
    # face-mask source; below this the pairing is flagged for manual review.
    _HEAD_BAND_MM: float = 150.0
    _MIN_HEAD_COVERAGE: float = 0.5

    @classmethod
    def _head_coverage(cls, ct: DicomSeries, pet: DicomSeries) -> Optional[float]:
        """Fraction (0..1) of the PET's head band that ``ct`` covers in z.

        The head band is the most-superior ``_HEAD_BAND_MM`` of the PET's
        z-extent (LPS z increases toward the head, so the face is at the top).
        Returns ``None`` when either series' z-extent is unknown.
        """
        ct_z, pet_z = getattr(ct, 'z_extent', None), getattr(pet, 'z_extent', None)
        if not ct_z or not pet_z:
            return None
        pet_top = pet_z[1]
        band_lo, band_hi = pet_top - cls._HEAD_BAND_MM, pet_top
        overlap = min(band_hi, ct_z[1]) - max(band_lo, ct_z[0])
        return max(0.0, overlap) / cls._HEAD_BAND_MM

    @staticmethod
    def _ct_reformat_penalty(series: DicomSeries) -> int:
        """Rank a CT for face-segmentation suitability (lower is better).

        Returns ``1`` for MPR reformats, reformatted reconstructions and
        localizers/scouts - derived views that are poor sources for face
        segmentation and world-coordinate mask projection - and ``0`` for a
        native (typically axial) acquisition.  Based on ImageType (0008,0008);
        an absent/empty ImageType is treated as a native acquisition.
        """
        tokens = {str(t).upper() for t in (getattr(series, 'image_type', ()) or ())}
        joined = ' '.join(tokens)
        is_reformat = (
            'MPR' in joined
            or 'REFORMATTED' in tokens
            or 'LOCALIZER' in tokens
            or 'PROJECTION IMAGE' in joined
        )
        return 1 if is_reformat else 0

    @staticmethod
    def _parse_dicom_datetime(dt_str: str) -> Optional[datetime]:
        """Parse a DICOM DT string (0008,002A) to a Python :class:`datetime`.

        Handles the forms produced by combining AcquisitionDate + AcquisitionTime:

        * ``YYYYMMDDHHMMSS.FFFFFF[+/-HHMM]``
        * ``YYYYMMDDHHMMSS``
        * ``YYYYMMDDHHMM``
        * ``YYYYMMDD``

        Args:
            dt_str: Raw DICOM DT/DA+TM combined string value (may be empty).

        Returns:
            Parsed :class:`datetime`, or ``None`` if the string is empty or
            unparseable.
        """
        if not dt_str:
            return None
        s = str(dt_str).strip()
        # Strip timezone offset (+HHMM or -HHMM appended after position 8)
        for sep in ('+', '-'):
            pos = s.rfind(sep)
            if pos > 8:
                s = s[:pos]
                break
        # Try formats in decreasing specificity; for the fractional-second form
        # use the full string (no truncation).
        for fmt, max_len in (
            ('%Y%m%d%H%M%S.%f', None),
            ('%Y%m%d%H%M%S',    14),
            ('%Y%m%d%H%M',      12),
            ('%Y%m%d',           8),
        ):
            try:
                val = s if max_len is None else s[:max_len]
                if not val:
                    continue
                return datetime.strptime(val, fmt)
            except (ValueError, TypeError):
                pass
        return None

