"""Deface priority election and series ordering.

This module provides the DefacePriorityElector class which is responsible for:
- Electing the primary deface candidate within each
  (patient, study, FrameOfReferenceUID, modality) group.
- Reordering the series list so that within each group the primary series
  always precedes the non-primary members, which is a hard requirement for
  the mask-caching pipeline (the primary must be processed—and its mask
  persisted—before the non-primaries attempt to look up a cached mask).

This concern is intentionally separated from DicomSeriesFactory (construction)
and from DefaceService (defacing execution).
"""

from collections import defaultdict
from typing import Dict, List, Set, Tuple

from dicom_series import DicomSeries


class DefacePriorityElector:
    """Elects primary deface candidates and orders series accordingly.

    A *candidate* series is one whose modality appears in ``best_modalities``
    and that carries a non-empty ``frame_of_reference_uid``.  Among all
    candidates that share the same (patient, study, FrameOfReferenceUID,
    modality) group the *primary* is the series with the largest spatial
    coverage (``spatial_volume_cm3``) breaking ties by the finest voxel size
    (smallest ``min_voxel_size_mm``).

    The ordering guarantee after :meth:`elect_and_sort` is that, for every
    candidate group, the elected primary appears before all other members of
    that group in the returned list.  Non-candidate series retain their
    original relative ordering with respect to each other and are placed where
    their group would naturally fall (first-encounter order is preserved).

    Attributes:
        best_modalities: Upper-cased set of modalities eligible for election.
        logger: Logger instance.
    """

    def __init__(self, best_modalities: List[str], logger) -> None:
        """Initialise DefacePriorityElector.

        Args:
            best_modalities: List of modality strings for which primary
                election should run (e.g. ``["CT", "MR"]``).
            logger: Logger instance used for debug output.
        """
        self.best_modalities: Set[str] = {m.upper() for m in best_modalities}
        self.logger = logger

    def elect_and_sort(self, all_series: List[DicomSeries]) -> List[DicomSeries]:
        """Mark primary deface candidates and return a reordered series list.

        The returned list preserves the original encounter order of groups
        (i.e. the position of a group in the output is determined by the
        position of the first series from that group in the input).  Within
        each candidate group the elected primary is always the first element.

        Args:
            all_series: Flat list of :class:`DicomSeries` objects as produced
                by :class:`DicomSeriesFactory`.

        Returns:
            Reordered list with the same series objects (no copies are made).
            ``is_primary_deface_candidate`` is set to ``True`` on each elected
            primary.
        """
        if not self.best_modalities:
            return list(all_series)

        def _election_key(s: DicomSeries) -> tuple:
            return (s.spatial_volume_cm3 or 0, -(s.min_voxel_size_mm or float('inf')))

        # ── Pass 1: split series into candidate groups and non-candidates ─
        # Candidates are grouped by (patient, study, FOR, modality) and the
        # running primary series per group is tracked simultaneously
        candidate_groups: Dict[Tuple, List[DicomSeries]] = defaultdict(list)
        group_primary:    Dict[Tuple, DicomSeries] = {}
        non_candidates:   List[DicomSeries] = []

        for s in all_series:
            if (s.modality or '').upper() in self.best_modalities and s.frame_of_reference_uid:
                group_key = (
                    s.original_patient_id,
                    s.original_patient_name,
                    s.original_patient_birthdate,
                    s.original_study_uid,
                    s.frame_of_reference_uid,
                    (s.modality or '').upper(),
                )
                candidate_groups[group_key].append(s)
                if group_key not in group_primary or _election_key(s) > _election_key(group_primary[group_key]):
                    group_primary[group_key] = s
            else:
                non_candidates.append(s)

        # ── Pass 2: mark primary series, log, and rebuild ordered list ─────────
        # Iterate over groups: mark the elected primary series,
        # emit it first, then append the remaining group members in their
        # original order.  Non-candidates are appended at the end; their
        # relative ordering to candidate groups is irrelevant for the pipeline.
        result: List[DicomSeries] = []

        for group_key, group_series in candidate_groups.items():
            primary = group_primary[group_key]
            primary.is_primary_deface_candidate = True

            _, _, _, study_uid, for_uid, modality = group_key
            if getattr(primary, 'spatial_volume_cm3', None) is not None:
                self.logger.info(
                    f"Primary deface candidate  study={study_uid!r} "
                    f"FOR={for_uid!r} modality={modality}: "
                    f"series {primary.original_series_uid} "
                    f"(volume={primary.spatial_volume_cm3:.3f} cm³, "
                    f"min_voxel={primary.min_voxel_size_mm:.2f} mm)"
                )
            else:
                self.logger.info(
                    f"Primary deface candidate  study={study_uid!r} "
                    f"FOR={for_uid!r} modality={modality}: "
                    f"series {primary.original_series_uid}"
                )

            result.append(primary)
            result.extend(s for s in group_series if s is not primary)

        result.extend(non_candidates)
        return result
