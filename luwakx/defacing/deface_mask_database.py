#!/usr/bin/env python

"""
Deface mask database: two-table SQLite store for PET/CT defacing state.

Two tables live in the same database file:

``deface_mask_cache``
    Content store.  One row per (patient, study, FrameOfReference, modality,
    ct_series_instance_uid).  Written by :class:`DefaceService` after the ML
    face mask is computed for each CT series.  Cache hit requires an exact
    series UID match so that geometry is guaranteed to be identical.

    Cache key is SHA-256 of:
        project_hash_root || PatientID || PatientName || PatientBirthDate ||
        StudyInstanceUID  || FrameOfReferenceUID

``deface_series_pairing``
    Election record.  One row per (study, FrameOfReference, PET series).
    Written by :class:`DefacePriorityElector` *before* any series is processed,
    recording which CT series was elected as the temporal closest match for each
    PET.  ``mask_path`` is NULL until the paired CT mask has been computed and
    is filled in by :class:`DefaceService`.

See conformance documentation ("Deface Mask Cache Database" section):
https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md
"""

import json
import os
import sqlite3
import threading
import hashlib
import traceback
from typing import Any, Dict, List, Optional

from ..logging.luwak_logger import get_logger, log_project_stacktrace


class DefaceMaskDatabase:
    """Thread-safe SQLite database for caching primary defacing masks per spatial reference.

    For each combination of (patient, FrameOfReferenceUID, modality) only the mask
    with the *largest spatial volume* and *finest voxel resolution* (smallest voxel
    dimension) is kept. This ensures that subsequent series sharing the same frame
    can reuse a pre-computed high-quality mask instead of running the ML model again.

    The stored mask is a SimpleITK-compatible NRRD file located inside the private
    mapping folder. Together with the stored origin/spacing/direction the mask can
    be resampled onto any other series that shares the same FrameOfReferenceUID.

    Supports concurrent reads and serialised writes (WAL mode) for parallel
    single-node processing.
    """

    def __init__(self, db_path: str, project_hash_root: str = '') -> None:
        """Initialise the deface mask cache database.

        Args:
            db_path: Path to the SQLite database file.  The parent directory is
                     created automatically if it does not exist.
            project_hash_root: Optional project identifier.  Including it in the
                               hash guarantees that the same patient in two
                               different projects gets independent cache entries.
        """
        self.db_path = db_path
        self.project_hash_root = project_hash_root
        self.logger = get_logger('deface_mask_db')

        # Serialise writes; reads are concurrent (WAL mode).
        self._write_lock = threading.Lock()

        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        self.conn = sqlite3.connect(
            db_path,
            check_same_thread=False,
            timeout=30.0,
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA synchronous=NORMAL')
        self.conn.execute('PRAGMA busy_timeout=30000')

        self._create_tables()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _create_tables(self) -> None:
        """Create the two-table database schema if it does not already exist.

        Tables
        ------
        deface_mask_cache
            Content store keyed on ``(cache_key, modality)``.  Written by
            :class:`DefaceService` when a ML mask is computed for a primary CT.
        deface_series_pairing
            Election record keyed on
            ``(study_instance_uid, frame_of_reference_uid, pet_series_uid)``.
            Written by :class:`DefacePriorityElector` before processing starts.
        """
        cursor = self.conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS deface_mask_cache (
                cache_key              TEXT    NOT NULL,
                modality               TEXT    NOT NULL,
                ct_series_instance_uid TEXT    NOT NULL DEFAULT '',
                mask_path              TEXT    NOT NULL,
                spacing                TEXT,
                origin                 TEXT,
                direction              TEXT,
                frame_of_reference_uid TEXT,
                study_instance_uid     TEXT,
                anonymized_patient_id  TEXT,
                anonymized_study_uid   TEXT,
                created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (cache_key, modality, ct_series_instance_uid)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS deface_series_pairing (
                study_instance_uid      TEXT NOT NULL,
                frame_of_reference_uid  TEXT NOT NULL,
                pet_series_uid          TEXT NOT NULL,
                ct_series_uid           TEXT NOT NULL,
                mask_path               TEXT,
                elected_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                mask_written_at         TIMESTAMP,
                PRIMARY KEY (study_instance_uid, frame_of_reference_uid, pet_series_uid)
            )
        ''')

        # Migrations
        existing_cols = {row[1] for row in cursor.execute('PRAGMA table_info(deface_mask_cache)')}

        if 'study_instance_uid' not in existing_cols:
            cursor.execute('ALTER TABLE deface_mask_cache ADD COLUMN study_instance_uid TEXT')

        # An intermediate schema added linked_pet_series_uid to the PK of
        # deface_mask_cache.  Recreate the table with the correct
        # (cache_key, modality) PK, preserving only homogeneous-group rows.
        if 'linked_pet_series_uid' in existing_cols:
            self._migrate_remove_pet_uid_from_mask_cache(cursor)

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_dmc_cache_key
            ON deface_mask_cache (cache_key, modality, ct_series_instance_uid)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_dsp_ct_series
            ON deface_series_pairing (study_instance_uid, frame_of_reference_uid, ct_series_uid)
        ''')

        self.conn.commit()

    def _migrate_remove_pet_uid_from_mask_cache(self, cursor) -> None:
        """Recreate ``deface_mask_cache`` without the intermediate ``linked_pet_series_uid`` column.

        This migration handles the transitional schema where
        ``linked_pet_series_uid`` was erroneously part of the primary key.
        Only homogeneous-group rows (``linked_pet_series_uid = ''``) are
        preserved; per-PET-linked rows are discarded because the
        ``deface_series_pairing`` table now owns that relationship.
        """
        self.logger.info(
            'Migrating deface_mask_cache: removing linked_pet_series_uid from schema'
        )
        cursor.execute('''
            CREATE TABLE deface_mask_cache_v2 (
                cache_key              TEXT    NOT NULL,
                modality               TEXT    NOT NULL,
                mask_path              TEXT    NOT NULL,
                spacing                TEXT,
                origin                 TEXT,
                direction              TEXT,
                frame_of_reference_uid TEXT,
                study_instance_uid     TEXT,
                ct_series_instance_uid TEXT,
                anonymized_patient_id  TEXT,
                anonymized_study_uid   TEXT,
                created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (cache_key, modality)
            )
        ''')
        cursor.execute('''
            INSERT OR IGNORE INTO deface_mask_cache_v2
                (cache_key, modality, mask_path,
                 spacing, origin, direction, frame_of_reference_uid, study_instance_uid,
                 anonymized_patient_id, anonymized_study_uid, created_at, updated_at)
            SELECT cache_key, modality, mask_path,
                   spacing, origin, direction, frame_of_reference_uid, study_instance_uid,
                   anonymized_patient_id, anonymized_study_uid, created_at, updated_at
            FROM   deface_mask_cache
            WHERE  linked_pet_series_uid = ''
        ''')
        cursor.execute('DROP TABLE deface_mask_cache')
        cursor.execute('ALTER TABLE deface_mask_cache_v2 RENAME TO deface_mask_cache')
        self.conn.commit()
        self.logger.info(
            'Migration complete: deface_mask_cache rebuilt with (cache_key, modality) PK'
        )

    # ------------------------------------------------------------------
    # Key computation
    # ------------------------------------------------------------------

    def _compute_key(
        self,
        patient_id: str,
        patient_name: str,
        birthdate: str,
        study_instance_uid: str,
        frame_of_reference_uid: str,
    ) -> str:
        """Compute a deterministic SHA-256 cache key.

        Identifiers are normalised (stripped, uppercased) before hashing so that
        minor metadata variations do not create duplicate entries.

        Args:
            patient_id: Original DICOM PatientID.
            patient_name: Original DICOM PatientName.
            birthdate: Original DICOM PatientBirthDate.
            study_instance_uid: DICOM StudyInstanceUID (0020,000D).
            frame_of_reference_uid: DICOM FrameOfReferenceUID (0020,0052).

        Returns:
            Hex-encoded SHA-256 digest.
        """
        def norm(s: str) -> str:
            return ' '.join(str(s).strip().upper().split())

        combined = (
            f"{self.project_hash_root}||"
            f"{norm(patient_id)}||{norm(patient_name)}||"
            f"{norm(birthdate)}||{norm(study_instance_uid)}||"
            f"{norm(frame_of_reference_uid)}"
        )
        return hashlib.sha256(combined.encode('utf-8')).hexdigest()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_primary_mask(
        self,
        patient_id: str,
        patient_name: str,
        birthdate: str,
        study_instance_uid: str,
        frame_of_reference_uid: str,
        modality: str,
        ct_series_instance_uid: str = '',
    ) -> Optional[Dict[str, Any]]:
        """Retrieve the cached mask for the given series.

        Looks up the exact row using ``(cache_key, modality, ct_series_instance_uid)``
        so that geometry is guaranteed to be identical on a cache hit.
        PET lookups use :meth:`get_pairing` instead.

        Read-only; thread-safe for concurrent callers.

        Args:
            patient_id: Original DICOM PatientID.
            patient_name: Original DICOM PatientName.
            birthdate: Original DICOM PatientBirthDate.
            study_instance_uid: DICOM StudyInstanceUID.
            frame_of_reference_uid: DICOM FrameOfReferenceUID.
            modality: DICOM Modality string (e.g. ``"CT"``, ``"MR"``).
            ct_series_instance_uid: Original SeriesInstanceUID of the CT series.

        Returns:
            Dictionary with keys
                ``mask_path``, ``spacing`` (list[float]), ``origin`` (list[float]),
                ``direction`` (list[float]), ``frame_of_reference_uid``,
                ``anonymized_patient_id``, ``anonymized_study_uid``
            or ``None`` if no entry exists.
        """
        cache_key = self._compute_key(patient_id, patient_name, birthdate, study_instance_uid, frame_of_reference_uid)
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                '''
                SELECT mask_path,
                       spacing, origin, direction,
                       frame_of_reference_uid, study_instance_uid,
                       ct_series_instance_uid,
                       anonymized_patient_id, anonymized_study_uid
                FROM   deface_mask_cache
                WHERE  cache_key = ? AND modality = ? AND ct_series_instance_uid = ?
                ''',
                (cache_key, modality, ct_series_instance_uid or ''),
            )
            row = cursor.fetchone()
            if row is None:
                return None

            # Guard against mask files deleted outside of Luwak (e.g. manual
            # cleanup, or a crashed run that left the DB entry but removed the
            # file during pre-resume cleanup).  Treat a missing file as a cache
            # miss so the pipeline re-runs ML inference and updates the entry.
            mask_path = row['mask_path']
            if mask_path and not os.path.exists(mask_path):
                self.logger.warning(
                    f'DefaceMaskDatabase: cached mask file missing on disk, '
                    f'treating as cache miss: {mask_path}'
                )
                return None

            return {
                'mask_path':              mask_path,
                'spacing':                json.loads(row['spacing'])   if row['spacing']   else None,
                'origin':                 json.loads(row['origin'])    if row['origin']    else None,
                'direction':              json.loads(row['direction'])  if row['direction'] else None,
                'frame_of_reference_uid': row['frame_of_reference_uid'],
                'study_instance_uid':     row['study_instance_uid'],
                'ct_series_instance_uid': row['ct_series_instance_uid'],
                'anonymized_patient_id':  row['anonymized_patient_id'],
                'anonymized_study_uid':   row['anonymized_study_uid'],
            }

        except Exception as e:
            log_project_stacktrace(self.logger, e)
            self.logger.warning(f'DefaceMaskDatabase.get_primary_mask failed: {e}')
            return None

    def upsert_mask(
        self,
        patient_id: str,
        patient_name: str,
        birthdate: str,
        study_instance_uid: str,
        frame_of_reference_uid: str,
        modality: str,
        mask_path: str,
        spacing: Optional[list] = None,
        origin: Optional[list] = None,
        direction: Optional[list] = None,
        anonymized_patient_id: Optional[str] = None,
        anonymized_study_uid: Optional[str] = None,
        ct_series_instance_uid: Optional[str] = None,
    ) -> None:
        """Insert or replace a mask entry in ``deface_mask_cache``.

        Called by :class:`DefaceService` after the ML mask is computed for a
        primary CT candidate.  Replaces any existing entry unconditionally.

        Thread-safe (serialised write).

        Args:
            patient_id: Original DICOM PatientID.
            patient_name: Original DICOM PatientName.
            birthdate: Original DICOM PatientBirthDate.
            study_instance_uid: DICOM StudyInstanceUID.
            frame_of_reference_uid: DICOM FrameOfReferenceUID.
            modality: DICOM Modality string.
            mask_path: Path to the NRRD mask file (relative to ``private_folder``).
            spacing: SimpleITK spacing [sx, sy, sz] in mm.
            origin: SimpleITK origin [ox, oy, oz] in mm (LPS).
            direction: SimpleITK direction as 9-float row-major list.
            anonymized_patient_id: Anonymised patient ID for path reconstruction.
            anonymized_study_uid: Anonymised study UID for path reconstruction.
            ct_series_instance_uid: Original SeriesInstanceUID of the CT that
                produced the mask (stored for auditability).
        """
        cache_key = self._compute_key(patient_id, patient_name, birthdate, study_instance_uid, frame_of_reference_uid)

        spacing_json   = json.dumps(list(spacing))   if spacing   is not None else None
        origin_json    = json.dumps(list(origin))    if origin    is not None else None
        direction_json = json.dumps(list(direction)) if direction is not None else None

        with self._write_lock:
            try:
                cursor = self.conn.cursor()
                _ct_uid = ct_series_instance_uid or ''
                cursor.execute(
                    '''
                    INSERT INTO deface_mask_cache
                        (cache_key, modality, ct_series_instance_uid, mask_path,
                         spacing, origin, direction, frame_of_reference_uid, study_instance_uid,
                         anonymized_patient_id, anonymized_study_uid,
                         updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(cache_key, modality, ct_series_instance_uid) DO UPDATE SET
                        mask_path              = excluded.mask_path,
                        spacing                = excluded.spacing,
                        origin                 = excluded.origin,
                        direction              = excluded.direction,
                        frame_of_reference_uid = excluded.frame_of_reference_uid,
                        study_instance_uid     = excluded.study_instance_uid,
                        anonymized_patient_id  = excluded.anonymized_patient_id,
                        anonymized_study_uid   = excluded.anonymized_study_uid,
                        updated_at             = CURRENT_TIMESTAMP
                    ''',
                    (
                        cache_key, modality, _ct_uid, mask_path,
                        spacing_json, origin_json, direction_json,
                        frame_of_reference_uid, study_instance_uid,
                        anonymized_patient_id, anonymized_study_uid,
                    ),
                )
                self.conn.commit()
                self.logger.debug(
                    f'Upserted deface mask  modality={modality}  FOR={frame_of_reference_uid}'
                )
            except Exception as e:
                log_project_stacktrace(self.logger, e)
                self.logger.warning(f'DefaceMaskDatabase.upsert_mask failed: {e}')

    # ------------------------------------------------------------------
    # Pairing table API
    # ------------------------------------------------------------------

    def upsert_pairing(
        self,
        study_instance_uid: str,
        frame_of_reference_uid: str,
        pet_series_uid: str,
        ct_series_uid: str,
    ) -> None:
        """Record or update the CT elected as primary for a given PET.

        Called by :class:`DefacePriorityElector` after election, before any
        series processing begins.  Writes to ``deface_series_pairing``.
        ``mask_path`` and ``mask_written_at`` are left NULL; they are filled in
        later by :meth:`update_pairing_mask_path`.

        Thread-safe (serialised write).

        Args:
            study_instance_uid: DICOM StudyInstanceUID shared by PET and CT.
            frame_of_reference_uid: DICOM FrameOfReferenceUID shared by PET and CT.
            pet_series_uid: Original SeriesInstanceUID of the PET series.
            ct_series_uid: Original SeriesInstanceUID of the elected CT primary.
        """
        with self._write_lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute(
                    '''
                    INSERT INTO deface_series_pairing
                        (study_instance_uid, frame_of_reference_uid,
                         pet_series_uid, ct_series_uid)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(study_instance_uid, frame_of_reference_uid, pet_series_uid)
                    DO UPDATE SET
                        ct_series_uid = excluded.ct_series_uid,
                        elected_at    = CURRENT_TIMESTAMP
                    ''',
                    (study_instance_uid, frame_of_reference_uid, pet_series_uid, ct_series_uid),
                )
                self.conn.commit()
                self.logger.debug(
                    f'Upserted pairing: PET {pet_series_uid!r} -> CT {ct_series_uid!r}'
                )
            except Exception as e:
                log_project_stacktrace(self.logger, e)
                self.logger.warning(f'DefaceMaskDatabase.upsert_pairing failed: {e}')

    def update_pairing_mask_path(
        self,
        study_instance_uid: str,
        frame_of_reference_uid: str,
        pet_series_uid: str,
        mask_path: str,
    ) -> None:
        """Set ``mask_path`` and ``mask_written_at`` on an existing pairing row.

        Called by :class:`DefaceService` after the CT mask NRRD file is saved.
        The ``mask_path`` is stored relative to ``private_folder`` (same
        convention as ``deface_mask_cache``).

        Thread-safe (serialised write).

        Args:
            study_instance_uid: DICOM StudyInstanceUID.
            frame_of_reference_uid: DICOM FrameOfReferenceUID.
            pet_series_uid: Original SeriesInstanceUID of the PET series.
            mask_path: Path to the NRRD mask (relative to ``private_folder``).
        """
        with self._write_lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute(
                    '''
                    UPDATE deface_series_pairing
                    SET    mask_path = ?, mask_written_at = CURRENT_TIMESTAMP
                    WHERE  study_instance_uid = ?
                      AND  frame_of_reference_uid = ?
                      AND  pet_series_uid = ?
                    ''',
                    (mask_path, study_instance_uid, frame_of_reference_uid, pet_series_uid),
                )
                self.conn.commit()
            except Exception as e:
                log_project_stacktrace(self.logger, e)
                self.logger.warning(f'DefaceMaskDatabase.update_pairing_mask_path failed: {e}')

    def get_pairing(
        self,
        study_instance_uid: str,
        frame_of_reference_uid: str,
        pet_series_uid: str,
    ) -> Optional[Dict[str, Any]]:
        """Return the pairing record for a PET series, or ``None`` if absent.

        Used by :class:`DefaceService` to locate the CT mask path for a pending
        PET defacing step.

        Read-only; thread-safe for concurrent callers.

        Args:
            study_instance_uid: DICOM StudyInstanceUID.
            frame_of_reference_uid: DICOM FrameOfReferenceUID.
            pet_series_uid: Original SeriesInstanceUID of the PET series.

        Returns:
            Dict with keys ``ct_series_uid``, ``mask_path`` (``None`` until
            computed), ``mask_written_at``; or ``None`` if no row exists.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                '''
                SELECT ct_series_uid, mask_path, mask_written_at
                FROM   deface_series_pairing
                WHERE  study_instance_uid = ?
                  AND  frame_of_reference_uid = ?
                  AND  pet_series_uid = ?
                ''',
                (study_instance_uid, frame_of_reference_uid, pet_series_uid),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return {
                'ct_series_uid':   row['ct_series_uid'],
                'mask_path':       row['mask_path'],
                'mask_written_at': row['mask_written_at'],
            }
        except Exception as e:
            log_project_stacktrace(self.logger, e)
            self.logger.warning(f'DefaceMaskDatabase.get_pairing failed: {e}')
            return None

    def get_pairings_for_ct(
        self,
        study_instance_uid: str,
        frame_of_reference_uid: str,
        ct_series_uid: str,
    ) -> List[Dict[str, Any]]:
        """Return all pairing rows where ``ct_series_uid`` is the elected primary.

        Used by :class:`DefaceService._persist_mask_to_db` to update
        ``mask_path`` for every PET that was paired with this CT.

        Read-only; thread-safe for concurrent callers.

        Args:
            study_instance_uid: DICOM StudyInstanceUID.
            frame_of_reference_uid: DICOM FrameOfReferenceUID.
            ct_series_uid: Original SeriesInstanceUID of the CT primary.

        Returns:
            List of dicts with keys ``pet_series_uid``, ``mask_path``,
            ``mask_written_at``; empty list on error or no matches.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                '''
                SELECT pet_series_uid, mask_path, mask_written_at
                FROM   deface_series_pairing
                WHERE  study_instance_uid = ?
                  AND  frame_of_reference_uid = ?
                  AND  ct_series_uid = ?
                ''',
                (study_instance_uid, frame_of_reference_uid, ct_series_uid),
            )
            return [
                {
                    'pet_series_uid':  row['pet_series_uid'],
                    'mask_path':       row['mask_path'],
                    'mask_written_at': row['mask_written_at'],
                }
                for row in cursor.fetchall()
            ]
        except Exception as e:
            log_project_stacktrace(self.logger, e)
            self.logger.warning(f'DefaceMaskDatabase.get_pairings_for_ct failed: {e}')
            return []

    def get_all_mask_paths(self) -> List[str]:
        """Return all distinct mask_path values stored in both tables.

        Used at cleanup time to delete NRRD files that were written to the
        private mapping folder when the database is not being persisted.
        Paths are relative to ``private_folder`` as stored in the DB.

        Returns:
            List of relative mask path strings (may be empty).
        """
        try:
            cursor = self.conn.cursor()
            # Collect from deface_mask_cache (one path per CT mask)
            cursor.execute('SELECT DISTINCT mask_path FROM deface_mask_cache WHERE mask_path IS NOT NULL')
            paths = {row[0] for row in cursor.fetchall()}
            # Collect from deface_series_pairing (same physical files, different lookup)
            cursor.execute('SELECT DISTINCT mask_path FROM deface_series_pairing WHERE mask_path IS NOT NULL')
            paths.update(row[0] for row in cursor.fetchall())
            return list(paths)
        except Exception as e:
            self.logger.warning(f'DefaceMaskDatabase.get_all_mask_paths failed: {e}')
            return []

    def get_stats(self) -> Dict[str, int]:
        """Return basic statistics about both tables.

        Returns:
            Dict with keys ``total_masks``, ``total_pairings``,
            ``completed_pairings`` (pairings whose mask has been written).
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM deface_mask_cache')
            total_masks = cursor.fetchone()[0]
            cursor.execute('SELECT COUNT(*) FROM deface_series_pairing')
            total_pairings = cursor.fetchone()[0]
            cursor.execute('SELECT COUNT(*) FROM deface_series_pairing WHERE mask_path IS NOT NULL')
            completed_pairings = cursor.fetchone()[0]
            return {
                'total_masks':        total_masks,
                'total_pairings':     total_pairings,
                'completed_pairings': completed_pairings,
            }
        except Exception:
            return {'total_masks': 0, 'total_pairings': 0, 'completed_pairings': 0}

    def close(self) -> None:
        """Close the database connection."""
        try:
            self.conn.close()
        except Exception:
            pass
