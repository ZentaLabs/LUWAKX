#!/usr/bin/env python

"""
Deface Mask Database for caching primary defacing masks per FrameOfReferenceUID.

This module provides thread-safe SQLite-based caching of defacing masks so that the
primary (largest spatial coverage + finest resolution) mask computed for a given patient
and spatial reference frame can be reused across series and across runs.

The database key is a hash of:
    project_hash_root || PatientID || PatientName || PatientBirthDate || FrameOfReferenceUID

This ensures that masks are scoped to a single spatial reference frame per patient
and can later be resampled and applied to other series sharing the same frame.

See conformance documentation ("Deface Mask Cache Database" section):
https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md
"""

import json
import os
import sqlite3
import threading
import hashlib
import traceback
from typing import Any, Dict, Optional

from luwak_logger import get_logger, log_project_stacktrace


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
        """Create the database schema if it does not already exist."""
        cursor = self.conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS deface_mask_cache (
                cache_key              TEXT    NOT NULL,
                modality               TEXT    NOT NULL,
                mask_path              TEXT    NOT NULL,
                spatial_volume_cm3     REAL    NOT NULL,
                min_voxel_size_mm      REAL    NOT NULL,
                spacing                TEXT,
                origin                 TEXT,
                direction              TEXT,
                frame_of_reference_uid TEXT,
                study_instance_uid     TEXT,
                anonymized_patient_id  TEXT,
                anonymized_study_uid   TEXT,
                created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (cache_key, modality)
            )
        ''')

        # Migration: add study_instance_uid column to existing databases
        existing_cols = {row[1] for row in cursor.execute('PRAGMA table_info(deface_mask_cache)')}
        if 'study_instance_uid' not in existing_cols:
            cursor.execute('ALTER TABLE deface_mask_cache ADD COLUMN study_instance_uid TEXT')

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_dcm_cache_key
            ON deface_mask_cache (cache_key, modality)
        ''')

        self.conn.commit()

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
    ) -> Optional[Dict[str, Any]]:
        """Retrieve the cached primary mask for the given patient / study / frame / modality.

        Read-only; thread-safe for concurrent callers.

        Args:
            patient_id: Original DICOM PatientID.
            patient_name: Original DICOM PatientName.
            birthdate: Original DICOM PatientBirthDate.
            study_instance_uid: DICOM StudyInstanceUID.
            frame_of_reference_uid: DICOM FrameOfReferenceUID.
            modality: DICOM Modality string (e.g. "CT", "MR").

        Returns:
            Dictionary with keys
                ``mask_path``, ``spatial_volume_cm3``, ``min_voxel_size_mm``,
                ``spacing`` (list[float]), ``origin`` (list[float]),
                ``direction`` (list[float]), ``frame_of_reference_uid``,
                ``anonymized_patient_id``, ``anonymized_study_uid``
            or ``None`` if no entry exists or the mask file is no longer on disk.
        """
        cache_key = self._compute_key(patient_id, patient_name, birthdate, study_instance_uid, frame_of_reference_uid)
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                '''
                SELECT mask_path, spatial_volume_cm3, min_voxel_size_mm,
                       spacing, origin, direction,
                       frame_of_reference_uid, study_instance_uid,
                       anonymized_patient_id, anonymized_study_uid
                FROM   deface_mask_cache
                WHERE  cache_key = ? AND modality = ?
                ''',
                (cache_key, modality),
            )
            row = cursor.fetchone()
            if row is None:
                return None

            mask_path = row['mask_path']
            # mask_path is stored as a relative path (relative to private_folder).
            # The caller is responsible for resolving it to an absolute path and
            # checking whether the file exists on disk.

            return {
                'mask_path':             mask_path,
                'spatial_volume_cm3':    row['spatial_volume_cm3'],
                'min_voxel_size_mm':     row['min_voxel_size_mm'],
                'spacing':               json.loads(row['spacing'])  if row['spacing']   else None,
                'origin':                json.loads(row['origin'])   if row['origin']    else None,
                'direction':             json.loads(row['direction']) if row['direction'] else None,
                'frame_of_reference_uid': row['frame_of_reference_uid'],
                'study_instance_uid':     row['study_instance_uid'],
                'anonymized_patient_id':  row['anonymized_patient_id'],
                'anonymized_study_uid':   row['anonymized_study_uid'],
            }

        except Exception as e:
            log_project_stacktrace(self.logger, e)
            self.logger.warning(f"DefaceMaskDatabase.get_primary_mask failed: {e}")
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
        spatial_volume_cm3: float,
        min_voxel_size_mm: float,
        spacing: Optional[list] = None,
        origin: Optional[list] = None,
        direction: Optional[list] = None,
        anonymized_patient_id: Optional[str] = None,
        anonymized_study_uid: Optional[str] = None,
    ) -> None:
        """Insert or replace the mask entry for the given patient / study / frame / modality.

        This method is called after a new primary mask has been computed.  It
        **replaces** any existing entry unconditionally; callers are responsible
        for deciding whether the new mask is actually better before calling this.

        Thread-safe (serialised write).

        Args:
            patient_id: Original DICOM PatientID.
            patient_name: Original DICOM PatientName.
            birthdate: Original DICOM PatientBirthDate.
            study_instance_uid: DICOM StudyInstanceUID.
            frame_of_reference_uid: DICOM FrameOfReferenceUID.
            modality: DICOM Modality string.
            mask_path: Absolute path to the NRRD mask file.
            spatial_volume_cm3: Spatial volume covered by the mask in cm³.
            min_voxel_size_mm: Smallest voxel dimension in mm (used as a
                               resolution proxy; lower is better).
            spacing: SimpleITK spacing tuple/list [sx, sy, sz] in mm.
            origin: SimpleITK origin tuple/list [ox, oy, oz] in mm (LPS).
            direction: SimpleITK direction as a flat list of 9 floats (row-major).
            anonymized_patient_id: Anonymised patient ID for path reconstruction.
            anonymized_study_uid: Anonymised study UID for path reconstruction.
        """
        cache_key = self._compute_key(patient_id, patient_name, birthdate, study_instance_uid, frame_of_reference_uid)

        spacing_json   = json.dumps(list(spacing))   if spacing   is not None else None
        origin_json    = json.dumps(list(origin))    if origin    is not None else None
        direction_json = json.dumps(list(direction)) if direction is not None else None

        with self._write_lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute(
                    '''
                    INSERT INTO deface_mask_cache
                        (cache_key, modality, mask_path, spatial_volume_cm3, min_voxel_size_mm,
                         spacing, origin, direction, frame_of_reference_uid, study_instance_uid,
                         anonymized_patient_id, anonymized_study_uid, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(cache_key, modality) DO UPDATE SET
                        mask_path             = excluded.mask_path,
                        spatial_volume_cm3    = excluded.spatial_volume_cm3,
                        min_voxel_size_mm     = excluded.min_voxel_size_mm,
                        spacing               = excluded.spacing,
                        origin                = excluded.origin,
                        direction             = excluded.direction,
                        frame_of_reference_uid = excluded.frame_of_reference_uid,
                        study_instance_uid    = excluded.study_instance_uid,
                        anonymized_patient_id = excluded.anonymized_patient_id,
                        anonymized_study_uid  = excluded.anonymized_study_uid,
                        updated_at            = CURRENT_TIMESTAMP
                    ''',
                    (
                        cache_key, modality, mask_path,
                        spatial_volume_cm3, min_voxel_size_mm,
                        spacing_json, origin_json, direction_json,
                        frame_of_reference_uid, study_instance_uid,
                        anonymized_patient_id, anonymized_study_uid,
                    ),
                )
                self.conn.commit()
                self.logger.debug(
                    f"Upserted deface mask for modality={modality}, FOR={frame_of_reference_uid}: {mask_path}"
                )
            except Exception as e:
                log_project_stacktrace(self.logger, e)
                self.logger.warning(f"DefaceMaskDatabase.upsert_mask failed: {e}")

    def get_stats(self) -> Dict[str, int]:
        """Return basic statistics about the cache.

        Returns:
            Dict with key ``total_masks``.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM deface_mask_cache')
            total = cursor.fetchone()[0]
            return {'total_masks': total}
        except Exception:
            return {'total_masks': 0}

    def close(self) -> None:
        """Close the database connection."""
        try:
            self.conn.close()
        except Exception:
            pass
