#!/usr/bin/env python
"""
Job Checkpoint Database -- stop/resume support for Luwak anonymization jobs.

Two tables:

``jobs``
    One row per job run attempt.  Keyed on ``(input_folder, output_folder)``.
    ``scan_status`` tracks whether the full header-scan + series-grouping
    phase committed its result.  ``config_hash`` detects config drift between
    a stopped run and a resume attempt.

``series_status``
    One row per series per job.  A single ``processing_status`` TEXT column
    stores the :class:`~luwakx.pipeline.processing_status.ProcessingStatus`
    name of the last *completed* stage.  Only series whose status is
    ``EXPORTED`` are considered fully done.

    The ordering defined on ``ProcessingStatus`` drives cleanup decisions:

    ============ =====================================================
    Status       Resume from / Cleanup action
    ============ =====================================================
    ORIGINAL     organize step   - nothing to clean
    ORGANIZED    deface step     - organised copies intact, keep them
    DEFACED      anonymize step  - delete ``output_base_path``
    ANONYMIZED   export step     - delete ``output_base_path``,
                                   purge CSV/Parquet rows
    EXPORTED     skip entirely   - nothing
    ============ =====================================================

Usage sketch::

    db = JobCheckpointDatabase(db_path)
    job_id = db.get_or_create_job(input_folder, output_folder, private_folder, config_hash)
    db.upsert_series(job_id, anonymized_series_uid, ...)     # after scan
    db.mark_scan_complete(job_id)                            # after full scan committed
    db.mark_series_status(job_id, uid, ProcessingStatus.ORGANIZED)
    db.mark_series_status(job_id, uid, ProcessingStatus.DEFACED)
    db.mark_series_status(job_id, uid, ProcessingStatus.ANONYMIZED)
    db.mark_series_status(job_id, uid, ProcessingStatus.EXPORTED)
"""

import hashlib
import json
import os
import shutil
import sqlite3
import threading
import uuid
from typing import Dict, List, Optional, Set

from ..pipeline.processing_status import ProcessingStatus


class JobCheckpointDatabase:
    """Thread-safe SQLite database for per-series pipeline checkpoint tracking."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
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
        cursor = self.conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS jobs (
                job_id          TEXT PRIMARY KEY,
                input_folder    TEXT NOT NULL,
                output_folder   TEXT NOT NULL,
                private_folder  TEXT NOT NULL,
                config_hash     TEXT NOT NULL,
                scan_status     TEXT NOT NULL DEFAULT 'PENDING',
                started_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (input_folder, output_folder)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS series_status (
                job_id                  TEXT    NOT NULL,
                anonymized_series_uid   TEXT    NOT NULL,
                original_series_uid     TEXT,
                original_patient_id     TEXT,
                original_study_uid      TEXT,
                anonymized_patient_id   TEXT,
                anonymized_study_uid    TEXT,
                modality                TEXT,
                series_order            INTEGER DEFAULT 0,
                primary_ct_series_uid   TEXT,
                worker_partition        INTEGER DEFAULT 0,
                organized_base_path     TEXT,
                defaced_base_path       TEXT,
                output_base_path        TEXT,
                processing_status       TEXT    NOT NULL DEFAULT 'ORIGINAL',
                last_updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (job_id, anonymized_series_uid)
            )
        ''')

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_ss_job
            ON series_status (job_id, processing_status)
        ''')

        self.conn.commit()

    # ------------------------------------------------------------------
    # Config hashing
    # ------------------------------------------------------------------

    @staticmethod
    def compute_config_hash(config: dict) -> str:
        """SHA-256 of the recipe-affecting subset of *config*.

        Only keys that influence anonymization output are included so that
        innocuous changes (numWorkers, keepTempFiles, logLevel) do not
        prevent resuming a job.
        """
        relevant_keys = {
            'inputFolder', 'outputDeidentifiedFolder', 'outputPrivateMappingFolder',
            'recipes', 'patientIdPrefix', 'projectHashRoot',
            'customTags', 'selectedModalities',
            'physicalFacePixelationSizeMm', 'faceDilationMarginMm',
        }
        subset = {k: v for k, v in config.items() if k in relevant_keys}
        serialised = json.dumps(subset, sort_keys=True, ensure_ascii=True)
        return hashlib.sha256(serialised.encode()).hexdigest()

    # ------------------------------------------------------------------
    # Job management
    # ------------------------------------------------------------------

    def get_or_create_job(
        self,
        input_folder: str,
        output_folder: str,
        private_folder: str,
        config_hash: str,
    ) -> Optional[str]:
        """Return the job_id for an existing resumable job, or create a new one.

        If an existing job for ``(input_folder, output_folder)`` is found:
        - ``config_hash`` matches  -> return its job_id (resume path).
        - ``config_hash`` differs  -> return ``None`` (config drift; caller warns).

        Returns:
            job_id string, or None when config drift is detected.
        """
        with self._write_lock:
            cursor = self.conn.cursor()
            cursor.execute(
                'SELECT job_id, config_hash FROM jobs '
                'WHERE input_folder = ? AND output_folder = ?',
                (input_folder, output_folder),
            )
            row = cursor.fetchone()

            if row is not None:
                if row['config_hash'] != config_hash:
                    return None  # config drift
                return row['job_id']

            job_id = str(uuid.uuid4())
            cursor.execute(
                'INSERT INTO jobs '
                '(job_id, input_folder, output_folder, private_folder, config_hash) '
                'VALUES (?, ?, ?, ?, ?)',
                (job_id, input_folder, output_folder, private_folder, config_hash),
            )
            self.conn.commit()
            return job_id

    def get_job_scan_status(self, job_id: str) -> str:
        """Return 'PENDING' or 'COMPLETE' for the given job."""
        cursor = self.conn.cursor()
        cursor.execute('SELECT scan_status FROM jobs WHERE job_id = ?', (job_id,))
        row = cursor.fetchone()
        return row['scan_status'] if row else 'PENDING'

    def mark_scan_complete(self, job_id: str) -> None:
        """Mark the scan phase as complete (all series have been upserted)."""
        with self._write_lock:
            self.conn.execute(
                "UPDATE jobs SET scan_status = 'COMPLETE', last_updated_at = CURRENT_TIMESTAMP "
                'WHERE job_id = ?',
                (job_id,),
            )
            self.conn.commit()

    def touch_job(self, job_id: str) -> None:
        """Update ``last_updated_at`` - called on graceful stop."""
        with self._write_lock:
            self.conn.execute(
                'UPDATE jobs SET last_updated_at = CURRENT_TIMESTAMP WHERE job_id = ?',
                (job_id,),
            )
            self.conn.commit()

    # ------------------------------------------------------------------
    # Series status
    # ------------------------------------------------------------------

    def upsert_series(
        self,
        job_id: str,
        anonymized_series_uid: str,
        original_series_uid: str = '',
        original_patient_id: str = '',
        original_study_uid: str = '',
        anonymized_patient_id: str = '',
        anonymized_study_uid: str = '',
        modality: str = '',
        series_order: int = 0,
        primary_ct_series_uid: str = '',
        worker_partition: int = 0,
        organized_base_path: str = '',
        defaced_base_path: str = '',
        output_base_path: str = '',
    ) -> None:
        """Insert or update a series row.

        Uses ``INSERT OR IGNORE`` so that existing ``processing_status`` is
        never overwritten during a re-scan.  Path / partition columns are
        refreshed via a follow-up ``UPDATE`` so that any change in
        ``numWorkers`` between runs is persisted.
        """
        with self._write_lock:
            self.conn.execute(
                '''
                INSERT OR IGNORE INTO series_status
                    (job_id, anonymized_series_uid,
                     original_series_uid, original_patient_id, original_study_uid,
                     anonymized_patient_id, anonymized_study_uid,
                     modality, series_order, primary_ct_series_uid, worker_partition,
                     organized_base_path, defaced_base_path, output_base_path,
                     processing_status)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ''',
                (job_id, anonymized_series_uid,
                 original_series_uid, original_patient_id, original_study_uid,
                 anonymized_patient_id, anonymized_study_uid,
                 modality, series_order, primary_ct_series_uid, worker_partition,
                 organized_base_path, defaced_base_path, output_base_path,
                 ProcessingStatus.ORIGINAL.name),
            )
            self.conn.execute(
                '''
                UPDATE series_status
                SET    series_order = ?,
                       primary_ct_series_uid = ?,
                       worker_partition = ?,
                       organized_base_path = ?,
                       defaced_base_path = ?,
                       output_base_path = ?,
                       last_updated_at = CURRENT_TIMESTAMP
                WHERE  job_id = ? AND anonymized_series_uid = ?
                  AND  processing_status = ?
                ''',
                (series_order, primary_ct_series_uid, worker_partition,
                 organized_base_path, defaced_base_path, output_base_path,
                 job_id, anonymized_series_uid, ProcessingStatus.ORIGINAL.name),
            )
            self.conn.commit()

    def mark_series_status(
        self, job_id: str, series_uid: str, status: ProcessingStatus
    ) -> None:
        """Record that *series_uid* has reached *status*.

        Only advances the status - never moves it backwards, so a duplicate
        call is harmless.
        """
        with self._write_lock:
            self.conn.execute(
                '''
                UPDATE series_status
                SET    processing_status = ?,
                       last_updated_at   = CURRENT_TIMESTAMP
                WHERE  job_id = ? AND anonymized_series_uid = ?
                ''',
                (status.name, job_id, series_uid),
            )
            self.conn.commit()

    def reset_series_status(self, job_id: str, series_uid: str) -> None:
        """Reset a series back to ORIGINAL (pre-resume cleanup)."""
        self.mark_series_status(job_id, series_uid, ProcessingStatus.ORIGINAL)

    def get_series_row(self, job_id: str, series_uid: str) -> Optional[dict]:
        """Return the full status row for one series, or None."""
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM series_status WHERE job_id = ? AND anonymized_series_uid = ?',
            (job_id, series_uid),
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_completed_series_uids(self, job_id: str) -> Set[str]:
        """Return UIDs of series whose status is EXPORTED (fully done)."""
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT anonymized_series_uid FROM series_status '
            'WHERE job_id = ? AND processing_status = ?',
            (job_id, ProcessingStatus.EXPORTED.name),
        )
        return {row['anonymized_series_uid'] for row in cursor.fetchall()}

    def get_incomplete_series_rows(self, job_id: str) -> List[dict]:
        """Return all series rows that are NOT yet EXPORTED, ordered by series_order."""
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM series_status '
            'WHERE job_id = ? AND processing_status != ? '
            'ORDER BY series_order',
            (job_id, ProcessingStatus.EXPORTED.name),
        )
        return [dict(row) for row in cursor.fetchall()]

    # ------------------------------------------------------------------
    # Pre-resume cleanup
    # ------------------------------------------------------------------

    def cleanup_incomplete_series(self, job_id: str, logger=None) -> None:
        """Delete partial artifacts for every non-EXPORTED series.

        Cleanup rules (derived from ``ProcessingStatus`` ordering)
        ----------------------------------------------------------
        ``ORIGINAL``
            Nothing has been written yet - nothing to clean.

        ``ORGANIZED``
            Organised copies are intact and valid; keep them so resume can
            skip re-organize.  No output DICOMs exist yet.

        ``DEFACED``
            NRRDs are already at their final destinations (moved by
            ``_export_nrrd_files`` before the status was recorded).  Only
            ``output_base_path`` may have partial anonymized files - delete it.

        ``ANONYMIZED``
            Anonymize completed but export crashed mid-write.  Delete
            ``output_base_path`` and purge export file rows (handled by
            :meth:`purge_series_from_export_files`).

        In all cases the ``processing_status`` is reset to ``ORIGINAL`` so the
        pipeline re-processes from the correct stage on resume.
        """
        rows = self.get_incomplete_series_rows(job_id)
        if not rows:
            return

        if logger:
            logger.info(f'Resume cleanup: {len(rows)} incomplete series to clean up')

        for row in rows:
            uid = row['anonymized_series_uid']
            status_name = row.get('processing_status', ProcessingStatus.ORIGINAL.name)
            org_path = row.get('organized_base_path') or ''
            def_path = row.get('defaced_base_path') or ''
            out_path = row.get('output_base_path') or ''

            try:
                status = ProcessingStatus[status_name]
            except KeyError:
                status = ProcessingStatus.ORIGINAL

            if logger:
                logger.debug(f'Cleanup series {uid}: last status={status.name}')

            if status < ProcessingStatus.ORGANIZED:
                # Nothing written yet
                pass
            elif status < ProcessingStatus.DEFACED:
                # Organize done; defacing not started - keep organised copies
                # Nothing else to delete
                pass
            else:
                # DEFACED or ANONYMIZED: output_base_path may have partial files
                _rmtree_safe(out_path, logger, label='output_base_path')

            if status < ProcessingStatus.ORGANIZED:
                # Organized copies may be partial
                _rmtree_safe(org_path, logger, label='organized_base_path (partial)')
                _rmtree_safe(def_path, logger, label='defaced_base_path (partial)')

            # Reset to ORIGINAL so pipeline reprocesses from the start of the
            # first incomplete stage.
            self.reset_series_status(job_id, uid)

    def purge_series_from_export_files(
        self,
        incomplete_series_uids: Set[str],
        uid_mappings_file: str,
        metadata_file: str,
        review_flags_file: str,
        logger=None,
    ) -> None:
        """Remove rows for *incomplete_series_uids* from the three export files.

        Only series whose status was ``ANONYMIZED`` at stop time could have rows
        in the export files (partial export crash).  It is safe to call this for
        all incomplete series - rows simply won't be found for earlier stages.

        ``uid_mappings.csv`` and ``review_flags.csv`` are rewritten in-place.
        ``metadata.parquet`` is rewritten via pyarrow.
        A truncated last line in any CSV is repaired before row-removal.
        """
        if not incomplete_series_uids:
            return

        for csv_path in (uid_mappings_file, review_flags_file):
            _purge_csv_rows(csv_path, 'anonymized_series_uid', incomplete_series_uids, logger)

        _purge_parquet_rows(metadata_file, 'anonymized_series_uid', incomplete_series_uids, logger)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# File-level purge helpers
# ---------------------------------------------------------------------------

def _rmtree_safe(path: str, logger, label: str = '') -> None:
    if not path or not os.path.exists(path):
        return
    try:
        shutil.rmtree(path)
        if logger:
            logger.debug(f'Removed {label}: {path}')
    except Exception as exc:
        if logger:
            logger.warning(f'Could not remove {label} ({path}): {exc}')


def _purge_csv_rows(
    csv_path: str,
    key_column: str,
    uids_to_remove: Set[str],
    logger,
) -> None:
    """Rewrite *csv_path* keeping only rows whose *key_column* is NOT in *uids_to_remove*."""
    if not os.path.exists(csv_path):
        return

    import csv as _csv

    try:
        _repair_truncated_csv(csv_path, logger)

        with open(csv_path, 'r', newline='') as fh:
            reader = _csv.DictReader(fh)
            fieldnames = reader.fieldnames or []
            if key_column not in fieldnames:
                return
            kept_rows = [
                row for row in reader
                if row.get(key_column) not in uids_to_remove
            ]

        tmp_path = csv_path + '.tmp'
        with open(tmp_path, 'w', newline='') as fh:
            writer = _csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(kept_rows)

        os.replace(tmp_path, csv_path)

        if logger:
            logger.debug(
                f'Purged {len(uids_to_remove)} series from {csv_path}; '
                f'{len(kept_rows)} rows remaining'
            )

    except Exception as exc:
        if logger:
            logger.warning(f'Could not purge rows from {csv_path}: {exc}')


def _repair_truncated_csv(path: str, logger) -> None:
    """Truncate a CSV file back to the last complete newline-terminated line."""
    try:
        with open(path, 'rb') as fh:
            data = fh.read()
        if not data or data[-1:] == b'\n':
            return
        last_nl = data.rfind(b'\n')
        if last_nl == -1:
            return
        if logger:
            logger.warning(f'Truncated CSV detected: trimming incomplete last line in {path}')
        with open(path, 'wb') as fh:
            fh.write(data[: last_nl + 1])
    except Exception as exc:
        if logger:
            logger.warning(f'Could not repair truncated CSV {path}: {exc}')


def _purge_parquet_rows(
    parquet_path: str,
    key_column: str,
    uids_to_remove: Set[str],
    logger,
) -> None:
    """Rewrite *parquet_path* without rows whose *key_column* is in *uids_to_remove*."""
    if not os.path.exists(parquet_path):
        return

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pq.read_table(parquet_path)
        if key_column not in table.schema.names:
            return

        mask = pa.array(
            [uid not in uids_to_remove for uid in table.column(key_column).to_pylist()]
        )
        filtered = table.filter(mask)

        tmp_path = parquet_path + '.tmp'
        pq.write_table(filtered, tmp_path, compression='snappy')
        os.replace(tmp_path, parquet_path)

        if logger:
            logger.debug(
                f'Purged {len(uids_to_remove)} series from {parquet_path}; '
                f'{len(filtered)} rows remaining'
            )

    except Exception as exc:
        if logger:
            logger.warning(f'Could not purge rows from {parquet_path}: {exc}')
