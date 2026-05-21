"""Tests for DefaceMaskDatabase schema migrations."""

import sqlite3
import tempfile
import os
import unittest

from luwakx.defacing.deface_mask_database import DefaceMaskDatabase


def _open_raw(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


class TestMigrateCtSeriesInstanceUidRename(unittest.TestCase):
    """ct_series_instance_uid -> series_instance_uid rename migration."""

    def _make_old_schema_db(self, path: str) -> None:
        """Create a DB with the original ct_series_instance_uid column name."""
        conn = _open_raw(path)
        conn.execute('''
            CREATE TABLE deface_mask_cache (
                cache_key              TEXT NOT NULL,
                modality               TEXT NOT NULL,
                ct_series_instance_uid TEXT NOT NULL DEFAULT '',
                mask_path              TEXT NOT NULL,
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
        conn.execute('''
            INSERT INTO deface_mask_cache
                (cache_key, modality, ct_series_instance_uid, mask_path)
            VALUES ('key1', 'CT', 'series.1.2.3', '/masks/a.nrrd')
        ''')
        conn.commit()
        conn.close()

    def test_rename_applied_on_open(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, 'deface.db')
            self._make_old_schema_db(db_path)

            db = DefaceMaskDatabase(db_path)
            db.close()

            conn = _open_raw(db_path)
            cols = {row[1] for row in conn.execute('PRAGMA table_info(deface_mask_cache)')}
            conn.close()

            self.assertIn('series_instance_uid', cols)
            self.assertNotIn('ct_series_instance_uid', cols)

    def test_existing_rows_preserved_after_rename(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, 'deface.db')
            self._make_old_schema_db(db_path)

            db = DefaceMaskDatabase(db_path)
            db.close()

            conn = _open_raw(db_path)
            rows = conn.execute('SELECT * FROM deface_mask_cache').fetchall()
            conn.close()

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['series_instance_uid'], 'series.1.2.3')
            self.assertEqual(rows[0]['mask_path'], '/masks/a.nrrd')


class TestMigrateRemovePetUidFromMaskCache(unittest.TestCase):
    """linked_pet_series_uid removal migration."""

    def _make_pet_uid_schema_db(self, path: str, use_ct_col_name: bool = False) -> None:
        """Create a DB with the intermediate linked_pet_series_uid schema.

        Args:
            use_ct_col_name: If True, use the *old* ct_series_instance_uid column
                name to simulate a database that never had the rename applied.
        """
        series_col = 'ct_series_instance_uid' if use_ct_col_name else 'series_instance_uid'
        conn = _open_raw(path)
        conn.execute(f'''
            CREATE TABLE deface_mask_cache (
                cache_key              TEXT NOT NULL,
                modality               TEXT NOT NULL,
                {series_col}           TEXT NOT NULL DEFAULT '',
                linked_pet_series_uid  TEXT NOT NULL DEFAULT '',
                mask_path              TEXT NOT NULL,
                spacing                TEXT,
                origin                 TEXT,
                direction              TEXT,
                frame_of_reference_uid TEXT,
                study_instance_uid     TEXT,
                anonymized_patient_id  TEXT,
                anonymized_study_uid   TEXT,
                created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (cache_key, modality, {series_col}, linked_pet_series_uid)
            )
        ''')
        # Row that should be kept: linked_pet_series_uid = '' (homogeneous group)
        conn.execute(f'''
            INSERT INTO deface_mask_cache
                (cache_key, modality, {series_col}, linked_pet_series_uid, mask_path)
            VALUES ('key1', 'CT', 'series.1.2.3', '', '/masks/keep.nrrd')
        ''')
        # Row that should be dropped: linked to a specific PET
        conn.execute(f'''
            INSERT INTO deface_mask_cache
                (cache_key, modality, {series_col}, linked_pet_series_uid, mask_path)
            VALUES ('key1', 'CT', 'series.1.2.3', 'pet.9.9.9', '/masks/drop.nrrd')
        ''')
        conn.commit()
        conn.close()

    def test_linked_pet_col_removed(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, 'deface.db')
            self._make_pet_uid_schema_db(db_path)

            db = DefaceMaskDatabase(db_path)
            db.close()

            conn = _open_raw(db_path)
            cols = {row[1] for row in conn.execute('PRAGMA table_info(deface_mask_cache)')}
            conn.close()

            self.assertNotIn('linked_pet_series_uid', cols)

    def test_homogeneous_row_kept_pet_linked_row_dropped(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, 'deface.db')
            self._make_pet_uid_schema_db(db_path)

            db = DefaceMaskDatabase(db_path)
            db.close()

            conn = _open_raw(db_path)
            rows = conn.execute('SELECT mask_path FROM deface_mask_cache').fetchall()
            conn.close()

            paths = {r[0] for r in rows}
            self.assertIn('/masks/keep.nrrd', paths)
            self.assertNotIn('/masks/drop.nrrd', paths)

    def test_migration_with_old_ct_col_name(self):
        """DB has both linked_pet_series_uid AND ct_series_instance_uid (oldest schema).

        This is the exact scenario described in the bug: the rename migration must
        run before the linked_pet removal so that the INSERT...SELECT can find
        series_instance_uid.
        """
        # ignore_cleanup_errors avoids Windows PermissionError if the constructor
        # fails mid-way and leaves the SQLite connection open.
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            db_path = os.path.join(tmp, 'deface.db')
            self._make_pet_uid_schema_db(db_path, use_ct_col_name=True)

            # Must not raise "no such column: series_instance_uid"
            db = DefaceMaskDatabase(db_path)
            db.close()

            conn = _open_raw(db_path)
            cols = {row[1] for row in conn.execute('PRAGMA table_info(deface_mask_cache)')}
            rows = conn.execute('SELECT mask_path FROM deface_mask_cache').fetchall()
            conn.close()

            self.assertIn('series_instance_uid', cols)
            self.assertNotIn('ct_series_instance_uid', cols)
            self.assertNotIn('linked_pet_series_uid', cols)
            paths = {r[0] for r in rows}
            self.assertIn('/masks/keep.nrrd', paths)
            self.assertNotIn('/masks/drop.nrrd', paths)


if __name__ == '__main__':
    unittest.main()
