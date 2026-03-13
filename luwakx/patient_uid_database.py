#!/usr/bin/env python

import sqlite3
import hashlib
import os
import secrets
import threading
from typing import Optional, Tuple


class PatientUIDDatabase:
    """
    Thread-safe database for managing patient ID mappings during anonymization.
    
    Uses hash-based lookup to generate consistent sequential patient IDs
    across an anonymization project (e.g., "Zenta00", "Zenta01", etc.).
    
    Supports concurrent reads and serialized writes for parallel processing
    on single or multiple nodes.
    
    See conformance documentation ("Patient UID Database" section):
    https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#81-output-files-generated-by-luwak
    """
    
    def __init__(self, db_path: str, patient_id_prefix: str, project_hash_root: str = ''):
        """
        Initialize the patient UID database.
        
        Args:
            db_path: Path to SQLite database file
            patient_id_prefix: Prefix for generated patient IDs (e.g., "Patient")
            project_hash_root: Optional project identifier for isolation
        """
        self.db_path = db_path
        self.patient_id_prefix = patient_id_prefix
        self.project_hash_root = project_hash_root
        
        # Thread lock for write operations (serializes writes across threads)
        self._write_lock = threading.Lock()
        
        # Create database directory if needed
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Initialize database connection with settings for concurrent access
        self.conn = sqlite3.connect(
            db_path, 
            check_same_thread=False,  # Allow multi-threaded access
            timeout=30.0  # Wait up to 30 seconds for locks
        )
        self.conn.row_factory = sqlite3.Row
        
        # Enable WAL mode for better concurrent read/write performance
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA synchronous=NORMAL')
        self.conn.execute('PRAGMA busy_timeout=30000')  # 30 second timeout
        
        self._create_tables()
        
    def _create_tables(self):
        """Create database schema if it doesn't exist."""
        cursor = self.conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS patient_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_hash TEXT NOT NULL UNIQUE,
                anonymized_patient_id TEXT NOT NULL,
                random_token BLOB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create index for faster lookups
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_original_hash 
            ON patient_mappings(original_hash)
        ''')
        
        self.conn.commit()
    
    def _compute_patient_hash(self, patient_id: str, patient_name: str, birthdate: str) -> str:
        """
        Compute SHA256 hash from patient identifiers.
        
        Includes project_hash_root for project-specific isolation, ensuring
        the same patient gets different anonymized IDs in different projects.
        
        Normalizes identifiers (trim whitespace, uppercase) to ensure
        consistent hashing even with minor metadata variations.
        
        Args:
            patient_id: Original patient ID
            patient_name: Original patient name
            birthdate: Patient birth date
            
        Returns:
            Hex string of SHA256 hash
        """
        # Normalize identifiers to handle metadata variations
        # - Strip leading/trailing whitespace
        # - Convert to uppercase for case-insensitive matching
        # - Replace multiple spaces with single space
        normalized_id = ' '.join(str(patient_id).strip().upper().split())
        normalized_name = ' '.join(str(patient_name).strip().upper().split())
        normalized_birthdate = ' '.join(str(birthdate).strip().upper().split())
        
        # Include project_hash_root for true project isolation
        # Same patient will have different hash (and thus different ID) in different projects
        combined = f"{self.project_hash_root}||{normalized_id}||{normalized_name}||{normalized_birthdate}"
        return hashlib.sha256(combined.encode('utf-8')).hexdigest()
    
    def get_cached_patient_id(self, original_patient_id: str, 
                               original_patient_name: str, 
                               birthdate: str) -> Optional[Tuple[str, bytes]]:
        """
        Get existing anonymized patient ID and random token from database (read-only).
        
        This method performs a read-only lookup and does NOT create
        new entries. Thread-safe for concurrent reads.
        
        Args:
            original_patient_id: Original patient ID from DICOM
            original_patient_name: Original patient name from DICOM
            birthdate: Patient birth date from DICOM
            
        Returns:
            Tuple of (anonymized_patient_id, random_token) if found, None otherwise
        """
        # Compute hash of original identifiers (includes project_hash_root)
        original_hash = self._compute_patient_hash(
            original_patient_id, 
            original_patient_name, 
            birthdate
        )
        
        # Read from database (no lock needed for reads in WAL mode)
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT anonymized_patient_id, random_token
            FROM patient_mappings 
            WHERE original_hash = ?
        ''', (original_hash,))
        
        row = cursor.fetchone()
        
        if row:
            return (row['anonymized_patient_id'], row['random_token'])
        
        return None
    
    def store_patient_id(self, original_patient_id: str, 
                         original_patient_name: str, 
                         birthdate: str) -> Tuple[str, bytes]:
        """
        Create and store new anonymized patient ID and random token in database.
        
        This method generates a new sequential ID, a cryptographically secure
        random token, and stores both. Thread-safe with write locking to 
        prevent race conditions.
        
        Args:
            original_patient_id: Original patient ID from DICOM
            original_patient_name: Original patient name from DICOM
            birthdate: Patient birth date from DICOM
            
        Returns:
            Tuple of (newly created anonymized patient ID, random token bytes)
        """
        # Compute hash of original identifiers
        original_hash = self._compute_patient_hash(
            original_patient_id, 
            original_patient_name, 
            birthdate
        )
        
        # Acquire write lock to serialize write operations
        with self._write_lock:
            # Double-check if entry was created by another thread
            cursor = self.conn.cursor()
            cursor.execute('''
                SELECT anonymized_patient_id, random_token
                FROM patient_mappings 
                WHERE original_hash = ?
            ''', (original_hash,))
            
            row = cursor.fetchone()
            if row:
                # Another thread created it while we were waiting
                return (row['anonymized_patient_id'], row['random_token'])
            
            # Create new sequential ID
            # Find the highest sequence number for this prefix
            cursor.execute('''
                SELECT anonymized_patient_id 
                FROM patient_mappings 
                WHERE anonymized_patient_id LIKE ?
                ORDER BY id DESC
                LIMIT 1
            ''', (f"{self.patient_id_prefix}%",))
            
            last_row = cursor.fetchone()
            
            if last_row:
                # Extract sequence number from last ID
                last_id = last_row['anonymized_patient_id']
                try:
                    # Remove prefix and parse number
                    seq_str = last_id[len(self.patient_id_prefix):]
                    last_seq = int(seq_str)
                    next_seq = last_seq + 1
                except (ValueError, IndexError):
                    # Fallback if parsing fails
                    next_seq = 0
            else:
                # First patient in this project
                next_seq = 0
            
            # Format with zero-padding (e.g., "000000", "000001", ...)
            new_patient_id = f"{self.patient_id_prefix}{next_seq:06d}"
            
            # Generate cryptographically secure random token (256 bits / 32 bytes)
            random_token = secrets.token_bytes(256 // 8)
            
            # Insert new mapping
            cursor.execute('''
                INSERT INTO patient_mappings (original_hash, anonymized_patient_id, random_token)
                VALUES (?, ?, ?)
            ''', (original_hash, new_patient_id, random_token))
            
            self.conn.commit()
            
            return (new_patient_id, random_token)
    
    def get_stats(self) -> dict:
        """
        Get statistics about the patient database.
        
        Returns:
            Dictionary with patient count and other stats
        """
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT COUNT(*) as total_patients
            FROM patient_mappings
        ''')
        
        row = cursor.fetchone()
        
        return {
            'total_patients': row['total_patients'] if row else 0,
            'db_path': self.db_path,
            'prefix': self.patient_id_prefix
        }
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
