#!/usr/bin/env python

"""
LLM Result Cache for DICOM Anonymization.

This module provides thread-safe SQLite-based caching for LLM results in DICOM anonymization,
avoiding redundant API calls across parallel processing and multiple runs.
"""

import os
import sqlite3
import threading
import hashlib
import traceback

# Import the centralized logger
from luwak_logger import get_logger, log_project_stacktrace


class LLMResultCache:
    """
    Thread-safe SQLite-based cache for LLM results in DICOM anonymization.
    
    Provides persistent caching of LLM PHI/PII detection results to avoid
    redundant API calls across parallel processing and multiple runs.
    """
    
    def __init__(self, cache_file_path, project_hash_root="", cache_ttl_days=30):
        """
        Initialize the LLM result cache.
        
        Args:
            cache_file_path (str): Path to SQLite cache file
            project_hash_root (str): Project hash root for cache key generation
            cache_ttl_days (int): Cache TTL in days (default: 30)
        """
        self.cache_file_path = cache_file_path
        self.project_hash_root = project_hash_root
        self.cache_ttl_days = cache_ttl_days
        self.logger = get_logger('llm_cache')
        
        # Thread-local storage for database connections
        self._local = threading.local()
        
        # Initialize database
        self._init_database()
    
    def _get_connection(self):
        """Get thread-local database connection."""
        if not hasattr(self._local, 'connection'):
            # Create connection with thread-safe settings
            self._local.connection = sqlite3.connect(
                self.cache_file_path,
                timeout=30.0,  # 30 second timeout
                check_same_thread=False
            )
            # Enable WAL mode for better concurrent access
            self._local.connection.execute("PRAGMA journal_mode=WAL")
            self._local.connection.execute("PRAGMA synchronous=NORMAL")
            self._local.connection.execute("PRAGMA busy_timeout=30000")  # 30 seconds
        return self._local.connection
    
    def _init_database(self):
        """Initialize the SQLite database with required schema."""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.cache_file_path), exist_ok=True)
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Create cache table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS llm_phi_cache (
                    cache_key TEXT PRIMARY KEY,
                    input_text TEXT NOT NULL,
                    llm_model TEXT NOT NULL,
                    phi_result INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    project_hash TEXT NOT NULL
                )
            """)
            
            # Create index for faster lookups
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_cache_key_project 
                ON llm_phi_cache(cache_key, project_hash)
            """)
            
            # Create index for cleanup by timestamp
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_created_at 
                ON llm_phi_cache(created_at)
            """)
            
            conn.commit()
            self.logger.debug(f"Initialized LLM cache database: {self.cache_file_path}")
            
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
            log_project_stacktrace(self.logger, e)
            raise
    
    def _generate_cache_key(self, input_text, model):
        """
        Generate a deterministic cache key for the input.
        
        Args:
            input_text (str): Input text to cache
            model (str): LLM model name
            
        Returns:
            str: Hexadecimal cache key
        """
        # Combine input text, and model for uniqueness
        key_source = f"{model}:{input_text}"
        return hashlib.sha256(key_source.encode('utf-8')).hexdigest()
    
    def get_cached_result(self, input_text, model):
        """
        Retrieve cached LLM result if available and not expired.
        
        Args:
            input_text (str): Input text to check
            model (str): LLM model name
            
        Returns:
            int or None: Cached PHI result (0/1) or None if not cached/expired
        """
        try:
            cache_key = self._generate_cache_key(input_text, model)
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Query with expiration check
            cursor.execute("""
                SELECT phi_result 
                FROM llm_phi_cache 
                WHERE cache_key = ? 
                  AND datetime(created_at, '+{} days') > datetime('now')
            """.format(self.cache_ttl_days), (cache_key,))
            
            result = cursor.fetchone()
            if result:
                self.logger.debug(f"Cache HIT for key: {cache_key[:16]}...")
                return result[0]
            
            self.logger.debug(f"Cache MISS for key: {cache_key[:16]}...")
            return None
            
        except Exception as e:
            self.logger.warning(f"Error retrieving from LLM cache: {e}")
            return None
    
    def store_result(self, input_text, model, phi_result):
        """
        Store LLM result in cache.
        
        Args:
            input_text (str): Input text that was processed
            model (str): LLM model name used
            phi_result (int): PHI detection result (0 or 1)
        """
        try:
            cache_key = self._generate_cache_key(input_text, model)
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Use INSERT OR REPLACE to handle duplicates
            cursor.execute("""
                INSERT OR REPLACE INTO llm_phi_cache 
                (cache_key, input_text, llm_model, phi_result, project_hash, created_at)
                VALUES (?, ?, ?, ?, ?, datetime('now'))
            """, (cache_key, input_text, model, phi_result, self.project_hash_root))
            
            conn.commit()
            self.logger.debug(f"Cached result for key: {cache_key[:16]}... -> {phi_result}")
            
        except Exception as e:
            self.logger.warning(f"Error storing to LLM cache: {e}")
    
    def cleanup_expired(self):
        """Remove expired entries from cache."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                DELETE FROM llm_phi_cache 
                WHERE datetime(created_at, '+{} days') <= datetime('now')
            """.format(self.cache_ttl_days))
            
            deleted_count = cursor.rowcount
            conn.commit()
            
            if deleted_count > 0:
                self.logger.info(f"Cleaned up {deleted_count} expired cache entries")
                
        except Exception as e:
            self.logger.warning(f"Error cleaning up LLM cache: {e}")
    
    def get_cache_stats(self):
        """Get cache statistics."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_entries,
                    COUNT(CASE WHEN datetime(created_at, '+{} days') > datetime('now') THEN 1 END) as valid_entries,
                    MAX(created_at) as latest_entry
                FROM llm_phi_cache 
                WHERE project_hash = ?
            """.format(self.cache_ttl_days), (self.project_hash_root,))
            
            result = cursor.fetchone()
            return {
                'total_entries': result[0] or 0,
                'valid_entries': result[1] or 0,
                'latest_entry': result[2],
                'cache_file': self.cache_file_path
            }
            
        except Exception as e:
            self.logger.warning(f"Error getting cache stats: {e}")
            return {'error': str(e)}
    
    def close(self):
        """Close database connection for current thread."""
        if hasattr(self._local, 'connection'):
            try:
                self._local.connection.close()
                delattr(self._local, 'connection')
            except Exception as e:
                self.logger.warning(f"Error closing LLM cache connection: {e}")
    
    def __del__(self):
        """Destructor to ensure database connections are closed."""
        try:
            self.close()
        except:
            pass  # Ignore errors during cleanup