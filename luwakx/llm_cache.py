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
    
    Supports concurrent reads and serialized writes for parallel processing
    on single or multiple nodes (with shared filesystem).
    """
    
    def __init__(self, cache_file_path, cache_ttl_days=30):
        """
        Initialize the LLM result cache.
        
        Args:
            cache_file_path (str): Path to SQLite cache file
            cache_ttl_days (int): Cache TTL in days (default: 30)
        """
        self.cache_file_path = cache_file_path
        self.cache_ttl_days = cache_ttl_days
        self.logger = get_logger('llm_cache')
        
        # Thread lock for write operations (serializes writes across threads)
        self._write_lock = threading.Lock()
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.cache_file_path), exist_ok=True)
        
        # Initialize database connection with settings for concurrent access
        self.conn = sqlite3.connect(
            self.cache_file_path,
            check_same_thread=False,  # Allow multi-threaded access
            timeout=30.0  # Wait up to 30 seconds for locks
        )
        
        # Enable WAL mode for better concurrent read/write performance
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA synchronous=NORMAL')
        self.conn.execute('PRAGMA busy_timeout=30000')  # 30 second timeout
        
        # Initialize database
        self._init_database()
    
    def _init_database(self):
        """Initialize the SQLite database with required schema."""
        try:
            cursor = self.conn.cursor()
            
            # Create cache table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS llm_phi_cache (
                    cache_key TEXT PRIMARY KEY,
                    input_text TEXT NOT NULL,
                    llm_model TEXT NOT NULL,
                    phi_result INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create index for cleanup by timestamp
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_created_at 
                ON llm_phi_cache(created_at)
            """)
            
            self.conn.commit()
            self.logger.debug(f"Initialized LLM cache database: {self.cache_file_path}")
            
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
            log_project_stacktrace(self.logger, e)
            raise
    
    def _generate_cache_key(self, input_text, model):
        """
        Generate a deterministic cache key for the input.
        
        PHI/PII detection results are universal across all projects,
        so the cache is shared globally for efficiency.
        
        Args:
            input_text (str): Input text to cache
            model (str): LLM model name
            
        Returns:
            str: Hexadecimal cache key
        """
        # Hash only input and model - results are project-independent
        key_source = f"{model}:{input_text}"
        return hashlib.sha256(key_source.encode('utf-8')).hexdigest()
    
    def get_cached_result(self, input_text, model):
        """
        Retrieve cached LLM result if available and not expired (read-only).
        
        This method performs a read-only lookup and does NOT create
        new entries. Thread-safe for concurrent reads.
        
        Args:
            input_text (str): Input text to check
            model (str): LLM model name
            
        Returns:
            int or None: Cached PHI result (0/1) or None if not cached/expired
        """
        try:
            cache_key = self._generate_cache_key(input_text, model)
            
            # Read from database (no lock needed for reads in WAL mode)
            cursor = self.conn.cursor()
            
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
        
        This method creates a new cache entry with write locking to prevent
        race conditions. Thread-safe with write serialization.
        
        Args:
            input_text (str): Input text that was processed
            model (str): LLM model name used
            phi_result (int): PHI detection result (0 or 1)
        """
        try:
            cache_key = self._generate_cache_key(input_text, model)
            
            # Acquire write lock to serialize write operations
            with self._write_lock:
                cursor = self.conn.cursor()
                
                # Use INSERT OR REPLACE to handle duplicates
                cursor.execute("""
                    INSERT OR REPLACE INTO llm_phi_cache 
                    (cache_key, input_text, llm_model, phi_result, created_at)
                    VALUES (?, ?, ?, ?, datetime('now'))
                """, (cache_key, input_text, model, phi_result))
                
                self.conn.commit()
                self.logger.private(f"Cached result for key: {cache_key[:16]}... -> {phi_result}")
            
        except Exception as e:
            self.logger.warning(f"Error storing to LLM cache: {e}")
    
    def cleanup_expired(self):
        """Remove expired entries from cache."""
        try:
            # Acquire write lock for cleanup operation
            with self._write_lock:
                cursor = self.conn.cursor()
                
                cursor.execute("""
                    DELETE FROM llm_phi_cache 
                    WHERE datetime(created_at, '+{} days') <= datetime('now')
                """.format(self.cache_ttl_days))
                
                deleted_count = cursor.rowcount
                self.conn.commit()
                
                if deleted_count > 0:
                    self.logger.info(f"Cleaned up {deleted_count} expired cache entries")
                
        except Exception as e:
            self.logger.warning(f"Error cleaning up LLM cache: {e}")
    
    def get_cache_stats(self):
        """Get cache statistics."""
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_entries,
                    COUNT(CASE WHEN datetime(created_at, '+{} days') > datetime('now') THEN 1 END) as valid_entries,
                    MAX(created_at) as latest_entry
                FROM llm_phi_cache
            """.format(self.cache_ttl_days))
            
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
        """Close database connection."""
        if self.conn:
            try:
                self.conn.close()
            except Exception as e:
                self.logger.warning(f"Error closing LLM cache connection: {e}")
    
    def __del__(self):
        """Destructor to ensure database connections are closed."""
        try:
            self.close()
        except:
            pass  # Ignore errors during cleanup