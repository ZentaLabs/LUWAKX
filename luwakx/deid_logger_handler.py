# luwakx/deid_logger_handler.py

import time
from tqdm import tqdm


class DeidProgressHandler:
    """Handles progress reporting and deid bot output routing during anonymization.

    Provides a tqdm terminal progress bar updated file-by-file, plus
    interval-based log messages for structured logging. Also acts as the
    write stream for deid's internal bot, forwarding errors and warnings
    to the project logger without corrupting the progress bar.

    Usage in the anonymization loop::

        handler.init_progress(total_files)
        for f in files:
            replace_identifiers(...)
            handler.update_progress(f)
        handler.close()   # called in finally block
    """

    def __init__(
        self,
        luwak_logger,
        total_slices,
        interval_sec=6,
        percent_interval=10,
        series_uid_name=None,
    ):
        self.luwak_logger = luwak_logger
        self.total_slices = total_slices
        self.anonymized_count = 0
        self.interval_sec = interval_sec
        self.percent_interval = percent_interval
        self.series_uid_name = series_uid_name
        self.last_log_time = time.time()
        self.last_logged_percent = 0
        self.pbar = None

    # ------------------------------------------------------------------
    # deid bot stream interface  (bot.outputStream / bot.errorStream)
    # ------------------------------------------------------------------

    def write(self, msg):
        """Receive output from deid's internal bot logger.

        Errors and warnings are forwarded to the project logger and also
        written via tqdm.write() so they appear above the progress bar
        without corrupting it.
        """
        import traceback

        if "ERROR" in msg or "Error" in msg:
            self.luwak_logger.error(msg.strip())
            tqdm.write(f"[ERROR] {msg.strip()}")
            # Log project-relevant stack trace for errors only
            tb_list = traceback.extract_stack()
            import os
            project_path = os.path.abspath(os.path.dirname(__file__))
            start_idx = next(
                (i for i, frame in enumerate(tb_list)
                 if project_path in os.path.abspath(frame.filename)),
                0,
            )
            filtered = tb_list[start_idx:]
            formatted = "".join(traceback.format_list(filtered))
            self.luwak_logger.error(f"Project stack trace block (deid):\n{formatted}")
        elif "WARNING" in msg or "Warning" in msg:
            self.luwak_logger.warning(msg.strip())
            tqdm.write(f"[WARNING] {msg.strip()}")
        # Other deid bot output (e.g. "Anonymized file") is intentionally
        # suppressed here; progress is tracked explicitly via update_progress().

    def flush(self):
        pass

    # ------------------------------------------------------------------
    # Explicit progress tracking (called from the anonymization loop)
    # ------------------------------------------------------------------

    def init_progress(self, total):
        """Initialise the tqdm bar and reset counters.

        Call this once before starting the per-file loop.
        """
        self.total_slices = total
        self.anonymized_count = 0
        self.last_log_time = time.time()
        self.last_logged_percent = 0
        self.luwak_logger.info(
            f"Starting anonymization of {total} files for series {self.series_uid_name}"
        )
        if self.pbar is not None:
            self.pbar.close()
        self.pbar = tqdm(
            total=total,
            desc=f"Series {self.series_uid_name}",
            unit="file",
            leave=True,
        )

    def update_progress(self, current_file=None):
        """Advance progress by one file and emit a log message when due.

        Call this once after each successful replace_identifiers() call.
        Logging is throttled by *both* ``percent_interval`` and
        ``interval_sec`` so that large series emit regular but not
        excessive log lines.
        """
        self.anonymized_count += 1
        if self.pbar is not None:
            self.pbar.update(1)

        if self.total_slices == 0:
            return

        current_percent = (self.anonymized_count / self.total_slices) * 100
        current_time = time.time()
        time_since_last_log = current_time - self.last_log_time
        percent_threshold = self.last_logged_percent + self.percent_interval

        should_log = (
            current_percent >= percent_threshold
            or time_since_last_log >= self.interval_sec
        )

        if should_log and self.anonymized_count < self.total_slices:
            msg = (
                f"Anonymized {self.anonymized_count} slices out of {self.total_slices} "
                f"({current_percent:.1f}% complete) for series {self.series_uid_name}"
            )
            self.luwak_logger.info(msg)
            self.last_log_time = current_time
            self.last_logged_percent = (
                int(current_percent / self.percent_interval) * self.percent_interval
            )

    def close(self):
        """Close the tqdm bar and emit the final 100 % log line."""
        if self.pbar is not None:
            self.pbar.close()
            self.pbar = None
        if self.total_slices > 0 and self.anonymized_count == self.total_slices:
            msg = (
                f"Anonymized {self.anonymized_count} slices out of {self.total_slices} "
                f"(100.0% complete) for series {self.series_uid_name}"
            )
            self.luwak_logger.info(msg)
