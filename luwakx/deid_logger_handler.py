# luwakx/deid_logger_handler.py

import re
import time
from tqdm import tqdm

# Regex to extract a DICOM tag pair (XXXX, XXXX) from deid / pydicom warning messages.
_RE_TAG      = re.compile(r'\(([0-9a-fA-F]{4})[,\s]+([0-9a-fA-F]{4})\)')
# Regex to extract a two-to-three-letter VR token (e.g. "IS", "DS", "LO").
_RE_VR       = re.compile(r'\bVR[\s\'":]+([A-Z]{2,3})\b|\b([A-Z]{2,3})[\s\'":]+VR\b')
# Regex to extract a quoted value from the message.
_RE_VALUE    = re.compile(r"[\"']([^\"']{0,256})[\"']")
# Keywords that indicate the message is about an invalid VR *content* (not just any warning).
_VR_WARN_KEYWORDS = ("invalid", "not valid", "not a valid", "vr", "value representation")


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
        review_collector=None,
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
        # Optional ReviewFlagCollector - receives VR-format warnings caught from deid/pydicom.
        self.review_collector = review_collector
        # SOPInstanceUID of the file currently being processed; updated before each
        # replace_identifiers() call so warnings can be attributed to the right instance.
        self._current_sop_uid: str = "*"

    # ------------------------------------------------------------------
    # deid bot stream interface  (bot.outputStream / bot.errorStream)
    # ------------------------------------------------------------------

    def set_current_instance_uid(self, sop_instance_uid: str) -> None:
        """Record the SOPInstanceUID of the file about to be processed.

        Call this immediately before each ``replace_identifiers()`` invocation so
        that VR-format warnings emitted during that call can be attributed to the
        correct DICOM instance in the review CSV.
        """
        self._current_sop_uid = sop_instance_uid or "*"

    def write(self, msg):
        """Receive output from deid's internal bot logger.

        Errors and warnings are forwarded to the project logger and also
        written via tqdm.write() so they appear above the progress bar
        without corrupting it.

        Additionally, WARNING messages that indicate an invalid VR *content*
        (e.g. ``DS`` field containing letters) are parsed and forwarded to the
        ``ReviewFlagCollector`` when one is attached.
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
            # Forward to review CSV: deid errors indicate per-instance (or series-level)
            # failures that should be visible to reviewers.
            self._try_capture_deid_error(msg)
        elif "WARNING" in msg or "Warning" in msg:
            self.luwak_logger.warning(msg.strip())
            tqdm.write(f"[WARNING] {msg.strip()}")
            self._try_capture_vr_warning(msg)
        # Other deid bot output (e.g. "Anonymized file") is intentionally
        # suppressed here; progress is tracked explicitly via update_progress().

    def _try_capture_vr_warning(self, msg: str) -> None:
        """Parse *msg* for a VR-format violation and forward it to the collector.

        Only fires when:
        * a ``review_collector`` is attached, and
        * the message lower-cased contains at least one VR invalid-content keyword.
        """
        if self.review_collector is None:
            return
        msg_lower = msg.lower()
        if not any(kw in msg_lower for kw in _VR_WARN_KEYWORDS):
            return

        # Try to extract a tag (XXXX, XXXX) from the message.
        tag_match = _RE_TAG.search(msg)
        if tag_match:
            tag_group   = tag_match.group(1).upper()
            tag_element = tag_match.group(2).upper()
        else:
            tag_group   = "UNKN"
            tag_element = "UNKN"

        # Try to extract a VR token.
        vr_match = _RE_VR.search(msg)
        if vr_match:
            vr = (vr_match.group(1) or vr_match.group(2) or "").upper()
        else:
            vr = ""

        # Try to extract a quoted value (take first match).
        value_match = _RE_VALUE.search(msg)
        original_value = value_match.group(1) if value_match else ""

        try:
            from review_flag_collector import ReviewFlagCollector
            self.review_collector.add_flag(
                tag_group       = tag_group,
                tag_element     = tag_element,
                attribute_name  = "",
                keyword         = "",
                vr              = vr,
                vm              = "",
                reason          = ReviewFlagCollector.REASON_VR_FORMAT_INVALID,
                sop_instance_uid= self._current_sop_uid,
                original_value  = original_value,
                keep            = 1,  # deid passes the value through when format is bad
                output_value    = original_value,
            )
        except Exception:
            pass  # Never let review-CSV logic break the main anonymization flow

    def _try_capture_deid_error(self, msg: str) -> None:
        """Forward an ERROR message from deid's bot to the review_collector.

        Records a flag with ``tag_group='*'`` and ``tag_element='*'`` (no specific
        tag is identified) and ``reason=REASON_SERIES_FAILED`` so reviewers can see
        that deid reported an error during processing of this instance/series.

        Only fires when a ``review_collector`` is attached.
        """
        if self.review_collector is None:
            return

        # Truncate long messages so the CSV stays readable.
        recorded_msg = msg.strip()[:512]

        try:
            from review_flag_collector import ReviewFlagCollector
            self.review_collector.add_flag(
                tag_group       = "*",
                tag_element     = "*",
                attribute_name  = "deid error",
                keyword         = "",
                vr              = "",
                vm              = "",
                reason          = ReviewFlagCollector.REASON_SERIES_FAILED,
                sop_instance_uid= self._current_sop_uid,
                original_value  = recorded_msg,
                keep            = 0,
                output_value    = "",
            )
        except Exception:
            pass  # Never let review-CSV logic break the main anonymization flow

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
