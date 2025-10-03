# luwakx/deid_logger_handler.py

import threading
import time

class DeidProgressHandler:
    def __init__(self, luwak_logger, total_slices, interval_sec=6, series_folder_name=None):
        self.luwak_logger = luwak_logger
        self.total_slices = total_slices
        self.anonymized_count = 0
        self.lock = threading.Lock()
        self._stop = False
        self.interval_sec = interval_sec
        self.series_folder_name = series_folder_name
        self._thread = threading.Thread(target=self._report_progress)
        self._thread.daemon = True
        self._thread.start()

    def write(self, msg):
        # Immediate reporting for errors or warnings
        if "ERROR" in msg or "Error" in msg or "WARNING" in msg or "Warning" in msg:
            self.luwak_logger.warning(msg.strip())
        elif "Anonymized file" in msg:
            with self.lock:
                self.anonymized_count += 1
        # Ignore other lines

    def flush(self):
        pass

    def close(self):
        self._stop = True
        self._thread.join()

    def _report_progress(self):
        while not self._stop:
            time.sleep(self.interval_sec)
            with self.lock:
                percent = (self.anonymized_count / self.total_slices) * 100 if self.total_slices else 0
                msg = (
                    f"Anonymized {self.anonymized_count} slices out of {self.total_slices} "
                    f"({percent:.1f}% complete) for series {self.series_folder_name}"
                )
                self.luwak_logger.info(msg)
