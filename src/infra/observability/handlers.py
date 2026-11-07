"""Logging off the pipeline's threads: a bounded queue, drained by a thread."""

from __future__ import annotations

import copy
import logging
from logging import handlers
import queue
import threading


class BoundedQueueHandler(handlers.QueueHandler):
    """Puts records in a bounded queue; drops them when it is full.

    A slow sink (a remote collector) must never slow the pipelines down, nor
    make them run out of memory: when the queue is full, records are
    dropped and counted, and the count is logged once there is room again.

    Attributes:
        dropped: The records dropped since the last report.
    """

    def __init__(self, records: queue.Queue[logging.LogRecord]) -> None:
        super().__init__(records)
        self._records = records
        self.dropped = 0
        self._lock = threading.Lock()

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        """Freezes the message, keeping the exception for the formatters.

        The default would format the record here, with this handler's
        formatter: each sink formats it its own way instead.
        """
        prepared = copy.copy(record)
        prepared.msg = record.getMessage()
        prepared.args = None
        return prepared

    def enqueue(self, record: logging.LogRecord) -> None:
        try:
            self._records.put_nowait(record)
        except queue.Full:
            with self._lock:
                self.dropped += 1
            return
        with self._lock:
            dropped, self.dropped = self.dropped, 0
        if dropped:
            notice = logging.makeLogRecord({
                "name": "etl.logging",
                "levelno": logging.WARNING,
                "levelname": "WARNING",
                "msg": (
                    f"{dropped} log record(s) dropped: the log queue was full"
                ),
                "etl.dropped": dropped,
            })
            try:
                self._records.put_nowait(notice)
            except queue.Full:
                with self._lock:
                    self.dropped += dropped
