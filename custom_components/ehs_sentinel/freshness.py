"""Runtime-only source evidence. Restored HA attributes are never evidence."""

from datetime import datetime, timezone
from time import monotonic

DEFAULT_MAX_AGE = 900


class SourceEvidence:
    """Per-register receipt clocks, invalidated at every transport boundary."""

    def __init__(self):
        self.epoch = 0
        self.connected = False
        self.receipts = {}

    def connect(self, connected):
        self.epoch += 1
        self.connected = connected
        self.receipts.clear()

    def record(self, name, received, epoch, timestamp):
        if not self.connected or epoch != self.epoch:
            return False
        previous = self.receipts.get(name)
        if previous and previous[0] > received:
            return False  # Parallel packet workers must not move clocks backwards.
        self.receipts[name] = (received, timestamp)
        return True

    def fresh(self, name, max_age=DEFAULT_MAX_AGE):
        receipt = self.receipts.get(name)
        return bool(self.connected and receipt and 0 <= monotonic() - receipt[0] <= max_age)

    @staticmethod
    def clock():
        return monotonic(), datetime.now(timezone.utc).isoformat()
