import os
import time
from dataclasses import dataclass, asdict


@dataclass
class CsePerformanceCounter:
    timestamp: str
    bucket: str
    object_key: str
    file_type: str
    operation: str
    cse: str
    duration: float


class CsePerformanceCounters:
    read = "read"
    write = "write"
    head = "head"

    def __init__(self):
        self._counters = []

    def add_counter(self, bucket, object_key, operation, cse, duration):
        file_extension = file_extension = os.path.splitext(object_key)
        file_type = file_extension[1].replace('.', '', 1).lower() if file_extension[1] else ''
        cse_string = "CSE" if cse else "NO CSE"
        counter = CsePerformanceCounter(timestamp=time.asctime(), bucket=bucket,
                                        object_key=object_key,
                                        operation=operation, cse=cse_string,
                                        file_type=file_type,
                                        duration=duration)
        self._counters.append(asdict(counter))
