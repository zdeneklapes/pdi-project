from datetime import datetime
from typing import Literal, List


class MyLogger:
    # instance = None

    def __init__(self, environment: List[Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]] = None):
        if environment is None:
            environment = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        self.ENVIRONMENT: List[Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]] = environment

    def _get_time(self):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def debug(self, message):
        if "DEBUG" in self.ENVIRONMENT:
            print(f"DEBUG ({self._get_time()}): {message}")

    def info(self, message):
        if "INFO" in self.ENVIRONMENT:
            print(f"INFO ({self._get_time()}): {message}")

    def warning(self, message):
        if "WARNING" in self.ENVIRONMENT:
            print(f"WARNING: {message}")

    def _assert(self, condition, message):
        if not condition:
            assert False, message

    def debug_overwrite(self, message):
        if "DEBUG" in self.ENVIRONMENT:
            print(f"\rDEBUG: {message}", end="")
