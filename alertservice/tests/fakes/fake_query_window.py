from typing import Any, Dict, List


class FakeQueryWindow:
    def __init__(self, rows: List[Dict[str, Any]] = None):
        self._rows = rows or []
        self.calls: List[tuple] = []

    def __call__(self, pipeline: str, window_type: str, window_value: int) -> List[Dict[str, Any]]:
        self.calls.append((pipeline, window_type, window_value))
        return self._rows
