from typing import Any, Dict, List


class FakeSummaryStore:
    def __init__(self):
        self.stored: List[Dict[str, Any]] = []

    def __call__(self, summary: Dict[str, Any]) -> None:
        self.stored.append(summary)
