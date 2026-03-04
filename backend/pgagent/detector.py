"""Detector: runs all deterministic rules and returns detections."""

from __future__ import annotations

from pgagent.config import Settings
from pgagent.models import Detection, SnapshotWithDeltas
from pgagent.rules import ALL_RULES


class Detector:
    """Run every registered rule against a snapshot and collect detections.

    The caller is responsible for routing detections that have
    ``llm_reasoning_needed=True`` to the LLM pipeline and the rest
    directly to the suggestion engine.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    # -- public API --------------------------------------------------------

    def detect(
        self,
        snapshot_with_deltas: SnapshotWithDeltas,
    ) -> list[Detection]:
        """Execute every rule and return an aggregated list of detections."""
        detections: list[Detection] = []
        for rule_fn in ALL_RULES:
            detections.extend(rule_fn(snapshot_with_deltas, self._settings))
        return detections

    # -- convenience helpers -----------------------------------------------

    @staticmethod
    def needs_llm(detections: list[Detection]) -> list[Detection]:
        """Return only the detections that require LLM reasoning."""
        return [d for d in detections if d.llm_reasoning_needed]

    @staticmethod
    def direct(detections: list[Detection]) -> list[Detection]:
        """Return only the detections that can be handled directly."""
        return [d for d in detections if not d.llm_reasoning_needed]
