"""LLM-powered reasoning engine for pgAgent using Groq."""

from __future__ import annotations

import json
import logging
from typing import Any

from pgagent.config import Settings
from pgagent.exceptions import (
    LLMConnectionError,
    LLMError,
    LLMRateLimitError,
    LLMResponseParseError,
    LLMTimeoutError,
)
from pgagent.models import (
    Detection,
    LLMIndexRecommendation,
    LLMPrioritizationResult,
    Severity,
)
from pgagent.prompts import build_index_recommendation_prompt, build_prioritization_prompt
from pgagent.sidecar import SidecarDB

logger = logging.getLogger(__name__)

# Severity ordering for fallback prioritization (most urgent first).
_SEVERITY_ORDER: dict[Severity, int] = {
    Severity.CRITICAL: 0,
    Severity.HIGH: 1,
    Severity.MEDIUM: 2,
    Severity.LOW: 3,
}


class Reasoner:
    """LLM-powered reasoning for index recommendations and detection prioritization.

    All public methods are designed for graceful degradation: they catch errors
    internally and return safe defaults so the agent never crashes due to LLM
    failures.
    """

    def __init__(self, settings: Settings, sidecar: SidecarDB) -> None:
        self._settings = settings
        self._sidecar = sidecar
        self._enabled = False
        self._client: Any = None  # groq.Groq instance or None

        if not settings.groq_api_key:
            logger.warning(
                "No Groq API key configured (PGAGENT_GROQ_API_KEY). "
                "LLM reasoning is disabled; the agent will operate without "
                "index recommendations or LLM-based prioritization."
            )
            return

        try:
            import groq

            self._client = groq.Groq(api_key=settings.groq_api_key)
            self._enabled = True
            logger.info("Groq LLM client initialized (model=%s).", settings.llm_model)
        except ImportError:
            logger.warning(
                "groq package is not installed. LLM reasoning is disabled. "
                "Install with: pip install groq"
            )
        except Exception:
            logger.exception("Failed to initialize Groq client. LLM reasoning is disabled.")

    # ── Public API ────────────────────────────────────────────────────────

    def recommend_indexes(
        self,
        detection: Detection,
        table_info: dict[str, Any],
    ) -> list[LLMIndexRecommendation]:
        """Recommend indexes for a SEQ_SCAN_HEAVY detection.

        Args:
            detection: The Detection that triggered this analysis.
            table_info: Dict with keys:
                - table_columns: list[str]
                - existing_indexes: list[str]
                - slow_queries: list[dict] (each with query, calls, mean_exec_time, rows)
                - table_stats: dict with seq_scan, idx_scan, n_live_tup, n_dead_tup,
                  table_size_bytes

        Returns:
            A list of LLMIndexRecommendation objects. Returns an empty list
            on failure or when the LLM is disabled/degraded.
        """
        if not self._enabled:
            logger.debug("LLM disabled; skipping index recommendation.")
            return []

        # Check consecutive failure threshold for graceful degradation.
        consecutive_failures = self._sidecar.get_consecutive_llm_failures()
        if consecutive_failures >= self._settings.llm_max_consecutive_failures:
            logger.warning(
                "LLM has %d consecutive failures (threshold=%d). "
                "Skipping index recommendation to avoid cascading errors.",
                consecutive_failures,
                self._settings.llm_max_consecutive_failures,
            )
            return []

        table_name = detection.target_table or "unknown"

        try:
            messages = build_index_recommendation_prompt(
                table_name=table_name,
                table_columns=table_info.get("table_columns", []),
                existing_indexes=table_info.get("existing_indexes", []),
                slow_queries=table_info.get("slow_queries", []),
                table_stats=table_info.get("table_stats", {}),
            )

            raw_response = self._call_llm(messages, purpose="index_recommendation")

            # Parse and validate the response.
            data = json.loads(raw_response)
            raw_recommendations = data.get("recommendations", [])
            if not isinstance(raw_recommendations, list):
                raise LLMResponseParseError(
                    f"Expected 'recommendations' to be a list, got {type(raw_recommendations).__name__}"
                )

            recommendations: list[LLMIndexRecommendation] = []
            for item in raw_recommendations:
                # Inject table_name if the LLM omitted it.
                if "table_name" not in item:
                    item["table_name"] = table_name
                rec = LLMIndexRecommendation.model_validate(item)
                recommendations.append(rec)

            # Success: clear the failure tracker.
            self._sidecar.clear_llm_failures()

            logger.info(
                "LLM recommended %d index(es) for table %s.",
                len(recommendations),
                table_name,
            )
            return recommendations

        except LLMError:
            # Already recorded in _call_llm; just log and return safe default.
            logger.exception(
                "LLM error during index recommendation for table %s.", table_name
            )
            return []
        except json.JSONDecodeError as exc:
            self._sidecar.record_llm_failure("response_parse", str(exc))
            logger.exception(
                "Failed to parse LLM JSON response for index recommendation on table %s.",
                table_name,
            )
            return []
        except Exception as exc:
            self._sidecar.record_llm_failure("unexpected", str(exc))
            logger.exception(
                "Unexpected error during index recommendation for table %s.", table_name
            )
            return []

    def prioritize_detections(
        self,
        detections: list[Detection],
    ) -> LLMPrioritizationResult:
        """Prioritize a list of detections by urgency and impact.

        Falls back to severity-based ordering when the LLM is unavailable or
        when there are 0-1 detections.

        Args:
            detections: The detections to prioritize.

        Returns:
            An LLMPrioritizationResult with ordered IDs and reasoning.
        """
        # Trivial cases: no LLM needed.
        if len(detections) <= 1:
            return LLMPrioritizationResult(
                ordered_detection_ids=[d.id for d in detections if d.id is not None],
                reasoning="Single or no detections; no prioritization needed.",
            )

        if not self._enabled:
            logger.debug("LLM disabled; falling back to severity-based prioritization.")
            return self._severity_fallback(detections)

        # Check consecutive failure threshold.
        consecutive_failures = self._sidecar.get_consecutive_llm_failures()
        if consecutive_failures >= self._settings.llm_max_consecutive_failures:
            logger.warning(
                "LLM has %d consecutive failures (threshold=%d). "
                "Falling back to severity-based prioritization.",
                consecutive_failures,
                self._settings.llm_max_consecutive_failures,
            )
            return self._severity_fallback(detections)

        try:
            det_dicts: list[dict[str, str | int]] = []
            for d in detections:
                entry: dict[str, Any] = {
                    "id": d.id if d.id is not None else 0,
                    "detection_type": d.detection_type.value,
                    "severity": d.severity.value,
                    "message": d.message,
                }
                if d.target_table:
                    entry["target_table"] = d.target_table
                det_dicts.append(entry)

            messages = build_prioritization_prompt(det_dicts)
            raw_response = self._call_llm(messages, purpose="prioritization")

            data = json.loads(raw_response)
            result = LLMPrioritizationResult.model_validate(data)

            # Success: clear the failure tracker.
            self._sidecar.clear_llm_failures()

            logger.info(
                "LLM prioritized %d detections. Top priority: ID %s.",
                len(result.ordered_detection_ids),
                result.ordered_detection_ids[0] if result.ordered_detection_ids else "none",
            )
            return result

        except LLMError:
            logger.exception("LLM error during detection prioritization.")
            return self._severity_fallback(detections)
        except json.JSONDecodeError as exc:
            self._sidecar.record_llm_failure("response_parse", str(exc))
            logger.exception("Failed to parse LLM JSON response for prioritization.")
            return self._severity_fallback(detections)
        except Exception as exc:
            self._sidecar.record_llm_failure("unexpected", str(exc))
            logger.exception("Unexpected error during detection prioritization.")
            return self._severity_fallback(detections)

    # ── Internal helpers ──────────────────────────────────────────────────

    def _call_llm(self, messages: list[dict[str, str]], purpose: str) -> str:
        """Invoke the Groq chat completions API and return the raw content string.

        Handles and translates all expected Groq error types into the pgAgent
        exception hierarchy, and records failures in the sidecar.

        Args:
            messages: The list of chat messages (system + user).
            purpose: A short label for logging, e.g. "index_recommendation".

        Returns:
            The raw text content from the LLM response.

        Raises:
            LLMConnectionError: Could not reach Groq.
            LLMRateLimitError: Rate limit exceeded.
            LLMTimeoutError: Request timed out.
            LLMResponseParseError: Response was empty or malformed.
        """
        import groq

        try:
            response = self._client.chat.completions.create(
                model=self._settings.llm_model,
                messages=messages,
                temperature=0,
                response_format={"type": "json_object"},
                timeout=self._settings.llm_timeout_seconds,
            )
        except groq.APIConnectionError as exc:
            self._sidecar.record_llm_failure("connection", str(exc))
            raise LLMConnectionError(
                f"Failed to connect to Groq API during {purpose}: {exc}"
            ) from exc
        except groq.RateLimitError as exc:
            self._sidecar.record_llm_failure("rate_limit", str(exc))
            raise LLMRateLimitError(
                f"Groq rate limit exceeded during {purpose}: {exc}"
            ) from exc
        except groq.APITimeoutError as exc:
            self._sidecar.record_llm_failure("timeout", str(exc))
            raise LLMTimeoutError(
                f"Groq request timed out during {purpose}: {exc}"
            ) from exc
        except groq.APIStatusError as exc:
            self._sidecar.record_llm_failure("api_error", str(exc))
            raise LLMConnectionError(
                f"Groq API error during {purpose}: {exc}"
            ) from exc

        # Extract the content from the response.
        content = response.choices[0].message.content if response.choices else None
        if not content:
            self._sidecar.record_llm_failure("empty_response", "LLM returned empty content")
            raise LLMResponseParseError(
                f"LLM returned empty response during {purpose}."
            )

        logger.debug("LLM response for %s: %s", purpose, content[:200])
        return content

    @staticmethod
    def _severity_fallback(detections: list[Detection]) -> LLMPrioritizationResult:
        """Produce a deterministic severity-based ordering as a fallback.

        Detections are sorted by severity (CRITICAL > HIGH > MEDIUM > LOW),
        with ties broken by detection time (earlier first).
        """
        sorted_dets = sorted(
            detections,
            key=lambda d: (
                _SEVERITY_ORDER.get(d.severity, 99),
                d.detected_at,
            ),
        )
        return LLMPrioritizationResult(
            ordered_detection_ids=[d.id for d in sorted_dets if d.id is not None],
            reasoning="Fallback: ordered by severity (CRITICAL > HIGH > MEDIUM > LOW), then detection time.",
        )
