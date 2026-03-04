"""Safety validator for pgAgent suggestions.

All proposed database changes pass through these 7 safety rules before
execution is permitted.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta

from pgagent.config import Settings
from pgagent.models import ActionType, Suggestion
from pgagent.sidecar import SidecarDB

logger = logging.getLogger(__name__)

# Regex patterns for system catalog detection.
# Matches pg_catalog, information_schema, or pg_* system tables used as
# modification targets (after FROM, JOIN, UPDATE, INTO, TABLE, INDEX ON, etc.).
_SYSTEM_CATALOG_PATTERN = re.compile(
    r"""
    (?:
        \b(?:FROM|JOIN|UPDATE|INTO|TABLE|ON|VACUUM|ANALYZE|INDEX)\s+  # target keywords
    )
    (?:
        pg_catalog\.\w+             # pg_catalog.anything
        | information_schema\.\w+   # information_schema.anything
        | pg_\w+                    # pg_* system tables (pg_class, pg_stat_*, ...)
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

_CONCURRENTLY_PATTERN = re.compile(r"\bCONCURRENTLY\b", re.IGNORECASE)

# Mutation action types that count toward concurrent-mutation limits.
_MUTATION_ACTIONS = frozenset({ActionType.CREATE_INDEX, ActionType.DROP_INDEX})


class SafetyValidator:
    """Validates suggestions against 7 safety rules before execution."""

    def __init__(self, sidecar: SidecarDB, settings: Settings) -> None:
        self._sidecar = sidecar
        self._settings = settings

    def validate(self, suggestion: Suggestion) -> tuple[bool, str]:
        """Run all safety rules against *suggestion*.

        Returns ``(True, "")`` when the suggestion passes all checks,
        or ``(False, reason)`` on the first rule that rejects it.
        """
        checks = [
            self._check_system_catalog,
            self._check_pk_unique_index_drop,
            self._check_concurrently,
            self._check_index_rate_limit,
            self._check_kill_threshold,
            self._check_rollback_cooldown,
            self._check_concurrent_mutations,
        ]
        for check in checks:
            ok, reason = check(suggestion)
            if not ok:
                logger.warning(
                    "Suggestion id=%s rejected by %s: %s",
                    suggestion.id,
                    check.__name__,
                    reason,
                )
                return False, reason
        return True, ""

    # ── Rule 1: No system catalog modification ───────────────────────

    def _check_system_catalog(self, suggestion: Suggestion) -> tuple[bool, str]:
        """SQL must not reference pg_catalog, information_schema, or pg_*
        system tables as targets."""
        if _SYSTEM_CATALOG_PATTERN.search(suggestion.sql):
            return (
                False,
                "SQL references system catalog tables (pg_catalog / information_schema / pg_*)",
            )
        return True, ""

    # ── Rule 2: No PK / unique index drops ───────────────────────────

    def _check_pk_unique_index_drop(self, suggestion: Suggestion) -> tuple[bool, str]:
        """If action_type is DROP_INDEX, check whether the index is primary
        or unique by looking up the latest snapshot data stored in the sidecar."""
        if suggestion.action_type != ActionType.DROP_INDEX:
            return True, ""

        index_name = suggestion.target_index
        if not index_name:
            return True, ""

        # Look up index metadata from the most recent snapshot.
        snapshots = self._sidecar.get_recent_snapshots(hours=1)
        for snap in reversed(snapshots):  # most recent first
            indexes = snap.get("data", {}).get("indexes", [])
            for idx in indexes:
                if idx.get("index_name") == index_name:
                    if idx.get("is_primary"):
                        return (
                            False,
                            f"Cannot drop primary-key index: {index_name}",
                        )
                    if idx.get("is_unique"):
                        return (
                            False,
                            f"Cannot drop unique index: {index_name}",
                        )
                    # Found the index and it's neither PK nor unique.
                    return True, ""

        # Index not found in snapshots -- allow (we can't prove it's PK/unique).
        return True, ""

    # ── Rule 3: CONCURRENTLY required for index ops ──────────────────

    def _check_concurrently(self, suggestion: Suggestion) -> tuple[bool, str]:
        """CREATE INDEX and DROP INDEX SQL must contain the CONCURRENTLY keyword."""
        if suggestion.action_type not in (
            ActionType.CREATE_INDEX,
            ActionType.DROP_INDEX,
        ):
            return True, ""

        if not _CONCURRENTLY_PATTERN.search(suggestion.sql):
            return (
                False,
                "Index DDL must use CONCURRENTLY to avoid holding exclusive locks",
            )
        return True, ""

    # ── Rule 4: Index rate limit ─────────────────────────────────────

    def _check_index_rate_limit(self, suggestion: Suggestion) -> tuple[bool, str]:
        """CREATE_INDEX must be < max_index_creates_per_hour; DROP_INDEX must
        be < max_index_drops_per_hour."""
        if suggestion.action_type == ActionType.CREATE_INDEX:
            recent = self._sidecar.count_recent_actions(ActionType.CREATE_INDEX, hours=1)
            if recent >= self._settings.max_index_creates_per_hour:
                return (
                    False,
                    f"Index creation rate limit reached ({recent}/{self._settings.max_index_creates_per_hour} per hour)",
                )
        elif suggestion.action_type == ActionType.DROP_INDEX:
            recent = self._sidecar.count_recent_actions(ActionType.DROP_INDEX, hours=1)
            if recent >= self._settings.max_index_drops_per_hour:
                return (
                    False,
                    f"Index drop rate limit reached ({recent}/{self._settings.max_index_drops_per_hour} per hour)",
                )
        return True, ""

    # ── Rule 5: Kill threshold ───────────────────────────────────────

    def _check_kill_threshold(self, suggestion: Suggestion) -> tuple[bool, str]:
        """KILL_CONNECTION actions are capped per cycle (1-hour window)."""
        if suggestion.action_type != ActionType.KILL_CONNECTION:
            return True, ""

        recent = self._sidecar.count_recent_actions(ActionType.KILL_CONNECTION, hours=1)
        if recent >= self._settings.kill_threshold_per_cycle:
            return (
                False,
                f"Kill connection threshold reached ({recent}/{self._settings.kill_threshold_per_cycle} per cycle)",
            )
        return True, ""

    # ── Rule 6: Cooldown after rollback ──────────────────────────────

    def _check_rollback_cooldown(self, suggestion: Suggestion) -> tuple[bool, str]:
        """After a rollback, reject new mutations until the cooldown expires."""
        if suggestion.action_type not in _MUTATION_ACTIONS:
            return True, ""

        last_rollback = self._sidecar.get_last_rollback_time()
        if last_rollback is None:
            return True, ""

        cooldown_end = last_rollback + timedelta(
            seconds=self._settings.rollback_cooldown_seconds
        )
        if datetime.utcnow() < cooldown_end:
            remaining = (cooldown_end - datetime.utcnow()).total_seconds()
            return (
                False,
                f"Rollback cooldown active ({int(remaining)}s remaining)",
            )
        return True, ""

    # ── Rule 7: Max concurrent mutations ─────────────────────────────

    def _check_concurrent_mutations(self, suggestion: Suggestion) -> tuple[bool, str]:
        """Only one mutation (CREATE_INDEX / DROP_INDEX) at a time."""
        if suggestion.action_type not in _MUTATION_ACTIONS:
            return True, ""

        if self._sidecar.has_active_mutation():
            return (
                False,
                "Another mutation is already in progress",
            )
        return True, ""
