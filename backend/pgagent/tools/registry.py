"""Tool registry — maps action types to execution functions with metadata."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from pgagent.models import ActionType, RiskLevel


@dataclass
class ToolInfo:
    action_type: ActionType
    func: Callable[..., Any]
    risk_level: RiskLevel
    reversible: bool
    description: str


class ToolRegistry:
    """Registry of available tools/actions."""

    def __init__(self) -> None:
        self._tools: dict[ActionType, ToolInfo] = {}

    def register(
        self,
        action_type: ActionType,
        func: Callable[..., Any],
        risk_level: RiskLevel,
        reversible: bool,
        description: str,
    ) -> None:
        self._tools[action_type] = ToolInfo(
            action_type=action_type,
            func=func,
            risk_level=risk_level,
            reversible=reversible,
            description=description,
        )

    def get(self, action_type: ActionType) -> ToolInfo | None:
        return self._tools.get(action_type)

    def list_tools(self) -> list[ToolInfo]:
        return list(self._tools.values())


def create_default_registry() -> ToolRegistry:
    """Create registry with all built-in tools."""
    from pgagent.tools.connection_tools import kill_idle_transaction
    from pgagent.tools.index_tools import create_index, drop_index
    from pgagent.tools.maintenance_tools import analyze_table, vacuum_table

    registry = ToolRegistry()
    registry.register(
        ActionType.CREATE_INDEX,
        create_index,
        RiskLevel.MEDIUM,
        reversible=True,
        description="Create an index concurrently",
    )
    registry.register(
        ActionType.DROP_INDEX,
        drop_index,
        RiskLevel.HIGH,
        reversible=False,
        description="Drop an index concurrently",
    )
    registry.register(
        ActionType.VACUUM,
        vacuum_table,
        RiskLevel.LOW,
        reversible=False,
        description="Vacuum a table",
    )
    registry.register(
        ActionType.ANALYZE,
        analyze_table,
        RiskLevel.LOW,
        reversible=False,
        description="Analyze a table",
    )
    registry.register(
        ActionType.KILL_CONNECTION,
        kill_idle_transaction,
        RiskLevel.MEDIUM,
        reversible=False,
        description="Terminate an idle-in-transaction backend",
    )
    return registry
