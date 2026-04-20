from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Mapping, Optional, Tuple


@dataclass(slots=True, frozen=True)
class StakeHistoryItem:
    transaction_id: int
    transaction_type: str
    amount: Decimal
    balance_before: Decimal
    balance_after: Decimal
    transaction_ref: str
    created_at: datetime


@dataclass(slots=True, frozen=True)
class StakeBoundaryValidation:
    lower_limit: Decimal
    upper_limit: Decimal
    warning_lower: Decimal
    warning_upper: Decimal
    current_balance: Decimal
    is_within_bounds: bool
    approaching_lower_warning: bool
    approaching_upper_warning: bool
    reached_lower_limit: bool
    reached_upper_limit: bool


@dataclass(slots=True, frozen=True)
class StakeMonitorSummary:
    session_id: int
    gambler_id: int
    session_status: str
    end_reason: Optional[str]
    current_stake: Decimal
    starting_stake: Decimal
    peak_stake: Decimal
    lowest_stake: Decimal
    volatility: Decimal
    total_changes: int
    boundary_validation: StakeBoundaryValidation


@dataclass(slots=True, frozen=True)
class StakeHistoryReport:
    session_id: int
    gambler_id: int
    transaction_count: int
    starting_balance: Decimal
    ending_balance: Decimal
    net_change: Decimal
    transaction_breakdown: Mapping[str, int]
    monitor_summary: StakeMonitorSummary
    transactions: Tuple[StakeHistoryItem, ...]
