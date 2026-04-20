from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional


@dataclass(slots=True)
class GamblerProfile:
    gambler_id: Optional[int]
    username: str
    full_name: str
    email: str
    initial_stake: Decimal
    current_stake: Decimal
    win_threshold: Decimal
    loss_threshold: Decimal
    min_required_stake: Decimal = Decimal("0.00")
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass(slots=True)
class BettingPreferences:
    profile_id: Optional[int]
    min_bet: Decimal
    max_bet: Decimal
    preferred_game_type: str
    auto_play_enabled: bool = False
    auto_play_max_games: int = 0
    session_loss_limit: Optional[Decimal] = None
    session_win_target: Optional[Decimal] = None
    updated_at: Optional[datetime] = None