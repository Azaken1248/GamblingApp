from __future__ import annotations

import asyncio
from typing import Any

from celery import Celery

from config.cache_manager import RedisCacheManager
from config.database import Database
from config.settings import load_settings


settings = load_settings()
app = Celery(
    "gambling_app",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
)
app.conf.update(
    accept_content=["json"],
    result_serializer="json",
    task_serializer="json",
    timezone="UTC",
    enable_utc=True,
)

celery_app = app


def _fetch_static_reference_rows() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    database = Database(settings=settings)

    with database.session(dictionary=True) as (_, cursor):
        cursor.execute(
            """
            SELECT
                strategy_id,
                strategy_code,
                strategy_name,
                strategy_type,
                is_progressive,
                is_active,
                created_at
            FROM BETTING_STRATEGIES
            ORDER BY strategy_id
            """
        )
        strategies = list(cursor.fetchall())

        cursor.execute(
            """
            SELECT
                odds_config_id,
                odds_type,
                fixed_multiplier,
                american_odds,
                decimal_odds,
                probability_payout_factor,
                house_edge,
                is_default,
                created_at,
                updated_at
            FROM ODDS_CONFIGURATIONS
            ORDER BY odds_config_id
            """
        )
        odds_configurations = list(cursor.fetchall())

    return strategies, odds_configurations


@app.task(name="tasks.refresh_static_reference_cache")
def refresh_static_reference_cache() -> dict[str, int]:
    strategies, odds_configurations = _fetch_static_reference_rows()

    async def _refresh() -> None:
        cache_manager = RedisCacheManager(settings=settings)
        try:
            await cache_manager.warm_static_reference_cache(
                betting_strategies=strategies,
                odds_configurations=odds_configurations,
            )
        finally:
            await cache_manager.close()

    asyncio.run(_refresh())

    return {
        "betting_strategies": len(strategies),
        "odds_configurations": len(odds_configurations),
    }


@app.task(name="tasks.ping")
def ping() -> str:
    return "pong"


from . import audit_tasks as audit_tasks
from . import report_tasks as report_tasks
from . import simulation_tasks as simulation_tasks