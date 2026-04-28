from __future__ import annotations

import asyncio
from typing import Any, Mapping

from config.cache_manager import RedisCacheManager
from config.database import Database
from models.session_models import session_summary_to_payload
from services.game_session_manager import GameSessionManager

from . import app, settings


async def _store_progress(
    cache_manager: RedisCacheManager,
    *,
    task_id: str,
    session_id: int,
    gambler_id: int,
    requested_games: int,
    completed_games: int,
    state: str,
    message: str,
) -> None:
    percentage = 0.0 if requested_games <= 0 else round((completed_games / requested_games) * 100.0, 2)
    await cache_manager.store_task_progress(
        task_id,
        {
            "task_id": task_id,
            "session_id": session_id,
            "gambler_id": gambler_id,
            "requested_games": requested_games,
            "completed_games": completed_games,
            "percentage": percentage,
            "state": state,
            "message": message,
        },
    )


async def _run_session_simulation(
    task: Any,
    *,
    task_id: str,
    simulation_payload: Mapping[str, Any],
) -> dict[str, Any]:
    cache_manager = RedisCacheManager(settings=settings)
    database = Database(settings=settings)

    session_id = int(simulation_payload["session_id"])
    gambler_id = int(simulation_payload["gambler_id"])
    total_games = int(simulation_payload["total_games"])
    strategy_code = str(simulation_payload.get("strategy_code", "MANUAL"))
    bet_amount = simulation_payload.get("bet_amount")
    win_probability = simulation_payload.get("win_probability")
    payout_multiplier = simulation_payload.get("payout_multiplier", "1.00")
    fixed_amount = simulation_payload.get("fixed_amount")
    percentage = simulation_payload.get("percentage")
    base_amount = simulation_payload.get("base_amount")

    manager = GameSessionManager(
        database=database,
        settings=settings,
        cache_manager=cache_manager,
    )

    await cache_manager.clear_task_state(task_id)
    start_payload = {
        "task_id": task_id,
        "session_id": session_id,
        "gambler_id": gambler_id,
        "requested_games": total_games,
        "completed_games": 0,
        "percentage": 0.0,
        "state": "STARTED",
        "message": f"Auto-play simulation started for session {session_id}.",
    }
    await cache_manager.store_task_progress(task_id, start_payload)
    task.update_state(state="STARTED", meta=start_payload)

    async def progress_callback(completed_games: int, requested_games: int, message: str) -> None:
        await _store_progress(
            cache_manager,
            task_id=task_id,
            session_id=session_id,
            gambler_id=gambler_id,
            requested_games=requested_games,
            completed_games=completed_games,
            state="PROGRESS",
            message=message,
        )
        task.update_state(
            state="PROGRESS",
            meta={
                "task_id": task_id,
                "session_id": session_id,
                "gambler_id": gambler_id,
                "requested_games": requested_games,
                "completed_games": completed_games,
                "percentage": 0.0 if requested_games <= 0 else round((completed_games / requested_games) * 100.0, 2),
                "state": "PROGRESS",
                "message": message,
            },
        )

    try:
        result = await manager.execute_continued_session(
            session_id=session_id,
            total_games=total_games,
            strategy_code=strategy_code,
            bet_amount=bet_amount,
            win_probability=win_probability,
            payout_multiplier=payout_multiplier,
            fixed_amount=fixed_amount,
            percentage=percentage,
            base_amount=base_amount,
            progress_callback=progress_callback,
        )

        final_payload = {
            "task_id": task_id,
            "session_id": result.session_id,
            "gambler_id": result.gambler_id,
            "requested_games": result.requested_games,
            "executed_games": result.executed_games,
            "state": "SUCCESS",
            "message": "Auto-play simulation completed.",
            "summary": session_summary_to_payload(result.summary),
        }
        await cache_manager.store_task_result(task_id, final_payload)
        await cache_manager.store_task_progress(
            task_id,
            {
                "task_id": task_id,
                "session_id": result.session_id,
                "gambler_id": result.gambler_id,
                "requested_games": result.requested_games,
                "completed_games": result.executed_games,
                "percentage": 0.0 if result.requested_games <= 0 else round((result.executed_games / result.requested_games) * 100.0, 2),
                "state": "SUCCESS",
                "message": "Auto-play simulation completed.",
            },
        )
        task.update_state(state="SUCCESS", meta=final_payload)
        return final_payload
    except Exception as exc:
        failure_payload = {
            "task_id": task_id,
            "session_id": session_id,
            "gambler_id": gambler_id,
            "requested_games": total_games,
            "completed_games": 0,
            "percentage": 0.0,
            "state": "FAILURE",
            "message": str(exc),
            "error": str(exc),
        }
        await cache_manager.store_task_result(task_id, failure_payload)
        await cache_manager.store_task_progress(task_id, failure_payload)
        task.update_state(state="FAILURE", meta=failure_payload)
        raise


@app.task(name="tasks.simulation_tasks.run_session_simulation", bind=True)
def run_session_simulation(self, simulation_payload: Mapping[str, Any]) -> dict[str, Any]:
    task_id = self.request.id or str(simulation_payload.get("task_id") or "")
    if not task_id:
        raise RuntimeError("Celery did not assign a task id for the simulation job.")

    return asyncio.run(
        _run_session_simulation(
            self,
            task_id=task_id,
            simulation_payload=simulation_payload,
        )
    )