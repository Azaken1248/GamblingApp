from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from config.cache_manager import RedisCacheManager
from config.database import Database
from models.session_models import SessionSummary
from services.game_session_manager import GameSessionManager
from services.gambler_profile_service import GamblerProfileService
from services.stake_management_service import StakeManagementService
from services.win_loss_calculator import WinLossCalculator
from tracking_and_reports.report_payloads import SessionReportBundle, session_report_to_payload

from . import app, settings


def _progress_payload(
    *,
    task_id: str,
    session_id: int,
    gambler_id: int,
    phase: str,
    completed_steps: int,
    total_steps: int,
    state: str,
    message: str,
) -> dict[str, Any]:
    percentage = 0.0 if total_steps <= 0 else round((completed_steps / total_steps) * 100.0, 2)
    return {
        "task_id": task_id,
        "session_id": session_id,
        "gambler_id": gambler_id,
        "phase": phase,
        "completed_steps": completed_steps,
        "total_steps": total_steps,
        "percentage": percentage,
        "state": state,
        "message": message,
    }


async def _generate_session_report(
    task: Any,
    *,
    task_id: str,
    session_id: int,
) -> dict[str, Any]:
    cache_manager = RedisCacheManager(settings=settings)
    database = Database(settings=settings)
    session_manager = GameSessionManager(database=database, settings=settings, cache_manager=cache_manager)
    win_loss_calculator = WinLossCalculator(database=database, settings=settings)
    stake_service = StakeManagementService(database=database, settings=settings)
    profile_service = GamblerProfileService(database=database, settings=settings)

    total_steps = 4

    try:
        await cache_manager.clear_session_report_state(session_id)

        await cache_manager.store_report_progress(
            session_id,
            _progress_payload(
                task_id=task_id,
                session_id=session_id,
                gambler_id=0,
                phase="SESSION_SUMMARY",
                completed_steps=0,
                total_steps=total_steps,
                state="STARTED",
                message=f"Loading session {session_id} metadata.",
            ),
        )
        task.update_state(
            state="STARTED",
            meta=_progress_payload(
                task_id=task_id,
                session_id=session_id,
                gambler_id=0,
                phase="SESSION_SUMMARY",
                completed_steps=0,
                total_steps=total_steps,
                state="STARTED",
                message=f"Loading session {session_id} metadata.",
            ),
        )

        session_summary: SessionSummary = await asyncio.to_thread(
            session_manager.get_session_summary,
            session_id,
        )
        gambler_id = session_summary.lifecycle.gambler_id

        await cache_manager.store_report_progress(
            session_id,
            _progress_payload(
                task_id=task_id,
                session_id=session_id,
                gambler_id=gambler_id,
                phase="WIN_LOSS",
                completed_steps=1,
                total_steps=total_steps,
                state="PROGRESS",
                message="Calculating win/loss statistics.",
            ),
        )
        task.update_state(
            state="PROGRESS",
            meta=_progress_payload(
                task_id=task_id,
                session_id=session_id,
                gambler_id=gambler_id,
                phase="WIN_LOSS",
                completed_steps=1,
                total_steps=total_steps,
                state="PROGRESS",
                message="Calculating win/loss statistics.",
            ),
        )
        win_loss_statistics = await asyncio.to_thread(
            win_loss_calculator.get_win_loss_statistics,
            session_id,
        )

        await cache_manager.store_report_progress(
            session_id,
            _progress_payload(
                task_id=task_id,
                session_id=session_id,
                gambler_id=gambler_id,
                phase="STAKE_HISTORY",
                completed_steps=2,
                total_steps=total_steps,
                state="PROGRESS",
                message="Building stake history report.",
            ),
        )
        task.update_state(
            state="PROGRESS",
            meta=_progress_payload(
                task_id=task_id,
                session_id=session_id,
                gambler_id=gambler_id,
                phase="STAKE_HISTORY",
                completed_steps=2,
                total_steps=total_steps,
                state="PROGRESS",
                message="Building stake history report.",
            ),
        )
        stake_history_report = await asyncio.to_thread(
            stake_service.generate_stake_history_report,
            session_id,
        )

        await cache_manager.store_report_progress(
            session_id,
            _progress_payload(
                task_id=task_id,
                session_id=session_id,
                gambler_id=gambler_id,
                phase="GAMBLER_STATS",
                completed_steps=3,
                total_steps=total_steps,
                state="PROGRESS",
                message="Loading gambler profile statistics.",
            ),
        )
        task.update_state(
            state="PROGRESS",
            meta=_progress_payload(
                task_id=task_id,
                session_id=session_id,
                gambler_id=gambler_id,
                phase="GAMBLER_STATS",
                completed_steps=3,
                total_steps=total_steps,
                state="PROGRESS",
                message="Loading gambler profile statistics.",
            ),
        )
        gambler_statistics = await asyncio.to_thread(
            profile_service.retrieve_profile_statistics,
            gambler_id,
        )

        bundle = SessionReportBundle(
            session_id=session_id,
            gambler_id=gambler_id,
            generated_at=datetime.now(timezone.utc),
            session_summary=session_summary,
            win_loss_statistics=win_loss_statistics,
            stake_history_report=stake_history_report,
            gambler_statistics=gambler_statistics,
        )

        await cache_manager.store_session_report_bundle(session_id, session_report_to_payload(bundle))

        final_payload = _progress_payload(
            task_id=task_id,
            session_id=session_id,
            gambler_id=gambler_id,
            phase="COMPLETE",
            completed_steps=total_steps,
            total_steps=total_steps,
            state="SUCCESS",
            message="Session report cached and ready.",
        )
        await cache_manager.store_report_progress(session_id, final_payload)
        task.update_state(state="SUCCESS", meta=final_payload)
        return final_payload
    except Exception as exc:
        failure_payload = {
            "task_id": task_id,
            "session_id": session_id,
            "gambler_id": 0,
            "phase": "FAILURE",
            "completed_steps": 0,
            "total_steps": total_steps,
            "percentage": 0.0,
            "state": "FAILURE",
            "message": str(exc),
            "error": str(exc),
        }
        await cache_manager.store_report_progress(session_id, failure_payload)
        task.update_state(state="FAILURE", meta=failure_payload)
        raise
    finally:
        await cache_manager.close()


@app.task(name="tasks.report_tasks.generate_session_report", bind=True)
def generate_session_report(self, session_id: int) -> dict[str, Any]:
    task_id = self.request.id
    if not task_id:
        raise RuntimeError("Celery did not assign a task id for the report job.")

    return asyncio.run(
        _generate_session_report(
            self,
            task_id=task_id,
            session_id=session_id,
        )
    )