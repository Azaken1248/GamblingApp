from __future__ import annotations

from datetime import datetime, timezone
import json
from contextlib import asynccontextmanager
from typing import Any, Mapping, Sequence

import redis.asyncio as redis
from redis.asyncio.client import Redis
from redis.exceptions import RedisError

from config.settings import Settings


class RedisCacheManager:
    _BETTING_STRATEGIES_KEY = "cache:betting_strategies"
    _ODDS_CONFIGURATIONS_KEY = "cache:odds_configurations"
    _REPORT_BUNDLE_PREFIX = "cache:report:bundle:"
    _REPORT_PROGRESS_PREFIX = "cache:report:progress:"
    _REPORT_SESSION_SUMMARY_PREFIX = "cache:report:session_summary:"
    _REPORT_WIN_LOSS_PREFIX = "cache:report:win_loss:"
    _REPORT_STAKE_HISTORY_PREFIX = "cache:report:stake_history:"
    _REPORT_GAMBLER_STATS_PREFIX = "cache:report:gambler_stats:"
    _GAMBLER_LOCK_PREFIX = "lock:gambler:"
    _TASK_PROGRESS_SUFFIX = ":progress"
    _TASK_RESULT_SUFFIX = ":result"

    def __init__(self, settings: Settings, *, ttl_seconds: int = 3600) -> None:
        self._settings = settings
        self._ttl_seconds = ttl_seconds
        self._redis: Redis[str] = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            decode_responses=True,
        )

    async def close(self) -> None:
        await self._redis.aclose()

    async def cache_betting_strategy(self, strategy_row: Mapping[str, Any]) -> None:
        strategy_code = self._strategy_code(strategy_row)
        await self._redis.hset(
            self._BETTING_STRATEGIES_KEY,
            mapping={strategy_code: self._encode_row(strategy_row)},
        )
        await self._redis.expire(self._BETTING_STRATEGIES_KEY, self._ttl_seconds)

    async def cache_betting_strategies(self, strategy_rows: Sequence[Mapping[str, Any]]) -> None:
        if not strategy_rows:
            return

        payload = {
            self._strategy_code(row): self._encode_row(row)
            for row in strategy_rows
        }
        await self._redis.hset(self._BETTING_STRATEGIES_KEY, mapping=payload)
        await self._redis.expire(self._BETTING_STRATEGIES_KEY, self._ttl_seconds)

    async def get_betting_strategy(self, strategy_code: str) -> dict[str, Any] | None:
        encoded = await self._redis.hget(
            self._BETTING_STRATEGIES_KEY,
            self._normalize_key(strategy_code),
        )
        return self._decode_row(encoded)

    async def cache_odds_configuration(self, odds_row: Mapping[str, Any]) -> None:
        odds_type = self._odds_type(odds_row)
        payload = {odds_type: self._encode_row(odds_row)}
        if bool(odds_row.get("is_default")):
            payload["default"] = self._encode_row(odds_row)

        await self._redis.hset(self._ODDS_CONFIGURATIONS_KEY, mapping=payload)
        await self._redis.expire(self._ODDS_CONFIGURATIONS_KEY, self._ttl_seconds)

    async def cache_odds_configurations(self, odds_rows: Sequence[Mapping[str, Any]]) -> None:
        if not odds_rows:
            return

        payload: dict[str, str] = {}
        for row in odds_rows:
            odds_type = self._odds_type(row)
            encoded = self._encode_row(row)
            payload[odds_type] = encoded
            if bool(row.get("is_default")):
                payload["default"] = encoded

        await self._redis.hset(self._ODDS_CONFIGURATIONS_KEY, mapping=payload)
        await self._redis.expire(self._ODDS_CONFIGURATIONS_KEY, self._ttl_seconds)

    async def get_odds_configuration(self, odds_type: str) -> dict[str, Any] | None:
        encoded = await self._redis.hget(
            self._ODDS_CONFIGURATIONS_KEY,
            self._normalize_key(odds_type),
        )
        return self._decode_row(encoded)

    async def get_default_odds_configuration(self) -> dict[str, Any] | None:
        encoded = await self._redis.hget(self._ODDS_CONFIGURATIONS_KEY, "default")
        return self._decode_row(encoded)

    async def store_report_progress(self, session_id: int, payload: Mapping[str, Any]) -> None:
        await self._redis.set(
            self._report_progress_key(session_id),
            self._encode_row(payload),
            ex=self._ttl_seconds,
        )

    async def get_report_progress(self, session_id: int) -> dict[str, Any] | None:
        encoded = await self._redis.get(self._report_progress_key(session_id))
        return self._decode_row(encoded)

    async def store_session_report_bundle(self, session_id: int, payload: Mapping[str, Any]) -> None:
        payload_dict = dict(payload)

        await self._redis.set(
            self._report_bundle_key(session_id),
            self._encode_row(payload_dict),
            ex=self._ttl_seconds,
        )
        await self._redis.set(
            self._report_session_summary_key(session_id),
            self._encode_row(payload_dict["session_summary"]),
            ex=self._ttl_seconds,
        )
        await self._redis.set(
            self._report_win_loss_key(session_id),
            self._encode_row(payload_dict["win_loss_statistics"]),
            ex=self._ttl_seconds,
        )
        await self._redis.set(
            self._report_stake_history_key(session_id),
            self._encode_row(payload_dict["stake_history_report"]),
            ex=self._ttl_seconds,
        )
        await self._redis.set(
            self._report_gambler_stats_key(session_id),
            self._encode_row(payload_dict["gambler_statistics"]),
            ex=self._ttl_seconds,
        )

    async def get_session_report_bundle(self, session_id: int) -> dict[str, Any] | None:
        encoded = await self._redis.get(self._report_bundle_key(session_id))
        if encoded is not None:
            return self._decode_row(encoded)

        session_summary = await self._redis.get(self._report_session_summary_key(session_id))
        win_loss_statistics = await self._redis.get(self._report_win_loss_key(session_id))
        stake_history_report = await self._redis.get(self._report_stake_history_key(session_id))
        gambler_statistics = await self._redis.get(self._report_gambler_stats_key(session_id))

        if not all((session_summary, win_loss_statistics, stake_history_report, gambler_statistics)):
            return None

        return {
            "session_id": session_id,
            "gambler_id": self._extract_nested_gambler_id(session_summary, win_loss_statistics),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "session_summary": self._decode_row(session_summary),
            "win_loss_statistics": self._decode_row(win_loss_statistics),
            "stake_history_report": self._decode_row(stake_history_report),
            "gambler_statistics": self._decode_row(gambler_statistics),
        }

    async def clear_session_report_state(self, session_id: int) -> None:
        await self._redis.delete(
            self._report_progress_key(session_id),
            self._report_bundle_key(session_id),
            self._report_session_summary_key(session_id),
            self._report_win_loss_key(session_id),
            self._report_stake_history_key(session_id),
            self._report_gambler_stats_key(session_id),
        )

    async def store_task_progress(self, task_id: str, payload: Mapping[str, Any]) -> None:
        await self._redis.set(
            self._task_progress_key(task_id),
            self._encode_row(payload),
            ex=self._ttl_seconds,
        )

    async def get_task_progress(self, task_id: str) -> dict[str, Any] | None:
        encoded = await self._redis.get(self._task_progress_key(task_id))
        return self._decode_row(encoded)

    async def store_task_result(self, task_id: str, payload: Mapping[str, Any]) -> None:
        await self._redis.set(
            self._task_result_key(task_id),
            self._encode_row(payload),
            ex=self._ttl_seconds,
        )

    async def get_task_result(self, task_id: str) -> dict[str, Any] | None:
        encoded = await self._redis.get(self._task_result_key(task_id))
        return self._decode_row(encoded)

    async def clear_task_state(self, task_id: str) -> None:
        await self._redis.delete(
            self._task_progress_key(task_id),
            self._task_result_key(task_id),
        )

    @asynccontextmanager
    async def acquire_gambler_lock(
        self,
        gambler_id: int,
        *,
        timeout: int = 30,
        blocking_timeout: float = 5.0,
    ):
        lock = self._redis.lock(
            name=f"{self._GAMBLER_LOCK_PREFIX}{gambler_id}",
            timeout=timeout,
            blocking_timeout=blocking_timeout,
            thread_local=False,
        )

        acquired = await lock.acquire()
        if not acquired:
            raise TimeoutError(f"Timed out waiting for gambler lock: {gambler_id}")

        try:
            yield
        finally:
            try:
                await lock.release()
            except RedisError:
                pass

    async def warm_static_reference_cache(
        self,
        *,
        betting_strategies: Sequence[Mapping[str, Any]],
        odds_configurations: Sequence[Mapping[str, Any]],
    ) -> None:
        await self.cache_betting_strategies(betting_strategies)
        await self.cache_odds_configurations(odds_configurations)

    @staticmethod
    def _normalize_key(value: str) -> str:
        return value.strip().upper()

    def _strategy_code(self, strategy_row: Mapping[str, Any]) -> str:
        return self._normalize_key(str(strategy_row["strategy_code"]))

    def _odds_type(self, odds_row: Mapping[str, Any]) -> str:
        return self._normalize_key(str(odds_row["odds_type"]))

    def _task_progress_key(self, task_id: str) -> str:
        return f"{task_id}{self._TASK_PROGRESS_SUFFIX}"

    def _task_result_key(self, task_id: str) -> str:
        return f"{task_id}{self._TASK_RESULT_SUFFIX}"

    def _report_bundle_key(self, session_id: int) -> str:
        return f"{self._REPORT_BUNDLE_PREFIX}{session_id}"

    def _report_progress_key(self, session_id: int) -> str:
        return f"{self._REPORT_PROGRESS_PREFIX}{session_id}"

    def _report_session_summary_key(self, session_id: int) -> str:
        return f"{self._REPORT_SESSION_SUMMARY_PREFIX}{session_id}"

    def _report_win_loss_key(self, session_id: int) -> str:
        return f"{self._REPORT_WIN_LOSS_PREFIX}{session_id}"

    def _report_stake_history_key(self, session_id: int) -> str:
        return f"{self._REPORT_STAKE_HISTORY_PREFIX}{session_id}"

    def _report_gambler_stats_key(self, session_id: int) -> str:
        return f"{self._REPORT_GAMBLER_STATS_PREFIX}{session_id}"

    @staticmethod
    def _extract_nested_gambler_id(*payloads: str) -> int:
        for payload in payloads:
            decoded = json.loads(payload)
            gambler_id = decoded.get("gambler_id")
            if gambler_id is not None:
                return int(gambler_id)
        raise KeyError("Unable to infer gambler_id from report cache payloads.")

    @staticmethod
    def _encode_row(row: Mapping[str, Any]) -> str:
        return json.dumps(dict(row), default=str)

    @staticmethod
    def _decode_row(encoded: str | None) -> dict[str, Any] | None:
        if encoded is None:
            return None
        return json.loads(encoded)