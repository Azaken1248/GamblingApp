from __future__ import annotations

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
    _GAMBLER_LOCK_PREFIX = "lock:gambler:"

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

    @staticmethod
    def _encode_row(row: Mapping[str, Any]) -> str:
        return json.dumps(dict(row), default=str)

    @staticmethod
    def _decode_row(encoded: str | None) -> dict[str, Any] | None:
        if encoded is None:
            return None
        return json.loads(encoded)