"""Caching.

Two different caches matter and they live at different layers:

* **exact-match response cache** (this module) - same prompt, same answer.
* **prefix / KV cache** (inside the runtime - vLLM, SGLang, Ray Serve) - same system
  prompt, reuse the prefill. The framework does not implement that; it only makes
  routing *cache-affine* so a request lands on the replica that already has it.
"""

from __future__ import annotations

import copy
import time
from collections import OrderedDict
from typing import Optional

from .contracts import Response


class MemoryCache:
    """Bounded LRU with per-entry TTL. Correct for one gateway, not for a fleet."""

    def __init__(self, max_entries: int = 1024, default_ttl_s: float = 300.0) -> None:
        self.max_entries = max_entries
        self.default_ttl_s = default_ttl_s
        self._entries: "OrderedDict[str, tuple[float, Response]]" = OrderedDict()
        self.hits = 0
        self.misses = 0

    async def get(self, key: str) -> Optional[Response]:
        entry = self._entries.get(key)
        if entry is None:
            self.misses += 1
            return None
        expires_at, value = entry
        if expires_at < time.monotonic():
            self._entries.pop(key, None)
            self.misses += 1
            return None
        self._entries.move_to_end(key)
        self.hits += 1
        return copy.deepcopy(value)

    async def set(self, key: str, value: Response, ttl_s: Optional[float] = None) -> None:
        ttl = self.default_ttl_s if ttl_s is None else ttl_s
        self._entries[key] = (time.monotonic() + ttl, copy.deepcopy(value))
        self._entries.move_to_end(key)
        while len(self._entries) > self.max_entries:
            self._entries.popitem(last=False)

    def stats(self) -> dict:
        total = self.hits + self.misses
        return {
            "entries": len(self._entries),
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": (self.hits / total) if total else 0.0,
        }


class NullCache:
    """Explicitly no caching, so the pipeline needs no ``if cache is None`` branches."""

    async def get(self, key: str) -> Optional[Response]:
        return None

    async def set(self, key: str, value: Response, ttl_s: Optional[float] = None) -> None:
        return None

    def stats(self) -> dict:
        return {"mode": "disabled"}
