"""Thread-safe snapshot of fact rows and derived gauge metrics."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Mapping, MutableMapping


def _stable_row_key(key: object | None, value: Mapping[str, Any] | None) -> str | None:
    if value and value.get("program_name") is not None:
        ws = value.get("window_start")
        pn = value.get("program_name")
        return f"{ws!s}|{pn!s}"
    if isinstance(key, Mapping):
        parts = [f"{k}={key[k]!s}" for k in sorted(key)]
        return "|".join(parts) if parts else None
    if key is not None:
        return str(key)
    return None


@dataclass
class MetricsStore:
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _rows: MutableMapping[str, dict[str, Any]] = field(default_factory=dict, repr=False)
    messages_total: int = 0
    errors_total: int = 0
    last_error: str | None = None
    last_message_ts: float | None = None
    consumer_running: bool = False

    def apply_message(self, key: object | None, value: Mapping[str, Any] | None) -> None:
        rk = _stable_row_key(key, value)
        with self._lock:
            self.messages_total += 1
            self.last_message_ts = time.time()
            if value is None:
                if rk is not None and rk in self._rows:
                    del self._rows[rk]
                return
            if rk is None:
                rk = f"__anon_{self.messages_total}"
            self._rows[rk] = dict(value)

    def record_error(self, message: str) -> None:
        with self._lock:
            self.errors_total += 1
            self.last_error = message

    def set_consumer_running(self, running: bool) -> None:
        with self._lock:
            self.consumer_running = running

    def snapshot_metrics(self) -> dict[str, Any]:
        with self._lock:
            rows = list(self._rows.values())
            mt = self.messages_total
            et = self.errors_total
            le = self.last_error
            lm = self.last_message_ts
            cr = self.consumer_running

        programs: set[str] = set()
        nb_list: list[int] = []
        top_name: str | None = None
        top_nb = 0

        for r in rows:
            pn = r.get("program_name")
            if pn is not None:
                programs.add(str(pn))
            try:
                n = int(r.get("nb_activities", 0))
            except (TypeError, ValueError):
                n = 0
            nb_list.append(n)
            if n >= top_nb:
                top_nb = n
                top_name = str(pn) if pn is not None else None

        return {
            "consumer_running": cr,
            "messages_total": mt,
            "errors_total": et,
            "last_error": le,
            "last_message_ts": lm,
            "last_message_ts_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(lm)) if lm else None,
            "row_count": len(rows),
            "distinct_programs": len(programs),
            "sum_nb_activities": sum(nb_list),
            "max_nb_activities": max(nb_list) if nb_list else 0,
            "top_program_name": top_name,
            "top_program_nb": top_nb,
        }
