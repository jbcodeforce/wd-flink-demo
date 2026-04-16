"""Thread-safe snapshot of fact rows and derived gauge metrics."""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, MutableMapping

# Top-N programs by total volume in the histogram; remaining names roll into "Other".
_HISTOGRAM_TOP_K = 8


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


def _window_start_to_month_utc(value: object) -> str | None:
    """Map a deserialized window_start to UTC calendar month ``YYYY-MM``."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m")
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1e12:
            ts = ts / 1000.0
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.strftime("%Y-%m")
    if isinstance(value, str):
        s = value.strip()
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt.strftime("%Y-%m")
        except ValueError:
            return None
    return None


def _histogram_by_program_month(
    rows: list[dict[str, Any]],
) -> tuple[list[str], list[dict[str, Any]]]:
    """Sum ``nb_activities`` per (UTC month, program_name); top programs + Other."""
    agg: dict[tuple[str, str], int] = defaultdict(int)
    for r in rows:
        ym = _window_start_to_month_utc(r.get("window_start"))
        if ym is None:
            continue
        pn = r.get("program_name")
        pn = "UNKNOWN" if pn is None else str(pn)
        try:
            n = int(r.get("nb_activities", 0))
        except (TypeError, ValueError):
            n = 0
        agg[(ym, pn)] += n

    if not agg:
        return [], []

    months_sorted = sorted({k[0] for k in agg})
    prog_totals: dict[str, int] = defaultdict(int)
    for (ym, pn), v in agg.items():
        prog_totals[pn] += v

    ranked = sorted(prog_totals.keys(), key=lambda p: (-prog_totals[p], p))
    top_progs = ranked[:_HISTOGRAM_TOP_K]
    other_progs = set(prog_totals.keys()) - set(top_progs)

    series: list[dict[str, Any]] = []
    for prog in top_progs:
        values = [agg.get((ym, prog), 0) for ym in months_sorted]
        series.append({"program_name": prog, "values": values})

    if other_progs:
        values = [sum(agg.get((ym, pn), 0) for pn in other_progs) for ym in months_sorted]
        series.append({"program_name": "Other", "values": values})

    return months_sorted, series


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

        hist_months, hist_series = _histogram_by_program_month(rows)

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
            "histogram_months": hist_months,
            "histogram_series": hist_series,
        }
