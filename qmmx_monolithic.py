#!/usr/bin/env python3
# QMMX Monolithic (v6.1) — Engine + GUI + SQLite + Polygon + Retraining + Chart
import os, sqlite3, time, threading, json, math, requests, queue
from datetime import datetime, timezone
import tkinter as tk
from tkinter import ttk, messagebox
from tkinter import scrolledtext
import random
from statistics import mean, median, pstdev  # if not already there
from collections import defaultdict, deque

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.pyplot as plt
from typing import List, Dict, Optional, Tuple, Any
from q_voice import QVoice

CHART_WORKER_TAG = "CHART_DATA"

# --- tiny helpers used by OnlinePolicy ---
def _sigmoid(x: float) -> float:
    if x < -50:
        return 0.0
    if x > 50:
        return 1.0
    import math as _m
    return 1.0 / (1.0 + _m.exp(-x))

def _one_hot(val: str, choices: List[str]) -> List[float]:
    return [1.0 if val == c else 0.0 for c in choices]

# --- diagnostic monitor wired to audit() + UI log ---
_diag_sink = None
def set_diagnostic_sink(fn):
    global _diag_sink
    _diag_sink = fn

class _Diag:
    def ping(self, component: str) -> None:
        if _diag_sink:
            _diag_sink("DIAG", "PING", f"{component} ok", {})

    def report_error(self, component: str, message: str, extra: Optional[Dict]=None) -> None:
        if _diag_sink:
            _diag_sink("DIAG", "ERROR", f"{component}: {message}", extra or {})

diagnostic_monitor = _Diag()

# Optional ML
try:
    import joblib
    from sklearn.linear_model import LogisticRegression
    SKLEARN_OK = True
except Exception:
    SKLEARN_OK = False

APP_NAME = "QMMX Monolithic v6.1"
DB_PATH = "qmmx.db"

# ============================
# Database & Persistence Layer
# ============================

def db_connect():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def db_init(conn):
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS settings(
        k TEXT PRIMARY KEY,
        v TEXT NOT NULL
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS price_levels(
        id INTEGER PRIMARY KEY,
        color TEXT NOT NULL,
        level_type TEXT NOT NULL,
        level_index INTEGER NOT NULL,
        price REAL NOT NULL
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS audit_log(
        id INTEGER PRIMARY KEY,
        ts TEXT NOT NULL,
        phase TEXT NOT NULL,
        code TEXT NOT NULL,
        message TEXT NOT NULL,
        extras_json TEXT
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS trades(
        id INTEGER PRIMARY KEY,
        ts_open TEXT,
        ts_close TEXT,
        symbol TEXT,
        side TEXT,
        entry REAL,
        exit REAL,
        stop REAL,
        target REAL,
        reason_open TEXT,
        reason_close TEXT,
        pnl REAL
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS contact_events(
        id INTEGER PRIMARY KEY,
        ts TEXT NOT NULL,
        symbol TEXT NOT NULL,
        level_color TEXT NOT NULL,
        level_type TEXT NOT NULL,
        level_index INTEGER NOT NULL,
        level_price REAL NOT NULL,
        approach TEXT,
        reaction TEXT,
        distance REAL
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS policy_events (
        id INTEGER PRIMARY KEY,
        ts TEXT NOT NULL,
        phase TEXT NOT NULL,                 -- 'entry' or 'exit'
        action TEXT NOT NULL,                -- e.g., go_long/go_short/skip or exit_now/hold
        features_json TEXT NOT NULL,         -- JSON payload (features/context)
        label INTEGER,                       -- 1/0 assigned on trade close
        trade_id INTEGER,                    -- FK to trades.id
        notes TEXT
    );
    """)
    conn.commit()

def settings_get(conn, key, default=None):
    cur = conn.cursor()
    cur.execute("SELECT v FROM settings WHERE k=?", (key,))
    row = cur.fetchone()
    return row[0] if row else default

def settings_set(conn, key, value):
    cur = conn.cursor()
    cur.execute("INSERT INTO settings(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v;", (key, value))
    conn.commit()

def load_levels(conn):
    cur = conn.cursor()
    cur.execute("SELECT color, level_type, level_index, price FROM price_levels ORDER BY color, level_type, level_index;")
    rows = cur.fetchall()
    return [{"color": c, "type": t, "index": i, "price": float(p)} for (c,t,i,p) in rows]

def replace_levels(conn, levels):
    cur = conn.cursor()
    cur.execute("DELETE FROM price_levels;")
    cur.executemany("INSERT INTO price_levels(color, level_type, level_index, price) VALUES(?,?,?,?)",
                    [(lv['color'], lv['type'], lv['index'], float(lv['price'])) for lv in levels])
    conn.commit()

def audit(conn, phase, code, message, extras=None):
    cur = conn.cursor()
    cur.execute("INSERT INTO audit_log(ts, phase, code, message, extras_json) VALUES(?,?,?,?,?)",
                (utcnow(), phase, code, message, json.dumps(extras or {})))
    conn.commit()

def utcnow():
    return datetime.now(timezone.utc).isoformat()

# ============================
# Price Feed (Polygon.io)
# ============================

class MarketStatus:
    def __init__(self, is_open: bool, session: str):
        self.is_open = is_open
        self.session = session  # "open", "closed", "extended-hours", etc.

class PriceFeed:
    def __init__(self, symbol):
        self.symbol = symbol
        self.session = requests.Session()

    def get_market_status(self, api_key) -> MarketStatus:
        try:
            r = self.session.get("https://api.polygon.io/v1/marketstatus/now", params={"apiKey": api_key}, timeout=6)
            if r.status_code != 200:
                return MarketStatus(False, "unknown")
            j = r.json()
            market = j.get("market","closed")
            is_open = (market == "open") 
            return MarketStatus(is_open, market)
        except Exception:
            return MarketStatus(False, "unknown")

    def get_prev_close(self, api_key):
        url = f"https://api.polygon.io/v2/aggs/ticker/{self.symbol.upper()}/prev"
        try:
            r = self.session.get(url, params={"apiKey": api_key, "adjusted": "true"}, timeout=6)
            if r.status_code != 200:
                return None, f"HTTP {r.status_code}: {r.text[:120]}"
            j = r.json()
            results = j.get("results") or []
            if not results:
                return None, "No prev results"
            c = results[0].get("c")
            return float(c) if c is not None else None, None
        except Exception as e:
            return None, str(e)

    def get_last_trade(self, api_key):
        url = f"https://api.polygon.io/v2/last/trade/{self.symbol.upper()}"
        try:
            r = self.session.get(url, params={"apiKey": api_key}, timeout=6)
            if r.status_code != 200:
                return None, None, f"HTTP {r.status_code}: {r.text[:120]}"
            data = r.json()
            res = data.get("results") or {}
            price = res.get("p")
            t_ns = res.get("t")
            if price is None or t_ns is None:
                return None, None, "Malformed results"
            t_ms = int(t_ns // 1_000_000)
            return float(price), t_ms, None
        except Exception as e:
            return None, None, str(e)

    def get_minute_bars(self, api_key, minutes=60):
        """Fetch recent 1-minute bars over the last 24h window, then keep the latest 'minutes'."""
        import datetime as _dt
        end = int(_dt.datetime.now(_dt.timezone.utc).timestamp()) * 1000
        start = end - 24 * 60 * 60 * 1000  # last 24 hours
        url = f"https://api.polygon.io/v2/aggs/ticker/{self.symbol.upper()}/range/1/minute/{start}/{end}"
        try:
            r = self.session.get(url, params={"apiKey": api_key, "adjusted": "true", "sort": "asc", "limit": 5000}, timeout=10)
            if r.status_code != 200:
                return [], f"HTTP {r.status_code}: {r.text[:120]}"
            j = r.json()
            results = j.get("results") or []
            if not results:
                return [], "No minute bars returned"
            bars = [{"t": b.get("t"), "o": b.get("o"), "h": b.get("h"), "l": b.get("l"), "c": b.get("c")} for b in results if all(k in b for k in ("t","o","h","l","c"))]
            # keep the latest 'minutes' bars
            if len(bars) > minutes:
                bars = bars[-minutes:]
            return bars, None
        except Exception as e:
            return [], str(e)

# ============================
# Engine & Decision Logic
# ============================

# Reason codes
NOLEVELS = "NOLEVELS"
MISSING_API_KEY = "MISSING_API_KEY"
PRICE_STALE = "PRICE_STALE"
TOO_FAR = "TOO_FAR"
COOLDOWN = "COOLDOWN"
CONF_LOW = "CONF_LOW"
IN_POSITION = "IN_POSITION"
LEVEL_OVERTOUCHED = "LEVEL_OVERTOUCHED"
DIR_UNKNOWN = "DIR_UNKNOWN"
RISK_INVALID = "RISK_INVALID"
OK = "OK"

class EngineState:
    def __init__(self):
        self.last_price = None
        self.last_ts_ms = None
        self.cooldown_until_ms = 0
        self.open_trade_id = None
        self.level_touch_counts = {}
        self.last_direction = None
    def in_cooldown(self, now_ms):
        return now_ms < self.cooldown_until_ms
    def set_cooldown(self, now_ms, seconds):
        self.cooldown_until_ms = now_ms + (seconds * 1000)

# ==== Integrated planners & policy (insert full class code here) ====
# 1) OnlinePolicy  (from your file)
class OnlinePolicy:
    """
    Online logistic model that learns the probability an action will be profitable.
    We keep TWO heads (entry_head, exit_head) so we can learn biases independently.

      p(action_is_good | features) = sigmoid(w • x)

    Actions we score:
      - Entry head: "go_long", "go_short", "skip"
      - Exit head:  "exit_now", "hold"

    Labels:
      - For entries: label=1 if the trade produced positive pnl at close, else 0
      - For exits:   label=1 if exiting when model said 'exit_now' yielded better pnl
                     than holding for the next K bars (measured ex-post), else 0
    """

    def __init__(self, lr: float = 0.03, l2: float = 1e-6, use_perceptron: bool = False):
        self.lr = lr
        self.l2 = l2
        self.use_perceptron = use_perceptron
        # weight vectors by action
        self.w_entry = {
            "go_long":  [],
            "go_short": [],
            "skip":     []
        }
        self.w_exit = {
            "exit_now": [],
            "hold":     []
        }
        self._dim = None

    # -------- Feature builder (compact, consistent with your rules) --------
    def build_features(
        self,
        *,
        proximity_abs: float,          # |price - level|
        volume_trend: float,           # avg(last half vols toward level) - avg(first half)
        approach: str,                 # "from_above" | "from_below"
        confluence: bool,
        minutes_since_open: int
    ) -> List[float]:
        # Normalize/clip
        prox = min(1.0, proximity_abs)             # within $1 bucketed
        vt   = max(-1.0, min(1.0, volume_trend / 1e6))  # scale if you use raw share counts
        ao   = _one_hot(approach, ["from_above", "from_below"])
        cf   = [1.0 if confluence else 0.0]
        tod  = [min(1.0, minutes_since_open / 390.0)]
        x = [1.0, prox, vt] + ao + cf + tod  # bias + features
        if self._dim is None:
            self._dim = len(x)
            # init weights small
            for d in self.w_entry:
                self.w_entry[d] = [0.0] * self._dim
            for d in self.w_exit:
                self.w_exit[d] = [0.0] * self._dim
        return x

    # -------- Core math --------
    def _dot(self, w: List[float], x: List[float]) -> float:
        return sum(wi * xi for wi, xi in zip(w, x))

    def _sgd_update(self, w: List[float], x: List[float], y: int, pred: float) -> None:
        # Logistic regression SGD with L2
        grad = [(pred - y) * xi + self.l2 * wi for wi, xi in zip(w, x)]
        for i in range(len(w)):
            w[i] -= self.lr * grad[i]

    def _perc_update(self, w: List[float], x: List[float], y: int, pred_bin: int) -> None:
        # Simple perceptron
        err = y - pred_bin
        for i in range(len(w)):
            w[i] += self.lr * err * x[i]

    # -------- Public API: Entry head --------
    def score_entry(self, x: List[float]) -> Dict[str, float]:
        scores = {}
        for a, w in self.w_entry.items():
            z = self._dot(w, x)
            scores[a] = _sigmoid(z)
        return scores

    def update_entry(self, x: List[float], action: str, label: int) -> None:
        w = self.w_entry[action]
        pred = _sigmoid(self._dot(w, x))
        if self.use_perceptron:
            self._perc_update(w, x, label, 1 if pred >= 0.5 else 0)
        else:
            self._sgd_update(w, x, label, pred)

    # -------- Public API: Exit head --------
    def score_exit(self, x: List[float]) -> Dict[str, float]:
        scores = {}
        for a, w in self.w_exit.items():
            z = self._dot(w, x)
            scores[a] = _sigmoid(z)
        return scores

    def update_exit(self, x: List[float], action: str, label: int) -> None:
        w = self.w_exit[action]
        pred = _sigmoid(self._dot(w, x))
        if self.use_perceptron:
            self._perc_update(w, x, label, 1 if pred >= 0.5 else 0)
        else:
            self._sgd_update(w, x, label, pred)

# 2) SmartEntryPlanner  (from your file)
class SmartEntryPlanner:
    """
    Stock-only entry planner that decides LONG/SHORT entries around user-defined price levels
    using volume behavior and approach context.

    Core rules:
      - Decreasing volume into a level → higher probability of REVERSAL at/near the level.
      - Increasing volume into a level → higher probability of PENETRATION/CONTINUATION to the next level.
      - Confluence handling: If two (or more) levels sit close together and price pierces the first,
        slightly pierces the second, then snaps back, favor a REVERSAL.
    """

    def __init__(
        self,
        proximity_window: float = 0.35,          # how close to consider "at the level"
        confluence_window: float = 0.6,          # distance within which levels form a "cluster"
        slight_pierce_fraction: float = 0.12,    # fraction of proximity_window that counts as "slight"
        vol_lookback: int = 5,                   # bars to assess volume trend into the level
        min_bars_for_trend: int = 3,             # minimum bars required to form a trend
        min_retrace_ticks: float = 0.08,         # minimal snap-back size to confirm reversal after pierce
        entry_slippage: float = 0.03,            # offset to avoid chasing (enter a touch past the level)
        freshness_seconds: int = 180,            # pattern freshness window (3 minutes)
    ):
        self.proximity_window = proximity_window
        self.confluence_window = confluence_window
        self.slight_pierce_window = max(slight_pierce_fraction * proximity_window, 1e-6)
        self.vol_lookback = vol_lookback
        self.min_bars_for_trend = min_bars_for_trend
        self.min_retrace_ticks = min_retrace_ticks
        self.entry_slippage = entry_slippage
        self.freshness_seconds = freshness_seconds

    # ---------------------------
    # Public API
    # ---------------------------
    def should_enter(
        self,
        *,
        symbol: str,
        current_price: float,
        current_volume: float,
        current_time: float,  # epoch seconds
        levels: List[Dict],   # [{"price": float, "type": "solid|dashed", "color": str}, ...]
        price_history: List[Tuple[float, float, float]],  # [(price, volume, epoch_seconds), ...] oldest→newest
        pattern: Dict,        # expects: {"timestamp": float, "level": float, "approach_direction": "from_above|from_below"}
    ) -> Optional[Dict]:
        """
        Returns an entry signal dict or None. Signal fields:
          {
            "symbol": str,
            "timestamp": float,
            "side": "long"|"short",
            "basis": "reversal"|"continuation",
            "level_price": float,
            "entry_price": float,
            "stop_hint": float,
            "target_hint": Optional[float],
            "reason": str,
            "confluence": Optional[Dict]
          }
        """
        try:
            # 1) Freshness guard
            if not self._is_fresh(current_time, pattern.get("timestamp")):
                diagnostic_monitor.report_error("entry_planner", "Pattern too old for entry")
                return None

            base_level = pattern.get("level")
            if base_level is None or not levels:
                diagnostic_monitor.report_error("entry_planner", "Missing levels or base level")
                return None

            # 2) Must be approaching a level
            nearest_level = self._nearest_level(current_price, levels)
            if not nearest_level:
                diagnostic_monitor.report_error("entry_planner", "No nearby level")
                return None

            level_price = nearest_level["price"]
            if not self._within_proximity(current_price, level_price):
                diagnostic_monitor.report_error("entry_planner", "Not within proximity window")
                return None

            # 3) Establish approach direction from recent price action if not provided
            approach = pattern.get("approach_direction") or self._infer_approach(price_history, level_price)
            if approach not in ("from_above", "from_below"):
                diagnostic_monitor.report_error("entry_planner", "Unknown approach direction")
                return None

            # 4) Volume trend into the level
            vol_trend = self._volume_trend_toward_level(price_history, level_price, approach)
            if vol_trend is None:
                diagnostic_monitor.report_error("entry_planner", "Insufficient data for volume trend")
                return None

            # 5) Detect confluence cluster (if any)
            cluster = self._confluence_cluster(levels, level_price)
            confluence_info = cluster if len(cluster) > 1 else None

            # 6) Decide basis: reversal vs continuation
            #    - decreasing volume into level → reversal
            #    - increasing volume into level → continuation
            if vol_trend < 0:
                # Reversal logic
                side = "long" if approach == "from_above" else "short"
                basis = "reversal"

                # If cluster exists, prefer reversal confirmation pattern:
                #    pierce first level, slight pierce of second, then snap back ≥ min_retrace_ticks.
                if confluence_info and self._has_reverse_after_slight_second_pierce(price_history, cluster, approach):
                    reason = "Confluence snap-back reversal after slight second-level pierce"
                else:
                    reason = "Decreasing volume into level favors reversal"

                entry_price, stop_hint = self._reversal_prices(level_price, approach)
                target_hint = self._next_level_target(levels, level_price, side)

            else:
                # Continuation logic
                side = "short" if approach == "from_above" else "long"
                basis = "continuation"

                # Continuation into cluster: expect penetration of first and drive to next/second level
                if confluence_info:
                    reason = "Increasing volume into confluence favors penetration toward next level"
                else:
                    reason = "Increasing volume into level favors penetration/continuation"

                entry_price, stop_hint = self._continuation_prices(level_price, approach)
                target_hint = self._next_level_target(levels, level_price, side)

            signal = {
                "symbol": symbol,
                "timestamp": current_time,
                "side": side,
                "basis": basis,
                "level_price": float(level_price),
                "entry_price": float(entry_price),
                "stop_hint": float(stop_hint),
                "target_hint": float(target_hint) if target_hint is not None else None,
                "reason": reason,
                "confluence": confluence_info,
            }

            diagnostic_monitor.ping("entry_planner")
            return signal

        except Exception as e:
            diagnostic_monitor.report_error("entry_planner", f"Planner failed: {e}")
            return None

    # ---------------------------
    # Helpers
    # ---------------------------
    def _is_fresh(self, now_ts: float, pattern_ts: Optional[float]) -> bool:
        if pattern_ts is None:
            return False
        return (now_ts - pattern_ts) <= self.freshness_seconds

    def _within_proximity(self, price: float, level: float) -> bool:
        return abs(price - level) <= self.proximity_window

    def _nearest_level(self, price: float, levels: List[Dict]) -> Optional[Dict]:
        closest = None
        best = float("inf")
        for lv in levels:
            d = abs(price - float(lv["price"]))
            if d < best:
                best = d
                closest = lv
        return closest

    def _infer_approach(self, price_history: List[Tuple[float, float, float]], level: float) -> Optional[str]:
        """
        Basic approach inference: compare last two prices to the level.
        """
        if len(price_history) < 2:
            return None
        p1, _, _ = price_history[-2]
        p2, _, _ = price_history[-1]
        # If we moved down toward the level → from_above; moved up toward the level → from_below
        if abs(p2 - level) < abs(p1 - level):
            return "from_above" if p1 > level else "from_below"
        return None

    def _volume_trend_toward_level(
        self,
        price_history: List[Tuple[float, float, float]],
        level: float,
        approach: str
    ) -> Optional[float]:
        """
        Returns a signed trend (rough slope) of volume for the last N bars as price moved toward the level.
          < 0 → decreasing volume into the level (reversal bias)
          > 0 → increasing volume into the level (continuation bias)
        """
        if len(price_history) < max(self.vol_lookback, self.min_bars_for_trend):
            return None

        # Take last vol_lookback bars that *reduced* distance to the level.
        seq = price_history[-self.vol_lookback:]
        filtered: List[float] = []
        prev_dist = None
        for (p, v, _) in seq:
            d = abs(p - level)
            if prev_dist is None or d <= prev_dist:
                filtered.append(v)
            prev_dist = d

        if len(filtered) < self.min_bars_for_trend:
            # If not enough "toward level" bars, just use last N volumes
            filtered = [v for _, v, _ in seq]

        # Simple slope: avg(last half) - avg(first half)
        k = max(2, len(filtered) // 2)
        first = filtered[:k]
        last = filtered[-k:]
        first_avg = sum(first) / len(first)
        last_avg = sum(last) / len(last)
        return last_avg - first_avg

    def _confluence_cluster(self, levels: List[Dict], anchor_price: float) -> List[float]:
        """
        Returns a sorted list of level prices forming a confluence around anchor within confluence_window.
        """
        cluster = []
        for lv in levels:
            p = float(lv["price"])
            if abs(p - anchor_price) <= self.confluence_window:
                cluster.append(p)
        return sorted(set(cluster))

    def _has_reverse_after_slight_second_pierce(
        self,
        price_history: List[Tuple[float, float, float]],
        cluster: List[float],
        approach: str
    ) -> bool:
        """
        Confluence pattern:
          - Price pierces the first (nearest) level,
          - Slightly pierces the second,
          - Then snaps back across the second by at least min_retrace_ticks.
        """
        if len(cluster) < 2 or len(price_history) < 3:
            return False

        # Identify first and second in approach direction
        if approach == "from_above":
            first, second = max(cluster), sorted(cluster)[-2]  # going down: hit higher first, then lower
        else:
            first, second = min(cluster), sorted(cluster)[1]   # going up: hit lower first, then higher

        prices = [p for (p, _, _) in price_history[-8:]]

        def _pierced(level: float) -> bool:
            return any(abs(p - level) <= self.proximity_window for p in prices)

        def _slight_pierce(level: float) -> bool:
            return any(self.proximity_window < abs(p - level) <= (self.proximity_window + self.slight_pierce_window) for p in prices)

        if not _pierced(first):
            return False
        if not _slight_pierce(second):
            return False

        # Snap-back check: last price moves back across the second by min_retrace_ticks
        last_price = prices[-1]
        if approach == "from_above":
            # Down into levels, reversal implies last_price > second + min_retrace_ticks
            return last_price >= (second + self.min_retrace_ticks)
        else:
            # Up into levels, reversal implies last_price < second - min_retrace_ticks
            return last_price <= (second - self.min_retrace_ticks)

    def _reversal_prices(self, level: float, approach: str) -> Tuple[float, float]:
        """
        Suggest entry just beyond the level in the reversal direction, with a stop just past the opposite side.
        """
        if approach == "from_above":
            # Coming down → reversal is LONG
            entry = level + self.entry_slippage
            stop = level - (self.proximity_window + self.slight_pierce_window)
        else:
            # Coming up → reversal is SHORT
            entry = level - self.entry_slippage
            stop = level + (self.proximity_window + self.slight_pierce_window)
        return (round(entry, 2), round(stop, 2))

    def _continuation_prices(self, level: float, approach: str) -> Tuple[float, float]:
        """
        Suggest entry on the penetration side, with stop on the far side of the level.
        """
        if approach == "from_above":
            # Coming down → continuation is SHORT
            entry = level - self.entry_slippage
            stop = level + (self.proximity_window)
        else:
            # Coming up → continuation is LONG
            entry = level + self.entry_slippage
            stop = level - (self.proximity_window)
        return (round(entry, 2), round(stop, 2))

    def _next_level_target(
        self,
        levels: List[Dict],
        reference_level: float,
        side: str
    ) -> Optional[float]:
        """
        For continuation or post-reversal move, hint the next level in the trade direction.
        """
        prices = sorted([float(lv["price"]) for lv in levels])
        if side == "long":
            higher = [p for p in prices if p > reference_level]
            return round(higher[0], 2) if higher else None
        else:
            lower = [p for p in prices if p < reference_level]
            return round(lower[-1], 2) if lower else None
        
# 3) ExitStrategy  (from your file)
class ExitStrategy:
    def __init__(
        self,
        proximity_window: float = 0.35,          # how close to consider "at the level"
        confluence_window: float = 0.6,          # levels within this distance are a "cluster"
        slight_pierce_fraction: float = 0.12,    # portion of proximity_window that counts as "slight"
        vol_lookback: int = 5,                   # bars to assess volume trend into the level
        min_bars_for_trend: int = 3,             # minimum bars required to form a trend
        min_retrace_ticks: float = 0.08          # minimal snap-back size to confirm reversal after pierce
    ):
        self.proximity_window = proximity_window
        self.confluence_window = confluence_window
        self.slight_pierce_window = max(slight_pierce_fraction * proximity_window, 1e-6)
        self.vol_lookback = vol_lookback
        self.min_bars_for_trend = min_bars_for_trend
        self.min_retrace_ticks = min_retrace_ticks

    # ------------------------------------------------------------------
    # Public APIs
    # ------------------------------------------------------------------
    def evaluate(self, **params) -> Dict[str, Any]:
        """
        Convenience wrapper to support your Flask route:
          /exit_strategy  →  exit_strategy.evaluate(**params)
        Expecting:
          {
            "open_trade": {...},             # required: dict with "direction" and ideally "entry_price"
            "current_price": float,          # required
            "levels": [ { "price": float }, ... ],  # required
            "recent_bars": [ (price, volume, ts), ... ]  # optional, oldest→newest
          }
        """
        try:
            trade = params.get("open_trade") or params.get("trade")
            price = float(params.get("current_price"))
            levels = params.get("levels") or []
            recent_bars = params.get("recent_bars")  # optional

            result = self.should_exit(
                open_trade=trade,
                current_price=price,
                levels=levels,
                recent_bars=recent_bars,
                now_ts=params.get("now_ts")
            )
            diagnostic_monitor.ping("exit_planner")
            return result
        except Exception as e:
            diagnostic_monitor.report_error("exit_planner", f"evaluate() failed: {e}")
            return {"exit": False, "reason": f"exit_planner error: {e}"}

    def should_exit(
        self,
        open_trade: Dict,
        current_price: float,
        levels: List[Dict],
        recent_bars: Optional[List[Tuple[float, float, float]]] = None,
        now_ts: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Main exit decision:
          - Find nearest level and check proximity.
          - Infer approach (from_above / from_below).
          - Compute volume trend as price moves INTO that level.
          - Decreasing volume → reversal bias. Increasing volume → continuation bias.
          - Exit if the likely move goes AGAINST our position.
          - Handle confluence: pierce first, slight pierce second, snap-back → reversal (exit if against position).

        Returns:
          {
            "exit": bool,
            "reason": str,
            "basis": "reversal"|"continuation"|None,
            "level_price": float|None,
            "at_price": float,
            "confluence": dict|None
          }
        """
        try:
            # Basic guards
            if not open_trade or not isinstance(open_trade, dict):
                return {"exit": False, "reason": "No open_trade provided", "basis": None, "level_price": None, "at_price": current_price, "confluence": None}

            if not levels:
                return {"exit": False, "reason": "No levels available", "basis": None, "level_price": None, "at_price": current_price, "confluence": None}

            direction = (open_trade.get("direction") or "").lower()
            if direction not in ("long", "short"):
                return {"exit": False, "reason": "Unknown trade direction", "basis": None, "level_price": None, "at_price": current_price, "confluence": None}

            # 1) Nearest level + proximity
            nearest = self._nearest_level(current_price, levels)
            if not nearest:
                return {"exit": False, "reason": "No nearby level", "basis": None, "level_price": None, "at_price": current_price, "confluence": None}

            level_price = float(nearest["price"])
            if not self._within_proximity(current_price, level_price):
                return {"exit": False, "reason": "Not within proximity window", "basis": None, "level_price": level_price, "at_price": current_price, "confluence": None}

            # 2) Approach direction (from_above / from_below)
            approach = self._infer_approach(recent_bars, level_price)
            if approach not in ("from_above", "from_below"):
                # fallback using just current vs level
                approach = "from_above" if current_price > level_price else "from_below"

            # 3) Volume trend into the level (signed slope)
            vol_trend = self._volume_trend_toward_level(recent_bars, level_price)
            # vol_trend < 0 → decreasing (reversal bias), > 0 → increasing (continuation bias)
            # If no bars/volume available, we cannot use volume logic; fall back to conservative hold.
            if vol_trend is None:
                return {
                    "exit": False,
                    "reason": "Insufficient volume data to assess exit",
                    "basis": None,
                    "level_price": level_price,
                    "at_price": current_price,
                    "confluence": None
                }

            # 4) Confluence detection
            cluster = self._confluence_cluster(levels, level_price)
            confluence_info = {"cluster": cluster} if len(cluster) > 1 else None

            # 5) Decide reversal vs continuation basis
            if vol_trend < 0:
                # Reversal expected at/near level
                basis = "reversal"
                goes_up = (approach == "from_above")   # if approaching from above, reversal bounce is up
                goes_down = (approach == "from_below") # if approaching from below, reversal bounce is down

                # Confluence snap-back strengthens reversal expectation
                if confluence_info and self._has_reverse_after_slight_second_pierce(recent_bars, cluster, approach):
                    reason_core = "Confluence snap-back reversal"
                else:
                    reason_core = "Decreasing volume into level favors reversal"

                # Exit if reversal is AGAINST our position
                if (direction == "long" and goes_down) or (direction == "short" and goes_up):
                    return {
                        "exit": True,
                        "reason": f"{reason_core} against {direction}",
                        "basis": basis,
                        "level_price": level_price,
                        "at_price": current_price,
                        "confluence": confluence_info
                    }
                else:
                    return {
                        "exit": False,
                        "reason": f"{reason_core} but not against {direction}",
                        "basis": basis,
                        "level_price": level_price,
                        "at_price": current_price,
                        "confluence": confluence_info
                    }

            else:
                # Continuation through the level is likely
                basis = "continuation"
                # Continuation direction is toward the level and beyond:
                # - from_above → downside continuation through level
                # - from_below → upside continuation through level
                cont_down = (approach == "from_above")
                cont_up = (approach == "from_below")

                if confluence_info:
                    reason_core = "Increasing volume into confluence favors penetration"
                else:
                    reason_core = "Increasing volume into level favors continuation"

                # Exit if continuation is AGAINST our position
                if (direction == "long" and cont_down) or (direction == "short" and cont_up):
                    return {
                        "exit": True,
                        "reason": f"{reason_core} against {direction}",
                        "basis": basis,
                        "level_price": level_price,
                        "at_price": current_price,
                        "confluence": confluence_info
                    }
                else:
                    return {
                        "exit": False,
                        "reason": f"{reason_core} but not against {direction}",
                        "basis": basis,
                        "level_price": level_price,
                        "at_price": current_price,
                        "confluence": confluence_info
                    }

        except Exception as e:
            diagnostic_monitor.report_error("exit_planner", f"should_exit() failed: {e}")
            return {"exit": False, "reason": f"exit_planner error: {e}", "basis": None, "level_price": None, "at_price": current_price, "confluence": None}

    def should_escalate_on_target(
        self,
        *,
        open_trade: Dict[str, Any],
        current_price: float,
        levels: List[Dict[str, Any]],
        recent_bars: Optional[List[Tuple[float, float, float]]] = None
    ) -> Dict[str, Any]:
        """
        Called exactly when price is at/near the current target.
        If momentum/volume favors continuation in the trade direction,
        propose rolling the target to the next level and trailing the stop.
        Returns:
        {
            "escalate": bool,
            "next_target": float | None,
            "trail_stop": float | None,
            "basis": "continuation" | "reversal" | None,
            "score": float
        }
        """
        try:
            # Reuse the main decision to get 'basis' ("continuation" or "reversal")
            res = self.should_exit(
                open_trade=open_trade,
                current_price=current_price,
                levels=levels,
                recent_bars=recent_bars
            )
            basis = res.get("basis")
            if not res.get("exit") and basis == "continuation":
                # Determine side
                side = open_trade.get("direction") or open_trade.get("side")
                side = "long" if str(side).lower() in ("long", "buy") else "short"

                # Anchor around the level used by should_exit if provided
                anchor_price = float(res.get("level_price") or current_price)

                # Find next level beyond anchor in trade direction
                next_target = self._next_level_target(levels, anchor_price, side)
                if next_target is None:
                    return {"escalate": False, "next_target": None, "trail_stop": None, "basis": basis, "score": 0.0}

                # Trail stop toward breakeven or just beyond the anchor
                entry = float(open_trade.get("entry") or open_trade.get("entry_price") or current_price)
                if side == "long":
                    trail = max(entry, anchor_price - self.proximity_window)
                else:
                    trail = min(entry, anchor_price + self.proximity_window)

                # Crude confidence score: could be replaced with ML later
                score = 0.70
                return {
                    "escalate": True,
                    "next_target": float(next_target),
                    "trail_stop": float(round(trail, 2)),
                    "basis": basis,
                    "score": score
                }

            # Otherwise: do not escalate
            return {"escalate": False, "next_target": None, "trail_stop": None, "basis": basis, "score": 0.0}
        except Exception:
            return {"escalate": False, "next_target": None, "trail_stop": None, "basis": None, "score": 0.0}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _within_proximity(self, price: float, level: float) -> bool:
        return abs(price - level) <= self.proximity_window

    def _nearest_level(self, price: float, levels: List[Dict]) -> Optional[Dict]:
        closest = None
        best = float("inf")
        for lv in levels:
            p = float(lv["price"])
            d = abs(price - p)
            if d < best:
                best = d
                closest = lv
        return closest

    def _infer_approach(self, bars: Optional[List[Tuple[float, float, float]]], level: float) -> Optional[str]:
        """
        Infer approach using last two prices relative to level.
        bars: list of (price, volume, ts) oldest→newest
        """
        if not bars or len(bars) < 2:
            return None
        p1 = bars[-2][0]
        p2 = bars[-1][0]
        # If |p2-level| < |p1-level| → moving toward level
        if abs(p2 - level) < abs(p1 - level):
            return "from_above" if p1 > level else "from_below"
        return None

    def _volume_trend_toward_level(
        self,
        bars: Optional[List[Tuple[float, float, float]]],
        level: float
    ) -> Optional[float]:
        """
        Returns a signed trend of volume over the last N bars as price moved TOWARD the level.
        < 0 → decreasing volume (reversal bias)
        > 0 → increasing volume (continuation bias)
        """
        if not bars or len(bars) < max(self.vol_lookback, self.min_bars_for_trend):
            return None

        seq = bars[-self.vol_lookback:]  # last N
        filtered_vols: List[float] = []
        prev_dist = None
        for (p, v, _) in seq:
            d = abs(p - level)
            if prev_dist is None or d <= prev_dist:
                filtered_vols.append(v)
            prev_dist = d

        if len(filtered_vols) < self.min_bars_for_trend:
            filtered_vols = [v for _, v, _ in seq]  # fallback to raw vols

        # Simple slope proxy: avg(last half) - avg(first half)
        k = max(2, len(filtered_vols) // 2)
        first = filtered_vols[:k]
        last = filtered_vols[-k:]
        first_avg = sum(first) / len(first)
        last_avg = sum(last) / len(last)
        return last_avg - first_avg

    def _confluence_cluster(self, levels: List[Dict], anchor_price: float) -> List[float]:
        """
        Gather levels that lie within the confluence_window around anchor.
        Returns sorted unique prices.
        """
        cluster = []
        for lv in levels:
            p = float(lv["price"])
            if abs(p - anchor_price) <= self.confluence_window:
                cluster.append(p)
        return sorted(set(cluster))
    
    def _next_level_target(self, levels: List[Dict[str, Any]], ref_price: float, side: str) -> Optional[float]:
        """
        Given a list of levels ({'price': ...}) and a reference price near the current target,
        return the next level beyond ref_price in the trade direction.
        """
        prices = sorted(float(lv["price"]) for lv in levels)
        if side == "long":
            higher = [p for p in prices if p > ref_price + 1e-9]
            return min(higher) if higher else None
        else:
            lower = [p for p in prices if p < ref_price - 1e-9]
            return max(lower) if lower else None

    def _has_reverse_after_slight_second_pierce(
        self,
        bars: Optional[List[Tuple[float, float, float]]],
        cluster: List[float],
        approach: str
    ) -> bool:
        """
        Confluence pattern:
          - Price pierces the first (nearest) level,
          - Slightly pierces the second,
          - Then snaps back across the second by at least min_retrace_ticks.
        """
        if not bars or len(cluster) < 2 or len(bars) < 3:
            return False

        prices = [p for (p, _, _) in bars[-8:]]

        if approach == "from_above":
            # moving down: hit higher first, then lower
            first = max(cluster)
            # second is next lower in the cluster
            lower_sorted = sorted(cluster)
            # find the index of first and take previous one if exists
            try:
                i = lower_sorted.index(first)
                if i == 0:
                    return False
                second = lower_sorted[i - 1]
            except ValueError:
                return False
        else:
            # from_below (moving up): hit lower first, then higher
            first = min(cluster)
            higher_sorted = sorted(cluster)
            try:
                i = higher_sorted.index(first)
                if i == len(higher_sorted) - 1:
                    return False
                second = higher_sorted[i + 1]
            except ValueError:
                return False

        def _pierced(level: float) -> bool:
            return any(abs(p - level) <= self.proximity_window for p in prices)

        def _slight_pierce(level: float) -> bool:
            return any(self.proximity_window < abs(p - level) <= (self.proximity_window + self.slight_pierce_window) for p in prices)

        if not _pierced(first):
            return False
        if not _slight_pierce(second):
            return False

        last_price = prices[-1]
        if approach == "from_above":
            # Down into levels; reversal implies last_price >= second + min_retrace_ticks
            return last_price >= (second + self.min_retrace_ticks)
        else:
            # Up into levels; reversal implies last_price <= second - min_retrace_ticks
            return last_price <= (second - self.min_retrace_ticks)

class LevelTouchMemory:
    """
    Tracks touches of your levels inside an accumulation box.

    - Diminishing confidence for repeated touches
    - Per-level/per-side cooldown
    - Bounce budget (max attempts)
    - Edge fatigue detection (many taps toward one edge with rising volume)
    """
    def __init__(self,
                 tol_bps=8,                 # proximity to call a 'touch' (0.08%)
                 min_time_gap_sec=180,      # don't count touches that are too close in time
                 min_price_gap_bps=4,       # and not far enough in price from last touch
                 decay=0.85,                # confidence multiplier per extra touch
                 max_bounces=2,             # allowed trades per level/side inside same box
                 fatigue_hits=3,            # taps toward an edge to call it 'fatigued'
                 fatigue_window_min=30,     # window for counting edge taps
                 fatigue_vol_k=1.20):       # rising short/long volume ratio to flag fatigue
        self.tol_bps = tol_bps
        self.min_time_gap_ms = min_time_gap_sec * 1000
        self.min_price_gap_bps = min_price_gap_bps
        self.decay = decay
        self.max_bounces = max_bounces
        self.fatigue_hits = fatigue_hits
        self.fatigue_window_ms = fatigue_window_min * 60 * 1000
        self.fatigue_vol_k = fatigue_vol_k

        # (level_price_rounded, side) -> dict(count,last_ts,last_price)
        self.touch = defaultdict(lambda: {"count":0,"last_ts":None,"last_px":None})

        # edge fatigue tracker: keep taps near box edges with volume trend
        # store deque of (ts_ms, edge, vol_short_over_long)
        self.edge_taps = deque(maxlen=1000)

    @staticmethod
    def _bps(px, ref):  # basis points distance
        return 0.0 if ref <= 0 else abs(px - ref) / ref * 10000.0

    def _roundL(self, L):
        # reduce floating noise in dict keys
        return round(float(L), 2)

    def reset_box(self):
        self.touch.clear()
        self.edge_taps.clear()

    def register_touch_if_any(self, ts_ms, o, h, l, c, v, levels, price_now, box_low, box_high,
                              vol_ma_s=None, vol_ma_l=None):
        """
        Call on each finished minute bar while you're in 'accumulation'.
        Detects level touches and logs taps near box edges (for fatigue).
        """
        if not levels:
            return

        # detect any level pierced by this bar
        for L in levels:
            Lr = self._roundL(L)
            # bar pierced or close enough at close
            near = (l - 1e-9) <= Lr <= (h + 1e-9) or self._bps(c, Lr) <= self.tol_bps
            if not near:
                continue

            # pick side by approach (if close below level -> SHORT touch; above -> LONG touch)
            side = "SHORT" if c > Lr else "LONG"
            key = (Lr, side)
            rec = self.touch[key]
            # de-duplicate noisy rapid re-hits
            if rec["last_ts"] is not None:
                if ts_ms - rec["last_ts"] < self.min_time_gap_ms:
                    continue
                if rec["last_px"] is not None and self._bps(c, rec["last_px"]) < self.min_price_gap_bps:
                    continue
            rec["count"] += 1
            rec["last_ts"] = ts_ms
            rec["last_px"] = c

        # edge tap logging for fatigue
        if box_low is not None and box_high is not None:
            at_top = h >= (box_high - 1e-9)
            at_bot = l <= (box_low + 1e-9)
            ratio = (vol_ma_s / vol_ma_l) if (vol_ma_s and vol_ma_l and vol_ma_l > 0) else 1.0
            if at_top:
                self.edge_taps.append((ts_ms, "top", ratio))
            if at_bot:
                self.edge_taps.append((ts_ms, "bot", ratio))

    def edge_fatigued(self, now_ms):
        """
        Returns 'top' or 'bot' if that edge is fatigued (many taps recently with rising volume).
        """
        if not self.edge_taps:
            return None
        t0 = now_ms - self.fatigue_window_ms
        top_hits = [r for (t,e,r) in self.edge_taps if t >= t0 and e == "top"]
        bot_hits = [r for (t,e,r) in self.edge_taps if t >= t0 and e == "bot"]

        def _is_fatigued(arr):
            if len(arr) < self.fatigue_hits:
                return False
            # require average ratio above threshold to call it 'pressing'
            avg = sum(arr[-self.fatigue_hits:]) / float(self.fatigue_hits)
            return avg >= self.fatigue_vol_k

        if _is_fatigued(top_hits):
            return "top"
        if _is_fatigued(bot_hits):
            return "bot"
        return None

    def allow_trade_at(self, level_price, side, now_ms):
        """
        Before placing a bounce trade inside the box, call this.
        Returns (allowed, reason, conf_multiplier)
        """
        key = (self._roundL(level_price), side)
        rec = self.touch[key]
        # too many bounces at this level/side?
        if rec["count"] >= self.max_bounces:
            return (False, f"Bounce budget exhausted at {key}", 1.0)

        # enforce per-level/per-side cooldown
        if rec["last_ts"] is not None and now_ms - rec["last_ts"] < self.min_time_gap_ms:
            return (False, f"Per-level cooldown active at {key}", 1.0)

        # diminishing confidence with each extra touch
        mult = (self.decay ** max(0, rec["count"] - 0))  # 1st touch count==1 => decay^1 after we record later
        return (True, "ok", mult)

class AccumulationBreakoutGuard:
    """
    Detects accumulation (compressed range), confirms breakout with volume,
    and exposes a regime that can gate your entries.
    """
    def __init__(self,
                 box_lookback_min=60,       # minutes to form/verify the box
                 min_bars=30,               # require enough bars to trust the box
                 compression_bp=18,         # max box height ~<= 0.18% of price
                 vol_short=5, vol_long=20,  # volume spike = short MA > k * long MA
                 vol_k=1.40,                # spike multiplier
                 reenter_clear_bars=3):     # bars back inside box to clear regime
        self.bars = deque(maxlen=600)  # store (ts_ms, o,h,l,c,v)
        self.box_lookback_min = box_lookback_min
        self.min_bars = min_bars
        self.compression_bp = compression_bp
        self.vol_short = vol_short
        self.vol_long = vol_long
        self.vol_k = vol_k
        self.reenter_clear_bars = reenter_clear_bars

        self.box_low = None
        self.box_high = None
        self.box_ts0 = None
        self.regime = "unknown"  # unknown|accumulation|breakout_up|breakout_down
        self._inside_count = 0

    def push_minute_bar(self, ts_ms, o, h, l, c, v):
        self.bars.append((ts_ms, o, h, l, c, v))
        self._update_state()

    def _subset_last_minutes(self, minutes):
        if not self.bars:
            return []
        t_end = self.bars[-1][0]
        lim = minutes * 60 * 1000
        return [b for b in self.bars if t_end - b[0] <= lim]

    @staticmethod
    def _ma(vals, n):
        if n <= 0 or len(vals) < n:
            return None
        return sum(vals[-n:]) / float(n)

    def _update_state(self):
        window = self._subset_last_minutes(self.box_lookback_min)
        if len(window) < self.min_bars:
            self.regime = "unknown"
            self.box_low = self.box_high = self.box_ts0 = None
            self._inside_count = 0
            return

        highs = [b[2] for b in window]
        lows  = [b[3] for b in window]
        closes = [b[4] for b in window]
        vols = [b[5] for b in window]
        price_now = closes[-1]

        box_low = min(lows)
        box_high = max(highs)
        box_height = box_high - box_low
        # compress threshold in price terms (basis points of current price)
        compress_thresh = price_now * (self.compression_bp / 10000.0)

        vol_ma_s = self._ma(vols, self.vol_short)
        vol_ma_l = self._ma(vols, self.vol_long)

        # Establish / maintain the accumulation box
        if box_height <= max(1e-6, compress_thresh):
            # compressed: in accumulation unless currently in breakout that hasn't failed
            if self.regime not in ("breakout_up", "breakout_down"):
                self.regime = "accumulation"
            self.box_low, self.box_high = box_low, box_high
            if self.box_ts0 is None:
                self.box_ts0 = window[0][0]
        else:
            # not compressed: keep last box if we had one; else unknown
            if self.regime not in ("breakout_up", "breakout_down"):
                self.regime = "unknown"

        # Breakout confirmation with volume spike
        if self.box_low is not None and self.box_high is not None and vol_ma_s and vol_ma_l:
            # up breakout: close cleanly above box high AND short vol MA > k * long vol MA
            if closes[-1] > (self.box_high + 1e-6) and (vol_ma_s > self.vol_k * vol_ma_l):
                self.regime = "breakout_up"
                self._inside_count = 0
            # down breakout
            elif closes[-1] < (self.box_low - 1e-6) and (vol_ma_s > self.vol_k * vol_ma_l):
                self.regime = "breakout_down"
                self._inside_count = 0

        # Regime clear: if after breakout we re-enter box and *stay inside* a few bars
        if self.regime in ("breakout_up", "breakout_down") and self.box_low is not None:
            if self.box_low <= closes[-1] <= self.box_high:
                self._inside_count += 1
                if self._inside_count >= self.reenter_clear_bars:
                    self.regime = "accumulation"  # back to box
            else:
                self._inside_count = 0

    # --- Public helpers ---
    def current_box(self):
        return (self.box_low, self.box_high)

    def allow_trade(self, side):
        """
        Gate entries:
        - In breakout_up: allow LONGs; block SHORTs.
        - In breakout_down: allow SHORTs; block LONGs.
        - In accumulation/unknown: allow both (engine's other rules apply).
        """
        if self.regime == "breakout_up" and side == "SHORT":
            return False, "Counter-trend blocked (breakout_up)"
        if self.regime == "breakout_down" and side == "LONG":
            return False, "Counter-trend blocked (breakout_down)"
        return True, self.regime
        
class MonolithicEngine:
    def __init__(self, conn, symbol="SPY", exit_planner=None):
        self.conn = conn
        self.symbol = symbol
        self.state = EngineState()
        self.feed = PriceFeed(symbol)
        self.acc_guard = AccumulationBreakoutGuard()

        # buffers / caches
        self.recent_bars = []
        self.levels_cache = load_levels(conn)

        # engine settings
        self.CONTACT_PROX           = float(settings_get(conn, "CONTACT_PROX", "0.05"))
        self.Q_SIGNAL_COOLDOWN_S    = int(settings_get(conn, "Q_SIGNAL_COOLDOWN", "8"))
        self.REVERSE_TOUCH_DECAY    = 0.08
        self.STOP_PADDING           = float(settings_get(conn, "STOP_PADDING", "0.35"))
        self.TP_PADDING             = float(settings_get(conn, "TP_PADDING", "0.25"))
        self._contact_latch = {}   # key -> bool (are we currently “inside” this level’s window?)


        # policy switches / thresholds
        self.ENABLE_VETO            = settings_get(self.conn, "ENABLE_VETO", "1") == "1"
        self.VETO_VOL_STRONG        = float(settings_get(self.conn, "VETO_VOL_STRONG", "0.25"))
        self.VETO_PROX              = float(settings_get(self.conn, "VETO_PROX", "0.06"))
        self.DISABLE_ML_GATE        = settings_get(self.conn, "DISABLE_ML_GATE", "0") == "1"

        # min confidence (accepts 0–1 or 0–100)
        self.Q_MIN_PROB             = self._read_prob_threshold()

        # use the provided planner or make a default one
        self.exit_planner = exit_planner or ExitStrategy(
            proximity_window=0.35,
            confluence_window=0.6,
            slight_pierce_fraction=0.12,
            vol_lookback=5,
            min_bars_for_trend=3,
            min_retrace_ticks=0.08
        )

        self.touchmem = LevelTouchMemory()

        # ML model (leave your existing code as-is)
        self.model = None
        self.model_path = os.path.join(os.path.dirname(__file__), 'models', 'qmmx_lr.joblib')
        if SKLEARN_OK and os.path.exists(self.model_path):
            try:
                self.model = joblib.load(self.model_path)
            except Exception:
                self.model = None

        audit(self.conn, "DEBUG", "GATE_MODE",
            f"blend={(settings_get(self.conn,'USE_BLEND','0')=='1')}")

    def reload_levels(self):
        self.levels_cache = load_levels(self.conn)

    def compute_confidence(self, level, price, direction, touch_count):
        dist = abs(price - level["price"])
        base = max(0.0, 1.0 - (dist / max(0.0001, self.CONTACT_PROX)))
        base += (0.08 if level["type"] == "solid" else 0.02)
        if touch_count <= 1:
            base += 0.10
        elif touch_count == 2:
            base -= self.REVERSE_TOUCH_DECAY
        else:
            base -= (self.REVERSE_TOUCH_DECAY * 2)
        if direction in ("up", "down"):
            base += 0.03
        return float(max(0.0, min(1.0, base)))
    
    def _read_prob_threshold(self):
        """
        Read min confidence from settings. Accepts either 0–1.0 or 0–100.
        Also supports legacy key 'minp'.
        """
        raw = settings_get(self.conn, "Q_MIN_PROB", None)
        if raw is None:
            raw = settings_get(self.conn, "minp", "0.60")
        try:
            val = float(raw)
        except Exception:
            val = 0.60
        # If someone typed 55 or 60, treat as percent.
        if val > 1.0:
            val = val / 100.0
        # safety clamp
        if not (0.0 <= val <= 0.99):
            val = 0.60
        return val

    def reload_thresholds(self):
        self.Q_MIN_PROB = self._read_prob_threshold()
        audit(self.conn, "SETTINGS", "ENGINE_APPLIED", f"Q_MIN_PROB={self.Q_MIN_PROB:.2f}")


    def _ml_allowed(self, extras):
        if not self.model:
            return True, None
        lvl_type = 1 if (extras.get("level", ["","solid","",])[1] == "solid" or extras.get("type","solid")=="solid") else 0
        distf = abs(extras.get("level_price", 0.0) - extras.get("stop", extras.get("level_price", 0.0)))
        touch = float(extras.get("touch_count", 1))
        direc = 1 if extras.get("direction") == "up" else 0
        X = [[lvl_type, distf, touch, direc]]
        try:
            proba = self.model.predict_proba(X)[0][1]
            return (proba >= self.Q_MIN_PROB), float(proba)
        except Exception:
            return True, None
        
    def _insert_policy_event(self, phase: str, action: str, features: dict, trade_id=None, notes: str = ""):
        # 1) persist the structured event (unchanged)
        try:
            cur = self.conn.cursor()
            cur.execute(
                "INSERT INTO policy_events(ts, phase, action, features_json, label, trade_id, notes) "
                "VALUES(?,?,?,?,?,?,?)",
                (utcnow(), phase, action, json.dumps(features), None, trade_id, notes)
            )
            self.conn.commit()
        except Exception:
            pass

        # 2) ALSO write a human-readable line to the audit_log (this is what your Log tab shows)
        try:
            line = self._format_policy_line(phase, action, features)
            # 'audit' should already exist in your codebase; it appends to audit_log and UI
            audit(self.conn, phase.upper(), action.upper(), line, features)
            # If your UI uses a direct text widget instead of reading the table, also echo out:
            if hasattr(self, "_log_ui"):
                self._log_ui(phase.upper(), action.upper(), line)
        except Exception:
            pass

    def evaluate_entry(self, price_current, prev_price, now_ms, api_key_present):
        # 0) API key
        if not api_key_present:
            self._insert_policy_event("entry", "skip", {"reason": "MISSING_API_KEY"})
            return False, MISSING_API_KEY, "No Polygon API key set.", {}

        # 1) Fresh price
        if price_current is None or self.state.last_ts_ms is None or (now_ms - self.state.last_ts_ms) > 15000:
            self._insert_policy_event("entry", "skip", {
                "reason": "PRICE_STALE",
                "last_ts_ms": self.state.last_ts_ms,
                "now": now_ms
            })
            return False, PRICE_STALE, "Price None or stale (>15s).", {"last_ts_ms": self.state.last_ts_ms, "now": now_ms}

        # 2) Not already in a trade
        if self.state.open_trade_id is not None:
            self._insert_policy_event("entry", "skip", {
                "reason": "IN_POSITION",
                "open_trade_id": self.state.open_trade_id
            })
            return False, IN_POSITION, "Already in a position.", {"trade_id": self.state.open_trade_id}

        # 3) Cooldown
        if self.state.in_cooldown(now_ms):
            self._insert_policy_event("entry", "skip", {
                "reason": "COOLDOWN",
                "cooldown_until_ms": self.state.cooldown_until_ms
            })
            return False, COOLDOWN, "Signal cooldown active.", {"until": self.state.cooldown_until_ms}

        # 4) Levels
        if not self.levels_cache:
            self._insert_policy_event("entry", "skip", {"reason": "NOLEVELS"})
            return False, NOLEVELS, "No levels loaded.", {}

        # 5) Direction from prev -> current
        EPS = 1e-9
        direction = None
        if prev_price is not None:
            if price_current > prev_price + EPS:
                direction = "up"
            elif price_current < prev_price - EPS:
                direction = "down"
            else:
                direction = self.state.last_direction  # reuse last non-flat direction
        if direction is None:
            self._insert_policy_event("entry", "skip", {"reason": "DIR_UNKNOWN"})
            return False, DIR_UNKNOWN, "Flat tick; cannot infer approach.", {}

        # 6) Nearest level & distance
        nearest = min(self.levels_cache, key=lambda L: abs(L["price"] - price_current))
        dist = abs(nearest["price"] - price_current)
        if dist > self.CONTACT_PROX:
            self._insert_policy_event("entry", "skip", {
                "reason": "TOO_FAR",
                "level_price": float(nearest["price"]),
                "proximity_abs": float(dist),
                "CONTACT_PROX": float(self.CONTACT_PROX),
            })
            return False, TOO_FAR, (
                f"Nearest level {nearest['color']}/{nearest['type']}[{nearest['index']}] "
                f"@{nearest['price']:.2f} too far ({dist:.2f})."
            ), {"dist": dist}

        # 7) Touch count: increment once per contact (even if we don't trade)
        key = (nearest["color"], nearest["type"], nearest["index"])
        inside = (dist <= self.CONTACT_PROX)

        # latch logic to avoid incrementing every tick while hovering
        latched = self._contact_latch.get(key, False)
        if inside and not latched:
            # first tick of a NEW touch
            self.state.level_touch_counts[key] = self.state.level_touch_counts.get(key, 0) + 1
            self._contact_latch[key] = True
        elif not inside and latched:
            # we left the window → release
            self._contact_latch[key] = False

        # also release latches for other levels we've drifted away from
        for L in (self.levels_cache or []):
            k_other = (L["color"], L["type"], L["index"])
            if k_other != key and self._contact_latch.get(k_other):
                if abs(float(L["price"]) - float(price_current)) > self.CONTACT_PROX:
                    self._contact_latch[k_other] = False

        touch_count = self.state.level_touch_counts.get(key, 0)
        if touch_count >= 4:
            self._insert_policy_event("entry", "skip", {
                "reason": "LEVEL_OVERTOUCHED",
                "level": list(key),
                "touch_count": int(touch_count),
            })
            return False, LEVEL_OVERTOUCHED, f"Level over-touched (#{touch_count}).", {
                "level": key, "touch_count": touch_count
            }
        
                # ---- LevelTouchMemory gate for accumulation bounces (before #8 Confidence) ----
        touch_decay_mult = 1.0
        if getattr(self, "acc_guard", None) and self.acc_guard.regime == "accumulation":
            # Decide which edge we’re interacting with based on approach direction
            # (SHORTs are top-edge bounces; LONGs are bottom-edge bounces)
            edge_for_this = "top" if direction == "down" else "bot"

            # 3a) Edge fatigue: if the edge is being pressed with rising volume, block bounces there
            fatigued = self.touchmem.edge_fatigued(now_ms)
            if fatigued == edge_for_this:
                self._insert_policy_event("entry", "skip", {
                    "reason": "EDGE_FATIGUE",
                    "edge": fatigued,
                    "level_price": float(nearest["price"])
                })
                return False, VETO, f"Edge fatigue blocks {('SHORT' if direction=='down' else 'LONG')} bounce", {}

            # 3b) Per-level budget/cooldown and diminishing confidence
            ok_touch, why, mult = self.touchmem.allow_trade_at(
                level_price=nearest["price"],
                side=("SHORT" if direction == "down" else "LONG"),
                now_ms=now_ms
            )
            if not ok_touch:
                self._insert_policy_event("entry", "skip", {
                    "reason": why,
                    "level_price": float(nearest["price"])
                })
                return False, VETO, why, {}

            touch_decay_mult = float(mult)
        else:
            touch_decay_mult = 1.0

        # (place this just above "# 8) Confidence")
        qmin = float(self.Q_MIN_PROB or 0.0)

        # 8) Confidence (your handcrafted conf before ML)
        conf = self.compute_confidence(nearest, price_current, direction, touch_count) * touch_decay_mult
        if conf < qmin:
            self._insert_policy_event("entry", "skip", {
                "reason": "CONF_LOW",
                "conf": float(conf),
                "Q_MIN_PROB": float(qmin),
                "level_price": float(nearest["price"]),
                "proximity_abs": float(dist),
                "approach": ("from_below" if direction == "up" else "from_above"),
                "touch_count": int(touch_count),
            })
            return False, CONF_LOW, f"Confidence {conf:.2f} < min {qmin:.2f}.", {
                "level": key, "level_price": nearest["price"], "conf": conf,
                "touch_count": touch_count, "dir": direction
            }

        # 9) Side + risk scaffold (stop/target derived from level)
        extras = {
            "side": ("long" if direction == "up" else "short"),
            "level": key,
            "level_price": float(nearest["price"]),
            "conf": float(conf),
            "touch_count": int(touch_count),
            "direction": direction
        }
        # --- Accumulation/Breakout regime gate (blocks counter-trend after a volume-confirmed break) ---
        if getattr(self, "acc_guard", None):
            intended = "LONG" if extras["side"] == "long" else "SHORT"
            ok_gate, why = self.acc_guard.allow_trade(intended)
            if not ok_gate:
                # optional: structured breadcrumb for your Log tab
                self._insert_policy_event("entry", "policy_skip", {
                    "reason": "ACC_BREAKOUT_GATE",
                    "detail": why,
                    "level_price": float(extras["level_price"]),
                    "side": extras["side"],
                    "conf": float(extras.get("conf", 0.0)),
                    "touch_count": int(extras.get("touch_count", 0)),
                })
                return False, 904, why, {**extras}
        # ------------------------------------------------------------------

        if extras["side"] == "long":
            stop = nearest["price"] - float(settings_get(self.conn, "STOP_PADDING", "0.35"))
            target = nearest["price"] + float(settings_get(self.conn, "TP_PADDING", "0.25"))
        else:
            stop = nearest["price"] + float(settings_get(self.conn, "STOP_PADDING", "0.35"))
            target = nearest["price"] - float(settings_get(self.conn, "TP_PADDING", "0.25"))
        extras["stop"], extras["target"] = float(stop), float(target)

        # 10) Behavioral veto (soft; defaults to ALLOW unless strong contradictory evidence)
        if self.ENABLE_VETO:
            approach     = "from_below" if direction == "up" else "from_above"
            volume_slope = self._calc_volume_slope(getattr(self, "recent_bars", []), window=6)
            confluence   = self._has_confluence_near(nearest["price"], within=0.15)

            allowed, veto_code, _ = self._soft_veto(
                side=extras["side"],
                price=float(price_current),
                level_price=float(nearest["price"]),
                volume_slope=float(volume_slope),
                approach=approach,
                confluence=bool(confluence),
                proximity_abs=float(dist),
                slight_pierce_window=(getattr(self, "CONTACT_PROX", 0.1) * 0.12),
            )
            if not allowed:
                self._insert_policy_event("entry", "policy_skip", {
                    "reason": veto_code,
                    "prox": float(dist),
                    "vol_slope": float(volume_slope),
                    "confluence": bool(confluence),
                    "approach": approach,
                    "level_price": float(nearest["price"]),
                    "side": extras["side"],
                    "conf": float(extras["conf"]),
                    "touch_count": int(extras["touch_count"]),
                })
                return False, VETO, f"Veto {veto_code}", {**extras, "veto": veto_code}

        # 11) ML / blended gate (respects kill-switch)
        # decide mode: AND (hard gate) vs blended (70/30)
        use_blend = getattr(self, "USE_BLEND_OVERRIDE", None)
        if use_blend is None:
            use_blend = (settings_get(self.conn, "USE_BLEND", "0") == "1")

        # weights (default 70/30), normalized just in case
        w_rules = float(settings_get(self.conn, "W_RULES", "0.7") or 0.7)
        w_ml    = float(settings_get(self.conn, "W_ML", "0.3") or 0.3)
        s = w_rules + w_ml
        if s <= 0:
            w_rules, w_ml, s = 1.0, 0.0, 1.0
        w_rules, w_ml = w_rules / s, w_ml / s

        mlp = None
        ok_ml = True

        if not self.DISABLE_ML_GATE:
            ok_ml, prob = self._ml_allowed(extras)
            mlp = float(prob) if prob is not None else float(conf)
        else:
            mlp = float(extras.get("ml_prob", extras.get("conf", conf)))

        if use_blend:
            blended = float(w_rules) * float(conf) + float(w_ml) * float(mlp)
            if blended < qmin:
                self._insert_policy_event("entry", "skip", {
                    "reason": "COMBINED_LOW",
                    "conf": float(conf),
                    "ml_prob": float(mlp),
                    "blended": float(blended),
                    "Q_MIN_PROB": float(qmin),
                })
                return False, CONF_LOW, f"Blended {blended:.2f} < min {qmin:.2f}", {**extras, "ml_prob": mlp, "blended": blended}
            extras["ml_prob"] = float(mlp)
            extras["blended"] = float(blended)
        else:
            # classic AND: require ML to pass too (unless ML gate is disabled)
            if not self.DISABLE_ML_GATE and not ok_ml:
                self._insert_policy_event("entry", "skip", {
                    "reason": "ML_CONF_LOW",
                    "ml_prob": float(mlp),
                    "Q_MIN_PROB": float(qmin),
                    "level_price": float(nearest["price"]),
                    "proximity_abs": float(dist),
                    "approach": ("from_below" if direction == "up" else "from_above"),
                    "touch_count": int(touch_count),
                })
                return False, CONF_LOW, f"ML prob {mlp:.2f} < min {qmin:.2f}", {**extras, "ml_prob": mlp}
            extras["ml_prob"] = float(mlp)

        # 12) Pre-open policy event so open_trade() can attach it
        try:
            feats = {
                "proximity_abs": float(abs(extras["level_price"] - float(price_current))),
                "approach": ("from_below" if direction == "up" else "from_above"),
                "touch_count": int(extras["touch_count"]),
                "conf": float(extras["conf"]),
                "ml_prob": float(extras.get("ml_prob", extras["conf"])),
            }
            self._insert_policy_event("entry", f"go_{extras['side']}", feats, trade_id=None, notes="PRE_OPEN")
        except Exception:
            pass

        return True, OK, "Entry allowed.", extras
    
    def _soft_veto(self, *, side, price, level_price, volume_slope, approach, confluence, proximity_abs, slight_pierce_window):
        # If we have little evidence, do NOT veto.
        if abs(volume_slope) < 0.05 and not confluence:
            return True, "INCONCLUSIVE", {"note": "weak evidence"}

        # Your rule-of-thumb:
        # - Approaching a level: decreasing volume ⇒ reversal; increasing volume ⇒ penetration.
        # - Only veto when the chosen side contradicts the likely behavior AND we are very near the level.
        strong = self.VETO_VOL_STRONG
        near   = proximity_abs <= max(self.VETO_PROX, slight_pierce_window)

        if approach == "from_below":
            # Up towards resistance
            if near and side == "long"  and volume_slope < -strong: return False, "CONTRA_VOL_LONG",  {}
            if near and side == "short" and volume_slope >  strong: return False, "CONTRA_VOL_SHORT", {}
        else:
            # Down towards support
            if near and side == "long"  and volume_slope >  strong: return False, "CONTRA_VOL_LONG",  {}
            if near and side == "short" and volume_slope < -strong: return False, "CONTRA_VOL_SHORT", {}

        # Confluence is a weak nudge toward continuation; never a hard veto by itself.
        return True, "ALLOW", {}

    def _calc_volume_slope(self, bars, window=6):
        if not bars or len(bars) < 3:
            return 0.0

        def _vol(b):
            # Try multiple common keys; default to 0 if absent
            return (b.get("v") or b.get("volume") or b.get("V") or 0.0)

        last = bars[-min(window, len(bars)):]
        vols = [float(_vol(b)) for b in last]
        half = max(2, len(vols) // 2)
        v1 = sum(vols[:half]) / half
        v2 = sum(vols[-half:]) / half
        if v1 == 0 and v2 == 0:
            return 0.0
        return (v2 - v1) / (abs(v1) + 1e-9)

    def _on_minute_close(self, ts_ms, o, h, l, c, v):
        """
        Call this once per finished 1-min bar.
        - Keeps a small bar buffer
        - Feeds the Accumulation guard (if you added it)
        - Registers level touches while in accumulation
        """
        # keep a compact (price, volume, ts) history for other features
        self.recent_bars.append({"price": float(c), "volume": float(v), "ts": int(ts_ms)})
        if len(self.recent_bars) > 240:  # ~ last 4 hours
            self.recent_bars = self.recent_bars[-240:]

        # simple volume MAs from recent_bars (5/20)
        vols = [float(b.get("volume", 0.0)) for b in self.recent_bars]
        vol_ma_s = (sum(vols[-5:]) / max(1, min(5, len(vols)))) if vols else 0.0
        vol_ma_l = (sum(vols[-20:]) / max(1, min(20, len(vols)))) if vols else 0.0

        # If you added AccumulationBreakoutGuard elsewhere, feed it:
        if getattr(self, "acc_guard", None):
            try:
                self.acc_guard.push_minute_bar(ts_ms, o, h, l, c, v)
            except Exception:
                pass

        # While in accumulation, record touches & edge taps
        if getattr(self, "acc_guard", None) and self.acc_guard.regime == "accumulation":
            box_low, box_high = self.acc_guard.current_box()
            try:
                self.touchmem.register_touch_if_any(
                    ts_ms, o, h, l, c, v,
                    levels=self.levels_cache,
                    price_now=c,
                    box_low=box_low,
                    box_high=box_high,
                    vol_ma_s=vol_ma_s,
                    vol_ma_l=vol_ma_l
                )
            except Exception:
                pass

        # If we’ve flipped into a breakout regime, clear per-box touch memory
        if getattr(self, "acc_guard", None) and self.acc_guard.regime in ("breakout_up", "breakout_down"):
            self.touchmem.reset_box()

    def ingest_tick(self, ts_ms: int, price: float, volume: float = 0.0):
        """
        Feed every tick/price update here.
        Rolls a 1-minute OHLCV bar and calls _on_minute_close(...) when the minute ends.
        """
        m = int(ts_ms // 60000)  # minute bucket
        cur = getattr(self, "_cur_bar", None)

        if cur is None or cur["m"] != m:
            # finalize previous bar
            if cur is not None:
                self._on_minute_close(
                    cur["ts0_ms"], cur["o"], cur["h"], cur["l"], cur["c"], cur["v"]
                )
            # start new bar
            self._cur_bar = {
                "m": m,
                "ts0_ms": (ts_ms - (ts_ms % 60000)),  # minute start
                "o": float(price), "h": float(price), "l": float(price), "c": float(price),
                "v": float(volume or 0.0),
            }
        else:
            # update current bar
            cur["c"] = float(price)
            if price > cur["h"]: cur["h"] = float(price)
            if price < cur["l"]: cur["l"] = float(price)
            cur["v"] += float(volume or 0.0)

    def _has_confluence_near(self, target_price, within=0.15):
        return sum(1 for L in self.levels_cache if abs(L["price"] - target_price) <= within) >= 2

    def open_trade(self, side, entry, stop, target, reason_open):
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO trades(ts_open, symbol, side, entry, stop, target, reason_open) "
            "VALUES(?,?,?,?,?,?,?)",
            (utcnow(), self.symbol, side, entry, stop, target, reason_open)
        )
        self.conn.commit()
        trade_id = cur.lastrowid
        self.state.open_trade_id = trade_id

        # Attach most recent unlabeled entry policy_event to this trade
        try:
            cur.execute("""
                UPDATE policy_events
                SET trade_id = ?
                WHERE id = (
                    SELECT id FROM policy_events
                    WHERE phase='entry' AND trade_id IS NULL
                    ORDER BY id DESC
                    LIMIT 1
                )
            """, (trade_id,))
            self.conn.commit()
        except Exception:
            pass

        return trade_id

    def close_trade(self, trade_id, exit_price, reason_close):
        cur = self.conn.cursor()
        cur.execute("SELECT side, entry FROM trades WHERE id=?", (trade_id,))
        row = cur.fetchone()
        if not row:
            return
        side, entry = row
        entry = float(entry)
        exit_price = float(exit_price)
        pnl = (exit_price - entry) if side == "long" else (entry - exit_price)

        # WRITE the close into the DB
        cur.execute(
            "UPDATE trades SET ts_close=?, exit=?, reason_close=?, pnl=? WHERE id=?",
            (utcnow(), exit_price, reason_close, pnl, trade_id)
        )
        self.conn.commit()
        # Label the entry policy event for this trade (engine does not own the policy object)
        try:
            label = 1 if pnl > 0 else 0
            cur = self.conn.cursor()
            cur.execute(
                "UPDATE policy_events SET label=? WHERE trade_id=? AND phase='entry' AND label IS NULL",
                (label, trade_id)
            )
            self.conn.commit()
            # Note: Online update of the in-memory policy should be triggered from the App side if needed.
        except Exception:
            pass

        # clear open state
        self.state.open_trade_id = None

    def maybe_escalate_on_target(self, price_current: float, recent_bars=None):
        """
        If price is at/near the current trade's target, ask ExitStrategy whether
        continuation is favored. If yes, roll target to next level and trail stop.
        Returns (escalated: bool, meta: dict|None)
        """
        if self.state.open_trade_id is None:
            return False, None

        cur = self.conn.cursor()
        cur.execute("SELECT id, side, entry, stop, target FROM trades WHERE id=?", (self.state.open_trade_id,))
        row = cur.fetchone()
        if not row:
            return False, None

        trade_id, side, entry, stop, target = row
        side = str(side).lower()
        entry = float(entry); stop = float(stop); target = float(target)
        price = float(price_current)

        # only consider when we're within the same proximity window you use for contact
        if abs(price - target) > self.CONTACT_PROX:
            return False, None

        # Build a minimal context for ExitStrategy
        open_trade_ctx = {"direction": side, "entry": entry}

        # Use cached levels (reload externally when you change levels)
        levels = self.levels_cache or []

        # recent_bars should be a list of (price, volume, ts) oldest→newest; it's OK if None
        esc = self.exit_planner.should_escalate_on_target(
            open_trade=open_trade_ctx,
            current_price=price,
            levels=levels,
            recent_bars=recent_bars
        )

        if esc.get("escalate") and esc.get("next_target") is not None:
            new_target = float(esc["next_target"])
            new_stop = float(esc.get("trail_stop") or stop)

            # Update the open trade in place (do NOT close—keep managing it)
            cur.execute("UPDATE trades SET stop=?, target=? WHERE id=?", (new_stop, new_target, trade_id))
            self.conn.commit()

            # OPTIONAL: audit / UI log if you have helpers for it
            try:
                audit(self.conn, "EXIT", "ESCALATE",
                    f"Rolled target {target:.2f}→{new_target:.2f}, trailed stop→{new_stop:.2f}",
                    {"basis": esc.get("basis"), "score": esc.get("score")})
                self._log_ui("EXIT", "ESCALATE",
                            f"Rolled target to {new_target:.2f}, trail stop {new_stop:.2f}")
                if hasattr(self, "qvoice"):
                    self.qvoice.say("VOL_INC_CONT", kind="ENTRY_EVAL", symbol=self.symbol,
                                    notes=f"Escalated: {target:.2f}→{new_target:.2f}")
            except Exception:
                pass

            return True, {"old_target": target, "new_target": new_target, "new_stop": new_stop}
        else:
            # No continuation edge → let caller decide (may close normally)
            return False, {"basis": esc.get("basis")}

# ============================
# GUI (Tkinter)
# ============================

class QMMXApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("1180x800")

        # 1) DB and QVoice first
        self.conn = db_connect()
        db_init(self.conn)
        self.qvoice = QVoice(DB_PATH)  # ok to init early

        self.trade_filters = {
            "symbol": "",
            "side": "",        # "long"/"short" or ""
            "date_from": "",   # text, e.g. "2025-08-20"
            "date_to": "",     # text
        }

        # 2) Plain-Python attributes BEFORE engine
        self.symbol = settings_get(self.conn, "symbol", "SPY")
        self.api_key = settings_get(self.conn, "polygon_api_key", "")
        self.allow_ah = settings_get(self.conn, "allow_after_hours", "0") == "1"
        self.chart_candles = int(settings_get(self.conn, "chart_candles", "120") or 120)
        self.starting_balance = float(settings_get(self.conn, "portfolio_start", "10000") or 10000.0)

        # 3) Build planners/policy
        self.policy = OnlinePolicy(lr=0.03, l2=1e-6, use_perceptron=False)
        self.entry_planner = SmartEntryPlanner(
            proximity_window=0.35, confluence_window=0.6,
            slight_pierce_fraction=0.12, vol_lookback=5,
            min_bars_for_trend=3, min_retrace_ticks=0.08,
            entry_slippage=0.03, freshness_seconds=180
        )
        self._load_policy()

        self.exit_planner = ExitStrategy(
            proximity_window=0.35, confluence_window=0.6,
            slight_pierce_fraction=0.12, vol_lookback=5,
            min_bars_for_trend=3, min_retrace_ticks=0.08
        )

        # 4) NOW build the engine and pass the planner + symbol
        self.engine = MonolithicEngine(self.conn, symbol=self.symbol, exit_planner=self.exit_planner)

        # 5) UI, threads, timers
        self.log_text = None
        self.engine_thread = None
        self.running = False
        self.ui_queue = queue.Queue()
        self._build_ui()
        self._load_levels_into_ui()
        self._start_price_loop()
        self._schedule_price_ping()
        self._start_retrain_scheduler()
        self._refresh_portfolio_ui()
        self._trade_artists = []   # matplotlib artists we add for trades

        audit(
            self.conn, "DEBUG", "GATES",
            (
                f"policy={(settings_get(self.conn,'DISABLE_POLICY_GATE','0')!='1')} "
                f"veto={self.engine.ENABLE_VETO} "
                f"ml={not self.engine.DISABLE_ML_GATE} "
                f"prox={self.engine.CONTACT_PROX} "
                f"stop={self.engine.STOP_PADDING} "
                f"tp={self.engine.TP_PADDING} "
                f"blend={(settings_get(self.conn,'USE_BLEND','0')=='1')} "
                f"w_rules={settings_get(self.conn,'W_RULES','0.7')} "
                f"w_ml={settings_get(self.conn,'W_ML','0.3')}"
            )
        )

        # === Policy persistence ===
    def _policy_store_path(self) -> str:
        models_dir = os.path.join(os.path.dirname(__file__), "models")
        os.makedirs(models_dir, exist_ok=True)
        return os.path.join(models_dir, "online_policy.joblib")

    def _load_policy(self):
        try:
            path = self._policy_store_path()
            if os.path.exists(path):
                obj = joblib.load(path)
                # Expected: {"w_entry": {...}, "w_exit": {...}, "dim": int, "cfg": {...}}
                self.policy.w_entry = obj.get("w_entry", self.policy.w_entry)
                self.policy.w_exit  = obj.get("w_exit",  self.policy.w_exit)
                self.policy._dim    = obj.get("dim",     self.policy._dim)
                cfg = obj.get("cfg", {})
                self.policy.lr = cfg.get("lr", self.policy.lr)
                self.policy.l2 = cfg.get("l2", self.policy.l2)
                self.policy.use_perceptron = cfg.get("use_perceptron", self.policy.use_perceptron)
                self._log_ui("POLICY", "LOAD", f"Loaded OnlinePolicy from {os.path.basename(path)}")
        except Exception as e:
            self._log_ui("POLICY", "LOAD_ERR", f"Failed loading OnlinePolicy: {e}")

    def _save_policy(self):
        try:
            path = self._policy_store_path()
            payload = {
                "w_entry": self.policy.w_entry,
                "w_exit":  self.policy.w_exit,
                "dim":     self.policy._dim,
                "cfg": {"lr": self.policy.lr, "l2": self.policy.l2, "use_perceptron": self.policy.use_perceptron}
            }
            joblib.dump(payload, path)
            self._log_ui("POLICY", "SAVE", f"Saved OnlinePolicy → {os.path.basename(path)}")
        except Exception as e:
            self._log_ui("POLICY", "SAVE_ERR", f"Failed saving OnlinePolicy: {e}")

    def _insert_policy_event(self, phase: str, action: str, features: dict, trade_id=None, notes: str = ""):
        try:
            cur = self.conn.cursor()
            cur.execute(
                "INSERT INTO policy_events(ts, phase, action, features_json, label, trade_id, notes) "
                "VALUES(?,?,?,?,?,?,?)",
                (utcnow(), phase, action, json.dumps(features), None, trade_id, notes)
            )
            self.conn.commit()
        except Exception:
            pass
        # Mirror a readable line into the audit log (what the Log tab shows)
        try:
            line = self._format_policy_line(phase, action, features)
            audit(self.conn, phase.upper(), action.upper(), line, features)
            if hasattr(self, "_log_ui"):  # if you also buffer text to a widget
                self._log_ui(phase.upper(), action.upper(), line)
        except Exception:
            pass

    def _format_policy_line(self, phase: str, action: str, f: dict) -> str:
        reason      = str(f.get("reason", action)).upper()
        conf        = f.get("conf");     minp   = f.get("Q_MIN_PROB") or f.get("min")
        prox        = f.get("prox") or f.get("proximity_abs")
        level_price = f.get("level_price")
        approach    = f.get("approach"); touch  = f.get("touch_count")
        vol_slope   = f.get("vol_slope") or f.get("volume_trend")
        confl       = f.get("confluence"); mlp  = f.get("ml_prob")

        parts = [reason]
        if conf is not None and minp is not None: parts.append(f"conf={float(conf):.2f} < min={float(minp):.2f}")
        if prox is not None:        parts.append(f"prox={float(prox):.03f}")
        if level_price is not None: parts.append(f"lvl={float(level_price):.2f}")
        if approach:                parts.append(f"appr={approach}")
        if touch is not None:       parts.append(f"touch#{int(touch)}")
        if vol_slope is not None:   parts.append(f"volSlope={float(vol_slope):+.2f}")
        if confl is not None:       parts.append(f"confL={bool(confl)}")
        if mlp is not None:         parts.append(f"ml={float(mlp):.2f}")
        return " | ".join(parts)
    
    def _build_trades_tab(self, frame):
        # -- Filter bar --
        top = ttk.Frame(frame); top.pack(fill=tk.X, pady=6)

        ttk.Label(top, text="Symbol").pack(side=tk.LEFT, padx=(0,4))
        self.trf_symbol = ttk.Entry(top, width=8)
        self.trf_symbol.insert(0, self.trade_filters.get("symbol","") or self.symbol)
        self.trf_symbol.pack(side=tk.LEFT)

        ttk.Label(top, text="Side").pack(side=tk.LEFT, padx=(10,4))
        self.trf_side = ttk.Combobox(top, state="readonly", width=7,
                                    values=["(any)", "long", "short"])
        self.trf_side.set("(any)")
        self.trf_side.pack(side=tk.LEFT)

        ttk.Label(top, text="From").pack(side=tk.LEFT, padx=(10,4))
        self.trf_from = ttk.Entry(top, width=12)
        self.trf_from.insert(0, "")
        self.trf_from.pack(side=tk.LEFT)

        ttk.Label(top, text="To").pack(side=tk.LEFT, padx=(10,4))
        self.trf_to = ttk.Entry(top, width=12)
        self.trf_to.insert(0, "")
        self.trf_to.pack(side=tk.LEFT)

        ttk.Button(top, text="Apply", command=self._apply_trades_filters).pack(side=tk.LEFT, padx=(10,4))
        ttk.Button(top, text="Reset", command=self._reset_trades_filters).pack(side=tk.LEFT, padx=4)
        ttk.Button(top, text="Export CSV", command=self._export_trades_csv).pack(side=tk.LEFT, padx=(10,4))

        # -- Table --
        cols = ("id","open","close","symbol","side","entry","exit","stop","target","R","PnL","reason_open","reason_close")
        tv = ttk.Treeview(frame, columns=cols, show="headings", height=20)
        self.trades_tv = tv
        for c in cols:
            tv.heading(c, text=c)
            tv.column(c, width=90 if c not in ("reason_open","reason_close") else 160, anchor="w")

        # row colors
        tv.tag_configure("win",  background="#eaffea")  # light green
        tv.tag_configure("loss", background="#ffecec")  # light red
        tv.tag_configure("open", background="#eef3ff")  # faint blue for open trades

        tv.pack(fill=tk.BOTH, expand=True)

        legend = ttk.Frame(frame); legend.pack(anchor="w", pady=4)
        ttk.Label(legend, text="Legend: ").pack(side=tk.LEFT)
        lbl_w = ttk.Label(legend, text="WIN", background="#eaffea"); lbl_w.pack(side=tk.LEFT, padx=4)
        lbl_l = ttk.Label(legend, text="LOSS", background="#ffecec"); lbl_l.pack(side=tk.LEFT, padx=4)
        lbl_o = ttk.Label(legend, text="OPEN", background="#eef3ff"); lbl_o.pack(side=tk.LEFT, padx=4)

        # enter key on fields applies
        for w in (self.trf_symbol, self.trf_from, self.trf_to):
            w.bind("<Return>", lambda e: self._apply_trades_filters())
        self.trf_side.bind("<<ComboboxSelected>>", lambda e: self._apply_trades_filters())

        self._refresh_trades_tab()

    def _apply_trades_filters(self):
        self.trade_filters["symbol"]    = self.trf_symbol.get().strip()
        self.trade_filters["side"]      = "" if self.trf_side.get()=="(any)" else self.trf_side.get()
        self.trade_filters["date_from"] = self.trf_from.get().strip()
        self.trade_filters["date_to"]   = self.trf_to.get().strip()
        self._refresh_trades_tab()

    def _reset_trades_filters(self):
        self.trf_symbol.delete(0, "end"); self.trf_symbol.insert(0, self.symbol)
        self.trf_side.set("(any)")
        self.trf_from.delete(0, "end"); self.trf_to.delete(0, "end")
        self.trade_filters.update({"symbol": self.symbol, "side":"", "date_from":"", "date_to":""})
        self._refresh_trades_tab()

    def _refresh_trades_tab(self):
        tv = getattr(self, "trades_tv", None)
        if not tv: return
        for r in tv.get_children():
            tv.delete(r)

        cur = self.conn.cursor()
        cur.execute("""
            SELECT id, ts_open, ts_close, symbol, side, entry, exit, stop, target, reason_open, reason_close
            FROM trades
            ORDER BY id DESC
            LIMIT 1000
        """)
        rows = cur.fetchall()

        # parse helpers
        def parse_dt(s):
            if not s: return None
            try:
                dt = datetime.fromisoformat(str(s).replace("Z",""))
                return dt
            except Exception:
                return None

        def signed_R(side, entry, exit_px, stop):
            if exit_px is None or stop is None: return None
            risk = max(1e-9, abs(float(entry)-float(stop)))
            raw = (float(exit_px) - float(entry)) / risk
            return raw if str(side).lower()=="long" else -raw

        def pnl_usd(side, entry, exit_px):
            if exit_px is None: return None
            units = 1.0
            diff = (float(exit_px) - float(entry)) * (1.0 if str(side).lower()=="long" else -1.0)
            return diff * units

        # read filters
        f_symbol = (self.trade_filters.get("symbol") or "").upper()
        f_side   = (self.trade_filters.get("side") or "").lower()
        df_txt   = self.trade_filters.get("date_from") or ""
        dt_txt   = self.trade_filters.get("date_to") or ""
        df_dt    = parse_dt(df_txt) if df_txt else None
        dt_dt    = parse_dt(dt_txt) if dt_txt else None

        # filter + insert
        for (tid, o, c, sym, side, entry, exit_px, stop, target, ro, rc) in rows:
            # filters
            if f_symbol and sym and f_symbol not in str(sym).upper(): 
                continue
            if f_side and str(side).lower()!=f_side:
                continue
            o_dt = parse_dt(o); c_dt = parse_dt(c)
            # date filter checks: if close exists use close; else use open
            key_dt = c_dt or o_dt
            if df_dt and key_dt and key_dt < df_dt: 
                continue
            if dt_dt and key_dt and key_dt > dt_dt: 
                continue

            R = signed_R(side, entry, exit_px, stop)
            pnl = pnl_usd(side, entry, exit_px)

            tag = ()
            if exit_px is None:
                tag = ("open",)
            elif R is not None and R >= 0:
                tag = ("win",)
            elif R is not None and R < 0:
                tag = ("loss",)

            tv.insert("", "end", values=(
                tid, o or "", c or "", sym or "", side or "",
                f"{float(entry):.2f}" if entry is not None else "",
                f"{float(exit_px):.2f}" if exit_px is not None else "",
                f"{float(stop):.2f}" if stop is not None else "",
                f"{float(target):.2f}" if target is not None else "",
                f"{R:+.2f}" if R is not None else "",
                f"{pnl:+.2f}" if pnl is not None else "",
                ro or "", rc or ""
            ), tags=tag)

    def _export_trades_csv(self):
        import csv, os
        path = os.path.join(os.path.dirname(__file__), "trades_export.csv")
        cur = self.conn.cursor()
        cur.execute("""
            SELECT id, ts_open, ts_close, symbol, side, entry, exit, stop, target, reason_open, reason_close
            FROM trades ORDER BY id
        """)
        rows = cur.fetchall()
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["id","ts_open","ts_close","symbol","side","entry","exit","stop","target","reason_open","reason_close"])
            for r in rows: w.writerow(r)
        messagebox.showinfo("Export", f"Saved {len(rows)} rows to {path}")

    # ---------- UI Construction ----------
    def _build_ui(self):
        nb = ttk.Notebook(self)
        nb.pack(fill=tk.BOTH, expand=True)
        self.notebook = nb

        # Live
        self.live_frame = ttk.Frame(nb, padding=10)
        nb.add(self.live_frame, text="Live")
        self._build_live_tab(self.live_frame)

        # Levels
        self.levels_frame = ttk.Frame(nb, padding=10)
        nb.add(self.levels_frame, text="Levels")
        self._build_levels_tab(self.levels_frame)

                # Log
        self.log_tab = ttk.Frame(nb, padding=10)
        nb.add(self.log_tab, text="Log")

        log_frame = ttk.LabelFrame(self.log_tab, text="Audit Log (Why trade / why not)")
        log_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, state=tk.DISABLED)
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Wire diagnostic monitor to audit + UI log
        set_diagnostic_sink(
            lambda phase, code, msg, extra=None: (
                audit(self.conn, phase, code, msg, (extra or {})),
                self._log_ui(phase, code, msg)
            )
        )
        # Q Voice (natural-language narrator)
        self.voice_tab = ttk.Frame(nb, padding=10)
        nb.add(self.voice_tab, text="Q Voice")
        # Re-instantiate with UI attached (keeps DB, adds panel)
        self.qvoice = QVoice(DB_PATH, ui_parent=self.voice_tab)

        # Settings
        self.settings_frame = ttk.Frame(nb, padding=10)
        nb.add(self.settings_frame, text="Settings")
        self._build_settings_tab(self.settings_frame)

        footer = ttk.Frame(self, padding=8)
        footer.pack(fill=tk.X)
        self.start_btn = ttk.Button(footer, text="Start Engine", command=self.start_engine)
        self.stop_btn = ttk.Button(footer, text="Stop Engine", command=self.stop_engine, state=tk.DISABLED)
        self.start_btn.pack(side=tk.LEFT, padx=5)
        self.stop_btn.pack(side=tk.LEFT, padx=5)

        #Trades
        self.trades_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.trades_frame, text="Trades")
        self._build_trades_tab(self.trades_frame)

    # ---------- Charting ----------
    def _build_chart(self, parent):
        self.figure = plt.Figure(figsize=(9.2, 3.3), dpi=100)
        self.ax = self.figure.add_subplot(111)
        self.canvas = FigureCanvasTkAgg(self.figure, master=parent)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        btns = ttk.Frame(parent)
        btns.pack(fill=tk.X, pady=4)
        ttk.Button(btns, text="Refresh Chart", command=self._refresh_chart).pack(side=tk.LEFT)

        self.after(60000, self._auto_chart_refresh)

    def _auto_chart_refresh(self):
        try:
            self._refresh_chart()
        except Exception:
            pass
        finally:
            self.after(60000, self._auto_chart_refresh)

    def _refresh_chart(self):
        api_key = self.api_key or settings_get(self.conn, "polygon_api_key", "")
        if not api_key:
            return

        # prevent overlapping fetches
        if getattr(self, "_chart_busy", False):
            return
        self._chart_busy = True

        def worker():
            try:
                bars, err = self.engine.feed.get_minute_bars(api_key, minutes=self.chart_candles)
                try:
                    levels = load_levels(self.conn)  # DB off UI thread too
                except Exception:
                    levels = []

                if bars:
                    # make volume slope available to the engine
                    self.engine.recent_bars = list(bars[-180:])

                # hand off to UI thread
                self.ui_queue.put((CHART_WORKER_TAG, {"bars": bars, "levels": levels, "err": err}))
            finally:
                # release busy flag on UI thread
                self.after(0, lambda: setattr(self, "_chart_busy", False))

        threading.Thread(target=worker, daemon=True).start()

    def _mpl_color(self, c):
        """Map DB color strings/tuples to Matplotlib-friendly colors."""
        if not c:
            return "#2c82c9"
        if isinstance(c, str):
            c = c.strip()
            if c.startswith("#"):
                return c  # already a hex
            M = {
                "blue":"#1f77b4","b":"#1f77b4",
                "cyan":"#17becf","c":"#17becf","teal":"#20b2aa",
                "green":"#2ca02c","g":"#2ca02c",
                "red":"#d62728","r":"#d62728","magenta":"#e377c2","m":"#e377c2",
                "purple":"#9467bd","p":"#9467bd",
                "orange":"#ff7f0e","o":"#ff7f0e","gold":"#f0b400","yellow":"#f6c343","y":"#f6c343",
                "gray":"#7f7f7f","grey":"#7f7f7f","black":"#000000","k":"#000000","white":"#ffffff","w":"#ffffff",
            }
            return M.get(c.lower(), c)  # fall back to whatever it is
        if isinstance(c, (tuple, list)) and len(c) == 3:
            # Allow (R,G,B) in 0–255 or 0–1
            return tuple(v/255.0 for v in c) if max(c) > 1 else tuple(c)
        return "#2c82c9"

    def _render_chart_payload(self, payload):
        bars = payload.get("bars") or []
        err  = payload.get("err")
        levels = payload.get("levels") or []

        ax = self.ax
        ax.clear()

        if err or not bars:
            ax.set_title(f"{self.symbol} — no bars yet")
            self.canvas.draw_idle()
            return

        # candles
        for i, b in enumerate(bars):
            o, h, l, c = b["o"], b["h"], b["l"], b["c"]
            ax.plot([i, i], [l, h])  # wick
            bottom = min(o, c)
            height = abs(c - o) if abs(c - o) > 1e-9 else 0.0001
            ax.add_patch(plt.Rectangle((i - 0.3, bottom), 0.6, height, fill=True))

        # levels (from worker)
        for lv in load_levels(self.conn):
            color = self._mpl_color(lv.get("color"))
            ls    = "--" if lv.get("type") == "dashed" else "-"
            self.ax.axhline(
                float(lv["price"]),
                linestyle=ls,
                linewidth=1.6,
                color=color,         # <- explicit color
                alpha=0.95,
                zorder=15            # draw above candles
            )

        # NEW: overlay trade markers on top of candles
        self._draw_trade_markers(self.ax, bars)

        # y-bounds w/ margin
        prices = [b["o"] for b in bars] + [b["c"] for b in bars] + [b["h"] for b in bars] + [b["l"] for b in bars]
        pmin, pmax = min(prices), max(prices)
        margin = (pmax - pmin) * 0.05 if pmax > pmin else 1.0
        ax.set_ylim(pmin - margin, pmax + margin)

        ax.set_xlim(-1, len(bars) + 1)
        ax.set_title(f"{self.symbol} — last {len(bars)} minutes")
        ax.set_xlabel("minute bars (most recent on right)")
        ax.set_ylabel("price")
        self.figure.autofmt_xdate()
        self.figure.tight_layout()
        self.canvas.draw_idle()
    
    def _draw_trade_markers(self, ax, bars):
        # (a) optionally clear previous artists if you’re tracking them
        for a in getattr(self, "_trade_artists", []):
            try: a.remove()
            except: pass
        self._trade_artists = []

        # (b) fetch recent trades
        cur = self.conn.cursor()
        cur.execute("""
            SELECT id, ts_open, ts_close, symbol, side, entry, exit, stop, target
            FROM trades
            WHERE symbol=?
            ORDER BY id DESC LIMIT 300
        """, (self.symbol,))
        rows = cur.fetchall()

        # (c) map trade timestamps → bar indices
        def _parse_ts_ms(ts):
            if not ts: return None
            from datetime import datetime, timezone
            try:
                dt = datetime.fromisoformat(str(ts).replace("Z",""))
                if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                return int(dt.timestamp()*1000)
            except: return None

        def bar_ts(b):
            t = b.get("t") or b.get("ts")
            if isinstance(t, (int,float)): return int(t)
            if isinstance(t, str):
                try: return _parse_ts_ms(t)
                except: return None
            return None

        bar_times = [bar_ts(b) for b in bars]

        def idx_for(ts_ms):
            if ts_ms is None: return None
            # nearest by absolute diff if timestamps exist
            if any(bt is not None for bt in bar_times):
                best_i, best_d = None, float("inf")
                for i, bt in enumerate(bar_times):
                    if bt is None: continue
                    d = abs(bt - ts_ms)
                    if d < best_d: best_d, best_i = d, i
                return best_i
            # fallback: right edge
            return len(bars)-1

        # (d) draw
        for (tid, ts_o, ts_c, sym, side, entry, exit_px, stop, target) in rows:
            io = idx_for(_parse_ts_ms(ts_o))
            ic = idx_for(_parse_ts_ms(ts_c)) if ts_c else None

            # entry marker
            if entry is not None and io is not None:
                m = "^" if str(side).lower()=="long" else "v"
                art = ax.scatter([io], [float(entry)], marker=m, s=60, zorder=5)
                self._trade_artists.append(art)
                txt = ax.text(io, float(entry), f"#{tid}", fontsize=7, va="bottom", ha="left", alpha=0.7)
                self._trade_artists.append(txt)

            # stop/target short guides
            if io is not None and stop is not None:
                h = ax.hlines(float(stop), max(0, io-5), min(len(bars)-1, (ic or io)+5),
                            linestyles="dotted", linewidth=0.8, alpha=0.6)
                self._trade_artists.append(h)
            if io is not None and target is not None:
                h = ax.hlines(float(target), max(0, io-5), min(len(bars)-1, (ic or io)+5),
                            linestyles="dotted", linewidth=0.8, alpha=0.6)
                self._trade_artists.append(h)

            # exit + connector
            if exit_px is not None and ic is not None and io is not None:
                e = ax.scatter([ic], [float(exit_px)], marker="o", s=36, zorder=5)
                self._trade_artists.append(e)
                line, = ax.plot([io, ic], [float(entry), float(exit_px)], linestyle=":", linewidth=1.0, alpha=0.8)
                self._trade_artists.append(line)

    def _parse_ts_ms(self, ts_text):
        """Accepts 'YYYY-MM-DD HH:MM:SS' or ISO-like; returns epoch ms (int) or None."""
        try:
            # stored via utcnow(); usually naive UTC
            dt = datetime.fromisoformat(ts_text.replace("Z", ""))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except Exception:
            return None
    
    def _nearest_bar_index(self, bars, ts_ms):
        if ts_ms is None:
            return None
        # polygon/your feed usually has 't' (ms) or 'ts' (ms)
        arr = [b.get("t") or b.get("ts") for b in bars]
        if not arr or any(v is None for v in arr):
            return None
        # binary-ish search
        lo, hi = 0, len(arr)-1
        while lo < hi:
            mid = (lo + hi) // 2
            if arr[mid] < ts_ms:
                lo = mid + 1
            else:
                hi = mid
        # choose closer of lo and lo-1
        best = lo
        if lo > 0 and abs(arr[lo-1]-ts_ms) < abs(arr[lo]-ts_ms):
            best = lo-1
        return best

    # ---------- Live Tab ----------
    def _build_live_tab(self, frame):
        top = ttk.Frame(frame)
        top.pack(fill=tk.X, pady=6)
        ttk.Label(top, text="Symbol:", font=("Arial", 11, "bold")).pack(side=tk.LEFT)
        self.symbol_var = tk.StringVar(value=self.symbol)
        self.symbol_entry = ttk.Entry(top, textvariable=self.symbol_var, width=10)
        self.symbol_entry.pack(side=tk.LEFT, padx=6)
        ttk.Button(top, text="Set", command=self._apply_symbol).pack(side=tk.LEFT, padx=4)

        self.price_var = tk.StringVar(value="—")
        ttk.Label(top, text="Last Price:", font=("Arial", 11, "bold")).pack(side=tk.LEFT, padx=(20,4))
        ttk.Label(top, textvariable=self.price_var, foreground="#0a84ff").pack(side=tk.LEFT)
        ttk.Button(top, text="Test Price", command=self._test_price).pack(side=tk.LEFT, padx=8)

        self.status_var = tk.StringVar(value="Engine idle.")
        ttk.Label(frame, textvariable=self.status_var).pack(anchor="w", pady=4)
        btn_row = ttk.Frame(frame)
        btn_row.pack(fill=tk.X, pady=(0,6))
        ttk.Button(btn_row, text="Start Engine", command=self.start_engine).pack(side=tk.LEFT)
        ttk.Button(btn_row, text="Stop Engine", command=self.stop_engine).pack(side=tk.LEFT, padx=6)
       # add the Sim button to the same row as Start/Stop (btn_row uses pack)
        ttk.Button(
            btn_row,
            text="Sim last bars",
            command=lambda:self.simulate_last_bars(n=self.chart_candles, touch_limit=1, with_gates=True)

        ).pack(side=tk.LEFT, padx=6)
        ttk.Button(top, text="Sim last bars (gated)",
           command=lambda: self.simulate_last_bars(n=self.chart_candles,
                                                   touch_limit=1,
                                                   with_gates=True)
        ).pack(side=tk.LEFT, padx=4)
        ttk.Button(
            btn_row,
             text="Monte Carlo",
             command=lambda: self.simulate_monte_carlo(n=self.chart_candles, touch_limit=1, trials=500, with_gates=True)
        ).pack(side=tk.LEFT, padx=6)

        chart_frame = ttk.LabelFrame(frame, text="Candles (1-min, configurable)")
        chart_frame.pack(fill=tk.BOTH, expand=True, pady=6)
        self._build_chart(chart_frame)

        pos_frame = ttk.LabelFrame(frame, text="Open Position")
        pos_frame.pack(fill=tk.X, pady=6)
        self.pos_side = tk.StringVar(value="—")
        self.pos_entry = tk.StringVar(value="—")
        self.pos_stop = tk.StringVar(value="—")
        self.pos_target = tk.StringVar(value="—")
        ttk.Label(pos_frame, text="Side:").grid(row=0, column=0, sticky="w", padx=6, pady=3)
        ttk.Label(pos_frame, textvariable=self.pos_side).grid(row=0, column=1, sticky="w")
        ttk.Label(pos_frame, text="Entry:").grid(row=0, column=2, sticky="w", padx=6)
        ttk.Label(pos_frame, textvariable=self.pos_entry).grid(row=0, column=3, sticky="w")
        ttk.Label(pos_frame, text="Stop:").grid(row=1, column=0, sticky="w", padx=6)
        ttk.Label(pos_frame, textvariable=self.pos_stop).grid(row=1, column=1, sticky="w")
        ttk.Label(pos_frame, text="Target:").grid(row=1, column=2, sticky="w", padx=6)
        ttk.Label(pos_frame, textvariable=self.pos_target).grid(row=1, column=3, sticky="w")

        # --- Portfolio summary ---
        port_frame = ttk.LabelFrame(frame, text="Portfolio")
        port_frame.pack(fill=tk.X, pady=6)

        self.port_start = tk.StringVar(value=f"{self.starting_balance:.2f}")
        self.port_real = tk.StringVar(value="0.00")
        self.port_unreal = tk.StringVar(value="0.00")
        self.port_equity = tk.StringVar(value=f"{self.starting_balance:.2f}")
        self.port_wins = tk.StringVar(value="0")
        self.port_losses = tk.StringVar(value="0")

        r = 0
        ttk.Label(port_frame, text="Starting:").grid(row=r, column=0, sticky="w", padx=6)
        ttk.Label(port_frame, textvariable=self.port_start).grid(row=r, column=1, sticky="w")
        ttk.Label(port_frame, text="Realized P&L:").grid(row=r, column=2, sticky="w", padx=12)
        ttk.Label(port_frame, textvariable=self.port_real).grid(row=r, column=3, sticky="w")
        ttk.Label(port_frame, text="Unrealized P&L:").grid(row=r, column=4, sticky="w", padx=12)
        ttk.Label(port_frame, textvariable=self.port_unreal).grid(row=r, column=5, sticky="w")
        r += 1
        ttk.Label(port_frame, text="Equity:").grid(row=r, column=0, sticky="w", padx=6, pady=(4,0))
        ttk.Label(port_frame, textvariable=self.port_equity).grid(row=r, column=1, sticky="w", pady=(4,0))
        ttk.Label(port_frame, text="Wins:").grid(row=r, column=2, sticky="w", padx=12, pady=(4,0))
        ttk.Label(port_frame, textvariable=self.port_wins).grid(row=r, column=3, sticky="w", pady=(4,0))
        ttk.Label(port_frame, text="Losses:").grid(row=r, column=4, sticky="w", padx=12, pady=(4,0))
        ttk.Label(port_frame, textvariable=self.port_losses).grid(row=r, column=5, sticky="w", pady=(4,0))


    # ---------- Levels Tab ----------
    def _build_levels_tab(self, frame):
        desc = ttk.Label(frame, text="Enter levels for each color. Use one field per price. Click 'Save Levels' to activate.", foreground="#555")
        desc.pack(anchor="w")
        self.level_entries = {}
        container = ttk.Frame(frame)
        container.pack(fill=tk.BOTH, expand=True, pady=8)
        columns = [("blue","BLUE Levels"), ("orange","ORANGE Levels"), ("black","BLACK Levels"), ("teal","TEAL Levels")]
        for col_idx, (color, label) in enumerate(columns):
            col = ttk.LabelFrame(container, text=label, padding=8)
            col.grid(row=0, column=col_idx, padx=6, sticky="n")
            self._build_level_column(col, color)
        ttk.Button(frame, text="Save Levels", command=self._save_levels_clicked).pack(pady=8)

    def _build_level_column(self, parent, color):
        self.level_entries[color] = {"solid": [], "dashed": []}
        for kind in ("solid", "dashed"):
            lf = ttk.LabelFrame(parent, text=f"{kind.capitalize()} Lines", padding=6)
            lf.pack(fill=tk.BOTH, expand=True, pady=4)
            btn_row = ttk.Frame(lf)
            btn_row.pack(fill=tk.X, pady=2)
            add_btn = ttk.Button(btn_row, text="+", width=3, command=lambda c=color,k=kind: self._add_level_field(c,k, parent=lf))
            rm_btn = ttk.Button(btn_row, text="–", width=3, command=lambda c=color,k=kind: self._remove_level_field(c,k))
            ttk.Label(btn_row, text="Prices").pack(side=tk.LEFT)
            add_btn.pack(side=tk.RIGHT, padx=2)
            rm_btn.pack(side=tk.RIGHT, padx=2)
            initial = 4 if kind=="solid" else 5
            for _ in range(initial):
                self._add_level_field(color, kind, parent=lf)

    def _add_level_field(self, color, kind, parent):
        e = ttk.Entry(parent, width=10)
        e.pack(fill=tk.X, pady=2)
        self.level_entries[color][kind].append(e)

    def _remove_level_field(self, color, kind):
        arr = self.level_entries[color][kind]
        if arr:
            w = arr.pop()
            try:
                w.destroy()
            except:
                pass

    # ---------- Settings Tab ----------
    def _build_settings_tab(self, frame):
        ttk.Label(frame, text="Polygon.io API Key").grid(row=0, column=0, sticky="w", padx=4, pady=6)
        self.api_var = tk.StringVar(value=self.api_key or "")
        ttk.Entry(frame, textvariable=self.api_var, width=50, show="•").grid(row=0, column=1, sticky="w")
        ttk.Button(frame, text="Save API Key", command=self._save_api_key).grid(row=0, column=2, padx=6)

        ttk.Label(frame, text="Chart candles (1-min)").grid(row=6, column=0, sticky="w", padx=4)
        self.chart_candles_var = tk.StringVar(value=str(self.chart_candles))
        ttk.Entry(frame, textvariable=self.chart_candles_var, width=8).grid(row=6, column=1, sticky="w")

        ttk.Label(frame, text="Starting Balance ($)").grid(row=7, column=0, sticky="w", padx=4)
        self.portfolio_start_var = tk.StringVar(value=f"{self.starting_balance:.2f}")
        ttk.Entry(frame, textvariable=self.portfolio_start_var, width=12).grid(row=7, column=1, sticky="w") 

        ttk.Label(frame, text="Contact Proximity ($)").grid(row=1, column=0, sticky="w", padx=4, pady=6)
        self.prox_var = tk.StringVar(value=str(self.engine.CONTACT_PROX))
        ttk.Entry(frame, textvariable=self.prox_var, width=8).grid(row=1, column=1, sticky="w")
        ttk.Label(frame, text="Min Confidence").grid(row=1, column=2, sticky="w", padx=(16,4))
        self.minprob_var = tk.StringVar(value=str(self.engine.Q_MIN_PROB))
        ttk.Entry(frame, textvariable=self.minprob_var, width=8).grid(row=1, column=3, sticky="w")

        ttk.Label(frame, text="Cooldown (s)").grid(row=2, column=0, sticky="w", padx=4, pady=6)
        self.cooldown_var = tk.StringVar(value=str(self.engine.Q_SIGNAL_COOLDOWN_S))
        ttk.Entry(frame, textvariable=self.cooldown_var, width=8).grid(row=2, column=1, sticky="w")

        self.ah_var = tk.BooleanVar(value=self.allow_ah)
        ttk.Checkbutton(frame, text="Allow After-Hours (A/H) trading and prices", variable=self.ah_var).grid(row=3, column=0, columnspan=2, sticky="w", padx=4, pady=6)

        ttk.Button(frame, text="Save Engine Settings", command=self._save_engine_settings).grid(row=4, column=0, pady=8, sticky="w")

        ttk.Label(frame, text="Auto Retrain (HH:MM, 24h)").grid(row=5, column=0, sticky="w", padx=4)
        self.retrain_time_var = tk.StringVar(value=settings_get(self.conn, 'retrain_time', '02:00'))
        ttk.Entry(frame, textvariable=self.retrain_time_var, width=8).grid(row=5, column=1, sticky="w")
        self.auto_retrain_var = tk.BooleanVar(value=(settings_get(self.conn, 'auto_retrain', '1')=='1'))
        ttk.Checkbutton(frame, text="Enable Daily Retrain", variable=self.auto_retrain_var).grid(row=5, column=2, sticky="w")
        ttk.Button(frame, text="Retrain Now", command=self._retrain_now).grid(row=5, column=3, padx=6)

    # ---------- UI handlers ----------
    def _apply_symbol(self):
        sym = self.symbol_var.get().strip().upper()
        if not sym:
            return
        self.symbol = sym
        settings_set(self.conn, "symbol", sym)
        self.engine.symbol = sym
        self.engine.feed = PriceFeed(sym)
        self._log_ui("MISC", "SYMBOL_SET", f"Symbol set to {sym}")
        self._refresh_chart()

    def _save_api_key(self):
        self.api_key = self.api_var.get().strip()
        settings_set(self.conn, "polygon_api_key", self.api_key)
        messagebox.showinfo("Saved", "Polygon.io API key saved.")
        self._log_ui("SETTINGS", "APIKEY_SAVED", "API key saved.")

    def _save_engine_settings(self):
        try:
            prox = float(self.prox_var.get())
            minp = float(self.minprob_var.get())
            cd = int(self.cooldown_var.get())
        except Exception as e:
            messagebox.showerror("Error", f"Invalid values: {e}")
            return
        settings_set(self.conn, "CONTACT_PROX", str(prox))
        settings_set(self.conn, "Q_MIN_PROB", str(minp))
        settings_set(self.conn, "Q_SIGNAL_COOLDOWN", str(cd))
        settings_set(self.conn, "allow_after_hours", "1" if self.ah_var.get() else "0")
        settings_set(self.conn, "retrain_time", self.retrain_time_var.get().strip())
        settings_set(self.conn, "auto_retrain", "1" if self.auto_retrain_var.get() else "0")
        settings_set(self.conn, "chart_candles", self.chart_candles_var.get().strip())
        try:
            self.chart_candles = max(20, int(self.chart_candles_var.get()))
        except Exception:
            self.chart_candles = 120
        # Save starting balance
        sb = (self.portfolio_start_var.get() or "").strip()
        try:
            self.starting_balance = float(sb)
        except Exception:
            pass
        settings_set(self.conn, "portfolio_start", f"{self.starting_balance:.2f}")
        self.engine.CONTACT_PROX = prox
        self.engine.Q_MIN_PROB = minp
        self.engine.Q_SIGNAL_COOLDOWN_S = cd
        self.allow_ah = self.ah_var.get()
        messagebox.showinfo("Saved", "Engine settings saved.")
        self._log_ui("SETTINGS", "ENGINE_SAVED", f"prox={prox} minp={minp} cooldown={cd} ah={self.allow_ah}")

    def _save_levels_clicked(self):
        levels = []
        for color, kinds in self.level_entries.items():
            for kind, widgets in kinds.items():
                for idx, e in enumerate(widgets):
                    txt = e.get().strip()
                    if not txt:
                        continue
                    try:
                        price = float(txt)
                    except:
                        messagebox.showerror("Invalid", f"{color} {kind} value #{idx+1} is not a number: {txt}")
                        return
                    levels.append({"color": color, "type": kind, "index": idx, "price": price})
        replace_levels(self.conn, levels)
        self.engine.reload_levels()
        messagebox.showinfo("Levels Saved", f"Saved {len(levels)} levels. Engine will use them on next tick.")
        self._log_ui("LEVELS", "LEVELS_SAVED", f"{len(levels)} levels saved.")
        self._refresh_chart()

    def _load_levels_into_ui(self):
        # prebuild entry widgets first
        # Create structure identical to build
        pass  # entries are created in _build_levels_tab; we fill values below

        stored = load_levels(self.conn)
        # fill values matching index
        for lv in stored:
            # find the correct widget; expand if needed
            arr = self.level_entries.get(lv["color"], {}).get(lv["type"], [])
            idx = lv["index"]
            while len(arr) <= idx:
                # find a parent frame to pass to add
                # search through notebook tab
                # this is simplified: if missing, skip filling
                break
            try:
                if idx < len(arr):
                    arr[idx].delete(0, tk.END)
                    arr[idx].insert(0, f"{lv['price']:.2f}")
            except Exception:
                pass

    # ---------- Engine Loop ----------
    def start_engine(self):
        if self.running: return
        self.running = True
        self.start_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        self.engine_thread = threading.Thread(target=self._engine_loop, daemon=True)
        self.engine_thread.start()
        self.status_var.set("Engine running...")

    def stop_engine(self):
        if not self.running: return
        self.running = False
        self.start_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
        self.status_var.set("Engine stopped.")
        self._save_policy()

    def _engine_loop(self):
        last_levels_reload = 0
        while self.running:
            try:
                api_key = self.api_key or settings_get(self.conn, "polygon_api_key", "")
                if not api_key:
                    audit(self.conn, "FEED", MISSING_API_KEY, "No API key set.")
                    self._log_ui("FEED", MISSING_API_KEY, "No API key set.")
                    time.sleep(1.0)
                    continue

                status = self.engine.feed.get_market_status(api_key)
                market_open = (status.session == "open") or (self.allow_ah and status.session == "extended-hours")

                if market_open:
                    price, t_ms, err = self.engine.feed.get_last_trade(api_key)
                    if err:
                        audit(self.conn, "FEED", "FEED_ERR", f"Price fetch error: {err}")
                        self._log_ui("FEED", "FEED_ERR", f"{err}")
                        time.sleep(1.0)
                        continue

                    prev_price = self.engine.state.last_price
                    now_ms = int(time.time() * 1000)

                    # INSERT ↓↓↓
                    volume_current = 0.0
                    self.engine.ingest_tick(int(t_ms), float(price), float(volume_current))
                    # INSERT ↑↑↑

                    # Evaluate using prev -> current BEFORE updating state
                    ok, code, msg, extras = self.engine.evaluate_entry(
                        price_current=price,
                        prev_price=prev_price,
                        now_ms=now_ms,
                        api_key_present=bool(api_key)
                    )

                    # Evaluate using prev -> current BEFORE updating state
                    ok, code, msg, extras = self.engine.evaluate_entry(
                        price_current=price,
                        prev_price=prev_price,
                        now_ms=now_ms,
                        api_key_present=bool(api_key)
                    )

                    # Now update state for the next tick
                    self.engine.state.last_ts_ms = t_ms
                    self.engine.state.last_price = price
                    if prev_price is not None and price != prev_price:
                        self.engine.state.last_direction = "up" if price > prev_price else "down"

                    self.ui_queue.put(("price", price))
                else:
                    pclose, err = self.engine.feed.get_prev_close(api_key)
                    if pclose is not None:
                        self.ui_queue.put(("price", pclose))
                    self.status_var.set(f"Market closed ({status.session}). Using official close.")
                    time.sleep(2.0)
                    continue

                if self.engine.state.open_trade_id is not None:
                    cur = self.conn.cursor()
                    cur.execute("SELECT side, stop, target FROM trades WHERE id=?", (self.engine.state.open_trade_id,))
                    row = cur.fetchone()
                    # --- FIX: Fetch recent bars before checking for escalation ---
                    # Fetch the last ~10 minutes of bar data to analyze momentum.
                    recent_bars, _ = self.engine.feed.get_minute_bars(self.api_key, minutes=10)
                    # --- END FIX ---
                    if row:
                        side, stop, target = row

                        if side == "long":
                            if price <= stop:
                                self.engine.close_trade(self.engine.state.open_trade_id, price, "STOP")
                                audit(self.conn, "EXIT", "STOP", f"Stop hit at {price:.2f}", {})
                                self._log_ui("EXIT", "STOP", f"Stop hit @ {price:.2f}")
                                self.engine.state.set_cooldown(now_ms, self.engine.Q_SIGNAL_COOLDOWN_S)
                                self._refresh_portfolio_ui()
                                continue  # move to next loop tick

                            elif price >= target:
                                # Try escalation; if it doesn't escalate, book the win
                                escalated, meta = self.engine.maybe_escalate_on_target(price, recent_bars)
                                if not escalated:
                                    self.engine.close_trade(self.engine.state.open_trade_id, price, "TARGET")
                                    audit(self.conn, "EXIT", "TARGET", f"Target hit at {price:.2f}", {})
                                    self._log_ui("EXIT", "TARGET", f"Target hit @ {price:.2f}")
                                    self.engine.state.set_cooldown(now_ms, self.engine.Q_SIGNAL_COOLDOWN_S)
                                    self._refresh_portfolio_ui()
                                continue

                        elif side == "short":
                            if price >= stop:
                                self.engine.close_trade(self.engine.state.open_trade_id, price, "STOP")
                                audit(self.conn, "EXIT", "STOP", f"Stop hit at {price:.2f}", {})
                                self._log_ui("EXIT", "STOP", f"Stop hit @ {price:.2f}")
                                self.engine.state.set_cooldown(now_ms, self.engine.Q_SIGNAL_COOLDOWN_S)
                                self._refresh_portfolio_ui()
                                continue

                            elif price <= target:
                                escalated, meta = self.engine.maybe_escalate_on_target(price, recent_bars)
                                if not escalated:
                                    self.engine.close_trade(self.engine.state.open_trade_id, price, "TARGET")
                                    audit(self.conn, "EXIT", "TARGET", f"Target hit at {price:.2f}", {})
                                    self._log_ui("EXIT", "TARGET", f"Target hit @ {price:.2f}")
                                    self.engine.state.set_cooldown(now_ms, self.engine.Q_SIGNAL_COOLDOWN_S)
                                    self._refresh_portfolio_ui()
                                continue

                    self._refresh_position_ui()
                else:
                    if now_ms - last_levels_reload > 5000:
                        self.engine.reload_levels()
                        last_levels_reload = now_ms

                    if not ok:
                        if code not in (TOO_FAR,):
                            audit(self.conn, "ENTRY", code, msg, {"price": price, **extras})
                            self._log_ui("ENTRY", code, msg)
                            # QVoice narration for skipped entries
                            try:
                                self.qvoice.narrate_entry_evaluation(
                                    symbol=self.symbol,
                                    code=code,
                                    level_type=(extras.get("level")[1] if extras.get("level") else None),
                                    direction=extras.get("dir") or extras.get("direction"),
                                    proximity=(extras.get("dist") if extras.get("dist") is not None else abs(price - extras.get("level_price", price))),
                                    confidence=extras.get("conf", 0.0),
                                    min_conf=self.engine.Q_MIN_PROB,
                                    ml_prob=extras.get("ml_prob"),
                                    min_prob=self.engine.Q_MIN_PROB,
                                    volume_trend=None,
                                    touches=extras.get("touch_count"),
                                    notes=msg
                                )
                            except Exception:
                                pass
                    else:
                        tid = None  # replaced open block
                        # --- Entry Planner + Policy gating ---
                        try:
                            # Pull nearest level and side from decision extras
                            lvl_price = float(extras["level_price"])
                            side = extras["side"]  # "long" or "short"

                            # Infer approach consistent with side
                            approach = "from_below" if side == "long" else "from_above"

                            # Minutes since open
                            now_ts = time.time()
                            try:
                                mins_open = self._minutes_since_open(now_ts)
                            except Exception:
                                mins_open = 0

                            # Features
                            proximity_abs = abs(price - lvl_price)
                            # Confluence: cluster count near this level
                            try:
                                levels = load_levels(self.conn)
                            except Exception:
                                levels = []
                            cluster = [lv for lv in levels if abs(float(lv.get("price", 0.0)) - lvl_price) <= 0.6]
                            has_confluence = (len(cluster) > 1)
                            # Volume trend placeholder (0.0 neutral if you’re not tracking bars here)
                            volume_trend = 0.0

                            # Build features & score
                            if hasattr(self, "policy") and hasattr(self, "_build_features_for_policy"):
                                x = self._build_features_for_policy(
                                    proximity_abs=proximity_abs,
                                    volume_trend=volume_trend,
                                    approach=approach,
                                    confluence=has_confluence,
                                    minutes_since_open=mins_open,
                                )
                                scores = self.policy.score_entry(x)
                                chosen = "go_long" if side == "long" else "go_short"
                                pass_gate = (scores.get(chosen, 0.5) >= 0.60) and (scores.get("skip", 0.0) < 0.55)
                            else:
                                x, scores, pass_gate = None, {}, False
                            
                            # Toggle to bypass the UI policy gate (defaults to ON/bypass so you can trade)
                            disable_policy_gate = settings_get(self.conn, "DISABLE_POLICY_GATE", "0") == "1"

                            if disable_policy_gate:
                                pass_gate = True

                            if not pass_gate:
                                # Log a structured policy_skip so it shows up with details
                                self.engine._insert_policy_event("entry", "policy_skip", {
                                    "reason": "ONLINE_POLICY",
                                    "prox": float(proximity_abs),
                                    "level_price": float(lvl_price),
                                    "side": side,
                                    "approach": approach,
                                    "scores": {k: round(v, 3) for k, v in (scores or {}).items()},
                                    "conf": float(extras.get("conf", 0.0)),
                                    "touch_count": int(extras.get("touch_count", 1)),
                                })
                                self._log_ui("ENTRY", "POLICY_SKIP",
                                            f"ONLINE_POLICY veto {side.upper()} @ {price:.2f} (lvl {lvl_price:.2f})")
                                continue  # do not try to open the trade

                            if pass_gate:
                                tid = self.engine.open_trade(
                                    extras["side"], price, extras["stop"], extras["target"],
                                    f"contact@{lvl_price:.2f} conf={extras['conf']:.2f}"
                                )
                                audit(self.conn, "ENTRY", "EXECUTE", f"ENTRY {extras['side']} @ {price:.2f}", extras)
                                # QVoice narration for executed entry
                                try:
                                    self.qvoice.narrate_entry_evaluation(
                                        symbol=self.symbol,
                                        code="ENTRY_EXECUTE",
                                        level_type=(extras.get("level")[1] if extras.get("level") else None),
                                        direction=("from_below" if extras.get("side")=="long" else "from_above"),
                                        proximity=abs(price - extras.get("level_price", price)),
                                        confidence=extras.get("conf", 0.0),
                                        min_conf=self.engine.Q_MIN_PROB,
                                        ml_prob=extras.get("ml_prob"),
                                        min_prob=self.engine.Q_MIN_PROB,
                                        volume_trend=None,
                                        touches=extras.get("touch_count"),
                                        notes="Conditions aligned with policy and thresholds."
                                    )
                                except Exception:
                                    pass
                                self._log_ui(
                                    "ENTRY", "EXECUTE",
                                    f"{extras['side'].upper()} @ {price:.2f} | stop {extras['stop']:.2f} | target {extras['target']:.2f}"
                                )
                                # Log policy event to label on close
                                try:
                                    if x is not None:
                                        cur = self.conn.cursor()
                                        action = "go_long" if extras["side"] == "long" else "go_short"
                                        cur.execute(
                                            "INSERT INTO policy_events (ts, phase, action, features_json, trade_id, notes) "
                                            "VALUES (?,?,?,?,?,?)",
                                            (utcnow(), "entry", action, json.dumps({"x": x}), tid,
                                             f"level={lvl_price:.2f}; conf={extras['conf']:.2f}")
                                        )
                                        self.conn.commit()
                                except Exception:
                                    pass

                                key = tuple(extras["level"])
                                self._refresh_position_ui()

                        except Exception as ex:
                            # Fallback to original open so we don't halt trading
                            try:
                                tid = self.engine.open_trade(extras["side"], price, extras["stop"], extras["target"],
                                                             f"contact@{extras['level_price']:.2f} conf={extras['conf']:.2f}")
                                audit(self.conn, "ENTRY", "EXECUTE", f"ENTRY {extras['side']} @ {price:.2f}", extras)
                                self._log_ui(
                                    "ENTRY", "EXECUTE",
                                    f"{extras['side'].upper()} @ {price:.2f} | stop {extras['stop']:.2f} | target {extras['target']:.2f}"
                                )
                                key = tuple(extras["level"])
                                self._refresh_position_ui()
                            except Exception as ex2:
                                audit(self.conn, "ENTRY", "OPEN_ERR", f"{ex2}", {"price": price, **extras})
                                self._log_ui("ENTRY", "OPEN_ERR", str(ex2))
                            # QVoice narration for policy veto
                            try:
                                self.qvoice.narrate_entry_evaluation(
                                    symbol=self.symbol,
                                    code="POLICY_SKIP",
                                    level_type=None,
                                    direction=("from_below" if side=="long" else "from_above"),
                                    proximity=proximity_abs,
                                    confidence=extras.get("conf", 0.0),
                                    min_conf=self.engine.Q_MIN_PROB,
                                    ml_prob=scores.get("go_long" if side=="long" else "go_short"),
                                    min_prob=0.60,
                                    volume_trend=None,
                                    touches=extras.get("touch_count"),
                                    notes=f"scores={ {k: round(v,2) for k,v in (scores or {}).items()} }"
                                )
                            except Exception:
                                pass

                time.sleep(0.7)
            except Exception as e:
                audit(self.conn, "MISC", "ENGINE_ERR", f"{e}", {})
                self._log_ui("MISC", "ENGINE_ERR", str(e))
                time.sleep(1.0)

    # ---------- UI updating helpers ----------
    def _start_price_loop(self):
        self.after(200, self._drain_ui_queue)

    def _drain_ui_queue(self):
        try:
            while True:
                kind, payload = self.ui_queue.get_nowait()
                if kind == CHART_WORKER_TAG:
                    self._render_chart_payload(payload)
                # ...handle other kinds...
        except queue.Empty:
            pass
        finally:
            self.after(200, self._drain_ui_queue)

    def _refresh_position_ui(self):
        cur = self.conn.cursor()
        if self.engine.state.open_trade_id is None:
            self.pos_side.set("—"); self.pos_entry.set("—")
            self.pos_stop.set("—"); self.pos_target.set("—")
            return
        cur.execute("SELECT side, entry, stop, target FROM trades WHERE id=?", (self.engine.state.open_trade_id,))
        row = cur.fetchone()
        if row:
            side, entry, stop, target = row
            self.pos_side.set(side.upper())
            self.pos_entry.set(f"{entry:.2f}")
            self.pos_stop.set(f"{stop:.2f}")
            self.pos_target.set(f"{target:.2f}")

    def _minutes_since_open(self, now_ts: float) -> int:
        try:
            t = datetime.fromtimestamp(now_ts)
            open_t = t.replace(hour=9, minute=30, second=0, microsecond=0)
            return max(0, int((t - open_t).total_seconds() // 60))
        except Exception:
            return 0

    def _build_features_for_policy(self, *, proximity_abs, volume_trend, approach, confluence, minutes_since_open):
        # Pass-through to OnlinePolicy's feature builder (keeps it consistent)
        return self.policy.build_features(
            proximity_abs=proximity_abs,
            volume_trend=volume_trend,
            approach=approach,
            confluence=confluence,
            minutes_since_open=minutes_since_open
        )

    def _portfolio_snapshot(self, last_price=None):
        """
        equity = starting_balance + realized_pnl + unrealized_pnl
        realized_pnl: sum of closed trades' pnl from `trades`
        unrealized_pnl: current open trade vs latest price (engine holds one at a time)
        """
        cur = self.conn.cursor()
        row = cur.execute("SELECT COALESCE(SUM(pnl),0) FROM trades WHERE ts_close IS NOT NULL").fetchone()
        realized = float(row[0] or 0.0)

        unreal = 0.0
        if self.engine.state.open_trade_id is not None:
            trow = cur.execute("SELECT side, entry FROM trades WHERE id=?", (self.engine.state.open_trade_id,)).fetchone()
            if trow:
                side, entry = trow
                if last_price is None:
                    try:
                        last_price = float(self.price_var.get())
                    except Exception:
                        last_price = None
                if last_price is not None:
                    if side == "long":
                        unreal = (last_price - float(entry))
                    else:
                        unreal = (float(entry) - last_price)

        equity = self.starting_balance + realized + unreal
        return {
            "starting": self.starting_balance,
            "realized": realized,
            "unrealized": unreal,
            "equity": equity
        }

    def _refresh_portfolio_ui(self):
        # Closed PnL + win/loss counts
        cur = self.conn.cursor()
        cur.execute(
            "SELECT COALESCE(SUM(pnl),0), "
            "SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN pnl<=0 THEN 1 ELSE 0 END) "
            "FROM trades WHERE ts_close IS NOT NULL"
        )
        total_pnl, wins, losses = cur.fetchone()
        total_pnl = float(total_pnl or 0.0)

        try:
            last_price = float(self.price_var.get())
        except Exception:
            last_price = None
        snap = self._portfolio_snapshot(last_price=last_price)

        self.port_start.set(f"{snap['starting']:.2f}")
        self.port_real.set(f"{snap['realized']:.2f}")
        self.port_unreal.set(f"{snap['unrealized']:.2f}")
        self.port_equity.set(f"{snap['equity']:.2f}")
        self.port_wins.set(str(wins or 0))
        self.port_losses.set(str(losses or 0))

    def _log_ui(self, topic: str, code: str, message: str, extra: dict | None = None):
        ts = time.strftime("%H:%M:%S")
        line = f"[{ts}] {topic}:{code} — {message}"

        # Avoid Tk __getattr__ surprises: grab the attribute directly from __dict__
        widget = getattr(self, "log_text", None)

        # If UI isn't ready or widget isn't a Text, print to stdout and bail
        try:
            import tkinter as tk  # local import to avoid circulars in some layouts
            if widget is None or not isinstance(widget, tk.Text):
                try:
                    print(line)
                    if extra:
                        import json
                        print(json.dumps(extra, indent=2))
                except Exception:
                    pass
                return
        except Exception:
            # Even importing tk failed? just print
            try:
                print(line)
            except Exception:
                pass
            return

        # Safe UI write
        try:
            widget.configure(state=tk.NORMAL)
            widget.insert(tk.END, line + "\n")
            if extra:
                import json
                widget.insert(tk.END, json.dumps(extra, indent=2) + "\n")
            widget.see(tk.END)
            widget.configure(state=tk.DISABLED)
        except Exception:
            try:
                print(line)
            except Exception:
                pass

    def on_close(self):
        self.stop_engine()
        try: self.conn.close()
        except: pass
        self.destroy()

    def simulate_monte_carlo(self, n=600, touch_limit=1, trials=500, with_gates=True,
                         entry_slip_std=0.01, level_jitter_std=0.02,
                         stop_slip_std=0.00, target_slip_std=0.00, seed=None):
        """
        Monte Carlo robustness test on last N minute bars.
        - Uses your current levels, pads (STOP/TP), CONTACT_PROX, and (optionally) gates.
        - Adds realistic noise: entry slippage, level jitter (affects stop/target), optional stop/TP slip.
        - If both stop and target sit inside the same bar, resolves with a distance-weighted coin flip.
        Reports: Total R distribution, VaR(5%), CVaR(5%), worst drawdown, win/loss/open counts (mean).
        """
        try:
            import time
            from statistics import mean, median, pstdev
            if seed is not None:
                random.seed(seed)

            # ---------- Pull data & knobs ----------
            bars   = list(getattr(self.engine, "recent_bars", []))[-n:]
            levels = list(getattr(self.engine, "levels_cache", []))
            if not bars or not levels:
                messagebox.showinfo("Monte Carlo", "Need bars and levels. Refresh the chart, then try again.")
                return

            prox = float(self.engine.CONTACT_PROX)
            sp   = float(self.engine.STOP_PADDING)
            tp   = float(self.engine.TP_PADDING)

            # ---------- Build candidate signals (same detector as your sim) ----------
            def nearest_level(px: float) -> float:
                L = min(levels, key=lambda lv: abs(float(lv["price"]) - px))
                return float(L["price"])

            candidates = []
            touch_counts = {}
            prev_c = None

            # use the real engine gate if requested (exactly like live)
            save_override = getattr(self.engine, "USE_BLEND_OVERRIDE", None)
            # leave blend/AND mode to whatever your engine is set to globally
            try:
                for i, b in enumerate(bars):
                    o, h, l, c = float(b["o"]), float(b["h"]), float(b["l"]), float(b["c"])
                    if prev_c is None:
                        prev_c = c
                        continue

                    lvl = nearest_level(c)
                    proximity_abs = abs(lvl - c)
                    if proximity_abs > prox:
                        prev_c = c
                        continue

                    side = "long" if c > prev_c else "short"

                    k = round(lvl, 4)
                    tc = touch_counts.get(k, 0) + 1
                    if tc > touch_limit:
                        prev_c = c
                        continue
                    touch_counts[k] = tc

                    # Optionally apply the full gate stack
                    if with_gates:
                        now_ms = int(time.time() * 1000) + i * 60_000
                        self.engine.state.last_ts_ms = now_ms  # ensure "fresh"
                        allowed, code, reason, extras = self.engine.evaluate_entry(
                            price_current=c, prev_price=prev_c, now_ms=now_ms, api_key_present=True
                        )
                        if not allowed:
                            prev_c = c
                            continue
                        # Use engine's scaffold for stop/target if available
                        lvl = float(extras.get("level_price", lvl))
                        side = extras.get("side", side)

                    # Risk scaffold off the (possibly updated) level
                    if side == "long":
                        stop   = lvl - sp
                        target = lvl + tp
                    else:
                        stop   = lvl + sp
                        target = lvl - tp

                    candidates.append({
                        "i": i, "side": side, "lvl": float(lvl),
                        "entry": float(c), "stop": float(stop), "target": float(target)
                    })
                    prev_c = c
            finally:
                self.engine.USE_BLEND_OVERRIDE = save_override

            if not candidates:
                messagebox.showinfo("Monte Carlo", "No candidates found with current settings.")
                return

            # ---------- Run trials ----------
            def walk_outcome(cand, rand):
                """Simulate one trade outcome with noisy params; return (R, outcome)."""
                side = cand["side"]
                # jitter the level and (optionally) stop/target and entry
                lvl_j = cand["lvl"] + rand.gauss(0.0, level_jitter_std)
                entry = cand["entry"] + rand.gauss(0.0, entry_slip_std)

                if side == "long":
                    stop   = lvl_j - sp + rand.gauss(0.0, stop_slip_std)
                    target = lvl_j + tp + rand.gauss(0.0, target_slip_std)
                else:
                    stop   = lvl_j + sp + rand.gauss(0.0, stop_slip_std)
                    target = lvl_j - tp + rand.gauss(0.0, target_slip_std)

                risk   = abs(entry - stop) or 1e-9
                reward = abs(target - entry)

                # walk forward bars to see hit order
                for j in range(cand["i"] + 1, len(bars)):
                    hh, ll = float(bars[j]["h"]), float(bars[j]["l"])
                    stop_hit   = (ll <= stop) if side == "long" else (hh >= stop)
                    target_hit = (hh >= target) if side == "long" else (ll <= target)

                    if stop_hit and target_hit:
                        # distance-weighted coin flip for tie (who is "closer" to entry intrabar)
                        up_span   = max(0.0, hh - entry)
                        down_span = max(0.0, entry - ll)
                        p_target_first = up_span / (up_span + down_span + 1e-9)
                        if rand.random() < p_target_first:
                            return (reward / risk, "tp")
                        else:
                            return (-1.0, "stop")
                    elif target_hit:
                        return (reward / risk, "tp")
                    elif stop_hit:
                        return (-1.0, "stop")

                return (0.0, "open")

            totals, worst_dds, win_counts, loss_counts, open_counts = [], [], [], [], []
            rng = random.Random(seed)  # independent RNG

            for t in range(trials):
                eq = 0.0
                peak = 0.0
                max_dd = 0.0
                wins = losses = opens = 0

                for cand in candidates:
                    r, outcome = walk_outcome(cand, rng)
                    eq += r
                    peak = max(peak, eq)
                    max_dd = min(max_dd, eq - peak)  # negative number
                    if outcome == "tp":   wins += 1
                    elif outcome == "stop": losses += 1
                    else:                 opens += 1

                totals.append(eq)
                worst_dds.append(max_dd)
                win_counts.append(wins)
                loss_counts.append(losses)
                open_counts.append(opens)

            # ---------- Summarize ----------
            totals_sorted = sorted(totals)
            p05_idx = max(0, int(0.05 * len(totals_sorted)) - 1)
            var_05  = totals_sorted[p05_idx] if totals_sorted else 0.0
            cvar_05 = mean(totals_sorted[:p05_idx+1]) if p05_idx >= 0 else 0.0

            msg = (
                f"Candidates: {len(candidates)} | Trials: {trials}\n"
                f"Total R — mean {mean(totals):+.2f}, median {median(totals):+.2f}, "
                f"stdev {pstdev(totals):.2f}\n"
                f"Best {max(totals):+.2f} | Worst {min(totals):+.2f}\n"
                f"VaR(5%) {var_05:+.2f} | CVaR(5%) {cvar_05:+.2f}\n"
                f"Max drawdown (R) — mean {mean(worst_dds):+.2f}, worst {min(worst_dds):+.2f}\n"
                f"Avg counts — wins {mean(win_counts):.1f}, losses {mean(loss_counts):.1f}, open {mean(open_counts):.1f}"
            )
            audit(self.conn, "MC", "SUMMARY", msg)
            self._log_ui("MC", "SUMMARY", msg)
            messagebox.showinfo("Monte Carlo", msg)

        except Exception as e:
            import traceback
            audit(self.conn, "MC", "ERROR", str(e))
            audit(self.conn, "MC", "ERROR_TRACE", traceback.format_exc())
            try:
                messagebox.showerror("Monte Carlo error", str(e))
            except Exception:
                pass

    def simulate_last_bars(self, n=1200, touch_limit=2, with_gates=False,
                       policy_min=0.60, policy_skip_max=0.55):
        """
        What-if sim on the last N minute bars already in memory (self.engine.recent_bars)
        using current levels and the engine's STOP/TP paddings.
        Reports wins/losses, Total/Avg R, and Max Profit/Loss.
        """
        try:
            bars = list(getattr(self.engine, "recent_bars", []))[-n:]
            levels = list(getattr(self.engine, "levels_cache", []))
            if not bars or not levels:
                messagebox.showinfo("Sim", "Need bars and levels first. Refresh the chart, then try again.")
                return

            prox = float(self.engine.CONTACT_PROX)
            sp   = float(self.engine.STOP_PADDING)
            tp   = float(self.engine.TP_PADDING)

            touch_counts = {}
            trades = []

            def nearest_level(px: float) -> float:
                L = min(levels, key=lambda lv: abs(float(lv["price"]) - px))
                return float(L["price"])

            prev_c = None
            for i, b in enumerate(bars):
                o, h, l, c = float(b["o"]), float(b["h"]), float(b["l"]), float(b["c"])
                if prev_c is None:
                    prev_c = c
                    continue

                lvl = nearest_level(c)
                proximity_abs = abs(lvl - c)
                if proximity_abs > prox:
                    prev_c = c
                    continue

                # side by approach (same as engine)
                side = "long" if c > prev_c else "short"

                # ---------- GATES (use the engine's real gate) ----------
                if with_gates:
                    # fabricate a fresh timestamp so the "PRICE_STALE" check passes
                    now_ms = int(time.time() * 1000) + i * 60_000
                    self.engine.state.last_ts_ms = now_ms  # make price "fresh"

                    # Run the real gate (policy + veto + ML) exactly like live
                    allowed, code, reason, details = self.engine.evaluate_entry(
                        price_current=c,
                        prev_price=prev_c,
                        now_ms=now_ms,
                        api_key_present=True,   # ok for sim
                    )
                    if not allowed:
                        # helpful breadcrumb in the Log tab
                        audit(self.conn, "SIM", f"GATE_SKIP:{code}",
                            f"{reason} prox={abs(lvl - c):.03f}")
                        prev_c = c
                        continue
                # --------------------------------------------------------
                        
                k = round(lvl, 4)
                tc = touch_counts.get(k, 0) + 1
                if tc > touch_limit:
                    prev_c = c
                    continue
                touch_counts[k] = tc

                # risk scaffold off the level
                if side == "long":
                    stop   = lvl - sp
                    target = lvl + tp
                else:
                    stop   = lvl + sp
                    target = lvl - tp

                entry = c

                # walk forward to see which hits first
                hit = None
                for j in range(i + 1, len(bars)):
                    hh, ll = float(bars[j]["h"]), float(bars[j]["l"])
                    if side == "long":
                        if ll <= stop:   hit = ("stop", j); break
                        if hh >= target: hit = ("tp",   j); break
                    else:
                        if hh >= stop:   hit = ("stop", j); break
                        if ll <= target: hit = ("tp",   j); break

                if hit is None:
                    outcome = "open"
                    exit_px = float(bars[-1]["c"])
                    r = 0.0
                else:
                    outcome = hit[0]
                    exit_px = target if outcome == "tp" else stop
                    risk    = abs(entry - stop) or 1e-9
                    reward  = abs(target - entry)
                    r       = (reward / risk) if outcome == "tp" else -1.0

                trades.append({
                    "i": i, "side": side, "lvl": round(lvl, 2),
                    "entry": round(entry, 2), "stop": round(stop, 2), "target": round(target, 2),
                    "exit": round(exit_px, 2), "outcome": outcome, "R": round(r, 2),
                    "prox": round(proximity_abs, 3), "touch#": tc
                })

                prev_c = c

            # -------- summarize --------
            wins  = sum(1 for t in trades if t["outcome"] == "tp")
            loss  = sum(1 for t in trades if t["outcome"] == "stop")
            openx = sum(1 for t in trades if t["outcome"] == "open")

            closed  = [t for t in trades if t["outcome"] in ("tp", "stop")]
            total_R = round(sum(t["R"] for t in trades), 2)
            avg_R   = round(sum(t["R"] for t in closed) / max(1, len(closed)), 2)

            def signed_pnl_usd(t):
                sgn = 1.0 if t["side"] == "long" else -1.0
                return (t["exit"] - t["entry"]) * sgn  # $/share

            max_win_R      = round(max((t["R"] for t in closed), default=0.0), 2)
            max_loss_R     = round(min((t["R"] for t in closed), default=0.0), 2)
            max_profit_usd = round(max((signed_pnl_usd(t) for t in closed), default=0.0), 2)
            max_loss_usd   = round(min((signed_pnl_usd(t) for t in closed), default=0.0), 2)

            # Log + popup
            audit(self.conn, "SIM", "SUMMARY",
                f"N={n} | trades={len(trades)} | wins={wins} loss={loss} open={openx} | "
                f"total_R={total_R} avg_R(closed)={avg_R} | "
                f"max_win_R={max_win_R} max_loss_R={max_loss_R} | "
                f"max_profit=${max_profit_usd} max_loss=${max_loss_usd} | "
                f"prox={prox} stop={sp} tp={tp}")

            for t in trades:
                pnl_usd = signed_pnl_usd(t)
                audit(self.conn, "SIM", "TRADE",
                    f"{t['outcome'].upper():5s} | {t['side']:5s} @ {t['entry']:.2f} → {t['exit']:.2f} "
                    f"| lvl {t['lvl']:.2f} | R={t['R']:+.2f} | ${pnl_usd:+.2f} "
                    f"| prox={t['prox']:.03f} touch#{t['touch#']}")

            messagebox.showinfo(
                "Sim complete",
                f"Trades: {len(trades)} | Wins: {wins} Loss: {loss} Open: {openx}\n"
                f"Total R: {total_R} | Avg R (closed): {avg_R}\n"
                f"Max Profit: ${max_profit_usd} | Max Loss: ${max_loss_usd}\n"
                f"Max Win (R): {max_win_R} | Max Loss (R): {max_loss_R}"
            )

        except Exception as e:
            audit(self.conn, "SIM", "ERROR", str(e))
            try:
                messagebox.showerror("Sim error", str(e))
            except Exception:
                pass
            import traceback
            audit(self.conn, "SIM", "ERROR_TRACE", traceback.format_exc())
            return

    # ---------- Test Price & Pings ----------
    def _test_price(self):
        api_key = self.api_key or settings_get(self.conn, "polygon_api_key", "")
        if not api_key:
            messagebox.showwarning("Polygon Key", "Please save your Polygon.io API key in Settings.")
            self._log_ui("FEED", "MISSING_API_KEY", "No API key set (Test Price).")
            return
        price, t_ms, err = self.engine.feed.get_last_trade(api_key)
        if err:
            self._log_ui("FEED", "FEED_ERR", f"Test Price error: {err}")
            messagebox.showerror("Price Error", f"Polygon error:\n{err}")
        else:
            self.price_var.set(f"{price:.2f}")
            self._log_ui("FEED", "PRICE_OK", f"Test Price OK: {price:.2f}")

    def _schedule_price_ping(self):
        try:
            api_key = self.api_key or settings_get(self.conn, "polygon_api_key", "")
            if api_key:
                status = self.engine.feed.get_market_status(api_key)
                if status.is_open or self.allow_ah:
                    price, t_ms, err = self.engine.feed.get_last_trade(api_key)
                    if not err and price is not None:
                        self.price_var.set(f"{price:.2f}")
                else:
                    pclose, err = self.engine.feed.get_prev_close(api_key)
                    if pclose is not None:
                        self.price_var.set(f"{pclose:.2f}")
        except Exception:
            pass
        self.after(3000, self._schedule_price_ping)

    # ---------- Retraining ----------
    def _start_retrain_scheduler(self):
        self._retrain_last_day = None
        def loop():
            import datetime as dt
            while True:
                try:
                    if settings_get(self.conn, "auto_retrain", "1") == "1":
                        hhmm = settings_get(self.conn, "retrain_time", "02:00")
                        now = dt.datetime.now()
                        target = now.replace(hour=int(hhmm.split(":")[0]), minute=int(hhmm.split(":")[1]), second=0, microsecond=0)
                        if now >= target and (self._retrain_last_day != now.date()):
                            self._retrain_last_day = now.date()
                            self._do_retrain()
                    time.sleep(30)
                except Exception:
                    time.sleep(60)
        t = threading.Thread(target=loop, daemon=True)
        t.start()

    def _retrain_from_labeled_events(self, max_rows: int = 500):
        """
        Incrementally update OnlinePolicy from policy_events that have a label
        and have not been trained yet (tracked via settings key 'last_trained_policy_event_id').
        """
        last_id = int(settings_get(self.conn, "last_trained_policy_event_id", "0") or 0)
        cur = self.conn.cursor()
        cur.execute("""
            SELECT id, phase, action, features_json, label
            FROM policy_events
            WHERE label IS NOT NULL
            AND id > ?
            ORDER BY id ASC
            LIMIT ?
        """, (last_id, max_rows))
        rows = cur.fetchall()
        if not rows:
            return 0

        trained_up_to = last_id
        n_updates = 0
        for _id, phase, action, fjson, label in rows:
            try:
                feats = json.loads(fjson or "{}")
                # Build exactly the same feature vector the model expects:
                x = self.policy.build_features(
                    proximity_abs=float(feats.get("proximity_abs", 0.0)),
                    volume_trend=float(feats.get("volume_trend", 0.0)),
                    approach=str(feats.get("approach", "from_above")),
                    confluence=bool(feats.get("confluence", False)),
                    minutes_since_open=int(feats.get("minutes_since_open", 0))
                )
                y = int(label)
                if phase == "entry":
                    # Map to the action actually taken at the time
                    a = action if action in ("go_long","go_short","skip") else "skip"
                    self.policy.update_entry(x, a, y)
                    n_updates += 1
                elif phase == "exit":
                    a = action if action in ("exit_now","hold") else "hold"
                    self.policy.update_exit(x, a, y)
                    n_updates += 1
                trained_up_to = _id
            except Exception as e:
                self._log_ui("POLICY", "TRAIN_ERR", f"policy_event {_id} failed: {e}")

        # Persist watermark + save model snapshot
        settings_set(self.conn, "last_trained_policy_event_id", str(trained_up_to))
        self._save_policy()
        self._log_ui("POLICY", "TRAIN", f"Updated from {n_updates} events; watermark → {trained_up_to}")
        return n_updates
    
    def _auto_tune_conf_threshold(self, window: int = 200):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT label FROM policy_events
            WHERE phase='entry' AND label IS NOT NULL
            ORDER BY id DESC LIMIT ?
        """, (window,))
        labels = [r[0] for r in cur.fetchall()]
        if len(labels) < 30:
            return
        winrate = sum(1 for x in labels if int(x)==1) / len(labels)
        # Nudge threshold toward winrate with mild inertia
        cur_thr = float(settings_get(self.conn, "Q_MIN_PROB", "0.60") or 0.60)
        target  = min(0.70, max(0.45, winrate))  # clamp
        new_thr = 0.8 * cur_thr + 0.2 * target
        settings_set(self.conn, "Q_MIN_PROB", f"{new_thr:.2f}")
        self._log_ui("POLICY", "TUNE", f"Winrate={winrate:.2f} → Q_MIN_PROB {cur_thr:.2f}→{new_thr:.2f}")

    def _start_retrain_scheduler(self):
        # already mentioned in your init; here’s the body
        try:
            self._retrain_from_labeled_events(max_rows=1000)
        finally:
            self.after(2 * 60 * 1000, self._start_retrain_scheduler)

    def _retrain_now(self):
        self._do_retrain()

    def _do_retrain(self):
        if not SKLEARN_OK:
            self._log_ui("RETRAIN", "SKLEARN_MISSING", "scikit-learn not installed; cannot retrain.")
        try:
            X, y = self._build_training_data()
            if len(X) < 50:
                self._log_ui("RETRAIN", "INSUFFICIENT_DATA", f"Found {len(X)} samples; need at least 50.")
                return
                try:
                    c = self.conn.execute("SELECT COUNT(*) FROM contact_events").fetchone()[0]
                    t = self.conn.execute("SELECT COUNT(*) FROM trades WHERE ts_close IS NOT NULL").fetchone()[0]
                    self.qvoice.narrate_retrain(code="RETRAIN_WAIT", seen_contacts=int(c), seen_trades=int(t), min_required=50)
                except Exception:
                    pass
            model = LogisticRegression(max_iter=1000)
            model.fit(X, y)
            models_dir = os.path.join(os.path.dirname(__file__), 'models')
            os.makedirs(models_dir, exist_ok=True)
            out_path = os.path.join(models_dir, 'qmmx_lr.joblib')
            joblib.dump(model, out_path)
            self.engine.model = model
            self._log_ui("RETRAIN", "OK", f"Retrained on {len(X)} samples; saved model.")
            try:
                c = self.conn.execute("SELECT COUNT(*) FROM contact_events").fetchone()[0]
                t = self.conn.execute("SELECT COUNT(*) FROM trades WHERE ts_close IS NOT NULL").fetchone()[0]
                self.qvoice.narrate_retrain(code="RETRAIN_OK", seen_contacts=int(c), seen_trades=int(t), min_required=50)
            except Exception:
                pass
        except Exception as e:
            self._log_ui("RETRAIN", "ERR", str(e))

    def _build_training_data(self):
        cur = self.conn.cursor()
        cur.execute("SELECT id, ts, symbol, level_color, level_type, level_index, level_price, approach, reaction, distance FROM contact_events ORDER BY id ASC")
        rows = cur.fetchall()
        cur.execute("SELECT id, ts_open, ts_close, side, entry, exit, pnl FROM trades ORDER BY id ASC")
        trades = cur.fetchall()
        import datetime as dt
        def parse_iso(s):
            try:
                return dt.datetime.fromisoformat(s.replace('Z','+00:00'))
            except Exception:
                return None
        X, y = [], []
        for (cid, ts, sym, color, ltype, lidx, lprice, approach, reaction, dist) in rows:
            t_contact = parse_iso(ts)
            if not t_contact: continue
            best = None
            for (tid, ts_open, ts_close, side, entry, exit_p, pnl) in trades:
                to = parse_iso(ts_open) if ts_open else None
                if not to: continue
                delta = (to - t_contact).total_seconds()
                if 0 <= delta <= 120:
                    best = (tid, ts_open, ts_close, side, entry, exit_p, pnl)
                    break
            if not best: continue
            lvl_type = 1 if ltype == "solid" else 0
            direc = 1 if approach == "up" else 0
            distf = float(dist) if dist is not None else 0.0
            X.append([lvl_type, distf, direc])
            y.append(1 if (best[-1] is not None and best[-1] > 0) else 0)
        return X, y

if __name__ == "__main__":
    app = QMMXApp()
    app.protocol("WM_DELETE_WINDOW", app.on_close)
    app.mainloop()
