import sqlite3
import datetime
import json
import threading
from typing import Any, Dict, Optional, Tuple

try:
    import tkinter as tk
    from tkinter import ttk
except Exception:
    tk = None
    ttk = None


class QVoice:
    """
    Q's natural-language narrator.

    Responsibilities:
      - Translate audit codes and raw decision payloads into clear English.
      - Persist explanations to SQLite in table `q_explanations`.
      - (Optional) Render a Tk panel that streams explanations live.
    """

    # Canonical mapping of terse codes → human sentences.
    # You can extend this without touching the rest of the system.
    CODEBOOK = {
        "PRICE_STALE": "Skipped: incoming price data was stale (older than the freshness window).",
        "DIR_UNKNOWN": "Skipped: couldn’t determine short-term direction from the last few candles.",
        "TOO_FAR": "Skipped: price was outside the allowed proximity to the target level.",
        "CONF_LOW": "Skipped: system confidence was below the entry threshold.",
        "POLICY_SKIP": "Skipped by policy: learned model indicates this setup has a poor expectancy.",
        "LEVEL_WEAK": "Caution: repeated contacts weakened this level’s edge; standing down.",
        "CONTACT_OK": "Level contact detected and within proximity window.",
        "ENTRY_EXECUTE": "Entering position: conditions aligned with a high-probability reaction.",
        "EXIT_EXECUTE": "Exiting position: conditions now favor the opposite behavior.",
        "STOP_HIT": "Exit: protective stop was triggered.",
        "TARGET_HIT": "Exit: profit target reached.",
        "RETRAIN_OK": "Model retrained successfully; new weights applied.",
        "RETRAIN_WAIT": "Retraining deferred: not enough labeled examples yet.",
        "VOL_DEC_REV": "Volume was decreasing into the level, favoring a reversal.",
        "VOL_INC_CONT": "Volume was increasing into the level, favoring a penetration/continuation.",
        "CONFLUENCE_SNAP": "Confluence behavior: pierce → slight pierce → snapback; reversal expected.",
        "COOLDOWN": "Skipped: cooldown in effect; avoiding clustered entries.",
    }

    def __init__(
        self,
        db_path: str,
        ui_parent: Optional["tk.Widget"] = None,   # Optional Tk container for live panel
        max_rows_in_panel: int = 300,
        create_table_if_missing: bool = True
    ):
        self.db_path = db_path
        self._lock = threading.RLock()
        self.max_rows_in_panel = max_rows_in_panel

        if create_table_if_missing:
            self._ensure_table()

        # Optional UI
        self.ui_parent = ui_parent
        self.ui_text = None
        if ui_parent is not None and tk is not None and ttk is not None:
            self._build_ui(ui_parent)

    # ---------- Public API ----------

    def say(self, code: str, **payload: Any) -> None:
        """
        Main entrypoint. Call this anywhere you currently do an audit/log entry.

        Example:
            qvoice.say("POLICY_SKIP",
                       symbol="SPY",
                       level_type="blue_solid_1",
                       proximity=0.12,
                       conf=0.47, min_conf=0.60,
                       ml_prob=0.41, min_prob=0.60,
                       direction="down",
                       volume_trend="decreasing")

        This will:
          1) Translate into human text.
          2) Store into `q_explanations`.
          3) Stream to the optional UI panel.
        """
        natural = self._translate(code, payload)
        row = self._persist(code, natural, payload)
        self._ui_append(natural, row)

    def narrate_entry_evaluation(
        self,
        *,
        symbol: str,
        code: str,
        level_type: str,
        direction: str,
        proximity: float,
        confidence: float,
        min_conf: float,
        ml_prob: Optional[float] = None,
        min_prob: Optional[float] = None,
        volume_trend: Optional[str] = None,
        touches: Optional[int] = None,
        notes: Optional[str] = None
    ) -> None:
        """
        Convenience narrator specifically for entry decisions.
        """
        payload = dict(
            kind="ENTRY_EVAL",
            symbol=symbol,
            level_type=level_type,
            direction=direction,
            proximity=proximity,
            conf=confidence,
            min_conf=min_conf,
            ml_prob=ml_prob,
            min_prob=min_prob,
            volume_trend=volume_trend,
            touches=touches,
            notes=notes,
        )
        self.say(code, **payload)

    def narrate_exit(
        self,
        *,
        symbol: str,
        code: str,
        reason: str,
        pnl: Optional[float] = None,
        volume_trend: Optional[str] = None,
        notes: Optional[str] = None
    ) -> None:
        """
        Narrate exits (stop/target/logic).
        """
        payload = dict(
            kind="EXIT",
            symbol=symbol,
            reason=reason,
            pnl=pnl,
            volume_trend=volume_trend,
            notes=notes,
        )
        self.say(code, **payload)

    def narrate_retrain(
        self,
        *,
        code: str,
        seen_contacts: int,
        seen_trades: int,
        min_required: int,
        notes: Optional[str] = None
    ) -> None:
        payload = dict(
            kind="RETRAIN",
            seen_contacts=seen_contacts,
            seen_trades=seen_trades,
            min_required=min_required,
            notes=notes,
        )
        self.say(code, **payload)

    # ---------- DB & UI ----------

    def fetch_recent(self, limit: int = 200) -> Tuple[Tuple]:
        """
        Returns tuples of (id, ts, code, text, payload_json)
        """
        with self._conn() as cx:
            cur = cx.execute(
                "SELECT id, ts, code, text, payload_json "
                "FROM q_explanations ORDER BY id DESC LIMIT ?",
                (int(limit),)
            )
            return tuple(cur.fetchall())

    def clear(self) -> None:
        """
        Clear all explanations (does not affect other tables).
        """
        with self._conn() as cx:
            cx.execute("DELETE FROM q_explanations;")
            cx.commit()
        self._ui_replace("— cleared —\n")

    # ---------- Internals ----------

    def _ensure_table(self) -> None:
        with self._conn() as cx:
            cx.execute(
                """
                CREATE TABLE IF NOT EXISTS q_explanations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    code TEXT NOT NULL,
                    text TEXT NOT NULL,
                    payload_json TEXT
                );
                """
            )
            cx.commit()

    def _conn(self) -> sqlite3.Connection:
        cx = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        cx.execute("PRAGMA journal_mode=WAL;")
        return cx

    def _translate(self, code: str, payload: Dict[str, Any]) -> str:
        """
        Turn a terse code + context into a natural-language sentence (or two).
        """
        base = self.CODEBOOK.get(code, f"Event: {code}")
        parts = [base]

        # Enrich with context if present
        sym = payload.get("symbol")
        if sym:
            parts.append(f"[{sym}]")

        # Entry/exit/retrain specific add-ons
        kind = payload.get("kind")

        if kind == "ENTRY_EVAL":
            lvl = payload.get("level_type")
            dirn = payload.get("direction")
            prox = payload.get("proximity")
            conf = payload.get("conf")
            minc = payload.get("min_conf")
            prob = payload.get("ml_prob")
            minp = payload.get("min_prob")
            vt = payload.get("volume_trend")
            tc = payload.get("touches")

            if lvl: parts.append(f"at level: {lvl}")
            if dirn: parts.append(f"approach: {dirn}")
            if prox is not None: parts.append(f"proximity: {prox:.4f}")
            if conf is not None and minc is not None:
                parts.append(f"conf {conf:.2f}/{minc:.2f}")
            if prob is not None and minp is not None:
                parts.append(f"ml {prob:.2f}/{minp:.2f}")
            if vt: parts.append(f"volume {vt}")
            if tc is not None: parts.append(f"touches {tc}")

        elif kind == "EXIT":
            rsn = payload.get("reason")
            pnl = payload.get("pnl")
            vt = payload.get("volume_trend")
            if rsn: parts.append(f"reason: {rsn}")
            if pnl is not None: parts.append(f"pnl: {pnl:+.2f}")
            if vt: parts.append(f"volume {vt}")

        elif kind == "RETRAIN":
            c = payload.get("seen_contacts")
            t = payload.get("seen_trades")
            m = payload.get("min_required")
            parts.append(f"dataset: contacts={c}, trades={t}, needs ≥ {m}")

        # Free-form note
        notes = payload.get("notes")
        if notes:
            parts.append(f"note: {notes}")

        # Join; keep tidy
        sentence = " | ".join(str(p) for p in parts if p)
        return sentence

    def _persist(self, code: str, natural_text: str, payload: Dict[str, Any]) -> Tuple[int, str]:
        ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        row_id = None
        with self._lock:
            with self._conn() as cx:
                cur = cx.execute(
                    "INSERT INTO q_explanations (ts, code, text, payload_json) VALUES (?, ?, ?, ?)",
                    (ts, code, natural_text, json.dumps(payload or {}, ensure_ascii=False))
                )
                row_id = cur.lastrowid
                cx.commit()
        return (row_id, ts)

    # ---------- UI ----------

    def _build_ui(self, parent: "tk.Widget") -> None:
        frame = ttk.Frame(parent)
        frame.pack(fill="both", expand=True)

        toolbar = ttk.Frame(frame)
        toolbar.pack(side="top", fill="x")

        btn_refresh = ttk.Button(toolbar, text="Refresh", command=self._ui_refresh)
        btn_clear = ttk.Button(toolbar, text="Clear (DB + Panel)", command=self.clear)
        btn_backfill = ttk.Button(toolbar, text="Backfill from Audit", command=self._ui_backfill_from_audit)

        btn_refresh.pack(side="left", padx=4, pady=4)
        btn_clear.pack(side="left", padx=4, pady=4)
        btn_backfill.pack(side="left", padx=4, pady=4)

        self.ui_text = tk.Text(frame, height=24)
        self.ui_text.pack(fill="both", expand=True)
        self._ui_refresh()

    def _ui_append(self, natural_text: str, row_meta: Tuple[int, str]) -> None:
        if not self.ui_text:
            return
        rid, ts = row_meta
        line = f"{rid:06d} | {ts} | {natural_text}\n"
        self.ui_text.insert("end", line)
        # Trim
        current = float(self.ui_text.index('end-1c').split('.')[0])
        if current > self.max_rows_in_panel:
            self.ui_text.delete("1.0", "2.0")
        self.ui_text.see("end")

    def _ui_replace(self, content: str) -> None:
        if not self.ui_text:
            return
        self.ui_text.delete("1.0", "end")
        self.ui_text.insert("1.0", content)
        self.ui_text.see("end")

    def _ui_refresh(self) -> None:
        rows = self.fetch_recent(limit=self.max_rows_in_panel)
        buf = []
        for rid, ts, code, text, _pj in rows[::-1]:
            buf.append(f"{rid:06d} | {ts} | {text}")
        self._ui_replace("\n".join(buf) + ("\n" if buf else "— empty —\n"))

    def _ui_backfill_from_audit(self) -> None:
        """
        Optional helper: if your monolithic file already logs terse messages
        into a table named `audit_log (ts TEXT, kind TEXT, payload_json TEXT)`,
        this will convert recent rows into Q explanations.
        """
        try:
            with self._conn() as cx:
                cur = cx.execute(
                    "SELECT ts, kind, payload_json FROM audit_log ORDER BY ts DESC LIMIT 500"
                )
                rows = cur.fetchall()
        except Exception:
            self._ui_append("Audit table not found; backfill skipped.", (0, ""))
            return

        inserted = 0
        for ts, kind, payload_json in rows:
            try:
                payload = json.loads(payload_json) if payload_json else {}
            except Exception:
                payload = {"raw": payload_json}

            # Map audit kinds into our codebook defaults
            code = kind
            if code not in self.CODEBOOK:
                # Attempt a friendly default
                if "ENTRY" in code.upper(): code = "ENTRY_EXECUTE"
                elif "EXIT" in code.upper(): code = "EXIT_EXECUTE"
                elif "RETRAIN" in code.upper(): code = "RETRAIN_OK"

            natural = self._translate(code, payload)
            self._persist(code, natural, payload)
            inserted += 1

        self._ui_append(f"Backfilled {inserted} rows from audit_log.", (0, ""))
