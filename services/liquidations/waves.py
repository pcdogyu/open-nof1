"""
Shared liquidation wave detection utilities for the dashboard and automation.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from threading import Lock
from typing import Deque, Dict, Iterable, List, Optional, Sequence


@dataclass
class WaveMetrics:
    flv: float
    baseline: float
    price_change_pct: float
    price_drop_pct: float
    price_rise_pct: float
    le: Optional[float]
    pc: Optional[float]
    density_per_min: float
    lpi: float
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    start_price: Optional[float]
    end_price: Optional[float]
    duration_minutes: float
    window_size: int
    long_total: float
    short_total: float


@dataclass
class WaveEvent:
    instrument: str
    signal_code: str
    signal_text: str
    direction: str
    event_id: int
    detected_at: datetime
    price: Optional[float]
    metrics: WaveMetrics
    liquidation_side: Optional[str] = None
    liquidation_side_label: Optional[str] = None


@dataclass
class WaveSignal:
    instrument: str
    wave_label: str
    status: str
    signal_text: str
    signal_code: Optional[str]
    signal_class: str
    direction: Optional[str]
    metrics: WaveMetrics
    severity: int
    event_id: Optional[int]
    event_detected_at: Optional[datetime]
    liquidation_side: Optional[str] = None
    liquidation_side_label: Optional[str] = None

    def to_payload(self) -> dict:
        metrics = self.metrics
        return {
            "instrument": self.instrument,
            "wave": self.wave_label,
            "status": self.status,
            "signal": self.signal_text,
            "signal_code": self.signal_code,
            "signal_class": self.signal_class,
            "direction": self.direction,
            "severity": self.severity,
            "event_id": self.event_id,
            "event_detected_at": metrics.end_time.isoformat() if metrics.end_time else None,
            "liquidation_side": self.liquidation_side,
            "liquidation_side_label": self.liquidation_side_label,
            "metrics": {
                "flv": metrics.flv,
                "baseline": metrics.baseline,
                "price_change_pct": metrics.price_change_pct,
                "price_drop_pct": metrics.price_drop_pct,
                "price_rise_pct": metrics.price_rise_pct,
                "le": metrics.le,
                "pc": metrics.pc,
                "density_per_min": metrics.density_per_min,
                "lpi": metrics.lpi,
                "end_price": metrics.end_price,
                "start_price": metrics.start_price,
                "long_total": metrics.long_total,
                "short_total": metrics.short_total,
            },
        }


@dataclass
class _WaveState:
    baseline: Optional[float] = None
    wave_counter: int = 0
    current_wave: Optional[int] = None
    recent_flv: Deque[float] = field(default_factory=lambda: deque(maxlen=3))
    last_seen_ts: Optional[datetime] = None
    active_signal_code: Optional[str] = None
    event_counters: Dict[str, int] = field(default_factory=dict)
    last_signal: Optional[WaveSignal] = None


class WaveDetector:
    """
    Maintains liquidation wave states for multiple instruments and produces
    human-readable signals plus automation events.
    """

    def __init__(
        self,
        *,
        window_size: int = 50,
        multiplier: float = 2.0,
        price_drop_threshold: float = 0.7,
        settle_threshold: float = 0.2,
        smoothing: float = 0.2,
    ) -> None:
        self.window_size = max(5, window_size)
        self.multiplier = max(1.0, multiplier)
        self.price_drop_threshold = max(0.0, price_drop_threshold)
        self.settle_threshold = max(0.0, settle_threshold)
        self.smoothing = max(0.0, min(1.0, smoothing))
        self._states: Dict[str, _WaveState] = {}
        self._signals: Dict[str, WaveSignal] = {}
        self._events: Dict[tuple[str, str], WaveEvent] = {}
        self._lock = Lock()

    def update(
        self,
        instrument: str,
        records: Sequence[dict],
        *,
        baseline_override: float | None = None,
    ) -> WaveSignal:
        name = instrument.upper().strip() or "UNKNOWN"
        with self._lock:
            signal = self._update_locked(name, records, baseline_override)
            self._signals[name] = signal
            return signal

    def snapshot(self, instruments: Iterable[str]) -> List[WaveSignal]:
        signals: List[WaveSignal] = []
        with self._lock:
            for inst in instruments:
                symbol = inst.upper().strip()
                if not symbol:
                    continue
                signal = self._signals.get(symbol)
                if signal is None:
                    signal = self._build_default_signal(symbol)
                    self._signals[symbol] = signal
                signals.append(signal)
        return signals

    def iter_events(self, signal_codes: Optional[Sequence[str]] = None) -> List[WaveEvent]:
        with self._lock:
            events = list(self._events.values())
        if signal_codes:
            allowed = {code for code in signal_codes}
            events = [event for event in events if event.signal_code in allowed]
        events.sort(key=lambda evt: evt.detected_at or datetime.min)
        return events

    def get_event(self, instrument: str, signal_code: str) -> Optional[WaveEvent]:
        key = (instrument.upper(), signal_code)
        with self._lock:
            return self._events.get(key)

    def _update_locked(
        self,
        instrument: str,
        records: Sequence[dict],
        baseline_override: float | None = None,
    ) -> WaveSignal:
        state = self._states.setdefault(instrument, _WaveState())
        window = self._prepare_window(records)
        if not window:
            signal = self._build_default_signal(instrument)
            state.last_signal = signal
            return signal
        metrics = self._compute_metrics(window, state, baseline_override=baseline_override)
        signal = self._evaluate_signal(instrument, state, metrics, window[-1]["timestamp"])
        state.last_signal = signal
        return signal

    def _prepare_window(self, records: Sequence[dict]) -> List[dict]:
        normalized: List[dict] = []
        for record in records or []:
            timestamp = record.get("timestamp")
            ts_obj = self._coerce_datetime(timestamp)
            normalized.append(
                {
                    "timestamp": ts_obj,
                    "long_qty": self._coerce_float(record.get("long_qty")),
                    "short_qty": self._coerce_float(record.get("short_qty")),
                    "net_qty": self._coerce_float(record.get("net_qty")),
                    "last_price": self._coerce_float(record.get("last_price")),
                    "notional_value": self._coerce_float(record.get("notional_value")),
                }
            )
        normalized.sort(key=lambda row: row["timestamp"] or datetime.min)
        if len(normalized) > self.window_size:
            normalized = normalized[-self.window_size :]
        return normalized

    def _compute_metrics(
        self,
        window: Sequence[dict],
        state: _WaveState,
        *,
        baseline_override: float | None = None,
    ) -> WaveMetrics:
        flv = 0.0
        fls = 0.0
        total_long = 0.0
        total_short = 0.0
        first_ts = window[0]["timestamp"]
        last_ts = window[-1]["timestamp"]
        first_price = self._coerce_float(window[0].get("last_price"))
        last_price = self._coerce_float(window[-1].get("last_price"))
        for entry in window:
            long_qty = abs(self._coerce_float(entry.get("long_qty")) or 0.0)
            short_qty = abs(self._coerce_float(entry.get("short_qty")) or 0.0)
            total_long += long_qty
            total_short += short_qty
            qty = self._extract_quantity(entry)
            flv += qty
            notional = entry.get("notional_value")
            if notional is None and qty > 0 and entry.get("last_price"):
                notional = qty * (entry.get("last_price") or 0.0)
            if notional is not None:
                fls += abs(notional)
        duration_minutes = 0.0
        if first_ts and last_ts:
            duration_minutes = max(((last_ts - first_ts).total_seconds()) / 60.0, 0.0)
        price_change_pct = 0.0
        if first_price and first_price > 0 and last_price:
            price_change_pct = ((last_price - first_price) / first_price) * 100.0
        price_drop_pct = abs(price_change_pct) if price_change_pct < 0 else 0.0
        price_rise_pct = price_change_pct if price_change_pct > 0 else 0.0
        density = flv if duration_minutes <= 0 else flv / max(duration_minutes, 1 / 60)
        if baseline_override is not None and baseline_override >= 0:
            state.baseline = max(baseline_override, 1e-9)
        else:
            target_baseline = state.baseline if state.baseline is not None else flv
            smoothed = ((target_baseline or flv) * (1 - self.smoothing)) + (flv * self.smoothing)
            state.baseline = max(smoothed, 1e-9)
        le = (flv / price_drop_pct) if price_drop_pct > 0 else None
        pc = (fls / price_drop_pct) if price_drop_pct > 0 else None
        lpi = flv / state.baseline if state.baseline else 0.0
        return WaveMetrics(
            flv=flv,
            baseline=state.baseline,
            price_change_pct=price_change_pct,
            price_drop_pct=price_drop_pct,
            price_rise_pct=price_rise_pct,
            le=le,
            pc=pc,
            density_per_min=density,
            lpi=lpi,
            start_time=first_ts,
            end_time=last_ts,
            start_price=first_price,
            end_price=last_price,
            duration_minutes=duration_minutes,
            window_size=len(window),
            long_total=total_long,
            short_total=total_short,
        )

    def _describe_liquidation_side(self, metrics: WaveMetrics) -> tuple[Optional[str], Optional[str]]:
        long_total = metrics.long_total or 0.0
        short_total = metrics.short_total or 0.0
        total = long_total + short_total
        if total <= 0:
            return None, None
        dominance = abs(long_total - short_total) / total
        threshold = 0.15
        if dominance <= threshold:
            return "mixed", "多空强平接近"
        if long_total > short_total:
            share = (long_total / total) * 100
            return "long", f"多单被强平为主 ({share:.0f}%)"
        share = (short_total / total) * 100
        return "short", f"空单被强平为主 ({share:.0f}%)"

    def _evaluate_signal(
        self,
        instrument: str,
        state: _WaveState,
        metrics: WaveMetrics,
        latest_timestamp: Optional[datetime],
    ) -> WaveSignal:
        wave_label = "--"
        status = "观望"
        signal_text = "观察中"
        signal_class = ""
        signal_code: Optional[str] = None
        direction: Optional[str] = None
        severity = 1
        liquidation_side, liquidation_label = self._describe_liquidation_side(metrics)

        state.recent_flv.append(metrics.flv)
        flv_dropping = (
            len(state.recent_flv) == state.recent_flv.maxlen
            and state.recent_flv[0] > state.recent_flv[1] > state.recent_flv[2]
        )
        start_condition = (
            metrics.flv > metrics.baseline * self.multiplier
            and metrics.price_drop_pct >= self.price_drop_threshold
        )
        settle_condition = (
            metrics.flv < metrics.baseline and abs(metrics.price_change_pct) < self.settle_threshold
        )
        absorbing = metrics.price_drop_pct < self.settle_threshold and metrics.flv > metrics.baseline
        short_absorb = absorbing and liquidation_side == "short"
        top_condition = (
            metrics.price_rise_pct >= self.price_drop_threshold
            and metrics.lpi >= self.multiplier
        )
        short_reversal = top_condition and liquidation_side == "short"

        if state.current_wave is None and start_condition:
            state.wave_counter += 1
            state.current_wave = state.wave_counter
        elif state.current_wave is not None and settle_condition:
            state.current_wave = None

        if state.current_wave is not None:
            wave_label = f"Wave {state.current_wave}"
            status = "波次进行"
            signal_text = f"Wave {state.current_wave} 进行中"
            signal_class = "wave-signal-warn"
            severity = 4
        elif short_absorb:
            status = "顶部预警"
            signal_text = "空单被强平 → 顶部信号"
            base_text = "空单被强平 → 顶部信号"
            signal_text = f"{base_text} · {liquidation_label}" if liquidation_label else base_text
            signal_class = "wave-signal-warn"
            signal_code = "top_signal"
            direction = "sell"
            severity = 3
        elif absorbing:
            status = "吸收"
            signal_text = "强平被吸收 → 底部信号"
            base_text = "强平被吸收 → 底部信号"
            signal_text = f"{base_text} · {liquidation_label}" if liquidation_label else base_text
            signal_class = "wave-signal-bottom"
            signal_code = "bottom_absorb"
            direction = "buy"
            severity = 2
        elif short_reversal:
            status = "反转机会"
            base_text = "空单爆仓 → 反转信号"
            signal_text = f"{base_text} · {liquidation_label}" if liquidation_label else base_text
            signal_class = "wave-signal-warn"
            signal_code = "short_reversal"
            direction = "sell"
            severity = 4
        elif flv_dropping and liquidation_side == "short":
            status = "衰减"
            base_text = "空单爆仓量快速下降"
            signal_text = f"{base_text} · {liquidation_label}" if liquidation_label else base_text
            signal_class = "wave-signal-bottom"
            signal_code = "short_decay"
            direction = "buy"
            severity = 2
        elif top_condition:
            status = "顶部预警"
            signal_text = "顶部信号"
            signal_class = "wave-signal-warn"
            signal_code = "top_signal"
            direction = "sell"
            severity = 3
        elif flv_dropping:
            status = "衰减"
            signal_text = "爆仓量连续下降"
            signal_class = "wave-signal-bottom"
            severity = 2
        elif start_condition:
            status = "警戒"
            signal_text = "满足触发阈值，留意下一波"
            signal_class = "wave-signal-strong"
            severity = 3

        if wave_label == "--" and state.wave_counter > 0:
            wave_label = f"Wave {state.wave_counter}"

        new_tick = False
        if latest_timestamp and (
            state.last_seen_ts is None or latest_timestamp > state.last_seen_ts
        ):
            new_tick = True
            state.last_seen_ts = latest_timestamp

        event_id: Optional[int] = None
        if signal_code in {"bottom_absorb", "top_signal", "short_reversal"} and direction:
            trigger_new = new_tick and state.active_signal_code != signal_code
            if trigger_new:
                count = state.event_counters.get(signal_code, 0) + 1
                state.event_counters[signal_code] = count
                event_id = count
                detected_at = latest_timestamp or datetime.now()
                event = WaveEvent(
                    instrument=instrument,
                    signal_code=signal_code,
                    signal_text=signal_text,
                    direction=direction,
                    event_id=count,
                    detected_at=detected_at,
                    price=metrics.end_price,
                    metrics=metrics,
                    liquidation_side=liquidation_side,
                    liquidation_side_label=liquidation_label,
                )
                self._events[(instrument, signal_code)] = event
                state.active_signal_code = signal_code
            elif state.active_signal_code != signal_code:
                event_id = None
        else:
            state.active_signal_code = None

        signal = WaveSignal(
            instrument=instrument,
            wave_label=wave_label,
            status=status,
            signal_text=signal_text,
            signal_code=signal_code,
            signal_class=signal_class,
            direction=direction,
            metrics=metrics,
            severity=severity,
            event_id=event_id,
            event_detected_at=latest_timestamp,
            liquidation_side=liquidation_side,
            liquidation_side_label=liquidation_label,
        )
        return signal

    def _build_default_signal(self, instrument: str) -> WaveSignal:
        metrics = WaveMetrics(
            flv=0.0,
            baseline=0.0,
            price_change_pct=0.0,
            price_drop_pct=0.0,
            price_rise_pct=0.0,
            le=None,
            pc=None,
            density_per_min=0.0,
            lpi=0.0,
            start_time=None,
            end_time=None,
            start_price=None,
            end_price=None,
            duration_minutes=0.0,
            window_size=0,
            long_total=0.0,
            short_total=0.0,
        )
        return WaveSignal(
            instrument=instrument,
            wave_label="--",
            status="无数据",
            signal_text="等待数据",
            signal_code=None,
            signal_class="",
            direction=None,
            metrics=metrics,
            severity=1,
            event_id=None,
            event_detected_at=None,
            liquidation_side=None,
            liquidation_side_label=None,
        )

    @staticmethod
    def _coerce_float(value: object) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            try:
                return float(value)
            except (TypeError, ValueError):
                return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_datetime(value: object) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value
        if not value:
            return None
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    def _extract_quantity(self, entry: dict) -> float:
        net_qty = abs(self._coerce_float(entry.get("net_qty")) or 0.0)
        long_qty = abs(self._coerce_float(entry.get("long_qty")) or 0.0)
        short_qty = abs(self._coerce_float(entry.get("short_qty")) or 0.0)
        return max(net_qty, long_qty, short_qty)


_DETECTOR = WaveDetector()


def update_wave(
    instrument: str,
    records: Sequence[dict],
    *,
    baseline_override: float | None = None,
) -> WaveSignal:
    return _DETECTOR.update(instrument, records, baseline_override=baseline_override)


def bulk_update(
    grouped_records: Dict[str, Sequence[dict]],
    *,
    baseline_overrides: Dict[str, float] | None = None,
) -> Dict[str, WaveSignal]:
    results: Dict[str, WaveSignal] = {}
    for instrument, rows in grouped_records.items():
        baseline = None
        if baseline_overrides:
            baseline = baseline_overrides.get(instrument.upper())
        try:
            results[instrument.upper()] = _DETECTOR.update(
                instrument,
                rows,
                baseline_override=baseline,
            )
        except Exception:
            results[instrument.upper()] = _DETECTOR.update(instrument, [], baseline_override=baseline)
    return results


def snapshot_signals(instruments: Iterable[str]) -> List[WaveSignal]:
    return _DETECTOR.snapshot(instruments)


def iter_events(signal_codes: Optional[Sequence[str]] = None) -> List[WaveEvent]:
    return _DETECTOR.iter_events(signal_codes)


def get_event(instrument: str, signal_code: str) -> Optional[WaveEvent]:
    return _DETECTOR.get_event(instrument, signal_code)
