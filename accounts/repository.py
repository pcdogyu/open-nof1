"""
Repository abstractions for account, position, and trade storage.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import List, Optional

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.flux_table import FluxRecord

from accounts.models import Account, AccountSnapshot, Balance, Order, Position, Trade
from data_pipeline.influx import InfluxConfig

logger = logging.getLogger(__name__)


class AccountRepository(ABC):
    """Interface for persisting and retrieving account state."""

    @abstractmethod
    def upsert_account(self, account: Account) -> None:
        raise NotImplementedError

    @abstractmethod
    def record_position(self, position: Position) -> None:
        raise NotImplementedError

    @abstractmethod
    def record_trade(self, trade: Trade) -> None:
        raise NotImplementedError

    @abstractmethod
    def record_equity_point(self, account: Account) -> None:
        raise NotImplementedError

    @abstractmethod
    def record_balance(self, balance: Balance) -> None:
        raise NotImplementedError

    @abstractmethod
    def record_order(self, order: Order) -> None:
        raise NotImplementedError

    @abstractmethod
    def list_accounts(self) -> List[Account]:
        raise NotImplementedError

    @abstractmethod
    def get_account(self, account_id: str) -> Optional[Account]:
        raise NotImplementedError

    @abstractmethod
    def list_positions(self, account_id: str) -> List[Position]:
        raise NotImplementedError

    @abstractmethod
    def list_trades(self, account_id: str, limit: int = 50) -> List[Trade]:
        raise NotImplementedError

    @abstractmethod
    def get_equity_curve(self, account_id: str, limit: int = 100) -> List[dict]:
        raise NotImplementedError

    @abstractmethod
    def list_balances(self, account_id: str) -> List[Balance]:
        raise NotImplementedError

    @abstractmethod
    def list_orders(self, account_id: str, limit: int = 50) -> List[Order]:
        raise NotImplementedError

    def get_snapshot(self, account_id: str, *, trades_limit: int = 50) -> Optional[AccountSnapshot]:
        account = self.get_account(account_id)
        if account is None:
            return None
        positions = self.list_positions(account_id)
        trades = self.list_trades(account_id, limit=trades_limit)
        return AccountSnapshot(account=account, positions=positions, recent_trades=trades)


class InfluxAccountRepository(AccountRepository):
    """InfluxDB-backed account repository."""

    ACCOUNT_MEASUREMENT = "account_snapshots"
    POSITION_MEASUREMENT = "account_positions"
    TRADE_MEASUREMENT = "account_trades"
    EQUITY_CURVE_MEASUREMENT = "account_equity_curve"
    BALANCE_MEASUREMENT = "account_balances"
    ORDER_MEASUREMENT = "account_orders"

    def __init__(self, config: Optional[InfluxConfig] = None) -> None:
        self._config = config or InfluxConfig.from_env()
        if not self._config.token:
            raise ValueError("InfluxDB token is required for InfluxAccountRepository.")
        self._client = InfluxDBClient(
            url=self._config.url,
            token=self._config.token,
            org=self._config.org,
        )
        self._write_api = self._client.write_api()
        self._query_api = self._client.query_api()

    # ------------------------------------------------------------------ mutations
    def upsert_account(self, account: Account) -> None:
        updated_at = _to_time_ns(account.updated_at)
        created_at_ns = _to_time_ns(account.created_at)
        point = (
            Point(self.ACCOUNT_MEASUREMENT)
            .tag("account_id", account.account_id)
            .tag("model_id", account.model_id)
            .field("base_currency", account.base_currency)
            .field("starting_equity", account.starting_equity)
            .field("cash_balance", account.cash_balance)
            .field("equity", account.equity)
            .field("pnl", account.pnl)
            .field("created_at_ns", created_at_ns)
            .time(updated_at, WritePrecision.NS)
        )
        self._write_api.write(bucket=self._config.bucket, org=self._config.org, record=point)

    def record_position(self, position: Position) -> None:
        """Persist the latest state for an account position."""
        point = (
            Point(self.POSITION_MEASUREMENT)
            .tag("account_id", position.account_id)
            .tag("position_id", position.position_id)
            .tag("instrument_id", position.instrument_id)
            .field("side", position.side)
            .field("quantity", position.quantity)
            .field("entry_price", position.entry_price)
            .time(_to_time_ns(position.updated_at), WritePrecision.NS)
        )
        if position.mark_price is not None:
            point = point.field("mark_price", position.mark_price)
        if position.leverage is not None:
            point = point.field("leverage", position.leverage)
        if position.unrealized_pnl is not None:
            point = point.field("unrealized_pnl", position.unrealized_pnl)
        self._write_api.write(bucket=self._config.bucket, org=self._config.org, record=point)

    def record_trade(self, trade: Trade) -> None:
        """Persist an immutable trade execution record."""
        point = (
            Point(self.TRADE_MEASUREMENT)
            .tag("account_id", trade.account_id)
            .tag("trade_id", trade.trade_id)
            .tag("model_id", trade.model_id)
            .tag("instrument_id", trade.instrument_id)
            .field("side", trade.side)
            .field("quantity", trade.quantity)
            .field("price", trade.price)
            .time(_to_time_ns(trade.executed_at), WritePrecision.NS)
        )
        if trade.fee is not None:
            point = point.field("fee", trade.fee)
        if trade.realized_pnl is not None:
            point = point.field("realized_pnl", trade.realized_pnl)
        if trade.close_price is not None:
            point = point.field("close_price", trade.close_price)
        self._write_api.write(bucket=self._config.bucket, org=self._config.org, record=point)

    def record_equity_point(self, account: Account) -> None:
        """Append an equity data point used for time-series analytics."""
        point = (
            Point(self.EQUITY_CURVE_MEASUREMENT)
            .tag("account_id", account.account_id)
            .tag("model_id", account.model_id)
            .field("equity", account.equity)
            .field("pnl", account.pnl)
            .time(_to_time_ns(account.updated_at), WritePrecision.NS)
        )
        self._write_api.write(bucket=self._config.bucket, org=self._config.org, record=point)

    def record_balance(self, balance: Balance) -> None:
        point = (
            Point(self.BALANCE_MEASUREMENT)
            .tag("account_id", balance.account_id)
            .tag("currency", balance.currency)
            .field("total", balance.total)
            .field("available", balance.available)
            .field("frozen", balance.frozen)
            .field("equity", balance.equity)
            .time(_to_time_ns(balance.updated_at), WritePrecision.NS)
        )
        self._write_api.write(bucket=self._config.bucket, org=self._config.org, record=point)

    def record_order(self, order: Order) -> None:
        point = (
            Point(self.ORDER_MEASUREMENT)
            .tag("account_id", order.account_id)
            .tag("order_id", order.order_id)
            .tag("instrument_id", order.instrument_id)
            .field("model_id", order.model_id)
            .field("side", order.side)
            .field("order_type", order.order_type)
            .field("size", order.size)
            .field("filled_size", order.filled_size)
            .field("price", order.price if order.price is not None else 0.0)
            .field("average_price", order.average_price if order.average_price is not None else 0.0)
            .field("state", order.state)
            .field("created_at_ns", _to_time_ns(order.created_at))
            .time(_to_time_ns(order.updated_at), WritePrecision.NS)
        )
        self._write_api.write(bucket=self._config.bucket, org=self._config.org, record=point)

    # ------------------------------------------------------------------ queries
    def list_accounts(self) -> List[Account]:
        query = f"""
from(bucket: "{self._config.bucket}")
  |> range(start: -30d)
  |> filter(fn: (r) => r["_measurement"] == "{self.ACCOUNT_MEASUREMENT}")
  |> filter(fn: (r) =>
        r._field == "starting_equity" or
        r._field == "cash_balance" or
        r._field == "equity" or
        r._field == "pnl" or
        r._field == "created_at_ns" or
        r._field == "base_currency")
  |> group(columns: ["account_id", "_field"]) 
  |> last()
"""
        records = self._safe_query(query)
        return _merge_account_records(records)

    def get_account(self, account_id: str) -> Optional[Account]:
        query = f"""
from(bucket: "{self._config.bucket}")
  |> range(start: -90d)
  |> filter(fn: (r) => r["_measurement"] == "{self.ACCOUNT_MEASUREMENT}")
  |> filter(fn: (r) => r["account_id"] == "{account_id}")
  |> filter(fn: (r) =>
        r._field == "starting_equity" or
        r._field == "cash_balance" or
        r._field == "equity" or
        r._field == "pnl" or
        r._field == "created_at_ns" or
        r._field == "base_currency")
  |> group(columns: ["account_id", "_field"]) 
  |> last()
"""
        records = self._safe_query(query)
        accounts = _merge_account_records(records)
        return accounts[0] if accounts else None

    def list_positions(self, account_id: str) -> List[Position]:
        query = f"""
from(bucket: "{self._config.bucket}")
  |> range(start: -7d)
  |> filter(fn: (r) => r["_measurement"] == "{self.POSITION_MEASUREMENT}")
  |> filter(fn: (r) => r["account_id"] == "{account_id}")
  |> group(columns: ["position_id", "_field"]) 
  |> last()
"""
        records = self._safe_query(query)
        return _merge_position_records(records)

    def list_trades(self, account_id: str, limit: int = 50) -> List[Trade]:
        query = f"""
from(bucket: "{self._config.bucket}")
  |> range(start: -30d)
  |> filter(fn: (r) => r["_measurement"] == "{self.TRADE_MEASUREMENT}")
  |> filter(fn: (r) => r["account_id"] == "{account_id}")
  |> group(columns: ["trade_id", "_field"]) 
  |> last()
"""
        records = self._safe_query(query)
        trades = _merge_trade_records(records)
        trades.sort(key=lambda t: t.executed_at, reverse=True)
        return trades[:limit]

    def get_equity_curve(self, account_id: str, limit: int = 100) -> List[dict]:
        query = f"""
from(bucket: "{self._config.bucket}")
  |> range(start: -30d)
  |> filter(fn: (r) => r["_measurement"] == "{self.EQUITY_CURVE_MEASUREMENT}")
  |> filter(fn: (r) => r["account_id"] == "{account_id}")
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: {limit})
  |> sort(columns: ["_time"], desc: false)
"""
        records = self._safe_query(query)
        curve: List[dict] = []
        for record in records:
            curve.append(
                {
                    "timestamp": record.get_time().isoformat(),
                    "equity": _optional_float(record.get_value()) or 0.0,
                    "field": record.get_field(),
                }
            )
        # Filter to equity field only for readability.
        equity_only = [point for point in curve if point["field"] == "equity"]
        if equity_only:
            return [
                {"timestamp": point["timestamp"], "equity": point["equity"]}
                for point in equity_only
            ]
        return curve

    def list_balances(self, account_id: str) -> List[Balance]:
        query = f"""
from(bucket: "{self._config.bucket}")
  |> range(start: -1d)
  |> filter(fn: (r) => r["_measurement"] == "{self.BALANCE_MEASUREMENT}")
  |> filter(fn: (r) => r["account_id"] == "{account_id}")
  |> group(columns: ["currency"])
  |> last()
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
"""
        records = self._safe_query(query)
        return [_record_to_balance(record) for record in records]

    def list_orders(self, account_id: str, limit: int = 50) -> List[Order]:
        query = f"""
from(bucket: "{self._config.bucket}")
  |> range(start: -7d)
  |> filter(fn: (r) => r["_measurement"] == "{self.ORDER_MEASUREMENT}")
  |> filter(fn: (r) => r["account_id"] == "{account_id}")
  |> group(columns: ["order_id", "_field"]) 
  |> last()
"""
        records = self._safe_query(query)
        orders = _merge_order_records(records)
        orders.sort(key=lambda o: o.updated_at, reverse=True)
        return orders[:limit]

    # ------------------------------------------------------------------ helpers
    def _safe_query(self, flux_query: str) -> List[FluxRecord]:
        try:
            tables = self._query_api.query(org=self._config.org, query=flux_query)
        except Exception as exc:  # pragma: no cover - network path
            logger.warning("Failed to query InfluxDB: %s", exc, exc_info=True)
            return []

        records: List[FluxRecord] = []
        for table in tables:
            for record in table.records:
                records.append(record)
        return records

    def close(self) -> None:
        self._write_api.__del__()  # type: ignore[attr-defined]
        self._client.close()


def _record_to_account(record: FluxRecord) -> Account:
    values = record.values
    created_at_ns = values.get("created_at_ns")
    created_at = _from_ns(created_at_ns) if created_at_ns else record.get_time()
    updated_at = record.get_time()
    return Account(
        account_id=str(values.get("account_id")),
        model_id=str(values.get("model_id")),
        base_currency=str(values.get("base_currency", "USD")),
        starting_equity=float(values.get("starting_equity", 0.0)),
        cash_balance=float(values.get("cash_balance", 0.0)),
        equity=float(values.get("equity", 0.0)),
        pnl=float(values.get("pnl", 0.0)),
        created_at=created_at,
        updated_at=updated_at,
    )


def _merge_account_records(records: List[FluxRecord]) -> List[Account]:
    grouped: dict[str, dict] = {}
    times: dict[str, datetime] = {}
    created_ns: dict[str, int] = {}
    for rec in records:
        vals = rec.values
        aid = str(vals.get("account_id"))
        field = str(vals.get("_field"))
        grouped.setdefault(aid, {"account_id": aid})
        times[aid] = rec.get_time()
        if field == "created_at_ns":
            try:
                created_ns[aid] = int(rec.get_value())
            except Exception:
                pass
        else:
            grouped[aid][field] = rec.get_value()
        # tags we might need
        if "model_id" in vals:
            grouped[aid]["model_id"] = vals.get("model_id")
    out: List[Account] = []
    for aid, data in grouped.items():
        created_at = _from_ns(created_ns.get(aid)) if created_ns.get(aid) else times.get(aid)
        out.append(
            Account(
                account_id=aid,
                model_id=str(data.get("model_id", "")),
                base_currency=str(data.get("base_currency", "USD")),
                starting_equity=float(data.get("starting_equity", 0.0) or 0.0),
                cash_balance=float(data.get("cash_balance", 0.0) or 0.0),
                equity=float(data.get("equity", 0.0) or 0.0),
                pnl=float(data.get("pnl", 0.0) or 0.0),
                created_at=created_at or datetime.now(tz=timezone.utc),
                updated_at=times.get(aid) or datetime.now(tz=timezone.utc),
            )
        )
    return out


def _record_to_position(record: FluxRecord) -> Position:
    values = record.values
    return Position(
        position_id=str(values.get("position_id")),
        account_id=str(values.get("account_id")),
        instrument_id=str(values.get("instrument_id")),
        side=str(values.get("side")),
        quantity=float(values.get("quantity", 0.0)),
        entry_price=float(values.get("entry_price", 0.0)),
        mark_price=_optional_float(values.get("mark_price")),
        leverage=_optional_float(values.get("leverage")),
        unrealized_pnl=_optional_float(values.get("unrealized_pnl")),
        updated_at=record.get_time(),
    )


def _merge_position_records(records: List[FluxRecord]) -> List[Position]:
    by_pos: dict[str, dict] = {}
    by_time: dict[str, datetime] = {}
    for rec in records:
        v = rec.values
        pid = str(v.get("position_id"))
        field = str(v.get("_field"))
        by_pos.setdefault(pid, {
            "position_id": pid,
            "account_id": str(v.get("account_id")),
            "instrument_id": str(v.get("instrument_id", "")),
        })
        by_time[pid] = rec.get_time()
        by_pos[pid][field] = rec.get_value()
    out: List[Position] = []
    for pid, data in by_pos.items():
        out.append(
            Position(
                position_id=pid,
                account_id=str(data.get("account_id", "")),
                instrument_id=str(data.get("instrument_id", "")),
                side=str(data.get("side", "")),
                quantity=float(data.get("quantity", 0.0) or 0.0),
                entry_price=float(data.get("entry_price", 0.0) or 0.0),
                mark_price=_optional_float(data.get("mark_price")),
                leverage=_optional_float(data.get("leverage")),
                unrealized_pnl=_optional_float(data.get("unrealized_pnl")),
                updated_at=by_time.get(pid) or datetime.now(tz=timezone.utc),
            )
        )
    return out


def _record_to_trade(record: FluxRecord) -> Trade:
    values = record.values
    executed_at = record.get_time()
    return Trade(
        trade_id=str(values.get("trade_id")),
        account_id=str(values.get("account_id")),
        model_id=str(values.get("model_id")),
        instrument_id=str(values.get("instrument_id")),
        side=str(values.get("side")),
        quantity=float(values.get("quantity", 0.0)),
        price=float(values.get("price", 0.0)),
        fee=_optional_float(values.get("fee")),
        realized_pnl=_optional_float(values.get("realized_pnl")),
        close_price=_optional_float(values.get("close_price")),
        executed_at=executed_at,
    )


def _merge_trade_records(records: List[FluxRecord]) -> List[Trade]:
    by_id: dict[str, dict] = {}
    times: dict[str, datetime] = {}
    for rec in records:
        v = rec.values
        tid = str(v.get("trade_id"))
        field = str(v.get("_field"))
        by_id.setdefault(
            tid,
            {
                "trade_id": tid,
                "account_id": str(v.get("account_id")),
                "model_id": str(v.get("model_id", "")),
                "instrument_id": str(v.get("instrument_id", "")),
            },
        )
        times[tid] = rec.get_time()
        by_id[tid][field] = rec.get_value()
    out: List[Trade] = []
    for tid, data in by_id.items():
        out.append(
            Trade(
                trade_id=tid,
                account_id=str(data.get("account_id", "")),
                model_id=str(data.get("model_id", "")),
                instrument_id=str(data.get("instrument_id", "")),
                side=str(data.get("side", "")),
                quantity=float(data.get("quantity", 0.0) or 0.0),
                price=float(data.get("price", 0.0) or 0.0),
                fee=_optional_float(data.get("fee")),
                realized_pnl=_optional_float(data.get("realized_pnl")),
                close_price=_optional_float(data.get("close_price")),
                executed_at=times.get(tid) or datetime.now(tz=timezone.utc),
            )
        )
    return out


def _record_to_balance(record: FluxRecord) -> Balance:
    values = record.values
    updated_at = record.get_time()
    currency = str(values.get("currency"))
    account_id = str(values.get("account_id"))
    balance_id = f"{account_id}-{currency}"
    return Balance(
        balance_id=balance_id,
        account_id=account_id,
        currency=currency,
        total=float(values.get("total", 0.0)),
        available=float(values.get("available", 0.0)),
        frozen=float(values.get("frozen", 0.0)),
        equity=float(values.get("equity", 0.0)),
        updated_at=updated_at,
    )


def _record_to_order(record: FluxRecord) -> Order:
    values = record.values
    updated_at = record.get_time()
    created_ns = values.get("created_at_ns")
    created_at = _from_ns(created_ns) if created_ns is not None else updated_at
    price = _optional_float(values.get("price"))
    average_price = _optional_float(values.get("average_price"))
    return Order(
        order_id=str(values.get("order_id")),
        account_id=str(values.get("account_id")),
        model_id=str(values.get("model_id", "")),
        instrument_id=str(values.get("instrument_id")),
        side=str(values.get("side")),
        order_type=str(values.get("order_type")),
        size=float(values.get("size", 0.0)),
        filled_size=float(values.get("filled_size", 0.0)),
        price=price,
        average_price=average_price,
        state=str(values.get("state")),
        created_at=created_at,
        updated_at=updated_at,
    )


def _merge_order_records(records: List[FluxRecord]) -> List[Order]:
    by_id: dict[str, dict] = {}
    updated: dict[str, datetime] = {}
    created_ns: dict[str, int] = {}
    for rec in records:
        v = rec.values
        oid = str(v.get("order_id"))
        field = str(v.get("_field"))
        by_id.setdefault(oid, {
            "order_id": oid,
            "account_id": str(v.get("account_id")),
            "model_id": str(v.get("model_id", "")),
            "instrument_id": str(v.get("instrument_id", "")),
        })
        updated[oid] = rec.get_time()
        if field == "created_at_ns":
            try:
                created_ns[oid] = int(rec.get_value())
            except Exception:
                pass
        else:
            by_id[oid][field] = rec.get_value()
    out: List[Order] = []
    for oid, data in by_id.items():
        created_at = _from_ns(created_ns.get(oid)) if created_ns.get(oid) else updated.get(oid)
        out.append(
            Order(
                order_id=oid,
                account_id=str(data.get("account_id", "")),
                model_id=str(data.get("model_id", "")),
                instrument_id=str(data.get("instrument_id", "")),
                side=str(data.get("side", "")),
                order_type=str(data.get("order_type", "")),
                size=float(data.get("size", 0.0) or 0.0),
                filled_size=float(data.get("filled_size", 0.0) or 0.0),
                price=_optional_float(data.get("price")),
                average_price=_optional_float(data.get("average_price")),
                state=str(data.get("state", "")),
                created_at=created_at or datetime.now(tz=timezone.utc),
                updated_at=updated.get(oid) or datetime.now(tz=timezone.utc),
            )
        )
    return out


def _optional_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_time_ns(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000_000)


def _from_ns(ns: object) -> datetime:
    try:
        ns_int = int(ns)
    except (TypeError, ValueError):
        return datetime.now(tz=timezone.utc)
    seconds, nanos = divmod(ns_int, 1_000_000_000)
    return datetime.fromtimestamp(seconds + nanos / 1_000_000_000, tz=timezone.utc)
