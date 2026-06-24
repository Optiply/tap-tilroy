"""Tests for the Tilroy purchase-orders stream."""

# ruff: noqa: S101, SLF001

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from tap_tilroy.streams.purchase import PurchaseOrdersStream


class DummyLogger:
    """Minimal logger for object.__new__ stream tests."""

    def info(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return None

    def warning(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return None


def make_stream() -> PurchaseOrdersStream:
    """Create a lightweight stream instance without live tap initialization."""
    stream = object.__new__(PurchaseOrdersStream)
    stream.logger = DummyLogger()
    stream.name = PurchaseOrdersStream.name
    stream.replication_key = PurchaseOrdersStream.replication_key
    stream.default_count = PurchaseOrdersStream.default_count
    stream._config = {"start_date": "2019-01-01T00:00:00Z"}
    stream._tap = SimpleNamespace(_resolved_shop_ids=[])
    return stream


def test_request_params_do_not_use_order_date_cursor() -> None:
    """Purchase-order requests fetch pages; modified filtering is client-side."""
    stream = make_stream()

    params = stream._get_request_params(warehouse_id=20, status="delivered", page=3)

    assert params == {
        "count": PurchaseOrdersStream.default_count,
        "page": 3,
        "warehouseNumber": 20,
        "status": "delivered",
    }
    assert "orderDateFrom" not in params
    assert "orderDateTo" not in params


def test_post_process_uses_modified_timestamp_as_replication_key() -> None:
    """Modified timestamp is exposed as a top-level Singer replication key."""
    stream = make_stream()
    row = {
        "tilroyId": "6952898fd773c71239f3f755",
        "number": "PO-179",
        "orderDate": "2026-01-02T09:00:00.000Z",
        "modified": {"timestamp": "2026-03-26T13:24:00.000Z"},
    }

    processed = stream.post_process(row)

    assert processed is not None
    assert processed["modified_timestamp"] == datetime(
        2026,
        3,
        26,
        13,
        24,
        tzinfo=timezone.utc,
    )
    assert processed["orderDate"] == datetime(2026, 1, 2, 9, 0, tzinfo=timezone.utc)


def test_get_records_filters_by_modified_timestamp() -> None:
    """Old order dates still emit when the modified timestamp is inside state."""
    stream = make_stream()
    stream._get_start_date = lambda: datetime(2026, 3, 1, tzinfo=timezone.utc)  # type: ignore[method-assign]
    stream._fetch_all_for_filter = lambda _warehouse_id, _status: iter(  # type: ignore[method-assign]
        [
            {
                "tilroyId": "old-change",
                "number": "PO-179",
                "orderDate": "2026-01-02T09:00:00.000Z",
                "modified": {"timestamp": "2026-03-26T13:24:00.000Z"},
            },
            {
                "tilroyId": "too-old",
                "number": "PO-1",
                "orderDate": "2026-01-01T09:00:00.000Z",
                "modified": {"timestamp": "2026-02-01T13:24:00.000Z"},
            },
        ]
    )

    records = list(stream.get_records(None))

    assert [record["number"] for record in records] == ["PO-179"]
