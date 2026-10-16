"""Offline contract checks for purchase order exports."""

import json
from urllib.parse import parse_qs, urlparse
from unittest.mock import patch

import pytest
import requests
from jsonschema import validate
from singer_sdk.exceptions import FatalAPIError

from tap_tilroy.tap import TapTilroy
from tap_tilroy.streams.purchase_exports import PurchaseOrderExportsStream

CONFIG = {
    "api_url": "https://api.tilroy.com",
    "tilroy_api_key": "offline-tenant",
    "x_api_key": "offline-gateway",
    "start_date": "2026-01-01T12:30:00Z",
}
ORDER = {
    "tilroyId": "001", "dateExported": "2026-02-01T12:30:00Z",
    "number": "0001", "comment": "NA", "supplierReference": "0002",
    "lines": [{"id": "line-1", "warehouse": {"number": "001"}, "comment": "test"}],
}


def response(data, headers=None):
    result = requests.Response()
    result.status_code = 200
    result._content = json.dumps(data).encode()
    result.headers.update(headers or {})
    return result


def test_discovery_and_paginated_sync_resume():
    with patch.object(requests.Session, "send", side_effect=AssertionError("Live HTTP forbidden")):
        tap = TapTilroy(config=CONFIG)
        stream = tap.streams["purchase_order_exports"]
        assert stream.replication_key == "dateExported"
        assert stream.primary_keys == ["tilroyId"]
        validate(ORDER, stream.schema)
        catalog = tap.catalog.to_dict()
        assert "purchase_order_exports" in {s["stream"] for s in catalog["streams"]}
        assert "dateExported" not in tap.streams["purchase_orders"].schema["properties"]
        second = dict(ORDER, tilroyId="002", dateExported="2026-02-02T12:30:00Z")
        with patch.object(stream, "_request_with_backoff", side_effect=[response([ORDER]), response(second), response([])]) as fetch:
            stream.sync()
            stream.finalize_state_progress_markers()
        queries = [parse_qs(urlparse(call.args[0].url).query) for call in fetch.call_args_list]
        assert [q["page"] for q in queries] == [["1"], ["2"], ["3"]]
        assert len({q["dateExportedSince"][0] for q in queries}) == 1
        assert fetch.call_args.args[0].headers["Tilroy-Api-Key"] == "offline-tenant"
        assert fetch.call_args.args[0].headers["x-api-key"] == "offline-gateway"
        state = tap.state["bookmarks"][stream.name]
        assert state["replication_key_value"] == second["dateExported"]
        resumed = TapTilroy(config=CONFIG, state=tap.state).streams[stream.name]
        assert isinstance(resumed, PurchaseOrderExportsStream)
        with patch.object(resumed, "_request_with_backoff", return_value=response([])) as fetch:
            resumed.sync()
        query = parse_qs(urlparse(fetch.call_args.args[0].url).query)
        assert query["dateExportedSince"] == ["2026-02-01T12:30:00+00:00"]
        assert stream.post_process(dict(ORDER)) == ORDER


def test_paging_headers_stop_without_extra_request():
    stream = TapTilroy(config=CONFIG).streams["purchase_order_exports"]
    assert isinstance(stream, PurchaseOrderExportsStream)
    with patch.object(stream, "_request_with_backoff", return_value=response([ORDER], {"X-Paging-CurrentPage": "1", "X-Paging-PageCount": "1"})) as fetch:
        assert list(stream.request_records(None)) == [ORDER]
        assert fetch.call_count == 1


@pytest.mark.parametrize("bad_page", [
    None, {"code": 400, "message": "error"}, [{"tilroyId": "1"}],
    [dict(ORDER, dateExported="invalid")], [ORDER],
])
def test_bad_or_repeated_page_fails_without_committing_state(bad_page):
    tap = TapTilroy(config=CONFIG)
    stream = tap.streams["purchase_order_exports"]
    with patch.object(stream, "_request_with_backoff", side_effect=[response([ORDER]), response(bad_page)]):
        with pytest.raises(FatalAPIError):
            stream.sync()
    assert "replication_key_value" not in tap.state["bookmarks"][stream.name]


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_permanent_http_errors_fail(status):
    stream = TapTilroy(config=CONFIG).streams["purchase_order_exports"]
    assert isinstance(stream, PurchaseOrderExportsStream)
    result = response({"message": "error"})
    result.status_code = status
    with pytest.raises(FatalAPIError):
        stream.validate_response(result)
