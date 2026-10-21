"""Offline checks for colour names on product records."""

import json
from unittest.mock import patch

import requests

from tap_tilroy.tap import TapTilroy

CONFIG = {
    "api_url": "https://api.tilroy.com",
    "tilroy_api_key": "offline-tenant",
    "x_api_key": "offline-gateway",
    "start_date": "2026-01-01T12:30:00Z",
}
SHADES = {"tilroyId": 1, "code": "LIPSTICK", "colours": [{"code": "c1", "skus": []}, {"code": "c2", "skus": []}]}
PLAIN = {"tilroyId": 2, "code": "PERFUME", "colours": [{"code": "NoColour", "skus": []}]}
DETAIL = {"tilroyId": 1, "colours": [
    {"code": "c2", "descriptions": [{"languageCode": "EN", "standard": "FIERCE"}]},
    {"code": "c1", "descriptions": [{"languageCode": "EN", "standard": "SINFUL"}]},
]}


def response(data):
    result = requests.Response()
    result.status_code = 200
    result._content = json.dumps(data).encode()
    return result


def sync(state, detail):
    stream = TapTilroy(config=CONFIG, state=state).streams["products"]
    stream.clear_collected_ids()

    def fetch(prepared, _context):
        if "/v2/products/" not in prepared.url:
            return response([SHADES, PLAIN])
        if isinstance(detail, Exception):
            raise detail
        return response(detail)

    with patch.object(requests.Session, "send", side_effect=AssertionError("Live HTTP forbidden")), \
            patch.object(stream, "_request_with_backoff", side_effect=fetch) as calls:
        records = [json.loads(json.dumps(r)) for r in stream.get_records(None)]
    return records, [c.args[0].url for c in calls.call_args_list]


def colour_names(record):
    return [(c.get("descriptions") or [{}])[0].get("standard") for c in record["colours"]]


def test_full_sync_adds_colour_names_by_code_only_for_coloured_products():
    records, urls = sync({}, DETAIL)
    assert [colour_names(r) for r in records] == [["SINFUL", "FIERCE"], [None]]
    assert [u for u in urls if "/v2/products/" in u] == ["https://api.tilroy.com/product-bulk/production/v2/products/1"]


def test_full_sync_keeps_product_when_v2_fails():
    records, _ = sync({}, requests.ConnectionError("down"))
    assert [r["tilroyId"] for r in records] == [1, 2]
    assert colour_names(records[0]) == [None, None]


def test_incremental_sync_makes_no_v2_calls():
    state = {"bookmarks": {"products": {"replication_key": "extraction_timestamp",
                                        "replication_key_value": "2026-10-01T00:00:00+00:00"}}}
    _, urls = sync(state, DETAIL)
    assert urls and all("/export/products" in u for u in urls)
