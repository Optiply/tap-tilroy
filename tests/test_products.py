"""Tests for Tilroy product streams."""

from __future__ import annotations

from tap_tilroy.streams.products import ProductsStream


def test_products_schema_exposes_sku_moq() -> None:
    """The export stream advertises Tilroy's SKU-level MOQ field."""
    sku_properties = (
        ProductsStream.schema["properties"]["colours"]["items"]["properties"]
        ["skus"]["items"]["properties"]
    )

    assert sku_properties["MOQ"] == {"type": ["number", "null"]}
