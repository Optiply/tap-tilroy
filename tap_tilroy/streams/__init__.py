"""Tilroy tap stream modules."""

from tap_tilroy.streams.shops import ShopsStream
from tap_tilroy.streams.products import ProductDetailsStream, ProductsStream, SuppliersStream
from tap_tilroy.streams.sales import SalesStream
from tap_tilroy.streams.stock import StockStream, StockChangesStream, StockDeltasStream
from tap_tilroy.streams.prices import PricesStream
from tap_tilroy.streams.purchase import PurchaseOrdersStream
from tap_tilroy.streams.purchase_exports import PurchaseOrderExportsStream
from tap_tilroy.streams.transfers import TransfersStream

__all__ = [
    "ShopsStream",
    "ProductDetailsStream",
    "ProductsStream",
    "SuppliersStream",
    "SalesStream",
    "StockStream",
    "StockChangesStream",
    "StockDeltasStream",
    "PricesStream",
    "PurchaseOrdersStream",
    "PurchaseOrderExportsStream",
    "TransfersStream",
]
