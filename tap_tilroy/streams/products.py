"""Product and Supplier streams for Tilroy API."""

from __future__ import annotations

import time
import typing as t
from datetime import datetime, timezone

from singer_sdk import typing as th

from tap_tilroy.client import DynamicRoutingStream, TilroyStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


# Full product detail (including sku.code) only from single-product endpoint
PRODUCT_V2_PATH = "/product-bulk/production/v2/products"


class ProductsStream(DynamicRoutingStream):
    """Lightweight product stream that paginates the list endpoint directly.

    Emits records from the list response (no individual v2 fetches).
    Primary purpose: collect SKU IDs for use by PricesStream and StockStream.

    - First sync (no state): GET /product-bulk/production/products
    - Incremental: GET /product-bulk/production/export/products?dateFrom=X
    """

    name = "products"
    historical_path = "/product-bulk/production/products"
    incremental_path = "/product-bulk/production/export/products"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = "extraction_timestamp"  # Synthetic replication key
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    default_count = 1000  # Product API allows up to 1000 per page

    # SKU IDs from colours[].skus[].tilroyId — consumed by PricesStream and StockStream.
    # Product IDs from product-level tilroyId — consumed by ProductDetailsStream.
    # _collected_product_ids_set is a dedup guard; _collected_product_ids preserves order.
    _collected_sku_ids: t.ClassVar[set[str]] = set()
    _collected_product_ids: t.ClassVar[list[str]] = []
    _collected_product_ids_set: t.ClassVar[set[str]] = set()

    schema = th.PropertiesList(
        th.Property("tilroyId", th.CustomType({"type": ["string", "integer"]})),
        th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property(
            "descriptions",
            th.ArrayType(
                th.ObjectType(
                    th.Property("languageCode", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("standard", th.CustomType({"type": ["string", "number", "null"]})),
                )
            ),
        ),
        th.Property("supplier", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("brand", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property(
            "colours",
            th.ArrayType(
                th.ObjectType(
                    th.Property("tilroyId", th.CustomType({"type": ["string", "integer"]})),
                    th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property(
                        "skus",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("tilroyId", th.CustomType({"type": ["string", "integer"]})),
                                th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
                                th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                                th.Property("costPrice", th.NumberType),
                                th.Property(
                                    "barcodes",
                                    th.ArrayType(
                                        th.ObjectType(
                                            th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                                            th.Property("quantity", th.NumberType),
                                            th.Property("isInternal", th.BooleanType),
                                        )
                                    ),
                                ),
                                th.Property(
                                    "size",
                                    th.ObjectType(
                                        th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                                    ),
                                ),
                                th.Property("rrp", th.CustomType({"type": ["array", "object", "string", "null"]})),
                                th.Property(
                                    "lifeStatus",
                                    th.ObjectType(
                                        th.Property("code", th.StringType),
                                    ),
                                ),
                            )
                        ),
                    ),
                    th.Property("pictures", th.ArrayType(th.ObjectType())),
                )
            ),
        ),
        th.Property("isUsed", th.BooleanType),
        th.Property("suppliers", th.CustomType({"type": ["array", "object", "string", "null"]})),
        th.Property("extraction_timestamp", th.DateTimeType),
    ).to_dict()

    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """Paginate list endpoint directly. No individual v2 fetches."""
        is_incremental = self._has_existing_state()
        list_path = self.incremental_path if is_incremental else self.historical_path
        start_time = time.time()

        sync_type = "INCREMENTAL" if is_incremental else "HISTORICAL"
        self.logger.info(
            f"=== [{self.name}] STARTING {sync_type} SYNC === (path: {list_path})"
        )

        page = 1
        total_yielded = 0

        while True:
            params: dict[str, t.Any] = {"count": self.default_count, "page": page}

            if is_incremental:
                start_date = self._get_start_date(context)
                params["dateFrom"] = start_date.strftime("%Y-%m-%d")

            list_url = f"{self.url_base.rstrip('/')}{list_path}"
            prepared = self.build_prepared_request(
                method="GET",
                url=list_url,
                params=params,
                headers=self.http_headers,
            )

            try:
                response = self._request_with_backoff(prepared, context)
            except Exception as e:
                self.logger.error(f"[{self.name}] Request failed: {e}")
                break
            if response.status_code != 200:
                self.logger.error(
                    f"[{self.name}] API error {response.status_code}: {response.text[:500]}"
                )
                break

            records = list(self.parse_response(response))
            page_count = len(records)

            for record in records:
                processed = self.post_process(record, context)
                if processed:
                    total_yielded += 1
                    yield processed

            total_pages = None
            try:
                current = int(response.headers.get("X-Paging-CurrentPage", 1))
                total_pages = int(response.headers.get("X-Paging-PageCount", 1))
            except (ValueError, TypeError):
                current = page

            pct = (100 * page / total_pages) if total_pages else 0
            self.logger.info(
                f"[{self.name}] Page {page}/{total_pages or '?'} ({pct:.0f}%) | "
                f"+{page_count} products | Total: {total_yielded:,}"
            )

            if page_count < self.default_count:
                break
            if total_pages and current >= total_pages:
                break

            page += 1

        elapsed = time.time() - start_time
        self.logger.info(
            f"=== [{self.name}] {sync_type} SYNC COMPLETE === "
            f"{total_yielded:,} products in {elapsed:.0f}s"
        )

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Add extraction timestamp and collect SKU IDs."""
        if not row:
            return None

        row["extraction_timestamp"] = datetime.now(timezone.utc).isoformat()
        self._normalize_sku_life_status(row)
        self._collect_ids(row)

        return row

    @staticmethod
    def _normalize_sku_life_status(product: dict) -> None:
        """Ensure lifeStatus is always {"code": ...}, even if API returns a plain string."""
        for colour in product.get("colours") or []:
            if not isinstance(colour, dict):
                continue
            for sku in colour.get("skus") or []:
                if not isinstance(sku, dict):
                    continue
                ls = sku.get("lifeStatus")
                if isinstance(ls, str):
                    sku["lifeStatus"] = {"code": ls}
                elif ls is not None and not isinstance(ls, dict):
                    sku["lifeStatus"] = None

    def _collect_ids(self, product: dict) -> None:
        """Extract and store product ID and SKU IDs from product record."""
        product_id = product.get("tilroyId")
        if product_id:
            pid_str = str(product_id)
            if pid_str not in self._collected_product_ids_set:
                self._collected_product_ids_set.add(pid_str)
                self._collected_product_ids.append(pid_str)

                if len(self._collected_product_ids) % 1000 == 0:
                    self.logger.info(
                        f"[{self.name}] Collected {len(self._collected_product_ids)} product IDs..."
                    )

        self._collect_sku_ids(product)

    def _collect_sku_ids(self, product: dict) -> None:
        """Extract and store SKU IDs from product record."""
        colours = product.get("colours", [])
        if not isinstance(colours, list):
            return

        for colour in colours:
            skus = colour.get("skus", [])
            if not isinstance(skus, list):
                continue

            for sku in skus:
                sku_id = sku.get("tilroyId")
                if sku_id:
                    sku_id_str = str(sku_id)
                    if sku_id_str not in self._collected_sku_ids:
                        self._collected_sku_ids.add(sku_id_str)

                        if len(self._collected_sku_ids) % 500 == 0:
                            self.logger.info(
                                f"[{self.name}] Collected {len(self._collected_sku_ids)} SKU IDs..."
                            )

    @classmethod
    def get_collected_sku_ids(cls) -> list[str]:
        """Get the collected SKU IDs for use by PricesStream/StockStream."""
        return list(cls._collected_sku_ids)

    @classmethod
    def get_collected_product_ids(cls) -> list[str]:
        """Get the collected product IDs for use by ProductDetailsStream."""
        return list(cls._collected_product_ids)

    @classmethod
    def clear_collected_ids(cls) -> None:
        """Clear all collected IDs."""
        cls._collected_sku_ids.clear()
        cls._collected_product_ids.clear()
        cls._collected_product_ids_set.clear()

    def finalize_child_contexts(self) -> None:
        """Log final collection statistics."""
        self.logger.info(
            f"[{self.name}] Final: {len(self._collected_product_ids)} product IDs, "
            f"{len(self._collected_sku_ids)} SKU IDs collected"
        )


class ProductDetailsStream(TilroyStream):
    """Full product detail stream — pure singular v2/products/{id} calls.

    Consumes product IDs collected by ProductsStream (which runs first),
    then fetches each product individually via GET v2/products/{id}.
    No list pagination of its own.
    """

    name = "product_details"
    path = PRODUCT_V2_PATH  # Only used by SDK internals; actual URLs built manually
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = "extraction_timestamp"
    replication_method = "INCREMENTAL"

    # Full singular-response schema so no fields are dropped by schema validation
    schema = th.PropertiesList(
        th.Property("tilroyId", th.CustomType({"type": ["string", "integer"]})),
        th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("codeAlt", th.StringType),
        th.Property("serialNumberSale", th.BooleanType),
        th.Property("supplierReference", th.StringType),
        th.Property("supplierDescription", th.StringType),
        th.Property("visibleOnline", th.BooleanType),
        th.Property("priceInfo", th.StringType),
        th.Property("nextSaleDiscounts", th.ObjectType()),
        th.Property(
            "descriptions",
            th.ArrayType(
                th.ObjectType(
                    th.Property("languageCode", th.StringType),
                    th.Property("standard", th.StringType),
                    th.Property("web", th.StringType),
                    th.Property("webName", th.StringType),
                    th.Property("seoTitle", th.StringType),
                    th.Property("seoDescription", th.StringType),
                    th.Property("ticket", th.StringType),
                    th.Property("webShort", th.StringType),
                    th.Property("priceLabelDescription", th.StringType),
                    th.Property("canonicalUrl", th.StringType),
                    th.Property("videoPath", th.StringType),
                    th.Property("freeTexts", th.ArrayType(th.StringType)),
                )
            ),
        ),
        th.Property("supplier", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("brand", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("uom", th.ObjectType(
            th.Property("code", th.StringType),
            th.Property("showOnline", th.BooleanType),
        )),
        th.Property("sizeRange", th.CustomType({"type": ["object", "null"]})),
        th.Property("countryVats", th.ArrayType(th.ObjectType())),
        th.Property("gender", th.CustomType({"type": ["object", "null"]})),
        th.Property("groupLevels", th.ArrayType(th.ObjectType())),
        th.Property(
            "colours",
            th.ArrayType(
                th.ObjectType(
                    th.Property("tilroyId", th.CustomType({"type": ["string", "integer", "null"]})),
                    th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("supplierReference", th.StringType),
                    th.Property("baseColour", th.CustomType({"type": ["object", "null"]})),
                    th.Property("descriptions", th.ArrayType(th.ObjectType())),
                    th.Property("productColourDescriptions", th.ArrayType(th.ObjectType())),
                    th.Property("season", th.CustomType({"type": ["object", "null"]})),
                    th.Property(
                        "skus",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("tilroyId", th.CustomType({"type": ["string", "integer"]})),
                                th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
                                th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                                th.Property("consumerPrice", th.NumberType),
                                th.Property("costPrice", th.NumberType),
                                th.Property("maxDiscount", th.NumberType),
                                th.Property("lifeStatus", th.StringType),
                                th.Property("wholeSalePrice", th.NumberType),
                                th.Property("origin", th.StringType),
                                th.Property("length", th.NumberType),
                                th.Property("width", th.NumberType),
                                th.Property("height", th.NumberType),
                                th.Property("content", th.NumberType),
                                th.Property("MOQ", th.NumberType),
                                th.Property("IOQ", th.NumberType),
                                th.Property(
                                    "barcodes",
                                    th.ArrayType(
                                        th.ObjectType(
                                            th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                                            th.Property("quantity", th.NumberType),
                                            th.Property("isInternal", th.BooleanType),
                                        )
                                    ),
                                ),
                                th.Property(
                                    "size",
                                    th.ObjectType(
                                        th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                                        th.Property("sizeOrder", th.NumberType),
                                        th.Property("skuCode", th.StringType),
                                    ),
                                ),
                                th.Property("weight", th.CustomType({"type": ["object", "null"]})),
                            )
                        ),
                    ),
                    th.Property(
                        "pictures",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("name", th.StringType),
                                th.Property("isDefault", th.BooleanType),
                                th.Property("sortOrder", th.NumberType),
                                th.Property("swatch", th.BooleanType),
                                th.Property("showOnline", th.BooleanType),
                            )
                        ),
                    ),
                )
            ),
        ),
        th.Property("used", th.CustomType({"type": ["object", "null"]})),
        th.Property("configurator", th.CustomType({"type": ["object", "null"]})),
        th.Property("readOnly", th.BooleanType),
        th.Property("deliverynoteAllowed", th.BooleanType),
        th.Property("extraction_timestamp", th.DateTimeType),
    ).to_dict()

    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """Fetch each product individually via GET v2/products/{id}.

        Uses product IDs collected by ProductsStream (which runs first).
        """
        product_ids = ProductsStream.get_collected_product_ids()

        if not product_ids:
            self.logger.warning(
                f"[{self.name}] No product IDs found. Ensure ProductsStream runs first."
            )
            return

        total = len(product_ids)
        total_yielded = 0
        start_time = time.time()
        last_log_time = start_time

        self.logger.info(
            f"=== [{self.name}] STARTING FULL SYNC === ({total:,} products to fetch)"
        )

        for i, product_id in enumerate(product_ids, 1):
            single_url = f"{self.url_base.rstrip('/')}{PRODUCT_V2_PATH}/{product_id}"
            prepared = self.build_prepared_request(
                method="GET",
                url=single_url,
                params={},
                headers=self.http_headers,
            )

            try:
                response = self._request_with_backoff(prepared, context)
            except Exception as e:
                self.logger.warning(
                    f"[{self.name}] Product {product_id} failed: {e}"
                )
                continue

            if response.status_code == 404:
                continue
            if response.status_code != 200:
                self.logger.warning(
                    f"[{self.name}] Product {product_id}: HTTP {response.status_code}"
                )
                continue

            try:
                full_product = response.json()
            except Exception:
                continue

            if isinstance(full_product, dict) and full_product.get("tilroyId") is not None:
                processed = self.post_process(full_product, context)
                if processed:
                    total_yielded += 1
                    yield processed

            now = time.time()
            if now - last_log_time >= 10 or i % 500 == 0:
                elapsed = now - start_time
                rate = i / elapsed if elapsed > 0 else 0
                remaining = total - i
                eta_secs = remaining / rate if rate > 0 else 0
                eta_min = int(eta_secs // 60)
                eta_sec = int(eta_secs % 60)

                self.logger.info(
                    f"[{self.name}] Progress: {i:,}/{total:,} ({100*i/total:.1f}%) | "
                    f"{total_yielded:,} records | {rate:.1f}/s | ETA: {eta_min}m {eta_sec}s"
                )
                last_log_time = now

        elapsed = time.time() - start_time
        rate = total / elapsed if elapsed > 0 else 0
        self.logger.info(
            f"=== [{self.name}] FULL SYNC COMPLETE === "
            f"{total:,} products -> {total_yielded:,} records in {elapsed:.0f}s ({rate:.1f}/s)"
        )

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Add extraction_timestamp only; pass through full singular response."""
        if not row:
            return None
        row["extraction_timestamp"] = datetime.now(timezone.utc).isoformat()
        return row


class SuppliersStream(TilroyStream):
    """Stream for Tilroy suppliers.

    Fetches supplier master data. This is typically a small dataset.
    """

    name = "suppliers"
    path = "/product-bulk/production/suppliers"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = None
    replication_method = "FULL_TABLE"
    records_jsonpath = "$[*]"
    default_count = 10000  # Suppliers are typically few, fetch all at once
    _preserve_null_like_string_fields = frozenset({"code", "name"})

    schema = th.PropertiesList(
        th.Property("tilroyId", th.IntegerType),
        # Supplier codes are identifiers, not numeric values. Keep them as strings
        # so leading zeroes from Tilroy (for example "00560") survive CSV output.
        th.Property("code", th.CustomType({"type": ["string", "null"]})),
        th.Property("name", th.CustomType({"type": ["string", "null"]})),
    ).to_dict()
