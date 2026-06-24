"""Purchase Orders stream for Tilroy API."""

from __future__ import annotations

import typing as t
from datetime import datetime, timezone

from singer_sdk import typing as th

from tap_tilroy.client import TilroyStream


if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


# All statuses to query (API requires status with warehouseNumber)
PURCHASE_ORDER_STATUSES = ["draft", "open", "delivered", "cancelled", "backorder"]


class PurchaseOrdersStream(TilroyStream):
    """Stream for Tilroy purchase orders.

    The Tilroy purchase-orders endpoint exposes modified timestamps on records,
    but the live API ignores tested modified/dateModified request parameters.
    Therefore this stream fetches the paginated purchase-order set and applies
    the modified cursor client-side. This prevents old orders with later status
    changes from being skipped by an orderDate bookmark.

    Note: warehouseNumber filter requires status filter.
    """

    name = "purchase_orders"
    path = "/purchaseapi/production/purchaseorders"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = "modified_timestamp"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    default_count = 100

    schema = th.PropertiesList(
        th.Property("tilroyId", th.CustomType({"type": ["string", "integer"]})),
        th.Property("number", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("orderDate", th.DateTimeType),
        th.Property("supplier", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("supplierReference", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("requestedDeliveryDate", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("warehouse", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("currency", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("prices", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("status", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("created", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("modified", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("modified_timestamp", th.DateTimeType),
        th.Property(
            "lines",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "sku",
                        th.ObjectType(
                            th.Property("tilroyId", th.CustomType({"type": ["string", "number", "null"]})),
                            th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
                        ),
                    ),
                    th.Property(
                        "warehouse",
                        th.ObjectType(
                            th.Property("number", th.IntegerType),
                            th.Property("name", th.CustomType({"type": ["string", "number", "null"]})),
                        ),
                    ),
                    th.Property("status", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("requestedDeliveryDate", th.DateTimeType),
                    th.Property(
                        "qty",
                        th.ObjectType(
                            th.Property("ordered", th.IntegerType),
                            th.Property("delivered", th.IntegerType),
                            th.Property("backOrder", th.IntegerType),
                            th.Property("cancelled", th.IntegerType),
                        ),
                    ),
                    th.Property("prices", th.CustomType({"type": ["object", "string", "null"]})),
                    th.Property("discount", th.CustomType({"type": ["object", "string", "null"]})),
                    th.Property("id", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("created", th.CustomType({"type": ["object", "string", "null"]})),
                    th.Property("modified", th.CustomType({"type": ["object", "string", "null"]})),
                )
            ),
        ),
    ).to_dict()

    @staticmethod
    def _parse_timestamp(value: object) -> datetime | None:
        """Parse a Tilroy timestamp into a comparable aware datetime."""
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, str) and value:
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
        else:
            return None

        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed

    def _get_stream_state(self) -> dict:
        """Return the global stream state when available."""
        try:
            state = self.get_context_state(None)
        except AttributeError:
            state = None

        if isinstance(state, dict):
            return state

        return {}

    def _get_start_date(self) -> datetime:
        """Determine the modified start date from the global bookmark.

        Legacy deployments bookmarked this stream by orderDate. That bookmark is
        intentionally ignored once the replication key changes, otherwise old
        orders modified after their original order date would remain skipped.
        """
        stream_state = self._get_stream_state()
        if stream_state.get("replication_key") not in (None, self.replication_key):
            self.logger.info(
                "[%s] Ignoring legacy %s bookmark while switching to %s",
                self.name,
                stream_state.get("replication_key"),
                self.replication_key,
            )
        else:
            bookmark_date = stream_state.get("replication_key_value")
            if not bookmark_date:
                bookmark_date = self.get_starting_timestamp(None)
            if bookmark_date:
                parsed = self._parse_timestamp(bookmark_date)
                if parsed:
                    return parsed

        config_start = self.config.get("start_date", "2010-01-01T00:00:00Z")
        parsed_config = self._parse_timestamp(config_start)
        if parsed_config:
            return parsed_config

        date_part = str(config_start).split("T")[0]
        return datetime.strptime(date_part, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    def _get_request_params(
        self,
        warehouse_id: int | None,
        status: str | None,
        page: int,
    ) -> dict[str, t.Any]:
        """Build request params for the unfiltered purchase-order page.

        The live API ignores modified/dateModified query params on this endpoint,
        so incremental filtering happens after parsing records.
        """
        params: dict[str, t.Any] = {
            "count": self.default_count,
            "page": page,
        }

        if warehouse_id and status:
            params["warehouseNumber"] = warehouse_id
            params["status"] = status

        return params

    def _fetch_page(
        self,
        warehouse_id: int | None,
        status: str | None,
        page: int,
    ) -> tuple[list[dict], bool]:
        """Fetch a single page of purchase orders."""
        params = self._get_request_params(warehouse_id, status, page)

        prepared = self.build_prepared_request(
            method="GET",
            url=self.get_url(None),
            params=params,
            headers=self.http_headers,
        )
        response = self._request_with_backoff(prepared, None)

        current_page = int(response.headers.get("X-Paging-CurrentPage", 1))
        total_pages = int(response.headers.get("X-Paging-PageCount", 1))
        has_more = current_page < total_pages

        records = list(self.parse_response(response))

        return records, has_more

    def _fetch_all_for_filter(
        self,
        warehouse_id: int | None,
        status: str | None,
    ) -> t.Iterable[dict]:
        """Fetch all pages for a warehouse/status combination.

        Args:
            warehouse_id: Warehouse number filter.
            status: Status filter.

        Yields:
            Purchase order records.
        """
        page = 1
        while True:
            records, has_more = self._fetch_page(warehouse_id, status, page)
            
            # Break if no records returned (avoid infinite loop)
            if not records:
                break
            yield from records

            if not has_more:
                break
            page += 1

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Fetch purchase orders and filter records by modified timestamp.

        This avoids partition-based state. Single global bookmark is maintained.
        """
        start_date = self._get_start_date()
        warehouse_ids = getattr(self._tap, "_resolved_shop_ids", [])

        if not warehouse_ids:
            self.logger.info(
                "[%s] Fetching all purchase orders modified since %s",
                self.name,
                start_date.isoformat(),
            )
            raw_records = self._fetch_all_for_filter(None, None)
        else:
            self.logger.info(
                "[%s] Fetching purchase orders for warehouses %s modified since %s",
                self.name,
                warehouse_ids,
                start_date.isoformat(),
            )
            raw_records = (
                record
                for wh_id in warehouse_ids
                for status in PURCHASE_ORDER_STATUSES
                for record in self._fetch_all_for_filter(wh_id, status)
            )

        for record in raw_records:
            processed = self.post_process(record, context)
            if not processed:
                continue

            modified_at = self._parse_timestamp(processed.get(self.replication_key))
            if modified_at and modified_at >= start_date:
                yield processed

    def post_process(
        self,
        row: dict,
        context: Context | None = None,  # noqa: ARG002
    ) -> dict | None:
        """Post-process purchase order record.

        Validates required fields and converts date formats.
        """
        if not row:
            return None

        # Skip error responses
        if "code" in row and "message" in row:
            self.logger.warning(
                "[%s] Skipping error record: %s",
                self.name,
                row["message"],
            )
            return None

        # Validate orderDate exists
        if not row.get("orderDate"):
            self.logger.warning("[%s] Skipping record without orderDate", self.name)
            return None

        # Parse orderDate string to datetime
        order_date = row["orderDate"]
        parsed_order_date = self._parse_timestamp(order_date)
        if parsed_order_date:
            row["orderDate"] = parsed_order_date
        else:
            self.logger.warning(
                "[%s] Invalid orderDate format: %s",
                self.name,
                order_date,
            )
            return None

        modified_at = None
        modified = row.get("modified")
        if isinstance(modified, dict):
            modified_at = self._parse_timestamp(modified.get("timestamp"))

        # Some historical/source records may not carry modified metadata. Keep
        # them syncable by falling back to orderDate, while normal records use
        # the actual modified timestamp as the incremental cursor.
        row[self.replication_key] = modified_at or parsed_order_date

        return row
