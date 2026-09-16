"""Purchase order exports, independent of the purchase-order search stream."""

from __future__ import annotations

from copy import deepcopy
from datetime import timedelta
import typing as t

from singer_sdk.exceptions import FatalAPIError

from tap_tilroy.client import TilroyStream
from tap_tilroy.streams.purchase import PurchaseOrdersStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class PurchaseOrderExportsStream(TilroyStream):
    """Read exported orders with a stable per-run date filter and SDK state."""

    name = "purchase_order_exports"
    path = "/purchaseapi/production/export/orders"
    # Singer SDK supports declarative class attributes for keys and schemas.
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]  # type: ignore[reportIncompatibleMethodOverride]
    replication_key = "dateExported"
    replication_method = "INCREMENTAL"

    # Export orders share the search payload, but add export dates and comments.
    schema: t.ClassVar[dict[str, t.Any]] = deepcopy(  # type: ignore[reportIncompatibleMethodOverride]
        t.cast("dict[str, t.Any]", PurchaseOrdersStream.schema),
    )
    schema["properties"].pop("modified_timestamp")
    schema["properties"]["dateExported"] = {"type": "string", "format": "date-time"}
    schema["properties"]["comment"] = {"type": ["string", "null"]}
    schema["required"] = ["tilroyId", "dateExported"]
    schema["properties"]["lines"]["items"]["properties"]["comment"] = {
        "type": ["string", "null"],
    }
    schema["properties"]["lines"]["items"]["properties"]["warehouse"][
        "properties"
    ]["number"] = {"type": ["string", "integer", "null"]}

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        """Use the export bookmark, replaying a day to cover boundary ties."""
        params = super().get_url_params(context, next_page_token)
        start = self.get_starting_timestamp(context)
        if start:
            start -= timedelta(days=1)
        else:
            start = PurchaseOrdersStream._parse_timestamp(
                self.config.get("start_date") or "2010-01-01T00:00:00Z",
            )
        if start is None:
            raise ValueError("Invalid start_date for purchase_order_exports")
        params["dateExportedSince"] = start.isoformat()
        return params

    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """Page until empty or the header's final page; reject repeated pages.

        The docs show a single object despite describing a list. Accept both,
        but reject malformed/error payloads instead of silently advancing state.
        Without paging headers, request through the first empty page rather
        than assuming the server always honors count.
        """
        params = self.get_url_params(context, 1)
        seen_pages = set()
        while True:
            prepared = self.build_prepared_request(
                method="GET", url=self.get_url(context),
                params=params, headers=self.http_headers,
            )
            response = self._request_with_backoff(prepared, context)
            data = response.json()
            records = [data] if isinstance(data, dict) else data
            if not isinstance(records, list):
                raise FatalAPIError("Expected an export order or list of orders")
            for record in records:
                if (
                    not isinstance(record, dict)
                    or not record.get("tilroyId")
                    or not PurchaseOrdersStream._parse_timestamp(record.get("dateExported"))
                ):
                    raise FatalAPIError("Export record requires tilroyId and valid dateExported")
            if not records:
                return
            signature = tuple((str(r["tilroyId"]), r["dateExported"]) for r in records)
            if signature in seen_pages:
                raise FatalAPIError("Purchase export pagination repeated a page")
            seen_pages.add(signature)
            page_count = response.headers.get("X-Paging-PageCount")
            final_page = False
            if page_count is not None:
                try:
                    current = int(response.headers.get("X-Paging-CurrentPage", params["page"]))
                    total = int(page_count)
                except (ValueError, TypeError) as exc:
                    raise FatalAPIError("Invalid purchase export paging headers") from exc
                if current != params["page"] or total < current:
                    raise FatalAPIError("Inconsistent purchase export paging headers")
                final_page = current == total
            yield from records
            if final_page:
                return
            params["page"] += 1

    def post_process(
        self, row: dict, context: Context | None = None,
    ) -> dict:
        """Preserve source IDs, references and nested export data unchanged."""
        return row
