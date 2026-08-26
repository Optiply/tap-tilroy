"""Tilroy tap class."""

from __future__ import annotations

import json
import logging
import typing as t

import requests
from singer_sdk import Tap
from singer_sdk import typing as th
from singer_sdk.streams import Stream

from tap_tilroy.streams import (
    PricesStream,
    ProductDetailsStream,
    ProductsStream,
    PurchaseOrdersStream,
    SalesStream,
    ShopsStream,
    StockChangesStream,
    StockDeltasStream,
    StockStream,
    SuppliersStream,
    TransfersStream,
)

# Stream types in default order (products first for SKU collection; product_details after products)
STREAM_TYPES: list[type[Stream]] = [
    ProductsStream,
    ProductDetailsStream,
    ShopsStream,
    PurchaseOrdersStream,
    StockChangesStream,
    StockDeltasStream,
    SalesStream,
    SuppliersStream,
    PricesStream,
    StockStream,
    TransfersStream,
]


class TapTilroy(Tap):
    """Tilroy tap for extracting data from the Tilroy API.

    When loading state from a file (e.g. for testing), the file may contain
    either the state value only ({"bookmarks": {...}}) or a full STATE message
    ({"type": "STATE", "value": {"bookmarks": {...}}}). Both are accepted.

    This tap supports the following streams:
    - products: Product catalog with SKU information (run first for stock/prices)
    - product_details: Full product detail from singular GET v2/products/{id}; keep products on
    - shops: Store/location data
    - purchase_orders: Purchase order history
    - stock_changes: Inventory movement history (snapshots)
    - stock_deltas: Inventory change events with deltas (transfers, corrections, etc.)
    - sales: Sales transactions
    - suppliers: Supplier master data
    - prices: Price rules per SKU
    - stock: Current stock levels (depends on products for SKU IDs)
    - transfers: Stock movements between shops (inter-store transfers)
    """

    name = "tap-tilroy"
    
    # Path to config file for persisting config changes
    config_file: str | None = None

    config_jsonschema = th.PropertiesList(
        th.Property(
            "tilroy_api_key",
            th.StringType,
            required=True,
            secret=True,
            description="The Tilroy API key for authentication",
        ),
        th.Property(
            "x_api_key",
            th.StringType,
            required=True,
            secret=True,
            description="The AWS API Gateway key for authentication",
        ),
        th.Property(
            "api_url",
            th.StringType,
            required=True,
            description="The base URL for the Tilroy API (e.g., https://api.tilroy.com)",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest date to sync data from (ISO 8601 format)",
        ),
        th.Property(
            "shop_ids",
            th.StringType,
            description="Comma-separated shop tilroyIds to filter streams (e.g., '1,2,3'). Tap will auto-resolve shop numbers.",
        ),
        th.Property(
            "shop_numbers",
            th.StringType,
            description="Comma-separated shop numbers to filter streams (e.g., '1672,1673'). Tap will auto-resolve tilroyIds.",
        ),
    ).to_dict()

    # Resolved shop mappings (populated in __init__)
    _resolved_shop_ids: list[int]
    _resolved_shop_numbers: list[int]

    def __init__(
        self,
        config: dict | list[str] | None = None,
        catalog: dict | str | None = None,
        state: dict | str | None = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        **kwargs,
    ) -> None:
        """Initialize the tap.

        Suppresses excessive schema warnings for streams that return
        many undocumented fields. Also resolves shop ID/number mappings.
        Stores config file path for persisting config changes.
        """
        self._resolved_shop_ids = []
        self._resolved_shop_numbers = []

        if isinstance(config, list) and config:
            self.config_file = config[0]
        elif isinstance(config, str):
            self.config_file = config
        
        super().__init__(
            config=config,
            catalog=catalog,
            state=state,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
            **kwargs,
        )

        for stream_name in ("sales", "products"):
            logging.getLogger(f"tap-tilroy.{stream_name}").setLevel(logging.ERROR)

        self._resolve_shop_mappings()

    def load_state(self, state: dict[str, t.Any]) -> None:
        """Load state, accepting both value-only and full STATE message format.

        When state is read from a file that was saved from a STATE message
        (e.g. for local testing), it may be {"type": "STATE", "value": {...}}.
        The SDK expects {"bookmarks": {...}}. We unwrap so both work.
        """
        if isinstance(state, dict) and "value" in state and "bookmarks" not in state:
            state = state.get("value") or state
        super().load_state(state)

    def _write_config(self) -> None:
        """Write current config back to the config file.
        
        This persists any runtime config changes (e.g., resolved shop IDs,
        refreshed tokens, etc.) so they're available on the next run.
        """
        if not self.config_file:
            self.logger.debug("No config file path stored, skipping config write")
            return
        
        try:
            with open(self.config_file, "w") as outfile:
                json.dump(dict(self._config), outfile, indent=4)
            self.logger.info(f"Config saved to {self.config_file}")
        except Exception as e:
            self.logger.warning(f"Failed to write config to {self.config_file}: {e}")

    def _parse_csv_ids(self, value: str | list | None) -> list[int]:
        """Parse comma-separated string or list into list of integers.
        
        Args:
            value: Comma-separated string like "1,2,3" or list of ints.
            
        Returns:
            List of integers.
        """
        if not value:
            return []
        
        # Already a list (e.g., from state or programmatic config)
        if isinstance(value, list):
            return [int(v) for v in value if v]
        
        # Parse comma-separated string
        if isinstance(value, str):
            return [int(x.strip()) for x in value.split(",") if x.strip()]
        
        return []

    def _format_csv_ids(self, values: list[int]) -> str:
        """Format list of integers as comma-separated string.
        
        Args:
            values: List of integers.
            
        Returns:
            Comma-separated string like "1,2,3".
        """
        return ",".join(str(v) for v in values)

    def _resolve_shop_mappings(self) -> None:
        """Fetch shops and resolve ID/number mappings.
        
        If shop_ids and/or shop_numbers are configured, fetches shops from API,
        resolves IDs <-> numbers, and merges into one consistent set (so adding
        shop_ids to existing shop_numbers just adds more shops; order does not matter).
        Persists both lists back to config.
        
        Config values are comma-separated strings like "1,2,3".
        """
        filter_ids = self._parse_csv_ids(self.config.get("shop_ids"))
        filter_numbers = self._parse_csv_ids(self.config.get("shop_numbers"))
        
        if not filter_ids and not filter_numbers:
            self.logger.info("No shop filters configured - streams will fetch all data")
            return
        
        self.logger.info("Fetching shops to resolve ID/number mappings...")
        try:
            response = requests.get(
                f"{self.config['api_url']}/shopapi/production/shops",
                headers={
                    "Tilroy-Api-Key": self.config["tilroy_api_key"],
                    "x-api-key": self.config["x_api_key"],
                },
                timeout=30,
            )
            response.raise_for_status()
            shops = response.json()
        except Exception as e:
            self.logger.error(f"Failed to fetch shops for mapping: {e}")
            self._resolved_shop_ids = filter_ids
            self._resolved_shop_numbers = filter_numbers
            return
        
        id_to_number = {}
        number_to_id = {}
        for s in shops:
            try:
                tid = int(s.get("tilroyId", 0))
                num = int(s.get("number", 0))
                id_to_number[tid] = num
                number_to_id[num] = tid
            except (ValueError, TypeError):
                continue
        
        resolved_ids = set(filter_ids)
        resolved_numbers = set(filter_numbers)
        for sid in filter_ids:
            if sid in id_to_number:
                resolved_numbers.add(id_to_number[sid])
        for num in filter_numbers:
            if num in number_to_id:
                resolved_ids.add(number_to_id[num])
        
        self._resolved_shop_ids = sorted(resolved_ids)
        self._resolved_shop_numbers = [
            id_to_number[sid] for sid in self._resolved_shop_ids if sid in id_to_number
        ]
        self.logger.info(
            f"Resolved shop mappings: IDs {self._resolved_shop_ids}, "
            f"numbers {self._resolved_shop_numbers}"
        )
        
        if self._resolved_shop_ids and self._resolved_shop_numbers:
            self._config["shop_ids"] = self._format_csv_ids(self._resolved_shop_ids)
            self._config["shop_numbers"] = self._format_csv_ids(self._resolved_shop_numbers)
            self._write_config()

        """
        Load shop filters independently.
        self._resolved_shop_ids = self._parse_csv_ids(self.config.get("shop_ids"))
        self._resolved_shop_numbers = self._parse_csv_ids(
            self.config.get("shop_numbers")
        )
        """

    def discover_streams(self) -> list[Stream]:
        """Return list of discovered streams.

        Returns:
            List of stream instances.
        """
        return [stream_class(self) for stream_class in STREAM_TYPES]

    def sync_all(self) -> None:
        """Sync all streams with dependency ordering.

        Ensures ProductsStream runs first to collect SKU IDs that
        StockStream depends on.
        """
        self._reset_state_progress_markers()
        self._set_compatible_replication_methods()

        products_stream = self.streams.get("products")
        prices_stream = self.streams.get("prices")
        stock_stream = self.streams.get("stock")

        ordered_streams = self._build_stream_order(
            products_stream=products_stream,
            prices_stream=prices_stream,
            stock_stream=stock_stream,
        )

        if products_stream:
            products_stream.clear_collected_ids()

        for stream in ordered_streams:
            self._sync_stream(stream, products_stream)

        for stream in self.streams.values():
            stream.log_sync_costs()

    def _build_stream_order(
        self,
        products_stream: Stream | None,
        prices_stream: Stream | None,
        stock_stream: Stream | None,
    ) -> list[Stream]:
        """Build ordered list of streams respecting dependencies.

        Products runs first (collects product IDs + SKU IDs).
        Prices and stock run last (depend on collected SKU IDs).
        All other streams (product_details, sales, etc.) run in between.

        Args:
            products_stream: The products stream (runs first).
            prices_stream: The prices stream (runs after products).
            stock_stream: The stock stream (runs after products).

        Returns:
            Ordered list of streams.
        """
        dependent_streams = {products_stream, prices_stream, stock_stream}
        other_streams = [
            s for s in self.streams.values() if s not in dependent_streams
        ]

        ordered = []

        # Products must run first (collects IDs for product_details, prices, stock)
        if products_stream:
            ordered.append(products_stream)

        # Other streams (product_details, sales, stock_changes, etc.)
        ordered.extend(other_streams)

        # Dependent streams run last
        if prices_stream:
            ordered.append(prices_stream)
        if stock_stream:
            ordered.append(stock_stream)

        return ordered

    def _sync_stream(
        self,
        stream: Stream,
        products_stream: Stream | None,
    ) -> None:
        """Sync a single stream with proper handling.

        Args:
            stream: The stream to sync.
            products_stream: The products stream for finalization.
        """
        if not stream.selected and not stream.has_selected_descendents:
            self.logger.info(f"Skipping deselected stream '{stream.name}'")
            return

        if stream.parent_stream_type:
            self.logger.debug(
                f"Skipping child stream '{stream.name}' (called by parent)"
            )
            return

        stream.sync()
        stream.finalize_state_progress_markers()

        if stream is products_stream and hasattr(products_stream, "finalize_child_contexts"):
            products_stream.finalize_child_contexts()


if __name__ == "__main__":
    TapTilroy.cli()
