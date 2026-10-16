# tap-tilroy

`tap-tilroy` is a Singer tap for Tilroy.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPI repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

Install from PyPI:

```bash
pipx install tap-tilroy
```

Install from GitHub:

```bash
pipx install git+https://github.com/ORG_NAME/tap-tilroy.git@main
```

-->

## Configuration

### Accepted Config Options

<!--
Developer TODO: Provide a list of config options accepted by the tap.

This section can be created by copy-pasting the CLI output from:

```
tap-tilroy --about --format=markdown
```
-->

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-tilroy --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your tap requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Stream dependencies

- **Products** must run before **stock** (and optionally **prices**). The tap sync order ensures products run first; it collects SKU IDs that the stock stream uses for per-SKU API calls.
- **Prices**: `GET /priceapi/production/price/rules` with `count` and `page`; paginate until no more records or API reports no more pages. 504 responses are retried with backoff. Products stream is not required for prices.

## Purchase order exports

`purchase_order_exports` reads `GET /purchaseapi/production/export/orders`
([API reference](https://tilroy-apidocs-prd.s3.eu-west-1.amazonaws.com/purchaseapi.html#tag/export-import/paths/~1export~1%7Btype%7D/get)).
It is independent of `purchase_orders` and preserves nested order lines. Uses
existing API keys, `tilroyId` as the primary key, and `dateExported` for incremental
state. `dateExportedSince` starts from `start_date` (default 2010-01-01 UTC),
with a one-day replay of the SDK starting timestamp to cover boundary ties.
Requests use `count=100` and increasing `page`, stopping at the paging headers'
final page or an empty response. Repeated pages and invalid records fail the sync.
No shop/status filters or separate deletion feed are applied. API response shape
and export retention have not been verified live; offline tests cover both the
list described by the docs and their single-object example.

## Usage

You can easily run `tap-tilroy` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-tilroy --version
tap-tilroy --help
tap-tilroy --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

Prerequisites:

- Python 3.9+
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
uv run pytest
```

You can also test the `tap-tilroy` CLI interface directly using `uv run`:

```bash
uv run tap-tilroy --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-tilroy
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-tilroy --version

# OR run a test ELT pipeline:
meltano run tap-tilroy target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
