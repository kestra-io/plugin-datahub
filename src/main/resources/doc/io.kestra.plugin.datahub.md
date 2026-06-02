# How to use the DataHub plugin

Run DataHub ingestion recipes from Kestra flows to push metadata into your DataHub catalog.

## Authentication

Authentication is configured inside the `recipe` itself — set the GMS server URL and token in the recipe's sink section (e.g. under `sink.config.server` and `sink.config.token`). You can also pass credentials via the `env` map. Store secrets in [secrets](https://kestra.io/docs/concepts/secret) and apply connection properties globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

## Tasks

`Ingestion` runs a DataHub ingestion recipe using the `datahub ingest` CLI in a container (default image `acryldata/datahub-ingestion:head`) — set `recipe` (required, either an inline Map with the full recipe YAML structure, or a `kestra://` URI pointing to a recipe file). Use `env` for environment variables, `namespaceFiles` to reference [namespace files](https://kestra.io/docs/concepts/namespace-files), `inputFiles` to stage files into the container, and `outputFiles` to retrieve results.
