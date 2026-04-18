# Kestra DataHub Plugin

## What

- Provides plugin components under `io.kestra.plugin.datahub`.
- Includes classes such as `DataHubLogConsumer`, `Ingestion`.

## Why

- This plugin integrates Kestra with DataHub.
- It provides tasks that run DataHub CLI ingestions and interact with DataHub metadata.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `datahub`

### Key Plugin Classes

- `io.kestra.plugin.datahub.Ingestion`

### Project Structure

```
plugin-datahub/
├── src/main/java/io/kestra/plugin/datahub/
├── src/test/java/io/kestra/plugin/datahub/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
