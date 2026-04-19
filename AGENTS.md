# Kestra DataHub Plugin

## What

- Provides plugin components under `io.kestra.plugin.datahub`.
- Includes classes such as `DataHubLogConsumer`, `Ingestion`.

## Why

- What user problem does this solve? Teams need to run DataHub CLI ingestions and interact with DataHub metadata from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps DataHub steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on DataHub.

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
