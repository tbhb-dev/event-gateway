# event-gateway

The event gateway receives webhook events from sources such as GitHub and Grafana, keeps a durable log of them, and delivers them to workflows and agent sessions.

The pre-alpha service accepts signed GitHub webhooks and retains them in an embedded JetStream stream for seven days.

The design is in tbhb-dev/wiki under [designs/event-gateway](https://github.com/tbhb-dev/wiki/tree/main/designs/event-gateway).

## Run the gateway

```sh
event-gateway serve -github-secret-file /path/to/secret -store-dir /path/to/data
```

The HTTP listener defaults to `127.0.0.1:8080`. It accepts `POST /webhooks/github` and serves `GET /healthz`. Use `event-gateway serve -help` for the listener, body cap, and JetStream memory and disk options.

Set `OTEL_EXPORTER_OTLP_ENDPOINT` to the base URL of an OTLP over HTTP collector to export traces, metrics, and logs. The gateway also writes structured logs to standard output.

Licensed under the [Apache License 2.0](LICENSE).
