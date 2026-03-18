# Poller 3000

A simple lost and found service for temporal workers.

## Config example

```yaml
temporal:
  targets:
    - host: "localhost"
      port: 7233
      namespace: "default"
    - host: "prod.temporal.io"
      port: 7233
      namespace: "production"

monitoring:
  poll_interval_seconds: 30
```

## How it works

Just iterating the defined targets and signaling if the worker is not found in the running flow and if the worker is found in the completed/terminated flow.

## TODO

Ability to define a handler for each target that will be called when the worker is not found in the running flow and if the worker is found in the completed/terminated flow.
