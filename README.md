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
  handlers:
    lost_worker: "/path/to/handler/lost-worker"
    redundant_worker: "/path/to/handler/redundant-worker"

```

## How it works

Just iterating the defined targets and signaling/handling if the worker is not found in the running flow and if the worker is found in the completed/terminated flow.

## Handlers

Handlers are called asynchronously (goroutine), so they don't block monitoring.

Example payload for `lost_worker`:

```json
{
    host: localhost,
    port: 7233,
    namespace: default,
    workflow_id: abc-123,
    run_id: run-456,
    workflow_type: MyWorkflow,
    task_queue: my-queue
}
```

Example payload for `redundant_worker`:

```json
{
    host: localhost,
    port: 7233,
    namespace: default,
    workflow_id: xyz-789,
    run_id: run-012,
    workflow_type: MyWorkflow,
    task_queue: my-queue,
    status: Completed,
    poller_count: 2
}
