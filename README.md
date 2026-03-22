# bob-the-broker

Minimal in-memory message broker with HTTP API and SSE subscriptions.

## Run
```bash
go run ./cmd/bobthebroker
```

Server listens on `:8080`.

## HTTP API

### Produce
`POST /produce`

Request body (JSON):
```json
{
  "topic": "jobs",
  "key": "worker-1",
  "value": "{\"id\":123,\"task\":\"ping\"}"
}
```

Response: `201 Created` on success.

Notes:
- `value` is a string. If you send JSON, encode it as a JSON string.
- Limits: request body up to 1 MB, `value` up to 256 KB.

### Fetch
`POST /fetch`

Request body (JSON):
```json
{
  "topic": "jobs",
  "partition": 0,
  "offset": 0,
  "limit": 100
}
```

Response (JSON array of messages):
```json
[
  { "topic": "jobs", "offset": 0, "key": "worker-1", "value": "{\"id\":123}" }
]
```

### Subscribe (SSE)
`GET /subscribe?topic=jobs`

Server-Sent Events stream. Each event payload is JSON:
```json
{ "topic": "jobs", "key": "worker-1", "value": "{\"id\":123}" }
```

Keep the connection open to receive new messages.
