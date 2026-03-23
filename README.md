# bob-the-broker

Minimal in-memory message broker with gRPC API (including server-streaming subscriptions).

## Run
```bash
go run ./cmd/bobthebroker
```

Server listens on `:50051` by default. Override with `PORT`.

## Docker
Build image:
```bash
docker build -t bob-the-broker .
```

Run container (default port mapping):
```bash
docker run -p 50051:50051 bob-the-broker
```

Run with custom host port:
```bash
docker run -p 9090:50051 bob-the-broker
```

Run with custom all host port:
```bash
docker run -p 9092:9092 -e PORT=9092 bob-the-broker
```

## gRPC API
Proto definition: `internal/proto/broker.proto`.

Service: `BrokerService`
- `Produce(ProduceRequest) returns (ProduceResponse)`
- `Fetch(FetchRequest) returns (FetchResponse)`
- `Subscribe(SubscribeRequest) returns (stream Message)`
- `HealthCheck(HealthCheckRequest) returns (HealthCheckResponse)`

Message schema (server responses):
```proto
message Message {
  string topic = 1;
  string key = 2;
  string value = 3;
  int64  offset = 4;
  int32  partition = 5;
}
```

### Examples (grpcurl)
Reflection is enabled, so `grpcurl` works out of the box.

List services:
```bash
grpcurl -plaintext localhost:50051 list
```

Produce:
```bash
grpcurl -plaintext -d '{"topic":"jobs","key":"worker-1","value":"{\"id\":123,\"task\":\"ping\"}"}' \
  localhost:50051 brokerpb.BrokerService/Produce
```

Fetch:
```bash
grpcurl -plaintext -d '{"topic":"jobs","partition":0,"offset":0,"limit":100}' \
  localhost:50051 brokerpb.BrokerService/Fetch
```

Subscribe (server stream):
```bash
grpcurl -plaintext -d '{"topic":"jobs"}' \
  localhost:50051 brokerpb.BrokerService/Subscribe
```

Health check:
```bash
grpcurl -plaintext localhost:50051 brokerpb.BrokerService/HealthCheck
```
