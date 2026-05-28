# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build
./mvnw clean package

# Run
./mvnw spring-boot:run

# Run tests
./mvnw test

# Run a single test class
./mvnw test -Dtest=AmpsBasicsApplicationTests

# Skip tests during build
./mvnw clean package -DskipTests
```

The app starts on port `9090` (configured in `application.yaml`).

## Architecture

This is a Spring Boot 4.x / Java 21 demo app that integrates with an **AMPS (Advanced Message Processing System)** server using the `amps-client` Java SDK (`com.crankuptheamps`).

### Message flow

```
REST POST /orders
    → OrderController
    → AmpsProducer  →  publishes JSON to AMPS topic  ("orders")
                                ↓
                        AMPS server routes message to queue ("orders-queue")
                                ↓
    AmpsConsumer  ←  subscribes to queue on ApplicationReadyEvent
        → deserializes JSON → Order
        → processOrder()
        → sends ACK via bookmark (at-least-once delivery)
```

### Key design points

- **Single shared `HAClient` bean** (`AmpsConfig`) — both producer and consumer inject this same connection. `AmpsShutdown` disconnects it on `ContextClosedEvent`.
- **Consumer starts on `ApplicationReadyEvent`** — `AmpsConsumer.startConsuming()` blocks in a `for (Message message : client.execute(command))` loop on the Spring event-listener thread. This is intentional for this demo; in production you'd run it on a separate thread.
- **Explicit ACK pattern** — the consumer only ACKs (via `Command("ack").setBookmark(...)`) after successful processing. Failed processing logs the error and skips the ACK, leaving the message unacknowledged for redelivery.
- **Producer publishes to a topic** (`amps.topic=orders`); the consumer subscribes to a **queue** (`amps.queue=orders-queue`). The AMPS server is responsible for routing the topic into the queue.

### Configuration (`application.yaml`)

| Key | Purpose |
|-----|---------|
| `amps.server.url` | AMPS server transport URL (tcp://host:port/amps/json) |
| `amps.client.name` | Logical name for this AMPS client instance |
| `amps.topic` | Topic name the producer publishes to |
| `amps.queue` | Queue name the consumer subscribes to |
| `server.port` | REST API port (default 9090) |

The AMPS server URL currently points to `172.21.12.69:9007` — update this if your AMPS server is on a different host/port. The app will fail to start if AMPS is unreachable because `HAClient.connectAndLogon()` is called eagerly in the `@Bean` method.

## REST API

```
POST http://localhost:9090/orders
Content-Type: application/json

{
  "orderId": "ORD-001",
  "product": "Widget",
  "quantity": 5,
  "price": 9.99,
  "status": "NEW"
}
```