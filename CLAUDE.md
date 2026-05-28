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

The app starts on port `9090`. Logs are written to both console and `logs/amps-basics.log`.

## AMPS Server Prerequisite

The app calls `HAClient.connectAndLogon()` eagerly at startup — it will fail to start if the AMPS server is unreachable.

The AMPS server config (`src/main/resources/amps-config.xml`, kept for reference) requires these directories to exist on the server before starting AMPS:

```bash
mkdir -p /home/shan/app-env/data/amps/logs
mkdir -p /home/shan/app-env/data/amps/sow
mkdir -p /home/shan/app-env/data/amps/journal
```

The `journal/` directory is especially critical — the queue won't deliver messages if the TransactionLog can't initialise. Check AMPS admin UI at `http://172.21.12.69:8085` to verify server health and queue depth.

## Architecture

Spring Boot 4.x / Java 21 app that integrates with an **AMPS (Advanced Message Processing System)** server via the `amps-client` Java SDK (`com.crankuptheamps`, v5.3.3).

### Message flow

```
REST POST /orders
    → OrderController
    → AmpsProducer.publish()  →  HAClient.publish("orders", json)
                                          ↓
                              AMPS server: SOW upsert on /orderId key
                              AMPS server: TransactionLog journal entry
                              AMPS server: enqueue into "orders-queue"
                                          ↓
    AmpsConsumer (daemon thread)  ←  blocking subscribe on "orders-queue"
        → message.getData() → Jackson → Order
        → processOrder()
        → sendAck() → message.ack()   ← native SDK ACK, releases lease
```

### Key design points

- **Single shared `HAClient` bean** (`AmpsConfig`) — producer and consumer both inject the same connection. `AmpsShutdown` cleanly disconnects it on `ContextClosedEvent`.
- **Consumer runs on a dedicated daemon thread** — `startConsuming()` (called on `ApplicationReadyEvent`) spawns a thread named `amps-consumer` that owns the blocking `for (Message m : execute(subscribe))` loop. The Spring event thread is never blocked.
- **Native `message.ack()`** — queue messages are acknowledged using the AMPS SDK's `Message.ack()` method. Manual `Command("ack")` construction was avoided because incorrect field usage (e.g. `.setTopic()` on an `ack` command) corrupts internal `StringField` state and causes NPE in `executeAsyncNoLock`.
- **At-least-once delivery** — the ACK is sent only after `processOrder()` succeeds. If processing throws, the inner try-catch logs the error, skips the ACK, and the message is re-leased after the 30 s `LeaseTimeout` for redelivery.
- **Producer publishes to a topic** (`amps.topic=orders`); consumer subscribes to a **queue** (`amps.queue=orders-queue`). The AMPS server routes topic publishes into the queue via `<Queue><Topic>orders</Topic>`.

### Configuration (`application.yaml`)

| Key | Purpose |
|-----|---------|
| `amps.server.url` | AMPS transport URL — format: `tcp://host:port/amps/json` |
| `amps.client.name` | Logical client name registered with the AMPS server |
| `amps.topic` | Topic the producer publishes to (`orders`) |
| `amps.queue` | Queue the consumer subscribes to (`orders-queue`) |
| `server.port` | REST API port (default `9090`) |
| `logging.file.name` | Log file path (default `logs/amps-basics.log`) |

### AMPS server topology (from `amps-config.xml`)

| Component | Name | Detail |
|-----------|------|--------|
| Transport | `tcp-json` | TCP, AMPS protocol, JSON message type, port 9007 |
| SOW topic | `orders` | Key: `/orderId` — upserts by order ID |
| Queue | `orders-queue` | Backed by `orders` topic, 30 s lease timeout |
| TransactionLog | `orders` | Required for queue bookmark/ACK to function |
| Admin UI | — | `http://172.21.12.69:8085` |

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

Expected log sequence on success:

```
[amps-consumer] Starting AMPS queue consumer: orders-queue
[amps-consumer] 📩 Received: {"orderId":"ORD-001",...}
[amps-consumer] 🔧 Processing orderId=ORD-001, product=Widget
[amps-consumer] ✅ ACK successful orderId=ORD-001 bookmark=...
```
