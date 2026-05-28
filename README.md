# amps-basics

A Spring Boot demo that shows end-to-end publish/subscribe with **AMPS (Advanced Message Processing System)** using the AMPS Java client SDK. An order is submitted via a REST endpoint, published to an AMPS topic, routed through a persistent queue, consumed by a background thread, and acknowledged using the native SDK ACK.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Runtime | Java 21 |
| Framework | Spring Boot 4.x |
| Messaging | AMPS 5.3.5 (`com.crankuptheamps:amps-client:5.3.3.0`) |
| Serialisation | Jackson |
| Boilerplate reduction | Lombok |

---

## Prerequisites

### 1. AMPS Server

The app connects to AMPS at startup and will fail to start if the server is unreachable.

**Create required directories on the AMPS host before starting the server:**

```bash
mkdir -p /home/shan/app-env/data/amps/logs
mkdir -p /home/shan/app-env/data/amps/sow
mkdir -p /home/shan/app-env/data/amps/journal
```

The `journal/` directory is critical — the queue bookmark/ACK mechanism depends on the TransactionLog. Without it, messages are received but never delivered to subscribers.

**Start the AMPS server:**

```bash
cd ~/amps-5.3.5
./bin/ampServer amps-config.xml
```

Verify via the admin UI: `http://172.21.12.69:8085`

### 2. Java 21 + Maven

```bash
java -version   # must be 21+
./mvnw -version
```

---

## Configuration

All settings are in `src/main/resources/application.yaml`:

```yaml
amps:
  server:
    url: tcp://172.21.12.69:9007/amps/json  # AMPS host:port
  client:
    name: springboot-amps-demo               # client identity on server
  topic: orders                              # producer publishes here
  queue: orders-queue                        # consumer subscribes here

server:
  port: 9090

logging:
  file:
    name: logs/amps-basics.log
```

Update `amps.server.url` if your AMPS server is on a different host or port.

---

## Running

```bash
./mvnw spring-boot:run
```

The app:
1. Connects and logs on to AMPS (`HAClient.connectAndLogon`)
2. Starts an `amps-consumer` daemon thread that subscribes to `orders-queue`
3. Exposes `POST /orders` on port `9090`

---

## Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────────────┐
│  Spring Boot App (port 9090)                                │
│                                                             │
│  ┌──────────────────┐        ┌──────────────────────────┐  │
│  │ OrderController  │        │ AmpsConsumer             │  │
│  │  POST /orders    │        │  (daemon thread)         │  │
│  └────────┬─────────┘        └──────────┬───────────────┘  │
│           │                             │ subscribe         │
│           ▼                             │ message.ack()     │
│  ┌──────────────────┐                   │                   │
│  │  AmpsProducer    │                   │                   │
│  │  .publish()      │                   │                   │
│  └────────┬─────────┘                   │                   │
│           │                             │                   │
│           └──────────┬──────────────────┘                   │
│                      │  HAClient (shared singleton)         │
│                      │  AmpsConfig @Bean                    │
│                      │  AmpsShutdown (on ContextClosed)     │
└──────────────────────┼──────────────────────────────────────┘
                       │  TCP / AMPS protocol / JSON
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  AMPS Server  (172.21.12.69:9007)                           │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Topic: "orders"  (SOW, key=/orderId)               │   │
│  │  • stores latest state per orderId                  │   │
│  │  • journals every publish to TransactionLog         │   │
│  └───────────────────────┬─────────────────────────────┘   │
│                          │ routes publishes                 │
│  ┌───────────────────────▼─────────────────────────────┐   │
│  │  Queue: "orders-queue"                              │   │
│  │  • persistent, lease-based delivery                 │   │
│  │  • LeaseTimeout: 30s                                │   │
│  │  • MaxBacklog: 100,000                              │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  Admin UI: http://172.21.12.69:8085                         │
└─────────────────────────────────────────────────────────────┘
```

### Message Flow (sequence)

```
Client          OrderController    AmpsProducer      AMPS Server       AmpsConsumer
  │                   │                 │                 │                  │
  │  POST /orders     │                 │                 │                  │
  │──────────────────▶│                 │                 │                  │
  │                   │  publish(order) │                 │                  │
  │                   │────────────────▶│                 │                  │
  │                   │                 │  publish JSON   │                  │
  │                   │                 │────────────────▶│                  │
  │                   │                 │                 │  SOW upsert      │
  │                   │                 │                 │  journal write   │
  │                   │                 │                 │  enqueue         │
  │                   │                 │                 │                  │
  │                   │                 │                 │  deliver (lease) │
  │                   │                 │                 │─────────────────▶│
  │                   │                 │                 │                  │  deserialise
  │                   │                 │                 │                  │  processOrder()
  │                   │                 │                 │  message.ack()   │
  │                   │                 │                 │◀─────────────────│
  │                   │                 │                 │  release lease,  │
  │                   │                 │                 │  remove from Q   │
  │  200 OK           │                 │                 │                  │
  │◀──────────────────│                 │                 │                  │
```

### At-Least-Once Delivery

AMPS queues use a **lease-based** model:

1. When a message is delivered to a subscriber it is **leased** (locked) for 30 seconds.
2. The consumer must call `message.ack()` within that window.
3. If the app crashes or the ACK is skipped (processing exception), the lease expires and AMPS re-delivers the message — guaranteeing at-least-once processing.
4. The ACK is sent **only after** `processOrder()` succeeds, so a processing failure never silently drops a message.

### ACK Implementation Note

The AMPS 5.3.x Java client exposes `Message.ack()` as the correct way to acknowledge queue messages. Manually constructing `new Command("ack").setTopic(...)` was found to corrupt the command's internal `StringField` state, causing `NullPointerException` in `Client.executeAsyncNoLock`. Using the native `message.ack()` avoids this entirely.

---

## REST API

### POST /orders — publish an order

**Request**

```http
POST http://localhost:9090/orders
Content-Type: application/json

{
  "orderId": "ORD-001",
  "product": "Laptop",
  "quantity": 2,
  "price": 999.99,
  "status": "NEW"
}
```

**Response**

```
200 OK
Order [ORD-001] queued successfully
```

---

## Log Output

A successful round-trip produces this sequence in `logs/amps-basics.log`:

```
INFO  AmpsConfig       : Amps Server URL: tcp://172.21.12.69:9007/amps/json
INFO  AmpsConfig       : AmpsClient connected and logon successfully
INFO  AmpsConsumer     : AMPS consumer thread started for queue: orders-queue
INFO  AmpsConsumer     : Starting AMPS queue consumer: orders-queue
INFO  OrderController  : Publish to Q - Order(orderId=ORD-001, ...)
INFO  AmpsProducer     : 📤 Published to AMPS [orders]: {...}
INFO  AmpsConsumer     : 📩 Received: {"orderId":"ORD-001",...}
INFO  AmpsConsumer     : 🔧 Processing orderId=ORD-001, product=Laptop
INFO  AmpsConsumer     : ✅ ACK successful orderId=ORD-001 bookmark=...
```

---

## Project Structure

```
src/main/java/com/shan/mq/amps/ampsbasics/
├── AmpsBasicsApplication.java       entry point
├── config/
│   ├── AmpsConfig.java              HAClient @Bean (connects at startup)
│   └── AmpsShutdown.java            disconnects HAClient on context close
├── controller/
│   └── OrderController.java         POST /orders
├── producer/
│   └── AmpsProducer.java            publishes Order JSON to AMPS topic
├── consumer/
│   └── AmpsConsumer.java            daemon thread; subscribes + ACKs queue
├── model/
│   └── Order.java                   orderId, product, quantity, price, status
└── utils/
    └── OrderUtil.java               (placeholder)

src/main/resources/
├── application.yaml                 app + AMPS + logging config
└── amps-config.xml                  AMPS server config (reference copy)
```
