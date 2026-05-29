
# AMPS Advanced Messaging and Queue Features

## Overview

AMPS queues are built on top of AMPS topics, which means queues automatically inherit many advanced messaging capabilities available in AMPS.

This architecture supports:

- Content filtering
- Queue querying
- SOW integration
- Delta publishing
- Views and aggregations
- Bookmark replay

---

# Queue Architecture

```text
Producer
   |
   v
Underlying Topic
   |
   v
AMPS Queue
   |
   +--> Queue Consumers
   |
   +--> Views
   |
   +--> Aggregations
   |
   +--> Bookmark Replay
```

---

# 1. Content Filtering on Queues

Consumers can subscribe to only a subset of messages in a queue using content filters.

Example filter:

```sql
/region = 'US'
```

This allows selective consumption without creating separate queues.

---

# 2. Queue Querying as a View

Every queue can be queried like a SOW/view using a `sow` command.

IMPORTANT:

Queue queries are read-only.

They:

- do NOT lease messages
- do NOT remove messages
- do NOT acknowledge messages

Useful for:

- dashboards
- support tools
- monitoring
- debugging

---

# Queue Query Architecture

```text
                +------------------+
                | PendingOrders    |
                | Queue            |
                +------------------+
                       |
             +---------+---------+
             |                   |
             v                   v

      Consumer Lease       Read-Only Query
      removes visibility   only inspects
```

---

# 3. SOW Topics as Queue Underlying Topics

AMPS supports SOW-backed topics as queue underlying topics.

IMPORTANT:

SOW stores latest state.

Queue stores EVERY publish event.

Example:

Messages published:

```json
{ "symbol":"AAPL", "price":100 }
{ "symbol":"AAPL", "price":101 }
{ "symbol":"AAPL", "price":102 }
```

SOW retains:

```json
{ "symbol":"AAPL", "price":102 }
```

Queue contains:

```text
100
101
102
```

Each publish becomes a queue event.

---

# Important Behavior

- Only publish messages enter queue
- Out-of-focus messages are NOT queued
- Deleting SOW records does NOT remove queue messages
- SOW expiration timestamps can be used by queue expiration

---

# Queue + SOW Lifecycle

```text
Publish
   |
   v

SOW Topic Updated
   |
   +--> Latest state updated
   |
   +--> Queue receives NEW event
```

---

# 4. Delta Messaging with Queues

Queues treat each publish as a NEW message.

Even when delta publishing is used:

- Queue subscribers receive FULL merged message
- Queue subscribers do NOT receive delta patches

---

# Example

Delta publish:

```json
{ "price":105 }
```

Merged result added to queue:

```json
{
  "symbol":"AAPL",
  "price":105,
  "qty":100
}
```

---

# 5. Views and Aggregations over Queues

AMPS supports:

- Views over queues
- Aggregated subscriptions over queues

These operate only on currently available queue messages.

When a message is leased:

- it disappears from queue view

When lease expires:

- it reappears

---

# Queue Visibility Lifecycle

```text
Queue Message
      |
      v

Available in View
      |
Consumer leases
      |
      v

Hidden from View
      |
Lease expires
      |
      v

Visible Again
```

---

# Enterprise Example

Queue:

```text
PendingOrders
```

Aggregation:

```sql
SUM(orderValue)
GROUP BY region
```

Provides real-time operational dashboards.

---

# 6. Bookmark Subscriptions to Queues

Queues normally support:

- at-most-once
- at-least-once

Once acknowledged, queue messages cannot be replayed directly.

AMPS solves replay using bookmark subscriptions.

IMPORTANT:

Bookmark replay on a queue becomes replay on the underlying topic.

This is NOT queue replay.

---

# Replay Architecture

```text
Bookmark Replay Request
          |
          v

Underlying Topic Replay
          |
          v

Publish/Subscribe Delivery
```

Replay messages:

- do NOT require ACK
- do NOT use leases
- do NOT use queue semantics

---

# Queue vs Bookmark Replay

| Feature | Queue Subscription | Bookmark Replay |
|---|---|---|
| ACK required | Yes | No |
| Lease tracking | Yes | No |
| Redelivery | Yes | No |
| Replay past messages | No | Yes |
| Delivery mode | Queue semantics | Pub/Sub semantics |

---

# Recommended Usage

Use queue subscriptions for:

- real-time processing
- reliable delivery

Use bookmark replay for:

- recovery
- audit replay
- analytics replay
- debugging
- rebuilding downstream systems

---

# Final Summary

AMPS queues are much more than traditional queues because they inherit topic capabilities.

Supported advanced capabilities include:

| Feature | Benefit |
|---|---|
| Content Filtering | selective consumption |
| Queue Querying | operational visibility |
| SOW Integration | latest-state + event processing |
| Delta Publish Support | efficient upstream publishing |
| Views/Aggregations | real-time queue analytics |
| Bookmark Replay | historical recovery/replay |

This makes AMPS queues extremely powerful for:

- trading systems
- event-driven microservices
- real-time analytics
- operational dashboards
- enterprise messaging systems


# AMPS Queueing — Simplified Summary

## 1. What is an AMPS Queue?

An AMPS queue is **not a separate storage system** like traditional MQs.

Instead:

- Messages are published to a normal AMPS topic.
- That topic is stored in the transaction log.
- The queue acts like a **view/tracker** over those messages.
- AMPS tracks:
    - which messages are pending
    - which subscriber received them
    - which messages are acknowledged

So:

```text
Publisher → Topic → Transaction Log → Queue View → Consumers
```

AMPS does not duplicate messages in memory.
It only stores lightweight metadata (~200 bytes/message).

---

# 2. Push Model (No Polling)

Traditional queues:

```text
Consumer repeatedly asks:
"Any message?"
"Any message?"
"Any message?"
```

AMPS queues use **push delivery**:

```text
Consumer subscribes once
AMPS pushes messages automatically
```

Benefits:

- lower latency
- less network traffic
- better throughput
- centralized load balancing

---

# 3. Backlog (Very Important)

Each consumer can define:

```text
max_backlog = number of unacknowledged messages allowed
```

Example:

```text
Consumer A backlog = 5
```

AMPS can send:

- up to 5 messages simultaneously
- before acknowledgments are required

---

## Why backlog matters

### backlog = 1

```text
Receive msg1
Process
Ack
Receive msg2
```

Slow.

---

### backlog = 5

```text
Receive msg1,msg2,msg3,msg4,msg5
Process continuously
Ack in parallel
```

Much faster.

60East recommends:

```text
backlog >= 2
```

for pipelining.

---

# 4. Delivery Semantics

AMPS supports 2 delivery guarantees.

---

# A. At-Most-Once

## Behavior

As soon as AMPS sends the message:

```text
message removed from queue immediately
```

If consumer crashes:

```text
message LOST
```

No redelivery.

---

## Equivalent to

```text
Destructive read
```

---

## Best for

Low-value / real-time data.

Examples:

- sensor streams
- market ticks
- telemetry

Where occasional loss is acceptable.

---

## Flow

```text
Publish → Deliver → Remove immediately
```

---

# B. At-Least-Once

## Behavior

Message stays in queue until consumer ACKs it.

If consumer crashes before ACK:

```text
AMPS re-delivers message
```

---

## Equivalent to

```text
Non-destructive read
```

---

## Uses Lease Mechanism

When message delivered:

```text
AMPS gives temporary ownership (lease)
```

Example:

```text
Lease = 30 seconds
```

If no ACK within 30 sec:

```text
lease expires
message returned to queue
another consumer gets it
```

---

## Best for

High-value processing.

Examples:

- payments
- orders
- DB updates
- financial trades

---

## Flow

```text
Publish
   ↓
Deliver
   ↓
Wait for ACK
   ↓
ACK → Remove
NO ACK → Redeliver
```

---

# 5. ACKNOWLEDGMENT

Consumers must acknowledge after successful processing.

Internally AMPS uses:

```text
sow_delete(bookmark)
```

---

# 6. ACK Options

## Normal ACK

```text
message processed successfully
remove from queue
```

---

## cancel

```text
return message back to queue
```

Used when:

- temporary failure
- retry needed

---

## expire

```text
remove permanently
```

Used when:

- invalid data
- corrupt payload
- unrecoverable error

---

# 7. Lease Expiration

If consumer:

- crashes
- disconnects
- hangs

Then:

```text
lease expires
message returns to queue
```

This provides fault tolerance.

---

# 8. MaxDeliveries / MaxCancels

Protection against infinite retries.

---

## MaxDeliveries

Maximum total deliveries allowed.

Example:

```text
MaxDeliveries = 5
```

After 5 failures:

```text
message expired / dead-lettered
```

---

## MaxCancels

Maximum manual returns to queue.

---

# 9. Queue Fairness Algorithms

Determines WHICH subscriber gets next message.

---

# A. fast

Fastest possible delivery.

```text
first available consumer wins
```

Optimized for:

- lowest latency

Not fair.

---

# B. round-robin

Even distribution.

Example:

```text
A → B → C → A → B → C
```

Good fairness.

---

# C. proportional (default for at-least-once)

AMPS prefers consumers with more free capacity.

Example:

| Consumer | Backlog | Used |
|---|---|---|
| A | 2 | 1 |
| B | 10 | 2 |

AMPS prefers:

```text
B
```

because B has more unused capacity.

Best for throughput.

---

# 10. Barrier Messages

Barrier message creates synchronization point.

AMPS will NOT process later messages until:

```text
all previous messages ACKed
```

Useful for:

- checkpoints
- transaction boundaries
- batch completion

---

# 11. Priority Queues

Normally:

```text
FIFO
```

With Priority enabled:

```text
higher priority delivered first
```

---

# 12. Message Expiration

If message sits too long:

```text
message expires automatically
```

Not considered an error.

Useful for:

- stale work
- outdated events

---

# 13. Exactly-Once Processing (Important Concept)

AMPS itself provides:

```text
at-most-once
OR
at-least-once
```

NOT true exactly-once automatically.

---

## Common Pattern for Exactly-Once

Application stores:

```text
processed_message_id
```

inside DB transaction.

If duplicate arrives:

```text
ignore it
ACK immediately
```

This is standard enterprise pattern.

---

# 14. Multiple Queues on Same Topic

Very powerful AMPS feature.

Same topic can feed:

```text
Queue A → realtime processing
Queue B → audit processing
Queue C → retry pipeline
```

Each queue has independent:

- retries
- fairness
- expiration
- consumers

---

# 15. Queue Recovery After Restart

AMPS rebuilds queue state using:

```text
transaction log
```

So after restart:

- pending messages restored
- leases restored
- acknowledgments restored

Very reliable.

---

# 16. FileBackedMetadata

Optional optimization.

Queue metadata stored on disk.

Benefits:

- lower RAM usage
- faster restart recovery
- good for huge queues

---

# 17. Core Mental Model

Think of AMPS queue as:

```text
Transactional topic
+
Message state tracking
+
Lease management
+
Subscriber load balancing
```

NOT as a separate queue database.

---

# 18. Most Important Production Concepts

If you remember only these:

| Concept | Meaning |
|---|---|
| Queue = view over topic | Messages stay in transaction log |
| Push model | No polling |
| Backlog | Max unacked messages per consumer |
| At-most-once | Fast but may lose messages |
| At-least-once | Reliable but duplicates possible |
| Lease | Temporary ownership of message |
| ACK | Removes message |
| cancel | Retry later |
| expire | Drop permanently |
| proportional fairness | Sends more work to consumers with more capacity |
| Barrier | Sequential checkpoint |
| Exactly-once | Must be implemented at application level |

---

# 19. Advanced Messaging and Queues

## Queues Use Full AMPS Messaging Features

Because queues are implemented as AMPS topics, queues support advanced AMPS features such as:

- content filtering
- views
- aggregation
- replay
- SOW integration
- delta publish
- bookmark replay

This makes AMPS queues much more powerful than traditional MQ systems.

---

## Content Filtering on Queues

Consumers can subscribe using filters.

Example:

```text
Only consume:
status = 'PENDING'
region = 'US'
priority > 5
```

This allows selective processing.

---

## Queue as a Read-Only View

Queues can be queried using:

```text
sow query
```

Example:

```text
Query PendingOrders queue
See all currently available messages
```

Important:

- querying does NOT lease messages
- querying does NOT remove messages
- queue delivery unaffected

Useful for:

- monitoring
- dashboards
- debugging
- admin tools

---

# 20. Queues over SOW Topics

If underlying topic is a SOW topic:

```text
Every publish becomes a NEW queue message
```

Even if SOW record updates existing key.

Important:

- queue tracks publishes
- not current SOW state

---

## Important Rules

### SOW delete does NOT remove queue message

```text
Delete from SOW ≠ remove from queue
```

---

### SOW expiration does NOT remove queue message

Queue manages expiration independently.

---

### Out-of-focus messages are NOT added to queue

Only publish operations enter queue.

---

# 21. Delta Messaging with Queues

Queues treat every update as a new message.

So:

```text
No delta replay from queue
```

Even if delta publish used:

```text
queue stores FULL merged message
```

Delta subscriptions on queues receive:

```text
FULL messages
NOT delta changes
```

---

# 22. Views and Aggregations over Queues

You can create:

- views
- aggregates
- analytics

on top of queues.

These operate ONLY on:

```text
currently available messages
```

Leased messages disappear temporarily.

Expired/acked messages disappear permanently.

---

## Example

```text
Total value of unprocessed orders
Average pending risk
Current failed jobs count
```

Very useful for operations dashboards.

---

# 23. Bookmark Replay vs Queue Subscription

Very important distinction.

---

## Queue Subscription

Provides queue guarantees:

- at-most-once
- at-least-once
- acknowledgments
- leases
- backlog
- fairness

Once processed:

```text
message not redelivered
```

---

## Bookmark Replay

Replay directly from transaction log.

Behaves like pub/sub.

Characteristics:

- replay anytime
- multiple consumers can replay same message
- no acknowledgments
- no leasing
- no queue semantics

---

## Important Rule

```text
Bookmark subscription to queue
=
bookmark replay of underlying topic
NOT real queue subscription
```

So:

```text
If you want queue semantics:
DO NOT use bookmark
```

---

# 24. Replacing Queue Subscriptions

AMPS supports atomic subscription replacement.

Can replace:

- topic
- filter
- backlog
- options

without disconnecting.

---

## Important Behavior

Outstanding leased messages remain valid.

Example:

```text
Old backlog = 10
New backlog = 5
```

Consumer may still temporarily hold:

```text
10 outstanding messages
```

No new messages delivered until outstanding drops below new limit.

---

# 25. Dead Letter Queue (DLQ)

AMPS supports automatic failed-message routing.

Typical flow:

```text
Main Queue
   ↓
Repeated failures
   ↓
Message expires
   ↓
Auto publish to Failed Queue
```

---

## Controls

### MaxCancels

Maximum retries after manual cancel.

---

### MaxDeliveries

Maximum total deliveries.

---

## Example

```text
Jobs Queue
   ↓
10 failed deliveries
   ↓
Move to FailedJobs queue
```

Useful for:

- manual review
- reconciliation
- poison messages
- invalid payload handling

---

# 26. Queue Types

AMPS provides 3 queue types.

---

## A. Queue

Distributed queue.

Shared across AMPS instances.

Consumers can connect anywhere.

---

## B. LocalQueue

Queue exists only on local instance.

No replication.

Useful for:

- local processing
- temporary workloads
- instance-specific tasks

---

## C. GroupLocalQueue

Queue shared only within specific AMPS group.

Useful for:

- regional processing
- shard-based workloads
- grouped consumers

---

# 27. Important Queue Configuration Options

| Config | Purpose |
|---|---|
| LeasePeriod | Lease timeout |
| Semantics | at-most-once / at-least-once |
| MaxBacklog | Queue backlog limit |
| MaxPerSubscriptionBacklog | Per consumer backlog |
| Expiration | Message expiry time |
| FairnessModel | Delivery algorithm |
| MaxDeliveries | Retry limit |
| MaxCancels | Cancel retry limit |
| Priority | Priority delivery |
| BarrierExpression | Barrier synchronization |
| FileBackedMetadata | Persist queue metadata |
| TargetQueueDepth | Limit active queue depth |

---

# 28. Multiple Underlying Topics

A queue can consume from multiple topics.

Configured using regex.

Example:

```text
ORDERS_ANALYTICS queue
receives:
- ORDERS
- ORDERS_ANALYTICS_DIRECT
```

Benefits:

- single publish feeds multiple queues
- easier publisher design
- shared message streams
- analytics + risk pipelines simultaneously

---

## Example Architecture

```text
Publish ORDERS
      ↓
+----------------+
| AnalyticsQueue |
+----------------+

+------------+
| RiskQueue  |
+------------+
```

One publish → multiple queue pipelines.

---

# 29. Priority Queues

Queues can process:

```text
highest priority first
```

instead of FIFO.

Priority calculated using expressions.

Example:

```text
priority = price * quantity
```

Higher trade value processed first.

Useful for:

- trading
- risk systems
- urgent workflows
- SLA processing

---

# 30. Barrier Messages (Advanced)

Barrier messages synchronize all consumers.

Behavior:

```text
All earlier messages must finish
before barrier released
```

Then:

```text
Barrier sent to ALL subscribers
```

Immediately auto-acknowledged.

---

## Uses

- end-of-day processing
- coordinated shutdown
- cache refresh
- reference data synchronization
- checkpointing

---

## Important Notes

Barrier messages:

- ignore backlog limits
- delivered to all subscribers
- pause later messages temporarily
- removed automatically

---

# 31. TargetQueueDepth

Used to limit active memory usage.

Example:

```text
TargetQueueDepth = 1000
```

Only first 1000 active messages:

- loaded
- tracked
- queryable
- deliverable

Remaining messages stay inactive in transaction log.

---

## Benefits

Good for:

- huge queues
- infrequent processing
- memory constrained systems

Tradeoff:

```text
Lower memory
Higher CPU/disk reads later
```

---

## Restrictions

Cannot use TargetQueueDepth with:

- Priority queues
- BarrierExpression queues

---

# 32. FileBackedMetadata vs TargetQueueDepth

## FileBackedMetadata

Stores queue metadata on disk.

Benefits:

- lower RAM
- faster recovery
- better runtime performance

---

## TargetQueueDepth

Limits active messages.

Benefits:

- much lower memory usage

Tradeoff:

- more transaction-log rereads
- slower queue activation

---

# 33. Core Advanced Queueing Mental Model

AMPS queueing is:

```text
Transactional Log
+
Smart Message Routing
+
Lease Tracking
+
Distributed Coordination
+
Advanced Filtering
+
Replay Capability
+
Queue Analytics
```

rather than a traditional standalone MQ broker.

---

# 34. Most Important Advanced Concepts

| Concept | Meaning |
|---|---|
| Queue is queryable | Can inspect pending messages |
| Bookmark replay != queue | Replay bypasses queue semantics |
| SOW updates create new queue messages | Queue tracks publishes |
| Views over queues | Real-time operational dashboards |
| Dead letter queues | Automatic failed-message handling |
| Multiple underlying topics | One publish feeds many queues |
| Priority queues | Deliver by business importance |
| Barrier messages | Synchronize all consumers |
| TargetQueueDepth | Limit active deliverable messages |
| FileBackedMetadata | Persist queue state efficiently |

