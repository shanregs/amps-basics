# AMPS 5.3.5 — Comprehensive Reference Summary

> Source: AMPS_server_5.3.5_documentation.pdf (1,152 pages)
> This document covers every major topic from the full documentation with concise, accurate explanations.

---

## Table of Contents

1. [What Is AMPS?](#1-what-is-amps)
2. [Core Design Principles](#2-core-design-principles)
3. [Platform, Requirements & Installation](#3-platform-requirements--installation)
4. [Message Concepts](#4-message-concepts)
5. [Content Filtering & Expressions](#5-content-filtering--expressions)
6. [AMPS Functions](#6-amps-functions)
7. [State of the World (SOW)](#7-state-of-the-world-sow)
8. [Transaction Log (Journal)](#8-transaction-log-journal)
9. [Message Queues](#9-message-queues)
10. [Delta Publish & Delta Subscribe](#10-delta-publish--delta-subscribe)
11. [Conflated Subscriptions & Conflated Topics](#11-conflated-subscriptions--conflated-topics)
12. [Views & Aggregation](#12-views--aggregation)
13. [Out-of-Focus (OOF) Messages](#13-out-of-focus-oof-messages)
14. [Select Lists — Field Projection](#14-select-lists--field-projection)
15. [SOW Result Management — Sorting & Pagination](#15-sow-result-management--sorting--pagination)
16. [Message Types](#16-message-types)
17. [Transports](#17-transports)
18. [Configuration File Structure](#18-configuration-file-structure)
19. [Logging](#19-logging)
20. [Event Topics](#20-event-topics)
21. [Command Acknowledgments](#21-command-acknowledgments)
22. [Automation — Actions](#22-automation--actions)
23. [Replication](#23-replication)
24. [High Availability](#24-high-availability)
25. [Security](#25-security)
26. [Monitoring](#26-monitoring)
27. [Capacity Planning & OS Tuning](#27-capacity-planning--os-tuning)
28. [AMPS Commands Reference](#28-amps-commands-reference)
29. [Protocol Reference](#29-protocol-reference)
30. [Utilities](#30-utilities)
31. [Deployment Checklist](#31-deployment-checklist)
32. [Troubleshooting](#32-troubleshooting)
33. [Developer SDKs & Flink Connector](#33-developer-sdks--flink-connector)
34. [Scenario-to-Feature Reference](#34-scenario-to-feature-reference)
35. [Glossary](#35-glossary)

---

## 1. What Is AMPS?

**AMPS (Advanced Message Processing System)** by 60East Technologies is a high-performance, low-latency messaging server for data-intensive applications. It is not a traditional message broker or database — it unifies multiple capabilities into a single engine:

- **Pub/Sub messaging** (many-to-many fan-out)
- **Message queues** (single consumer, guaranteed delivery)
- **Current-state storage** (State of the World / SOW — like a real-time database table)
- **Historical replay** (transaction log / journal)
- **Aggregation & analytics** (views, aggregated subscriptions)
- **Integrated replication** (built-in HA and distribution)
- **Content-aware filtering** (SQL-like expressions on message fields, no schema pre-declaration needed)

**Typical production use cases:** trade plant operations (including backtesting), risk calculations, elastic worker farms, view servers, message flow integration between systems.

---

## 2. Core Design Principles

AMPS is built around these engineering principles:

| Principle | Meaning |
|-----------|---------|
| Parallelize work | Exploit modern multi-socket, multi-core NUMA systems and fast storage. |
| Eliminate redundant work | Only do what is actually needed for the requested operation. |
| Reduce cross-system coordination | AMPS handles messaging, storage, replay, and replication together — no separate audit store needed. |
| Content-aware routing | Publishers publish raw data; AMPS inspects content to route to the right subscribers — no need for granular topic hierarchies. |
| Dual delivery paradigm | Pub/sub (fan-out) and queues (single consumer) are first-class citizens. |
| Hardware-aware design | Engineered for NUMA, flash storage, and high-bandwidth networks. |

---

## 3. Platform, Requirements & Installation

### Supported Platforms
- **64-bit Linux** (kernel 2.6 or later, x86-compatible). Production: min 4 GB RAM recommended.
- AMPS does **not** run natively on Windows; use Linux (physical, VM, or container).

### Client SDKs
Java, C#/.NET, C++, Python, JavaScript, Go.

### Installing
Unpack the distribution archive into a directory (referred to as `$AMPSDIR`).

```
$AMPSDIR/
  bin/       # ampServer binary and utilities
  lib/       # shared library dependencies
  docs/      # documentation
  sdk/       # include files for the AMPS extension API
```

Client libraries are a **separate download** from the AMPS developer pages.

### Starting the Server
```bash
# Generate a minimal sample config file
$AMPSDIR/bin/ampServer --sample-config > amps_config.xml

# Start with a config file
$AMPSDIR/bin/ampServer amps_config.xml
```

AMPS reads the config once at startup and keeps it in memory. **Config file changes on disk do not take effect until a restart.**

### Running as a Linux Service
AMPS supports both **systemd** and **System V init** scripts. 60East recommends capturing `stdout` and `stderr` for diagnostics.

---

## 4. Message Concepts

### Topics
A **topic** is a named message stream, like a channel or subject. Topic names are strings.
- Publishers publish to a topic by exact name.
- Subscribers subscribe by exact name **or** by a **regular expression** pattern.
- Example regex topic: subscribing to `orders.*` matches `orders`, `orders-eu`, `orders-us`, etc.
- **A publisher cannot publish to a regex topic** — a published message must have an exact topic name.

### Message Ordering
AMPS provides a **total order guarantee** within a single instance:

- Every subscription to a topic receives messages **in the exact order AMPS received them**.
- This order is preserved in the transaction log across restarts.
- The guarantee applies **across multiple topics** in the same instance for standard topics.
- **Exceptions:** views, queues, and conflated topics have their own ordering semantics (computed asynchronously), and `at-least-once` queues may re-deliver messages out of original order after a lease timeout.

### Messages and Headers
Each AMPS message has:
- A **payload** (the actual message content — JSON, FIX, XML, etc.)
- A set of **header fields** used for routing, filtering, and acknowledgment (e.g., `topic`, `filter`, `bookmark`, `sub_id`, `cmd_id`)

### Select Lists (Field Projection)
Subscribers can request that AMPS return only a subset of fields from each message — similar to `SELECT col1, col2 FROM table` instead of `SELECT *`.

Example select list option on a command:
```
select=[-/,+/event_id,+/description]
```
- `-/` means exclude all fields by default.
- `+/event_id` and `+/description` explicitly include those two fields.
- The select list is applied **after** content filtering — it does not affect which messages are matched, only which fields are delivered.

This is useful for UIs that display a summary table (show only ID + title) and drill down to full detail on demand.

---

## 5. Content Filtering & Expressions

### How Filtering Works
AMPS routes messages based on their **content** — no topic hierarchy redesign needed. A subscriber adds a `filter` to their subscription and AMPS evaluates the filter expression against every incoming message. Only messages where the expression is `true` are delivered to that subscriber.

This is analogous to a SQL `WHERE` clause. The topic is the "table"; the subscription is the "query".

### Filter Syntax
AMPS combines **XPath-style field identifiers** with **SQL-92 operators** and **PCRE2 regular expressions**.

**Field identifiers** use a `/field` path notation. Nested fields use `/parent/child`. XML attributes use `/@attr`.

```
# JSON message filter examples
/status == 'ACTIVE' AND /qty > 100
/price BETWEEN 10.0 AND 20.0
/symbol IN ('IBM', 'MSFT', 'AAPL')
/notes LIKE '%urgent%'
/id IS NOT NULL

# XML message filter
(/FIXML/Order/Instrmt/@Sym == 'IBM') AND (/FIXML/Order/@Px >= 90.00)

# FIX message filter (uses tag numbers as field identifiers)
/35 < 10 AND /34 == /9

# Regular expression match on a field value
REGEXP_MATCH(/description, 'order-[0-9]+')
```

### Supported Operators

| Category | Operators |
|----------|-----------|
| Comparison | `==`, `!=`, `<`, `<=`, `>`, `>=` |
| Range | `BETWEEN ... AND ...` |
| Set | `IN (...)` |
| Pattern | `LIKE` (SQL-style `%` and `_` wildcards) |
| Null | `IS NULL`, `IS NOT NULL` |
| Boolean | `AND`, `OR`, `NOT` |
| Arithmetic | `+`, `-`, `*`, `/`, `%` |
| Conditional | `IF(condition, true_val, false_val)` |

### Performance Tips
- **Equality comparisons** on indexed fields (hash indexes) are the fastest queries.
- `LIKE` and regex operators cannot use hash indexes — they fall back to scanning.
- Index your most-queried fields with a `HashIndex` in the SOW configuration.

---

## 6. AMPS Functions

AMPS provides a rich function library for use in filters, enrichment expressions, and view projections.

### String Functions
| Function | Description |
|----------|------------|
| `INSTR(haystack, needle)` | Position of `needle` in `haystack`; 0 if not found |
| `INSTR_I(...)` | Case-insensitive `INSTR` |
| `STREQUAL(s1, s2)` | String equality (returns 1 or 0) |
| `STREQUAL_I(s1, s2)` | Case-insensitive string equality |
| `BEGINS_WITH(s, prefix)` | True if `s` starts with `prefix` |
| `ENDS_WITH(s, suffix)` | True if `s` ends with `suffix` |
| `CONCAT(s1, s2, ...)` | Concatenate strings |
| `UPPER(s)` / `LOWER(s)` | Convert case |
| `LEFT(s, n)` / `RIGHT(s, n)` | Extract `n` chars from left/right |
| `TRIM(s)` / `LTRIM(s)` / `RTRIM(s)` | Remove whitespace |
| `REGEXP_MATCH(s, pattern)` | Extract text matching a PCRE2 regex |
| `REGEXP_REPLACE(s, pattern, replacement)` | Replace regex match |

### Numeric Functions
| Function | Description |
|----------|------------|
| `ROUND(n, digits)` | Round to N decimal places |
| `CEILING(n)` / `FLOOR(n)` | Round up / down |
| `SQRT(n)`, `POWER(n, exp)` | Square root, power |
| `EXP(n)`, `LOG2(n)` | Exponential, log base 2 |
| `SIGN(n)` | -1, 0, or 1 |
| `GREATEST(a,b,...)` / `LEAST(a,b,...)` | Max / min of arguments |
| `ABS(n)` | Absolute value |
| `WIDTH_BUCKET(val, min, max, count)` | Histogram bucket |
| `ACOS`, `ASIN`, `ATAN`, `COS`, `SIN`, `TAN`, `COT`, `SINH`, `TANH` | Trigonometric |
| `RADIANS(n)` / `DEGREES(n)` | Convert units |

### Date/Time Functions
| Function | Description |
|----------|------------|
| `UNIX_TIMESTAMP()` | Current time as Unix epoch (seconds) |
| `STRPTIME(str, fmt)` | Parse a date string into a timestamp |

### Aggregate Functions (used in Views)
| Function | Description |
|----------|------------|
| `COUNT(*)` | Count of records |
| `SUM(/field)` | Sum of a field across records |
| `MIN(/field)` / `MAX(/field)` | Min / max value |
| `AVG(/field)` | Average |
| `STDDEV_POP(/field)` | Population standard deviation |
| `GROUP_CONCAT(/field, separator)` | Concatenate values |
| `UNIQUE(/field)` | Count of distinct values |

### Message / SOW Meta-Functions
| Function | Description |
|----------|------------|
| `BOOKMARK()` | The bookmark of the current message |
| `SOW_KEY()` | The SOW key of the current message |
| `SOW_KEY_HASH()` | Hash of the SOW key |
| `LAST_UPDATED()` | Timestamp of last update to the SOW record |
| `LAST_LEASED()` | Timestamp of last queue lease |
| `LEASE_COUNT()` | How many times the message has been leased (re-delivered) |
| `CORRELATION_ID()` | The correlation ID from the message header |
| `LAST_READ()` | Last time the record was read |
| `TOPIC_NAME()` | Name of the topic the message was published to |

### Client Functions
| Function | Description |
|----------|------------|
| `USER()` | Authenticated username of the connected client |
| `CLIENT_VERSION()` | Version of the AMPS client library |

### Geospatial Functions
AMPS provides geospatial distance and bounding-box functions for filtering records by geographic coordinates.

### Array Functions
| Function | Description |
|----------|------------|
| `ARRAY_MIN(/arr)` | Minimum value in an array field |
| `ARRAY_TO_STRING(/arr, separator)` | Join array elements into a string |

### CRC Functions
| Function | Description |
|----------|------------|
| `CRC64(/field)` | 64-bit CRC of a field's value |

### Coalesce & Type Constructors
- `COALESCE(a, b, c, ...)` — return the first non-NULL value.
- `NAN_VALUE()` — create a NaN value explicitly.
- `CHAR_VALUE(n)` — produce the character with ASCII code `n`.

---

## 7. State of the World (SOW)

### What Is the SOW?
The SOW is AMPS's **current-value store**. It behaves like a database table that holds the **most recent version** of each distinct record (message). When a new message arrives for an existing key, the SOW is updated (upserted) rather than appended. This gives applications instant access to "what is the current state?" without scanning a message history.

Unlike a traditional database, the SOW stores messages **verbatim** in their original wire format (not shredded/deserialized) for maximum performance.

### How SOW Keys Work
Every SOW topic has a **SOW key** — the equivalent of a primary key. AMPS uses the key to identify which record to upsert.

**Three strategies for key generation:**

1. **Content-derived (most common):** AMPS computes the key from one or more message fields specified in the configuration.
   ```xml
   <Key>/orderId</Key>
   ```
   For a message `{"orderId": "ORD-001", "status": "NEW"}`, AMPS hashes the value `ORD-001` to produce the SOW key. A second publish with `"orderId": "ORD-001"` updates that record.

2. **Publisher-provided:** The publisher supplies the SOW key explicitly in the message header. No key field config is needed, but publishers must manage key uniqueness themselves.

3. **Custom module:** A pluggable C++ module generates the SOW key programmatically (e.g., combining fields, normalizing values).

### SOW Indexes
AMPS maintains two types of indexes on SOW topics to speed up queries:

**1. Hash Index** — pre-defined in config, stored in memory, extremely fast for **exact match** queries (`==` or `IN`).
```xml
<HashIndex>
    <Key>/customerId</Key>
</HashIndex>
<HashIndex>
    <Key>/zipCode</Key>
    <Key>/customerType</Key>  <!-- compound key: both fields together -->
</HashIndex>
```
- Hash index queries must use the **exact same set of fields** as the index and use **exact string comparison** (not numeric comparison).
- `LIKE`, `!=`, `BETWEEN`, `IS NULL` cannot use hash indexes.
- Starting in AMPS 5.3.1.0, `IN` as the first clause of an `AND` expression can also use hash indexes.

**2. Memo Index** — created automatically by AMPS when a field is first used in a query. Supports range queries, regex, and comparisons. Slower than hash indexes but requires no configuration.

### SOW Queries
A **SOW query** (`sow` command) retrieves all records matching a filter at the current point in time. It returns a snapshot — not a live subscription.

```
sow
  topic: orders
  filter: /status == 'ACTIVE' AND /qty > 50
  orderby: /price DESC
```
AMPS returns all matching records, bracketed by `group_begin` and `group_end` markers.

### Atomic Query + Subscribe (`sow_and_subscribe`)
The most common pattern when starting a client: get the current state **and** subscribe to future updates, with **no gap** between them. AMPS guarantees that no message published between the query and the subscription start is missed.

Flow:
1. AMPS sends all current SOW records matching the filter.
2. AMPS sends a `completed` acknowledgment to mark end of initial data.
3. AMPS continues sending new/updated messages matching the filter in real time.

### Historical SOW Queries
If a SOW topic is configured with a `History` block, you can query what the **current state was at a specific past point in time** — not message-by-message replay (that's the transaction log), but "what was the value of this record at 9am yesterday?"

```xml
<Topic>
    <Name>catalog</Name>
    <Key>/sku</Key>
    <MessageType>json</MessageType>
    <FileName>./sow/%n.sow</FileName>
    <History>
        <Window>7d</Window>        <!-- keep 7 days of historical state -->
        <Granularity>15m</Granularity> <!-- snapshot every 15 minutes -->
    </History>
</Topic>
```

### SOW Delete
Applications can delete records from the SOW using the `sow_delete` command. Three ways to specify which records to delete:

1. **Content filter:** Delete all records matching an expression. Use `1=1` to delete everything.
2. **SOW key list:** Provide a list of exact SOW key values (most efficient).
3. **Message data:** Provide a sample message; AMPS derives the SOW key from it and deletes that record.

When a record is deleted, AMPS sends an **OOF message** to subscriptions that have opted in.

### Per-Message Expiration (TTL)
SOW records can have a **time-to-live**. When a record's lifetime expires, AMPS removes it from the SOW and sends an OOF message to affected subscribers.

Two ways to set expiration:
- **Topic-level default** — applies to all messages in the topic unless overridden.
- **Per-message** — the publisher sets the expiration in the message header, overriding the topic default. Setting expiration to `0` on a message disables expiration for that message.

```xml
<SOW>
    <Topic>
        <Name>ORDERS</Name>
        <FileName>sow/%n.sow</FileName>
        <Expiration>30s</Expiration>   <!-- default: 30 second TTL -->
        <Key>/orderId</Key>
    </Topic>
</SOW>
```

Every publish or delta_publish to an existing record **resets its expiration timer**.

### SOW Maintenance
You can schedule automated maintenance on SOW topics using Actions:
- **Compact** — reclaim disk space from deleted records.
- **Delete expired records** — explicit cleanup on a schedule.

### Logical Topics (Regex SOW Topics)
A single physical SOW file can store multiple logical topic namespaces using a regex as the topic name. Useful when converting a system that used many fine-grained topic names.

```xml
<Topic>
    <Name>orders-.*</Name>  <!-- matches orders-us, orders-eu, etc. -->
    <MessageType>json</MessageType>
    <Key>/orderId</Key>
</Topic>
```

### Message Preprocessing & Enrichment
AMPS can modify a message **before** storing it in the SOW. Two stages:

**Preprocessing** — runs before the SOW key is computed. Use it when you need to transform a field that is part of the key.
```xml
<Preprocessing>
    <!-- Normalize source field; remove it if not in allowed list -->
    <Field>IF(/source IN ('a','e','f'), /source, NULL)
           AS /source HINT OPTIONAL</Field>
</Preprocessing>
```

**Enrichment** — runs after the SOW key is computed but before the message is written to the transaction log, SOW, views, or delivered to subscribers. Use it to add computed fields or default values.
```xml
<Enrichment>
    <!-- Compute fullName from two fields -->
    <Field>CONCAT(/firstName, " ", /lastName) AS /fullName</Field>
</Enrichment>
```

The `HINT OPTIONAL` directive means: if the result of the expression is NULL, omit the field entirely from the output rather than serializing a null value.

Enrichment fields use `HINT SET_CURRENT` when the enrichment should operate on the merged (current) state of the message rather than only the incoming fields.

---

## 8. Transaction Log (Journal)

### What Is the Transaction Log?
The transaction log is an **ordered, durable, append-only record** of every publish command AMPS processes. Each entry records the message exactly as received, in the order it arrived. Unlike the SOW (which keeps only the latest version), the transaction log keeps **every individual publish**.

The transaction log is stored in **journal files** on disk. It is the foundation for queues, replication, and durable subscriptions.

### What the Transaction Log Is Used For

| Use | How |
|-----|-----|
| **Resumable subscriptions** | Clients re-subscribe from their last ACK'd bookmark after a failure or restart. |
| **Message replay / audit** | Replay an exact sequence of messages, in order, at a configurable rate. |
| **Backtesting** | Replay at 2× or 10× original rate to stress-test or validate algorithms. |
| **Message queues** | A queue is a view over the transaction log. |
| **Replication** | All cross-instance replication flows through the transaction log. |

### Bookmark Subscriptions
Every message in the transaction log has a **bookmark** — an opaque, monotonically-increasing unique identifier assigned by AMPS. A subscriber can request to start from any point in the log by specifying a bookmark.

**Special bookmark values:**

| Bookmark | Meaning |
|----------|---------|
| `EPOCH` | Replay from the very beginning of the transaction log. |
| `MOST_RECENT` | Alias for the last bookmark the client library has acknowledged. Managed automatically by the `HAClient`. |
| `NOW` (or `0\|1\|`) | Start from this moment — no replay. |
| Timestamp | Start from the first message at or after a given timestamp. |
| Specific bookmark string | Start from the message immediately after this bookmark. |

Multiple bookmarks can be provided as a comma-separated list; AMPS starts from the earliest one.

### Bookmark Subscription Flow
1. Client requests a bookmark subscription with a starting bookmark.
2. AMPS replays messages from the transaction log from that point, matching the topic and filter.
3. Once replay catches up to "now", AMPS sends a `completed` acknowledgment.
4. AMPS continues sending live messages as they arrive.

### Durability Options for Bookmark Subscriptions

**Default behavior:** AMPS delivers a message to the subscriber once the message is persisted to the **local** transaction log.

**`fully_durable` option:** AMPS also waits until all **synchronous replication destinations** have persisted the message before delivering it. More latency, but guarantees multi-instance durability before the client processes the message.

**`live` option:** AMPS delivers messages **before** they are persisted to the transaction log. Lowest latency, but risk of inconsistency if the server fails before persisting. Use only when the latency reduction justifies the risk.

### Journal File Management
- Journal files are rotated when they reach a configured size.
- Old journal files can be **automatically compressed** or **deleted** on a schedule via Actions.
- `amps-grep` searches journal files for specific topic messages.
- `amps_journal_dump` inspects the raw binary journal content.

### Configuration
```xml
<TransactionLog>
    <Journal>
        <!-- Directory where journal files are stored -->
        <JournalDirectory>./journal/</JournalDirectory>
        <!-- Preallocate N files on startup to avoid latency spikes -->
        <PreallocatedJournalFiles>4</PreallocatedJournalFiles>
    </Journal>
    <Topic>
        <Name>orders</Name>
        <MessageType>json</MessageType>
    </Topic>
</TransactionLog>
```

---

## 9. Message Queues

### What Are AMPS Queues?
AMPS queues provide **single-consumer, guaranteed delivery** of messages. Each message is delivered to exactly one subscriber, and re-delivered if that subscriber fails to acknowledge it. This is fundamentally different from pub/sub, where every subscriber receives every message.

### How Queues Are Implemented
A queue is defined as a **view over one or more underlying topics**. Those underlying topics **must** be recorded in a transaction log — the queue tracks message positions within the transaction log.

Publishers publish to the **underlying topic** using a regular `publish` command. AMPS automatically routes messages from the topic into the queue. Consumers subscribe to the **queue name**, not the underlying topic.

### Message Lifecycle in a Queue
```
Publisher → publishes to "orders" topic
         → AMPS records in transaction log
         → Queue sees new entry
         → Message marked: AVAILABLE
         → Consumer subscribes to "orders-queue"
         → AMPS delivers message to consumer
         → Message marked: LEASED (with expiry)
         → Consumer processes message
         → Consumer calls message.ack()
         → AMPS marks message ACKNOWLEDGED (removed from queue)
         
         If no ACK within LeaseTimeout:
         → Lease expires → message returns to AVAILABLE
         → AMPS re-delivers to next consumer
```

### Delivery Semantics

**`at-least-once`** (most common):
- AMPS delivers the message with a **lease** (time-limited exclusive hold).
- The consumer must ACK before the `LeaseTimeout` expires.
- If the consumer crashes or doesn't ACK in time, AMPS re-delivers to another consumer.
- A message **may** be processed more than once (use idempotent processing).

**`at-most-once`**:
- AMPS delivers and marks the message done immediately — no re-delivery if the consumer fails.
- Suitable when duplicate processing is worse than missing a message.

### Delivery Fairness Algorithms
When multiple consumers are subscribed to the same queue, AMPS decides which consumer gets the next message based on the configured algorithm:

| Algorithm | Strategy | Default for |
|-----------|----------|------------|
| `proportional` | Deliver to the consumer with the **highest unused backlog ratio**. Keeps all consumers equally loaded. | `at-least-once` |
| `round-robin` | Deliver to the next consumer in rotation, skipping consumers with a full backlog. | `at-most-once` |
| `fast` | Deliver to the **first consumer found** with available backlog. Minimizes latency at the cost of fairness. | — |

**Example (proportional):**
- Consumer A: backlog=2, outstanding=1 → 50% used
- Consumer B: backlog=4, outstanding=3 → 75% used
- Consumer C: backlog=10, outstanding=4 → 40% used

Next message goes to Consumer C (least loaded).

### Backlog (Flow Control)
The **backlog** is the maximum number of un-ACK'd messages a consumer can have outstanding at any time. When a consumer's backlog is full, AMPS stops sending it new messages until it ACKs some. This is configured per subscription, not per queue.

A larger backlog allows the consumer to pipeline work and improves throughput. A smaller backlog limits memory usage and caps in-flight work.

### Priority
A queue can be configured with a `Priority` field. AMPS delivers higher-priority messages first. Within the same priority level, AMPS delivers in arrival order (FIFO).

### Barrier Messages
A **barrier** is a special message that forces AMPS to pause delivery until all messages published **before** the barrier have been acknowledged. After the barrier itself is ACK'd, delivery of subsequent messages resumes. This is used to create synchronization points in a processing pipeline.

A barrier message is identified by a `BarrierExpression` in the queue config — any message matching that expression is treated as a barrier.

### Acknowledging Messages
The consumer **must** call `message.ack()` using the AMPS SDK to acknowledge queue messages. This internally sends a `sow_delete` command with the message's bookmark.

> **Important:** Do **not** manually construct a `Command("ack")` with `setTopic()` — incorrect usage of the internal `StringField` state causes a `NullPointerException` in `executeAsyncNoLock`. Always use the SDK's `message.ack()` method.

### Message Expiration in Queues
Messages in a queue can expire. When a message has been waiting in the queue longer than the configured `Expiration` time without being leased, AMPS removes it. Expiration is **not** treated as an error. The queue's `ExpirationModel` parameter controls how the queue handles messages that have a per-message expiration from the underlying SOW topic.

### Advanced Queue Features

**Querying a queue as a view:**
You can issue a `sow` command against a queue to see what messages are currently available (not leased). This is read-only — it does not lease or remove messages.

**Views and aggregated subscriptions over queues:**
You can create a view with a queue as the underlying topic. The view reflects the currently **available** (not leased) messages. When a message is leased, it disappears from the view; when returned to the queue, it reappears.

**Content filtering for consumers:**
Consumers can subscribe to a queue with a content filter — they only receive messages matching their filter.

**Delta publish with queues:**
If the underlying topic is a SOW topic, publishers can delta_publish to it. The full merged message is what gets added to the queue (not the delta).

**Replicated queues:**
See [Section 23 — Replication](#23-replication) for distributed queue behavior.

### Handling Unprocessed / Expired Messages
An Action can be triggered `On SOW Message Expiration` for the queue, allowing you to redirect expired queue messages to a dead-letter topic or archive them elsewhere.

### Replacing a Queue Subscription
A consumer can replace its existing queue subscription (e.g., to change its content filter) using the `replace` option on the `subscribe` command with the same `sub_id`.

### Configuration
```xml
<SOW>
    <Queue>
        <Name>orders-queue</Name>
        <MessageType>json</MessageType>
        <UnderlyingTopic>orders</UnderlyingTopic>
        <Semantics>at-least-once</Semantics>
        <LeaseTimeout>30s</LeaseTimeout>
        <Expiration>1h</Expiration>
        <MaxBacklog>10</MaxBacklog>
        <DeliveryFairness>proportional</DeliveryFairness>
        <!-- Optional priority field -->
        <!-- <Priority>/priority</Priority> -->
        <!-- Optional barrier expression -->
        <!-- <BarrierExpression>/type == 'flush'</BarrierExpression> -->
    </Queue>
</SOW>
```

The underlying topic (`orders` above) **must** also be recorded in a `<TransactionLog>` for the queue to function.

---

## 10. Delta Publish & Delta Subscribe

### Delta Publish (`delta_publish`)
Instead of republishing a full message to update a SOW record, a publisher can send only the **fields that changed**. AMPS merges the incoming partial message into the existing SOW record.

**How it works:**
1. Publisher sends a `delta_publish` with only changed fields.
2. AMPS reads the current SOW record.
3. AMPS merges the changed fields into the current record.
4. The fully merged result is written to the SOW, transaction log, views, and delivered to subscribers.

**When to use it:**
- When bandwidth is scarce and messages are large.
- When updates to a record are frequent but small (e.g., updating only the `status` field of a large order object).

**Tradeoff:** Delta publish uses more CPU on the AMPS server (parse + merge + re-serialize). For small messages, a full publish may be cheaper overall.

**Limitations:** Not supported for `binary` or `struct` message types. Also not supported for `composite-global` message types.

### Delta Subscribe (`delta_subscribe`)
Instead of receiving full messages every time a SOW record changes, a subscriber can request only the **fields that changed** in each update.

**How it works:**
1. Subscriber issues `delta_subscribe` (or `sow_and_delta_subscribe`).
2. AMPS tracks the last state delivered to each delta subscriber.
3. When a record changes, AMPS computes the diff and sends only the changed fields.
4. The SOW key field is **always included** (so the subscriber knows which record was updated), plus all changed fields.

**Independence:** Delta publish and delta subscribe are completely independent. A publisher can do full publishes while a subscriber uses delta subscribe, and vice versa.

---

## 11. Conflated Subscriptions & Conflated Topics

### The Problem Conflation Solves
When a data source updates very rapidly (e.g., market prices ticking multiple times per second), a subscriber that only needs periodic snapshots (e.g., a screen refreshing every 2 seconds) would receive far more messages than it can use. Conflation merges multiple updates for the same record into one, delivering only the **latest** value at each interval.

### Per-Subscription Conflation
A subscriber can request conflation for its own subscription without any server-side topic config. AMPS holds updates for the configured interval and delivers the latest state at the end of each interval.

**Use when:** Only a small number of subscribers need conflation, or conflation is needed only situationally.

```
subscribe
  topic: PRICING
  opts: conflation=2s,conflation_key=/tickerId
```
For each distinct `tickerId`, AMPS holds updates for 2 seconds. If multiple price updates arrive within 2 seconds for the same `tickerId`, only the last one is delivered.

### Conflated Topics (Server-Side)
A conflated topic is a server-defined topic that applies conflation to **all subscribers** uniformly. It is a copy of an underlying SOW topic with a configurable update interval.

**Use when:** All or most subscribers for a topic benefit from the same conflation settings. More efficient than per-subscription conflation because AMPS only computes conflation once, not once per subscriber.

```xml
<SOW>
    <!-- The underlying "live" topic -->
    <Topic>
        <Name>PRICING</Name>
        <Key>/tickerId</Key>
        <MessageType>json</MessageType>
    </Topic>

    <!-- Conflated view of PRICING — max 1 update per tickerId per 3 seconds -->
    <ConflatedTopic>
        <Name>PRICING-3S</Name>
        <UnderlyingTopic>PRICING</UnderlyingTopic>
        <Interval>3s</Interval>
    </ConflatedTopic>
</SOW>
```

Subscribers that need real-time ticks subscribe to `PRICING`. Subscribers that only need periodic snapshots (e.g., a dashboard) subscribe to `PRICING-3S`.

### Ordering with Conflation
AMPS **does not** guarantee ordering for conflated messages — the latest value is delivered, not every intermediate value in sequence. This is expected behavior for conflation.

---

## 12. Views & Aggregation

### What Is a View?
A **view** in AMPS is a **materialized aggregation** defined over one or more SOW topics. It is analogous to a materialized view in RDBMS. AMPS computes and stores the view result in memory, keeping it updated as the underlying topics change.

Subscribers can subscribe to a view exactly like any SOW topic — they receive updates whenever the aggregated result changes.

Views are useful for:
- Reducing network traffic (sending pre-aggregated summaries instead of raw records)
- Simplifying subscriber code (subscriber doesn't need to aggregate)
- Creating join-based views across multiple topics

### Single Topic View (Projection + Aggregation)
```xml
<SOW>
    <Topic>
        <Name>orders</Name>
        <Key>/orderId</Key>
        <MessageType>json</MessageType>
    </Topic>

    <View>
        <Name>order-summary</Name>
        <UnderlyingTopic>orders</UnderlyingTopic>
        <!-- Project only specific fields -->
        <Projection>
            <Field>/orderId</Field>
            <Field>/customerId</Field>
            <Field>SUM(/price * /qty) AS /totalValue</Field>
            <Field>COUNT(*) AS /lineCount</Field>
        </Projection>
        <!-- Group by customerId (like GROUP BY) -->
        <GroupBy>/customerId</GroupBy>
    </View>
</SOW>
```

### Multi-Topic View (JOIN)
AMPS supports joining messages from two or more SOW topics on a common key field. In database terms, this is a **LEFT OUTER JOIN** — records from the primary topic always appear, with NULL values for fields from the joined topic if no matching record exists.

```xml
<View>
    <Name>order-with-address</Name>
    <UnderlyingTopic>
        <!-- Join Orders to Addresses on CustomerID -->
        <Join>[Orders].[/CustomerID]=[Addresses].[/CustomerID]</Join>
    </UnderlyingTopic>
    <Projection>
        <Field>[Orders]./orderId AS /orderId</Field>
        <Field>[Orders]./totalValue AS /totalValue</Field>
        <Field>[Addresses]./city AS /city</Field>
    </Projection>
</View>
```

**JOIN behavior:**
- If a `CustomerID` exists in both topics → a projected record is created.
- If a `CustomerID` exists only in `Addresses` → no record is produced.
- If a `CustomerID` exists only in `Orders` → record is produced with NULL for Addresses fields.
- NULL values are **not** considered equivalent in JOIN expressions (ANSI SQL behavior). This can be changed with `JoinNullEquivalency`.
- Join fields are compared as **strings** — numeric equivalence is not implied (`12345` ≠ `12345.00`).

### Multi-Field Joins
```xml
<UnderlyingTopic>
    <Join>[Orders].[/OrderId]=[OrderExtraInfo].[/OrderId]</Join>
    <Join>[Orders].[/OrderType]=[OrderExtraInfo].[/OrderType]</Join>
</UnderlyingTopic>
```
Multiple `Join` clauses are combined with logical AND — both conditions must be true.

### Cross-Message-Type Joins
```xml
<Join>[nvfix].[Orders].[/CustomerID]=[json].[Addresses].[/CustomerID]</Join>
```
When message types differ, the `[messagetype]` prefix is required.

### Views with Filters
You can add a `Filter` to a view's `UnderlyingTopic` so that only matching records are included in the view:
```xml
<UnderlyingTopic>
    <Filter>/status == 'ACTIVE'</Filter>
</UnderlyingTopic>
```

### View Update Timing
AMPS updates each view asynchronously, after a publish or delta_publish to an underlying topic is processed. Updates are processed in order for each view. The processing latency is typically very small but is not zero.

### Inline Conflation for Views
Views can specify an inline conflation interval to limit how frequently the view sends updates to subscribers:
```xml
<View>
    <Name>pricing-view</Name>
    <Interval>500ms</Interval>
    ...
</View>
```

### Aggregated Subscriptions (On-Demand Aggregation)
Instead of pre-defining a view, a subscriber can request aggregation on the fly by including aggregation expressions in a `sow` or `sow_and_subscribe` command. The result is computed at query time.

**Use aggregated subscriptions when:**
- The aggregation is used infrequently (no point maintaining a permanent view).
- The aggregation is needed ad hoc.

**Use views when:**
- Multiple clients need the same aggregation frequently (AMPS only computes it once for all of them).
- Subscribers need live updates as the underlying data changes.

---

## 13. Out-of-Focus (OOF) Messages

### What Are OOF Messages?
When a SOW record **previously matched** a subscription's filter but subsequently **no longer matches**, AMPS sends an **Out-of-Focus (OOF)** message to notify the subscriber. This allows the subscriber to remove or update that record from its local view without polling.

OOF messages are **optional** — the subscriber must explicitly request them using the `send_oof` option.

### When AMPS Sends an OOF Message

| Cause | OOF Triggered |
|-------|--------------|
| Record deleted (`sow_delete`) | Yes |
| Record expired (TTL) | Yes |
| Record updated so it no longer matches the filter | Yes |
| Record moved outside the pagination window | Yes |
| Subscriber is no longer entitled to view the updated record | Yes |

### OOF Message Content
- The OOF message body contains the **updated state** of the record (the state that caused it to go out of focus), **except** when the record was deleted or expired — in those cases, AMPS sends the state **before** the change.
- The header includes the reason for the OOF, allowing the subscriber to take different UI actions (e.g., show a "Cancelled" badge vs. a "Completed" badge vs. simply removing the row).
- For `delta_publish` that causes an OOF, AMPS sends the **fully merged** record, not the delta.
- For conflated subscriptions, the OOF body contains the **last state delivered to the subscriber**.

### OOF Support Matrix
OOF is only supported for:
- SOW topics
- Conflated topics
- Views

Regular pub/sub topics without a SOW do not support OOF (there is no "current state" to compare against).

---

## 14. Select Lists — Field Projection

### What Are Select Lists?
A select list tells AMPS to return only a subset of fields from each message, like `SELECT col1, col2` instead of `SELECT *`. This reduces network traffic when subscribers don't need the full message.

### Syntax
The select list is included as an option on the command:
```
select=[-/,+/event_id,+/description,+/status]
```
- `-/` — exclude all fields by default (start with nothing).
- `+/field_name` — include this specific field.
- Alternatively, start with `+/` (include all) and use `-/field_name` to exclude specific fields.

### Rules
- Applied **after** content filtering. The select list does not affect which messages are matched — only what is delivered.
- The underlying SOW record and transaction log are unaffected.
- Not all message types support select lists. Formats that require a complete, ordered message (e.g., some binary formats) cannot partially serialize.

---

## 15. SOW Result Management — Sorting & Pagination

### Sorting Results with `OrderBy`
SOW queries and `sow_and_subscribe` commands can specify a sort order:
```
orderby: /price DESC, /orderId ASC
```

Syntax: `/field [ASC | DESC] [TEXT]`
- `ASC` — ascending order (default).
- `DESC` — descending order.
- `TEXT` — treat the field as a string for sorting. Useful when values could be interpreted as numbers but should sort lexicographically (e.g., IDs like `"123"` vs `"99"`).

### Limiting Results with `top_n`
`top_n=20` returns only the first 20 matching records. Without `skip_n`, this applies only to the initial SOW query portion (not the live subscription).

### Skipping Records with `skip_n`
`skip_n=40` skips the first 40 matching records. **Must be used together with `top_n`**.

### Paginated Subscriptions
When both `top_n` and `skip_n` are specified on a `sow_and_subscribe` command, AMPS creates a **paginated subscription**. AMPS maintains a sorted window of results and delivers only records within the current page.

**Example:** Page 3 of 20 records per page:
```
sow_and_subscribe
  topic: orders
  filter: /status == 'ACTIVE'
  orderby: /createdAt DESC
  top_n: 20
  skip_n: 40
```

When the underlying data changes and records move in or out of the page window, AMPS delivers OOF messages (for records leaving the page) and normal messages (for records entering the page). This allows a real-time paginated data grid.

---

## 16. Message Types

### Overview
A **message type** defines the wire format of a message's payload. Each SOW topic and transport declares a message type. AMPS uses the message type to parse field values for filtering, aggregation, and projection.

AMPS's message processing engine is **message-type agnostic** — the same filter expressions, SOW operations, and queue features work identically for all types. The message type only determines how AMPS reads and writes the payload.

### Supported Message Types

| Type | Description | Special Notes |
|------|------------|---------------|
| `json` | Standard JSON | Default, most commonly used |
| `fix` | FIX protocol (tag=value pairs) | Field identifiers are tag numbers: `/35`, `/49` |
| `nvfix` | Name/Value FIX format | Like FIX but with named fields instead of tag numbers |
| `xml` | XML with XPath access | Field paths use `/element/subelement/@attribute` |
| `messagepack` | Binary MessagePack | Compact binary format, faster to parse than JSON |
| `bflat` | 60East proprietary binary | Extremely high-performance; optimized for AMPS |
| `bson` | Binary JSON | Used by MongoDB ecosystem tools |
| `protobuf` | Google Protocol Buffers | Requires `.proto` schema loaded at AMPS config time |
| `binary` | Raw bytes, uninterpreted | **No filtering, no SOW indexing, no views** |
| `struct` | Typed struct format | **No views, no composite-global** |
| `composite` | Combination of multiple types | Two variants: `composite-local` and `composite-global` |

### Composite Messages
Composite messages allow a single AMPS message to contain **multiple parts** of different types. Common use case: JSON metadata header + binary image payload.

**`composite-local`** — each part is treated as an independent section. You can filter on fields from any part, create views, and aggregate. Parts maintain separate XPath identities.

**`composite-global`** — all parts are treated as elements of a single document. `delta_publish` and views are not supported with `composite-global`.

**Unparsed payload section:** Every composite message type automatically includes an unparsed byte section at the end. This allows binary content (images, audio) to ride along with parsed metadata, without AMPS needing to interpret the binary data.

### FIX / NVFIX Field Separators
FIX and NVFIX message types support custom field and message separator configuration.

### Protocol Buffers (Protobuf)
Requires the `.proto` file to be loaded in the AMPS configuration. Nested message types are accessed using path notation (`/outer/inner/field`). Union types are supported via `IS NOT NULL` filtering.

---

## 17. Transports

### What Are Transports?
Transports define **how AMPS accepts incoming connections** from clients or other AMPS instances (for replication). Each transport specifies:
- The **network type** (TCP, WebSocket, TLS, etc.)
- The **address and port** to listen on
- The **protocol** (message header format)
- Optional authentication, entitlement, and slow-client policies

### Client Transports

| Type | Use Case |
|------|---------|
| `tcp` | Standard application clients (Java, C++, Python, Go, C#) |
| `websocket` | Browser-based clients (JavaScript), Galvanometer admin UI |
| `ssl` / `tls` | Encrypted TCP connection — same as `tcp` but with TLS |
| Unix Domain Socket (UDS) | High-speed local IPC on the same host |

### Replication Transports
Separate transport type `amps-replication` for incoming replication connections from other AMPS instances. Configured in `<Transports>` separately from client transports.

### Protocols (Header Format)
| Protocol | Used By |
|----------|---------|
| `amps` | All application clients (default, recommended) |
| `websocket` | WebSocket connections |
| `legacy` FIX/NVFIX/XML | Legacy clients (supported but not enhanced further) |

> The `amps` protocol is recommended for all new development. Legacy protocols will continue to work but won't receive new AMPS features.

### Configuration
```xml
<Transports>
    <!-- Application client transport -->
    <Transport>
        <Name>tcp-json</Name>
        <Type>tcp</Type>
        <InetAddr>0.0.0.0:9007</InetAddr>
        <MessageType>json</MessageType>
        <Protocol>amps</Protocol>
    </Transport>

    <!-- WebSocket transport for browser clients and Galvanometer -->
    <Transport>
        <Name>ws-admin</Name>
        <Type>websocket</Type>
        <InetAddr>0.0.0.0:9008</InetAddr>
        <MessageType>json</MessageType>
        <Protocol>websocket</Protocol>
    </Transport>

    <!-- TLS-encrypted client transport -->
    <Transport>
        <Name>tls-clients</Name>
        <Type>tcp</Type>
        <InetAddr>0.0.0.0:9009</InetAddr>
        <TLS>
            <Certificate>/path/to/cert.pem</Certificate>
            <Key>/path/to/key.pem</Key>
        </TLS>
    </Transport>

    <!-- Replication transport for incoming replication -->
    <Transport>
        <Name>replication</Name>
        <Type>amps-replication</Type>
        <InetAddr>0.0.0.0:10005</InetAddr>
    </Transport>
</Transports>
```

### Per-Transport Overrides
Each transport can override instance-level defaults for:
- Authentication module
- Entitlement module
- Slow client policies (`MessageMemoryLimit`, `MessageDiskLimit`)
- Compression settings

### Transport Filters
A transport filter is a pluggable module that inspects incoming commands and can reject or modify them before AMPS processes them. Useful for rate limiting, IP-based access control, or custom request validation.

### HTTP Preflight Support
AMPS WebSocket transports support HTTP CORS preflight requests for browser-based clients.

---

## 18. Configuration File Structure

### Structure Overview
AMPS uses an XML config file. The root element is `<AMPSConfig>`. The file is loaded **once at startup** — changes on disk require a server restart.

```xml
<AMPSConfig>
    <!-- Required: unique instance name -->
    <Name>my-amps-instance</Name>

    <!-- Optional: human-readable description -->
    <Description>Production order processing instance</Description>

    <!-- Transports: how clients connect -->
    <Transports>
        <Transport> ... </Transport>
    </Transports>

    <!-- SOW topics, queues, views, conflated topics -->
    <SOW>
        <Topic> ... </Topic>
        <Queue> ... </Queue>
        <View> ... </View>
        <ConflatedTopic> ... </ConflatedTopic>
    </SOW>

    <!-- Transaction log journals -->
    <TransactionLog>
        <Journal> ... </Journal>
        <Topic> ... </Topic>
    </TransactionLog>

    <!-- Outgoing replication configuration -->
    <Replication>
        <Destination> ... </Destination>
    </Replication>

    <!-- Logging targets -->
    <Logging>
        <Target> ... </Target>
    </Logging>

    <!-- Automated actions (scheduled, event-driven) -->
    <Actions>
        <Action> ... </Action>
    </Actions>

    <!-- Loadable modules (auth, entitlement, custom types) -->
    <Modules>
        <Module> ... </Module>
    </Modules>

    <!-- Default authentication for all transports -->
    <Authentication>
        <Module>my-auth-module</Module>
    </Authentication>

    <!-- Default entitlement for all transports -->
    <Entitlement>
        <Module>my-entitlement-module</Module>
    </Entitlement>

    <!-- Instance-wide slow client settings -->
    <MessageMemoryLimit>2GB</MessageMemoryLimit>
    <MessageDiskLimit>10GB</MessageDiskLimit>

    <!-- Admin/monitoring endpoint -->
    <Admin>
        <InetAddr>0.0.0.0:8085</InetAddr>
    </Admin>
</AMPSConfig>
```

### Unit Abbreviations in Config Values
AMPS accepts human-readable units for sizes and durations:

| Unit | Example | Meaning |
|------|---------|---------|
| `s` | `30s` | 30 seconds |
| `m` | `5m` | 5 minutes |
| `h` | `2h` | 2 hours |
| `d` | `7d` | 7 days |
| `KB` / `MB` / `GB` | `512MB` | Storage sizes |

### Environment Variables
Config values can reference shell environment variables:
```xml
<InetAddr>${AMPS_HOST}:${AMPS_PORT}</InetAddr>
```

AMPS also provides internal variables:

| Variable | Value |
|----------|-------|
| `AMPSDIR` | Directory containing the running `ampServer` binary |
| `AMPS_INSTANCE_NAME` | The `<Name>` of this AMPS instance |
| `AMPS_DATETIME` | Current timestamp in ISO-8601 format |
| `AMPS_UNIX_TIMESTAMP` | Current time as a Unix epoch |
| `AMPS_BYTE_XX` | Insert byte with hex value `XX` (useful for non-printable separators) |

### Modular Config with `<Include>`
Large configurations can be split into multiple files:
```xml
<Include>./transports.xml</Include>
<Include>./sow-topics.xml</Include>
```

### Slow Client Management Settings
```xml
<!-- Instance-wide memory limit for all client buffers combined -->
<MessageMemoryLimit>4GB</MessageMemoryLimit>

<!-- Instance-wide disk limit (for client offlining) -->
<MessageDiskLimit>20GB</MessageDiskLimit>
```

When `MessageMemoryLimit` is exceeded, AMPS **offlines** the most-consuming client (spills its buffered messages to disk). When `MessageDiskLimit` is also exceeded, AMPS **disconnects** the client and clears its buffer.

Per-client slow client limits can also be set at the transport level to protect against a single misbehaving client.

### Minidump Settings
AMPS generates a minidump (compact diagnostic snapshot) on crash rather than a full core dump. The minidump is much faster to produce on large instances.
- Configure max number of minidump files to retain.
- Configure where to write them.
- Recommend setting `ulimit -c 0` to prevent Linux from also writing a full core file.

---

## 19. Logging

### Log Message Format
```
2021-11-23T14:49:38.3442510-08:00 [1] info: 00-0015 AMPS initialization completed (0 seconds).
│                                  │   │     │       └─ Human-readable description
│                                  │   │     └─ Error ID: category-number (CC-NNNN)
│                                  │   └─ Log level
│                                  └─ AMPS thread number
└─ ISO-8601 timestamp
```

Error IDs have the format `CC-NNNN` where `CC` is the AMPS component/category and `NNNN` is the unique code within that component.

### Log Levels (lowest to highest severity)

| Level | Description | Recommended Use |
|-------|------------|----------------|
| `developer` | Detailed internal state; for 60East developers | Never in production |
| `trace` | All inbound/outbound data, full message content | Dev/test only; protect logs — contains message data |
| `stats` | Statistics messages | Optional |
| `info` | General operation messages | **Recommended for production** |
| `warning` | Issues AMPS corrected automatically | Minimum for production |
| `error` | Processing had to stop for specific operations | Always log |
| `critical` | Severe — AMPS may be degraded | Always log |
| `emergency` | AMPS cannot continue | Always log |

When you configure a level, AMPS logs **that level and all higher levels**.

### Log Targets
AMPS can log to multiple targets simultaneously: files, syslog, stdout/stderr.

```xml
<Logging>
    <!-- Log to stdout at info level and above -->
    <Target>
        <Protocol>stdout</Protocol>
        <Level>info</Level>
    </Target>

    <!-- Log to a rotating file -->
    <Target>
        <Protocol>file</Protocol>
        <FileName>./logs/amps-%Y%m%d.log</FileName>
        <Level>info</Level>
        <!-- Exclude specific error IDs -->
        <ExcludeErrors>00-0001,00-0004,12-1.*</ExcludeErrors>
        <!-- Explicitly include a specific error ID even if below level -->
        <IncludeErrors>00-0002</IncludeErrors>
    </Target>

    <!-- Log to syslog -->
    <Target>
        <Protocol>syslog</Protocol>
        <Level>warning</Level>
    </Target>
</Logging>
```

File names support date/time formatting tokens (`%Y`, `%m`, `%d`, `%H`, `%M`, `%S`) to create rolling log files.

### Looking Up Error Codes
```bash
$AMPSDIR/bin/ampserr --list          # list all error codes
$AMPSDIR/bin/ampserr 15-0008         # explain a specific code
```

---

## 20. Event Topics

AMPS publishes internal event notifications to **reserved topics** starting with `/AMPS/`. Applications can subscribe to these topics like any other AMPS topic (using content filters, etc.), allowing one application to monitor what other clients are doing.

Event topic messages are delivered in the message type of the subscribing connection (JSON, FIX, XML, etc.).

### `/AMPS/ClientStatus`
Published whenever:
- A client connects or disconnects
- A client issues a `logon` command
- A client subscribes or unsubscribes
- A client queries the SOW
- A client issues a `sow_delete`
- A client fails authentication

**Example JSON message:**
```json
{
  "ClientStatus": {
    "timestamp": "20250909T171919.976304Z",
    "event": "sow",
    "client_name": "my-app",
    "connection_name": "AMPS-Sample-any-tcp-9-242891694350073019",
    "topic": "orders",
    "filter": "/qty > 50",
    "sub_id": "1"
  }
}
```

Key fields: `event` (the command type), `client_name`, `topic`, `filter`, `reason` (on disconnect — explains why).

### `/AMPS/SOWStats`
Published periodically with statistics about each SOW topic (record counts, memory usage, etc.). Can be persisted automatically.

### Event Topic Persistence
Event topics can be persisted to the SOW so the latest state is available to new subscribers without waiting for the next event.

---

## 21. Command Acknowledgments

### How Acknowledgments Work
AMPS command processing is **asynchronous by design**. The client sends a command; AMPS processes it and returns an `ack` message at various checkpoints. The client correlates each `ack` back to its command using the `cmd_id` field.

Acknowledgments are **optional**. The AMPS client libraries automatically request the minimum set of ACKs needed to maintain their guarantees. Applications rarely need to manage ACKs explicitly.

### Acknowledgment Types

| Type | Meaning |
|------|---------|
| `received` | AMPS has received the command from the network |
| `processed` | AMPS has parsed and begun processing the command |
| `completed` | The command (or the synchronous portion of it) has finished |
| `persisted` | The result has been written to durable storage (transaction log and/or synchronous replication destinations) |
| `stats` | Returns statistics associated with the command (e.g., number of records matched by a SOW query) |

**Important:** ACKs for different commands may arrive **out of order**. For example, if a `publish` command waits for a `persisted` ACK from a synchronous replication destination, a subsequent `subscribe` command's `processed` ACK may arrive before the `publish`'s `persisted` ACK.

### Bookmark Subscriptions and ACKs
For bookmark subscriptions, the `persisted` ACK is used by the AMPS client library to track the last-processed bookmark. The `HAClient` uses this to know where to resume on reconnect.

---

## 22. Automation — Actions

### What Are Actions?
Actions allow AMPS to perform tasks automatically in response to server events, schedules, or signals — without any external scheduler or external program.

Each action has:
- One or more `<On>` clauses (when to trigger)
- One or more `<Do>` clauses (what to do)
- Optional `<Stop>` conditions (stop processing if a condition is met)
- Context variables available as `{{VARIABLE_NAME}}`

### Trigger Types (`<On>`)

| Trigger | Description |
|---------|------------|
| `Schedule` | Cron-like: run at a fixed interval or specific time |
| `Startup` / `Shutdown` | When AMPS starts or stops |
| `Signal` | Linux OS signal (SIGUSR1, etc.) |
| `ClientConnect` / `ClientDisconnect` | When any client connects or disconnects |
| `ClientLogon` | When a client completes logon |
| `Subscribe` / `Unsubscribe` | When a client creates or removes a subscription |
| `MessagePublished` | When a message is published to a matching topic |
| `MessageDelivered` | When a message is delivered to a subscriber |
| `MessageAffinity` | When AMPS needs to affinitize a new message |
| `SOWMessageExpired` | When a SOW record expires |
| `SOWMessageDeleted` | When a SOW record is deleted |
| `OOFMessage` | When an OOF message is produced |
| `MessageConditionTimeout` | When a message condition timeout fires |
| `MessageStateChange` | When a message changes state in the queue |
| `IncomingReplication` / `OutgoingReplication` | Replication connection changes |
| `REST` | An HTTP request to the admin interface endpoint |
| `MinidumpCreated` | When AMPS creates a diagnostic minidump |
| `ClientOfflineMessageBuffering` | When a client is being offlined to disk |
| `FilesystemCapacity` | When disk usage crosses a threshold |
| `CustomEvent` | An event raised explicitly by another action |

### Action Types (`<Do>`)

| Action | Description |
|--------|------------|
| `RotateLog` | Rotate the error/event log file |
| `CompressFiles` | Compress files matching a pattern |
| `RemoveFiles` | Delete files matching a pattern |
| `ManageJournalFiles` | Archive/compress/delete old journal files |
| `TruncateStatistics` | Remove old entries from the stats DB |
| `CompactSOW` | Compact a SOW topic file |
| `QuerySOW` | Run a SOW query and make results available as action variables |
| `DeleteSOWMessages` | Delete SOW records matching a filter |
| `PublishMessage` | Publish a message to a topic |
| `ExecuteSystemCommand` | Run an external shell command |
| `EnableTransport` / `DisableTransport` | Enable or disable a named transport |
| `ManageReplicationAck` | Control replication acknowledgment behavior |
| `IncrementCounter` | Increment a named counter in the action context |
| `RaiseCustomEvent` | Trigger a custom event for other actions to respond to |
| `ExtractValues` | Extract field values from a message into action variables |
| `TranslateData` | Transform data within the action context |
| `ManageQueueTransfers` | Control queue message ownership transfers |
| `ManageSecurity` | Enable or disable security on a transport |
| `CreateMinidump` | Create a diagnostic minidump on demand |
| `ShutdownAMPS` | Gracefully shut down the AMPS server |

### Context Variables
```xml
{{AMPS_INSTANCE_NAME}}  <!-- Name of this AMPS instance -->
{{AMPS_DATETIME}}       <!-- Current datetime in ISO-8601 -->
{{AMPS_UNIX_TIMESTAMP}} <!-- Current time as Unix epoch -->
{{AMPS_BYTE_XX}}        <!-- Insert raw byte with hex value XX -->
```
Custom variables can be set by `ExtractValues` or `IncrementCounter` actions and referenced in subsequent `Do` clauses within the same action run.

### Conditional Stop
```xml
<Stop>
    <GreaterThan>90%</GreaterThan>  <!-- stop if disk > 90% full -->
    <Expression>/error_count > 5</Expression>
</Stop>
```

### Example: Weekly Journal Archive
```xml
<Action>
    <On><Schedule>0 2 * * 0</Schedule></On>  <!-- Every Sunday at 2am -->
    <Do>
        <ManageJournalFiles>
            <Topic>orders</Topic>
            <Archive>true</Archive>
            <MaxAge>7d</MaxAge>
        </ManageJournalFiles>
    </Do>
</Action>
```

### Example: Alert When Disk Is Almost Full
```xml
<Action>
    <On>
        <FilesystemCapacity>
            <Path>/data</Path>
            <GreaterThan>85%</GreaterThan>
        </FilesystemCapacity>
    </On>
    <Do>
        <PublishMessage>
            <Topic>alerts</Topic>
            <Message>{"alert":"disk_high","path":"/data","instance":"{{AMPS_INSTANCE_NAME}}"}</Message>
        </PublishMessage>
    </Do>
</Action>
```

---

## 23. Replication

### Replication Basics
AMPS replication copies messages from one AMPS instance (the **source**) to another (the **destination**). All replication flows through the **transaction log** — only topics that are recorded in a transaction log can be replicated.

**Key characteristics:**
- **Point-to-point** — each replication link is between exactly two instances.
- **Push model** — the source is configured and actively pushes to the destination.
- **Transaction-log based** — replication sends the persisted result of `publish`, `delta_publish`, and `sow_delete` commands (not the raw command). Delta publishes are replicated as the **fully merged message**.
- **Ordered** — messages are replicated in the exact order they appear in the source's transaction log.
- **Guaranteed delivery** — messages in the transaction log cannot be removed until all synchronous replication destinations have acknowledged them.
- **Auto-resync** — if a destination goes offline and comes back, it automatically catches up from the transaction log.

### Delivery Guarantees (Sync Type)

| Mode | Behavior | Latency Impact |
|------|---------|----------------|
| `async` | Source publishes immediately without waiting for destination ACK. Message is eventually replicated. | Lowest latency |
| `sync` | Source waits for destination to persist the message before sending `persisted` ACK to the publisher. | Higher latency; full durability |

When a `sync` destination is offline and the timeout is exceeded, AMPS can be configured to **downgrade** the link to `async` temporarily.

### PassThrough Replication
By default, an AMPS instance only replicates messages that were **directly published to it** by a client. To also replicate messages that arrived via replication (necessary for topologies with 3+ instances), configure passthrough:
```xml
<Replication>
    <Destination>
        <Name>amps-3</Name>
        <PassThrough>true</PassThrough>
        ...
    </Destination>
</Replication>
```

### Replicated Queues
Queues can be replicated across instances while preserving delivery guarantees.

**Message ownership in replicated queues:**
- For a standard `Queue`: the instance where the message was **first published** owns it.
- For a `GroupLocalQueue`: the instance specified as `InitialOwner` owns new messages.
- Only the owning instance delivers the message to its local consumers.
- A non-owning instance requests an **ownership transfer** from the owner only if:
  1. It has consumers with available backlog, AND
  2. Sufficient time has passed without the owner delivering it.
- AMPS replicates ACKs (sow_delete commands) and internal queue management commands to keep all instances synchronized.

**Queue types and replication:**

| Queue Type | Initial Owner | Replicable |
|-----------|--------------|------------|
| `Queue` | First publisher's instance | Yes |
| `GroupLocalQueue` | Configured `InitialOwner` | Yes |
| `LocalQueue` | Each instance independently | No (not replicated) |

### Replication Security
Both incoming and outgoing replication connections can use authentication and entitlement:
```xml
<!-- Incoming replication transport with auth -->
<Transport>
    <Name>amps-replication</Name>
    <Type>amps-replication</Type>
    <InetAddr>10005</InetAddr>
    <Authentication><Module>my-auth</Module></Authentication>
    <Entitlement><Module>my-entitlement</Module></Entitlement>
</Transport>

<!-- Outgoing replication with authenticator for credentials -->
<Replication>
    <Destination>
        <Transport>
            <InetAddr>amps-2.example.com:10005</InetAddr>
            <Authenticator><Module>my-kerberos-auth</Module></Authenticator>
        </Transport>
    </Destination>
</Replication>
```

Mutual TLS is supported for replication links.

### Common Replication Topologies

| Topology | Description |
|----------|------------|
| **Active/Standby pair** | Two instances; source pushes to standby. Client fails over on disconnect. |
| **Hub and spoke** | One central instance replicates to multiple regional instances. |
| **Hub and spoke + HA** | Hub + spoke + each spoke has its own standby. |
| **Two-way (bidirectional)** | Each instance pushes to the other. AMPS deduplicates via sequence tracking to prevent message loops. |
| **Regional distribution** | Traffic stays local; replication provides consistency across regions. |

### Configuration Example (Two-Instance Active/Standby)
On **amps-1** (active):
```xml
<Replication>
    <Destination>
        <Name>amps-2</Name>
        <SyncType>async</SyncType>
        <Topic>
            <MessageType>json</MessageType>
            <Name>orders</Name>
        </Topic>
        <Transport>
            <InetAddr>amps-2-server.example.com:10005</InetAddr>
            <Type>amps-replication</Type>
        </Transport>
    </Destination>
</Replication>
```

---

## 24. High Availability

### HA Pillars in AMPS
1. **Transaction Log** — persistent, replayable record. Clients can resume from their last ACK after restart.
2. **Replication** — live message copy on another instance. If the primary fails, clients connect to the standby.
3. **`HAClient` in SDK** — client library that automatically reconnects and resumes subscriptions from the last acknowledged bookmark.

### `HAClient` Behavior
- Maintains a connection to one (or a list of) AMPS instances.
- On disconnect, automatically reconnects to the next available server.
- On reconnect, resumes all active subscriptions from the `MOST_RECENT` bookmark (the last persisted ACK).
- No messages are lost; no duplicate messages are delivered (assuming at-most-once or `fully_durable` mode).
- Publishers automatically retransmit un-ACK'd messages after reconnect.

### `ServerChooser`
A pluggable component (provided in the SDK) that `HAClient` calls to determine which server to connect to. The default implementation round-robins through a list of servers. Custom implementations can use AMPS's monitoring interface to pick the healthiest instance.

### Slow Client Management (HA-Relevant)
AMPS protects itself from misbehaving or slow clients that would otherwise exhaust memory:

1. **Client Offlining** — when memory usage exceeds the limit, AMPS buffers the client's pending messages to disk (offlines the client). The client connection remains open and continues processing, but at a lower priority.
2. **Client Disconnection** — if disk usage also exceeds its limit, AMPS disconnects the client and clears its buffer. The client's `HAClient` reconnects and resumes from bookmark.

AMPS selects the client consuming the **most resources** for offlining/disconnection.

**Per-client protection** — additionally, individual clients that are unresponsive (not reading messages) can be detected and disconnected independently.

---

## 25. Security

### Three Layers of AMPS Security

| Layer | Purpose |
|-------|---------|
| **Authentication** | Assigns and verifies the identity of a connecting client |
| **Entitlement** | Controls what that client is permitted to read/write |
| **Outbound credentials** | Credentials AMPS itself uses for outgoing replication connections |

### Authentication
Authentication modules are pluggable. AMPS calls the module for every `logon` command. The module verifies the identity and returns the authenticated user name.

**Built-in modules:**
- **`Multimethod`** — tries multiple authentication methods (Kerberos first, then LDAP, etc.).
- **`RESTful`** — sends credentials to an external HTTP/HTTPS web service. The web service validates and returns the identity.
- **`OAuth`** — validates OAuth bearer tokens against an OAuth server.
- **`Command Execution`** — runs an external program to authenticate. The program receives credentials via stdin and writes the result to stdout.
- **Custom modules** — developed using the AMPS Server SDK.

### Entitlement
Entitlement modules are called for every AMPS operation — publish, subscribe, SOW query, etc. They decide whether the authenticated identity is permitted to perform that operation on that resource.

**Granularity levels:**
- **Topic level** — can the user publish or subscribe to this topic?
- **Message/record level** — content-based security: which specific records within a topic can the user see? (Implemented as an entitlement filter expression.)
- **Field level** — which fields within a matching record can the user see?

**Built-in entitlement modules:**
- **Simple Access Entitlements** — restricts access to specific topics for all users.
- **RESTful Entitlement** — calls an HTTP web service that returns a permissions document.

**Permissions document format (RESTful):**
```json
{
  "user": "john",
  "permissions": [
    { "topic": "orders", "read": true, "write": false,
      "filter": "/region == 'US'" }
  ]
}
```
This allows John to subscribe to orders but only see US region orders.

### Disabling Entitlements
Individual transports or the whole instance can temporarily disable entitlement enforcement — useful for fallback during entitlement module failures. Should not be used in normal production.

### TLS/SSL
- Configured per-transport using a `<TLS>` block with `Certificate` and `Key` paths.
- Mutual TLS (client certificate verification) is also supported.
- Replication connections can use TLS.
- HTTPS is supported for RESTful auth and OAuth token requests.

### Providing Outbound Identity for Replication
When AMPS connects to a downstream replication destination that requires authentication, an `Authenticator` module provides the credentials:
```xml
<Destination>
    <Transport>
        <Authenticator>
            <Module>kerberos-auth</Module>
        </Authenticator>
    </Transport>
</Destination>
```

---

## 26. Monitoring

### Two Monitoring Components

**1. REST/JSON Monitoring Interface**
A RESTful HTTP API that provides statistics and administrative actions. The URL structure is hierarchical:

```
http://host:8085/amps/          -- root
http://host:8085/amps/host/     -- OS-level stats
http://host:8085/amps/instance/ -- AMPS instance stats
http://host:8085/amps/administrator/ -- admin actions
```

**Response formats:**
- Default: HTML/JSON tree view
- Append `.json` to any path: returns raw JSON
- Append `.csv` to leaf nodes: returns CSV

**Navigating the stats tree:**
- `/amps/host/cpu` — host CPU utilization
- `/amps/host/memory` — host memory
- `/amps/host/disks` — disk I/O stats
- `/amps/host/network` — network interface stats
- `/amps/instance/clients` — connected clients and their stats
- `/amps/instance/queues` — queue depths, lease counts
- `/amps/instance/sow` — SOW topic stats (record count, memory usage)
- `/amps/instance/transaction_log` — journal file status
- `/amps/instance/replication` — replication lag and connection status
- `/amps/instance/subscriptions` — active subscriptions
- `/amps/instance/views` — view computation stats
- `/amps/instance/memory` — AMPS memory pool usage
- `/amps/instance/uptime` — how long the instance has been running
- `/amps/instance/version` — AMPS version string
- `/amps/administrator/authorization/...` — enable/disable transports, disconnect clients

**Admin actions via REST:**
- Disconnect a specific client
- Enable or disable a transport
- Upgrade or downgrade a replication link (async ↔ sync)
- Force a queue ownership transfer

**2. Galvanometer**
A browser-based monitoring UI that connects via WebSocket. Features:
- Graphical charts of key metrics over time
- Live subscription and SOW query console (enter filters and see results in a grid)
- Replication topology view showing message flow across instances
- Works with any transport that has `websocket` protocol enabled

### Configuring Monitoring
```xml
<Admin>
    <InetAddr>0.0.0.0:8085</InetAddr>
</Admin>
```

### Statistics Database
AMPS optionally persists monitoring statistics to a **SQLite file** (`stats.db`):
- Queryable using standard SQLite tools or the `amps_stats` utility
- Enables historical analysis and trending
- Can be truncated on a schedule using Actions to prevent unbounded growth

Key metrics to watch in production:
- Queue depth (messages waiting)
- SOW record count vs. memory
- Transaction log journal file age and disk usage
- Replication lag (messages behind)
- Client connection count
- Host CPU and memory utilization

---

## 27. Capacity Planning & OS Tuning

### Capacity Planning Areas

**Memory (RAM)**
- SOW topics store records verbatim in memory. Estimate: `average_message_size_bytes × max_records × safety_factor`
- Each SOW index (hash or memo) also uses memory.
- Slow client buffers, conflated topic state, and aggregated subscription state all use memory.
- Rule of thumb: provision 200% of your estimated peak memory while retaining 10-20% free RAM headroom.

**Storage (Disk)**
- Transaction log journals grow at the publish rate × average message size. Estimate based on retention period.
- Replication multiplies this by the number of downstream instances.
- SOW files are bounded by the number of distinct records × average message size.

**Network**
- Inbound: publish rate × message size
- Outbound: subscriber count × message rate × message size × fan-out factor
- Replication: added to outbound bandwidth

**CPU**
- Delta publish/subscribe uses more CPU (parse + merge + diff).
- Views and aggregated subscriptions use CPU proportional to update rate × complexity.
- Content filtering for large subscriber populations uses CPU.

### Physical vs. Virtual vs. Container
- **Highest performance:** dedicated physical hardware with a single AMPS instance.
- **Flexible deployment:** VMs or containers (some latency overhead, less predictable performance).
- For multi-tenant hosts or VMs: **disable AMPS-level NUMA tuning** in config.

### Linux OS Settings (Required for Production)

| Setting | Recommended Value | Why |
|---------|------------------|-----|
| `ulimit -n` (file descriptors) | 32768 or more | AMPS needs 2× (clients + SOW files + journals) |
| `ulimit -c` (core dump size) | 0 | Prevent giant core files; AMPS uses minidumps |
| `/proc/sys/fs/aio-max-nr` | 1048576 | AMPS needs 16384 + 8192 per SOW topic in AIO operations |
| `/proc/sys/fs/file-max` | 1048576 | System-wide file descriptor limit |
| `/proc/sys/vm/max_map_count` | 655360 or higher | Memory-mapped file regions |
| `/proc/sys/net/core/somaxconn` | 4096 | Socket listen queue depth |
| `/proc/sys/vm/swappiness` | 1 or 0 | Minimize swapping; AMPS latency is swap-sensitive |
| `/sys/kernel/mm/transparent_hugepage/enabled` | `madvise` | Let AMPS request huge pages explicitly; avoid system-wide overhead |

**Persistent configuration** — add settings to `/etc/sysctl.conf` (survives reboots):
```
fs.aio-max-nr = 1048576
fs.file-max = 1048576
vm.max_map_count = 655360
net.core.somaxconn = 4096
vm.swappiness = 1
```

**Transparent Huge Pages (immediate effect, until reboot):**
```bash
echo madvise > /sys/kernel/mm/transparent_hugepage/enabled
```

---

## 28. AMPS Commands Reference

### Commands Sent by Clients to AMPS

| Command | Purpose |
|---------|---------|
| `logon` | Identify the client. Required as the first command. Sets `client_name`. |
| `publish` | Publish a full message to a topic. |
| `delta_publish` | Publish only changed fields; AMPS merges into existing SOW record. |
| `subscribe` | Subscribe to live message stream. Supports regex topics, filters, bookmarks, conflation. |
| `unsubscribe` | Cancel one or more subscriptions by `sub_id`. |
| `sow` | Query the current state of a SOW topic (returns a snapshot). |
| `sow_and_subscribe` | Atomic SOW query followed by live subscription (no gap). |
| `delta_subscribe` | Subscribe, receiving only changed fields per update. |
| `sow_and_delta_subscribe` | Atomic SOW query followed by delta subscribe. |
| `sow_delete` | Delete records from a SOW topic. |
| `heartbeat` | Start or refresh a keep-alive heartbeat timer. |
| `flush` | Request an ACK when all previous commands have been processed. |

### Key Command Header Fields

| Field | Description |
|-------|------------|
| `cmd` | Command name (`publish`, `subscribe`, etc.) |
| `topic` | Topic name or regex pattern |
| `filter` | Content filter expression |
| `bookmark` | Starting point for bookmark subscription |
| `sub_id` | Subscription ID (client-assigned or AMPS-generated) |
| `cmd_id` | Command ID for correlating ACKs |
| `ack_type` | Which ACK types to request (`received`, `processed`, `completed`, `persisted`) |
| `opts` | Command-specific options (e.g., `oof`, `conflation=2s`, `live`, `fully_durable`) |
| `orderby` | Sort order for SOW results |
| `top_n` | Limit result count |
| `skip_n` | Skip N records (pagination) |
| `expiration` | Message TTL (for publish) |
| `sow_key` / `sow_keys` | SOW key(s) for delete-by-key |

### Subscribe Command Options (`opts` field)
| Option | Effect |
|--------|--------|
| `oof` | Request OOF messages when records leave the subscription window |
| `bookmark` | Include the bookmark in each delivered message |
| `conflation=Ns,conflation_key=/field` | Apply per-subscription conflation |
| `replace` | Replace an existing subscription (change filter without re-subscribing) |
| `pause` / `resume` | Pause or resume message delivery on a subscription |
| `live` | Deliver messages before persistence (lowest latency, reduced durability) |
| `fully_durable` | Only deliver after all sync replication destinations acknowledge |
| `send_oof` | Alias for `oof` |

### Server Responses

| Response | Description |
|----------|------------|
| `ack` | Acknowledgment for a command. Contains `status` (`success` or error), `reason`, and statistics. |
| `publish` | A message delivered to a subscriber from a subscription. |
| `sow` | A record returned by a SOW query. |
| `oof` | Out-of-focus notification — a record no longer matches the subscription. |
| `group_begin` | Marks the start of a grouped result set (e.g., from a SOW query). |
| `group_end` | Marks the end of a grouped result set. |

### Heartbeat Command
Heartbeating keeps the TCP connection alive and lets AMPS detect dead clients. The AMPS client libraries manage heartbeats automatically.

```
heartbeat
  opts: start,5    # start heartbeat timer with 5-second interval
```

AMPS will disconnect the client if it doesn't receive a heartbeat response within the interval.

### Flush Command
```
flush
```
AMPS returns a `completed` ACK when all previously submitted commands from this client have been fully processed. Useful before a client exits to confirm all publishes have been received.

---

## 29. Protocol Reference

### AMPS Protocol (Recommended)
The `amps` protocol uses a simplified JSON-like header format. It is the default and recommended protocol for all client development.

Key header field abbreviations (used in wire format):

| Full Name | Abbreviation |
|-----------|-------------|
| `cmd` | `c` |
| `topic` | `t` |
| `filter` | `f` |
| `bookmark` | `bm` |
| `opts` | `o` |
| `ack_type` | `a` |
| `sub_id` | `SubscriptionId` |
| `cmd_id` | `cid` |
| `expiration` | `e` |
| `sow_key` | `k` |
| `sow_keys` | `SowKeys` |
| `seq` | `s` |
| `timestamp` | `ts` |

WebSocket connections use WebSocket framing but the `amps` protocol for command headers.

### Legacy Protocols
FIX/NVFIX and XML header protocols are supported for backward compatibility but **will not receive new AMPS features**. For all new development, use the `amps` protocol.

---

## 30. Utilities

| Utility | Purpose |
|---------|---------|
| `spark` | Interactive CLI client. Supports `publish`, `subscribe`, `sow`, `sow_delete`, and more. Good for testing and exploration. |
| `amps_journal_dump` | Inspect the binary content of journal files. Shows headers, message data, and sequence numbers. |
| `amps-grep` | Search journal files or AMPS log files for specific topic messages, client names, or error codes. |
| `ampserr` | Look up the meaning of AMPS error codes. Run `ampserr --list` for all codes. |
| `amps_sow_dump` | Inspect the binary content of SOW files. |
| `amps_queues_dump` | Inspect the queue ACK state file (`queues.ack`). |
| `amps_clients_dump` | Inspect the client ACK state file (`clients.ack`). |
| `amps_file_identify` | Identify the type of any AMPS binary file (journal, SOW, stats DB, etc.). |
| `amps_perf_test` | Benchmark storage device sequential write performance. Run **before** production to verify disk is fast enough. |
| `amps_stats` | Query the SQLite statistics database. Supports standard SQL queries. |
| `amps_minidump` | Package and submit a diagnostic minidump to 60East support. |

### `spark` Examples
```bash
# Publish a JSON message
spark publish -server tcp://localhost:9007/amps/json \
  -topic orders \
  '{"orderId":"ORD-001","status":"NEW","qty":10}'

# Subscribe to a topic with a filter
spark subscribe -server tcp://localhost:9007/amps/json \
  -topic orders \
  -filter '/status == "ACTIVE"'

# SOW query
spark sow -server tcp://localhost:9007/amps/json \
  -topic orders \
  -filter '/qty > 100'

# Delete records from SOW
spark sow_delete -server tcp://localhost:9007/amps/json \
  -topic orders \
  -filter '/status == "CANCELLED"'
```

### `amps_journal_dump` Output Structure
The dump is split into three sections per journal:
1. **Header** — general metadata (journal version, topic hash, sequence range)
2. **Index** — topic index for fast seeking within the journal
3. **Records** — individual messages with their headers, bookmarks, and payload

---

## 31. Deployment Checklist

Use this checklist before going to production:

| Step | Task |
|------|------|
| ✅ | **Capacity planning** — estimate memory (SOW + buffers), disk (journals × retention), network (throughput × fan-out × replication factor). Allow 150-200% of expected peak. |
| ✅ | **OS tuning** — apply all settings in [Section 27](#27-capacity-planning--os-tuning). |
| ✅ | **NUMA configuration** — if using dedicated physical hardware, enable AMPS NUMA tuning. Disable for VMs and multi-tenant hosts. |
| ✅ | **Create journal directories** — ensure `journal/`, `sow/`, `logs/` directories exist before starting. |
| ✅ | **Run as a Linux service** — configure systemd or System V init. Capture stdout/stderr. |
| ✅ | **Maintenance plan** — set up Actions for journal rotation, SOW compaction, stats DB truncation. |
| ✅ | **Monitoring strategy** — deploy Galvanometer; set alert thresholds for queue depth, replication lag, disk usage, memory. |
| ✅ | **Patch and upgrade plan** — define process for applying AMPS updates with minimal downtime. |
| ✅ | **Support plan** — document how to capture logs, minidumps, and config for 60East support. |
| ✅ | **Capacity test** — run the system at 150-200% of expected peak load before go-live. |
| ✅ | **Unique client names** — ensure all application clients use unique, identifiable names. |
| ✅ | **Unique instance names** — ensure all AMPS instances in a replication topology have unique names. |

### AMPS Version Numbering
Format: `MAJOR.MINOR.FEATURE.TIMESTAMP.TAG`

| Component | Meaning |
|-----------|---------|
| `MAJOR` | Breaking changes (file formats, network protocol, config) |
| `MINOR` | New features, backward compatible |
| `FEATURE` | `0` = long-term stable release; higher = feature releases |
| `TIMESTAMP` | Internal build timestamp |
| `TAG` | Exact code identifier for the build |

---

## 32. Troubleshooting

### Proactive Steps (Before Issues Occur)
1. Log at `info` level or more verbose in production. Store logs on a device with sufficient capacity.
2. Ensure all client applications use **unique, traceable names** (e.g., `appname-hostname-pid`).
3. Enable the administrative monitoring interface — Galvanometer provides a snapshot of current state.
4. Ensure all AMPS instances in a replication topology have **unique names**.
5. Learn what **normal** looks like: publisher count, subscriber count, queue depth, CPU usage. Deviations from normal are the first sign of trouble.

### Diagnostic Utilities

| Problem | Tool |
|---------|------|
| Find events for a specific client | `amps-grep client_name amps.log` |
| Inspect journal content | `amps_journal_dump A.000000000.journal` |
| Find specific message in journal | `amps-grep -topic orders journals/` |
| Look up an error code | `ampserr 15-0008` |
| Check SOW file integrity | `amps_sow_dump sow/orders.sow` |
| Query historical statistics | `amps_stats stats.db "SELECT * FROM ..."` |

### Common Disconnection Reasons

| Reason | Likely Cause |
|--------|-------------|
| Heartbeat timeout | Network congestion, client deadlock, overloaded client |
| Transport disabled | Admin manually disabled the transport via console |
| Memory limit exceeded | Slow client consumed too much server memory |
| Authentication failure | Incorrect credentials or auth module error |

### Troubleshooting Regular Expression Topics
If regex subscriptions don't match expected topics, verify the regex syntax is PCRE2-compatible and test with `spark subscribe` before deploying.

### Reading Replication Log Messages
Replication log messages include the instance name and the source of the message. Look for messages indicating "downgrade" (falling back from sync to async) or "offline" events — these indicate connectivity problems between instances.

---

## 33. Developer SDKs & Flink Connector

### Client Libraries
Available for: **Java, C#/.NET, C++, Python, JavaScript, Go**.

All SDKs provide a consistent API surface:

| Feature | SDK Support |
|---------|------------|
| `HAClient` with auto-reconnect | All languages |
| `ServerChooser` for failover | All languages |
| Named convenience methods (`publish()`, `subscribe()`, `sow()`) | All languages |
| Low-level `Command` object interface | All languages |
| Bookmark tracking and resume | All languages |
| Queue-aware consumer loop with `message.ack()` | All languages |
| Heartbeat management (automatic) | All languages |
| Optional message compression | All languages |
| TLS/SSL connections | All languages |

### `HAClient` Pattern
```java
// Java example
HAClient client = new HAClient("my-app");
client.connect("tcp://amps1:9007/amps/json,tcp://amps2:9007/amps/json");
client.logon();

// Publish — retransmits on reconnect
client.publish("orders", jsonMessage);

// Subscribe with bookmark resume
MessageStream stream = client.subscribe(
    m -> processMessage(m),
    "orders",
    "/status == 'NEW'",
    Client.Bookmarks.MOST_RECENT
);

// Queue consumer — ack after processing
for (Message m : client.subscribe("orders-queue")) {
    processOrder(m);
    m.ack();   // releases the queue lease
}
```

### Apache Flink Connector
AMPS provides a connector for Apache Flink to use AMPS as a streaming source or sink.

**AMPS Source** (consume from AMPS into Flink):
- Supports checkpointing — on checkpoint, saves the current AMPS bookmark.
- On restore from checkpoint, resumes from the saved bookmark (exactly-once with Flink semantics).
- Configurable parallelism — each Flink source task maintains its own subscription.
- Bounded or unbounded mode.

**AMPS Sink** (write from Flink to AMPS):
- Publishes Flink records to AMPS topics.
- Supports checkpointing for at-least-once delivery guarantees.
- Configurable parallelism.

**Flink Source ordering:** Within each parallel Flink task, messages maintain AMPS order. Across parallel tasks, global ordering is not guaranteed (standard Flink behavior).

---

## 34. Scenario-to-Feature Reference

| Use Case | AMPS Feature(s) |
|----------|----------------|
| Simple low-latency pub/sub — no persistence | Pub/Sub (`publish` + `subscribe`) |
| Pub/sub with audit trail | Transaction Log + Bookmark Subscription |
| Current snapshot of all active records | SOW (`sow_and_subscribe`) |
| Aggregated/summary view of a topic | Views or Aggregated Subscriptions |
| Ensure each message is processed exactly once | Message Queue + Transaction Log |
| Distribute work across a pool of workers | Message Queue (proportional delivery) |
| Resume a subscription after crash without missing messages | Transaction Log + `MOST_RECENT` Bookmark |
| Replay messages for backtesting | Transaction Log + `EPOCH` Bookmark |
| Update only changed fields (bandwidth optimization) | Delta Publish + Delta Subscribe |
| Reduce update rate for rapidly-changing data | Conflated Topics / Conflated Subscriptions |
| Provide a UI with current + live updates (no gap) | SOW and Subscribe (`sow_and_subscribe`) |
| Notify subscribers when a record no longer matches | OOF Messages |
| Join data from two topics | Views with JOIN |
| Point-in-time historical value query | Historical SOW Query |
| Add computed fields to every message | Enrichment |
| Highly available messaging with failover | Transaction Log + Replication + `HAClient` |
| Transform or clean messages on publish | Preprocessing + Enrichment |
| Coordinate dependent message processing | Queue Barriers |
| Track what other clients are subscribing to | `/AMPS/ClientStatus` event topic |
| Route different content to different queues | Content-filtered queues |

---

## 35. Glossary

| Term | Definition |
|------|-----------|
| **SOW** | State of the World — AMPS's integrated current-value store (like a database table with a primary key). |
| **SOW Key** | The primary key for a SOW record — derived from message content fields, publisher-supplied, or custom-generated. |
| **SowKey** | The internal hash/value AMPS stores as the key for a SOW record. |
| **Transaction Log** | Ordered, durable, append-only record of all publish commands. Stored in journal files. |
| **Journal** | Physical file on disk storing the transaction log. Multiple journal files are created as the log grows. |
| **Bookmark** | Opaque, monotonically-increasing identifier assigned by AMPS to each transaction log entry. Used to resume subscriptions. |
| **Bookmark Subscription** | A subscription that starts replay from a specific bookmark in the transaction log. |
| **`MOST_RECENT`** | SDK-level bookmark meaning "resume from the last ACK'd bookmark for this subscription". |
| **`EPOCH`** | Bookmark value meaning "start from the very beginning of the transaction log". |
| **Lease** | Temporary exclusive hold on a queue message. Held by a consumer; expires if not ACK'd within `LeaseTimeout`. |
| **LeaseTimeout** | Configurable duration after which an un-ACK'd queue message is released back to the queue for re-delivery. |
| **OOF (Out-of-Focus)** | A message sent to a subscriber when a SOW record that previously matched its filter no longer matches. |
| **Conflation** | The process of merging multiple updates for the same record and delivering only the latest value at a configured interval. |
| **Conflated Topic** | A server-defined topic applying conflation for all subscribers. |
| **PassThrough Replication** | Configuration allowing an AMPS instance to re-replicate messages it received via replication. Required for 3+ instance topologies. |
| **Backlog** | Maximum number of un-ACK'd messages a queue consumer can have outstanding simultaneously. |
| **HAClient** | High-availability client — auto-reconnects, resumes bookmarks, and retransmits un-ACK'd publishes on reconnect. |
| **ServerChooser** | Component used by `HAClient` to determine which AMPS server to connect to during reconnect. |
| **Galvanometer** | Browser-based AMPS monitoring and admin UI. |
| **View** | A materialized aggregation over one or more SOW topics — updated automatically as underlying topics change. |
| **Aggregated Subscription** | On-demand aggregation computed at query time, without pre-defining a permanent view. |
| **Barrier** | A queue message that blocks delivery of subsequent messages until all previous messages are ACK'd. |
| **Delta Publish** | Publishing only the changed fields of a message; AMPS merges into the existing SOW record. |
| **Delta Subscribe** | Subscribing to receive only the changed fields of each updated SOW record. |
| **Enrichment** | Adding or transforming fields in a message after the SOW key is calculated, before storage and delivery. |
| **Preprocessing** | Transforming fields in a message before the SOW key is calculated. Used when the key depends on computed values. |
| **Hash Index** | Pre-defined in-memory index on a SOW topic for fast exact-match queries. |
| **Memo Index** | Automatically-created in-memory index used for range queries, regex, and comparisons. |
| **Client Offlining** | AMPS spills a slow client's pending messages to disk to relieve memory pressure. |
| **Minidump** | Compact diagnostic snapshot of AMPS internal state, produced on crash or on demand. Much faster to generate than a core dump. |
| **`ampServer`** | The AMPS server binary. |
| **`AMPSDIR`** | Conventional name for the AMPS installation directory. |
| **60East** | The company that develops and sells AMPS. |
