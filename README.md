# 🦫 BeaverMQ

A lightweight, ultra‑fast message broker written in **pure C**, inspired by
RabbitMQ. It speaks the **native AMQP 0‑9‑1 wire protocol**, so standard
clients — Python's `pika`, the Java `amqp-client`, PHP's `php-amqplib` — connect
and interoperate without knowing they are talking to BeaverMQ instead of
RabbitMQ. Under the hood: an asynchronous `libuv` network core, an in‑memory
routing engine, an embedded HTTP management API, and a modern Material‑Design
web dashboard.

- **Native AMQP 0‑9‑1** — byte‑for‑byte wire compatible: protocol handshake,
  the exact method argument layouts, field tables, and the multi‑frame content
  flow (method → content‑header → body). Works with off‑the‑shelf clients.
- **Multi‑threaded** — one `libuv` event loop per worker thread, each with a
  `SO_REUSEPORT` listener so the kernel load‑balances connections across CPU
  cores. Worker count is configurable or auto‑detected. Verified race‑free
  (ThreadSanitizer) and leak‑free (Valgrind).
- **Asynchronous & non‑blocking** — scales to thousands of concurrent
  connections per worker.
- **Exchanges** — Direct, Fanout, and Topic (`*` / `#` wildcards), plus the
  default exchange (route by queue name).
- **Push delivery** — consumers receive messages instantly via the event loop;
  competing consumers share a queue round‑robin.
- **Acknowledgements** — manual ack (including `multiple`), `basic.nack` /
  `basic.reject` with requeue, requeue‑on‑disconnect, or fire‑and‑forget
  (`no_ack`).
- **Flow control** — enforced `basic.qos` prefetch windows per consumer and
  negotiated heartbeats (dead connections are detected and reaped).
- **Management API + dashboard** — JSON REST API and a Vuetify SPA on port
  `15672`.
- **Memory‑safe** — every module is verified leak‑free and race‑free under
  Valgrind (`memcheck` + `helgrind`).

---

## Table of contents

1. [Architecture](#architecture)
2. [Build](#build)
3. [Running the broker](#running-the-broker)
4. [The web dashboard](#the-web-dashboard)
5. [HTTP management API](#http-management-api)
6. [Using the broker (publish / consume)](#using-the-broker)
7. [The AMQP 0-9-1 protocol](#the-amqp-0-9-1-protocol)
8. [Testing](#testing)
9. [Performance & tuning](#performance--tuning)
10. [Project layout](#project-layout)

---

## Architecture

```
                  kernel load-balances (SO_REUSEPORT) :5672
        ┌───────────────┬───────────────┬───────────────┐
   AMQP │  worker 0     │  worker 1     │  worker N-1    │  each = one OS thread
 clients│ (main+signals)│               │               │  + one libuv loop:
   ────▶│  net.c        │  net.c        │  net.c         │   net.c  (listener,
        │  protocol.c   │  protocol.c   │  protocol.c    │           connections)
        │  dispatch.c   │  dispatch.c   │  dispatch.c    │   protocol.c (parser)
        │               │               │               │   dispatch.c (delivery)
        └───────┬───────┴───────┬───────┴───────┬───────┘
                ▼               ▼               ▼
        ┌──────────────────────────────────────────────┐
        │ broker.c   SHARED: queues + exchanges +        │  rwlock (parallel
        │            bindings (hashmap/queue/exchange/   │  routing on reads,
        │            message). Cross-thread delivery is  │  declares on write)
        │            routed via per-queue waiter wakeups.│  + per-queue locks
        └──────────────────────────────────────────────┘
   Browser / curl ─▶ http.c  HTTP management API + SPA :15672
                     (DEDICATED thread — never starved by AMQP load)
                     logger.c  thread-safe leveled logging
```

Each connection lives entirely on one worker's loop, so per‑connection I/O and
protocol parsing stay lock‑free. The only shared mutable state is the broker
(guarded by its registry lock + per‑queue locks) and the aggregate stats (C11
atomics). When a publish on worker A targets a queue with a consumer on worker
B, the broker wakes worker B through a thread‑safe per‑queue **waiter** list
(`uv_async_send`), and B delivers on its own loop — `uv_write` is only ever
called from the loop that owns the socket.

---

## Build

### Dependencies (Debian 13 / Trixie)

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake libuv1-dev libjansson-dev valgrind
```

| Library          | Used for                                   |
|------------------|--------------------------------------------|
| `libuv1-dev`     | asynchronous event loop & networking       |
| `libjansson-dev` | JSON for the management API                |
| `valgrind`       | memory/leak/race verification (tests only) |

### Compile

```bash
make            # builds build/beavermq
make test       # builds + runs the C unit tests
make debug      # build with AddressSanitizer + UBSan
make clean      # remove build artifacts
```

The build uses a strict warning set (`-Wall -Wextra -Wpedantic -Wshadow
-Wstrict-prototypes -Wmissing-prototypes`) and compiles warning‑free.

---

## Running the broker

```bash
./build/beavermq            # AMQP on :5672, management API + UI on :15672
./build/beavermq 5673       # use a custom AMQP port
make run                    # convenience target

# You can launch it from anywhere - it finds the dashboard automatically:
cd build && ./beavermq      # also works (locates ../web next to the binary)
```

The broker locates the `web/` dashboard folder automatically (relative to the
binary), so it works regardless of the directory you start it from. On startup
it logs which folder it picked:

```
INFO  serving web dashboard from '/path/to/BeaverMQ/web'
```

You should see (with auto‑detected cores):

```
INFO  BeaverMQ starting up (4 worker threads, AMQP :5672, HTTP :15672)
INFO  BeaverMQ management API listening on http://127.0.0.1:15672
INFO  BeaverMQ ready on 127.0.0.1:5672 across 4 cores; Ctrl-C to stop
```

Stop it with **Ctrl‑C** (or `SIGTERM`): every worker drains its connections,
closes its handles, and the process exits cleanly with no leaks.

### Configuration

Settings are read from (lowest to highest precedence): **defaults → config
file → environment → CLI**. The config file (`beavermq.conf`) is auto‑located
in `$BEAVERMQ_CONF`, the current directory, or next to the binary.

```ini
# beavermq.conf
threads   = auto      # worker threads; "auto" = number of CPU cores, or e.g. 4
amqp_port = 5672
http_port = 15672
bind      = 127.0.0.1 # default: loopback only; 0.0.0.0 exposes it to the network
log_level = info      # debug | info | warn | error
# max_connections  = 4096   # concurrent AMQP connections (0 = unlimited)
# max_message_size = 16m    # max message body (bytes, k/m suffix; default 16m)
```

| Setting   | Config key  | Env var               | CLI                       |
|-----------|-------------|-----------------------|---------------------------|
| Threads   | `threads`   | `BEAVERMQ_THREADS`    | —                         |
| AMQP port | `amqp_port` | `BEAVERMQ_AMQP_PORT`  | `./beavermq 5673`         |
| HTTP port | `http_port` | `BEAVERMQ_HTTP_PORT`  | —                         |
| Bind addr | `bind`      | `BEAVERMQ_BIND`       | —                         |
| Log level | `log_level` | `BEAVERMQ_LOG_LEVEL`  | —                         |
| Web root  | `web_root`  | `BEAVERMQ_WEB_ROOT`   | —                         |
| Max conns | `max_connections` | `BEAVERMQ_MAX_CONNECTIONS` | —              |
| Max msg   | `max_message_size` | `BEAVERMQ_MAX_MESSAGE_SIZE` | —            |

```bash
BEAVERMQ_THREADS=2 ./build/beavermq        # force 2 workers
./build/beavermq /etc/beavermq.conf        # explicit config file
```

The `web/` dashboard folder is auto‑located (`$BEAVERMQ_WEB_ROOT`, `./web`,
next to the binary, then `../web`), so the broker runs from any directory. If
it isn't found, the JSON API still works — only the HTML UI is unavailable.

### Clustering (3‑node Raft)

Clustering is **opt‑in** and off by default. A cluster is a fixed set of nodes
(typically 3) that elect a leader via Raft and replicate **durable** topology
across the cluster. Non‑durable queues/exchanges stay node‑local and fast, so
the message hot path is untouched unless you mark a queue `durable`.

Each node runs the broker exactly as before, plus three config keys:

| Setting        | Config key      | Env var                   | Meaning                                             |
|----------------|-----------------|---------------------------|-----------------------------------------------------|
| Enable         | `cluster`       | `BEAVERMQ_CLUSTER`        | `on` / `off`                                         |
| This node's id | `node_id`       | `BEAVERMQ_NODE_ID`        | 0‑based index of **this** node into `cluster_nodes` |
| Members        | `cluster_nodes` | `BEAVERMQ_CLUSTER_NODES`  | comma list of every node's `ip:port` **mesh** addr  |
| Election timeout | `election_timeout_ms` | —                   | Raft election floor in ms (window is `[t, 2t]`); empty = **1000** |
| Heartbeat      | `heartbeat_ms`  | —                         | leader heartbeat interval in ms; empty = **200**    |

**Election timing.** The defaults (1000 ms election / 200 ms heartbeat) are tuned
for a real LAN under load: the single cluster loop can block for tens of ms while
building/CRC'ing large replication batches or applying a big committed batch, and
the classic 150–300 ms Raft figures mistake such a hiccup for a dead leader —
causing an *election storm* (term climbs continuously, no stable leader, and
writes made during leaderless windows are lost). 1000/200 tolerates 5–10 missed
heartbeats. Raise `election_timeout_ms` further only on a high‑latency/overloaded
network; keep `heartbeat_ms` roughly `election_timeout_ms / 5` or smaller.

`cluster_nodes` is the **same on every node** (same order); only `node_id`
differs — it tells a node which entry in the list is itself. The `ip:port` is
the internal mesh endpoint (default port **6672**, distinct from AMQP 5672 /
HTTP 15672); its port also sets what this node listens on. The mesh binds to
`bind` (loopback by default - a real cluster must set e.g. `bind = 0.0.0.0`).

**Example: 3 VMs at 10.0.0.10 / .11 / .12** — identical `beavermq.conf` except
`node_id`:

```ini
# ── VM A (10.0.0.10) ──            # ── VM B (10.0.0.11) ──            # ── VM C (10.0.0.12) ──
cluster       = on                  cluster       = on                  cluster       = on
node_id       = 0                   node_id       = 1                   node_id       = 2
cluster_nodes = 10.0.0.10:6672, 10.0.0.11:6672, 10.0.0.12:6672   # ← same line on all three
```

`amqp_port` / `http_port` can stay at their defaults on every VM (they are
different hosts). Open the mesh port (**6672/tcp**) between the VMs in your
firewall / security group. Restart each broker after editing the config.

**Persistence (survive restart / power loss).** Set `data_dir` to a writable
directory and the node fsyncs its replicated log there (`cluster-<id>.log` +
`.meta` + `.snap`), replaying it on startup. Without `data_dir`, the cluster
still replicates in memory (survives a node going down while a majority stays
up) but a full restart starts empty.

The replicated log is **compacted** automatically: once a prefix of entries has
been committed, applied, replicated to every node, and (for messages) consumed
everywhere, it is dropped — the in‑RAM entries are freed, the on‑disk prefix is
hole‑punched back to the filesystem, and the dropped topology is preserved in a
small snapshot (`cluster-<id>.snap`). So disk and memory track the **live**
backlog, not the lifetime message count, and recovery loads the snapshot plus
the surviving tail instead of replaying all of history. A queue with an
un‑consumed backlog (no/slow consumer) holds its messages until they are
consumed — that is the floor compaction will not cross.

```ini
data_dir = /var/lib/beavermq        # BEAVERMQ_DATA_DIR; "" (default) = in-memory only
```

What replicates, and to where:

| Publish / declare | Replicated? | Survives restart (with `data_dir`)? |
|-------------------|-------------|-------------------------------------|
| `durable` queue / exchange / bind        | yes, to all nodes | yes |
| **persistent** message (`delivery_mode=2`) | yes, to all nodes | yes |
| transient queue / non‑persistent message | no — node‑local & fast | no |

You can publish to **any** node: a follower forwards the write to the leader
internally, which replicates it to everyone — so clients need no
leader‑awareness.

Verify the cluster formed — the dashboard gains a **Cluster** tab, or:

```bash
curl http://10.0.0.10:15672/api/cluster   # role, term, leader, members[]
```

The logs show `cluster: node N up …` and `peer M link UP` (a healthy 3‑node
mesh logs 6 `link UP` events total). Kill a node and the survivors elect a new
leader; bring it back and it rejoins and catches up automatically.

> **Scope today:** leader election + failover, durable‑topology and
> **persistent‑message** replication (publish to any node), on‑disk persistence
> with crash recovery, and rejoin/catch‑up — all implemented and tested.
> *Not yet:* coordinated exactly‑once delivery across replicas (a message
> replicated to 3 nodes is consumed independently per node), log compaction, and
> publisher‑confirm‑after‑commit.

### Supervisor mode ("let it crash")

By default a crash (`SIGSEGV`, `SIGABRT`, an unhandled exception) in any part
of the broker takes down the whole process. Supervisor mode wraps the broker
in a small, separate outer process that does nothing but watch it, and
respawns it immediately on a crash or a frozen event loop — no operator
intervention needed:

```bash
./build/beavermq --supervisor              # or: BEAVERMQ_SUPERVISOR=on ./build/beavermq
./build/beavermq --supervisor beavermq.conf
```

The supervisor never runs the broker itself — it `fork()`s and `exec()`s a
fresh copy of the same binary as the whole worker process (everything above,
completely unmodified), and:

- **Restarts on crash**, with a rate limit (default: give up after 5 restarts
  within 10 s, so a deterministic startup bug doesn't spin forever — the
  supervisor then exits with a non‑zero code instead of looping silently).
- **Detects a frozen (not crashed) event loop** via a heartbeat the worker
  writes every couple of seconds; a missed heartbeat is treated the same as
  a crash.
- **Forwards `SIGTERM`/`SIGINT`** to the worker and waits (default 5 s) for a
  graceful shutdown before falling back to `SIGKILL`.
- Writes `supervisor.pid` and `worker.pid` under `data_dir` (see below), and
  logs every spawn/crash/respawn/shutdown through the same logger as the
  broker itself.

| Setting                | Env var                                   | Default |
|-------------------------|--------------------------------------------|---------|
| Max restarts            | `BEAVERMQ_SUPERVISOR_MAX_RESTARTS`         | 5       |
| Restart window          | `BEAVERMQ_SUPERVISOR_RESTART_WINDOW_MS`    | 10000   |
| Graceful shutdown timeout | `BEAVERMQ_SUPERVISOR_SHUTDOWN_TIMEOUT_MS` | 5000    |
| Heartbeat interval       | `BEAVERMQ_SUPERVISOR_HEARTBEAT_MS`         | 2000    |
| Heartbeat timeout        | `BEAVERMQ_SUPERVISOR_HEARTBEAT_TIMEOUT_MS` | 6000    |
| Worker processes         | `BEAVERMQ_SUPERVISOR_WORKERS`              | 1       |

> **`BEAVERMQ_SUPERVISOR_WORKERS` stays at 1 unless `cluster = on`.** Each
> worker process is a completely independent broker with its own in‑memory
> queues/exchanges — it is *not* the same thing as the worker **threads**
> inside one process (those already share state safely). Running more than
> one worker **process** without clustering would silently split your queue
> namespace across uncoordinated processes behind the same port; the
> supervisor refuses to start in that configuration.

Set `data_dir` (same config key the cluster's WAL uses) so `supervisor.pid`
and `worker.pid` land somewhere predictable — useful for `kill -TERM $(cat
data_dir/supervisor.pid)` style operator scripts, and for
`tests/test_supervisor.sh`, which drives exactly that scenario end‑to‑end
(spawn, `SIGSEGV` the worker, verify the respawn, verify graceful shutdown).

---

## The web dashboard

Open **<http://localhost:15672/>** in a browser. The dashboard (Vue 3 +
Vuetify 3 + Chart.js, loaded from CDN) auto‑refreshes every 2 seconds and shows:

- **Stat cards** — Messages Ready, Publish Rate, Deliver Rate, Connections,
  Queues, Consumers, Workers, Uptime.
- **Live throughput chart** — published/s and delivered/s over time, plus
  net bytes in/out and lifetime totals.
- **Queues table with drill‑down** — ready depth, consumer count, in/out rates;
  click a row to expand a depth sparkline and totals.
- **Exchanges table** — name, type (direct/fanout/topic), binding count.
- **Connections table** — id, peer, protocol state, bytes in/out (aggregated
  across all worker threads).

> The page itself is served by the broker; the Vuetify/Vue assets load from a
> public CDN, so the browser needs internet access to render the styled UI.
> The JSON API below works with no external dependencies.

---

## HTTP management API

A small JSON REST API on port `15672`. All responses are
`Content-Type: application/json` with permissive CORS.

| Endpoint            | Description                                        |
|---------------------|----------------------------------------------------|
| `GET /api/overview` | broker info, workers, uptime, totals, publish count, bytes in/out |
| `GET /api/queues`   | every queue: depth, consumers, enqueued/dequeued, durable |
| `GET /api/exchanges`| every exchange: name, type, binding count          |
| `GET /api/connections` | every AMQP connection: peer, state, byte counts |
| `GET /api/cluster`  | Raft role, term, leader, members, replication state |
| `GET /api/vhosts`   | virtual hosts |
| `GET /api/users`    | users + tags |
| `GET /api/permissions` | per‑(user,vhost) configure/write/read patterns |
| `POST /api/vhosts`  | `{"name":"prod"}` — create a vhost |
| `POST /api/users`   | `{"name":"alice","password":"…","tags":"administrator"}` — create/update a user |
| `POST /api/permissions` | `{"user":"alice","vhost":"/","configure":".*","write":".*","read":".*"}` |
| `DELETE /api/vhosts/<name>` · `/api/users/<name>` · `/api/permissions/<user>/<vhost>` | remove |
| `GET /`             | the web dashboard (`index.html`)                   |

### Access control (vhosts, users, permissions)

Authentication and authorization are **replicated through Raft**, so the user /
vhost / permission table is identical on every node and survives restart +
compaction.

**First boot.** There is **no default user** (a `guest` default would silently
accept every stock AMQP client). A fresh broker refuses every AMQP login and
its management API/dashboard are open only long enough to create the first
administrator:

```bash
build/beavermq add-user admin 's3cret'          # first admin (bootstrap window)
build/beavermq add-user bob 'pw' "" -u admin:s3cret   # later users need -u
```

`add-user` posts to the local management API (`-H host -p port` to aim
elsewhere; in a cluster any live node works — the change replicates). It also
grants the new user full permissions on vhost `/`. The moment the first user
exists, **everything locks**: AMQP requires valid credentials, and the entire
management API + dashboard require HTTP **Basic auth** (the browser prompts;
mutations additionally require the `administrator` tag).

- **Auth** — SASL `PLAIN`; passwords are stored as a salted SHA‑256 hash. A bad
  login is refused with `403`.
- **Vhosts** — object names are **namespaced per vhost**: queue `orders` in `/`
  and in `prod` are different queues, and each vhost has its own `amq.*`
  exchanges. `Connection.Open` is rejected (`530`) unless the vhost exists and
  the user has a permission entry for it.
- **Permissions** — RabbitMQ’s model: three POSIX‑regex patterns per
  `(user, vhost)` — *configure* (declare), *write* (publish / bind source),
  *read* (consume·get / bind dest). A denied op raises a `403` channel
  exception. An empty pattern denies; `.*` allows all.
- **GUI** — the dashboard’s **Access control** page manages vhosts, users and
  permissions (admin only).

> PLAIN and HTTP Basic send credentials in clear text — run on a trusted
> network (TLS is not yet implemented).

```bash
curl -s http://localhost:15672/api/overview | jq
```

```json
{
  "broker": "BeaverMQ",
  "version": "1.0",
  "uptime_seconds": 42,
  "object_totals":  { "queues": 2, "exchanges": 1, "connections": 2 },
  "queue_totals":   { "messages_ready": 3, "total_enqueued": 3, "total_dequeued": 0 },
  "message_stats":  { "publish": 3 },
  "network":        { "connections_total": 5, "bytes_received": 1024 }
}
```

---

## Using the broker

BeaverMQ speaks standard AMQP 0‑9‑1, so you use it with any AMQP client.
Install `pika`:

```bash
pip install pika
```

### Publish

```python
import pika

conn = pika.BlockingConnection(pika.ConnectionParameters("localhost", 5672))
ch = conn.channel()

# Direct exchange example
ch.exchange_declare(exchange="orders", exchange_type="direct")
ch.queue_declare(queue="orders.new")
ch.queue_bind(queue="orders.new", exchange="orders", routing_key="new")
ch.basic_publish(exchange="orders", routing_key="new", body=b"order #1001")

# Or use the default exchange to publish straight to a queue by name:
ch.queue_declare(queue="logs")
ch.basic_publish(exchange="", routing_key="logs", body=b"hello",
                 properties=pika.BasicProperties(content_type="text/plain"))

conn.close()
```

### Consume (push delivery + ack)

```python
import pika

conn = pika.BlockingConnection(pika.ConnectionParameters("localhost", 5672))
ch = conn.channel()
ch.queue_declare(queue="logs")

def on_message(channel, method, properties, body):
    print("got:", body)
    channel.basic_ack(method.delivery_tag)        # omit if auto_ack=True

ch.basic_consume(queue="logs", on_message_callback=on_message)
ch.start_consuming()
```

- **Round‑robin**: multiple consumers on the same queue share its messages.
- **Requeue safety**: with manual ack, messages delivered but not yet acked are
  **requeued** if the consumer disconnects, so nothing is lost.
- **Properties**: AMQP content properties (content‑type, delivery‑mode, headers,
  message‑id, …) are preserved end‑to‑end.

### Exchange types

| Type     | Routing behaviour                                                |
|----------|-----------------------------------------------------------------|
| `direct` | deliver to queues whose binding key **equals** the routing key   |
| `fanout` | deliver to **all** bound queues (routing key ignored)            |
| `topic`  | pattern match: `*` = one word, `#` = zero+ words (dot‑separated) |
| `""`     | **default exchange** — deliver to the queue named by the routing key |

---

## The AMQP 0-9-1 protocol

BeaverMQ implements the AMQP 0‑9‑1 wire protocol. A connection begins with the
8‑byte protocol header `AMQP\0\0\x09\x01`, after which both sides exchange
**frames**:

```
+--------+-----------+------------------+----------------+--------+
| type   | channel   | payload_len      | payload ...    | 0xCE   |
| u8     | u16 (BE)  | u32 (BE)         | payload_len B  | u8     |
+--------+-----------+------------------+----------------+--------+
   type: 1=method  2=content-header  3=content-body  8=heartbeat
```

A method‑frame payload is `class_id(u16) | method_id(u16) | arguments`. A
published message spans **three** frames: a `Basic.Publish` method frame, a
content‑header frame (body size + properties), and one or more content‑body
frames — exactly as RabbitMQ does. The connection progresses through a state
machine:

```
CONNECTED ──AMQP header──▶ HANDSHAKE ──Connection.Open──▶ (ready)
                              │
                              ├── Channel.Open ──▶ CHANNEL_OPEN
                              │
                              └── Queue/Basic op ──▶ ACTIVE
```

Implemented classes/methods (official AMQP 0‑9‑1 ids):

| Class (id)       | Methods                                                   |
|------------------|-----------------------------------------------------------|
| Connection (10)  | Start/StartOk, Tune/TuneOk, Open/OpenOk, Close/CloseOk     |
| Channel (20)     | Open/OpenOk, Close/CloseOk                                 |
| Exchange (40)    | Declare/DeclareOk                                         |
| Queue (50)       | Declare/DeclareOk, Bind/BindOk                             |
| Basic (60)       | Qos/QosOk, Consume/ConsumeOk, Cancel/CancelOk, Publish,    |
|                  | Deliver, Get/GetOk/GetEmpty, Ack                          |

The handshake authenticates with SASL `PLAIN` (accepted as‑is — no user store).
Field tables (client capabilities, method `arguments`) are parsed safely.
Layouts live in [`include/protocol.h`](include/protocol.h) /
[`src/protocol.c`](src/protocol.c).

> **Compatibility:** verified against the official `pika` client (and a
> dependency‑free reference client in
> [`tests/amqp_raw_client.py`](tests/amqp_raw_client.py)). Standard AMQP 0‑9‑1
> clients connect, declare, publish and consume with no special configuration.

---

## Testing

### C unit tests

```bash
make test
```

- `test_frame`   — framing/codec round‑trips, partial frames, bounds checks
- `test_logger`  — leveled logging, concurrency (no torn lines)
- `test_routing` — message refcounting, FIFO ring buffer, topic matching,
  direct/fanout routing, and a 100k‑message 4×4 producer/consumer stress test

### Python integration tests

Start the broker, then (from the `tests/` directory):

```bash
# Real AMQP client (the compatibility proof):
pip install pika
python3 test_amqp_pika.py        # pika: declare/publish/get/consume/ack, props

# Dependency-free AMQP reference client (no pip needed):
python3 test_amqp_raw.py         # handshake, content frames, all exchange types,
                                 # round-robin, requeue, 200 KB multi-frame body

python3 test_management.py       # the JSON API reflects live state
python3 test_dashboard.py        # static dashboard is served correctly
python3 stress_throughput.py     # fast producer + consumer (perf-test style)
python3 stress_connections.py --count 5000   # connection concurrency stress
```

### Supervisor test

```bash
make && bash tests/test_supervisor.sh
```

Starts the broker under `--supervisor`, `SIGSEGV`s the worker, and verifies
it is respawned (new pid, `/api/healthz` responding again) while the
supervisor process itself survives — then verifies a clean exit on
`SIGTERM`. Not wired into `make test` (that target runs compiled `tests/*.c`
binaries only); run it as a separate step.

### Memory & race verification

The multi‑threaded build is checked race‑free with **ThreadSanitizer** (which
understands C11 atomics + pthreads) and leak‑free with **Valgrind**:

```bash
# Zero data races: run under ThreadSanitizer, then drive it with the tests.
make tsan && ./build/beavermq        # 0 data races, 0 warnings

# Zero leaks/errors under memcheck (rebuild normal first):
make && valgrind --leak-check=full --error-exitcode=99 ./build/beavermq
# (drive with the tests above, then Ctrl-C)
```

---

## Performance & tuning

BeaverMQ sustains **>130k msg/s** on a fanless Intel N95 (4 cores) with a single
producer and two consumers. Raw throughput is rarely the problem; the **tail
latency (p95/p99/max)** is what jumps around under load. Most of that jitter is
*scheduling*, not the broker — but a few knobs help a lot.

### What the broker already does

- **`TCP_NODELAY`** on every accepted socket (Nagle off) — low per-message latency.
- **One `write()` per delivery**: the `Basic.Deliver` method frame, content
  header and body frame go out in a single syscall.
- **Few copies on the data path** — important once messages get large (multi-kB):
  - *Publish:* a body that arrives in one frame (the usual case) is turned into a
    message with a **single** copy, straight from the read buffer — the old
    intermediate accumulation buffer is skipped.
  - *Deliver:* the body is sent by **scatter-gather (`writev`)** — only the small
    frame headers are built into a buffer; the body bytes are streamed from the
    message itself (a reference is held until the write completes) instead of
    being copied into the send buffer. Bodies are shared by reference across
    queues (fanout), never duplicated.
- **One allocation per message**: the message struct + exchange + routing key +
  body + properties live in a single block, and the per-connection read buffer is
  reused across reads — together these slash the malloc/free churn that shows up
  as latency spikes.
- **glibc allocator pinned** (`M_TRIM_THRESHOLD = -1`, `M_MMAP_MAX = 0`) so freed
  memory is reused instead of being returned to the kernel on every load dip
  (avoids page-fault/`munmap` storms). Trade-off: slightly higher steady RSS.
- **Per-worker event loops + `SO_REUSEPORT`**: connections are spread across
  cores by the kernel; the management/dashboard server runs on its **own** thread
  so metrics scraping never steals time from the AMQP loops.

### Right-size the worker count

This is the single biggest tail-latency lever when you co-locate a load
generator (e.g. `perf-test`) on the same box. On a 4-core N95, `threads = auto`
starts **4** broker workers; add a producer + 2 consumer threads (and the OS) and
you oversubscribe all 4 cores, so the scheduler preempts the broker mid-delivery
— that is exactly the 20–60 ms `max` spikes you see.

```ini
# beavermq.conf — leave 1–2 cores for clients/OS when benchmarking locally
threads = 2
```

Run the benchmark from **another machine** if you can; then `threads = auto`
(all 4 cores) is the right choice and the tail tightens considerably.

Optionally pin the broker to specific cores so it isn't migrated:

```bash
# give the broker cores 0–1, run perf-test on 2–3
taskset -c 0,1 ./build/beavermq
chrt -r 1 taskset -c 0,1 ./build/beavermq   # or with SCHED_RR priority (needs root)
```

### Kernel / sysctl tuning

These reduce queueing and syscall jitter at high connection/packet rates. Apply
live with `sudo sysctl -w <key>=<value>`, or persist them in
`/etc/sysctl.d/99-beavermq.conf` and run `sudo sysctl --system`:

```ini
# /etc/sysctl.d/99-beavermq.conf

# Larger socket buffers — fewer stalls when a burst arrives/leaves.
net.core.rmem_max            = 16777216
net.core.wmem_max            = 16777216
net.ipv4.tcp_rmem            = 4096 131072 16777216
net.ipv4.tcp_wmem            = 4096 131072 16777216

# Bigger accept/backlog + softirq budget for many short-lived connections.
net.core.somaxconn           = 4096
net.core.netdev_max_backlog  = 16384
net.ipv4.tcp_max_syn_backlog = 8192

# Recycle TIME_WAIT sockets faster when churning many connections (loopback/bench).
net.ipv4.tcp_tw_reuse        = 1
net.ipv4.ip_local_port_range = 1024 65535

# Latency over throughput: low-latency TCP, disable slow-start-after-idle.
net.ipv4.tcp_low_latency        = 1
net.ipv4.tcp_slow_start_after_idle = 0

# Pumping >100k small msgs/s over loopback can hit conntrack limits if a
# firewall/NAT is loaded — raise it or skip conntrack for loopback.
net.netfilter.nf_conntrack_max  = 262144
```

For benchmarking, also raise the open-file limit (the broker already bumps its
own soft limit to the hard cap, but the *client* needs headroom too):

```bash
ulimit -n 1048576
```

Host-level extras that flatten the tail on a small box:

- **CPU governor → performance** (stops the N95 down-clocking between bursts):
  `echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor`
- **Disable transparent huge pages** if you see periodic stalls:
  `echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/enabled`

### Large messages (multi-kB bodies)

With small messages the broker is logic/syscall bound; with large bodies it
becomes **memory-bandwidth and I/O bound**. At, say, 4 kB × ~90k msg/s the box
moves ~370 MB/s in **and** ~370 MB/s out — and on loopback the producer, broker
and consumers all share the same memory/network stack. On a single-channel
DDR4 mini-PC (e.g. the N95) that bandwidth, not AMQP logic, is the ceiling, so
median latency rises with size. The broker minimises its share of the copying
(see above), but the remaining wins are operational:

- **Run producers/consumers on another machine** so loopback isn't doubling the
  memory traffic, and use a real NIC's offloads.
- **Raise consumer prefetch** so consumers pull in batches and the broker isn't
  gated on per-message acks, e.g. `perf-test --qos 100` (a.k.a. prefetch).
- **Bigger socket buffers** (the `sysctl` block above) absorb bursts so a briefly
  slow consumer doesn't immediately backpressure the broker.

> Measure one change at a time. On a saturated 4-core box the worker-count and
> CPU-governor changes dominate; the sysctl buffers matter most once you scale
> connections, move clients onto the network, or push large messages.

---

## Project layout

```
BeaverMQ/
├── Makefile              # build (auto-discovers src/*.c), test, debug, tsan
├── beavermq.conf         # sample config (threads/ports/log level)
├── include/              # public headers (one per module)
│   ├── config.h          #   config file + env + core auto-detect
│   ├── logger.h          #   thread-safe leveled logging
│   ├── net.h             #   per-worker libuv TCP server + connections
│   ├── frame.h           #   AMQP 0-9-1 framing + field codecs
│   ├── protocol.h        #   AMQP method constants + state machine
│   ├── hashmap.h         #   string-keyed hash map (registry backbone)
│   ├── message.h         #   reference-counted message
│   ├── queue.h           #   thread-safe ring-buffer FIFO + waiters
│   ├── exchange.h        #   direct/fanout/topic routing + bindings
│   ├── broker.h          #   shared, thread-safe registry + routing core
│   ├── dispatch.h        #   per-worker consumer push dispatcher
│   └── http.h            #   embedded HTTP management server
├── src/                  # implementations (main.c = worker orchestration)
├── web/                  # the Material Design dashboard (index.html, app.js)
└── tests/                # C unit tests + Python integration tests + clients
```

---

## License

Provided as‑is for educational and experimental use.
```
🦫  Built in C with libuv, jansson, and Vuetify.
```
