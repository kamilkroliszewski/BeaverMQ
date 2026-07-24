/*
 * cluster.h - 3-node Raft consensus + log replication layer for BeaverMQ.
 *
 * CONTROL PLANE vs DATA PLANE
 * ---------------------------
 * BeaverMQ's data plane is N worker loops sharing a thread-safe broker. The
 * cluster *control plane* (who is leader, what is committed) is a separate,
 * single-writer concern, so it runs on its OWN dedicated loop/thread - exactly
 * like the HTTP management server (see main.c). The Raft state (current term,
 * vote, log) is touched only by that one loop, so the consensus core needs NO
 * locks, the same way a connection that lives on one worker loop needs none.
 *
 *   control plane (this module, 1 loop)      data plane (N worker loops)
 *   ----------------------------------       ---------------------------
 *   Raft: election / term / log / commit     AMQP clients, broker, dispatch
 *   STRICTLY deterministic: no randomness     MAY be probabilistic (routing,
 *   in WHO leads or WHAT commits              backpressure load-balancing)
 *
 * Cross-plane traffic uses the primitives the rest of BeaverMQ already uses:
 *   - a worker proposes a replicated op  -> uv_async wakes the cluster loop;
 *   - once an op COMMITS (majority-replicated) the cluster loop applies it to
 *     the shared broker (which is already thread-safe).
 *
 * WHY RAFT, NOT THE "EIGENVECTOR" SKETCH
 * --------------------------------------
 * Leader election must be an *agreement* protocol (quorum + monotonic term),
 * not a locally-computed centrality metric. Two nodes with asymmetric views of
 * a connection matrix would each elect themselves and double-commit -> split
 * brain and data loss. Raft has a machine-checked safety proof; for N=3 it is a
 * couple of RPCs and two timers - no matrix math, no floating point.
 *
 * Election-storm damping (the legitimate goal of "don't react to transient
 * network spikes") is handled the proven way: randomized election timeouts +
 * PreVote (CL_FRAME_PREVOTE), so a flapping link cannot bump the term and
 * disrupt a healthy leader.
 *
 * MESH
 * ----
 * A fully-connected N-node mesh. Each node listens on the cluster port and
 * dials persistent uv_tcp_t pipes to every other node, auto-reconnecting with
 * capped exponential backoff. Internal frames use a fixed 16-byte, naturally
 * 8-byte-aligned, big-endian header with a CRC32 over the payload. The 16-byte
 * header is packed once per frame; "zero-copy" applies to the *payload* (log
 * entries / message bodies are referenced, not duplicated, where possible).
 */
#ifndef BEAVERMQ_CLUSTER_H
#define BEAVERMQ_CLUSTER_H

#include <uv.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/socket.h> /* struct sockaddr_storage */

#ifdef __cplusplus
extern "C" {
#endif

/* Central broker registry, owned by broker.c (opaque here). */
typedef struct beaver_broker beaver_broker_t;
struct authstore;
/* A queued replication proposal (full type in cluster.c). */
struct cl_proposal;

#define CLUSTER_MAX_NODES   5
#define CLUSTER_MAX_PEERS   (CLUSTER_MAX_NODES - 1)
#define CLUSTER_DEFAULT_PORT 6672           /* internal mesh port (AMQP is 5672) */

/* ---- wire frame header --------------------------------------------------- *
 *
 *   offset  field     type   notes
 *      0    magic     u8     CLUSTER_FRAME_MAGIC
 *      1    version   u8     CLUSTER_PROTO_VERSION
 *      2    type      u8     cluster_frame_type_t
 *      3    flags     u8     reserved / per-type bits
 *      4    term      u32    sender's currentTerm (big-endian)
 *      8    length    u32    payload byte count following the header
 *     12    crc32     u32    CRC-32 (IEEE) over the payload bytes
 *   ------------------------------------------------------------------------
 *     16 bytes total. Always big-endian on the wire (see cluster_hdr_pack).
 */
#define CLUSTER_HDR_SIZE       16u
#define CLUSTER_FRAME_MAGIC    0xB7u         /* 'B'eaver cluster */
#define CLUSTER_PROTO_VERSION  1u
#define CLUSTER_MAX_PAYLOAD    (16u * 1024u * 1024u) /* guard vs hostile length */

typedef enum {
    /* discovery / handshake */
    CL_FRAME_HELLO        = 1,   /* announce node_id + cluster epoch on connect */
    CL_FRAME_HELLO_ACK    = 2,
    /* Raft control plane */
    CL_FRAME_PREVOTE      = 10,  /* "would you vote for me?" - no term bump */
    CL_FRAME_PREVOTE_RESP = 11,
    CL_FRAME_VOTE_REQ     = 12,  /* RequestVote */
    CL_FRAME_VOTE_RESP    = 13,
    CL_FRAME_APPEND       = 14,  /* AppendEntries; empty payload == heartbeat */
    CL_FRAME_APPEND_RESP  = 15,
    /* follower -> leader: forward a client-originated op for the leader to
     * append + replicate. This is the "publish to any node" internal routing. */
    CL_FRAME_FORWARD      = 16,
    /* leader -> followers: broadcast the cluster view (per-node replication
     * progress + derived state) so every node's status API/GUI is consistent. */
    CL_FRAME_STATUS       = 17,
    /* State transfer (InstallSnapshot): re-seed a node whose log starts below
     * the cluster's compaction point (fresh/wiped data_dir). BEGIN carries the
     * boundary + the topology/config blob; CHUNK carries live queue messages;
     * END closes the stream; ACK is the follower's flow-control/completion. */
    CL_FRAME_SNAP_BEGIN   = 18,
    CL_FRAME_SNAP_CHUNK   = 19,
    /* data plane (decoupled): broadcast load/entropy for backpressure routing */
    CL_FRAME_ROUTE_HINT   = 20,
    CL_FRAME_SNAP_END     = 21,
    CL_FRAME_SNAP_ACK     = 22,
    /* leader -> follower: cumulative ack for CL_FRAME_FORWARD batches. Payload
     * is the highest per-origin `seq` (see cluster_propose_tracked) committed
     * so far; the follower can then stop retransmitting anything <= it. */
    CL_FRAME_FORWARD_ACK  = 23,
} cluster_frame_type_t;

/* In-memory view of the header. Never memcpy'd onto the wire (endianness):
 * use cluster_hdr_pack / cluster_hdr_parse. */
typedef struct {
    uint8_t  magic;
    uint8_t  version;
    uint8_t  type;
    uint8_t  flags;
    uint32_t term;
    uint32_t length;
    uint32_t crc32;
} cluster_frame_hdr_t;

/* ---- roles & health ------------------------------------------------------ *
 * Note: there is deliberately no "Isolated" *role*. A node that cannot reach a
 * majority simply stays a follower/candidate that never wins - and we surface
 * that to the data plane as a derived HEALTH flag, so it can reject Basic.Publish
 * with a channel exception (the original "Scenario B" requirement), WITHOUT
 * inventing a consensus state that Raft's safety proof does not cover. */
typedef enum {
    CL_ROLE_FOLLOWER  = 0,
    CL_ROLE_CANDIDATE = 1,
    CL_ROLE_LEADER    = 2,
} cluster_role_t;

typedef enum {
    CL_HEALTH_OK       = 0,   /* part of a quorum (writes allowed) */
    CL_HEALTH_ISOLATED = 1,   /* cannot reach a majority: reject publishes */
} cluster_health_t;

/* Richer, admin-facing cluster state (derived by the leader, broadcast to all). */
typedef enum {
    CL_STATE_HEALTHY   = 0,   /* all nodes up and fully replicated */
    CL_STATE_SYNCING   = 1,   /* a follower is catching up (replication in progress) */
    CL_STATE_DEGRADED  = 2,   /* a node is down but a majority is still up */
    CL_STATE_NO_QUORUM = 3,   /* cannot reach a majority: read-only */
} cluster_state_t;

typedef enum {
    CL_LINK_DOWN        = 0,
    CL_LINK_CONNECTING  = 1,
    CL_LINK_HANDSHAKING = 2,
    CL_LINK_UP          = 3,
} cluster_link_state_t;

/* ---- replicated log ------------------------------------------------------ *
 * Lock-free-read ring buffer. The cluster loop is the single writer; the data
 * plane only ever READS commit_index (acquire) to learn what is durable. */
typedef struct {
    uint64_t term;     /* term in which the entry was created */
    uint64_t index;    /* monotonic log index (1-based) */
    uint32_t op_type;  /* cluster_op_t below */
    uint32_t len;
    void    *payload;  /* owned serialized op (pool-allocated in the hot path) */
} cluster_log_entry_t;

/* Replicated state-machine op types (what a committed entry mutates). */
typedef enum {
    CL_OP_NOOP          = 0,
    CL_OP_PUBLISH       = 1,  /* a routed message */
    CL_OP_DECLARE_QUEUE = 2,
    CL_OP_DECLARE_EXCH  = 3,
    CL_OP_BIND          = 4,
    CL_OP_ACK           = 5,
    /* Access-control config (applied to the authstore; carried in the snapshot
     * like topology, so it is replicated, persisted and survives compaction). */
    CL_OP_ADD_VHOST     = 6,  /* str(vhost) */
    CL_OP_DEL_VHOST     = 7,  /* str(vhost) */
    CL_OP_ADD_USER      = 8,  /* str(user) str(passhash) u32(tags) */
    CL_OP_DEL_USER      = 9,  /* str(user) */
    CL_OP_SET_PERM      = 10, /* str(user) str(vhost) str(conf) str(write) str(read) */
    CL_OP_CLEAR_PERM    = 11, /* str(user) str(vhost) */
} cluster_op_t;

typedef struct {
    cluster_log_entry_t *entries;       /* ring of (mask+1) slots (power of two) */
    uint32_t             cap;           /* == mask + 1 */
    uint32_t             mask;
    uint64_t             base_index;    /* highest index DROPPED by compaction;
                                         * in-RAM entries hold (base_index, last] */
    uint64_t             base_term;     /* term of base_index (log-match boundary) */
    _Atomic uint64_t     last_index;    /* highest appended  (release on append) */
    _Atomic uint64_t     commit_index;  /* highest committed (release on commit) */
    uint64_t             applied_index; /* highest applied to broker (loop-only) */
    uint64_t             durable_index; /* highest index FSYNC'd to disk locally;
                                         * compaction floor (NOT match, which is
                                         * only page-cache-acked and can be lost
                                         * on a crash before the async fsync) */
    uint64_t             compacted_to;  /* entries <= this had their RAM body freed
                                         * (replicated everywhere; file copy kept) */
} cluster_log_t;

/* ---- per-peer connection state ------------------------------------------- */
struct cluster_node;

typedef struct cluster_peer {
    uv_tcp_t  handle;        /* MUST be first: (uv_tcp_t*) <-> (cluster_peer_t*) */
    int       node_id;       /* peer's id (0..nnodes-1) */
    char      addr[72];      /* "ip:port" of this peer's cluster endpoint */
    struct sockaddr_storage  sa;
    int       have_sa;

    _Atomic int link_state;  /* cluster_link_state_t; readable cross-thread */
    int       outbound;      /* 1 if WE dial it (lower id dials higher) */
    int       closing;

    /* The active stream for this peer: points at &handle when we dialed it
     * (outbound), or at an accepted inbound socket's handle (inbound). All
     * sends to the peer go through ->link, so the rest of the code is symmetric. */
    uv_stream_t *link;
    /* For an inbound link, the heap holder that backs ->link (its uv_tcp_t is
     * embedded), freed when the link closes. NULL for an outbound link (the
     * handle is embedded in this peer). */
    void        *link_holder;

    uv_connect_t connect_req;
    uv_timer_t   reconnect_timer;
    uint32_t     backoff_ms;

    /* Raft leader bookkeeping (valid only while we are leader). With pipelining,
     * next_index is the window's leading edge (next index to SEND) and
     * match_index is the confirmed high-water; (next_index-1-match_index) is the
     * count of sent-but-unacknowledged entries (the sliding window occupancy). */
    uint64_t  next_index;    /* next log index to send this peer */
    uint64_t  match_index;   /* highest index known replicated on this peer */
    uint64_t  durable_index; /* highest index this peer reports FSYNC'd to disk;
                              * bounds compaction so a peer that crashes before
                              * its async fsync never lands below our base */
    /* State-transfer (snapshot) streaming to THIS peer (leader-side; active
     * while the peer is below the compaction point and being re-seeded). */
    struct {
        int       active;
        uint64_t  base;        /* boundary: dump covers cluster_id <= base */
        uint64_t  base_term;
        void     *queues;      /* beaver_queue_t** (ref'd), collected at start */
        size_t    nqueues, qi; /* current queue index */
        void     *msgs;        /* beaver_message_t** (ref'd) of current queue */
        size_t    nmsgs, mi;   /* current message index */
        uint32_t  unacked_chunks; /* flow control: pause when window full */
        int       end_sent;       /* SNAP_END emitted (send exactly once) */
    } snap;

    uint64_t  inflight;      /* entries SENT to this peer but not yet acked.
                              * Tracked explicitly (not next_index-1-match) so a
                              * just-reset match_index after a (re)connect can't
                              * make the sliding window look permanently full and
                              * stall catch-up sends to a rejoining follower. */

    /* Link-quality EWMA for DATA-PLANE Wave-Function Routing only. Fixed-point
     * microseconds; NEVER read by the consensus core. This is the honest,
     * decoupled remnant of "WFR": route new clients toward the lower-RTT,
     * lower-load nodes - it can be probabilistic because a bad routing guess
     * costs uneven load, not data loss. */
    _Atomic uint32_t rtt_ewma_us;
    _Atomic uint32_t miss_streak;

    /* Inbound frame reassembly (one frame may span several reads). */
    char    *rbuf;
    size_t   rbuf_len;
    size_t   rbuf_cap;

    struct cluster_node *node;  /* back-pointer */
} cluster_peer_t;

/* ---- node configuration -------------------------------------------------- */
typedef struct {
    int      self_id;                       /* this node's id (0..nnodes-1) */
    int      nnodes;                        /* cluster size (3) */
    char     bind_addr[64];                 /* mesh listen address */
    int      cluster_port;                  /* internal mesh port */
    char     node_addr[CLUSTER_MAX_NODES][72]; /* "ip:port" of EVERY node, by id */
    char     data_dir[256];                 /* dir for the persisted log ("" = off) */
    uint32_t election_min_ms;               /* randomized election timeout window */
    uint32_t election_max_ms;
    uint32_t heartbeat_ms;                  /* leader heartbeat interval */
    char     secret[128];                   /* shared PSK, identical on every
                                             * node; HMACs the HELLO handshake
                                             * so an attacker on the mesh
                                             * network cannot impersonate a
                                             * peer. Required (cluster_node_new
                                             * refuses to start without it). */
    int      durable_commit;                /* 1 (default) = commit needs a
                                             * durable (fsync'd) majority, not
                                             * just a page-cache-acked one */
} cluster_config_t;

void cluster_config_defaults(cluster_config_t *c);

/* ---- the cluster node (control-plane singleton) -------------------------- */
typedef struct cluster_node {
    uv_loop_t *loop;            /* dedicated cluster loop (not owned) */
    uv_tcp_t   listener;        /* inbound peer connections on cluster_port */

    int  self_id;
    int  nnodes;
    int  npeers;
    int  quorum;                /* nnodes/2 + 1 */
    char self_addr[72];         /* this node's own "ip:port" mesh endpoint */
    char secret[128];           /* shared PSK; HMACs the HELLO handshake */
    int  durable_commit;        /* 1 = commit needs a durable majority */
    cluster_peer_t peers[CLUSTER_MAX_PEERS];

    /* Raft state - single-writer (cluster loop). Term/role/leader are atomic so
     * the data plane can sample them lock-free; voted_for/votes are loop-only. */
    _Atomic uint64_t current_term;
    int              voted_for;      /* node_id, or -1 for none (this term) */
    _Atomic int      role;           /* cluster_role_t */
    _Atomic int      leader_id;      /* -1 if unknown */
    int              votes_granted;
    int              prevotes_granted;
    int              in_prevote;     /* 1 while a PreVote round is outstanding */
    uint64_t         last_leader_contact; /* uv_now() of last valid AppendEntries */

    cluster_log_t    log;
    void            *persist;        /* on-disk log/meta (cluster.c), or NULL */
    int              log_dirty;      /* unsynced appends pending a batch flush */
    _Atomic int      inbox_depth;    /* queued proposals (for backpressure) */
    _Atomic int      throttle;       /* 1 = leader congested -> pause producers */

    /* Cluster view: the leader broadcasts per-node replication progress + state
     * (CL_FRAME_STATUS); followers cache it so the status API/GUI is consistent
     * on every node. Written/read only on the cluster loop; the status API reads
     * it via cluster_get_status, which runs on the cluster loop's data. */
    struct { uint64_t match; int reachable; } view[CLUSTER_MAX_NODES];
    uint64_t         view_target;    /* leader's last_index (replication target) */
    uint64_t         view_commit;
    int              view_state;     /* cluster_state_t (cached on followers) */
    int              have_view;
    unsigned         status_tick;    /* throttles the status broadcast cadence */
    unsigned         trim_tick;       /* throttles malloc_trim (RSS giveback) */

    uv_timer_t  election_timer;      /* follower/candidate: triggers election */
    uv_timer_t  heartbeat_timer;     /* leader: sends AppendEntries heartbeats */
    uv_timer_t  fsync_timer;         /* periodic background fsync of the log */
    uv_timer_t  fwd_retry_timer;     /* follower: retry forwarding the inbox
                                      * when the leader is (re)elected/reachable */
    _Atomic int fwd_congested;       /* inbox too deep: pause producers (hysteresis) */
    uint32_t    election_min_ms, election_max_ms, heartbeat_ms;
    uint64_t    rng_state;           /* per-node PRNG (randomized timeouts) */

    uv_async_t  propose_async;       /* worker -> cluster: pending proposals */
    /* Proposal inbox: workers append under propose_lock and wake the cluster
     * loop via propose_async, which drains it. Topology declarations are rare,
     * so a small lock here never touches the message hot path. */
    pthread_mutex_t      propose_lock;
    struct cl_proposal  *propose_head;
    struct cl_proposal  *propose_tail;

    /* Every proposal (from ANY node's role) is assigned a `seq` from this
     * counter at submission time (cluster_propose_tracked). It is used both
     * as the dedup/ack key when forwarding to the leader (below) and as the
     * lookup key into propose_status_* for cluster_proposal_status() polling. */
    _Atomic uint64_t propose_seq_next;
#define CL_STATUS_TABLE_SIZE 4096
    _Atomic uint64_t propose_status_seq[CL_STATUS_TABLE_SIZE];
    _Atomic int      propose_status_val[CL_STATUS_TABLE_SIZE];

    /* Follower: proposals already sent to the leader in a CL_FRAME_FORWARD but
     * not yet confirmed committed (a successful cl_send()/uv_write() only
     * proves the bytes were queued locally, never that the leader received or
     * applied them - see forward_batch_to_leader). Retransmitted whole on
     * reconnect/new-leader and periodically until acked; never freed until a
     * CL_FRAME_FORWARD_ACK (or, as a last-resort safety net for the rare
     * multi-hop-relay case, a timeout) confirms them. */
    struct cl_proposal  *fwd_pending_head;
    struct cl_proposal  *fwd_pending_tail;
    unsigned             fwd_pending_tick; /* throttles the stale-resend sweep */

    /* Leader: per-origin-node highest `seq` already committed (dedup for
     * retransmitted CL_FRAME_FORWARD batches) and the FIFO of committed-index
     * -> (origin, seq) waiting for its CL_FRAME_FORWARD_ACK to be sent. */
    uint64_t             committed_fwd_seq[CLUSTER_MAX_NODES];
    struct cl_fwd_ack   *fwd_ack_pending_head;
    struct cl_fwd_ack   *fwd_ack_pending_tail;

    uv_async_t  shutdown_async;      /* any thread -> cluster: request stop */
    int         shutdown_async_installed;

    _Atomic int health;              /* cluster_health_t (data-plane gate) */
    _Atomic int storage_failed;      /* 1 once fsync()/write() has failed: the
                                      * node can no longer promise durability
                                      * and must be treated as unhealthy and,
                                      * if leader, step down. */

    beaver_broker_t *broker;         /* shared, thread-safe; apply target */
    struct authstore *authstore;     /* shared access-control table (apply target) */
    int  running;

    /* Topology snapshot accumulator: serialized DECLARE_QUEUE/EXCH/BIND ops
     * (each [op u32][len u32][payload]), deduped, carried forward across log
     * compaction so a truncated prefix's topology still rebuilds on recovery.
     * Bounded by topology size, not message count. */
    uint8_t  *topo;
    size_t    topo_len, topo_cap;
    unsigned  compact_tick;          /* throttles the compaction sweep cadence */
    uint64_t  compact_hint;          /* highest index replicated everywhere: the
                                      * leader computes it; a follower caches the
                                      * value the leader sends in AppendEntries */
    uint64_t  snap_warn_at;          /* rate-limits the "below compaction" warning */
} cluster_node_t;

/* ---- lifecycle ----------------------------------------------------------- */

/* Allocate a node bound to `loop` (typically a dedicated cluster loop/thread). */
cluster_node_t *cluster_node_new(uv_loop_t *loop, const cluster_config_t *cfg,
                                 beaver_broker_t *broker, struct authstore *auth);

/* Bind + listen the internal mesh port. Call BEFORE cluster_node_start (kept
 * separate so config parsing stays out of the lifecycle path). Returns 0 or a
 * negative libuv error. */
int  cluster_node_listen(cluster_node_t *n, const char *bind_addr, int port,
                         int backlog);

/* Dial peers and arm the election timer. Returns 0 or a negative libuv error.
 * Must run on the cluster loop thread (before uv_run). */
int  cluster_node_start(cluster_node_t *n);

/* Install an async so another thread can request shutdown of the cluster loop
 * via cluster_node_request_shutdown(). Call before running the loop. */
int  cluster_node_install_shutdown_async(cluster_node_t *n);

/* Ask the cluster node to shut down. Thread-safe (wakes its loop). */
void cluster_node_request_shutdown(cluster_node_t *n);

/* Close all handles on the cluster loop so uv_run can return. Idempotent.
 * Runs on the cluster loop thread. */
void cluster_node_stop(cluster_node_t *n);

/* Free node resources. Call after the loop ended. */
void cluster_node_free(cluster_node_t *n);

/* ---- data-plane queries (lock-free; safe from any worker thread) --------- */
cluster_role_t   cluster_role_of(const cluster_node_t *n);
int              cluster_leader_id(const cluster_node_t *n);
int              cluster_is_leader(const cluster_node_t *n);
uint64_t         cluster_term(const cluster_node_t *n);
cluster_health_t cluster_health(const cluster_node_t *n);

/* 1 when the cluster is congested (replication backlog high) and producers
 * should be paused to apply flow control. Lock-free; safe from any thread. */
int              cluster_should_throttle(const cluster_node_t *n);

/*
 * Propose a replicated op from a worker thread. Thread-safe: enqueues the op
 * and wakes the cluster loop via uv_async. Returns 0 if accepted for
 * replication (we are leader and healthy), or negative if this node is not the
 * leader (the caller should reject/redirect the AMQP Basic.Publish). The commit
 * is asynchronous; a completion/callback model is defined in the next phase.
 */
int cluster_propose(cluster_node_t *n, cluster_op_t op_type,
                    const void *data, size_t len);

/*
 * Like cluster_propose(), but returns a nonzero `seq` the caller can poll with
 * cluster_proposal_status() to learn when the op actually COMMITS (not merely
 * "accepted into the local inbox"). Returns 0 on outright failure (OOM) -
 * callers must treat that as an immediate error, not "still pending".
 */
uint64_t cluster_propose_tracked(cluster_node_t *n, cluster_op_t op_type,
                                 const void *data, size_t len);

typedef enum {
    CL_PROPOSAL_PENDING   = 0,
    CL_PROPOSAL_COMMITTED = 1,
    CL_PROPOSAL_REJECTED  = 2,
} cluster_proposal_status_t;

/* Poll the outcome of a `seq` returned by cluster_propose_tracked(). Safe from
 * any thread. A caller should poll with a bounded timeout: entries are not
 * kept forever (a fixed-size table; a very stale, already-resolved seq that
 * was overwritten by newer traffic reads back as PENDING, not an error). */
cluster_proposal_status_t cluster_proposal_status(cluster_node_t *n, uint64_t seq);

/* Topology replication convenience wrappers (encode the op, then propose).
 * Each returns a nonzero `seq` to poll with cluster_proposal_status(), or 0 on
 * outright failure (OOM). The committed op is applied to the broker on EVERY
 * node. */
uint64_t cluster_replicate_declare_queue(cluster_node_t *n, const char *vhost,
                                         const char *name,
                                         uint8_t flags);
uint64_t cluster_replicate_declare_exchange(cluster_node_t *n, const char *vhost,
                                            const char *name, int type,
                                            uint8_t flags);
uint64_t cluster_replicate_bind(cluster_node_t *n, const char *vhost,
                                const char *queue, const char *exchange,
                                const char *routing_key);

/* Replicate a published message through the cluster: appended to the Raft log
 * (forwarded to the leader if we are a follower), committed on a majority, then
 * applied (re-routed into the broker) on EVERY node. Used for persistent
 * publishes. Returns 0 if accepted for replication, negative if there is no
 * reachable leader. */
int cluster_replicate_publish(cluster_node_t *n, const char *vhost,
                              const char *exchange, const char *routing_key,
                              const void *body, size_t body_len,
                              const void *props, size_t props_len);

/* Like cluster_replicate_publish(), but returns a nonzero `seq` to poll with
 * cluster_proposal_status() so a publisher confirm (Basic.Ack) can be sent only
 * once the message COMMITS on a quorum. Returns 0 on outright failure (OOM /
 * oversized names / no reachable leader path). */
uint64_t cluster_replicate_publish_tracked(cluster_node_t *n, const char *vhost,
                              const char *exchange, const char *routing_key,
                              const void *body, size_t body_len,
                              const void *props, size_t props_len);

/* Replicate a queue's consume watermark so every node drains its replica copies
 * of messages with cluster_id <= watermark. Returns 0 if accepted. */
int cluster_replicate_consume(cluster_node_t *n, const char *vhost,
                              const char *queue, uint64_t watermark);

/* ---- access-control config replication (propose to the cluster) ---------- *
 * Each routes through the leader and applies to the authstore on every node. */
int cluster_replicate_add_vhost(cluster_node_t *n, const char *vhost);
int cluster_replicate_del_vhost(cluster_node_t *n, const char *vhost);
int cluster_replicate_add_user(cluster_node_t *n, const char *user,
                               const char *pass_hash, uint32_t tags);
int cluster_replicate_del_user(cluster_node_t *n, const char *user);
int cluster_replicate_set_perm(cluster_node_t *n, const char *user,
                               const char *vhost, const char *configure,
                               const char *write, const char *read);
int cluster_replicate_clear_perm(cluster_node_t *n, const char *user,
                                 const char *vhost);

/* ---- status snapshot (for the management API / dashboard) ---------------- *
 * A point-in-time view, safe to read from another thread (atomics for the
 * node-level fields; per-member match_index is a display-only monotonic read). */
typedef struct {
    int      node_id;
    int      is_self;
    int      is_leader;
    int      reachable;     /* link UP (self is always reachable) */
    uint64_t match_index;   /* leader's replication progress for this node */
    char     addr[72];
} cluster_member_t;

typedef struct {
    int              self_id;
    int              nnodes;
    int              leader_id;     /* -1 if unknown */
    int              quorum;
    uint64_t         term;
    uint64_t         commit_index;
    uint64_t         last_index;
    uint64_t         target_index;  /* leader's last_index: replication target */
    cluster_role_t   role;
    cluster_health_t health;
    cluster_state_t  state;         /* admin-facing: healthy/syncing/degraded/... */
    int              storage_failed; /* 1 if a local fsync()/write() has failed */
    int              nmembers;
    cluster_member_t members[CLUSTER_MAX_NODES];
} cluster_status_t;

void cluster_get_status(const cluster_node_t *n, cluster_status_t *out);

/* ---- frame codec (pure, no I/O; exposed for unit tests) ------------------ */
void     cluster_hdr_pack(uint8_t out[CLUSTER_HDR_SIZE], const cluster_frame_hdr_t *h);
int      cluster_hdr_parse(const uint8_t *in, size_t len, cluster_frame_hdr_t *out);
uint32_t cluster_crc32(const void *data, size_t len);

#ifdef __cplusplus
}
#endif

#endif /* BEAVERMQ_CLUSTER_H */
