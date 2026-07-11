/*
 * cluster.c - 3-node Raft consensus + mesh transport for BeaverMQ.
 *
 * This file currently implements the CONTROL-PLANE TRANSPORT and DISCOVERY:
 *   - the fixed 16-byte big-endian frame codec + CRC32,
 *   - the fully-meshed TCP layer: each node listens and dials its peers
 *     (lower id dials higher id, so each pair has exactly one connection),
 *     with HELLO/HELLO_ACK handshake and capped exponential-backoff reconnect,
 *   - the election / heartbeat libuv timers (armed and randomized).
 *
 * The Raft DECISION logic (PreVote, RequestVote, AppendEntries, commit advance,
 * apply-to-broker) is deliberately left as clearly marked `TODO (phase 2)`
 * stubs: shipping a plausible-but-unproven consensus core is worse than none.
 * The structures and the wire are in place so phase 2 is purely filling in the
 * state-machine handlers - no transport rework.
 *
 * THREADING: everything here runs on the single dedicated cluster loop, so the
 * Raft state needs no locks. The only cross-thread surface is the atomics the
 * data plane samples (term/role/leader/health) and the propose_async inbox.
 */
#include "cluster.h"
#include "logger.h"
#include "broker.h"
#include "message.h"
#include "authstore.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdlib.h>
#if defined(__GLIBC__)
#include <malloc.h>   /* malloc_trim: return freed arenas to the OS */
#endif
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

/* ---- big-endian helpers -------------------------------------------------- */

static void put_be32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}

static uint32_t get_be32(const uint8_t *p)
{
    return (uint32_t)p[0] << 24 | (uint32_t)p[1] << 16 |
           (uint32_t)p[2] << 8  | (uint32_t)p[3];
}

static void put_be64(uint8_t *p, uint64_t v)
{
    put_be32(p, (uint32_t)(v >> 32));
    put_be32(p + 4, (uint32_t)v);
}

static uint64_t get_be64(const uint8_t *p)
{
    return (uint64_t)get_be32(p) << 32 | get_be32(p + 4);
}

/* Raft timer callbacks (defined later; referenced by the state-machine helpers). */
static void election_timer_cb(uv_timer_t *timer);
static void heartbeat_timer_cb(uv_timer_t *timer);

/* ---- CRC-32 (IEEE 802.3, poly 0xEDB88820) -------------------------------- */

static uint32_t s_crc_tab[256];
static int      s_crc_ready;  /* set once; the table is filled identically each
                               * time so the only-on-the-cluster-loop usage and
                               * the test usage never observe a torn value. */

static void crc_build(void)
{
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++)
            c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        s_crc_tab[i] = c;
    }
    s_crc_ready = 1;
}

uint32_t cluster_crc32(const void *data, size_t len)
{
    if (!s_crc_ready)
        crc_build();
    const uint8_t *p = data;
    uint32_t c = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++)
        c = s_crc_tab[(c ^ p[i]) & 0xFF] ^ (c >> 8);
    return c ^ 0xFFFFFFFFu;
}

/* ---- frame header codec -------------------------------------------------- */

void cluster_hdr_pack(uint8_t out[CLUSTER_HDR_SIZE], const cluster_frame_hdr_t *h)
{
    out[0] = h->magic;
    out[1] = h->version;
    out[2] = h->type;
    out[3] = h->flags;
    put_be32(out + 4,  h->term);
    put_be32(out + 8,  h->length);
    put_be32(out + 12, h->crc32);
}

/* Returns 0 if a valid header was parsed, -1 otherwise (bad magic/version/len).
 * The CRC is NOT checked here (the payload is not yet in hand); callers verify
 * it against the payload once they have CLUSTER_HDR_SIZE + length bytes. */
int cluster_hdr_parse(const uint8_t *in, size_t len, cluster_frame_hdr_t *out)
{
    if (len < CLUSTER_HDR_SIZE)
        return -1;
    out->magic   = in[0];
    out->version = in[1];
    out->type    = in[2];
    out->flags   = in[3];
    out->term    = get_be32(in + 4);
    out->length  = get_be32(in + 8);
    out->crc32   = get_be32(in + 12);
    if (out->magic != CLUSTER_FRAME_MAGIC || out->version != CLUSTER_PROTO_VERSION)
        return -1;
    if (out->length > CLUSTER_MAX_PAYLOAD)
        return -1;
    return 0;
}

/* ---- config defaults ----------------------------------------------------- */

void cluster_config_defaults(cluster_config_t *c)
{
    memset(c, 0, sizeof(*c));
    c->self_id         = 0;
    c->nnodes          = 3;
    snprintf(c->bind_addr, sizeof(c->bind_addr), "0.0.0.0");
    c->cluster_port    = CLUSTER_DEFAULT_PORT;
    /* Election timing must be >> the worst-case "leader silent" window, which on
     * a real LAN under heavy replication is dominated NOT by network RTT but by
     * the single cluster loop occasionally blocking (building/CRC'ing large
     * AppendEntries batches, applying a big committed batch of 4 KiB messages).
     * The classic 150-300 ms Raft figures assume an idle, low-latency system;
     * under load a >150 ms loop hiccup looks like a dead leader and triggers an
     * election storm (term climbs ~1/s, no stable leader, writes can't commit
     * and are lost). etcd ships 1000 ms / 100 ms for the same reason. We use a
     * generous 1000-2000 ms election window with 200 ms heartbeats (5-10 missed
     * heartbeats tolerated) so transient loop hiccups never unseat the leader.
     * Tunable via the election_timeout_ms / heartbeat_ms config keys. */
    c->election_min_ms = 1000;
    c->election_max_ms = 2000;
    c->heartbeat_ms    = 200;
}

/* ---- small utilities ----------------------------------------------------- */

static uint64_t xorshift64(uint64_t *s)
{
    uint64_t x = *s;
    x ^= x << 13; x ^= x >> 7; x ^= x << 17;
    return *s = x ? x : 0x9E3779B97F4A7C15ull;
}

static uint32_t election_timeout_ms(cluster_node_t *n)
{
    uint32_t span = n->election_max_ms - n->election_min_ms + 1;
    return n->election_min_ms + (uint32_t)(xorshift64(&n->rng_state) % span);
}

/* Parse "host:port" (IPv4 only for now; IPv6 is a phase-2 TODO). */
static int parse_addr(const char *s, struct sockaddr_in *out)
{
    const char *colon = strrchr(s, ':');
    if (!colon)
        return -1;
    char host[64];
    size_t hl = (size_t)(colon - s);
    if (hl == 0 || hl >= sizeof(host))
        return -1;
    memcpy(host, s, hl);
    host[hl] = '\0';
    int port = atoi(colon + 1);
    if (port <= 0 || port > 65535)
        return -1;
    return uv_ip4_addr(host, port, out);
}

static int buf_append(char **buf, size_t *len, size_t *cap,
                      const char *data, size_t n)
{
    if (*len + n > *cap) {
        size_t nc = *cap ? *cap : 1024;
        while (nc < *len + n)
            nc *= 2;
        char *nb = realloc(*buf, nc);
        if (!nb)
            return -1;
        *buf = nb;
        *cap = nc;
    }
    memcpy(*buf + *len, data, n);
    *len += n;
    return 0;
}

/* ---- frame send ---------------------------------------------------------- */

static void on_write(uv_write_t *req, int status)
{
    if (status)
        LOG_DEBUG("cluster: write failed: %s", uv_strerror(status));
    free(req->data);  /* the combined header+payload buffer */
    free(req);
}

/* Build one frame (16-byte header + payload) and write it to `link`. The header
 * is packed once; the payload is copied once into the combined buffer. */
static int cl_send(uv_stream_t *link, uint8_t type, uint8_t flags,
                   uint32_t term, const void *payload, uint32_t plen)
{
    if (!link)
        return -1;
    size_t total = CLUSTER_HDR_SIZE + plen;
    char *buf = malloc(total);
    if (!buf)
        return -1;

    cluster_frame_hdr_t h = {
        .magic = CLUSTER_FRAME_MAGIC, .version = CLUSTER_PROTO_VERSION,
        .type = type, .flags = flags, .term = term, .length = plen,
        .crc32 = cluster_crc32(payload, plen),
    };
    cluster_hdr_pack((uint8_t *)buf, &h);
    if (plen)
        memcpy(buf + CLUSTER_HDR_SIZE, payload, plen);

    uv_write_t *req = malloc(sizeof(*req));
    if (!req) {
        free(buf);
        return -1;
    }
    req->data = buf;
    uv_buf_t b = uv_buf_init(buf, (unsigned)total);
    int rc = uv_write(req, link, &b, 1, on_write);
    if (rc) {
        free(buf);
        free(req);
    }
    return rc;
}

static void send_hello(cluster_node_t *n, uv_stream_t *link, uint8_t type)
{
    uint8_t body[4];
    put_be32(body, (uint32_t)n->self_id);
    cl_send(link, type, 0, (uint32_t)atomic_load(&n->current_term), body, sizeof(body));
}

/* ---- Raft state machine -------------------------------------------------- *
 * All of this runs on the single cluster loop, so it needs no locks. term /
 * role / leader_id / health are atomic ONLY so the data plane can sample them
 * lock-free; they are written exclusively from here. */

/* ---- on-disk persistence (Raft log + term/votedFor) ---------------------- *
 * Two files per node in data_dir: cluster-<id>.log (append-only entries) and
 * cluster-<id>.meta (currentTerm + votedFor). The log is ASYNC: records are
 * batched into one write() per batch and fsync'd in the background on a timer
 * (durability comes from replication to a majority's RAM, like quorum queues).
 * The log file is posix_fallocate'd in chunks to avoid block-allocation stalls.
 * meta IS fsync'd synchronously before a vote is answered (Raft safety). On
 * startup the log is replayed into memory; committed entries re-apply to the
 * broker via the normal commit path once the node rejoins or wins an election. */

static int log_ensure_cap(cluster_log_t *log, uint64_t need); /* fwd decl */
static void apply_op(cluster_node_t *n, const cluster_log_entry_t *e); /* fwd decl */
static cluster_log_entry_t *log_at(cluster_log_t *log, uint64_t idx); /* fwd decl */
static uint64_t log_term_at(cluster_log_t *log, uint64_t idx);        /* fwd decl */
static void wr_str(uint8_t **p, const char *s);                       /* fwd decl */
static int  rd_str(const uint8_t **p, const uint8_t *end,
                   char *out, size_t outsz);                          /* fwd decl */

#define CL_META_MAGIC 0x424D514Du /* 'BMQM' */
#define CL_META_SIZE  24          /* magic(4) ver(4) term(8) votedFor(4) pad(4) */
#define CL_REC_HDR    24          /* term(8) index(8) op(4) len(4) */

#define CL_FALLOC_CHUNK (64u * 1024u * 1024u) /* pre-allocate the log 64 MiB at a time */

typedef struct cl_persist {
    int       log_fd;
    int       meta_fd;
    uint64_t  log_size;     /* logical end of the log (incl. buffered, unwritten) */
    uint64_t  write_pos;    /* file offset already written (<= log_size) */
    uint64_t  alloc_size;   /* bytes posix_fallocate'd (avoids block-alloc stalls) */
    uint64_t *offsets;      /* offsets[k] = ABSOLUTE file offset of index
                             * (base_index + 1 + k) */
    uint32_t  off_cap;
    uint64_t  base_index;   /* mirror of log.base_index for offset indexing */
    uint64_t  base_offset;  /* file offset where surviving (post-compaction)
                             * records begin; prefix below is hole-punched */
    int       snap_fd;      /* topology snapshot file (cluster-N.snap), or -1 */
    char      dir[256];     /* data_dir, for building the .snap.tmp path */
    int       self_id;
    /* Write batching: append records here and flush them with ONE write() per
     * batch instead of a syscall per entry. */
    uint8_t  *wbuf;
    size_t    wbuf_len;
    size_t    wbuf_cap;
    int       dirty;        /* written-but-unsynced (fsync pending) */
} cl_persist_t;

static int write_all(int fd, const void *buf, size_t n)
{
    const uint8_t *p = buf;
    while (n) {
        ssize_t w = write(fd, p, n);
        if (w < 0) { if (errno == EINTR) continue; return -1; }
        p += w; n -= (size_t)w;
    }
    return 0;
}

static int off_ensure(cl_persist_t *ps, uint64_t need)
{
    if (need <= ps->off_cap) return 0;
    uint32_t nc = ps->off_cap ? ps->off_cap : 64;
    while (nc < need) nc *= 2;
    uint64_t *no = realloc(ps->offsets, (size_t)nc * sizeof(*no));
    if (!no) return -1;
    ps->offsets = no; ps->off_cap = nc;
    return 0;
}

static void persist_meta(cluster_node_t *n)
{
    cl_persist_t *ps = n->persist;
    if (!ps || ps->meta_fd < 0) return;
    uint8_t b[CL_META_SIZE];
    memset(b, 0, sizeof b);
    put_be32(b, CL_META_MAGIC);
    put_be32(b + 4, 1);
    put_be64(b + 8, atomic_load(&n->current_term));
    put_be32(b + 16, (uint32_t)n->voted_for);
    if (pwrite(ps->meta_fd, b, CL_META_SIZE, 0) == (ssize_t)CL_META_SIZE)
        fsync(ps->meta_fd);
}

/* Pre-allocate the log file in big chunks so the kernel never pauses our loop to
 * allocate blocks mid-write (posix_fallocate, #4). */
static void persist_reserve(cl_persist_t *ps, uint64_t end)
{
    if (end <= ps->alloc_size) return;
    uint64_t target = ((end + CL_FALLOC_CHUNK - 1) / CL_FALLOC_CHUNK) * CL_FALLOC_CHUNK;
    if (posix_fallocate(ps->log_fd, (off_t)ps->alloc_size,
                        (off_t)(target - ps->alloc_size)) == 0)
        ps->alloc_size = target;
    else
        ps->alloc_size = end; /* fallocate unsupported: just don't retry forever */
}

/* Flush the buffered record bytes to the page cache in ONE write() (#3). */
static void persist_write_flush(cluster_node_t *n)
{
    cl_persist_t *ps = n->persist;
    if (!ps || ps->log_fd < 0 || ps->wbuf_len == 0) return;
    persist_reserve(ps, ps->write_pos + ps->wbuf_len);
    if (lseek(ps->log_fd, (off_t)ps->write_pos, SEEK_SET) >= 0 &&
        write_all(ps->log_fd, ps->wbuf, ps->wbuf_len) == 0) {
        ps->write_pos += ps->wbuf_len;
    }
    ps->wbuf_len = 0;
}

static void persist_append(cluster_node_t *n, const cluster_log_entry_t *e)
{
    cl_persist_t *ps = n->persist;
    if (!ps || ps->log_fd < 0) return;
    if (off_ensure(ps, e->index - ps->base_index) != 0) return;

    size_t need = CL_REC_HDR + e->len;
    if (ps->wbuf_len + need > ps->wbuf_cap) {
        size_t nc = ps->wbuf_cap ? ps->wbuf_cap : (256u * 1024u);
        while (nc < ps->wbuf_len + need) nc *= 2;
        uint8_t *nb = realloc(ps->wbuf, nc);
        if (!nb) { persist_write_flush(n); if (need > ps->wbuf_cap) return; }
        else { ps->wbuf = nb; ps->wbuf_cap = nc; }
    }
    uint8_t *w = ps->wbuf + ps->wbuf_len;
    put_be64(w, e->term);     put_be64(w + 8, e->index);
    put_be32(w + 16, e->op_type); put_be32(w + 20, e->len);
    if (e->len) memcpy(w + CL_REC_HDR, e->payload, e->len);
    ps->wbuf_len += need;

    ps->offsets[e->index - ps->base_index - 1] = ps->log_size;
    ps->log_size += need;
    ps->dirty = 1;
    /* Bytes sit in wbuf until a batch boundary flushes them to the page cache;
     * fsync is async (persist_sync on a timer). Durability comes primarily from
     * REPLICATION (a majority holds the entry in RAM), like quorum queues. */
}

static void persist_sync(cluster_node_t *n)
{
    cl_persist_t *ps = n->persist;
    if (!ps || ps->log_fd < 0) return;
    persist_write_flush(n);
    if (ps->dirty) {
        fsync(ps->log_fd);
        ps->dirty = 0;
    }
    /* Everything written is now on stable storage: advance the durable index
     * (the floor below which it is safe to let the cluster compact). */
    n->log.durable_index = atomic_load(&n->log.last_index);
}

static void persist_truncate(cluster_node_t *n, uint64_t idx)
{
    cl_persist_t *ps = n->persist;
    if (!ps || ps->log_fd < 0 || idx < 1) return;
    persist_write_flush(n); /* push buffered bytes out before we ftruncate the file */
    uint64_t slot = idx - ps->base_index - 1;
    uint64_t newsize = (idx > ps->base_index && slot < ps->off_cap)
                       ? ps->offsets[slot] : ps->log_size;
    if (ftruncate(ps->log_fd, (off_t)newsize) == 0) {
        ps->log_size  = newsize;
        ps->write_pos = newsize;
        ps->dirty = 1;
    }
}

/* ---- log compaction (snapshot the dropped prefix's topology) -------------- *
 * The replicated log is append-only and otherwise grows forever (disk fills,
 * recovery slows). Once a prefix of entries is committed + applied + replicated
 * on EVERY node + (for message entries) consumed everywhere, it can be dropped:
 *   - message PUBLISH entries below the consume floor are gone from all queues,
 *     so they must NOT be re-enqueued on recovery -> safe to forget;
 *   - topology (DECLARE/BIND) entries must still rebuild on recovery -> they are
 *     carried in a small snapshot file (cluster-N.snap), accumulated as applied.
 * We keep snapshot_index/term as the log-matching boundary. Because we never
 * compact above all_replicated_index, no peer ever needs a dropped entry, so no
 * InstallSnapshot RPC is required (a peer that is merely behind is still served
 * from the surviving tail). */

#define CL_SNAP_MAGIC 0x424D5153u /* 'BMQS' */
#define CL_SNAP_HDR   36          /* magic4 ver4 baseIdx8 baseTerm8 baseOff8 topoLen4 */

/* Remember a topology op so a later compaction can replay it. Deduped by exact
 * bytes (declares/binds are idempotent and few). */
static void topo_add(cluster_node_t *n, uint32_t op, const void *data, uint32_t len)
{
    /* dedup: scan existing records for an identical (op,len,payload) */
    for (size_t i = 0; i + 8 <= n->topo_len; ) {
        uint32_t o = get_be32(n->topo + i), l = get_be32(n->topo + i + 4);
        if (o == op && l == len && (i + 8 + l <= n->topo_len) &&
            (len == 0 || memcmp(n->topo + i + 8, data, len) == 0))
            return; /* already have it */
        i += 8 + l;
    }
    size_t need = n->topo_len + 8 + len;
    if (need > n->topo_cap) {
        size_t nc = n->topo_cap ? n->topo_cap : 256;
        while (nc < need) nc *= 2;
        uint8_t *nb = realloc(n->topo, nc);
        if (!nb) return;
        n->topo = nb; n->topo_cap = nc;
    }
    put_be32(n->topo + n->topo_len, op);
    put_be32(n->topo + n->topo_len + 4, len);
    if (len) memcpy(n->topo + n->topo_len + 8, data, len);
    n->topo_len = need;
}

/* Serialized live-message record (shared by the .snap dump and the state-
 * transfer CHUNK frames):
 *   str(vhost) str(queue) str(exchange) str(rkey)
 *   u32 props_len, props, u32 body_len, body, u64 cluster_id */
static size_t snap_record_size(beaver_queue_t *q, const beaver_message_t *m)
{
    return 2 + strlen(queue_vhost(q)) + 2 + strlen(queue_name(q)) +
           2 + strlen(m->exchange ? m->exchange : "") +
           2 + strlen(m->routing_key ? m->routing_key : "") +
           4 + m->props_len + 4 + m->body_len + 8;
}
static void snap_write_record(uint8_t **w, beaver_queue_t *q,
                              const beaver_message_t *m)
{
    wr_str(w, queue_vhost(q));
    wr_str(w, queue_name(q));
    wr_str(w, m->exchange ? m->exchange : "");
    wr_str(w, m->routing_key ? m->routing_key : "");
    put_be32(*w, (uint32_t)m->props_len); *w += 4;
    if (m->props_len) { memcpy(*w, m->props, m->props_len); *w += m->props_len; }
    put_be32(*w, (uint32_t)m->body_len); *w += 4;
    if (m->body_len) { memcpy(*w, m->body, m->body_len); *w += m->body_len; }
    put_be64(*w, m->cluster_id); *w += 8;
}

/* broker_foreach_queue visitor: drop every buffered message (snapshot install
 * rebuilds the queues from the leader's dump + tail replay). */
static int purge_queue_cb(beaver_queue_t *q, void *ctx)
{
    (void)ctx;
    queue_purge(q);
    return 0;
}

/* broker_foreach_queue collector: ref every queue into a growable array. */
typedef struct { beaver_queue_t **q; size_t n, cap; } qcollect_t;
static int collect_queue_cb(beaver_queue_t *q, void *ctx)
{
    qcollect_t *c = ctx;
    if (c->n == c->cap) {
        size_t nc = c->cap ? c->cap * 2 : 16;
        beaver_queue_t **nq = realloc(c->q, nc * sizeof(*nq));
        if (!nq) return 1;
        c->q = nq; c->cap = nc;
    }
    c->q[c->n++] = queue_ref(q);
    return 0;
}

/* Atomically (temp + fsync + rename) write the snapshot: the compaction
 * boundary, the topology/config blob, and every live queued message with
 * cluster_id <= base (v2). On a normal node that message set is EMPTY (the
 * compaction floor never crosses a live message); it is non-empty only on a
 * node that was re-seeded by state transfer, whose live messages predate its
 * log - without persisting them here, a restart would silently lose them. */
static int snapshot_write(cluster_node_t *n, uint64_t base_index,
                          uint64_t base_term, uint64_t base_offset)
{
    cl_persist_t *ps = n->persist;
    if (!ps) return -1;
    size_t head_len = CL_SNAP_HDR + n->topo_len + 4; /* + trailing crc32 */
    uint8_t *buf = malloc(head_len);
    if (!buf) return -1;
    put_be32(buf, CL_SNAP_MAGIC);
    put_be32(buf + 4, 2);
    put_be64(buf + 8, base_index);
    put_be64(buf + 16, base_term);
    put_be64(buf + 24, base_offset);
    put_be32(buf + 32, (uint32_t)n->topo_len);
    if (n->topo_len) memcpy(buf + CL_SNAP_HDR, n->topo, n->topo_len);
    put_be32(buf + CL_SNAP_HDR + n->topo_len,
             cluster_crc32(buf, CL_SNAP_HDR + n->topo_len));

    char tmp[600], dst[600];
    snprintf(tmp, sizeof tmp, "%s/cluster-%d.snap.tmp", ps->dir, ps->self_id);
    snprintf(dst, sizeof dst, "%s/cluster-%d.snap", ps->dir, ps->self_id);
    int fd = open(tmp, O_RDWR | O_CREAT | O_TRUNC, 0644);
    int ok = fd >= 0 && write_all(fd, buf, head_len) == 0;
    free(buf);

    /* v2 live-message dump: u32 count, then records. Streamed per queue so a
     * large dump never needs one giant buffer. Torn writes are impossible for
     * the reader: the file only becomes visible via the rename after fsync. */
    uint32_t count = 0;
    off_t count_at = ok ? lseek(fd, 0, SEEK_CUR) : -1;
    uint8_t cnt_buf[4] = {0};
    ok = ok && write_all(fd, cnt_buf, 4) == 0;
    if (ok && n->broker) {
        qcollect_t qs = {0};
        broker_foreach_queue(n->broker, collect_queue_cb, &qs);
        for (size_t i = 0; ok && i < qs.n; i++) {
            size_t nm = 0;
            beaver_message_t **ms = queue_snapshot_refs(qs.q[i], base_index, &nm);
            for (size_t j = 0; ok && j < nm; j++) {
                size_t rl = snap_record_size(qs.q[i], ms[j]);
                uint8_t *rb = malloc(rl);
                if (!rb) { ok = 0; break; }
                uint8_t *w = rb;
                snap_write_record(&w, qs.q[i], ms[j]);
                ok = write_all(fd, rb, (size_t)(w - rb)) == 0;
                free(rb);
                count++;
            }
            for (size_t j = 0; j < nm; j++) message_unref(ms[j]);
            free(ms);
        }
        for (size_t i = 0; i < qs.n; i++) queue_unref(qs.q[i]);
        free(qs.q);
    }
    if (ok) {                          /* patch the record count in place */
        put_be32(cnt_buf, count);
        ok = pwrite(fd, cnt_buf, 4, count_at) == 4;
    }
    if (fd >= 0) { if (ok) fsync(fd); close(fd); }
    if (!ok || rename(tmp, dst) != 0) { unlink(tmp); return -1; }
    return 0;
}

/* Parse ONE live-message record and enqueue it into the broker. Advances *p.
 * Returns 0 on success, -1 on malformed input (caller stops parsing). */
static int snap_apply_record(cluster_node_t *n, const uint8_t **p,
                             const uint8_t *end)
{
    char vh[256], qn[256], ex[256], rk[256];
    if (rd_str(p, end, vh, sizeof vh) != 0 || rd_str(p, end, qn, sizeof qn) != 0 ||
        rd_str(p, end, ex, sizeof ex) != 0 || rd_str(p, end, rk, sizeof rk) != 0)
        return -1;
    if (end - *p < 4) return -1;
    uint32_t plen = get_be32(*p); *p += 4;
    const uint8_t *props = *p;
    if ((uint64_t)(end - *p) < plen) return -1;
    *p += plen;
    if (end - *p < 4) return -1;
    uint32_t blen = get_be32(*p); *p += 4;
    const uint8_t *body = *p;
    if ((uint64_t)(end - *p) < blen) return -1;
    *p += blen;
    if (end - *p < 8) return -1;
    uint64_t cid = get_be64(*p); *p += 8;

    beaver_queue_t *q = broker_get_queue(n->broker, vh, qn);
    if (!q) {   /* topology should have declared it; declare defensively */
        broker_declare_queue(n->broker, vh, qn, 0x02 /* durable */, NULL, NULL);
        q = broker_get_queue(n->broker, vh, qn);
        if (!q) return 0;   /* skip record rather than abort the install */
    }
    beaver_message_t *m = message_new_full(ex, rk, body, blen,
                                           plen ? props : NULL, plen);
    if (m) {
        static uint64_t dbg_last;    /* TEMP: dump-order check (single queue) */
        if (cid < dbg_last)
            LOG_WARN("DBG snap record out of order: %" PRIu64 " after %" PRIu64,
                     cid, dbg_last);
        dbg_last = cid;
        m->cluster_id = cid;
        queue_enqueue(q, m);
        message_unref(m);
    }
    queue_unref(q);
    return 0;
}

/* Read the snapshot at startup. On success fills base index/term/offset, leaves
 * the topology ops in n->topo (replayed by persist_load), and re-enqueues any
 * v2 live-message dump into the broker. 0 if a valid snapshot was loaded. */
static int snapshot_load(cluster_node_t *n, uint64_t *bi, uint64_t *bt, uint64_t *bo)
{
    cl_persist_t *ps = n->persist;
    if (!ps || ps->snap_fd < 0) return -1;
    off_t sz = lseek(ps->snap_fd, 0, SEEK_END);
    if (sz < CL_SNAP_HDR + 4) return -1;
    uint8_t *buf = mmap(NULL, (size_t)sz, PROT_READ, MAP_PRIVATE, ps->snap_fd, 0);
    if (buf == MAP_FAILED) return -1;
    int rc = -1;
    if (get_be32(buf) == CL_SNAP_MAGIC) {
        uint32_t topo_len = get_be32(buf + 32);
        size_t   crc_end  = CL_SNAP_HDR + (size_t)topo_len + 4;
        if (crc_end <= (size_t)sz &&
            cluster_crc32(buf, CL_SNAP_HDR + topo_len) ==
                get_be32(buf + CL_SNAP_HDR + topo_len)) {
            *bi = get_be64(buf + 8);
            *bt = get_be64(buf + 16);
            *bo = get_be64(buf + 24);
            if (topo_len) {  /* carry the ops forward (re-snapshotted next time) */
                uint8_t *nb = realloc(n->topo, topo_len);
                if (nb) { memcpy(nb, buf + CL_SNAP_HDR, topo_len);
                          n->topo = nb; n->topo_len = n->topo_cap = topo_len; }
            }
            rc = 0;
            /* v2 live-message dump (absent in v1 files). Applied AFTER the
             * caller replays the topology - but the caller replays topo after
             * we return, so records that need a queue declare it defensively. */
            if ((size_t)sz >= crc_end + 4) {
                const uint8_t *p = buf + crc_end, *end = buf + sz;
                uint32_t count = get_be32(p); p += 4;
                uint32_t done = 0;
                while (done < count && p < end &&
                       snap_apply_record(n, &p, end) == 0)
                    done++;
                if (done)
                    LOG_INFO("cluster: snapshot restored %u live messages", done);
            }
        }
    }
    munmap(buf, (size_t)sz);
    return rc;
}

/* Drop the on-disk prefix below the new base: rebase the offsets array and
 * punch a hole over the now-dead bytes [0, base_offset) so the blocks return to
 * the filesystem. The file keeps its logical size (sparse); recovery skips to
 * base_offset (stored in the snapshot). */
static void persist_compact(cluster_node_t *n, uint64_t new_base,
                            uint64_t new_base_offset)
{
    cl_persist_t *ps = n->persist;
    if (!ps || ps->log_fd < 0) return;
    persist_write_flush(n);
    uint64_t drop = new_base - ps->base_index;       /* entries removed */
    uint64_t live = (atomic_load(&n->log.last_index)) - new_base; /* survivors */
    if (ps->offsets && drop < ps->off_cap) {
        memmove(ps->offsets, ps->offsets + drop, (size_t)live * sizeof(*ps->offsets));
    }
    ps->base_index  = new_base;
    ps->base_offset = new_base_offset;
#if defined(FALLOC_FL_PUNCH_HOLE) && defined(FALLOC_FL_KEEP_SIZE)
    if (new_base_offset > 0)
        (void)fallocate(ps->log_fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                        0, (off_t)new_base_offset);
#endif
    ps->dirty = 1;
}

/* Orchestrate one compaction step: drop log entries (base_index, upto], clamped
 * so we never drop an un-applied, un-replicated, or still-un-consumed entry.
 * Runs on the cluster loop only. `upto` is the leader's all_replicated_index. */
static void log_compact_to(cluster_node_t *n, uint64_t upto)
{
    cluster_log_t *log = &n->log;
    if (upto > log->applied_index) upto = log->applied_index;  /* only applied */
    /* Never drop a still-live (un-consumed) message: keep everything from the
     * oldest live cluster_id onward. */
    if (n->broker) {
        uint64_t live = broker_min_live_cluster_id(n->broker);
        if (live != UINT64_MAX && upto >= live)
            upto = live ? live - 1 : 0;
    }
    if (upto <= log->base_index)
        return;
    if (upto - log->base_index < 4096)   /* batch: avoid churning tiny prefixes */
        return;

    uint64_t new_base_term = log_term_at(log, upto);
    cl_persist_t *ps = n->persist;
    /* File offset where the FIRST survivor (upto+1) begins. */
    uint64_t new_base_offset = ps ? ps->log_size : 0;
    if (ps && ps->offsets) {
        uint64_t slot = upto - ps->base_index; /* offset of index upto+1 */
        if (slot < ps->off_cap) new_base_offset = ps->offsets[slot];
    }
    /* 1) Persist the boundary + topology FIRST (so a crash mid-compaction just
     *    replays from the old log; the snapshot is only adopted once renamed). */
    if (snapshot_write(n, upto, new_base_term, new_base_offset) != 0)
        return;
    /* 2) Free the dropped entries' RAM (metadata + any remaining bodies) and
     *    shift the survivors to the front of the array. */
    uint64_t drop = upto - log->base_index;
    uint64_t last = atomic_load(&log->last_index);
    for (uint64_t i = log->base_index + 1; i <= upto; i++)
        free(log->entries[i - log->base_index - 1].payload);
    uint64_t survivors = last - upto;
    if (survivors)
        memmove(log->entries, log->entries + drop,
                (size_t)survivors * sizeof(*log->entries));
    log->base_index = upto;
    log->base_term  = new_base_term;
    if (log->compacted_to < upto) log->compacted_to = upto;
    /* 3) Reclaim the disk prefix. */
    if (ps) persist_compact(n, upto, new_base_offset);
    LOG_INFO("cluster: compacted log prefix up to index %" PRIu64
             " (%" PRIu64 " entries dropped, %" PRIu64 " live)",
             upto, drop, survivors);
}

static void persist_load(cluster_node_t *n)
{
    cl_persist_t *ps = n->persist;
    if (!ps) return;
    uint8_t mb[CL_META_SIZE];
    if (ps->meta_fd >= 0 &&
        pread(ps->meta_fd, mb, CL_META_SIZE, 0) == (ssize_t)CL_META_SIZE &&
        get_be32(mb) == CL_META_MAGIC) {
        atomic_store(&n->current_term, get_be64(mb + 8));
        n->voted_for = (int)(int32_t)get_be32(mb + 16);
    }
    if (ps->log_fd < 0) return;

    /* A snapshot, if present, defines the compaction boundary: the log file's
     * surviving records start at base_offset and at index base_index+1. The
     * snapshot's topology ops rebuild the queues/exchanges that the dropped
     * prefix had declared, so the tail's publishes route correctly. */
    uint64_t base_index = 0, base_term = 0, base_offset = 0;
    if (snapshot_load(n, &base_index, &base_term, &base_offset) == 0) {
        n->log.base_index   = base_index;
        n->log.base_term    = base_term;
        n->log.applied_index = base_index;  /* prefix was applied before drop */
        n->log.compacted_to  = base_index;
        ps->base_index  = base_index;
        ps->base_offset = base_offset;
        /* Rebuild topology from the snapshot (broker is already attached). */
        for (size_t i = 0; i + 8 <= n->topo_len; ) {
            uint32_t op = get_be32(n->topo + i), len = get_be32(n->topo + i + 4);
            if (i + 8 + len > n->topo_len) break;
            cluster_log_entry_t te = { .term = base_term, .index = base_index,
                                       .op_type = op, .len = len,
                                       .payload = n->topo + i + 8 };
            apply_op(n, &te);
            i += 8 + len;
        }
        LOG_INFO("cluster: loaded snapshot (base index %" PRIu64 ", term %" PRIu64
                 ", %zu topology bytes)", base_index, base_term, n->topo_len);
    }

    ps->log_size = base_offset;          /* surviving records begin here */
    uint64_t loaded = 0;                 /* count of records read past the base */
    for (;;) {
        uint8_t hdr[CL_REC_HDR];
        if (pread(ps->log_fd, hdr, CL_REC_HDR, (off_t)ps->log_size) != (ssize_t)CL_REC_HDR)
            break; /* EOF or torn header */
        uint64_t term = get_be64(hdr), index = get_be64(hdr + 8);
        uint32_t op = get_be32(hdr + 16), len = get_be32(hdr + 20);
        if (len > CLUSTER_MAX_PAYLOAD || index != base_index + loaded + 1)
            break; /* corrupt / out-of-order: stop and trim from here */
        uint8_t *payload = NULL;
        if (len) {
            payload = malloc(len);
            if (!payload) break;
            if (pread(ps->log_fd, payload, len,
                      (off_t)(ps->log_size + CL_REC_HDR)) != (ssize_t)len) {
                free(payload); break;
            }
        }
        uint64_t slot = index - base_index; /* 1-based count past the base */
        if (log_ensure_cap(&n->log, slot) != 0 || off_ensure(ps, slot) != 0) {
            free(payload); break;
        }
        cluster_log_entry_t *e = &n->log.entries[slot - 1];
        e->term = term; e->index = index; e->op_type = op; e->len = len;
        e->payload = payload;
        ps->offsets[slot - 1] = ps->log_size;
        ps->log_size += CL_REC_HDR + len;
        loaded = index - base_index;
    }
    atomic_store(&n->log.last_index, base_index + loaded);
    n->log.durable_index = base_index + loaded;  /* all recovered is on disk */
    if (ftruncate(ps->log_fd, (off_t)ps->log_size) != 0) /* drop torn tail */
        LOG_WARN("cluster: could not trim recovered log tail");
    ps->write_pos  = ps->log_size;   /* everything loaded is already on disk */
    ps->alloc_size = ps->log_size;
    if (loaded)
        LOG_INFO("cluster: recovered %" PRIu64 " log entries from disk (term %" PRIu64 ")",
                 loaded, (uint64_t)atomic_load(&n->current_term));
}

static void persist_init(cluster_node_t *n, const char *dir, int self_id)
{
    if (!dir || !dir[0]) return; /* persistence disabled */
    if (mkdir(dir, 0755) != 0 && errno != EEXIST) {
        LOG_ERROR("cluster: cannot create data_dir '%s' (%s); persistence off",
                  dir, strerror(errno));
        return;
    }
    cl_persist_t *ps = calloc(1, sizeof(*ps));
    if (!ps) return;
    ps->log_fd = ps->meta_fd = ps->snap_fd = -1;
    snprintf(ps->dir, sizeof ps->dir, "%s", dir);
    ps->self_id = self_id;
    char path[512];
    snprintf(path, sizeof path, "%s/cluster-%d.log", dir, self_id);
    ps->log_fd = open(path, O_RDWR | O_CREAT, 0644);
    snprintf(path, sizeof path, "%s/cluster-%d.meta", dir, self_id);
    ps->meta_fd = open(path, O_RDWR | O_CREAT, 0644);
    snprintf(path, sizeof path, "%s/cluster-%d.snap", dir, self_id);
    ps->snap_fd = open(path, O_RDWR | O_CREAT, 0644);
    if (ps->log_fd < 0 || ps->meta_fd < 0) {
        LOG_ERROR("cluster: cannot open persistence files in '%s' (%s)",
                  dir, strerror(errno));
        if (ps->log_fd >= 0) close(ps->log_fd);
        if (ps->meta_fd >= 0) close(ps->meta_fd);
        free(ps);
        return;
    }
    n->persist = ps;
    persist_load(n);
}

static void persist_free(cluster_node_t *n)
{
    cl_persist_t *ps = n->persist;
    if (!ps) return;
    if (ps->log_fd >= 0) close(ps->log_fd);
    if (ps->meta_fd >= 0) close(ps->meta_fd);
    if (ps->snap_fd >= 0) close(ps->snap_fd);
    free(ps->offsets);
    free(ps->wbuf);
    free(ps);
    n->persist = NULL;
}

/* ---- replicated log (growable array; log index is 1-based) --------------- */

typedef struct cl_proposal {
    struct cl_proposal *next;
    uint32_t op_type;
    uint32_t len;
    uint8_t  data[];
} cl_proposal_t;

static int log_ensure_cap(cluster_log_t *log, uint64_t need)
{
    if (need <= log->cap)
        return 0;
    uint32_t nc = log->cap ? log->cap : 16;
    while (nc < need)
        nc *= 2;
    cluster_log_entry_t *e = realloc(log->entries, (size_t)nc * sizeof(*e));
    if (!e)
        return -1;
    log->entries = e;
    log->cap = nc;
    return 0;
}

/* In-RAM entries hold indices (base_index, last_index]; slot = idx-base_index-1. */
static cluster_log_entry_t *log_at(cluster_log_t *log, uint64_t idx)
{
    if (idx <= log->base_index || idx > atomic_load(&log->last_index))
        return NULL;
    return &log->entries[idx - log->base_index - 1];
}

static uint64_t log_term_at(cluster_log_t *log, uint64_t idx)
{
    if (idx == log->base_index)         /* the compaction boundary (log-matching) */
        return log->base_term;
    cluster_log_entry_t *e = log_at(log, idx);
    return e ? e->term : 0;
}

static void log_truncate_from(cluster_node_t *n, uint64_t idx)
{
    cluster_log_t *log = &n->log;
    uint64_t last = atomic_load(&log->last_index);
    if (idx <= log->base_index)         /* never truncate into the compacted prefix */
        idx = log->base_index + 1;
    if (idx > last)
        return;
    for (uint64_t i = idx; i <= last; i++)
        free(log->entries[i - log->base_index - 1].payload);
    persist_truncate(n, idx);
    atomic_store(&log->last_index, idx - 1);
}

static int log_append(cluster_node_t *n, uint64_t term, uint32_t op,
                      const void *data, uint32_t len)
{
    cluster_log_t *log = &n->log;
    uint64_t last = atomic_load(&log->last_index);
    if (log_ensure_cap(log, (last + 1) - log->base_index) != 0)
        return -1;
    void *copy = NULL;
    if (len) {
        copy = malloc(len);
        if (!copy)
            return -1;
        memcpy(copy, data, len);
    }
    cluster_log_entry_t *e = &log->entries[last - log->base_index];
    e->term = term; e->index = last + 1; e->op_type = op; e->len = len;
    e->payload = copy;
    persist_append(n, e);                     /* write to disk (synced by a later batch persist_sync) */
    atomic_store(&log->last_index, last + 1); /* release: publish to readers */
    return 0;
}

/* ---- op (de)serialization (private wire format) -------------------------- */

static void wr8(uint8_t **p, uint8_t v)   { *(*p)++ = v; }
static void wr16(uint8_t **p, uint16_t v) { (*p)[0]=(uint8_t)(v>>8); (*p)[1]=(uint8_t)v; *p += 2; }
static void wr32(uint8_t **p, uint32_t v) { put_be32(*p, v); *p += 4; }
static void wr_str(uint8_t **p, const char *s)
{
    uint16_t n = (uint16_t)strlen(s);
    wr16(p, n);
    memcpy(*p, s, n);
    *p += n;
}

static uint8_t rd8(const uint8_t **p, const uint8_t *end)
{
    return (*p < end) ? *(*p)++ : 0;
}
static uint16_t rd16(const uint8_t **p, const uint8_t *end)
{
    if (*p + 2 > end) { *p = end; return 0; }
    uint16_t v = (uint16_t)((*p)[0] << 8 | (*p)[1]);
    *p += 2;
    return v;
}
static uint32_t rd32(const uint8_t **p, const uint8_t *end)
{
    if (*p + 4 > end) { *p = end; return 0; }
    uint32_t v = get_be32(*p);
    *p += 4;
    return v;
}
static int rd_str(const uint8_t **p, const uint8_t *end, char *out, size_t outsz)
{
    uint16_t n = rd16(p, end);
    if (*p + n > end || n >= outsz)
        return -1;
    memcpy(out, *p, n);
    out[n] = '\0';
    *p += n;
    return 0;
}

/* Apply one committed op to the local broker (runs on the cluster loop; the
 * broker is internally thread-safe). Declares are idempotent, so re-apply on a
 * node that already has the object is harmless. */
static void apply_op(cluster_node_t *n, const cluster_log_entry_t *e)
{
    if (!n->broker)
        return;
    const uint8_t *p = e->payload, *end = p + e->len;
    char vh[256], a[256], b[256], c[256];
    switch (e->op_type) {
    case CL_OP_DECLARE_QUEUE: {
        uint8_t flags = rd8(&p, end);
        if (rd_str(&p, end, vh, sizeof vh) == 0 &&
            rd_str(&p, end, a, sizeof a) == 0)
            broker_declare_queue(n->broker, vh, a, flags, NULL, NULL);
        break;
    }
    case CL_OP_DECLARE_EXCH: {
        uint8_t type  = rd8(&p, end);
        uint8_t flags = rd8(&p, end);
        if (rd_str(&p, end, vh, sizeof vh) == 0 &&
            rd_str(&p, end, a, sizeof a) == 0)
            broker_declare_exchange(n->broker, vh, a, (exchange_type_t)type,
                                    flags, NULL);
        break;
    }
    case CL_OP_BIND:
        if (rd_str(&p, end, vh, sizeof vh) == 0 &&
            rd_str(&p, end, a, sizeof a) == 0 &&
            rd_str(&p, end, b, sizeof b) == 0 &&
            rd_str(&p, end, c, sizeof c) == 0)
            broker_bind(n->broker, vh, a, b, c);
        break;
    case CL_OP_PUBLISH: {
        char ex[256], rk[256];
        if (rd_str(&p, end, vh, sizeof vh) != 0 ||
            rd_str(&p, end, ex, sizeof ex) != 0 ||
            rd_str(&p, end, rk, sizeof rk) != 0 || end - p < 4)
            break;
        uint32_t pl = get_be32(p); p += 4;
        const uint8_t *props = p;
        if ((uint64_t)(end - p) < pl) break;
        p += pl;
        if (end - p < 4) break;
        uint32_t bl = get_be32(p); p += 4;
        const uint8_t *body = p;
        if ((uint64_t)(end - p) < bl) break;
        beaver_message_t *m = message_new_full(ex, rk, body, bl, pl ? props : NULL, pl);
        if (m) {
            m->cluster_id = e->index;   /* cluster-wide identity = log index */
            broker_route(n->broker, vh, m);
            message_unref(m);
        }
        break;
    }
    case CL_OP_ACK: {
        /* Consume watermark: drop replica copies of everything consumed. */
        char qn[256];
        if (rd_str(&p, end, vh, sizeof vh) == 0 &&
            rd_str(&p, end, qn, sizeof qn) == 0 && end - p >= 8) {
            uint64_t wm = get_be64(p);
            beaver_queue_t *q = broker_get_queue(n->broker, vh, qn);
            if (q) { queue_drain_consumed(q, wm); queue_unref(q); }
        }
        break;
    }
    /* ---- access-control config (applied to the authstore) ---------------- */
    case CL_OP_ADD_VHOST:
        if (n->authstore && rd_str(&p, end, a, sizeof a) == 0) {
            authstore_add_vhost(n->authstore, a);
            /* A vhost carries its own standard exchange set. */
            broker_declare_default_exchanges(n->broker, a);
        }
        break;
    case CL_OP_DEL_VHOST:
        if (n->authstore && rd_str(&p, end, a, sizeof a) == 0)
            authstore_del_vhost(n->authstore, a);
        break;
    case CL_OP_ADD_USER: {
        char hash[256];
        if (n->authstore && rd_str(&p, end, a, sizeof a) == 0 &&
            rd_str(&p, end, hash, sizeof hash) == 0) {
            uint32_t tags = rd32(&p, end);
            authstore_add_user(n->authstore, a, hash, tags);
        }
        break;
    }
    case CL_OP_DEL_USER:
        if (n->authstore && rd_str(&p, end, a, sizeof a) == 0)
            authstore_del_user(n->authstore, a);
        break;
    case CL_OP_SET_PERM: {
        char cfg[256], wr[256], rd[256];
        if (n->authstore && rd_str(&p, end, a, sizeof a) == 0 &&
            rd_str(&p, end, b, sizeof b) == 0 &&
            rd_str(&p, end, cfg, sizeof cfg) == 0 &&
            rd_str(&p, end, wr, sizeof wr) == 0 &&
            rd_str(&p, end, rd, sizeof rd) == 0)
            authstore_set_perm(n->authstore, a, b, cfg, wr, rd);
        break;
    }
    case CL_OP_CLEAR_PERM:
        if (n->authstore && rd_str(&p, end, a, sizeof a) == 0 &&
            rd_str(&p, end, b, sizeof b) == 0)
            authstore_clear_perm(n->authstore, a, b);
        break;
    default:
        break;
    }
    /* Topology AND access-control config must survive log compaction (they
     * rebuild the broker/authstore on recovery even after the declaring entry is
     * dropped): carry them in the snapshot set. */
    if (e->op_type == CL_OP_DECLARE_QUEUE || e->op_type == CL_OP_DECLARE_EXCH ||
        e->op_type == CL_OP_BIND ||
        (e->op_type >= CL_OP_ADD_VHOST && e->op_type <= CL_OP_CLEAR_PERM))
        topo_add(n, e->op_type, e->payload, e->len);
    if (e->op_type == CL_OP_PUBLISH || e->op_type == CL_OP_ACK)
        LOG_DEBUG("cluster: applied op %u at index %" PRIu64, e->op_type, e->index);
    else
        LOG_INFO("cluster: applied op %u at index %" PRIu64, e->op_type, e->index);
}

static void apply_committed(cluster_node_t *n)
{
    uint64_t commit = atomic_load(&n->log.commit_index);
    while (n->log.applied_index < commit) {
        uint64_t idx = n->log.applied_index + 1;
        cluster_log_entry_t *e = log_at(&n->log, idx);
        if (e)
            apply_op(n, e);
        n->log.applied_index = idx;
    }
}

/* ---- log compaction (bound RAM) ----------------------------------------- *
 * Once an entry is committed, applied, AND replicated to EVERY node, its
 * message body is dead weight in RAM (the broker queue is the live copy and the
 * file holds the durable copy). Free those bodies. We keep the lightweight entry
 * metadata (term/index) so log-matching and recovery (from the untouched file)
 * still work; only the heap payloads are released. */

/* Highest index replicated on ALL nodes = min over self+peers of match. (The
 * leader itself holds everything, so this is effectively min(peer match).) */
/* Highest index FSYNC'd on EVERY node = min(self durable, each peer durable).
 * This - not all_replicated_index (which counts page-cache acks) - is the only
 * safe ceiling for log compaction: a peer that crashed after acking but before
 * its async fsync recovers BELOW its match, and if we had compacted past that
 * point we could never catch it up (the "below compaction point" storm). */
static uint64_t all_durable_index(cluster_node_t *n)
{
    uint64_t m = n->log.durable_index;
    for (int i = 0; i < n->npeers; i++)
        if (n->peers[i].durable_index < m)
            m = n->peers[i].durable_index;
    return m;
}

/* Free RAM bodies of entries (compacted_to, upto], clamped to what is committed
 * (hence applied). Safe: these entries are on every node and never resent. */
static void compact_payloads(cluster_node_t *n, uint64_t upto)
{
    cluster_log_t *log = &n->log;
    uint64_t commit = atomic_load(&log->commit_index);
    if (upto > commit)
        upto = commit;
    for (uint64_t i = log->compacted_to + 1; i <= upto; i++) {
        cluster_log_entry_t *e = log_at(log, i);
        if (e && e->payload) { free(e->payload); e->payload = NULL; }
    }
    if (upto > log->compacted_to)
        log->compacted_to = upto;
}

/* Leader: advance commit_index to the highest N replicated on a majority, but
 * only for an entry from the current term (Raft §5.4.2 - prevents committing a
 * stale entry that a later leader could overwrite). */
static void advance_commit(cluster_node_t *n)
{
    /* The commit index is the highest log index replicated on a majority: gather
     * {self last_index, each peer match_index}, sort descending, and take the
     * quorum-th value. Computed directly (O(nodes^2), nodes<=5) - NOT by scanning
     * the log backwards, which would be O(backlog) per call (quadratic overall). */
    uint64_t v[CLUSTER_MAX_NODES];
    int m = 0;
    v[m++] = atomic_load(&n->log.last_index); /* self has every appended entry */
    for (int i = 0; i < n->npeers; i++)
        v[m++] = n->peers[i].match_index;
    for (int i = 0; i < m; i++)
        for (int j = i + 1; j < m; j++)
            if (v[j] > v[i]) { uint64_t t = v[i]; v[i] = v[j]; v[j] = t; }

    uint64_t N = v[n->quorum - 1];            /* majority-replicated high-water */
    /* Raft §5.4.2: only commit an entry from the current term directly. */
    if (N > atomic_load(&n->log.commit_index) &&
        log_term_at(&n->log, N) == atomic_load(&n->current_term)) {
        atomic_store(&n->log.commit_index, N);
        apply_committed(n);
    }
    /* Drop RAM bodies that are durable on every node (leader-driven). Bounded by
     * the fsync'd index, not the page-cache ack, so a crashed peer can still be
     * re-sent any body it loses before its fsync. */
    compact_payloads(n, all_durable_index(n));
}

static void send_append(cluster_node_t *n, cluster_peer_t *p);

/* Sliding-window pipelining: keep up to this many sent-but-unacknowledged
 * entries in flight per peer, so the leader never stalls a full loop-RTT
 * between batches (critical on a LAN, where RTT >> localhost). */
/* ---- state transfer (InstallSnapshot) ------------------------------------ *
 * A peer whose next_index falls at or below the compaction point cannot be
 * served from the log (the prefix is gone). Instead the leader re-seeds it:
 *   SNAP_BEGIN  boundary (base index/term) + the topology/config blob
 *   SNAP_CHUNK  live queued messages with cluster_id <= base (capped frames,
 *               flow-controlled by per-chunk SNAP_ACKs, window of 4)
 *   SNAP_END    -> follower persists the snapshot and acks final
 * then normal AppendEntries stream the log tail from base+1. Correctness rests
 * on the compaction invariant: every dropped PUBLISH entry was consumed
 * everywhere, so "live queue contents + tail replay" IS the full state. */
#define CL_SNAP_WINDOW      4u
#define CL_SNAP_CHUNK_BYTES (256u * 1024u)

static void snap_reset(cluster_peer_t *p)
{
    beaver_message_t **ms = p->snap.msgs;
    for (size_t j = p->snap.mi; j < p->snap.nmsgs; j++) message_unref(ms[j]);
    free(ms);
    beaver_queue_t **qs = p->snap.queues;
    for (size_t i = p->snap.qi; i < p->snap.nqueues; i++) queue_unref(qs[i]);
    free(qs);
    memset(&p->snap, 0, sizeof(p->snap));
}

static void snap_start(cluster_node_t *n, cluster_peer_t *p)
{
    snap_reset(p);
    p->snap.base      = n->log.compacted_to;
    p->snap.base_term = log_term_at(&n->log, p->snap.base);
    qcollect_t qs = {0};
    if (n->broker)
        broker_foreach_queue(n->broker, collect_queue_cb, &qs);
    p->snap.queues  = qs.q;
    p->snap.nqueues = qs.n;
    p->snap.active  = 1;

    size_t blen = 8 + 8 + 4 + n->topo_len;
    uint8_t *body = malloc(blen);
    if (!body) { snap_reset(p); return; }
    uint8_t *w = body;
    put_be64(w, p->snap.base);      w += 8;
    put_be64(w, p->snap.base_term); w += 8;
    put_be32(w, (uint32_t)n->topo_len); w += 4;
    if (n->topo_len) { memcpy(w, n->topo, n->topo_len); w += n->topo_len; }
    int rc = cl_send(p->link, CL_FRAME_SNAP_BEGIN, 0,
                     (uint32_t)atomic_load(&n->current_term), body,
                     (uint32_t)(w - body));
    free(body);
    if (rc != 0) { snap_reset(p); return; }
    LOG_INFO("cluster: re-seeding peer %d via state transfer "
             "(boundary %" PRIu64 ")", p->node_id, p->snap.base);
}

/* Send up to the window's worth of CHUNK frames; when the dump is exhausted,
 * send END. Driven by snap_start and by each SNAP_ACK. */
static void snap_pump(cluster_node_t *n, cluster_peer_t *p)
{
    while (p->snap.active && p->snap.unacked_chunks < CL_SNAP_WINDOW) {
        /* Ensure a current message array (load the next queue when needed). */
        while (p->snap.mi >= p->snap.nmsgs && p->snap.qi < p->snap.nqueues) {
            free(p->snap.msgs);
            p->snap.msgs = NULL; p->snap.nmsgs = p->snap.mi = 0;
            beaver_queue_t **qs = p->snap.queues;
            beaver_queue_t *q = qs[p->snap.qi];
            size_t nm = 0;
            p->snap.msgs  = queue_snapshot_refs(q, p->snap.base, &nm);
            p->snap.nmsgs = nm;
            if (nm == 0) {                 /* empty queue: move to the next one */
                queue_unref(q);
                p->snap.qi++;
            }
        }
        if (p->snap.mi >= p->snap.nmsgs && p->snap.qi >= p->snap.nqueues) {
            /* Dump exhausted: close the stream - exactly ONCE (late chunk acks
             * re-enter here; a duplicate END would re-persist + re-ack). */
            if (!p->snap.end_sent) {
                p->snap.end_sent = 1;
                cl_send(p->link, CL_FRAME_SNAP_END, 0,
                        (uint32_t)atomic_load(&n->current_term), NULL, 0);
            }
            return;
        }

        /* Build one capped chunk from the current queue's array. */
        beaver_queue_t *q = ((beaver_queue_t **)p->snap.queues)[p->snap.qi];
        beaver_message_t **ms = p->snap.msgs;
        size_t   sz = 4;
        uint32_t nrec = 0;
        size_t   j = p->snap.mi;
        while (j < p->snap.nmsgs &&
               (nrec == 0 || sz + snap_record_size(q, ms[j]) <= CL_SNAP_CHUNK_BYTES)) {
            sz += snap_record_size(q, ms[j]);
            nrec++; j++;
        }
        uint8_t *body = malloc(sz);
        if (!body) return;             /* retry on the next ack */
        uint8_t *w = body;
        put_be32(w, nrec); w += 4;
        for (size_t k = p->snap.mi; k < j; k++)
            snap_write_record(&w, q, ms[k]);
        int rc = cl_send(p->link, CL_FRAME_SNAP_CHUNK, 0,
                         (uint32_t)atomic_load(&n->current_term), body,
                         (uint32_t)(w - body));
        free(body);
        if (rc != 0) return;           /* link trouble: reset happens on close */
        for (size_t k = p->snap.mi; k < j; k++) message_unref(ms[k]);
        p->snap.mi = j;
        p->snap.unacked_chunks++;
        if (p->snap.mi >= p->snap.nmsgs && p->snap.qi < p->snap.nqueues) {
            queue_unref(((beaver_queue_t **)p->snap.queues)[p->snap.qi]);
            p->snap.qi++;              /* next queue on the following iteration */
            p->snap.nmsgs = p->snap.mi = 0;   /* force a reload at the loop top */
            free(p->snap.msgs);
            p->snap.msgs = NULL;
        }
    }
}

#define CL_WINDOW_ENTRIES 8192u

/* Push as many capped AppendEntries batches to a peer as the window allows,
 * advancing next_index as we send (acks later advance match_index). Leader-only;
 * a no-op for a peer that is down or whose window is already full. */
static void pump_peer(cluster_node_t *n, cluster_peer_t *p)
{
    if (atomic_load(&n->role) != CL_ROLE_LEADER ||
        atomic_load(&p->link_state) != CL_LINK_UP)
        return;
    if (p->snap.active) {              /* state transfer in progress */
        snap_pump(n, p);
        return;
    }
    if (n->log.compacted_to > 0 && p->next_index <= n->log.compacted_to) {
        /* The peer needs entries the log no longer holds: re-seed it. */
        snap_start(n, p);
        snap_pump(n, p);
        return;
    }
    uint64_t last = atomic_load(&n->log.last_index);
    while (p->next_index <= last && p->inflight < CL_WINDOW_ENTRIES) {
        uint64_t before = p->next_index;
        send_append(n, p);              /* advances next_index + inflight by batch */
        if (p->next_index == before)
            break;                      /* nothing sent (alloc fail): avoid spin */
    }
}

static void replicate_to_peers(cluster_node_t *n)
{
    for (int i = 0; i < n->npeers; i++)
        pump_peer(n, &n->peers[i]);
}

/* A peer's link just came up: (re)start replication from the end of our log. We
 * reset match to 0 because a pre-disconnect match can't be trusted - across a
 * restart the peer may have lost an un-fsync'd tail. If it is actually behind,
 * its first NACK hint rewinds next_index in one jump. Leader-only via pump. */
static void peer_on_link_up(cluster_node_t *n, cluster_peer_t *p)
{
    p->next_index    = atomic_load(&n->log.last_index) + 1;
    p->match_index   = 0;
    p->durable_index = 0;   /* unknown until the peer reports (gates compaction) */
    p->inflight      = 0;
    snap_reset(p);          /* a broken transfer restarts from scratch */
    pump_peer(n, p);
}

/* Leader: append one op to the log WITHOUT syncing/replicating yet (batched). */
static void leader_append(cluster_node_t *n, uint32_t op,
                          const uint8_t *data, uint32_t len)
{
    if (atomic_load(&n->role) != CL_ROLE_LEADER)
        return;
    if (log_append(n, atomic_load(&n->current_term), op, data, len) == 0)
        n->log_dirty = 1;
}

/* Flow control SAFETY VALVE: async persistence raises the sustainable rate, but
 * a producer can still outrun it indefinitely - so when the leader's replication
 * backlog (appended-but-not-committed) grows very large we set a throttle flag
 * (hysteresis) to pause producers and bound memory. It is NOT an everyday rate
 * cap: the thresholds are high enough that normal load never trips it; it only
 * prevents the unbounded-backlog OOM. Broadcast to followers via AppendEntries;
 * the data plane reads it through cluster_should_throttle(). */
#define CL_FLOW_HIGH 16000u
#define CL_FLOW_LOW  6000u

static void leader_update_flow(cluster_node_t *n)
{
    uint64_t backlog = atomic_load(&n->log.last_index) -
                       atomic_load(&n->log.commit_index);
    int cur = atomic_load(&n->throttle);
    if (!cur && backlog > CL_FLOW_HIGH)
        atomic_store(&n->throttle, 1);
    else if (cur && backlog < CL_FLOW_LOW)
        atomic_store(&n->throttle, 0);
}

/* Leader: flush a batch of appends - ONE fsync, ONE replication round, ONE
 * commit scan. Amortizing these across many messages is what makes durable
 * replication fast instead of fsync-per-message slow. */
static void leader_flush(cluster_node_t *n)
{
    if (!n->log_dirty)
        return;
    n->log_dirty = 0;
    persist_write_flush(n);  /* push this batch's bytes to the page cache (1 write) */
    replicate_to_peers(n);   /* commit is gated on replication, not fsync */
    advance_commit(n);
    leader_update_flow(n);
}

/* Recompute the data-plane health gate. A leader is healthy only while it can
 * still reach a majority (self + UP peers >= quorum); a follower is healthy
 * while it knows a leader. Everything else (candidate / isolated) is unhealthy,
 * which makes the data plane reject Basic.Publish (Scenario B). */
static void update_health(cluster_node_t *n)
{
    int reachable = 1; /* self */
    for (int i = 0; i < n->npeers; i++)
        if (atomic_load(&n->peers[i].link_state) == CL_LINK_UP)
            reachable++;
    int role = atomic_load(&n->role);
    int healthy;
    if (role == CL_ROLE_LEADER)
        healthy = reachable >= n->quorum;
    else if (role == CL_ROLE_FOLLOWER && atomic_load(&n->leader_id) >= 0)
        healthy = 1;
    else
        healthy = 0;
    atomic_store(&n->health, healthy ? CL_HEALTH_OK : CL_HEALTH_ISOLATED);
}

static void arm_election_timer(cluster_node_t *n)
{
    uv_timer_start(&n->election_timer, election_timer_cb, election_timeout_ms(n), 0);
}

/* Broadcast a vote request to every connected peer. `prevote` selects PreVote
 * (term not yet incremented - speculative) vs the real RequestVote. */
static void send_vote_request(cluster_node_t *n, int prevote)
{
    uint64_t cur = atomic_load(&n->current_term);
    uint32_t hdr_term = prevote ? (uint32_t)(cur + 1) : (uint32_t)cur;
    uint64_t last = atomic_load(&n->log.last_index);
    uint8_t body[4 + 8 + 8];
    put_be32(body, (uint32_t)n->self_id);                 /* candidateId   */
    put_be64(body + 4, last);                             /* lastLogIndex  */
    put_be64(body + 12, log_term_at(&n->log, last));      /* lastLogTerm   */
    uint8_t type = prevote ? CL_FRAME_PREVOTE : CL_FRAME_VOTE_REQ;
    for (int i = 0; i < n->npeers; i++) {
        cluster_peer_t *p = &n->peers[i];
        if (atomic_load(&p->link_state) == CL_LINK_UP)
            cl_send(p->link, type, 0, hdr_term, body, sizeof(body));
    }
}

/* Is the candidate's log at least as up-to-date as ours? (Raft §5.4.1) A node
 * must refuse its vote to a candidate whose log is behind, or a committed entry
 * could be lost. Trivially true while logs are empty (phase 2a). */
static int log_is_current(cluster_node_t *n, uint64_t cand_idx, uint64_t cand_term)
{
    uint64_t my_idx  = atomic_load(&n->log.last_index);
    uint64_t my_term = log_term_at(&n->log, my_idx);
    if (cand_term != my_term)
        return cand_term > my_term;
    return cand_idx >= my_idx;
}

/* Leader -> follower AppendEntries carrying entries [next_index .. last].
 * An empty range is just a heartbeat. Payload:
 *   leaderId(u32) prevLogIndex(u64) prevLogTerm(u64) leaderCommit(u64)
 *   compactIndex(u64) entryCount(u32) then per entry:
 *   term(u64) index(u64) op(u32) len(u32) bytes */
/* Cap one AppendEntries so a far-behind follower never forces a multi-MB frame:
 * at most CL_MAX_APPEND_ENTRIES entries or CL_MAX_APPEND_BYTES of payload, then
 * the rest follow on the next ack (see the APPEND_RESP pipelining). */
#define CL_MAX_APPEND_ENTRIES 512u
#define CL_MAX_APPEND_BYTES   (1u << 20) /* 1 MiB */
#define CL_APPEND_FIXED       (4 + 8 + 8 + 8 + 8 + 4) /* fixed AppendEntries header */

static void send_append(cluster_node_t *n, cluster_peer_t *p)
{
    uint64_t last = atomic_load(&n->log.last_index);
    uint64_t ni   = p->next_index ? p->next_index : 1;

    /* A follower below our compaction point cannot be served from the log (its
     * bodies are freed and we have no snapshot transfer yet): send an empty
     * heartbeat instead of dereferencing freed payloads. Rare - only a node
     * disconnected longer than the compaction lag. */
    if (ni <= n->log.compacted_to) {
        uint64_t now = uv_now(n->loop);
        if (now - n->snap_warn_at > 10000) {  /* at most once / 10 s, not per frame */
            n->snap_warn_at = now;
            LOG_INFO("cluster: peer %d is below compaction point %" PRIu64
                     " - re-seeding it via state transfer", p->node_id,
                     n->log.compacted_to);
        }
        last = ni - 1; /* forces cnt = 0 (heartbeat only) */
    }
    uint64_t prev = ni - 1;

    /* Decide how many entries fit under both caps. */
    size_t   size = CL_APPEND_FIXED;
    uint32_t cnt  = 0;
    for (uint64_t i = ni; i <= last && cnt < CL_MAX_APPEND_ENTRIES; i++) {
        cluster_log_entry_t *e = log_at(&n->log, i);
        size_t esz = 8 + 8 + 4 + 4 + (e ? e->len : 0);
        if (cnt > 0 && size + esz > CL_MAX_APPEND_BYTES)
            break;
        size += esz;
        cnt++;
    }
    uint8_t *body = malloc(size);
    if (!body)
        return;
    uint8_t *w = body;
    put_be32(w, (uint32_t)n->self_id);              w += 4;
    put_be64(w, prev);                              w += 8;
    put_be64(w, log_term_at(&n->log, prev));        w += 8;
    put_be64(w, atomic_load(&n->log.commit_index)); w += 8;
    put_be64(w, all_durable_index(n));              w += 8; /* compactIndex (fsync'd everywhere) */
    put_be32(w, cnt);                               w += 4;
    for (uint64_t i = ni; i < ni + cnt; i++) {
        cluster_log_entry_t *e = log_at(&n->log, i);
        if (!e)
            continue;
        put_be64(w, e->term);    w += 8;
        put_be64(w, e->index);   w += 8;
        put_be32(w, e->op_type); w += 4;
        put_be32(w, e->len);     w += 4;
        if (e->len) { memcpy(w, e->payload, e->len); w += e->len; }
    }
    uint8_t flags = atomic_load(&n->throttle) ? 0x01u : 0u; /* congestion bit */
    cl_send(p->link, CL_FRAME_APPEND, flags,
            (uint32_t)atomic_load(&n->current_term), body, (uint32_t)(w - body));
    free(body);
    p->next_index += cnt; /* advance the window; the ack later advances match */
    p->inflight   += cnt; /* sent-but-unacked; reset on rewind, drained on ack */
}

/* A follower more than this many entries behind the leader is "catching up"
 * (SYNCING); a smaller gap is just normal pipeline lag and counts as HEALTHY. */
#define CL_SYNC_LAG 2000u

/* Leader-only: derive the admin-facing cluster state from replication progress. */
static cluster_state_t leader_cluster_state(const cluster_node_t *n)
{
    uint64_t last = atomic_load(&n->log.last_index);
    int reachable = 1; /* self */
    int caught_up = 1;
    for (int i = 0; i < n->npeers; i++) {
        if (atomic_load(&n->peers[i].link_state) != CL_LINK_UP)
            continue;
        reachable++;
        if (n->peers[i].match_index + CL_SYNC_LAG < last)
            caught_up = 0;
    }
    if (reachable < n->quorum) return CL_STATE_NO_QUORUM;
    if (reachable < n->nnodes) return CL_STATE_DEGRADED;
    return caught_up ? CL_STATE_HEALTHY : CL_STATE_SYNCING;
}

/* Leader -> all followers: broadcast each node's replication progress + the
 * derived state, so every node's status API/GUI is consistent (a follower does
 * not otherwise know its peers' match_index). */
static void send_status(cluster_node_t *n)
{
    if (atomic_load(&n->role) != CL_ROLE_LEADER)
        return;
    uint64_t last = atomic_load(&n->log.last_index);
    size_t size = 1 + 8 + 8 + 4 + (size_t)n->nnodes * (8 + 1);
    uint8_t *body = malloc(size);
    if (!body)
        return;
    uint8_t *w = body;
    *w++ = (uint8_t)leader_cluster_state(n);
    put_be64(w, last); w += 8;
    put_be64(w, atomic_load(&n->log.commit_index)); w += 8;
    put_be32(w, (uint32_t)n->nnodes); w += 4;
    for (int id = 0; id < n->nnodes; id++) {
        uint64_t match = 0; int reach = 0;
        if (id == n->self_id) { match = last; reach = 1; }
        else for (int i = 0; i < n->npeers; i++)
            if (n->peers[i].node_id == id) {
                match = n->peers[i].match_index;
                reach = atomic_load(&n->peers[i].link_state) == CL_LINK_UP;
                break;
            }
        put_be64(w, match); w += 8;
        *w++ = reach ? 1u : 0u;
    }
    for (int i = 0; i < n->npeers; i++)
        if (atomic_load(&n->peers[i].link_state) == CL_LINK_UP)
            cl_send(n->peers[i].link, CL_FRAME_STATUS, 0,
                    (uint32_t)atomic_load(&n->current_term), body, (uint32_t)(w - body));
    free(body);
}

static void send_vote_resp(cluster_node_t *n, cluster_peer_t *p, uint8_t type,
                           uint32_t term, int granted)
{
    uint8_t body[1] = { granted ? 1u : 0u };
    cl_send(p->link, type, 0, term, body, sizeof(body));
}

/* `hint` is what the leader uses to rewind next_index after a reject (and the
 * matchIndex on success). For a plain reject pass our last_index; for a term
 * CONFLICT pass an index strictly below prevLogIndex (see the receiver) so the
 * leader actually backs up past the divergence instead of resending forever. */
static void send_append_resp(cluster_node_t *n, cluster_peer_t *p, int success,
                             uint64_t hint)
{
    uint8_t body[1 + 8 + 8];
    body[0] = success ? 1u : 0u;
    put_be64(body + 1, hint);
    put_be64(body + 9, n->log.durable_index); /* our fsync'd ceiling (compaction) */
    cl_send(p->link, CL_FRAME_APPEND_RESP, 0,
            (uint32_t)atomic_load(&n->current_term), body, sizeof(body));
}

/* Adopt a strictly higher term and revert to follower (vote reset). Returns 1
 * if we stepped down. This is the Raft invariant that guarantees safety: seeing
 * any higher term immediately demotes us. */
static int observe_term(cluster_node_t *n, uint32_t term)
{
    if ((uint64_t)term <= atomic_load(&n->current_term))
        return 0;
    atomic_store(&n->current_term, term);
    n->voted_for = -1;
    n->in_prevote = 0;
    atomic_store(&n->role, CL_ROLE_FOLLOWER);
    atomic_store(&n->leader_id, -1);
    atomic_store(&n->throttle, 0); /* a new leader will re-assert congestion */
    uv_timer_stop(&n->heartbeat_timer);
    arm_election_timer(n);
    update_health(n);
    persist_meta(n); /* new term + cleared vote must be durable */
    return 1;
}

static void become_leader(cluster_node_t *n)
{
    atomic_store(&n->role, CL_ROLE_LEADER);
    atomic_store(&n->leader_id, n->self_id);
    n->in_prevote = 0;
    uv_timer_stop(&n->election_timer);
    uint64_t li = atomic_load(&n->log.last_index);
    for (int i = 0; i < n->npeers; i++) {
        n->peers[i].next_index    = li + 1;
        n->peers[i].match_index   = 0;
        n->peers[i].durable_index = 0;
        n->peers[i].inflight      = 0;
        snap_reset(&n->peers[i]);
    }
    /* Append a no-op in the new term so any prior-term entries become committed
     * (and applied) even without new client writes - matters after a restart. */
    log_append(n, atomic_load(&n->current_term), CL_OP_NOOP, NULL, 0);
    update_health(n);
    LOG_INFO("cluster: node %d became LEADER (term %" PRIu64 ")",
             n->self_id, atomic_load(&n->current_term));
    replicate_to_peers(n);  /* fill each peer's window (the no-op + any backlog) */
    uv_timer_start(&n->heartbeat_timer, heartbeat_timer_cb,
                   n->heartbeat_ms, n->heartbeat_ms);

    /* Bootstrap the default vhost '/' on a fresh cluster (through the log, so it
     * is replicated + persisted consistently). Deliberately NO default user:
     * clients (pika, perf-test) silently send guest/guest, so a guest account
     * would make the broker look unauthenticated. The operator must create the
     * first administrator explicitly (`beavermq add-user`); until then every
     * AMQP login is refused and only the first-user bootstrap works. */
    if (n->authstore && !authstore_vhost_exists(n->authstore, "/")) {
        cluster_replicate_add_vhost(n, "/");
        if (authstore_is_open(n->authstore))
            LOG_WARN("cluster: no users configured - AMQP logins are refused; "
                     "create the first admin: beavermq add-user <name> <password>");
    }
}

/* PreVote succeeded: increment the term and run a real election. */
static void become_candidate(cluster_node_t *n)
{
    n->in_prevote = 0;
    atomic_fetch_add(&n->current_term, 1);
    n->voted_for = n->self_id;
    atomic_store(&n->role, CL_ROLE_CANDIDATE);
    atomic_store(&n->leader_id, -1);
    n->votes_granted = 1; /* self */
    persist_meta(n);      /* term bump + self-vote durable before requesting */
    update_health(n);
    LOG_INFO("cluster: node %d starting election (term %" PRIu64 ")",
             n->self_id, atomic_load(&n->current_term));
    send_vote_request(n, 0);
    arm_election_timer(n);
}

/* Election timeout: run a PreVote round WITHOUT bumping the term, so a node on
 * the minority side of a partition can never disrupt a healthy leader by
 * inflating terms (this is the election-storm damping). */
static void start_prevote(cluster_node_t *n)
{
    n->in_prevote = 1;
    n->prevotes_granted = 1; /* self */
    atomic_store(&n->leader_id, -1);
    update_health(n);
    send_vote_request(n, 1);
    arm_election_timer(n);
}

/* ---- frame dispatch ------------------------------------------------------ */

static void cl_dispatch(cluster_node_t *n, cluster_peer_t *peer,
                        const cluster_frame_hdr_t *h, const uint8_t *payload)
{
    switch (h->type) {
    case CL_FRAME_HELLO:
        /* Inbound side already attached the peer (see inbound HELLO handling). */
        send_hello(n, peer->link, CL_FRAME_HELLO_ACK);
        atomic_store(&peer->link_state, CL_LINK_UP);
        update_health(n);
        peer_on_link_up(n, peer);
        LOG_INFO("cluster: peer %d link UP (inbound)", peer->node_id);
        break;
    case CL_FRAME_HELLO_ACK:
        atomic_store(&peer->link_state, CL_LINK_UP);
        update_health(n);
        peer_on_link_up(n, peer);
        LOG_INFO("cluster: peer %d link UP (outbound)", peer->node_id);
        break;

    /* ---- Raft control plane ---- */
    case CL_FRAME_PREVOTE: {
        /* PreVote must NOT change our term (it is speculative). Grant only if we
         * have not heard from a leader within an election timeout, the candidate
         * is not behind us in term, and its log is at least as up-to-date. */
        uint64_t cur = atomic_load(&n->current_term);
        uint64_t now = uv_now(n->loop);
        /* Leader stickiness (Raft dissertation §9.6): a node that itself believes
         * it is the leader counts as a fresh leader and must REFUSE to help a
         * rival start an election - otherwise a node rejoining after a restart
         * (whose log may be >= ours) disrupts a perfectly healthy leader. */
        int fresh_leader = atomic_load(&n->role) == CL_ROLE_LEADER ||
                           (now - n->last_leader_contact) < n->election_min_ms;
        int up_to_date = h->length < 4 + 8 + 8 ? 1 :
            log_is_current(n, get_be64(payload + 4), get_be64(payload + 12));
        int grant = !fresh_leader && (uint64_t)h->term >= cur + 1 && up_to_date;
        send_vote_resp(n, peer, CL_FRAME_PREVOTE_RESP, (uint32_t)cur, grant);
        break;
    }
    case CL_FRAME_PREVOTE_RESP:
        if (observe_term(n, h->term))
            break;
        if (n->in_prevote && h->length >= 1 && payload[0]) {
            if (++n->prevotes_granted >= n->quorum)
                become_candidate(n);
        }
        break;

    case CL_FRAME_VOTE_REQ: {
        observe_term(n, h->term); /* adopt a higher term -> follower, vote reset */
        uint64_t cur = atomic_load(&n->current_term);
        int cand = peer->node_id;
        int up_to_date = h->length < 4 + 8 + 8 ? 1 :
            log_is_current(n, get_be64(payload + 4), get_be64(payload + 12));
        int grant = 0;
        if ((uint64_t)h->term == cur && up_to_date &&
            (n->voted_for == -1 || n->voted_for == cand)) {
            grant = 1;
            n->voted_for = cand;
            persist_meta(n);       /* vote must be durable before we answer */
            arm_election_timer(n); /* granting a vote: defer our own timeout */
        }
        send_vote_resp(n, peer, CL_FRAME_VOTE_RESP, (uint32_t)cur, grant);
        break;
    }
    case CL_FRAME_VOTE_RESP:
        if (observe_term(n, h->term))
            break;
        if (atomic_load(&n->role) == CL_ROLE_CANDIDATE &&
            (uint64_t)h->term == atomic_load(&n->current_term) &&
            h->length >= 1 && payload[0]) {
            if (++n->votes_granted >= n->quorum)
                become_leader(n);
        }
        break;

    case CL_FRAME_APPEND: {
        observe_term(n, h->term);
        uint64_t cur = atomic_load(&n->current_term);
        if ((uint64_t)h->term < cur) {      /* stale leader: reject */
            send_append_resp(n, peer, 0, atomic_load(&n->log.last_index));
            break;
        }
        /* Valid leader for this term: (re)affirm follower role, defer election. */
        atomic_store(&n->role, CL_ROLE_FOLLOWER);
        atomic_store(&n->leader_id, peer->node_id);
        n->in_prevote = 0;
        n->last_leader_contact = uv_now(n->loop);
        atomic_store(&n->throttle, (h->flags & 0x01) ? 1 : 0); /* leader congestion */
        uv_timer_stop(&n->heartbeat_timer);
        arm_election_timer(n);
        update_health(n);

        const uint8_t *q = payload, *qend = payload + h->length;
        if (qend - q < CL_APPEND_FIXED) {
            send_append_resp(n, peer, 0, atomic_load(&n->log.last_index)); break;
        }
        q += 4;                                /* leaderId (== peer->node_id) */
        uint64_t prev_idx  = get_be64(q); q += 8;
        uint64_t prev_term = get_be64(q); q += 8;
        uint64_t leader_commit = get_be64(q); q += 8;
        uint64_t compact_idx   = get_be64(q); q += 8;
        uint32_t ecount = get_be32(q); q += 4;

        /* Log-matching: we must already hold prevLogIndex@prevLogTerm. */
        uint64_t mylast = atomic_load(&n->log.last_index);
        if (prev_idx > mylast) {
            /* We are simply too short - rewind the leader to our actual tail. */
            send_append_resp(n, peer, 0, mylast);
            break;
        }
        if (prev_idx > 0 && log_term_at(&n->log, prev_idx) != prev_term) {
            /* DIVERGENCE: our entry at prevLogIndex carries a DIFFERENT term -
             * e.g. an uncommitted tail a crashed leader left behind. Reporting
             * our last_index (>= prev_idx) would leave next_index unchanged and
             * the leader would resend the same rejected batch forever (the
             * flapping bug). Step it back one index (hint = prev_idx-1) so it
             * re-probes lower; every reject strictly decreases next_index until
             * the common prefix is found. The divergent tail is the uncommitted
             * suffix (committed entries are identical everywhere), so this is
             * short and never rewinds into the compacted region. */
            send_append_resp(n, peer, 0, prev_idx - 1);
            break;
        }
        /* Drop anything after prevLogIndex (resolve conflicts), then append. */
        log_truncate_from(n, prev_idx + 1);
        int ok = 1;
        for (uint32_t i = 0; i < ecount; i++) {
            if (qend - q < 8 + 8 + 4 + 4) { ok = 0; break; }
            uint64_t et = get_be64(q); q += 8;
            q += 8;                            /* entry index (implied by order) */
            uint32_t eop  = get_be32(q); q += 4;
            uint32_t elen = get_be32(q); q += 4;
            if ((uint64_t)(qend - q) < elen) { ok = 0; break; }
            if (log_append(n, et, eop, q, elen) != 0) { ok = 0; break; }
            q += elen;
        }
        if (!ok) { send_append_resp(n, peer, 0, atomic_load(&n->log.last_index)); break; }

        /* Push the appended batch to the page cache (1 write); fsync is async.
         * We ack on write-to-page-cache, not fsync. */
        persist_write_flush(n);
        uint64_t last = atomic_load(&n->log.last_index);
        uint64_t newc = leader_commit < last ? leader_commit : last;
        if (newc > atomic_load(&n->log.commit_index)) {
            atomic_store(&n->log.commit_index, newc);
            apply_committed(n);
        }
        compact_payloads(n, compact_idx); /* free bodies the leader says all hold */
        n->compact_hint = compact_idx;    /* drives prefix compaction (sweep timer) */
        send_append_resp(n, peer, 1, last);  /* matchIndex = our new last_index */
        break;
    }
    case CL_FRAME_APPEND_RESP: {
        if (observe_term(n, h->term))
            break;
        if (atomic_load(&n->role) != CL_ROLE_LEADER || h->length < 1 + 8)
            break;
        int success = payload[0];
        uint64_t hint = get_be64(payload + 1);
        if (h->length >= 1 + 8 + 8)             /* peer's fsync'd index (compaction) */
            peer->durable_index = get_be64(payload + 9);
        if (success) {
            if (hint > peer->match_index)
                peer->match_index = hint;       /* confirmed high-water advances */
            /* Re-derive in-flight from the confirmed gap: everything up to match
             * is acked, so only (next_index-1 - match) entries remain unacked. */
            peer->inflight = peer->next_index - 1 - peer->match_index;
            advance_commit(n);
        } else {
            /* Reject: the follower lacks/conflicts at prevLogIndex. `hint` is the
             * highest index it can currently accept after; rewind next_index to
             * re-probe from there. Reset inflight: nothing we sent past the rewind
             * point counts (it will be resent), so the window must not stay full
             * - otherwise pump would never resume catching this follower up. */
            if (hint + 1 < peer->next_index)
                peer->next_index = hint + 1;
            if (peer->match_index > hint)
                peer->match_index = hint;
            peer->inflight = 0;
        }
        pump_peer(n, peer);                     /* keep the window full */
        break;
    }

    case CL_FRAME_FORWARD:
        /* A follower forwarded a BATCH of client ops for us (the leader) to
         * replicate. Appended only; flushed once after this read's frame batch
         * (see on_peer_read -> leader_flush). If we are NOT the leader anymore
         * (leadership changed while the frame was in flight), re-propose each op
         * through our own inbox so it chases the current leader - dropping the
         * batch here would silently lose acknowledged client publishes. */
        if (h->length >= 4) {
            int is_leader = atomic_load(&n->role) == CL_ROLE_LEADER;
            const uint8_t *q = payload, *qend = payload + h->length;
            uint32_t count = get_be32(q); q += 4;
            for (uint32_t i = 0; i < count && qend - q >= 8; i++) {
                uint32_t op  = get_be32(q); q += 4;
                uint32_t len = get_be32(q); q += 4;
                if ((uint64_t)(qend - q) < len) break;
                if (is_leader)
                    leader_append(n, op, q, len);
                else
                    cluster_propose(n, (cluster_op_t)op, q, len);
                q += len;
            }
        }
        break;

    /* ---- state transfer (follower side) ---------------------------------- */
    case CL_FRAME_SNAP_BEGIN: {
        uint64_t cur = atomic_load(&n->current_term);
        if ((uint64_t)h->term < cur)
            break;                                  /* stale leader */
        observe_term(n, h->term);
        atomic_store(&n->role, CL_ROLE_FOLLOWER);
        atomic_store(&n->leader_id, peer->node_id);
        n->last_leader_contact = uv_now(n->loop);
        arm_election_timer(n);
        if (h->length < 8 + 8 + 4)
            break;
        const uint8_t *q = payload;
        uint64_t base  = get_be64(q); q += 8;
        uint64_t bterm = get_be64(q); q += 8;
        uint32_t tlen  = get_be32(q); q += 4;
        if ((uint64_t)(h->length - 20) < tlen)
            break;
        if (atomic_load(&n->log.last_index) >= base)
            break;                                  /* we don't need it */
        LOG_INFO("cluster: installing snapshot from leader %d "
                 "(boundary %" PRIu64 ")", peer->node_id, base);
        /* Reset broker state: the dump + tail replay will rebuild it. */
        if (n->broker)
            broker_foreach_queue(n->broker, purge_queue_cb, NULL);
        /* Reset the in-RAM log to the boundary. */
        cluster_log_t *log = &n->log;
        uint64_t last = atomic_load(&log->last_index);
        for (uint64_t i = log->base_index + 1; i <= last; i++) {
            cluster_log_entry_t *e = log_at(log, i);
            if (e) { free(e->payload); e->payload = NULL; }
        }
        log->base_index    = base;
        log->base_term     = bterm;
        log->applied_index = base;
        log->compacted_to  = base;
        log->durable_index = base;
        atomic_store(&log->last_index, base);
        atomic_store(&log->commit_index, base);
        /* Reset the on-disk log (the tail will be re-persisted from base+1). */
        cl_persist_t *ps = n->persist;
        if (ps && ps->log_fd >= 0) {
            ps->wbuf_len = 0;
            if (ftruncate(ps->log_fd, 0) != 0)
                LOG_WARN("cluster: snapshot install: ftruncate failed");
            ps->log_size = ps->write_pos = ps->alloc_size = 0;
            ps->base_index = base;
            ps->base_offset = 0;
            ps->dirty = 1;
        }
        /* Adopt the leader's topology/config blob and replay it. */
        if (tlen) {
            uint8_t *nb = realloc(n->topo, tlen);
            if (nb) {
                memcpy(nb, q, tlen);
                n->topo = nb; n->topo_len = n->topo_cap = tlen;
                for (size_t i = 0; i + 8 <= n->topo_len; ) {
                    uint32_t op = get_be32(n->topo + i);
                    uint32_t len = get_be32(n->topo + i + 4);
                    if (i + 8 + len > n->topo_len) break;
                    cluster_log_entry_t te = { .term = bterm, .index = base,
                                               .op_type = op, .len = len,
                                               .payload = n->topo + i + 8 };
                    apply_op(n, &te);
                    i += 8 + len;
                }
            }
        }
        update_health(n);
        break;
    }
    case CL_FRAME_SNAP_CHUNK: {
        if ((uint64_t)h->term < atomic_load(&n->current_term))
            break;
        n->last_leader_contact = uv_now(n->loop);
        arm_election_timer(n);
        if (h->length < 4)
            break;
        const uint8_t *q = payload, *qend = payload + h->length;
        uint32_t nrec = get_be32(q); q += 4;
        for (uint32_t i = 0; i < nrec && q < qend; i++)
            if (snap_apply_record(n, &q, qend) != 0)
                break;
        uint8_t ack[1 + 8];
        ack[0] = 0;
        put_be64(ack + 1, n->log.base_index);
        cl_send(peer->link, CL_FRAME_SNAP_ACK, 0,
                (uint32_t)atomic_load(&n->current_term), ack, sizeof ack);
        break;
    }
    case CL_FRAME_SNAP_END: {
        if ((uint64_t)h->term < atomic_load(&n->current_term))
            break;
        n->last_leader_contact = uv_now(n->loop);
        arm_election_timer(n);
        /* Persist the installed state: boundary + topology + the live dump
         * (snapshot_write walks our just-filled queues). Only now does the
         * install become durable - a crash before this point simply restarts
         * the transfer from scratch on the next connect. */
        if (n->persist)
            snapshot_write(n, n->log.base_index, n->log.base_term, 0);
        LOG_INFO("cluster: snapshot install complete (boundary %" PRIu64 ")",
                 n->log.base_index);
        uint8_t ack[1 + 8];
        ack[0] = 1;
        put_be64(ack + 1, n->log.base_index);
        cl_send(peer->link, CL_FRAME_SNAP_ACK, 0,
                (uint32_t)atomic_load(&n->current_term), ack, sizeof ack);
        update_health(n);
        break;
    }
    /* ---- state transfer (leader side) ------------------------------------ */
    case CL_FRAME_SNAP_ACK: {
        if (observe_term(n, h->term))
            break;
        if (atomic_load(&n->role) != CL_ROLE_LEADER || h->length < 1 + 8)
            break;
        int final = payload[0];
        uint64_t base = get_be64(payload + 1);
        if (final && peer->snap.active) {
            /* Peer is seeded at `base`: resume normal log replication. */
            snap_reset(peer);
            peer->next_index    = base + 1;
            peer->match_index   = base;
            peer->durable_index = base;
            peer->inflight      = 0;
            LOG_INFO("cluster: peer %d re-seeded up to %" PRIu64
                     "; streaming the log tail", peer->node_id, base);
            pump_peer(n, peer);
        } else if (peer->snap.active) {
            if (peer->snap.unacked_chunks > 0)
                peer->snap.unacked_chunks--;
            snap_pump(n, peer);
        }
        break;
    }

    case CL_FRAME_STATUS: {
        /* Leader broadcast of the cluster view: cache it so this follower's
         * status API/GUI shows real per-node replication progress + state. */
        observe_term(n, h->term);
        const uint8_t *q = payload, *qe = payload + h->length;
        if (qe - q < 1 + 8 + 8 + 4)
            break;
        int st = *q++;
        uint64_t tgt = get_be64(q); q += 8;
        uint64_t com = get_be64(q); q += 8;
        uint32_t nn  = get_be32(q); q += 4;
        if (nn > CLUSTER_MAX_NODES)
            break;
        for (uint32_t id = 0; id < nn && qe - q >= 9; id++) {
            n->view[id].match     = get_be64(q); q += 8;
            n->view[id].reachable = *q++;
        }
        n->view_target = tgt;
        n->view_commit = com;
        n->view_state  = st;
        n->have_view   = 1;
        break;
    }

    case CL_FRAME_ROUTE_HINT:
        /* DATA-PLANE only: refresh this peer's load/RTT EWMA for routing. */
        break;
    default:
        LOG_WARN("cluster: peer %d unknown frame type %u", peer->node_id, h->type);
        break;
    }
}

/* Pull as many complete frames as are buffered and dispatch them. Returns -1 on
 * a fatal protocol error (caller should drop the link). */
static int cl_drain_frames(cluster_node_t *n, cluster_peer_t *peer,
                           char **buf, size_t *len)
{
    size_t off = 0;
    while (*len - off >= CLUSTER_HDR_SIZE) {
        cluster_frame_hdr_t h;
        if (cluster_hdr_parse((const uint8_t *)*buf + off, *len - off, &h) != 0) {
            LOG_WARN("cluster: peer %d bad frame header", peer->node_id);
            return -1;
        }
        size_t need = CLUSTER_HDR_SIZE + h.length;
        if (*len - off < need)
            break;  /* wait for the rest of the payload */
        const uint8_t *payload = (const uint8_t *)*buf + off + CLUSTER_HDR_SIZE;
        if (cluster_crc32(payload, h.length) != h.crc32) {
            LOG_WARN("cluster: peer %d CRC mismatch", peer->node_id);
            return -1;
        }
        cl_dispatch(n, peer, &h, payload);
        off += need;
    }
    if (off) {  /* compact the consumed prefix */
        memmove(*buf, *buf + off, *len - off);
        *len -= off;
    }
    return 0;
}

/* ---- read path ----------------------------------------------------------- */

static void cl_alloc(uv_handle_t *handle, size_t suggested, uv_buf_t *buf)
{
    (void)handle;
    buf->base = malloc(suggested);
    buf->len  = buf->base ? suggested : 0;
}

static void schedule_reconnect(cluster_peer_t *peer);

/* Heap holder for an accepted inbound socket: we accept into a temporary holder,
 * read the HELLO, then bind the socket to the matching peer slot. Defined here
 * (ahead of on_link_closed) so the close path can free an orphaned holder. */
typedef struct cluster_inbound {
    uv_tcp_t        handle;   /* MUST be first */
    cluster_node_t *node;
    cluster_peer_t *peer;     /* resolved after HELLO */
    char           *rbuf;
    size_t          rbuf_len, rbuf_cap;
} cluster_inbound_t;

/* Single close callback for a peer link (outbound or inbound). handle->data is
 * the cluster_peer_t, or NULL for a stale inbound holder that a reconnect
 * orphaned (see the accept path). Frees the inbound holder if any, clears the
 * link, and (only for an outbound link that is not shutting down) schedules a
 * redial. An inbound link is not redialed - the lower-id side owns the redial. */
static void on_link_closed(uv_handle_t *handle)
{
    cluster_peer_t *peer = handle->data;
    if (!peer) {
        /* Orphaned inbound socket: a fresh connection from this peer already
         * replaced peer->link, so this stale holder must be freed WITHOUT
         * touching the live link (otherwise we'd tear down the new one). */
        cluster_inbound_t *in = (cluster_inbound_t *)handle;
        free(in->rbuf);
        free(in);
        return;
    }
    if (peer->link_holder) {
        free(peer->link_holder);
        peer->link_holder = NULL;
    }
    peer->link = NULL;
    if (peer->outbound && !peer->closing)
        schedule_reconnect(peer);
}

static void drop_peer_link(cluster_peer_t *peer)
{
    atomic_store(&peer->link_state, CL_LINK_DOWN);
    snap_reset(peer);        /* abort any in-progress state transfer */
    free(peer->rbuf);
    peer->rbuf = NULL;
    peer->rbuf_len = peer->rbuf_cap = 0;
    update_health(peer->node); /* a leader may have just lost its majority */
    if (peer->link && !uv_is_closing((uv_handle_t *)peer->link))
        uv_close((uv_handle_t *)peer->link, on_link_closed);
}

static void on_peer_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf)
{
    cluster_peer_t *peer = stream->data;
    cluster_node_t *n = peer->node;

    if (nread < 0) {
        if (nread != UV_EOF)
            LOG_DEBUG("cluster: peer %d read error: %s",
                      peer->node_id, uv_strerror((int)nread));
        free(buf->base);
        uv_read_stop(stream);
        drop_peer_link(peer);
        return;
    }
    if (nread > 0) {
        if (buf_append(&peer->rbuf, &peer->rbuf_len, &peer->rbuf_cap,
                       buf->base, (size_t)nread) != 0 ||
            cl_drain_frames(n, peer, &peer->rbuf, &peer->rbuf_len) != 0) {
            free(buf->base);
            uv_read_stop(stream);
            drop_peer_link(peer);
            return;
        }
        /* One fsync + one replication round for all FORWARD ops this read
         * appended (leader side); no-op otherwise. */
        leader_flush(n);
    }
    free(buf->base);
}

/* ---- outbound dial + reconnect ------------------------------------------- */

static void cl_dial(cluster_peer_t *peer);

static void reconnect_timer_cb(uv_timer_t *timer)
{
    cluster_peer_t *peer = timer->data;
    if (!peer->closing)
        cl_dial(peer);
}

static void schedule_reconnect(cluster_peer_t *peer)
{
    if (peer->closing)
        return;
    peer->backoff_ms = peer->backoff_ms ? peer->backoff_ms * 2 : 100;
    if (peer->backoff_ms > 2000)
        peer->backoff_ms = 2000;  /* cap */
    uv_timer_start(&peer->reconnect_timer, reconnect_timer_cb, peer->backoff_ms, 0);
}

static void on_connect(uv_connect_t *req, int status)
{
    cluster_peer_t *peer = req->data;
    if (status) {
        LOG_DEBUG("cluster: dial peer %d failed: %s",
                  peer->node_id, uv_strerror(status));
        peer->link = (uv_stream_t *)&peer->handle;
        uv_close((uv_handle_t *)&peer->handle, on_link_closed);
        return;
    }
    peer->backoff_ms = 0;
    peer->link = (uv_stream_t *)&peer->handle;
    peer->handle.data = peer;
    atomic_store(&peer->link_state, CL_LINK_HANDSHAKING);
    uv_read_start((uv_stream_t *)&peer->handle, cl_alloc, on_peer_read);
    send_hello(peer->node, peer->link, CL_FRAME_HELLO);
}

static void cl_dial(cluster_peer_t *peer)
{
    cluster_node_t *n = peer->node;
    if (!peer->have_sa) {
        LOG_WARN("cluster: peer %d has no resolved address", peer->node_id);
        schedule_reconnect(peer);
        return;
    }
    uv_tcp_init(n->loop, &peer->handle);
    peer->handle.data = peer;
    peer->connect_req.data = peer;
    atomic_store(&peer->link_state, CL_LINK_CONNECTING);
    peer->link = (uv_stream_t *)&peer->handle;
    int rc = uv_tcp_connect(&peer->connect_req, &peer->handle,
                            (const struct sockaddr *)&peer->sa, on_connect);
    if (rc) {
        LOG_DEBUG("cluster: connect start to peer %d failed: %s",
                  peer->node_id, uv_strerror(rc));
        uv_close((uv_handle_t *)&peer->handle, on_link_closed);
    }
}

/* ---- inbound accept ------------------------------------------------------ *
 * We don't know which peer dialed us until its HELLO arrives, so we accept into
 * a temporary holder, read the HELLO, then bind the socket to the matching peer
 * slot (peer->link points at the accepted handle). The holder type is defined
 * above (ahead of on_link_closed). */

static cluster_peer_t *find_peer(cluster_node_t *n, int node_id)
{
    for (int i = 0; i < n->npeers; i++)
        if (n->peers[i].node_id == node_id)
            return &n->peers[i];
    return NULL;
}

static void on_inbound_closed(uv_handle_t *handle)
{
    cluster_inbound_t *in = (cluster_inbound_t *)handle;
    free(in->rbuf);
    free(in);
}

static void on_inbound_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf)
{
    cluster_inbound_t *in = stream->data;
    cluster_node_t *n = in->node;

    if (nread < 0) {
        free(buf->base);
        uv_close((uv_handle_t *)stream, on_inbound_closed);
        return;
    }
    if (nread > 0)
        buf_append(&in->rbuf, &in->rbuf_len, &in->rbuf_cap, buf->base, (size_t)nread);
    free(buf->base);

    /* Expect a HELLO first; once we know the peer, hand the socket over. */
    if (in->rbuf_len < CLUSTER_HDR_SIZE)
        return;
    cluster_frame_hdr_t h;
    if (cluster_hdr_parse((const uint8_t *)in->rbuf, in->rbuf_len, &h) != 0 ||
        h.type != CL_FRAME_HELLO) {
        uv_close((uv_handle_t *)stream, on_inbound_closed);
        return;
    }
    if (in->rbuf_len < CLUSTER_HDR_SIZE + h.length)
        return;  /* wait for full HELLO */

    int peer_id = (int)get_be32((const uint8_t *)in->rbuf + CLUSTER_HDR_SIZE);
    cluster_peer_t *peer = find_peer(n, peer_id);
    if (!peer || peer->outbound) {  /* outbound peers we dial ourselves */
        LOG_WARN("cluster: rejecting inbound HELLO from id %d", peer_id);
        uv_close((uv_handle_t *)stream, on_inbound_closed);
        return;
    }

    /* A previous link for this peer may still be bound: either its death has not
     * been observed yet, or a half-open socket survived the peer's fast restart.
     * Tear it down DETACHED (handle->data = NULL) so its close callback frees
     * only that stale holder and never clobbers the fresh link we bind below.
     * Without this the leader keeps writing into the dead socket - the follower
     * hears nothing, times out, and the cluster flaps (term storm on rejoin). */
    if (peer->link) {
        uv_stream_t *old = peer->link;
        old->data = NULL;                       /* mark orphan for on_link_closed */
        peer->link = NULL;
        peer->link_holder = NULL;
        if (!uv_is_closing((uv_handle_t *)old))
            uv_close((uv_handle_t *)old, on_link_closed);
    }
    /* Drop any half-assembled bytes left in the peer buffer by the old stream. */
    free(peer->rbuf);
    peer->rbuf = NULL;
    peer->rbuf_len = peer->rbuf_cap = 0;

    /* Bind this accepted socket to the peer slot. The holder `in` stays alive as
     * the backing storage for the embedded uv_tcp_t (freed in on_link_closed);
     * only its reassembly buffer is handed over to the peer. */
    in->peer = peer;
    peer->link = stream;
    peer->link_holder = in;
    stream->data = peer;  /* subsequent reads route through on_peer_read */
    /* Move any bytes already buffered past the HELLO into the peer buffer. */
    size_t consumed = CLUSTER_HDR_SIZE + h.length;
    if (in->rbuf_len > consumed)
        buf_append(&peer->rbuf, &peer->rbuf_len, &peer->rbuf_cap,
                   in->rbuf + consumed, in->rbuf_len - consumed);
    free(in->rbuf);
    in->rbuf = NULL;
    in->rbuf_len = in->rbuf_cap = 0;
    /* Re-point reads to the peer read path and reply HELLO_ACK. */
    uv_read_stop(stream);
    uv_read_start(stream, cl_alloc, on_peer_read);
    send_hello(n, peer->link, CL_FRAME_HELLO_ACK);
    atomic_store(&peer->link_state, CL_LINK_UP);
    update_health(n);
    peer_on_link_up(n, peer);
    LOG_INFO("cluster: peer %d link UP (accepted)", peer_id);
}

static void on_new_peer_conn(uv_stream_t *server, int status)
{
    cluster_node_t *n = server->data;
    if (status) {
        LOG_DEBUG("cluster: accept failed: %s", uv_strerror(status));
        return;
    }
    cluster_inbound_t *in = calloc(1, sizeof(*in));
    if (!in)
        return;
    in->node = n;
    uv_tcp_init(n->loop, &in->handle);
    in->handle.data = in;
    if (uv_accept(server, (uv_stream_t *)&in->handle) != 0) {
        uv_close((uv_handle_t *)&in->handle, on_inbound_closed);
        return;
    }
    uv_read_start((uv_stream_t *)&in->handle, cl_alloc, on_inbound_read);
}

/* ---- election / heartbeat timers ----------------------------------------- */

static void election_timer_cb(uv_timer_t *timer)
{
    cluster_node_t *n = timer->data;
    if (atomic_load(&n->role) == CL_ROLE_LEADER)
        return; /* leaders do not run election timeouts */
    /* Follower lost contact, or candidate's election stalled: (re)run PreVote. */
    start_prevote(n);
}

static void heartbeat_timer_cb(uv_timer_t *timer)
{
    cluster_node_t *n = timer->data;
    if (atomic_load(&n->role) != CL_ROLE_LEADER) {
        uv_timer_stop(timer);
        return;
    }
    leader_update_flow(n); /* refresh congestion state every heartbeat */
    uint64_t last = atomic_load(&n->log.last_index);
    for (int i = 0; i < n->npeers; i++) {
        cluster_peer_t *p = &n->peers[i];
        if (atomic_load(&p->link_state) != CL_LINK_UP)
            continue;
        pump_peer(n, p);                 /* replicate if the peer is behind */
        if (!p->snap.active && p->next_index > last)
            send_append(n, p);           /* empty keepalive when caught up */
    }
    if (++n->status_tick % 5 == 0)        /* broadcast cluster view ~every 250ms */
        send_status(n);
}

/* Background group-fsync: flushes the log to disk off the commit hot path. */
static void fsync_timer_cb(uv_timer_t *timer)
{
    cluster_node_t *n = timer->data;
    persist_sync(n);
    /* Periodic log compaction sweep (~every 2 s, off the hot path): drop the
     * committed + applied + replicated + consumed prefix. The leader knows the
     * replicated-everywhere index directly; a follower uses the value the leader
     * last sent it. log_compact_to clamps to what is locally safe. */
    if (++n->compact_tick >= 80) {
        n->compact_tick = 0;
        uint64_t upto = atomic_load(&n->role) == CL_ROLE_LEADER
                        ? all_durable_index(n) : n->compact_hint;
        if (upto) log_compact_to(n, upto);
    }
#if defined(__GLIBC__)
    /* Replication and the broker queues churn through millions of equal-sized
     * message-body allocations. glibc keeps freed bodies in its per-arena pool
     * rather than returning them to the OS, so after a big drain (consume burst,
     * log compaction) RSS stays pinned at the high-water mark even though the
     * memory is logically free. That looks like a leak and worries operators.
     * Periodically (~every 5 s, off the hot path) hand the freed arenas back to
     * the kernel so RSS actually tracks live usage. */
    if (++n->trim_tick >= 200) {
        n->trim_tick = 0;
        malloc_trim(0);
    }
#endif
}

/* ---- cross-plane proposal inbox ------------------------------------------ */

static void fwd_retry_timer_cb(uv_timer_t *timer);

/* Forward-frame caps: many bounded frames instead of one giant one. An
 * unbounded frame (the whole inbox serialized at once) can exceed
 * CLUSTER_MAX_PAYLOAD - the receiver then rejects the header and DROPS THE
 * LINK, losing every forwarded message in flight and stalling replication
 * while it reconnects. (Real message-loss bug: publish-to-follower hiccups +
 * missing messages.) */
#define CL_FWD_MAX_OPS   1024u
#define CL_FWD_MAX_BYTES (256u * 1024u)

/* Inbox backpressure (hysteresis): when un-forwarded proposals pile up (no
 * reachable leader, or producers outrun the forward link), pause producers
 * instead of buffering without bound - and NEVER drop. */
#define CL_INBOX_HIGH 16000
#define CL_INBOX_LOW   4000

/* Put an un-forwardable batch BACK at the head of the inbox (order preserved:
 * these proposals predate anything a worker enqueued meanwhile). */
static void requeue_proposals(cluster_node_t *n, cl_proposal_t *head)
{
    if (!head)
        return;
    cl_proposal_t *tail = head;
    while (tail->next)
        tail = tail->next;
    pthread_mutex_lock(&n->propose_lock);
    tail->next = n->propose_head;
    n->propose_head = head;
    if (!n->propose_tail)
        n->propose_tail = tail;
    pthread_mutex_unlock(&n->propose_lock);
}

/* Follower: forward the batch to the leader in capped frames. Anything that
 * cannot be sent right now (no reachable leader, alloc/write failure) is
 * requeued and retried on fwd_retry_timer - proposals are NEVER dropped.
 * Frame payload: count(u32) then per op: op_type(u32) len(u32) bytes. */
static void forward_batch_to_leader(cluster_node_t *n, cl_proposal_t *list)
{
    while (list) {
        int lid = atomic_load(&n->leader_id);
        cluster_peer_t *lp = (lid >= 0) ? find_peer(n, lid) : NULL;
        if (!lp || atomic_load(&lp->link_state) != CL_LINK_UP)
            break;                            /* leader unreachable: retry later */

        /* Take a capped slice off the front of the list. */
        size_t   bytes = 4;
        uint32_t count = 0;
        cl_proposal_t *end = list;            /* first proposal NOT in the slice */
        while (end && count < CL_FWD_MAX_OPS &&
               (count == 0 || bytes + 8 + end->len <= CL_FWD_MAX_BYTES)) {
            bytes += 8 + end->len;
            count++;
            end = end->next;
        }

        uint8_t *body = malloc(bytes);
        if (!body)
            break;                            /* OOM: retry later, don't drop */
        uint8_t *w = body;
        put_be32(w, count); w += 4;
        for (cl_proposal_t *p = list; p != end; p = p->next) {
            put_be32(w, p->op_type); w += 4;
            put_be32(w, p->len);     w += 4;
            if (p->len) { memcpy(w, p->data, p->len); w += p->len; }
        }
        int rc = cl_send(lp->link, CL_FRAME_FORWARD, 0,
                         (uint32_t)atomic_load(&n->current_term),
                         body, (uint32_t)(w - body));
        free(body);
        if (rc != 0)
            break;                            /* link failed mid-drain: retry */

        /* The slice is on the wire: release it. */
        while (list != end) {
            cl_proposal_t *next = list->next;
            atomic_fetch_sub(&n->inbox_depth, 1);
            free(list);
            list = next;
        }
    }

    if (list) {
        /* Could not send (yet): keep the proposals and poll for a leader. */
        requeue_proposals(n, list);
        if (!uv_is_active((uv_handle_t *)&n->fwd_retry_timer))
            uv_timer_start(&n->fwd_retry_timer, fwd_retry_timer_cb, 60, 0);
    }
    if (atomic_load(&n->inbox_depth) < CL_INBOX_LOW)
        atomic_store(&n->fwd_congested, 0);
}

/* Drain proposals submitted by worker threads. On the leader they are appended
 * and replicated; on a follower they are forwarded in capped frames. */
static void drain_inbox(cluster_node_t *n)
{
    pthread_mutex_lock(&n->propose_lock);
    cl_proposal_t *list = n->propose_head;
    n->propose_head = n->propose_tail = NULL;
    pthread_mutex_unlock(&n->propose_lock);

    if (atomic_load(&n->role) != CL_ROLE_LEADER) {
        forward_batch_to_leader(n, list);
        return;
    }
    while (list) {
        cl_proposal_t *next = list->next;
        leader_append(n, list->op_type, list->data, list->len);
        atomic_fetch_sub(&n->inbox_depth, 1);
        free(list);
        list = next;
    }
    leader_flush(n); /* one replication round + commit scan for the whole batch */
    if (atomic_load(&n->inbox_depth) < CL_INBOX_LOW)
        atomic_store(&n->fwd_congested, 0);
}

static void propose_async_cb(uv_async_t *async)
{
    drain_inbox(async->data);
}

static void fwd_retry_timer_cb(uv_timer_t *timer)
{
    drain_inbox(timer->data);
}

/* ---- lifecycle ----------------------------------------------------------- */

cluster_node_t *cluster_node_new(uv_loop_t *loop, const cluster_config_t *cfg,
                                 beaver_broker_t *broker, struct authstore *auth)
{
    if (!loop || !cfg || cfg->nnodes < 1 || cfg->nnodes > CLUSTER_MAX_NODES)
        return NULL;
    cluster_node_t *n = calloc(1, sizeof(*n));
    if (!n)
        return NULL;

    n->loop   = loop;
    n->broker = broker;
    n->authstore = auth;
    n->self_id = cfg->self_id;
    n->nnodes  = cfg->nnodes;
    n->quorum  = cfg->nnodes / 2 + 1;
    if (cfg->self_id >= 0 && cfg->self_id < CLUSTER_MAX_NODES)
        snprintf(n->self_addr, sizeof(n->self_addr), "%s",
                 cfg->node_addr[cfg->self_id]);
    n->election_min_ms = cfg->election_min_ms;
    n->election_max_ms = cfg->election_max_ms;
    n->heartbeat_ms    = cfg->heartbeat_ms;
    n->rng_state = 0x9E3779B97F4A7C15ull ^ ((uint64_t)cfg->self_id + 1) * 0xD1B54A32D192ED03ull;

    atomic_init(&n->current_term, 0);
    n->voted_for = -1;
    atomic_init(&n->role, CL_ROLE_FOLLOWER);
    atomic_init(&n->leader_id, -1);
    atomic_init(&n->health, CL_HEALTH_ISOLATED);  /* until links come up */
    atomic_init(&n->log.last_index, 0);
    atomic_init(&n->log.commit_index, 0);
    atomic_init(&n->inbox_depth, 0);
    atomic_init(&n->throttle, 0);
    pthread_mutex_init(&n->propose_lock, NULL);

    /* Open the on-disk log + replay it (no-op if data_dir is empty). This may
     * override current_term/voted_for/last_index from persisted state. */
    persist_init(n, cfg->data_dir, cfg->self_id);

    /* Build the peer table: every node id except our own. Lower id dials. */
    int k = 0;
    for (int id = 0; id < cfg->nnodes; id++) {
        if (id == cfg->self_id)
            continue;
        cluster_peer_t *peer = &n->peers[k++];
        peer->node_id  = id;
        peer->node     = n;
        peer->outbound = (cfg->self_id < id);
        atomic_init(&peer->link_state, CL_LINK_DOWN);
        atomic_init(&peer->rtt_ewma_us, 0);
        atomic_init(&peer->miss_streak, 0);
        snprintf(peer->addr, sizeof(peer->addr), "%s", cfg->node_addr[id]);
        struct sockaddr_in sin;
        if (parse_addr(peer->addr, &sin) == 0) {
            memcpy(&peer->sa, &sin, sizeof(sin));
            peer->have_sa = 1;
        }
    }
    n->npeers = k;
    return n;
}

int cluster_node_start(cluster_node_t *n)
{
    /* The mesh listener must already be bound via cluster_node_listen(). */
    for (int i = 0; i < n->npeers; i++) {
        cluster_peer_t *peer = &n->peers[i];
        if (uv_timer_init(n->loop, &peer->reconnect_timer) == 0)
            peer->reconnect_timer.data = peer;
        if (peer->outbound)
            cl_dial(peer);
        /* inbound peers wait for on_new_peer_conn() */
    }

    uv_timer_init(n->loop, &n->election_timer);
    n->election_timer.data = n;
    uv_timer_start(&n->election_timer, election_timer_cb, election_timeout_ms(n), 0);

    uv_timer_init(n->loop, &n->heartbeat_timer);
    n->heartbeat_timer.data = n;
    /* heartbeat timer is armed only once we win an election (become_leader). */

    /* Background fsync of the replicated log (async persistence). */
    uv_timer_init(n->loop, &n->fsync_timer);
    n->fsync_timer.data = n;
    if (n->persist)
        uv_timer_start(&n->fsync_timer, fsync_timer_cb, 25, 25);

    /* Retry loop for un-forwarded proposals (armed on demand). */
    uv_timer_init(n->loop, &n->fwd_retry_timer);
    n->fwd_retry_timer.data = n;

    uv_async_init(n->loop, &n->propose_async, propose_async_cb);
    n->propose_async.data = n;

    n->running = 1;
    LOG_INFO("cluster: node %d started (%d peers, quorum %d)",
             n->self_id, n->npeers, n->quorum);
    return 0;
}

/* Bind + listen the mesh port. Separate from start() so config parsing stays
 * out of the hot lifecycle path; call before cluster_node_start(). */
int cluster_node_listen(cluster_node_t *n, const char *bind_addr, int port, int backlog)
{
    struct sockaddr_in addr;
    if (uv_tcp_init(n->loop, &n->listener) != 0)
        return -1;
    n->listener.data = n;
    if (uv_ip4_addr(bind_addr, port, &addr) != 0)
        return -1;
    if (uv_tcp_bind(&n->listener, (const struct sockaddr *)&addr, 0) != 0)
        return -1;
    return uv_listen((uv_stream_t *)&n->listener, backlog, on_new_peer_conn);
}

static void on_handle_closed(uv_handle_t *handle) { (void)handle; }

static void shutdown_async_cb(uv_async_t *async)
{
    cluster_node_stop(async->data);
}

int cluster_node_install_shutdown_async(cluster_node_t *n)
{
    int rc = uv_async_init(n->loop, &n->shutdown_async, shutdown_async_cb);
    if (rc)
        return rc;
    n->shutdown_async.data = n;
    n->shutdown_async_installed = 1;
    return 0;
}

void cluster_node_request_shutdown(cluster_node_t *n)
{
    if (n && n->shutdown_async_installed)
        uv_async_send(&n->shutdown_async);
}

void cluster_node_stop(cluster_node_t *n)
{
    if (!n || !n->running)
        return;
    n->running = 0;
    persist_sync(n); /* flush the log to disk on a clean shutdown */
    for (int i = 0; i < n->npeers; i++) {
        cluster_peer_t *peer = &n->peers[i];
        peer->closing = 1;
        if (!uv_is_closing((uv_handle_t *)&peer->reconnect_timer))
            uv_close((uv_handle_t *)&peer->reconnect_timer, on_handle_closed);
        if (peer->link && !uv_is_closing((uv_handle_t *)peer->link))
            uv_close((uv_handle_t *)peer->link, on_link_closed);
    }
    if (!uv_is_closing((uv_handle_t *)&n->listener))
        uv_close((uv_handle_t *)&n->listener, on_handle_closed);
    if (!uv_is_closing((uv_handle_t *)&n->election_timer))
        uv_close((uv_handle_t *)&n->election_timer, on_handle_closed);
    if (!uv_is_closing((uv_handle_t *)&n->heartbeat_timer))
        uv_close((uv_handle_t *)&n->heartbeat_timer, on_handle_closed);
    if (!uv_is_closing((uv_handle_t *)&n->fsync_timer))
        uv_close((uv_handle_t *)&n->fsync_timer, on_handle_closed);
    if (!uv_is_closing((uv_handle_t *)&n->fwd_retry_timer))
        uv_close((uv_handle_t *)&n->fwd_retry_timer, on_handle_closed);
    if (!uv_is_closing((uv_handle_t *)&n->propose_async))
        uv_close((uv_handle_t *)&n->propose_async, on_handle_closed);
    if (n->shutdown_async_installed &&
        !uv_is_closing((uv_handle_t *)&n->shutdown_async))
        uv_close((uv_handle_t *)&n->shutdown_async, on_handle_closed);
}

void cluster_node_free(cluster_node_t *n)
{
    if (!n)
        return;
    persist_free(n);
    for (int i = 0; i < n->npeers; i++)
        free(n->peers[i].rbuf);
    /* free any proposals that were never drained */
    cl_proposal_t *pr = n->propose_head;
    while (pr) {
        cl_proposal_t *nx = pr->next;
        free(pr);
        pr = nx;
    }
    /* free each in-RAM log entry's payload, then the entry array. Entries hold
     * indices (base_index, last_index], stored at slots [0, last-base). */
    uint64_t last = atomic_load(&n->log.last_index);
    for (uint64_t i = 0; i < last - n->log.base_index; i++)
        free(n->log.entries[i].payload);
    free(n->log.entries);
    free(n->topo);
    pthread_mutex_destroy(&n->propose_lock);
    free(n);
}

/* ---- data-plane queries (lock-free) -------------------------------------- */

cluster_role_t cluster_role_of(const cluster_node_t *n)
{
    return (cluster_role_t)atomic_load(&n->role);
}

int cluster_leader_id(const cluster_node_t *n)
{
    return atomic_load(&n->leader_id);
}

int cluster_is_leader(const cluster_node_t *n)
{
    return atomic_load(&n->role) == CL_ROLE_LEADER;
}

uint64_t cluster_term(const cluster_node_t *n)
{
    return atomic_load(&n->current_term);
}

cluster_health_t cluster_health(const cluster_node_t *n)
{
    return (cluster_health_t)atomic_load(&n->health);
}

int cluster_should_throttle(const cluster_node_t *n)
{
    /* Two sources: the leader's replication-backlog flag (broadcast in
     * AppendEntries) and this node's own forward-inbox congestion. */
    return n ? (atomic_load(&n->throttle) || atomic_load(&n->fwd_congested)) : 0;
}

void cluster_get_status(const cluster_node_t *n, cluster_status_t *out)
{
    memset(out, 0, sizeof(*out));
    out->self_id      = n->self_id;
    out->nnodes       = n->nnodes;
    out->quorum       = n->quorum;
    out->leader_id    = atomic_load(&n->leader_id);
    out->term         = atomic_load(&n->current_term);
    out->commit_index = atomic_load(&n->log.commit_index);
    out->last_index   = atomic_load(&n->log.last_index);
    out->role         = (cluster_role_t)atomic_load(&n->role);
    out->health       = (cluster_health_t)atomic_load(&n->health);

    int is_leader = out->role == CL_ROLE_LEADER;
    /* Per-node replication progress is the LEADER's knowledge. The leader has it
     * live; a follower uses the leader's broadcast view (CL_FRAME_STATUS) so the
     * GUI reads the same numbers on every node. */
    out->target_index = is_leader ? out->last_index : n->view_target;
    out->state = is_leader
        ? leader_cluster_state(n)
        : (n->have_view ? (cluster_state_t)n->view_state
           : (out->health == CL_HEALTH_OK ? CL_STATE_HEALTHY : CL_STATE_NO_QUORUM));

    int m = 0;
    for (int id = 0; id < n->nnodes && m < CLUSTER_MAX_NODES; id++) {
        cluster_member_t *mm = &out->members[m++];
        mm->node_id   = id;
        mm->is_leader = (out->leader_id == id);
        if (id == n->self_id) {
            mm->is_self     = 1;
            mm->reachable   = 1;
            mm->match_index = out->last_index; /* our own progress is exact */
            snprintf(mm->addr, sizeof(mm->addr), "%s", n->self_addr);
        } else {
            for (int i = 0; i < n->npeers; i++) {
                if (n->peers[i].node_id != id)
                    continue;
                snprintf(mm->addr, sizeof(mm->addr), "%s", n->peers[i].addr);
                if (is_leader) {
                    mm->reachable   = atomic_load(&n->peers[i].link_state) == CL_LINK_UP;
                    mm->match_index = n->peers[i].match_index;
                } else if (n->have_view) {
                    mm->reachable   = n->view[id].reachable;
                    mm->match_index = n->view[id].match;
                }
                break;
            }
        }
    }
    out->nmembers = m;
}

int cluster_propose(cluster_node_t *n, cluster_op_t op_type,
                    const void *data, size_t len)
{
    if (!n)
        return -1;
    /* Always accept and buffer: proposals are never dropped. If no leader is
     * reachable right now (election in progress), the batch waits in the inbox
     * and fwd_retry_timer re-forwards once one appears; meanwhile the depth
     * check below pauses producers (backpressure), so the buffer is bounded. */
    if (atomic_load(&n->inbox_depth) > CL_INBOX_HIGH)
        atomic_store(&n->fwd_congested, 1);
    cl_proposal_t *pr = malloc(sizeof(*pr) + len);
    if (!pr)
        return -1;
    pr->next = NULL;
    pr->op_type = (uint32_t)op_type;
    pr->len = (uint32_t)len;
    if (len)
        memcpy(pr->data, data, len);
    pthread_mutex_lock(&n->propose_lock);
    if (n->propose_tail)
        n->propose_tail->next = pr;
    else
        n->propose_head = pr;
    n->propose_tail = pr;
    pthread_mutex_unlock(&n->propose_lock);
    atomic_fetch_add(&n->inbox_depth, 1);
    uv_async_send(&n->propose_async);
    return 0;
}

int cluster_replicate_declare_queue(cluster_node_t *n, const char *vhost,
                                    const char *name, uint8_t flags)
{
    if (strlen(vhost) > 250 || strlen(name) > 250)
        return -1;
    uint8_t buf[1 + 2 * (2 + 251)];
    uint8_t *p = buf;
    wr8(&p, flags);
    wr_str(&p, vhost);
    wr_str(&p, name);
    return cluster_propose(n, CL_OP_DECLARE_QUEUE, buf, (size_t)(p - buf));
}

int cluster_replicate_declare_exchange(cluster_node_t *n, const char *vhost,
                                       const char *name, int type, uint8_t flags)
{
    if (strlen(vhost) > 250 || strlen(name) > 250)
        return -1;
    uint8_t buf[1 + 1 + 2 * (2 + 251)];
    uint8_t *p = buf;
    wr8(&p, (uint8_t)type);
    wr8(&p, flags);
    wr_str(&p, vhost);
    wr_str(&p, name);
    return cluster_propose(n, CL_OP_DECLARE_EXCH, buf, (size_t)(p - buf));
}

int cluster_replicate_bind(cluster_node_t *n, const char *vhost,
                           const char *queue, const char *exchange,
                           const char *routing_key)
{
    if (strlen(vhost) > 250 || strlen(queue) > 250 || strlen(exchange) > 250 ||
        strlen(routing_key) > 250)
        return -1;
    uint8_t buf[4 * (2 + 251)];
    uint8_t *p = buf;
    wr_str(&p, vhost);
    wr_str(&p, queue);
    wr_str(&p, exchange);
    wr_str(&p, routing_key);
    return cluster_propose(n, CL_OP_BIND, buf, (size_t)(p - buf));
}

int cluster_replicate_publish(cluster_node_t *n, const char *vhost,
                              const char *exchange, const char *routing_key,
                              const void *body, size_t body_len,
                              const void *props, size_t props_len)
{
    if (strlen(vhost) > 250 || strlen(exchange) > 250 || strlen(routing_key) > 250)
        return -1;
    /* vhost + exchange + rk (shortstr) | props (u32+bytes) | body (u32+bytes) */
    size_t need = (2 + strlen(vhost)) + (2 + strlen(exchange)) +
                  (2 + strlen(routing_key)) + (4 + props_len) + (4 + body_len);
    uint8_t *buf = malloc(need);
    if (!buf)
        return -1;
    uint8_t *p = buf;
    wr_str(&p, vhost);
    wr_str(&p, exchange);
    wr_str(&p, routing_key);
    put_be32(p, (uint32_t)props_len); p += 4;
    if (props_len) { memcpy(p, props, props_len); p += props_len; }
    put_be32(p, (uint32_t)body_len);  p += 4;
    if (body_len)  { memcpy(p, body, body_len);   p += body_len; }
    int rc = cluster_propose(n, CL_OP_PUBLISH, buf, (size_t)(p - buf));
    free(buf);
    return rc;
}

int cluster_replicate_consume(cluster_node_t *n, const char *vhost,
                              const char *queue, uint64_t watermark)
{
    if (strlen(vhost) > 250 || strlen(queue) > 250)
        return -1;
    uint8_t buf[2 * (2 + 251) + 8];
    uint8_t *p = buf;
    wr_str(&p, vhost);
    wr_str(&p, queue);
    put_be64(p, watermark); p += 8;
    return cluster_propose(n, CL_OP_ACK, buf, (size_t)(p - buf));
}

/* ---- access-control config replication ----------------------------------- */

int cluster_replicate_add_vhost(cluster_node_t *n, const char *vhost)
{
    if (strlen(vhost) > 250) return -1;
    uint8_t buf[2 + 251]; uint8_t *p = buf; wr_str(&p, vhost);
    return cluster_propose(n, CL_OP_ADD_VHOST, buf, (size_t)(p - buf));
}
int cluster_replicate_del_vhost(cluster_node_t *n, const char *vhost)
{
    if (strlen(vhost) > 250) return -1;
    uint8_t buf[2 + 251]; uint8_t *p = buf; wr_str(&p, vhost);
    return cluster_propose(n, CL_OP_DEL_VHOST, buf, (size_t)(p - buf));
}
int cluster_replicate_add_user(cluster_node_t *n, const char *user,
                               const char *pass_hash, uint32_t tags)
{
    if (strlen(user) > 250 || strlen(pass_hash) > 250) return -1;
    uint8_t buf[2 + 251 + 2 + 251 + 4]; uint8_t *p = buf;
    wr_str(&p, user); wr_str(&p, pass_hash); wr32(&p, tags);
    return cluster_propose(n, CL_OP_ADD_USER, buf, (size_t)(p - buf));
}
int cluster_replicate_del_user(cluster_node_t *n, const char *user)
{
    if (strlen(user) > 250) return -1;
    uint8_t buf[2 + 251]; uint8_t *p = buf; wr_str(&p, user);
    return cluster_propose(n, CL_OP_DEL_USER, buf, (size_t)(p - buf));
}
int cluster_replicate_set_perm(cluster_node_t *n, const char *user,
                               const char *vhost, const char *configure,
                               const char *write, const char *read)
{
    if (strlen(user) > 250 || strlen(vhost) > 250 || strlen(configure) > 250 ||
        strlen(write) > 250 || strlen(read) > 250) return -1;
    uint8_t buf[5 * (2 + 251)]; uint8_t *p = buf;
    wr_str(&p, user); wr_str(&p, vhost);
    wr_str(&p, configure); wr_str(&p, write); wr_str(&p, read);
    return cluster_propose(n, CL_OP_SET_PERM, buf, (size_t)(p - buf));
}
int cluster_replicate_clear_perm(cluster_node_t *n, const char *user,
                                 const char *vhost)
{
    if (strlen(user) > 250 || strlen(vhost) > 250) return -1;
    uint8_t buf[2 * (2 + 251)]; uint8_t *p = buf;
    wr_str(&p, user); wr_str(&p, vhost);
    return cluster_propose(n, CL_OP_CLEAR_PERM, buf, (size_t)(p - buf));
}
