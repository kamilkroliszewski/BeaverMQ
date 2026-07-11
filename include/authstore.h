/*
 * authstore.h - replicated access-control store for BeaverMQ.
 *
 * Holds the cluster's virtual hosts, users (salted-SHA-256 password hashes +
 * tags) and per-(user, vhost) permissions (configure/write/read regex patterns,
 * RabbitMQ-style). It is the apply target for the CL_OP_*_VHOST / *_USER / *_PERM
 * Raft ops, exactly like the broker is for topology - so the whole table is
 * replicated and consistent on every node, and survives restart + compaction
 * (the ops are carried in the cluster's topology snapshot).
 *
 * Threading: the apply_* mutators run on the cluster loop; the query functions
 * (verify/check/exists) run on worker threads. A single rwlock guards all of it.
 */
#ifndef BEAVERMQ_AUTHSTORE_H
#define BEAVERMQ_AUTHSTORE_H

#include <stddef.h>
#include <stdint.h>

typedef struct authstore authstore_t;

/* Permission kinds (RabbitMQ's triple). */
typedef enum {
    AUTH_CONFIGURE = 0,  /* declare/delete a queue or exchange */
    AUTH_WRITE     = 1,  /* publish to an exchange; bind source */
    AUTH_READ      = 2,  /* consume/get from a queue; bind dest  */
} auth_perm_t;

/* User tag bits (administrative capability). */
#define AUTH_TAG_NONE          0u
#define AUTH_TAG_ADMINISTRATOR 1u   /* may manage users/vhosts/perms */
#define AUTH_TAG_MANAGEMENT    2u   /* may view the management API   */

authstore_t *authstore_new(void);
void         authstore_free(authstore_t *s);

/* ---- mutators (cluster loop; idempotent upserts) ------------------------- */
int authstore_add_vhost(authstore_t *s, const char *vhost);
int authstore_del_vhost(authstore_t *s, const char *vhost);
int authstore_add_user(authstore_t *s, const char *user,
                       const char *pass_hash, uint32_t tags);
int authstore_del_user(authstore_t *s, const char *user);
int authstore_set_perm(authstore_t *s, const char *user, const char *vhost,
                       const char *configure, const char *write, const char *read);
int authstore_clear_perm(authstore_t *s, const char *user, const char *vhost);

/* ---- queries (worker threads) -------------------------------------------- */
/* 1 if the cluster has NO users yet (fresh/unconfigured: auth is open until the
 * first user is created, so a brand-new cluster is reachable to bootstrap). */
int authstore_is_open(authstore_t *s);
int authstore_vhost_exists(authstore_t *s, const char *vhost);
/* 1 if `user` has any permission entry for `vhost` (i.e. may use it at all). */
int authstore_can_access_vhost(authstore_t *s, const char *user, const char *vhost);
/* 1 if `user` exists and `password` matches its stored hash. */
int authstore_verify(authstore_t *s, const char *user, const char *password);
uint32_t authstore_user_tags(authstore_t *s, const char *user);
/* 1 if `user` is allowed `kind` access to `object` in `vhost`. */
int authstore_check(authstore_t *s, const char *user, const char *vhost,
                    auth_perm_t kind, const char *object);

/* ---- helpers ------------------------------------------------------------- */
/* Produce a storable salted-SHA-256 hash for `password` (hex). out_cap >= 80. */
void authstore_hash_password(const char *password, char *out, size_t out_cap);

/* ---- enumeration (management API; callback under the read lock) ----------- */
typedef void (*authstore_vhost_fn)(const char *vhost, void *ctx);
typedef void (*authstore_user_fn)(const char *user, uint32_t tags, void *ctx);
typedef void (*authstore_perm_fn)(const char *user, const char *vhost,
                                  const char *configure, const char *write,
                                  const char *read, void *ctx);
void authstore_foreach_vhost(authstore_t *s, authstore_vhost_fn fn, void *ctx);
void authstore_foreach_user(authstore_t *s, authstore_user_fn fn, void *ctx);
void authstore_foreach_perm(authstore_t *s, authstore_perm_fn fn, void *ctx);

#endif /* BEAVERMQ_AUTHSTORE_H */
