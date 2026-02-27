/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <string.h>
#include <json-c/json.h>
#include <stdlib.h>
#include <stdio.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <stdatomic.h>
#include <pinggy/pinggy.h>
#include <flock/flock-common.h>
#include "flock/flock-gateway.h"
#include <margo-timer.h>
#include "pinggy-gateway.h"

#define TUNNEL_TIMEOUT_MS_DEFAULT 10000
#define TIMER_INTERVAL_MS 1000

static int resolve_host_ipv4(const char *host, char *ipbuf, size_t ipbufsz)
{
    struct addrinfo hints, *res = NULL;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;

    if (getaddrinfo(host, NULL, &hints, &res) != 0)
        return -1;

    struct sockaddr_in *addr = (struct sockaddr_in *)res->ai_addr;

    if (!inet_ntop(AF_INET, &addr->sin_addr, ipbuf, ipbufsz)) {
        freeaddrinfo(res);
        return -1;
    }

    freeaddrinfo(res);
    return 0;
}

/*
 * Parse IP and port from a Mercury address.
 *
 * Supported formats:
 *   protocol://host:port              e.g.  tcp://1.2.3.4:5555
 *   transport+protocol://host:port    e.g.  zmq+tcp://1.2.3.4:5555
 *   protocol://[ipv6]:port            e.g.  tcp://[::1]:5555
 *
 * If prefix is non-NULL and prefix_size > 0, the address prefix
 * (everything up to and including "://") is written there,
 * e.g. "tcp://", "zmq+tcp://", "http+http://".
 */
static int parse_ip_and_port(const char *address, char *ip, size_t ip_size,
                             int *port, char *prefix, size_t prefix_size)
{
    if (!address || !ip || !port || ip_size == 0)
        return -1;

    const char *p = strstr(address, "://");
    if (!p)
        return -1;

    p += 3; // skip "://"

    // ---------- Store prefix (everything up to and including "://") ----------
    if (prefix && prefix_size > 0) {
        size_t plen = (size_t)(p - address);
        if (plen >= prefix_size)
            plen = prefix_size - 1;
        memcpy(prefix, address, plen);
        prefix[plen] = '\0';
    }

    const char *host_start = p;
    const char *host_end = NULL;
    const char *port_start = NULL;

    // ---------- IPv6 case: [addr]:port ----------
    if (*host_start == '[') {
        host_start++; // skip '['
        host_end = strchr(host_start, ']');
        if (!host_end)
            return -1;

        if (host_end[1] != ':')
            return -1;

        port_start = host_end + 2;
    }

    // ---------- IPv4 / hostname case ----------
    else {
        host_end = strrchr(host_start, ':');
        if (!host_end)
            return -1;

        port_start = host_end + 1;
    }

    // ---------- Copy IP ----------
    size_t host_len = (size_t)(host_end - host_start);
    if (host_len == 0 || host_len >= ip_size)
        return -1;

    memcpy(ip, host_start, host_len);
    ip[host_len] = '\0';

    // ---------- Parse port ----------
    char *endptr;
    long pval = strtol(port_start, &endptr, 10);

    if (endptr == port_start || *endptr != '\0')
        return -1;

    if (pval <= 0 || pval > 65535)
        return -1;

    *port = (int)pval;
    return 0;
}

typedef struct pinggy_gateway_context {
    struct json_object* config;
    margo_instance_id   mid;
    pinggy_ref_t        tunnel_ref;
    pinggy_ref_t        config_ref;
    margo_timer_t       resume_timer;
    atomic_bool         shutting_down;
    char                public_addr[512];
    char                local_addr[256];
    char                addr_prefix[64];
    char                pinggy_proto[16];
} pinggy_gateway_context;

/*
 * Derive the Pinggy tunnel protocol from the Margo address prefix.
 *
 * - Prefixes containing "http" (e.g. "http://", "http+http://") → "http"
 * - Prefixes containing "udp" or "quic"                         → "udp"
 * - Everything else (e.g. "tcp://", "zmq+tcp://", "ofi+verbs://") → "tcp"
 */
static const char* derive_pinggy_proto(const char* addr_prefix)
{
    if (strstr(addr_prefix, "http"))
        return "http";
    if (strstr(addr_prefix, "udp") || strstr(addr_prefix, "quic"))
        return "udp";
    return "tcp";
}

/* ---------- libpinggy callbacks ---------- */

static void on_tunnel_established(pinggy_void_p_t user_data,
                                  pinggy_ref_t tunnel_ref,
                                  pinggy_len_t num_urls,
                                  pinggy_char_p_p_t urls)
{
    (void)tunnel_ref;
    pinggy_gateway_context* ctx = (pinggy_gateway_context*)user_data;

    for (pinggy_len_t i = 0; i < num_urls; i++) {
        margo_trace(ctx->mid, "[flock:pinggy] Tunnel URL: %s", urls[i]);

        /* Look for the first URL matching the tunnel protocol */
        char expected_prefix[24];
        snprintf(expected_prefix, sizeof(expected_prefix), "%s://", ctx->pinggy_proto);
        size_t expected_len = strlen(expected_prefix);
        if (strncmp(urls[i], expected_prefix, expected_len) != 0)
            continue;

        /* Parse hostname:port from the tunnel URL */
        char hostname[256] = {0};
        int port = 0;
        /* parse_ip_and_port expects "proto://host:port" format, tcp:// works */
        char dummy_prefix[16] = {0};
        if (parse_ip_and_port(urls[i], hostname, sizeof(hostname),
                              &port, dummy_prefix, sizeof(dummy_prefix)) != 0) {
            margo_error(ctx->mid, "[flock:pinggy] Could not parse tunnel URL: %s", urls[i]);
            continue;
        }

        /* Resolve hostname to IPv4 */
        char resolved_ip[64] = {0};
        if (resolve_host_ipv4(hostname, resolved_ip, sizeof(resolved_ip)) != 0) {
            margo_error(ctx->mid, "[flock:pinggy] Could not resolve hostname: %s", hostname);
            continue;
        }

        snprintf(ctx->public_addr, sizeof(ctx->public_addr),
                 "%s%s:%d", ctx->addr_prefix, resolved_ip, port);
        margo_trace(ctx->mid, "[flock:pinggy] Public address: %s", ctx->public_addr);
        return;
    }

    margo_error(ctx->mid, "[flock:pinggy] No %s:// URL found in tunnel URLs",
                ctx->pinggy_proto);
}

static void on_tunnel_failed(pinggy_void_p_t user_data,
                             pinggy_ref_t tunnel_ref,
                             pinggy_const_char_p_t msg)
{
    (void)tunnel_ref;
    pinggy_gateway_context* ctx = (pinggy_gateway_context*)user_data;
    margo_error(ctx->mid, "[flock:pinggy] Tunnel failed: %s", msg);
}

static void on_disconnected(pinggy_void_p_t user_data,
                            pinggy_ref_t tunnel_ref,
                            pinggy_const_char_p_t error,
                            pinggy_len_t msg_size,
                            pinggy_char_p_p_t msg)
{
    (void)tunnel_ref;
    (void)msg_size;
    (void)msg;
    pinggy_gateway_context* ctx = (pinggy_gateway_context*)user_data;
    margo_error(ctx->mid, "[flock:pinggy] Tunnel disconnected: %s", error);
}

static void on_tunnel_error(pinggy_void_p_t user_data,
                            pinggy_ref_t tunnel_ref,
                            pinggy_uint32_t error_no,
                            pinggy_const_char_p_t error,
                            pinggy_bool_t recoverable)
{
    (void)tunnel_ref;
    (void)error_no;
    pinggy_gateway_context* ctx = (pinggy_gateway_context*)user_data;
    margo_error(ctx->mid, "[flock:pinggy] Tunnel error (recoverable=%d): %s",
                recoverable, error);
}

/* ---------- margo_timer callback ---------- */

static void resume_timer_callback(void* uargs)
{
    pinggy_gateway_context* ctx = (pinggy_gateway_context*)uargs;
    if (atomic_load(&ctx->shutting_down))
        return;

    pinggy_tunnel_resume_timeout(ctx->tunnel_ref, 0);

    if (!atomic_load(&ctx->shutting_down))
        margo_timer_start(ctx->resume_timer, TIMER_INTERVAL_MS);
}

/* ---------- gateway interface ---------- */

static flock_return_t pinggy_gateway_create(
        flock_gateway_init_args_t* args,
        void** context)
{
    pinggy_gateway_context* ctx = (pinggy_gateway_context*)calloc(1, sizeof(*ctx));
    if (!ctx) return FLOCK_ERR_ALLOCATION;

    ctx->mid = args->mid;
    ctx->config = json_object_new_object();
    atomic_init(&ctx->shutting_down, false);

    /* Get Margo self address */
    hg_addr_t self_addr = HG_ADDR_NULL;
    hg_size_t addr_str_size = sizeof(ctx->local_addr);

    margo_addr_self(args->mid, &self_addr);
    margo_addr_to_string(args->mid, ctx->local_addr, &addr_str_size, self_addr);
    margo_addr_free(args->mid, self_addr);
    margo_trace(args->mid, "[flock:pinggy] Margo address is %s", ctx->local_addr);

    /* Parse IP/port/prefix from Margo address */
    char local_ip[128] = {0};
    int local_port = 0;
    if (parse_ip_and_port(ctx->local_addr, local_ip, sizeof(local_ip),
                          &local_port, ctx->addr_prefix, sizeof(ctx->addr_prefix)) != 0) {
        margo_error(args->mid, "[flock:pinggy] Could not parse IP/PORT from Margo address");
        json_object_put(ctx->config);
        free(ctx);
        return FLOCK_ERR_OTHER;
    }
    margo_trace(args->mid, "[flock:pinggy] Margo address has IP=%s and PORT=%d (prefix=%s)",
                local_ip, local_port, ctx->addr_prefix);

    /* Suppress libpinggy debug output */
    pinggy_set_log_enable(pinggy_false);

    /* Create pinggy config */
    ctx->config_ref = pinggy_create_config();
    if (ctx->config_ref == INVALID_PINGGY_REF) {
        margo_error(args->mid, "[flock:pinggy] Failed to create pinggy config");
        json_object_put(ctx->config);
        free(ctx);
        return FLOCK_ERR_OTHER;
    }

    /* Set server address (default or from JSON config) */
    const char* server_address = "a.pinggy.io:443";
    if (args->config) {
        struct json_object* server_obj = json_object_object_get(args->config, "server_address");
        if (server_obj && json_object_is_type(server_obj, json_type_string))
            server_address = json_object_get_string(server_obj);
    }
    pinggy_config_set_server_address(ctx->config_ref, (pinggy_char_p_t)server_address);

    /* Set token if provided in JSON config */
    if (args->config) {
        struct json_object* token_obj = json_object_object_get(args->config, "token");
        if (token_obj && json_object_is_type(token_obj, json_type_string)) {
            const char* token = json_object_get_string(token_obj);
            pinggy_config_set_token(ctx->config_ref, (pinggy_char_p_t)token);
        }
    }

    /* Set timeout (default or from JSON config) */
    long long timeout_ms = TUNNEL_TIMEOUT_MS_DEFAULT;
    if (args->config) {
        struct json_object* timeout_obj = json_object_object_get(args->config, "timeout");
        if (timeout_obj && json_object_is_type(timeout_obj, json_type_int))
            timeout_ms = json_object_get_int64(timeout_obj);
    }

    /* Determine tunnel protocol: auto-detect from address prefix, allow override */
    const char* proto = derive_pinggy_proto(ctx->addr_prefix);
    if (args->config) {
        struct json_object* proto_obj = json_object_object_get(args->config, "protocol");
        if (proto_obj && json_object_is_type(proto_obj, json_type_string))
            proto = json_object_get_string(proto_obj);
    }
    snprintf(ctx->pinggy_proto, sizeof(ctx->pinggy_proto), "%s", proto);

    /* Add forwarding rule */
    char fwd_spec[256];
    snprintf(fwd_spec, sizeof(fwd_spec), "%s://%s:%d", ctx->pinggy_proto, local_ip, local_port);
    pinggy_config_add_forwarding_simple(ctx->config_ref, fwd_spec);
    margo_trace(args->mid, "[flock:pinggy] Forwarding to %s", fwd_spec);

    /* Create the tunnel */
    ctx->tunnel_ref = pinggy_tunnel_initiate(ctx->config_ref);
    if (ctx->tunnel_ref == INVALID_PINGGY_REF) {
        margo_error(args->mid, "[flock:pinggy] Failed to initiate tunnel");
        pinggy_free_ref(ctx->config_ref);
        json_object_put(ctx->config);
        free(ctx);
        return FLOCK_ERR_OTHER;
    }

    /* Register callbacks */
    pinggy_tunnel_set_on_tunnel_established_callback(
        ctx->tunnel_ref, on_tunnel_established, ctx);
    pinggy_tunnel_set_on_tunnel_failed_callback(
        ctx->tunnel_ref, on_tunnel_failed, ctx);
    pinggy_tunnel_set_on_disconnected_callback(
        ctx->tunnel_ref, on_disconnected, ctx);
    pinggy_tunnel_set_on_tunnel_error_callback(
        ctx->tunnel_ref, on_tunnel_error, ctx);

    /* Start tunnel in non-blocking mode */
    pinggy_tunnel_start_non_blocking(ctx->tunnel_ref);

    /* Poll until tunnel is established or timeout */
    double deadline = ABT_get_wtime() + timeout_ms / 1000.0;
    for (;;) {
        pinggy_tunnel_resume_timeout(ctx->tunnel_ref, 1000);
        if (ctx->public_addr[0] != '\0')
            break;
        if (ABT_get_wtime() >= deadline)
            break;
    }

    if (ctx->public_addr[0] == '\0') {
        margo_error(args->mid, "[flock:pinggy] Tunnel did not establish within timeout");
        pinggy_tunnel_stop(ctx->tunnel_ref);
        pinggy_free_ref(ctx->tunnel_ref);
        pinggy_free_ref(ctx->config_ref);
        json_object_put(ctx->config);
        free(ctx);
        return FLOCK_ERR_OTHER;
    }

    margo_trace(args->mid, "[flock:pinggy] Tunnel established, public address: %s",
                ctx->public_addr);

    /* Create margo timer for ongoing event processing */
    margo_timer_create(args->mid, resume_timer_callback, ctx, &ctx->resume_timer);
    margo_timer_start(ctx->resume_timer, TIMER_INTERVAL_MS);

    *context = ctx;
    return FLOCK_SUCCESS;
}

static flock_return_t pinggy_gateway_destroy(void* ctx_ptr)
{
    pinggy_gateway_context* ctx = (pinggy_gateway_context*)ctx_ptr;
    if (!ctx) return FLOCK_SUCCESS;

    atomic_store(&ctx->shutting_down, true);

    /* Cancel and destroy timer */
    if (ctx->resume_timer) {
        margo_timer_cancel(ctx->resume_timer);
        margo_timer_destroy(ctx->resume_timer);
    }

    /* Stop and free tunnel */
    pinggy_tunnel_stop(ctx->tunnel_ref);
    pinggy_free_ref(ctx->tunnel_ref);
    pinggy_free_ref(ctx->config_ref);

    json_object_put(ctx->config);
    free(ctx);
    return FLOCK_SUCCESS;
}

static flock_return_t pinggy_gateway_get_config(
    void* ctx, void (*fn)(void*, const struct json_object*), void* uargs)
{
    pinggy_gateway_context* context = (pinggy_gateway_context*)ctx;
    fn(uargs, context->config);
    return FLOCK_SUCCESS;
}

static const char* pinggy_gateway_get_public_address(void* ctx)
{
    pinggy_gateway_context* context = (pinggy_gateway_context*)ctx;
    return context ? context->public_addr : NULL;
}

static const char* pinggy_gateway_get_local_address(void* ctx)
{
    pinggy_gateway_context* context = (pinggy_gateway_context*)ctx;
    return context ? context->local_addr : NULL;
}

static flock_gateway_impl pinggy_gateway = {
    .name               = "pinggy",
    .init_gateway       = pinggy_gateway_create,
    .destroy_gateway    = pinggy_gateway_destroy,
    .get_config         = pinggy_gateway_get_config,
    .get_public_address = pinggy_gateway_get_public_address,
    .get_local_address  = pinggy_gateway_get_local_address
};

flock_return_t flock_register_pinggy_gateway(void)
{
    return flock_register_gateway(&pinggy_gateway);
}
