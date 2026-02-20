/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#define _XOPEN_SOURCE 600
#include <string.h>
#include <json-c/json.h>
#include <fcntl.h>
#include <spawn.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <sys/wait.h>
#include <errno.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <poll.h>
#include <termios.h>
#include <regex.h>
#include <time.h>
#include <flock/flock-common.h>
#include "flock/flock-gateway.h"
#include "pinggy-gateway.h"

extern char **environ;

#define TUNNEL_TIMEOUT_MS 30000
#define READ_BUF 4096
#define ACCUM_BUF 16384

typedef struct {
    pid_t pid;
    int   pty_master_fd;
    char  public_host[256];
    int   public_port;
    char  public_ip[64];
    char  public_url[512];
    char  public_addr[512];
    char  local_addr[256];
} ssh_tunnel_t;

static long long now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

static void strip_ansi(char *dst, size_t dstsz, const char *src)
{
    size_t di = 0;

    for (size_t i = 0; src[i] && di + 1 < dstsz; i++) {
        if (src[i] == '\x1b') {
            i++;
            if (src[i] == '[') {
                while (src[i] && !isalpha((unsigned char)src[i]))
                    i++;
            }
            continue;
        }
        dst[di++] = src[i];
    }

    dst[di] = '\0';
}

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

static void kill_process_group(pid_t pid)
{
    if (pid > 0)
        kill(-pid, SIGTERM);
}

static int start_ssh_tunnel(const char *ip, int port, ssh_tunnel_t *out)
{
    if (!ip || !out) {
        errno = EINVAL;
        return -1;
    }

    out->public_host[0] = 0;
    out->public_port    = 0;
    out->public_ip[0]   = 0;
    out->public_url[0]  = 0;
    out->public_addr[0] = 0;

    /* ---------- create PTY ---------- */

    int master_fd = posix_openpt(O_RDWR | O_NOCTTY);
    if (master_fd < 0)
        return -1;

    if (grantpt(master_fd) != 0 || unlockpt(master_fd) != 0) {
        close(master_fd);
        return -1;
    }

    char *slave_name = ptsname(master_fd);
    if (!slave_name) {
        close(master_fd);
        return -1;
    }

    int slave_fd = open(slave_name, O_RDWR | O_NOCTTY);
    if (slave_fd < 0) {
        close(master_fd);
        return -1;
    }

    /* ---------- make master nonblocking ---------- */

    int flags = fcntl(master_fd, F_GETFL, 0);
    fcntl(master_fd, F_SETFL, flags | O_NONBLOCK);

    /* ---------- spawn ssh ---------- */

    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);

    posix_spawn_file_actions_adddup2(&actions, slave_fd, STDIN_FILENO);
    posix_spawn_file_actions_adddup2(&actions, slave_fd, STDOUT_FILENO);
    posix_spawn_file_actions_adddup2(&actions, slave_fd, STDERR_FILENO);
    posix_spawn_file_actions_addclose(&actions, master_fd);

    posix_spawnattr_t attr;
    posix_spawnattr_init(&attr);

    /*
    short spawn_flags = POSIX_SPAWN_SETPGROUP;
    posix_spawnattr_setflags(&attr, spawn_flags);
    posix_spawnattr_setpgroup(&attr, 0);
    */
    short spawn_flags = 0;
    posix_spawnattr_setflags(&attr, spawn_flags);

    char remote_arg[256];
    snprintf(remote_arg, sizeof(remote_arg), "0:%s:%d", ip, port);

    char *argv[] = {
        "ssh",
        "-p", "443",
        "-R", remote_arg,
        "-o", "ExitOnForwardFailure=yes",
        "-o", "ServerAliveInterval=60",
        "-o", "ServerAliveCountMax=3",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "LogLevel=ERROR",
        "tcp@free.pinggy.io",
        NULL
    };

    pid_t pid;
    int status = posix_spawnp(&pid, "ssh", &actions, &attr, argv, environ);

    posix_spawn_file_actions_destroy(&actions);
    posix_spawnattr_destroy(&attr);
    close(slave_fd);

    if (status != 0) {
        close(master_fd);
        errno = status;
        return -1;
    }

    out->pid = pid;
    out->pty_master_fd = master_fd;

    /* ---------- prepare regex ---------- */

    regex_t re;
    regcomp(
        &re,
        "(tcp|http|https)://([a-zA-Z0-9.-]+):([0-9]+)",
        REG_EXTENDED);

    char readbuf[READ_BUF];
    char clean[READ_BUF];
    char accum[ACCUM_BUF];
    accum[0] = '\0';

    long long deadline = now_ms() + TUNNEL_TIMEOUT_MS;

    struct pollfd pfd = {
        .fd = master_fd,
        .events = POLLIN
    };

    /* ---------- read loop with timeout ---------- */

    while (now_ms() < deadline) {
        int remaining = (int)(deadline - now_ms());
        if (remaining < 0) remaining = 0;

        int pr = poll(&pfd, 1, remaining);
        if (pr < 0) {
            if (errno == EINTR)
                continue;
            goto fail;
        }

        if (pr == 0)
            break; /* timeout */

        ssize_t n = read(master_fd, readbuf, sizeof(readbuf) - 1);
        if (n <= 0)
            continue;

        readbuf[n] = '\0';

        strip_ansi(clean, sizeof(clean), readbuf);

        strncat(accum, clean, sizeof(accum) - strlen(accum) - 1);

        regmatch_t m[4];
        if (regexec(&re, accum, 4, m, 0) == 0) {
            int hlen = m[2].rm_eo - m[2].rm_so;
            int plen = m[3].rm_eo - m[3].rm_so;

            snprintf(out->public_host, sizeof(out->public_host),
                     "%.*s", hlen, accum + m[2].rm_so);

            char portbuf[32];
            snprintf(portbuf, sizeof(portbuf),
                     "%.*s", plen, accum + m[3].rm_so);

            out->public_port = atoi(portbuf);

            snprintf(out->public_url, sizeof(out->public_url),
                     "%.*s", (int)(m[0].rm_eo - m[0].rm_so),
                     accum + m[0].rm_so);

            resolve_host_ipv4(out->public_host,
                              out->public_ip,
                              sizeof(out->public_ip));

            snprintf(out->public_addr, sizeof(out->public_addr),
                    "tcp://%s:%d", out->public_ip, out->public_port);

            regfree(&re);
            return 0;
        }
    }

fail:
    regfree(&re);
    kill_process_group(pid);
    waitpid(pid, NULL, 0);
    close(master_fd);
    errno = ETIMEDOUT;
    return -1;
}

static int stop_ssh_tunnel(ssh_tunnel_t *t)
{
    if (!t)
        return -1;

    kill_process_group(t->pid);
    waitpid(t->pid, NULL, 0);

    if (t->pty_master_fd >= 0)
        close(t->pty_master_fd);

    return 0;
}

static int parse_ip_and_port(const char *address, char *ip, size_t ip_size, int *port)
{
    if (!address || !ip || !port || ip_size == 0)
        return -1;

    const char *p = strstr(address, "://");
    if (!p)
        return -1;

    p += 3; // skip "://"

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
    ssh_tunnel_t        tunnel;
} pinggy_gateway_context;


static flock_return_t pinggy_gateway_create(
        flock_gateway_init_args_t* args,
        void** context)
{
    int ret;
    pinggy_gateway_context* ctx = (pinggy_gateway_context*)calloc(1, sizeof(*ctx));
    if(!ctx) return FLOCK_ERR_ALLOCATION;

    ctx->config = json_object_new_object();

    hg_addr_t self_addr = HG_ADDR_NULL;
    hg_size_t addr_str_size = 256;

    margo_addr_self(args->mid, &self_addr);
    margo_addr_to_string(args->mid, ctx->tunnel.local_addr, &addr_str_size, self_addr);
    margo_addr_free(args->mid, self_addr);
    margo_trace(args->mid, "[flock:pinggy] Margo address is %s", ctx->tunnel.local_addr);

    char local_ip[128] = {0};
    int local_port = 0;
    ret = parse_ip_and_port(ctx->tunnel.local_addr, local_ip, 128, &local_port);
    if(ret == 0) {
        margo_trace(args->mid, "[flock:pinggy] Margo address has IP=%s and PORT=%d", local_ip, local_port);
    } else {
        margo_error(args->mid, "[flock:pinggy] Could not parse IP/PORT from Margo address");
        free(ctx);
        return FLOCK_ERR_OTHER;
    }

    ret = start_ssh_tunnel(local_ip, local_port, &ctx->tunnel);
    if(ret == 0) {
        margo_trace(args->mid, "[flock:pinggy] SSH tunnel started, public address is %s (%s)",
                    ctx->tunnel.public_addr, ctx->tunnel.public_url);
    } else {
        margo_error(args->mid, "[flock:pinggy] Could not start SSH tunnel");
        free(ctx);
        return FLOCK_ERR_OTHER;
    }

    *context = ctx;
    return FLOCK_SUCCESS;
}

static flock_return_t pinggy_gateway_destroy(void* ctx)
{
    pinggy_gateway_context* context = (pinggy_gateway_context*)ctx;
    stop_ssh_tunnel(&context->tunnel);
    json_object_put(context->config);
    free(context);
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
    return context ? context->tunnel.public_addr : NULL;
}

static const char* pinggy_gateway_get_local_address(void* ctx)
{
    pinggy_gateway_context* context = (pinggy_gateway_context*)ctx;
    return context ? context->tunnel.local_addr : NULL;
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
