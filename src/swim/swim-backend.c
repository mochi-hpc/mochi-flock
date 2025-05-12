/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <string.h>
#include <stdlib.h>
#include <json-c/json.h>
#include <margo-timer.h>
#include "flock/flock-backend.h"
#include "flock/flock-group-view.h"
#include "../provider.h"
#include "swim-backend.h"
#include "utlist.h"

#define PROT_PERIOD   1000.0
#define DPING_TIMEOUT  500.0
#define SUBGROUP_SIZE    2
#define SUSP_TIMEOUT     2
#define PB_BUFF_SIZE     4
#define PACKET_LOSS     50.0

#define SWIM_MEMBER_STATUS_VALUES \
    X(SWIM_MEMBER_ALIVE) \
    X(SWIM_MEMBER_SUSPECT) \
    X(SWIM_MEMBER_DEAD)

#define X(__status__) __status__,
enum { SWIM_MEMBER_STATUS_VALUES };
#undef X

static const char* const swim_member_statuses[] = {
#define X(__status__) #__status__,
    SWIM_MEMBER_STATUS_VALUES
#undef X
};

typedef struct swim_member_state {
    uint16_t    inc_nr;
    uint8_t     status;
} swim_member_state;

typedef struct swim_update {
    char*               addr;
    uint16_t            provider_id;
    swim_member_state   state;
} swim_update;

typedef struct suspect_member_link {
    struct swim_context* ctx;
    char* addr;
    uint16_t provider_id;
    margo_timer_t dead_timer;
    struct suspect_member_link* next;
} suspect_member_link;

typedef struct swim_context {
    margo_instance_id       mid;
    flock_group_view_t      view;
    margo_timer_t           prot_timer;
    /* SWIM protocol internal state */
    char*                   self_addr_string;
    uint16_t                self_provider_id;
    uint16_t                self_inc_nr;
    int*                    target_list;
    size_t                  target_list_ndx;
    suspect_member_link*    suspect_list;
    /* swim protocol dping/iping RPCs */
    hg_id_t                 dping_rpc_id;
    hg_id_t                 iping_rpc_id;
} swim_context;

/* SWIM dping RPCs */
static flock_return_t dping_send(swim_context* ctx,
    const char* target_address, uint16_t target_provider_id);
static DECLARE_MARGO_RPC_HANDLER(dping_rpc_ult)
static void dping_rpc_ult(hg_handle_t h);
/* SWIM iping RPCs */
static flock_return_t iping_send(swim_context* ctx,
    char* dping_target_address, uint16_t dping_target_provider_id);
static DECLARE_MARGO_RPC_HANDLER(iping_rpc_ult)
static void iping_rpc_ult(hg_handle_t h);

MERCURY_GEN_PROC(dping_in_t,
    ((hg_const_string_t)(source_address))\
    ((uint16_t)(source_provider_id)))
MERCURY_GEN_PROC(dping_out_t,
    ((uint32_t)(ret))) // XXX remove eventually
MERCURY_GEN_PROC(iping_in_t,
    ((hg_const_string_t)(source_address))\
    ((uint16_t)(source_provider_id))\
    ((hg_const_string_t)(target_address))\
    ((uint16_t)(target_provider_id)))
MERCURY_GEN_PROC(iping_out_t,
    ((uint32_t)(ret)))

/* SWIM group membership protocol */
static void protocol_timer_callback(void* args);
static void mark_suspect(swim_context* ctx, char* suspect_addr,
    uint16_t suspect_provider_id, uint16_t suspect_inc_nr);
static void mark_dead(swim_context* ctx);
static void shuffle_ping_targets(swim_context* ctx);


static flock_return_t swim_create_group(
        flock_backend_init_args_t* args,
        void** context)
{
    flock_return_t ret = FLOCK_SUCCESS;
    swim_context* ctx = NULL;

    // XXX check that the initial view has at least one member
    if(args->initial_view.members.size == 0) {
        margo_error(args->mid, "[flock] SWIM backend requires at least one member");
        return FLOCK_ERR_INVALID_ARGS;
    }

    margo_instance_id mid = args->mid;
    hg_addr_t self_addr;
    char      self_addr_string[256];
    hg_size_t self_addr_size = 256;
    hg_return_t hret = margo_addr_self(mid, &self_addr);
    if(hret != HG_SUCCESS) {
        ret = FLOCK_ERR_FROM_MERCURY;
        goto error;
    }

    hret = margo_addr_to_string(mid, self_addr_string, &self_addr_size, self_addr);
    margo_addr_free(mid, self_addr);
    if(hret != HG_SUCCESS) {
        ret = FLOCK_ERR_FROM_MERCURY;
        goto error;
    }

    /* allocate context */
    ctx = (swim_context*)calloc(1, sizeof(*ctx));
    if(!ctx) {
        ret = FLOCK_ERR_ALLOCATION;
        goto error;
    }
    ctx->mid = mid;
    ctx->self_addr_string = strdup(self_addr_string);
    ctx->self_provider_id = args->provider_id;

    /* move the initial view in the context */
    FLOCK_GROUP_VIEW_MOVE(&args->initial_view, &ctx->view);

    /* allocate the list of ping targets */
    ctx->target_list = calloc(ctx->view.members.size-1, sizeof(*(ctx->target_list)));
    if(!ctx->target_list) {
        ret = FLOCK_ERR_ALLOCATION;
        free(ctx->self_addr_string);
        free(ctx);
        goto error;
    }

    for(size_t i = 0, j = 0; i < ctx->view.members.size; ++i) {
        flock_member_t* member = &ctx->view.members.data[i];
        if((strcmp(member->address, ctx->self_addr_string) != 0) ||
            (member->provider_id != ctx->self_provider_id))
            ctx->target_list[j++] = i;
        member->extra.data  = calloc(1, sizeof(struct swim_member_state));
        member->extra.free  = free;
    }

    /* randomly shuffle list of ping recipients */
    shuffle_ping_targets(ctx);

    /* register the RPCs */
    ctx->dping_rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "flock_swim_dping",
        dping_in_t, dping_out_t, dping_rpc_ult,
        args->provider_id, args->pool);
    margo_register_data(mid, ctx->dping_rpc_id, ctx, NULL);
    ctx->iping_rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "flock_swim_iping",
        iping_in_t, iping_out_t, iping_rpc_ult,
        args->provider_id, args->pool);
    margo_register_data(mid, ctx->iping_rpc_id, ctx, NULL);

    /* create protocol timer */
    margo_timer_create(mid, protocol_timer_callback, ctx, &ctx->prot_timer);
    margo_timer_start(ctx->prot_timer, PROT_PERIOD);

    for(size_t i = 0; i < ctx->view.members.size; ++i) {
        flock_member_t* member = &ctx->view.members.data[i];
        swim_member_state* state = (member_state*)(member->extra.data);
        fprintf(stderr, "member %lu (%s:%d): inc_nr=%u status=%s\n", i, member->address, member->provider_id, state->inc_nr, swim_member_statuses[state->status]);
    }

    *context = ctx;

    fprintf(stderr, "%s:%u INIT\n", ctx->self_addr_string, ctx->self_provider_id);

error:
    return ret;
}

//
// SWIM protocol
//

static void protocol_timer_callback(void* args)
{
    swim_context* context = (swim_context*)args;

    /* restart protocol timer for next period */
    if(context->prot_timer != MARGO_TIMER_NULL)
        margo_timer_start(context->prot_timer, PROT_PERIOD);

    FLOCK_GROUP_VIEW_LOCK(&context->view);
    /* get ping target info */
    int dping_target_ndx = context->target_list[context->target_list_ndx++];
    if(context->target_list_ndx == (context->view.members.size-1)) {
        /* after a complete traversal of the random target list, reshuffle it */
        shuffle_ping_targets(context);
    }
    flock_member_t* target = &context->view.members.data[dping_target_ndx];
    swim_member_state* state = (swim_member_state*)(target->extra.data);
    char *target_address = strdup(target->address);
    uint16_t target_provider_id = target->provider_id;
    uint16_t target_inc_nr = state->inc_nr;
    FLOCK_GROUP_VIEW_UNLOCK(&context->view);
    if(!target_address)
        return;

    /* try to reach current ping target */
    if(dping_send(context, target_address, target_provider_id) != FLOCK_SUCCESS) {
        /* XXX dping attempt failed, use ipings to reach the target */
        if(iping_send(context, target_address, target_provider_id) != FLOCK_SUCCESS) {
            /* XXX iping attempt failed, mark as suspect and add to update list */
            mark_suspect(context, target_address, target_provider_id, target_inc_nr);
        }
    }

    free(target_address);
    return;
}

//
// dping RPC code
//

// XXX FLOCK_SUCCESS or other error codes
static flock_return_t dping_send(swim_context *ctx,
    const char *target_address, uint16_t target_provider_id)
{
    /* lookup mercury addr and create dping handle */
    hg_addr_t addr;
    hg_handle_t handle;
    hg_return_t hret = margo_addr_lookup(ctx->mid, target_address, &addr);
    if(hret != HG_SUCCESS) {
        margo_warning(ctx->mid, "[flock] Failed to lookup SWIM dping target addr");
        return FLOCK_ERR_FROM_MERCURY;
    }
    hret = margo_create(
            ctx->mid,
            addr,
            ctx->dping_rpc_id,
            &handle);
    if(hret != HG_SUCCESS) {
        margo_warning(ctx->mid, "[flock] Failed to create SWIM dping RPC handle");
        margo_addr_free(ctx->mid, addr);
        return FLOCK_ERR_FROM_MERCURY;
    }

    /* send dping request to target */
    fprintf(stderr, "[%.4lf] %s:%u dping SEND   to %s:%u\n", ABT_get_wtime(), ctx->self_addr_string, ctx->self_provider_id, target_address, target_provider_id);
    dping_in_t dping_in = {
        .source_address     = ctx->self_addr_string,
        .source_provider_id = ctx->self_provider_id,
    };
    hret = margo_provider_forward_timed(target_provider_id, handle,
        &dping_in, DPING_TIMEOUT);
    if(hret != HG_SUCCESS) {
        margo_warning(ctx->mid, "[flock] Failed to forward SWIM dping RPC");
        margo_destroy(handle);
        margo_addr_free(ctx->mid, addr);
        return FLOCK_ERR_FROM_MERCURY;
    }

    dping_out_t dping_out = {0};
    margo_get_output(handle, &dping_out); // XXX error

    if(dping_out.ret == FLOCK_SUCCESS)
        fprintf(stderr, "[%.4lf] %s:%u dping  ACK from %s:%u\n", ABT_get_wtime(), ctx->self_addr_string, ctx->self_provider_id, target_address, target_provider_id);
    margo_free_output(handle, &dping_out);
    margo_destroy(handle);
    margo_addr_free(ctx->mid, addr);
    return dping_out.ret;
}

static DEFINE_MARGO_RPC_HANDLER(dping_rpc_ult)
static void dping_rpc_ult(hg_handle_t h)
{
    dping_in_t in = {0};

    /* find the margo instance */
    margo_instance_id mid = margo_hg_handle_get_instance(h);

    /* find the context */
    const struct hg_info* info = margo_get_info(h);
    swim_context* ctx = margo_registered_data(mid, info->id);

    hg_return_t hret = margo_get_input(h, &in);
    // XXX if above succeeds, parse any membership updates in message

    dping_out_t dping_out = {
        .ret = FLOCK_SUCCESS
    };
    int rand_pct = rand() % 100;
    if(rand_pct <= PACKET_LOSS) {
        fprintf(stderr, "[%.4lf] %s:%u dping DROP from %s:%u\n", ABT_get_wtime(), ctx->self_addr_string, ctx->self_provider_id, in.source_address, in.source_provider_id);
        dping_out.ret = FLOCK_ERR_OTHER;
    }
    else {
        fprintf(stderr, "[%.4lf] %s:%u dping RECV from %s:%u\n", ABT_get_wtime(), ctx->self_addr_string, ctx->self_provider_id, in.source_address, in.source_provider_id);
    }
    margo_respond(h, &dping_out);
    margo_free_input(h, &in);
    margo_destroy(h);
    return;
}

//
// iping RPC code
//

struct iping_send_ult_args
{
    swim_context *ctx;
    char *iping_target_address;
    uint16_t iping_target_provider_id;
    char *dping_target_address;
    uint16_t dping_target_provider_id;
    int ret;
};

static void iping_send_ult(void *t_arg);

// XXX FLOCK_SUCCESS or other error codes
static flock_return_t iping_send(swim_context *ctx,
    char *dping_target_address, uint16_t dping_target_provider_id)
{
    ABT_pool pool = ABT_POOL_NULL;
    ABT_thread *iping_threads = NULL;
    struct iping_send_ult_args *iping_args = NULL;
    int iping_target_count = 0;
    int i;
    int ret;
    int iping_success = 0;
    flock_return_t iping_err = FLOCK_ERR_OTHER;

    /* use handler pool for dispatching iping threads */
    margo_get_handler_pool(ctx->mid, &pool);
    if(pool == ABT_POOL_NULL)
        return FLOCK_ERR_FROM_ARGOBOTS;

    iping_threads = malloc(SUBGROUP_SIZE * sizeof(*iping_threads));
    if(!iping_threads)
        return FLOCK_ERR_ALLOCATION;
    iping_args = malloc(SUBGROUP_SIZE * sizeof(*iping_args));
    if(!iping_args)
    {
        free(iping_threads);
        return FLOCK_ERR_ALLOCATION;
    }

    /* find random set of iping targets */
    FLOCK_GROUP_VIEW_LOCK(&ctx->view);
    int target_list_size = ctx->view.members.size-1;
    int r_ndx, r_start = rand() % target_list_size;
    i = 0;
    while(iping_target_count < SUBGROUP_SIZE) {
        r_ndx = (r_start + i) % target_list_size;
        if ((i > 0 ) && (r_ndx == r_start)) break;
        flock_member_t* iping_target = &ctx->view.members.data[ctx->target_list[r_ndx]];

        /* skip the dping target */
        if((strcmp(iping_target->address, dping_target_address) == 0) &&
            (iping_target->provider_id == dping_target_provider_id))  {
            i++;
            continue;
        }
        iping_args[iping_target_count].ctx = ctx;
        iping_args[iping_target_count].dping_target_address = dping_target_address;
        iping_args[iping_target_count].dping_target_provider_id = dping_target_provider_id;
        iping_args[iping_target_count].iping_target_address = strdup(iping_target->address);
        if(!iping_args[iping_target_count].iping_target_address) {
            continue; // XXX CLEANUP
        }
        iping_args[iping_target_count].iping_target_provider_id = iping_target->provider_id;
        iping_target_count++;
        i++;
    }
    FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);

    /* dispatch individual threads for handling each iping request */
    for(i = 0; i < iping_target_count; i++) {
        iping_threads[i] = ABT_THREAD_NULL;
        ret = ABT_thread_create(pool, iping_send_ult, &iping_args[i],
            ABT_THREAD_ATTR_NULL, &iping_threads[i]);
        if(ret != ABT_SUCCESS) {
            margo_warning(ctx->mid, "[flock] Failed to create SWIM iping RPC thread");
            free(iping_args[i].iping_target_address);
        }
    }

    /* XXX join/cleanup iping threads dispatched above */
    for(i = 0; i < iping_target_count; i++) {
        if(iping_threads[i] != ABT_THREAD_NULL)
        {
            ABT_thread_join(iping_threads[i]);
            ABT_thread_free(&iping_threads[i]);
            /* check return code for iping success */
            if(iping_args[i].ret == FLOCK_SUCCESS)
                iping_success++;
            else
                iping_err = iping_args[i].ret;
            free(iping_args[i].iping_target_address); // XXX
        }
    }
    free(iping_args);
    free(iping_threads);

    if(iping_success == 0)
        return iping_err;
    return FLOCK_SUCCESS;
}

// XXX thread for performing a single iping RPC request
static void iping_send_ult(void *t_arg)
{
    struct iping_send_ult_args *iping_args = (struct iping_send_ult_args *)t_arg;

    /* lookup mercury addr and create iping handle */
    hg_addr_t addr;
    hg_handle_t handle;
    hg_return_t hret = margo_addr_lookup(iping_args->ctx->mid,
        iping_args->iping_target_address, &addr);
    if(hret != HG_SUCCESS) {
        margo_warning(iping_args->ctx->mid, "[flock] Failed to lookup SWIM iping target addr");
        iping_args->ret = FLOCK_ERR_FROM_MERCURY;
        return;
    }
    hret = margo_create(
            iping_args->ctx->mid,
            addr,
            iping_args->ctx->iping_rpc_id,
            &handle);
    if(hret != HG_SUCCESS) {
        margo_warning(iping_args->ctx->mid, "[flock] Failed to create SWIM iping RPC handle");
        margo_addr_free(iping_args->ctx->mid, addr);
        iping_args->ret = FLOCK_ERR_FROM_MERCURY;
        return;
    }

    /* send iping request to target */
    fprintf(stderr, "[%.4lf] %s:%u iping SEND   to %s:%u (target=%s:%u)\n", ABT_get_wtime(), iping_args->ctx->self_addr_string, iping_args->ctx->self_provider_id, iping_args->iping_target_address, iping_args->iping_target_provider_id, iping_args->dping_target_address, iping_args->dping_target_provider_id);
    iping_in_t iping_in = {
        .source_address     = iping_args->ctx->self_addr_string,
        .source_provider_id = iping_args->ctx->self_provider_id,
        .target_address     = iping_args->dping_target_address,
        .target_provider_id = iping_args->dping_target_provider_id,
    };
    hret = margo_provider_forward_timed(iping_args->iping_target_provider_id,
        handle, &iping_in, DPING_TIMEOUT); // XXX TIMEOUT?
    if(hret != HG_SUCCESS) {
        margo_warning(iping_args->ctx->mid, "[flock] Failed to forward SWIM iping RPC");
        margo_destroy(handle);
        margo_addr_free(iping_args->ctx->mid, addr);
        iping_args->ret = FLOCK_ERR_FROM_MERCURY;
        return;
    }

    iping_out_t iping_out = {0};
    margo_get_output(handle, &iping_out); // XXX error

    fprintf(stderr, "[%.4lf] %s:%u iping  ACK from %s:%u (target=%s:%u) [%d]\n", ABT_get_wtime(), iping_args->ctx->self_addr_string, iping_args->ctx->self_provider_id, iping_args->iping_target_address, iping_args->iping_target_provider_id, iping_args->dping_target_address, iping_args->dping_target_provider_id, iping_out.ret);
    iping_args->ret = iping_out.ret;
    margo_free_output(handle, &iping_out);
    margo_destroy(handle);
    margo_addr_free(iping_args->ctx->mid, addr);
    return;
}

static DEFINE_MARGO_RPC_HANDLER(iping_rpc_ult)
static void iping_rpc_ult(hg_handle_t h)
{
    iping_in_t in = {0};
    iping_out_t out = {0};

    /* find the margo instance */
    margo_instance_id mid = margo_hg_handle_get_instance(h);

    /* find the context */
    const struct hg_info* info = margo_get_info(h);
    swim_context* ctx = margo_registered_data(mid, info->id);

    hg_return_t hret = margo_get_input(h, &in);
    if(hret != HG_SUCCESS) {
        out.ret = FLOCK_ERR_FROM_MERCURY;
        goto finish;
    }

    int rand_pct = rand() % 100;
    if(rand_pct <= PACKET_LOSS) {
        fprintf(stderr, "[%.4lf] %s:%u iping DROP from %s:%u (target=%s:%u)\n", ABT_get_wtime(), ctx->self_addr_string, ctx->self_provider_id, in.source_address, in.source_provider_id, in.target_address, in.target_provider_id);
        out.ret = FLOCK_ERR_OTHER;
    }
    else {
        fprintf(stderr, "[%.4lf] %s:%u iping RECV from %s:%u (target=%s:%u)\n", ABT_get_wtime(), ctx->self_addr_string, ctx->self_provider_id, in.source_address, in.source_provider_id, in.target_address, in.target_provider_id);

        /* send the dping and return the error code */
        out.ret = dping_send(ctx, in.target_address, in.target_provider_id);
    }

finish:
    margo_respond(h, &out);
    margo_free_input(h, &in);
    margo_destroy(h);
    return;
}

//
// SWIM membership management
//

static void dead_cb_callback(void* args);

static void mark_suspect(swim_context* ctx, char* suspect_addr,
    uint16_t suspect_provider_id, uint16_t suspect_inc_nr)
{
    suspect_member_link* suspect_member;

    if(SUSP_TIMEOUT == 0) {
        mark_dead(ctx);
        return;
    }

    /* pre-allocate a suspect member link now, to avoid needing to do this
     * while holding a lock later on
     */
    suspect_member = malloc(sizeof(*suspect_member));
    if(!suspect_member) {
        margo_warning(ctx->mid, "[flock] Failed to allocate SWIM suspect member link");
        return;
    }
    suspect_member->ctx = ctx;
    suspect_member->addr = strdup(suspect_addr);
    if(!suspect_member->addr) {
        margo_warning(ctx->mid, "[flock] Failed to allocate SWIM suspect member address");
        free(suspect_member);
        return;
    }
    suspect_member->provider_id = suspect_provider_id;
    margo_timer_create(ctx->mid, dead_cb_callback, suspect_member, &suspect_member->dead_timer);
    margo_timer_start(suspect_member->dead_timer, SUSP_TIMEOUT * PROT_PERIOD);

    FLOCK_GROUP_VIEW_LOCK(&ctx->view);

    /* proceed with the suspicion if:
     *   - the member is still in the group view
     *   - the member is not SUSPECT in a gte incarnation number
     *   - the member is not ALIVE in a gt incarnation number
     */
    flock_member_t *member = flock_group_view_find_member(&ctx->view,
        suspect_addr, suspect_provider_id);
    if (!member) {
        margo_warning(ctx->mid, "[flock] Ignoring suspicion of unknown member %s:%u",
            suspect_addr, suspect_provider_id);
        FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);
        goto suspect_cleanup;
    }
    swim_member_state* state = (swim_member_state*)(member->extra.data);
    if(((state->status == SWIM_MEMBER_SUSPECT) && (suspect_inc_nr <= state->inc_nr)) ||
        ((state->status == SWIM_MEMBER_ALIVE) && (suspect_inc_nr < state->inc_nr))) {
        margo_warning(ctx->mid, "[flock] Ignoring stale suspicion of member %s:%u (inc_nr=%u)",
            suspect_addr, suspect_provider_id, suspect_inc_nr);
        FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);
        goto suspect_cleanup;
    }

    /* remove any previous suspicion for this member */
    if(state->status == SWIM_MEMBER_SUSPECT) {
        suspect_member_link *old_suspect_member, *old_suspect_tmp;
        LL_FOREACH_SAFE(ctx->suspect_list, old_suspect_member, old_suspect_tmp) {
            if((strcmp(suspect_addr, old_suspect_member->addr) == 0) &&
                (suspect_provider_id == old_suspect_member->provider_id)) {
                LL_DELETE(ctx->suspect_list, old_suspect_member);
                break;
            }
        }
    }
    /* add new member suspicion to suspect list */
    LL_APPEND(ctx->suspect_list, suspect_member);

    // XXX add to piggyback buffer update list

    /* finally, mark member as SUSPECT */
    state->status = SWIM_MEMBER_SUSPECT;

    FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);

    fprintf(stderr, "[%.4lf] %s:%u SUSPECT %s:%u (inc_nr = %u)\n", ABT_get_wtime(), ctx->self_addr_string, ctx->self_provider_id, suspect_addr, suspect_provider_id, suspect_inc_nr);
    return;

suspect_cleanup:
    margo_timer_cancel(suspect_member->dead_timer);
    margo_timer_destroy(suspect_member->dead_timer);
    free(suspect_member->addr);
    free(suspect_member);
    return;
}

static void dead_cb_callback(void* args)
{
    suspect_member_link* member = (suspect_member_link*)args;
    fprintf(stderr, "[%.4lf] %s:%u DEAD %s:%u\n", ABT_get_wtime(), member->ctx->self_addr_string, member->ctx->self_provider_id, member->addr, member->provider_id);
    return;
}

static void mark_dead(swim_context* ctx)
{
    fprintf(stderr, "DEAD\n");
    return;
}

static void shuffle_ping_targets(swim_context* ctx)
{
    int target_list_size = ctx->view.members.size-1;

    ctx->target_list_ndx = 0;

    if(target_list_size <= 1)
        return; /* no need to shuffle if only one other group member */

    int r_list_ndx, tmp_val;
    /* run fisher-yates shuffle over list of target members */
    for(int i = target_list_size-1; i > 0; i--) {
        r_list_ndx = rand() % (i+1);
        tmp_val = ctx->target_list[r_list_ndx];
        ctx->target_list[r_list_ndx] = ctx->target_list[i];
        ctx->target_list[i] = tmp_val;
    }

    return;
}

/* manual serialization/deserialization routine for swim updates */
static hg_return_t hg_proc_swim_update_t(hg_proc_t proc, void *data)
{
}

static flock_return_t swim_destroy_group(void* ctx)
{
    if(!ctx) return FLOCK_SUCCESS;
    swim_context* context = (swim_context*)ctx;
    fprintf(stderr, "[%.4lf] %s:%u SHUTDOWN\n", ABT_get_wtime(), context->self_addr_string, context->self_provider_id);
    flock_group_view_clear(&context->view);
    /* cleanup any pending timers to run the SWIM protocol */
    if(context->prot_timer != MARGO_TIMER_NULL)
        margo_timer_cancel(context->prot_timer);
    margo_timer_destroy(context->prot_timer);
    /* cleanup the list of suspect members */
    suspect_member_link *suspect_member, *suspect_tmp;
    LL_FOREACH_SAFE(context->suspect_list, suspect_member, suspect_tmp) {
        LL_DELETE(context->suspect_list, suspect_member);
        margo_timer_cancel(suspect_member->dead_timer);
        margo_timer_destroy(suspect_member->dead_timer);
        free(suspect_member->addr);
        free(suspect_member);
    }
    if(context->dping_rpc_id) margo_deregister(context->mid, context->dping_rpc_id);
    if(context->iping_rpc_id) margo_deregister(context->mid, context->iping_rpc_id);
    free(context->target_list);
    free(context->self_addr_string);
    free(context);
    return FLOCK_SUCCESS;
}

static flock_return_t swim_get_config(
    void* ctx, void (*fn)(void*, const struct json_object*), void* uargs)
{
    // LCOV_EXCL_START
    (void)ctx;
    (void)fn;
    (void)uargs;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_STOP
}

static flock_return_t swim_get_view(
    void* ctx, void (*fn)(void*, const flock_group_view_t* view), void* uargs)
{
    // LCOV_EXCL_START
    (void)ctx;
    (void)fn;
    (void)uargs;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_STOP
}

static flock_return_t swim_add_metadata(
        void* ctx, const char* key, const char* value)
{
    // LCOV_EXCL_START
    (void)ctx;
    (void)key;
    (void)value;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_STOP
}

static flock_return_t swim_remove_metadata(
        void* ctx, const char* key)
{
    // LCOV_EXCL_START
    (void)ctx;
    (void)key;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_STOP
}

static flock_backend_impl swim_backend = {
    .name            = "swim",
    .init_group      = swim_create_group,
    .destroy_group   = swim_destroy_group,
    .get_config      = swim_get_config,
    .get_view        = swim_get_view,
    .add_metadata    = swim_add_metadata,
    .remove_metadata = swim_remove_metadata
};

flock_return_t flock_register_swim_backend(void)
{
    return flock_register_backend(&swim_backend);
}
