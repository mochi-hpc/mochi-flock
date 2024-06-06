/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_CLIENT_HPP
#define __FLOCK_CLIENT_HPP

#include <flock/flock-server.h>
#include <flock/cxx/exception.hpp>
#include <flock/cxx/group-view.hpp>

namespace flock {

class Observer {

    public:

    Observer(const Observer&) = delete;
    Observer(Observer&&) = delete;

    virtual ~Observer() = default;

    virtual void onMembershipUpdate(
        flock_update_t update,
        uint64_t rank,
        const char* address,
        uint16_t provider_id) = 0;

    virtual void onMetadataUpdate(
        const char* key,
        const char* value) = 0;

};

class Provider {

    public:

    Provider() = default;

    Provider(margo_instance_id mid,
             uint16_t provider_id,
             const char* config,
             GroupView& initial_view,
             ABT_pool pool = ABT_POOL_NULL,
             uint64_t credentials = 0) {
        m_mid = mid;
        flock_provider_args args;
        args.backend      = nullptr;
        args.credentials  = credentials;
        args.initial_view = &initial_view.m_view;
        args.pool         = pool;
        auto err = flock_provider_register(mid, provider_id, config, &args, &m_provider);
        FLOCK_CONVERT_AND_THROW(err);
        margo_provider_push_finalize_callback(
            mid, this, [](void* ctx) {
            auto self = static_cast<decltype(this)>(ctx);
            self->m_provider = FLOCK_PROVIDER_NULL;
        }, this);
    }

    ~Provider() {
        if(m_provider != FLOCK_PROVIDER_NULL) {
            margo_provider_pop_finalize_callback(m_mid, this);
            flock_provider_destroy(m_provider);
        }
    }

    Provider(Provider&& other)
    : m_provider(other.m_provider) {
        margo_provider_pop_finalize_callback(other.m_mid, &other);
        other.m_provider = FLOCK_PROVIDER_NULL;
        margo_provider_push_finalize_callback(
            m_mid, this, [](void* ctx) {
            auto self = static_cast<decltype(this)>(ctx);
            self->m_provider = FLOCK_PROVIDER_NULL;
        }, this);
    }

    Provider(const Provider&) = delete;

    Provider& operator=(const Provider&) = delete;

    Provider& operator=(Provider&& other) {
        if(this == &other) return *this;
        this->~Provider();
        m_provider = other.m_provider;
        other.m_provider = FLOCK_PROVIDER_NULL;
        margo_provider_pop_finalize_callback(other.m_mid, &other);
        other.m_provider = FLOCK_PROVIDER_NULL;
        margo_provider_push_finalize_callback(
            m_mid, this, [](void* ctx) {
            auto self = static_cast<decltype(this)>(ctx);
            self->m_provider = FLOCK_PROVIDER_NULL;
        }, this);
        return *this;
    }

    void addObserver(Observer* observer) {
        auto membership_update_fn =
            [](void* ctx, flock_update_t update, size_t rank, const char* address, uint16_t provider_id) {
                static_cast<Observer*>(ctx)->onMembershipUpdate(update, rank, address, provider_id);
            };
        auto metadata_update_fn =
            [](void* ctx, const char* key, const char* value) {
                static_cast<Observer*>(ctx)->onMetadataUpdate(key, value);
            };
        auto err = flock_provider_add_update_callbacks(
            m_provider, membership_update_fn, metadata_update_fn, observer);
        FLOCK_CONVERT_AND_THROW(err);
    }

    void removeObserver(Observer* observer) {
        auto err = flock_provider_remove_update_callbacks(m_provider, observer);
        FLOCK_CONVERT_AND_THROW(err);
    }

    std::string config() const {
        auto cfg = flock_provider_get_config(m_provider);
        if(!cfg) throw Exception{FLOCK_ERR_OTHER};
        auto result = std::string{cfg};
        free(cfg);
        return result;
    }

    auto handle() const {
        return m_provider;
    }

    operator flock_provider_t() const {
        return handle();
    }

    private:

    margo_instance_id m_mid      = MARGO_INSTANCE_NULL;
    flock_provider_t  m_provider = FLOCK_PROVIDER_NULL;
};

}

#endif
