/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_CLIENT_HPP
#define __FLOCK_CLIENT_HPP

#include <thallium.hpp>
#include <flock/flock-client.h>
#include <flock/flock-group.h>
#include <flock/cxx/exception.hpp>
#include <flock/cxx/group.hpp>

namespace flock {

class Client {

    public:

    Client() = default;

    Client(margo_instance_id mid, ABT_pool pool = ABT_POOL_NULL)
    : m_engine{mid} {
        auto err = flock_client_init(mid, pool, &m_client);
        FLOCK_CONVERT_AND_THROW(err);
    }

    Client(const thallium::engine& engine, thallium::pool pool = thallium::pool())
    : m_engine{engine} {
        auto err = flock_client_init(engine.get_margo_instance(), pool.native_handle(), &m_client);
        FLOCK_CONVERT_AND_THROW(err);
    }

    ~Client() {
        if(m_client != FLOCK_CLIENT_NULL) {
            flock_client_finalize(m_client);
        }
    }

    Client(Client&& other)
    : m_client(other.m_client) {
        other.m_client = FLOCK_CLIENT_NULL;
    }

    Client(const Client&) = delete;

    Client& operator=(const Client&) = delete;

    Client& operator=(Client&& other) {
        if(this == &other) return *this;
        if(m_client != FLOCK_CLIENT_NULL) {
            flock_client_finalize(m_client);
        }
        m_client = other.m_client;
        other.m_client = FLOCK_CLIENT_NULL;
        return *this;
    }

    GroupHandle makeGroupHandle(
        hg_addr_t addr,
        uint16_t provider_id,
        uint32_t mode = 0) const {
        flock_group_handle_t gh;
        auto err = flock_group_handle_create(
            m_client, addr, provider_id, mode, &gh);
        FLOCK_CONVERT_AND_THROW(err);
        return GroupHandle(gh, false);
    }

    auto handle() const {
        return m_client;
    }

    operator flock_client_t() const {
        return m_client;
    }

    auto engine() const {
        return m_engine;
    }

    private:

    thallium::engine m_engine;
    flock_client_t m_client = FLOCK_CLIENT_NULL;
};

}

#endif
