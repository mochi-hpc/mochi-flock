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

/**
 * @brief RAII wrapper around a flock_client_t handle.
 *
 * A Client is used to create GroupHandle objects for communicating
 * with Flock providers. It is move-only (non-copyable).
 *
 * @see flock_client_init, flock_client_finalize
 */
class Client {

    public:

    /**
     * @brief Default constructor. Creates an invalid (null) client.
     */
    Client() = default;

    /**
     * @brief Construct a Client from a Margo instance.
     *
     * @param mid Margo instance.
     * @param pool Argobots pool in which to run operations such as updates.
     *             Defaults to ABT_POOL_NULL (uses the Margo instance's default pool).
     *
     * @throws flock::Exception on failure.
     *
     * @see flock_client_init
     */
    Client(margo_instance_id mid, ABT_pool pool = ABT_POOL_NULL)
    : m_engine{mid} {
        auto err = flock_client_init(mid, pool, &m_client);
        FLOCK_CONVERT_AND_THROW(err);
    }

    /**
     * @brief Construct a Client from a Thallium engine.
     *
     * @param engine Thallium engine.
     * @param pool Thallium pool in which to run operations such as updates.
     *             Defaults to an invalid pool (uses the engine's default pool).
     *
     * @throws flock::Exception on failure.
     *
     * @see flock_client_init
     */
    Client(const thallium::engine& engine, thallium::pool pool = thallium::pool())
    : m_engine{engine} {
        auto err = flock_client_init(engine.get_margo_instance(), pool.native_handle(), &m_client);
        FLOCK_CONVERT_AND_THROW(err);
    }

    /**
     * @brief Destructor. Finalizes the underlying flock_client_t if valid.
     *
     * @see flock_client_finalize
     */
    ~Client() {
        if(m_client != FLOCK_CLIENT_NULL) {
            flock_client_finalize(m_client);
        }
    }

    /**
     * @brief Move constructor. Transfers ownership from another Client.
     *
     * @param other Client to move from. Left in a null state after the move.
     */
    Client(Client&& other)
    : m_client(other.m_client) {
        other.m_client = FLOCK_CLIENT_NULL;
    }

    /** @brief Copy constructor (deleted). Clients are move-only. */
    Client(const Client&) = delete;

    /** @brief Copy assignment operator (deleted). Clients are move-only. */
    Client& operator=(const Client&) = delete;

    /**
     * @brief Move assignment operator. Transfers ownership from another Client.
     *
     * If this Client already holds a valid handle, it is finalized first.
     *
     * @param other Client to move from. Left in a null state after the move.
     * @return Reference to this Client.
     */
    Client& operator=(Client&& other) {
        if(this == &other) return *this;
        if(m_client != FLOCK_CLIENT_NULL) {
            flock_client_finalize(m_client);
        }
        m_client = other.m_client;
        other.m_client = FLOCK_CLIENT_NULL;
        return *this;
    }

    /**
     * @brief Create a GroupHandle by contacting the group member at the
     * given Mercury address and provider ID.
     *
     * @param addr Mercury address of one of the group members.
     * @param provider_id Provider ID of the member.
     * @param mode Optional mode flags (bitwise OR of FLOCK_MODE_INIT_UPDATE
     *             and/or FLOCK_MODE_SUBSCRIBE). Defaults to 0.
     *
     * @return A GroupHandle for the specified group member.
     *
     * @throws flock::Exception on failure.
     *
     * @see flock_group_handle_create
     */
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

    /**
     * @brief Create a GroupHandle by looking up the given address string
     * and contacting the group member at that address and provider ID.
     *
     * @param addr_str Address string of one of the group members.
     * @param provider_id Provider ID of the member.
     * @param mode Optional mode flags (bitwise OR of FLOCK_MODE_INIT_UPDATE
     *             and/or FLOCK_MODE_SUBSCRIBE). Defaults to 0.
     *
     * @return A GroupHandle for the specified group member.
     *
     * @throws flock::Exception on failure.
     *
     * @see flock_group_handle_create
     */
    GroupHandle makeGroupHandle(
        const std::string& addr_str,
        uint16_t provider_id,
        uint32_t mode = 0) const {
        flock_group_handle_t gh;
        auto endpoint = m_engine.lookup(addr_str);
        auto err = flock_group_handle_create(
            m_client, endpoint.get_addr(), provider_id, mode, &gh);
        FLOCK_CONVERT_AND_THROW(err);
        return GroupHandle(gh, false);
    }

    /**
     * @brief Return the underlying C flock_client_t handle.
     *
     * @return The underlying flock_client_t handle.
     */
    auto handle() const {
        return m_client;
    }

    /**
     * @brief Implicit conversion to flock_client_t.
     *
     * @return The underlying flock_client_t handle.
     */
    operator flock_client_t() const {
        return m_client;
    }

    /**
     * @brief Return the Thallium engine associated with this Client.
     *
     * @return The Thallium engine.
     */
    auto engine() const {
        return m_engine;
    }

    private:

    thallium::engine m_engine;
    flock_client_t m_client = FLOCK_CLIENT_NULL;
};

}

#endif
