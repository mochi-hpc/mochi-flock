/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_CLIENT_HPP
#define __FLOCK_CLIENT_HPP

#include <thallium.hpp>
#include <flock/flock-server.h>
#include <flock/cxx/exception.hpp>
#include <flock/cxx/group-view.hpp>
#include <string>

namespace flock {

/**
 * @brief Abstract interface for observing group membership and metadata changes.
 *
 * Subclass this and override onMembershipUpdate() and onMetadataUpdate() to
 * receive notifications when the group's state changes. Register an observer
 * with Provider::addObserver() and unregister it with Provider::removeObserver().
 *
 * Observers are non-copyable and non-movable.
 *
 * @see Provider::addObserver, Provider::removeObserver
 * @see flock_membership_update_fn, flock_metadata_update_fn
 */
class Observer {

    public:

    /** @brief Copy constructor (deleted). Observers are non-copyable. */
    Observer(const Observer&) = delete;
    /** @brief Move constructor (deleted). Observers are non-movable. */
    Observer(Observer&&) = delete;

    /** @brief Virtual destructor. */
    virtual ~Observer() = default;

    /**
     * @brief Called when a group membership change occurs.
     *
     * @param update The type of update (FLOCK_MEMBER_JOINED, FLOCK_MEMBER_LEFT,
     *               FLOCK_MEMBER_DIED, or FLOCK_MEMBER_MOVED).
     * @param address Address of the member affected by the update.
     * @param provider_id Provider ID of the member affected by the update.
     */
    virtual void onMembershipUpdate(
        flock_update_t update,
        const char* address,
        uint16_t provider_id) = 0;

    /**
     * @brief Called when a group metadata key/value pair is updated.
     *
     * @param key Metadata key that was updated.
     * @param value New value for the metadata key.
     */
    virtual void onMetadataUpdate(
        const char* key,
        const char* value) = 0;

};

/**
 * @brief RAII wrapper around a flock_provider_t handle.
 *
 * A Provider registers a Flock group management service with a Margo instance,
 * managing group state via a pluggable backend. It is move-only (non-copyable).
 *
 * The Provider also registers a Margo finalize callback so that if the Margo
 * instance is finalized before the Provider is destroyed, the internal handle
 * is safely invalidated.
 *
 * @see flock_provider_register, flock_provider_destroy
 */
class Provider {

    public:

    /**
     * @brief Default constructor. Creates an invalid (null) provider.
     */
    Provider() = default;

    /**
     * @brief Construct a Provider from a Margo instance.
     *
     * The config string must be a JSON document specifying the bootstrap
     * method, backend type, and optional backend configuration. See
     * flock_provider_register() for the full configuration format.
     *
     * @param mid Margo instance.
     * @param provider_id Unique provider ID for this provider within the Margo instance.
     * @param config JSON configuration string.
     * @param initial_view Initial group view. The provider takes ownership of the
     *                     view's content and resets it to empty for the caller.
     * @param pool Argobots pool in which to run the provider's RPCs.
     *             Defaults to ABT_POOL_NULL (uses the Margo instance's default pool).
     *
     * @throws flock::Exception on failure.
     *
     * @see flock_provider_register
     */
    Provider(margo_instance_id mid,
             uint16_t provider_id,
             const char* config,
             GroupView& initial_view,
             ABT_pool pool = ABT_POOL_NULL) {
        m_mid = mid;
        flock_provider_args args;
        args.backend      = nullptr;
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

    /**
     * @brief Construct a Provider from a Thallium engine.
     *
     * Delegates to the Margo-based constructor.
     *
     * @param engine Thallium engine.
     * @param provider_id Unique provider ID for this provider within the engine.
     * @param config JSON configuration string.
     * @param initial_view Initial group view. The provider takes ownership of the
     *                     view's content and resets it to empty for the caller.
     * @param pool Thallium pool in which to run the provider's RPCs.
     *             Defaults to an invalid pool (uses the engine's default pool).
     *
     * @throws flock::Exception on failure.
     *
     * @see flock_provider_register
     */
    Provider(const thallium::engine& engine,
             uint16_t provider_id,
             const char* config,
             GroupView& initial_view,
             const thallium::pool& pool = thallium::pool())
    : Provider(engine.get_margo_instance(),
               provider_id,
               config,
               initial_view,
               pool.native_handle()) {}

    /**
     * @brief Destructor. Destroys the underlying provider and deregisters its RPCs.
     *
     * @see flock_provider_destroy
     */
    ~Provider() {
        if(m_provider != FLOCK_PROVIDER_NULL) {
            margo_provider_pop_finalize_callback(m_mid, this);
            flock_provider_destroy(m_provider);
        }
    }

    /**
     * @brief Move constructor. Transfers ownership from another Provider.
     *
     * @param other Provider to move from. Left in a null state after the move.
     */
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

    /** @brief Copy constructor (deleted). Providers are move-only. */
    Provider(const Provider&) = delete;

    /** @brief Copy assignment operator (deleted). Providers are move-only. */
    Provider& operator=(const Provider&) = delete;

    /**
     * @brief Move assignment operator. Transfers ownership from another Provider.
     *
     * If this Provider already holds a valid handle, it is destroyed first.
     *
     * @param other Provider to move from. Left in a null state after the move.
     * @return Reference to this Provider.
     */
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

    /**
     * @brief Register an Observer to be notified of membership and metadata changes.
     *
     * The observer pointer is used as the unique context for the callback
     * registration and can be passed to removeObserver() to unregister.
     * Multiple observers may be registered simultaneously with distinct pointers.
     *
     * @param observer Pointer to an Observer instance. Must remain valid until
     *                 removed or the provider is destroyed.
     *
     * @throws flock::Exception on failure.
     *
     * @see flock_provider_add_update_callbacks
     */
    void addObserver(Observer* observer) {
        auto membership_update_fn =
            [](void* ctx, flock_update_t update, const char* address, uint16_t provider_id) {
                static_cast<Observer*>(ctx)->onMembershipUpdate(update, address, provider_id);
            };
        auto metadata_update_fn =
            [](void* ctx, const char* key, const char* value) {
                static_cast<Observer*>(ctx)->onMetadataUpdate(key, value);
            };
        auto err = flock_provider_add_update_callbacks(
            m_provider, membership_update_fn, metadata_update_fn, observer);
        FLOCK_CONVERT_AND_THROW(err);
    }

    /**
     * @brief Unregister a previously registered Observer.
     *
     * @param observer Pointer to the Observer to remove. Must match a pointer
     *                 previously passed to addObserver().
     *
     * @throws flock::Exception on failure.
     *
     * @see flock_provider_remove_update_callbacks
     */
    void removeObserver(Observer* observer) {
        auto err = flock_provider_remove_update_callbacks(m_provider, observer);
        FLOCK_CONVERT_AND_THROW(err);
    }

    /**
     * @brief Return the JSON-formatted configuration of this provider.
     *
     * @return A string containing the provider's JSON configuration.
     *
     * @throws flock::Exception on failure.
     *
     * @see flock_provider_get_config
     */
    std::string config() const {
        auto cfg = flock_provider_get_config(m_provider);
        if(!cfg) throw Exception{FLOCK_ERR_OTHER};
        auto result = std::string{cfg};
        free(cfg);
        return result;
    }

    /**
     * @brief Return the underlying C flock_provider_t handle.
     *
     * @return The underlying flock_provider_t handle.
     */
    auto handle() const {
        return m_provider;
    }

    /**
     * @brief Implicit conversion to flock_provider_t.
     *
     * @return The underlying flock_provider_t handle.
     */
    operator flock_provider_t() const {
        return handle();
    }

    private:

    margo_instance_id m_mid      = MARGO_INSTANCE_NULL;
    flock_provider_t  m_provider = FLOCK_PROVIDER_NULL;
};

}

#endif
