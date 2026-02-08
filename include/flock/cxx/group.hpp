/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_GROUP_HPP
#define __FLOCK_GROUP_HPP

#include <flock/flock-group.h>
#include <flock/cxx/exception.hpp>
#include <flock/cxx/group-view.hpp>
#include <string_view>
#include <string>
#include <vector>
#include <functional>

namespace flock {

class Client;

/**
 * @brief RAII wrapper around a flock_group_handle_t, providing access
 * to a remote Flock group.
 *
 * A GroupHandle maintains a reference-counted handle to a remote group.
 * It can be created from a Client (via Client::makeGroupHandle()), from
 * a group file (via GroupHandle::FromFile()), or from a serialized view
 * (via GroupHandle::FromSerialized()).
 *
 * GroupHandle is copyable (reference-counted) and movable.
 *
 * @see flock_group_handle_create, flock_group_handle_release
 */
class GroupHandle {

    friend class Client;

    public:

    /**
     * @brief Default constructor. Creates an invalid (null) group handle.
     */
    GroupHandle() = default;

    /**
     * @brief Construct a GroupHandle from an existing C group handle.
     *
     * @param gh The C flock_group_handle_t to wrap.
     * @param copy If true (default), the reference count is incremented.
     *             If false, this object takes ownership without incrementing.
     *
     * @throws flock::Exception if incrementing the reference count fails.
     *
     * @see flock_group_handle_ref_incr
     */
    GroupHandle(flock_group_handle_t gh, bool copy=true)
    : m_gh(gh) {
        if(copy && (m_gh != FLOCK_GROUP_HANDLE_NULL)) {
            auto err = flock_group_handle_ref_incr(m_gh);
            FLOCK_CONVERT_AND_THROW(err);
        }
    }

    /**
     * @brief Construct a GroupHandle by contacting the group member at the
     * given address and provider ID.
     *
     * @param client A valid flock_client_t handle.
     * @param addr Mercury address of one of the group members.
     * @param provider_id Provider ID of the member.
     * @param check If true (default), the group member is contacted to verify
     *              its existence.
     *
     * @throws flock::Exception on failure.
     *
     * @see flock_group_handle_create
     */
    GroupHandle(flock_client_t client,
             hg_addr_t addr,
             uint16_t provider_id,
             bool check = true)
    {
        auto err = flock_group_handle_create(client,
            addr, provider_id, check, &m_gh);
        FLOCK_CONVERT_AND_THROW(err);
    }

    /**
     * @brief Copy constructor. Increments the reference count of the
     * underlying handle.
     *
     * @param other GroupHandle to copy from.
     *
     * @throws flock::Exception if incrementing the reference count fails.
     */
    GroupHandle(const GroupHandle& other)
    : m_gh(other.m_gh) {
        if(m_gh != FLOCK_GROUP_HANDLE_NULL) {
            auto err = flock_group_handle_ref_incr(m_gh);
            FLOCK_CONVERT_AND_THROW(err);
        }
    }

    /**
     * @brief Move constructor. Transfers ownership without changing the
     * reference count.
     *
     * @param other GroupHandle to move from. Left in a null state after the move.
     */
    GroupHandle(GroupHandle&& other)
    : m_gh(other.m_gh) {
        other.m_gh = FLOCK_GROUP_HANDLE_NULL;
    }

    /**
     * @brief Copy assignment operator. Releases the current handle (if any)
     * and increments the reference count of the source handle.
     *
     * @param other GroupHandle to copy from.
     * @return Reference to this GroupHandle.
     *
     * @throws flock::Exception on failure.
     */
    GroupHandle& operator=(const GroupHandle& other) {
        if(m_gh == other.m_gh || &other == this)
            return *this;
        if(m_gh != FLOCK_GROUP_HANDLE_NULL) {
            auto err = flock_group_handle_release(m_gh);
            FLOCK_CONVERT_AND_THROW(err);
        }
        m_gh = other.m_gh;
        if(m_gh != FLOCK_GROUP_HANDLE_NULL) {
            auto err = flock_group_handle_ref_incr(m_gh);
            FLOCK_CONVERT_AND_THROW(err);
        }
        return *this;
    }

    /**
     * @brief Move assignment operator. Releases the current handle (if any)
     * and transfers ownership from the source.
     *
     * @param other GroupHandle to move from. Left in a null state after the move.
     * @return Reference to this GroupHandle.
     *
     * @throws flock::Exception on failure.
     */
    GroupHandle& operator=(GroupHandle&& other) {
        if(m_gh == other.m_gh || &other == this)
            return *this;
        if(m_gh != FLOCK_GROUP_HANDLE_NULL) {
            auto err = flock_group_handle_release(m_gh);
            FLOCK_CONVERT_AND_THROW(err);
        }
        m_gh = other.m_gh;
        other.m_gh = FLOCK_GROUP_HANDLE_NULL;
        return *this;
    }

    /**
     * @brief Destructor. Releases the underlying group handle, decrementing
     * its reference count.
     *
     * @see flock_group_handle_release
     */
    ~GroupHandle() {
        if(m_gh != FLOCK_GROUP_HANDLE_NULL) {
            flock_group_handle_release(m_gh);
        }
    }

    /**
     * @brief Create a GroupHandle from a group file.
     *
     * The file must be a JSON document describing the group members
     * and metadata. See flock_group_handle_create_from_file() for
     * the expected format.
     *
     * @param client A valid flock_client_t handle.
     * @param filename Path to the group file.
     * @param mode Optional mode flags (bitwise OR of FLOCK_MODE_INIT_UPDATE
     *             and/or FLOCK_MODE_SUBSCRIBE). Defaults to 0.
     *
     * @return A GroupHandle for the group described in the file.
     *
     * @throws flock::Exception on failure.
     *
     * @see flock_group_handle_create_from_file
     */
    static GroupHandle FromFile(
            flock_client_t client,
            const char* filename,
            uint32_t mode = 0) {
        flock_group_handle_t gh;
        auto err = flock_group_handle_create_from_file(client, filename, mode, &gh);
        FLOCK_CONVERT_AND_THROW(err);
        return GroupHandle{gh};
    }

    /**
     * @brief Create a GroupHandle from a serialized group view.
     *
     * The serialized data must have been produced by serializing a GroupView
     * (e.g. via its std::string conversion operator) or by
     * flock_group_view_serialize().
     *
     * @param client A valid flock_client_t handle.
     * @param serialized_view A string_view containing the serialized group view data.
     * @param mode Optional mode flags (bitwise OR of FLOCK_MODE_INIT_UPDATE
     *             and/or FLOCK_MODE_SUBSCRIBE). Defaults to 0.
     *
     * @return A GroupHandle for the deserialized group.
     *
     * @throws flock::Exception on failure.
     *
     * @see flock_group_handle_create_from_serialized
     */
    static GroupHandle FromSerialized(
            flock_client_t client,
            std::string_view serialized_view,
            uint32_t mode = 0) {
        flock_group_handle_t gh;
        auto err = flock_group_handle_create_from_serialized(
            client, serialized_view.data(), serialized_view.size(), mode, &gh);
        FLOCK_CONVERT_AND_THROW(err);
        return GroupHandle{gh};
    }

    /**
     * @brief Return a copy of the group's current view.
     *
     * @return A GroupView containing a snapshot of the group's membership
     *         and metadata.
     *
     * @throws flock::Exception on failure.
     *
     * @see flock_group_get_view
     */
    GroupView view() const {
        GroupView v;
        auto err = flock_group_get_view(m_gh, &v.m_view);
        FLOCK_CONVERT_AND_THROW(err);
        return v;
    }

    /**
     * @brief Update the cached internal view of the group by contacting
     * one (or more) of its members. This is a blocking operation.
     *
     * @throws flock::Exception on failure.
     *
     * @see flock_group_update_view
     */
    void update() const {
        auto err = flock_group_update_view(m_gh, NULL);
        FLOCK_CONVERT_AND_THROW(err);
    }

    /**
     * @brief Return the underlying C flock_group_handle_t handle.
     *
     * @return The underlying flock_group_handle_t handle.
     */
    auto handle() const {
        return m_gh;
    }

    /**
     * @brief Implicit conversion to flock_group_handle_t.
     *
     * @return The underlying flock_group_handle_t handle.
     */
    operator flock_group_handle_t() const {
        return m_gh;
    }

    private:

    flock_group_handle_t m_gh = FLOCK_GROUP_HANDLE_NULL;

};

}

#endif
