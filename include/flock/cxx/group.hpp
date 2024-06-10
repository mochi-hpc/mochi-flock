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

class GroupHandle {

    friend class Client;

    public:

    GroupHandle() = default;

    GroupHandle(flock_group_handle_t gh, bool copy=true)
    : m_gh(gh) {
        if(copy && (m_gh != FLOCK_GROUP_HANDLE_NULL)) {
            auto err = flock_group_handle_ref_incr(m_gh);
            FLOCK_CONVERT_AND_THROW(err);
        }
    }

    GroupHandle(flock_client_t client,
             hg_addr_t addr,
             uint16_t provider_id,
             bool check = true)
    {
        auto err = flock_group_handle_create(client,
            addr, provider_id, check, &m_gh);
        FLOCK_CONVERT_AND_THROW(err);
    }

    GroupHandle(const GroupHandle& other)
    : m_gh(other.m_gh) {
        if(m_gh != FLOCK_GROUP_HANDLE_NULL) {
            auto err = flock_group_handle_ref_incr(m_gh);
            FLOCK_CONVERT_AND_THROW(err);
        }
    }

    GroupHandle(GroupHandle&& other)
    : m_gh(other.m_gh) {
        other.m_gh = FLOCK_GROUP_HANDLE_NULL;
    }

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

    ~GroupHandle() {
        if(m_gh != FLOCK_GROUP_HANDLE_NULL) {
            flock_group_handle_release(m_gh);
        }
    }

    static GroupHandle FromFile(
            flock_client_t client,
            const char* filename,
            uint32_t mode = 0) {
        flock_group_handle_t gh;
        auto err = flock_group_handle_create_from_file(client, filename, mode, &gh);
        FLOCK_CONVERT_AND_THROW(err);
        return GroupHandle{gh};
    }

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

    GroupView view() const {
        GroupView v;
        auto err = flock_group_get_view(m_gh, &v.m_view);
        FLOCK_CONVERT_AND_THROW(err);
        return v;
    }

    void update() const {
        auto err = flock_group_update_view(m_gh, NULL);
        FLOCK_CONVERT_AND_THROW(err);
    }

    auto handle() const {
        return m_gh;
    }

    operator flock_group_handle_t() const {
        return m_gh;
    }

    private:

    flock_group_handle_t m_gh = FLOCK_GROUP_HANDLE_NULL;

};

}

#endif
