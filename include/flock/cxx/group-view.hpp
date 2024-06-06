/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_GROUP_VIEW_HPP
#define __FLOCK_GROUP_VIEW_HPP

#include <flock/flock-group.h>
#include <flock/cxx/exception.hpp>
#include <memory>
#include <map>
#include <vector>
#include <string>
#include <functional>

namespace flock {

class GroupHandle;
class Provider;

class GroupView {

    friend class GroupHandle;
    friend class Provider;

    public:

    struct Member {
        uint64_t    rank;
        uint16_t    provider_id;
        std::string address;
    };

    GroupView() = default;

    GroupView(flock_group_view_t view) {
        FLOCK_GROUP_VIEW_MOVE(&view, &m_view);
    }

    ~GroupView() {
        flock_group_view_clear(&m_view);
    }

    GroupView(GroupView&& other) {
        FLOCK_GROUP_VIEW_MOVE(&other.m_view, &m_view);
    }

    GroupView& operator=(GroupView&& other) {
        if(this == &other) return *this;
        flock_group_view_clear(&m_view);
        FLOCK_GROUP_VIEW_MOVE(&other.m_view, &m_view);
        return *this;
    }

    auto digest() const {
        return m_view.digest;
    }

    void lock() {
        FLOCK_GROUP_VIEW_LOCK(&m_view);
    }

    void unlock() {
        FLOCK_GROUP_VIEW_UNLOCK(&m_view);
    }

    void clear() {
        flock_group_view_clear(&m_view);
    }

    void addMember(uint64_t rank, const char* address, uint16_t provider_id) {
        auto member = flock_group_view_add_member(&m_view, rank, provider_id, address);
        if(!member) throw Exception{FLOCK_ERR_RANK_USED};
    }

    void removeMember(uint64_t rank) {
        if(!flock_group_view_remove_member(&m_view, rank))
            throw Exception{FLOCK_ERR_NO_MEMBER};
    }

    Member findMember(uint64_t rank) const {
        auto member = flock_group_view_find_member(&m_view, rank);
        if(!member) throw Exception{FLOCK_ERR_NO_MEMBER};
        return Member{member->rank, member->provider_id, member->address};
    }

    std::vector<Member> members() const {
        std::vector<Member> result;
        result.reserve(m_view.members.size);
        for(size_t i = 0; i < m_view.members.size; ++i) {
            result.push_back(Member{
                m_view.members.data[i].rank,
                m_view.members.data[i].provider_id,
                m_view.members.data[i].address
            });
        }
        return result;
    }

    size_t maxNumMembers() const {
        return m_view.members.data[m_view.members.size-1].rank;
    }

    size_t numLiveMembers() const {
        return m_view.members.size;
    }

    void setMetadata(const char* key, const char* value) {
        flock_group_view_add_metadata(&m_view, key, value);
    }

    void removeMetadata(const char* key) {
        if(!flock_group_view_remove_metadata(&m_view, key))
            throw Exception{FLOCK_ERR_NO_METADATA};
    }

    std::map<std::string, std::string> metadata() const {
        std::map<std::string, std::string> result;
        for(size_t i = 0; i < m_view.metadata.size; ++i) {
            result.insert(
                std::make_pair<const std::string, std::string>(
                    m_view.metadata.data[i].key,
                    m_view.metadata.data[i].value));
        }
        return result;
    }

    std::string toString(margo_instance_id mid, uint64_t credentials = 0) const {
        std::string result;
        flock_return_t ret = flock_group_view_serialize(
            mid, credentials, &m_view,
            [](void* ctx, const char* content, size_t size) {
                auto str = static_cast<decltype(&result)>(ctx);
                str->assign(content, size);
            }, static_cast<void*>(&result));
        if(ret != FLOCK_SUCCESS) throw Exception{ret};
        return result;
    }

    private:

    flock_group_view_t m_view = FLOCK_GROUP_VIEW_INITIALIZER;
};

}

#endif
