/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_GROUP_VIEW_HPP
#define __FLOCK_GROUP_VIEW_HPP

#include <flock/flock-group.h>
#include <flock/cxx/exception.hpp>
#include <stdexcept>
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
        std::string address;
        uint16_t    provider_id;
    };

    struct Metadata {
        std::string key;
        std::string value;
    };

    struct MembersProxy {

        private:

        friend class GroupView;

        GroupView& m_owner;

        MembersProxy(GroupView& gv)
        : m_owner(gv) {}

        public:

        MembersProxy(MembersProxy&&) = default;

        void add(const char* address, uint16_t provider_id) {
            flock_group_view_add_member(&m_owner.m_view, address, provider_id);
        }

        void remove(size_t index) {
            auto member = flock_group_view_member_at(&m_owner.m_view, index);
            if(!flock_group_view_remove_member(&m_owner.m_view, member))
                throw Exception{FLOCK_ERR_NO_MEMBER};
        }

        void remove(const char* address, uint16_t provider_id) {
            auto member = flock_group_view_find_member(&m_owner.m_view, address, provider_id);
            if(!flock_group_view_remove_member(&m_owner.m_view, member))
                throw Exception{FLOCK_ERR_NO_MEMBER};
        }

        bool exists(const char* address, uint16_t provider_id) const {
            return static_cast<bool>(flock_group_view_find_member(
                    const_cast<flock_group_view_t*>(&m_owner.m_view), address, provider_id));
        }

        size_t count() const {
            return flock_group_view_member_count(&m_owner.m_view);
        }

        Member operator[](size_t i) const {
            if(i >= count()) throw std::out_of_range{"Invalid member index"};
            auto member = flock_group_view_member_at(&m_owner.m_view, i);
            return Member{member->address, member->provider_id};
        }
    };

    struct MetadataProxy {

        private:

        friend class GroupView;

        GroupView& m_owner;

        MetadataProxy(GroupView& gv)
        : m_owner(gv) {}

        public:

        MetadataProxy(MetadataProxy&&) = default;

        void add(const char* key, const char* value) {
            flock_group_view_add_metadata(&m_owner.m_view, key, value);
        }

        void remove(const char* key) {
            flock_group_view_remove_metadata(&m_owner.m_view, key);
        }

        size_t count() const {
            return flock_group_view_metadata_count(&m_owner.m_view);
        }

        Metadata operator[](size_t i) const {
            if(i >= count()) throw std::out_of_range{"Invalid metadata index"};
            auto metadata = flock_group_view_metadata_at(&m_owner.m_view, i);
            return Metadata{metadata->key, metadata->value};
        }

        const char* operator[](const char* key) const {
            return flock_group_view_find_metadata(&m_owner.m_view, key);
        }
    };

    GroupView() = default;

    GroupView(flock_group_view_t view) {
        FLOCK_GROUP_VIEW_MOVE(&view, &m_view);
    }

    ~GroupView() {
        clear();
    }

    GroupView(GroupView&& other) {
        FLOCK_GROUP_VIEW_MOVE(&other.m_view, &m_view);
    }

    GroupView& operator=(GroupView&& other) {
        if(this == &other) return *this;
        clear();
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

    auto members() {
        return MembersProxy{*this};
    }

    auto metadata() {
        return MetadataProxy{*this};
    }

    auto copy() const {
        auto result = GroupView{};
        auto members = const_cast<GroupView*>(this)->members();
        for(size_t i = 0; i < members.count(); ++i) {
            result.members().add(members[i].address.c_str(), members[i].provider_id);
        }
        auto metadata = const_cast<GroupView*>(this)->metadata();
        for(size_t i = 0; i < metadata.count(); ++i) {
            result.metadata().add(metadata[i].key.c_str(), metadata[i].value.c_str());
        }
        return result;
    }

    operator std::string() const {
        std::string result;
        flock_return_t ret = flock_group_view_serialize(
            &m_view, [](void* ctx, const char* content, size_t size) {
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
