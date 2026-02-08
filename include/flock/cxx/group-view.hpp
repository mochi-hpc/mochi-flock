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

/**
 * @brief C++ wrapper around flock_group_view_t, representing a group's
 * membership and metadata.
 *
 * A GroupView contains a sorted list of members (address + provider_id pairs)
 * and a sorted map of metadata (key-value string pairs). It maintains a digest
 * (hash) that is automatically updated when members or metadata are added or
 * removed, enabling efficient change detection.
 *
 * GroupView is move-only (non-copyable via assignment/construction); use copy()
 * to create an explicit deep copy. Access members and metadata through the
 * proxy objects returned by members() and metadata().
 *
 * The view is thread-safe when accessed via lock()/unlock().
 *
 * @see flock_group_view_t
 */
class GroupView {

    friend class GroupHandle;
    friend class Provider;

    public:

    /**
     * @brief Represents a group member with an address and provider ID.
     */
    struct Member {
        std::string address;     /**< Mercury address of the member. */
        uint16_t    provider_id; /**< Provider ID of the member. */
    };

    /**
     * @brief Represents a metadata entry as a key-value string pair.
     */
    struct Metadata {
        std::string key;   /**< Metadata key. */
        std::string value; /**< Metadata value. */
    };

    /**
     * @brief Proxy object for accessing and modifying the members of a GroupView.
     *
     * Obtained via GroupView::members(). Provides methods to add, remove, find,
     * and index members. All modifications automatically update the view's digest.
     */
    struct MembersProxy {

        private:

        friend class GroupView;

        GroupView& m_owner;

        MembersProxy(GroupView& gv)
        : m_owner(gv) {}

        public:

        MembersProxy(MembersProxy&&) = default;

        /**
         * @brief Add a member to the group view.
         *
         * The member is inserted in sorted order. The caller is responsible
         * for ensuring the (address, provider_id) pair is not already present.
         *
         * @param address Mercury address of the new member.
         * @param provider_id Provider ID of the new member.
         *
         * @see flock_group_view_add_member
         */
        void add(const char* address, uint16_t provider_id) {
            flock_group_view_add_member(&m_owner.m_view, address, provider_id);
        }

        /**
         * @brief Remove a member by index.
         *
         * @param index Zero-based index of the member to remove.
         *
         * @throws flock::Exception with FLOCK_ERR_NO_MEMBER if the index is invalid.
         *
         * @see flock_group_view_remove_member
         */
        void remove(size_t index) {
            auto member = flock_group_view_member_at(&m_owner.m_view, index);
            if(!flock_group_view_remove_member(&m_owner.m_view, member))
                throw Exception{FLOCK_ERR_NO_MEMBER};
        }

        /**
         * @brief Remove a member by address and provider ID.
         *
         * @param address Mercury address of the member to remove.
         * @param provider_id Provider ID of the member to remove.
         *
         * @throws flock::Exception with FLOCK_ERR_NO_MEMBER if the member is not found.
         *
         * @see flock_group_view_find_member, flock_group_view_remove_member
         */
        void remove(const char* address, uint16_t provider_id) {
            auto member = flock_group_view_find_member(&m_owner.m_view, address, provider_id);
            if(!flock_group_view_remove_member(&m_owner.m_view, member))
                throw Exception{FLOCK_ERR_NO_MEMBER};
        }

        /**
         * @brief Check whether a member with the given address and provider ID exists.
         *
         * @param address Mercury address to look up.
         * @param provider_id Provider ID to look up.
         *
         * @return true if the member exists, false otherwise.
         *
         * @see flock_group_view_find_member
         */
        bool exists(const char* address, uint16_t provider_id) const {
            return static_cast<bool>(flock_group_view_find_member(
                    const_cast<flock_group_view_t*>(&m_owner.m_view), address, provider_id));
        }

        /**
         * @brief Return the number of members in the group view.
         *
         * @return The number of members.
         *
         * @see flock_group_view_member_count
         */
        size_t count() const {
            return flock_group_view_member_count(&m_owner.m_view);
        }

        /**
         * @brief Access a member by index.
         *
         * @param i Zero-based index of the member.
         *
         * @return A Member struct containing the address and provider ID.
         *
         * @throws std::out_of_range if the index is out of bounds.
         *
         * @see flock_group_view_member_at
         */
        Member operator[](size_t i) const {
            if(i >= count()) throw std::out_of_range{"Invalid member index"};
            auto member = flock_group_view_member_at(&m_owner.m_view, i);
            return Member{member->address, member->provider_id};
        }
    };

    /**
     * @brief Proxy object for accessing and modifying the metadata of a GroupView.
     *
     * Obtained via GroupView::metadata(). Provides methods to add, remove,
     * and look up metadata entries. All modifications automatically update
     * the view's digest.
     */
    struct MetadataProxy {

        private:

        friend class GroupView;

        GroupView& m_owner;

        MetadataProxy(GroupView& gv)
        : m_owner(gv) {}

        public:

        MetadataProxy(MetadataProxy&&) = default;

        /**
         * @brief Add or update a metadata key-value pair.
         *
         * If the key already exists, its value is replaced.
         *
         * @param key Metadata key.
         * @param value Metadata value.
         *
         * @see flock_group_view_add_metadata
         */
        void add(const char* key, const char* value) {
            flock_group_view_add_metadata(&m_owner.m_view, key, value);
        }

        /**
         * @brief Remove a metadata entry by key.
         *
         * @param key Key of the metadata entry to remove.
         *
         * @see flock_group_view_remove_metadata
         */
        void remove(const char* key) {
            flock_group_view_remove_metadata(&m_owner.m_view, key);
        }

        /**
         * @brief Return the number of metadata entries in the group view.
         *
         * @return The number of metadata entries.
         *
         * @see flock_group_view_metadata_count
         */
        size_t count() const {
            return flock_group_view_metadata_count(&m_owner.m_view);
        }

        /**
         * @brief Access a metadata entry by index.
         *
         * @param i Zero-based index of the metadata entry.
         *
         * @return A Metadata struct containing the key and value.
         *
         * @throws std::out_of_range if the index is out of bounds.
         *
         * @see flock_group_view_metadata_at
         */
        Metadata operator[](size_t i) const {
            if(i >= count()) throw std::out_of_range{"Invalid metadata index"};
            auto metadata = flock_group_view_metadata_at(&m_owner.m_view, i);
            return Metadata{metadata->key, metadata->value};
        }

        /**
         * @brief Look up a metadata value by key.
         *
         * @param key Key to search for.
         *
         * @return Pointer to the value string, or NULL if the key is not found.
         *
         * @see flock_group_view_find_metadata
         */
        const char* operator[](const char* key) const {
            return flock_group_view_find_metadata(&m_owner.m_view, key);
        }
    };

    /**
     * @brief Default constructor. Creates an empty group view.
     */
    GroupView() = default;

    /**
     * @brief Construct a GroupView by moving from a C flock_group_view_t.
     *
     * The source view is left empty after the move.
     *
     * @param view C group view to move from.
     */
    GroupView(flock_group_view_t view) {
        FLOCK_GROUP_VIEW_MOVE(&view, &m_view);
    }

    /**
     * @brief Destructor. Clears and frees all members and metadata.
     *
     * @see flock_group_view_clear
     */
    ~GroupView() {
        clear();
    }

    /**
     * @brief Move constructor. Transfers ownership of the view's content.
     *
     * @param other GroupView to move from. Left empty after the move.
     */
    GroupView(GroupView&& other) {
        FLOCK_GROUP_VIEW_MOVE(&other.m_view, &m_view);
    }

    /**
     * @brief Move assignment operator. Clears the current content and
     * transfers ownership from the source.
     *
     * @param other GroupView to move from. Left empty after the move.
     * @return Reference to this GroupView.
     */
    GroupView& operator=(GroupView&& other) {
        if(this == &other) return *this;
        clear();
        FLOCK_GROUP_VIEW_MOVE(&other.m_view, &m_view);
        return *this;
    }

    /**
     * @brief Return the digest (hash) of the group view's content.
     *
     * The digest is automatically maintained as members and metadata are
     * added or removed. It can be used to efficiently detect whether two
     * views have the same content.
     *
     * @return The digest as a uint64_t.
     */
    auto digest() const {
        return m_view.digest;
    }

    /**
     * @brief Lock the view's internal mutex for thread-safe access.
     *
     * @see FLOCK_GROUP_VIEW_LOCK
     */
    void lock() {
        FLOCK_GROUP_VIEW_LOCK(&m_view);
    }

    /**
     * @brief Unlock the view's internal mutex.
     *
     * @see FLOCK_GROUP_VIEW_UNLOCK
     */
    void unlock() {
        FLOCK_GROUP_VIEW_UNLOCK(&m_view);
    }

    /**
     * @brief Clear all members and metadata from the view, resetting
     * the digest to 0.
     *
     * @see flock_group_view_clear
     */
    void clear() {
        flock_group_view_clear(&m_view);
    }

    /**
     * @brief Return a proxy object for accessing and modifying the
     * group's members.
     *
     * @return A MembersProxy bound to this GroupView.
     */
    auto members() {
        return MembersProxy{*this};
    }

    /**
     * @brief Return a proxy object for accessing and modifying the
     * group's metadata.
     *
     * @return A MetadataProxy bound to this GroupView.
     */
    auto metadata() {
        return MetadataProxy{*this};
    }

    /**
     * @brief Create a deep copy of this GroupView.
     *
     * All members and metadata are copied into a new GroupView.
     *
     * @return A new GroupView with the same content.
     */
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

    /**
     * @brief Serialize the group view to a JSON string.
     *
     * @return A std::string containing the JSON-serialized group view.
     *
     * @throws flock::Exception on serialization failure.
     *
     * @see flock_group_view_serialize
     */
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
