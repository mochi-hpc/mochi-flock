/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <flock/flock-group-view.h>
#include <flock/cxx/exception.hpp>
#include <flock/cxx/group-view.hpp>

namespace py11 = pybind11;
using namespace pybind11::literals;

struct margo_instance;
typedef struct margo_instance* margo_instance_id;
typedef py11::capsule pymargo_instance_id;

#define MID2CAPSULE(__mid)   py11::capsule((void*)(__mid), "margo_instance_id")
#define CAPSULE2MID(__caps)  (margo_instance_id)(__caps)

// Iterator helper for MembersProxy
struct MembersIterator {
    flock::GroupView::MembersProxy* proxy;
    size_t index;

    bool operator==(const MembersIterator& other) const {
        return index == other.index;
    }

    bool operator!=(const MembersIterator& other) const {
        return index != other.index;
    }

    MembersIterator& operator++() {
        ++index;
        return *this;
    }

    flock::GroupView::Member operator*() const {
        return (*proxy)[index];
    }
};

// Iterator helper for MetadataProxy
struct MetadataIterator {
    flock::GroupView::MetadataProxy* proxy;
    size_t index;

    bool operator==(const MetadataIterator& other) const {
        return index == other.index;
    }

    bool operator!=(const MetadataIterator& other) const {
        return index != other.index;
    }

    MetadataIterator& operator++() {
        ++index;
        return *this;
    }

    flock::GroupView::Metadata operator*() const {
        return (*proxy)[index];
    }
};

PYBIND11_MODULE(pyflock_common, m) {
    m.doc() = "Flock common python extension";
    py11::register_exception<flock::Exception>(m, "Exception", PyExc_RuntimeError);

    py11::class_<flock::GroupView::Member>(m, "Member")
        .def(py11::init<const std::string&, uint16_t>())
        .def_readonly("provider_id", &flock::GroupView::Member::provider_id)
        .def_readonly("address", &flock::GroupView::Member::address);

    py11::class_<flock::GroupView::Metadata>(m, "Metadata")
        .def_readonly("key", &flock::GroupView::Metadata::key)
        .def_readonly("value", &flock::GroupView::Metadata::value);

    py11::class_<flock::GroupView::MembersProxy, std::shared_ptr<flock::GroupView::MembersProxy>>(
            m, "MembersProxy")
        .def("__len__", &flock::GroupView::MembersProxy::count)
        .def_property_readonly("count", &flock::GroupView::MembersProxy::count)
        .def("add", &flock::GroupView::MembersProxy::add,
             "address"_a, "provider_id"_a)
        .def("remove", [](flock::GroupView::MembersProxy& proxy, size_t i) {
            proxy.remove(i);
        }, "index"_a)
        .def("remove", [](flock::GroupView::MembersProxy& proxy, const char* address, uint16_t provider_id) {
            proxy.remove(address, provider_id);
        }, "address"_a, "provider_id"_a)
        .def("exists", &flock::GroupView::MembersProxy::exists,
             "address"_a, "provider_id"_a)
        .def("__getitem__", &flock::GroupView::MembersProxy::operator[],
             "index"_a)
        .def("__delitem__", [](flock::GroupView::MembersProxy& proxy, size_t i) {
            proxy.remove(i);
        }, "index"_a)
        .def("__iter__", [](flock::GroupView::MembersProxy& proxy) {
            return py11::make_iterator(
                MembersIterator{&proxy, 0},
                MembersIterator{&proxy, proxy.count()}
            );
        }, py11::keep_alive<0, 1>())
        .def("__contains__", [](flock::GroupView::MembersProxy& proxy, const py11::tuple& member) {
            if (py11::len(member) != 2) {
                throw std::invalid_argument("Expected tuple of (address, provider_id)");
            }
            std::string address = member[0].cast<std::string>();
            uint16_t provider_id = member[1].cast<uint16_t>();
            return proxy.exists(address.c_str(), provider_id);
        })
        ;

    py11::class_<flock::GroupView::MetadataProxy, std::shared_ptr<flock::GroupView::MetadataProxy>>(
            m, "MetadataProxy")
        .def("__len__", &flock::GroupView::MetadataProxy::count)
        .def_property_readonly("count", &flock::GroupView::MetadataProxy::count)
        .def("add", &flock::GroupView::MetadataProxy::add,
             "key"_a, "value"_a)
        .def("remove", &flock::GroupView::MetadataProxy::remove,
             "key"_a)
        .def("__getitem__", [](flock::GroupView::MetadataProxy& proxy, const std::string& key) {
            return proxy[key.c_str()];
        }, "key"_a)
        .def("__getitem__", [](flock::GroupView::MetadataProxy& proxy, size_t index) {
            return proxy[index];
        }, "index"_a)
        .def("__delitem__", &flock::GroupView::MetadataProxy::remove,
           "key"_a)
        .def("__iter__", [](flock::GroupView::MetadataProxy& proxy) {
            return py11::make_iterator(
                MetadataIterator{&proxy, 0},
                MetadataIterator{&proxy, proxy.count()}
            );
        }, py11::keep_alive<0, 1>())
        .def("__contains__", [](flock::GroupView::MetadataProxy& proxy, const std::string& key) {
            return proxy[key.c_str()] != nullptr;
        })
        ;

    py11::class_<flock::GroupView, std::shared_ptr<flock::GroupView>>(m, "GroupView")
        .def(py11::init<>())
        .def_property_readonly("digest", &flock::GroupView::digest)
        .def("clear", &flock::GroupView::clear)
        .def("lock", &flock::GroupView::lock)
        .def("unlock", &flock::GroupView::unlock)
        .def("_members", &flock::GroupView::members, py11::keep_alive<0, 1>())
        .def("_metadata", &flock::GroupView::metadata, py11::keep_alive<0, 1>())
        .def("__str__", [](const flock::GroupView& gv) {
                return static_cast<std::string>(gv);
        })
        .def("copy", &flock::GroupView::copy)
        .def("serialize_to_file", [](const flock::GroupView& gv, const std::string& filename) {
            // Serialize to string first, then write to file
            std::string content = static_cast<std::string>(gv);
            // Use the C function which handles atomic write
            flock_group_view_t temp_view = FLOCK_GROUP_VIEW_INITIALIZER;
            auto ret = flock_group_view_from_string(content.c_str(), content.size(), &temp_view);
            if (ret != FLOCK_SUCCESS) {
                throw flock::Exception{ret};
            }
            ret = flock_group_view_serialize_to_file(&temp_view, filename.c_str());
            flock_group_view_clear(&temp_view);
            if (ret != FLOCK_SUCCESS) {
                throw flock::Exception{ret};
            }
        }, "filename"_a, "Serialize the group view to a file")
        .def_static("from_file", [](const std::string& filename) {
            flock_group_view_t view = FLOCK_GROUP_VIEW_INITIALIZER;
            auto ret = flock_group_view_from_file(filename.c_str(), &view);
            if (ret != FLOCK_SUCCESS) {
                throw flock::Exception{ret};
            }
            return flock::GroupView{view};
        }, "filename"_a, "Load a group view from a file")
        .def_static("from_string", [](const std::string& content) {
            flock_group_view_t view = FLOCK_GROUP_VIEW_INITIALIZER;
            auto ret = flock_group_view_from_string(content.c_str(), content.size(), &view);
            if (ret != FLOCK_SUCCESS) {
                throw flock::Exception{ret};
            }
            return flock::GroupView{view};
        }, "content"_a, "Load a group view from a serialized string")
        ;
}
