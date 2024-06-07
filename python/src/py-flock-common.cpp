/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <flock/cxx/exception.hpp>
#include <flock/cxx/group-view.hpp>

namespace py11 = pybind11;
using namespace pybind11::literals;

struct margo_instance;
typedef struct margo_instance* margo_instance_id;
typedef py11::capsule pymargo_instance_id;

#define MID2CAPSULE(__mid)   py11::capsule((void*)(__mid), "margo_instance_id")
#define CAPSULE2MID(__caps)  (margo_instance_id)(__caps)

PYBIND11_MODULE(pyflock_common, m) {
    m.doc() = "Flock common python extension";
    py11::register_exception<flock::Exception>(m, "Exception", PyExc_RuntimeError);

    py11::class_<flock::GroupView::Member>(m, "Member")
        .def_readonly("provider_id", &flock::GroupView::Member::provider_id)
        .def_readonly("address", &flock::GroupView::Member::address);

    py11::class_<flock::GroupView::Metadata>(m, "Metadata")
        .def_readonly("key", &flock::GroupView::Metadata::key)
        .def_readonly("value", &flock::GroupView::Metadata::value);

    py11::class_<flock::GroupView::MembersProxy>(m, "MembersProxy")
        .def("__len__", &flock::GroupView::MembersProxy::count)
        .def("count", &flock::GroupView::MembersProxy::count)
        .def("add", &flock::GroupView::MembersProxy::add)
        .def("remove", [](flock::GroupView::MembersProxy& proxy, size_t i) {
            proxy.remove(i);
        })
        .def("remove", [](flock::GroupView::MembersProxy& proxy, const char* address, uint16_t provider_id) {
            proxy.remove(address, provider_id);
        })
        .def("exists", &flock::GroupView::MembersProxy::exists)
        .def("__getitem__", &flock::GroupView::MembersProxy::operator[])
        .def("__delitem__", [](flock::GroupView::MembersProxy& proxy, size_t i) {
            proxy.remove(i);
        })
        ;

    py11::class_<flock::GroupView::MetadataProxy>(m, "MetadataProxy")
        .def("__len__", &flock::GroupView::MetadataProxy::count)
        .def("count", &flock::GroupView::MetadataProxy::count)
        .def("add", &flock::GroupView::MetadataProxy::add)
        .def("remove", &flock::GroupView::MetadataProxy::remove)
        .def("__getitem__", [](flock::GroupView::MetadataProxy& proxy, const char* key) {
            return proxy[key];
        })
        .def("__delitem__", &flock::GroupView::MetadataProxy::remove)
        ;

    py11::class_<flock::GroupView>(m, "GroupView")
        .def(py11::init<>())
        .def_property_readonly("digest", &flock::GroupView::digest)
        .def("clear", &flock::GroupView::clear)
        .def("lock", &flock::GroupView::lock)
        .def("unlock", &flock::GroupView::unlock)
        .def_property_readonly("members", &flock::GroupView::members, py11::keep_alive<0, 1>())
        .def_property_readonly("metadata", &flock::GroupView::metadata, py11::keep_alive<0, 1>())
        ;
}
