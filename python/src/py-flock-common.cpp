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
        .def_readonly("rank", &flock::GroupView::Member::rank)
        .def_readonly("provider_id", &flock::GroupView::Member::provider_id)
        .def_readonly("address", &flock::GroupView::Member::address);

    py11::class_<flock::GroupView>(m, "GroupView")
        .def(py11::init<>())
        .def_property_readonly("digest", &flock::GroupView::digest)
        .def("clear", &flock::GroupView::clear)
        .def("lock", &flock::GroupView::lock)
        .def("unlock", &flock::GroupView::unlock)
        .def("add_member", &flock::GroupView::addMember)
        .def("remove_member", &flock::GroupView::removeMember)
        .def("find_member", &flock::GroupView::findMember)
        .def_property_readonly("members", &flock::GroupView::members)
        ;
}
