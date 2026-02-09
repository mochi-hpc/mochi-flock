/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <flock/cxx/client.hpp>
#include <flock/cxx/group-view.hpp>
#include <flock/cxx/group.hpp>

namespace py11 = pybind11;
using namespace pybind11::literals;

struct margo_instance;
typedef struct margo_instance* margo_instance_id;
typedef py11::capsule pymargo_instance_id;

#define MID2CAPSULE(__mid)   py11::capsule((void*)(__mid), "margo_instance_id")
#define CAPSULE2MID(__caps)  (margo_instance_id)(__caps)

PYBIND11_MODULE(pyflock_client, m) {
    m.doc() = "Flock client python extension";
    py11::module_::import("pyflock_common");

    py11::class_<flock::Client, std::shared_ptr<flock::Client>>(m, "Client")
        .def(py11::init<pymargo_instance_id>())
        .def_property_readonly("margo_instance_id",
            [](const flock::Client& client) {
                return MID2CAPSULE(client.engine().get_margo_instance());
            })
        .def("make_group_handle",
             [](const flock::Client& client,
                const std::string& address,
                uint16_t provider_id) {
                auto addr = client.engine().lookup(address);
                return client.makeGroupHandle(addr.get_addr(), provider_id);
             },
             "Create a GroupHandle instance",
             "address"_a,
             "provider_id"_a=0,
             py11::keep_alive<0,1>())
        .def("make_group_handle_from_file",
             [](const flock::Client& client,
                const std::string& filename) {
                return flock::GroupHandle::FromFile(client, filename.c_str());
             },
             "Create a GroupHandle instance",
             "filename"_a,
             py11::keep_alive<0,1>())
        .def("make_group_handle_from_serialized",
             [](const flock::Client& client,
                std::string_view serialized) {
                return flock::GroupHandle::FromSerialized(client, serialized);
             },
             "Create a GroupHandle instance",
             "serialized"_a,
             py11::keep_alive<0,1>())
    ;
    py11::class_<flock::GroupHandle, std::shared_ptr<flock::GroupHandle>>(m, "GroupHandle")
        .def("update", &flock::GroupHandle::update)
        .def_property_readonly("view", &flock::GroupHandle::view)
    ;
}
