/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <flock/cxx/server.hpp>

namespace py11 = pybind11;
using namespace pybind11::literals;

struct margo_instance;
typedef struct margo_instance* margo_instance_id;
typedef struct hg_addr* hg_addr_t;
typedef py11::capsule pymargo_instance_id;
typedef py11::capsule pyhg_addr_t;

#define MID2CAPSULE(__mid)   py11::capsule((void*)(__mid), "margo_instance_id")
#define CAPSULE2MID(__caps)  (margo_instance_id)(__caps)

PYBIND11_MODULE(pyflock_server, m) {
    m.doc() = "Flock server python extension";

    py11::class_<flock::Provider>(m, "Provider")
        .def(py11::init<pymargo_instance_id, uint16_t, const char*, flock::GroupView&>())
    ;
}
