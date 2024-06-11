add_library (pyflock_client MODULE src/py-flock-client.cpp)
target_link_libraries (pyflock_client PRIVATE pybind11::module flock-cxx-headers PRIVATE coverage_config)
pybind11_extension (pyflock_client)
pybind11_strip (pyflock_client)

add_library (pyflock_server MODULE src/py-flock-server.cpp)
target_link_libraries (pyflock_server PRIVATE pybind11::module flock-cxx-headers PRIVATE coverage_config)
pybind11_extension (pyflock_server)
pybind11_strip (pyflock_server)

add_library (pyflock_common MODULE src/py-flock-common.cpp)
target_link_libraries (pyflock_common PRIVATE pybind11::module flock-cxx-headers PRIVATE coverage_config)
pybind11_extension (pyflock_common)
pybind11_strip (pyflock_common)

set (PY_VERSION ${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR})

install (TARGETS pyflock_client pyflock_server pyflock_common
         EXPORT flock-targets
         ARCHIVE DESTINATION lib/python${PY_VERSION}/site-packages
         LIBRARY DESTINATION lib/python${PY_VERSION}/site-packages)

install (DIRECTORY mochi/flock
         DESTINATION lib/python${PY_VERSION}/site-packages/mochi)
