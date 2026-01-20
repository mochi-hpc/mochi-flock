# set source files
set (server-src-files
     bootstrap.c
     provider.c)

set (client-src-files
     client.c
     group-handle.c
     view-serialize.c)

set (backend-src-files
     static/static-backend.c
     centralized/centralized-backend.c
     swim/swim-backend.c
     swim/swim-gossip-buffer.c)

set (bedrock-module-src-files
     bedrock-module.cpp)

# load package helper for generating cmake CONFIG packages
include (CMakePackageConfigHelpers)

# where to install files for "find_package"
set (flock-pkg "share/cmake/flock")

# set version fo shared library
set (flock-vers "${FLOCK_VERSION_MAJOR}.${FLOCK_VERSION_MINOR}")

# client library
add_library (flock-client ${client-src-files})
add_library (flock::client ALIAS flock-client)
target_link_libraries (flock-client
    PUBLIC PkgConfig::margo
    PRIVATE coverage_config)
target_include_directories (flock-client PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (flock-client BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (flock-client BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (flock-client
    PROPERTIES VERSION ${FLOCK_VERSION}
    SOVERSION ${FLOCK_VERSION_MAJOR})

# server library
add_library (flock-server ${server-src-files} ${backend-src-files})
add_library (flock::server ALIAS flock-server)
target_link_libraries (flock-server
    PUBLIC PkgConfig::margo
    PRIVATE coverage_config PkgConfig::json-c flock-client)
target_include_directories (flock-server PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (flock-server BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (flock-server BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
if (ENABLE_MPI)
    target_compile_definitions (flock-server PRIVATE ENABLE_MPI)
    target_link_libraries (flock-server PUBLIC MPI::MPI_C)
endif ()
set_target_properties (flock-server
    PROPERTIES VERSION ${FLOCK_VERSION}
    SOVERSION ${FLOCK_VERSION_MAJOR})

if (${ENABLE_BEDROCK})
# bedrock-module library
add_library (flock-bedrock-module ${bedrock-module-src-files})
target_link_libraries (flock-bedrock-module PRIVATE flock-server flock-client bedrock::module-api)
target_include_directories (flock-bedrock-module PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (flock-bedrock-module BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (flock-bedrock-module BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
if (ENABLE_MPI)
    target_compile_definitions (flock-bedrock-module PRIVATE ENABLE_MPI)
    target_link_libraries (flock-bedrock-module PUBLIC MPI::MPI_C)
endif ()
set_target_properties (flock-bedrock-module
    PROPERTIES VERSION ${FLOCK_VERSION}
    SOVERSION ${FLOCK_VERSION_MAJOR})
endif ()

add_library (flock-cxx-headers INTERFACE)
add_library (flock::headers ALIAS flock-cxx-headers)
target_link_libraries (flock-cxx-headers
    INTERFACE PkgConfig::margo thallium flock-client flock-server)
target_include_directories (flock-cxx-headers INTERFACE $<INSTALL_INTERFACE:include>)

# installation stuff (packaging and install commands)
write_basic_package_version_file (
    "flock-config-version.cmake"
    VERSION ${FLOCK_VERSION}
    COMPATIBILITY AnyNewerVersion)

# generate our config file for find_package()
set (INCLUDE_INSTALL_DIR include/)
set (LIB_INSTALL_DIR lib/)
configure_package_config_file (flock-config.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/flock-config.cmake
  INSTALL_DESTINATION ${LIB_INSTALL_DIR}/flock/cmake
  PATH_VARS INCLUDE_INSTALL_DIR)

# some bits for the pkg-config file
set (DEST_DIR "${CMAKE_INSTALL_PREFIX}")
set (SERVER_PRIVATE_LIBS "-lflock-server")
set (CLIENT_PRIVATE_LIBS "-lflock-client")
configure_file ("flock-server.pc.in" "flock-server.pc" @ONLY)
configure_file ("flock-client.pc.in" "flock-client.pc" @ONLY)

# configure config.h
configure_file ("config.h.in" "config.h" @ONLY)

# "make install" rules
install (TARGETS flock-server flock-client flock-cxx-headers
         EXPORT flock-targets
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
if (${ENABLE_BEDROCK})
    install (TARGETS flock-bedrock-module
             ARCHIVE DESTINATION lib
             LIBRARY DESTINATION lib)
endif ()
install (EXPORT flock-targets
         DESTINATION ${flock-pkg}
         FILE "flock-targets.cmake")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/flock-config.cmake"
               "${CMAKE_CURRENT_BINARY_DIR}/flock-config-version.cmake"
         DESTINATION ${flock-pkg} )
install (DIRECTORY ../include/flock
         DESTINATION include)
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/flock-server.pc"
               "${CMAKE_CURRENT_BINARY_DIR}/flock-client.pc"
         DESTINATION "lib/pkgconfig/")
