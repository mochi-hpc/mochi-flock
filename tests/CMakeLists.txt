file (GLOB test-sources ${CMAKE_CURRENT_SOURCE_DIR}/test-*.cpp)

foreach (test-source ${test-sources})
    get_filename_component (name ${test-source} NAME_WE)
    add_executable (${name} ${test-source})
    target_include_directories (${name} PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
        ${CMAKE_CURRENT_BINARY_DIR}/../src)
    target_link_libraries (${name}
        Catch2::Catch2WithMain flock::server flock::client coverage_config)
    if (name MATCHES "^test-mpi.*$")
        add_test (NAME ${name} COMMAND "${MPIEXEC_EXECUTABLE}" ${MPIEXEC_NUMPROC_FLAG} 5 ./${name})
        target_link_libraries (${name} MPI::MPI_C)
    else ()
        add_test (NAME ${name} COMMAND ./${name})
    endif ()
endforeach ()
