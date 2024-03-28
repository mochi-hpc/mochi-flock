file (GLOB test-sources ${CMAKE_CURRENT_SOURCE_DIR}/test-*.cpp)

foreach (test-source ${test-sources})
    get_filename_component (name ${test-source} NAME_WE)
    add_executable (${name} ${test-source})
    target_include_directories (${name} PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
        ${CMAKE_CURRENT_BINARY_DIR}/../src)
    target_link_libraries (${name}
        PRIVATE Catch2::Catch2WithMain flock::server flock::client coverage_config)
    add_test (NAME ${name} COMMAND ./${name})
endforeach ()
