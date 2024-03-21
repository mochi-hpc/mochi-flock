add_executable (example-server ${CMAKE_CURRENT_SOURCE_DIR}/server.c)
target_link_libraries (example-server flock-server)

add_executable (example-client ${CMAKE_CURRENT_SOURCE_DIR}/client.c)
target_link_libraries (example-client flock-client)
