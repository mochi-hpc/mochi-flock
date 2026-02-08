add_executable (13_flock_server server.cpp)
target_link_libraries (13_flock_server flock::cxx-headers flock::server)

add_executable (13_flock_client client.cpp)
target_link_libraries (13_flock_client flock::cxx-headers flock::client)
