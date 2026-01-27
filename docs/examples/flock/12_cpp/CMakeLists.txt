add_executable (12_flock_server server.cpp)
target_link_libraries (12_flock_server flock::headers flock::server)

add_executable (12_flock_client client.cpp)
target_link_libraries (12_flock_client flock::headers flock::client)
