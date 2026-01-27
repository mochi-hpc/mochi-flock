add_executable (02_flock_server server.c)
target_link_libraries(02_flock_server flock::server)
