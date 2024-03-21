add_executable (test-client test-client.cpp)
target_include_directories (test-client PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
  ${CMAKE_CURRENT_BINARY_DIR}/../src
)
target_link_libraries (test-client PRIVATE Catch2::Catch2WithMain alpha::server alpha::client)
add_test (NAME TestClient COMMAND ./test-client)
