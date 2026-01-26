set(LIB_NAME rocksdb)
# Collect all cpp files in this directory
file(GLOB TEST_SOURCES CONFIGURE_DEPENDS "*.cpp")

foreach (TEST_SRC ${TEST_SOURCES})
    # Extract file name without extension
    get_filename_component(TEST_NAME ${TEST_SRC} NAME_WE)
    # Create executable
    add_executable(${TEST_NAME} ${TEST_SRC})
    # Link main library
    target_link_libraries(${TEST_NAME} PRIVATE ${LIB_NAME})
    target_include_directories(${TEST_NAME}
            PRIVATE ${PROJECT_SOURCE_DIR}/include
    )
    # Register with CTest
    add_test(NAME ${TEST_NAME}
            COMMAND ${TEST_NAME}
    )
endforeach ()
