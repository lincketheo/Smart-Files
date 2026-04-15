cmake_minimum_required(VERSION 3.18)

file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/version.txt" TLCLIB_VERSION LIMIT_COUNT 1)

project(tlclib
    VERSION     "${TLCLIB_VERSION}"
    DESCRIPTION "Theo Lincke's standard c library and tools"
    LANGUAGES   C
)

set(CMAKE_C_STANDARD 11)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(GNUInstallDirs)
include(cmake/CompileConfig.cmake)
include(cmake/Options.cmake)
include(cmake/CompilerFlags.cmake)
include(cmake/Sanitizers.cmake)
add_subdirectory(src)

# Only build the test binary and package when tlclib is the top-level project
if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    include(cmake/Packaging.cmake)

    add_executable(tlclib_test test.c)
    target_link_libraries(tlclib_test PRIVATE tlclib)
    target_include_directories(tlclib_test PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")

    if(NOT WIN32)
        target_link_libraries(tlclib_test PRIVATE pthread)
    endif()

    install(TARGETS tlclib_test
        RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
        COMPONENT runtime
    )
endif()
