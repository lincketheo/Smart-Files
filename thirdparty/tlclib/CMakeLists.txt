cmake_minimum_required(VERSION 3.18)

file(STRINGS "${CMAKE_SOURCE_DIR}/version.txt" TLCLIB_VERSION LIMIT_COUNT 1)

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
include(cmake/Packaging.cmake)

add_subdirectory(src)

# Build the Test Binary
add_executable(test test.c)
target_link_libraries(test PRIVATE tlclib)
target_include_directories(test PUBLIC include)

# Link against pthread
if(NOT WIN32)
    target_link_libraries(test PRIVATE pthread)
endif()

install(TARGETS test
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    COMPONENT runtime
)
