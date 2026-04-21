cmake_minimum_required(VERSION 3.18)

file(STRINGS "${CMAKE_SOURCE_DIR}/version.txt" NS_VERSION LIMIT_COUNT 1)
project(smartfiles
    VERSION     "${NS_VERSION}"
    DESCRIPTION "Custom database engine for contiguous byte streams"
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

add_subdirectory(thirdparty)
add_subdirectory(lib)
add_subdirectory(apps)
add_subdirectory(samples)

# macOS leak check helper 
if(APPLE AND TARGET test_bin)
    add_custom_target(leaks
        COMMAND leaks --atExit -- $<TARGET_FILE:test_bin>
        DEPENDS test_bin
        COMMENT "Running leaks tool (macOS)"
    )
endif()
