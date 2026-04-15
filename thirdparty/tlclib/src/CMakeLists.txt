# Gather base sources
file(GLOB_RECURSE ALL_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.c")
file(GLOB_RECURSE POSIX_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/intf/os_posix/*.c")
file(GLOB_RECURSE WINDOWS_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/intf/os_windows/*.c")

# Start with all sources, strip apps and both platform dirs
set(TLCLIB_SOURCES ${ALL_SOURCES})
list(REMOVE_ITEM TLCLIB_SOURCES ${POSIX_SOURCES})    # Don't include platform
list(REMOVE_ITEM TLCLIB_SOURCES ${WINDOWS_SOURCES})

# If tests are disabled (NTEST defined), remove test sources
if(ENABLE_NTEST)
    file(GLOB_RECURSE TEST_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/tests/*.c")
    if(TEST_SOURCES)
        list(REMOVE_ITEM TLCLIB_SOURCES ${TEST_SOURCES})
    endif()
endif()

# Add back the correct platform
if (WIN32)
    list(APPEND TLCLIB_SOURCES ${WINDOWS_SOURCES})
else ()
    list(APPEND TLCLIB_SOURCES ${POSIX_SOURCES})
endif ()

# The main library
add_library(tlclib STATIC ${TLCLIB_SOURCES})
target_include_directories(tlclib 
  PUBLIC "${CMAKE_SOURCE_DIR}/include"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}"
)

# Add c11 option
if (MSVC)
    target_compile_options(tlclib PRIVATE /std:c11)
else ()
    target_compile_features(tlclib PRIVATE c_std_11)
endif ()

if (WIN32)
    target_include_directories(tlclib PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/intf/os_windows")
    target_link_libraries(tlclib PUBLIC ws2_32)
else ()
    target_include_directories(tlclib PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/intf/os_posix")
    target_link_libraries(tlclib PUBLIC pthread)
endif ()

set_target_properties(tlclib PROPERTIES
        OUTPUT_NAME "tlclib"
        VERSION "${PROJECT_VERSION}"
)

install(TARGETS tlclib
        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        COMPONENT development
)

install(DIRECTORY "${CMAKE_SOURCE_DIR}/include/"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
        COMPONENT development
        FILES_MATCHING PATTERN "*.h"
)

install(
        FILES
        "${CMAKE_SOURCE_DIR}/README.md"
        "${CMAKE_SOURCE_DIR}/LICENSE.md"
        DESTINATION "${CMAKE_INSTALL_DOCDIR}"
        COMPONENT runtime
        OPTIONAL
)
