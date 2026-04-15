# Gather base sources
file(GLOB_RECURSE NS_ALL_SOURCES  CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.c")
file(GLOB_RECURSE NS_TEST_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*test*.c")
file(GLOB_RECURSE NS_APP_SOURCES  CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/**/*_app.c")
file(GLOB_RECURSE NS_POSIX_SOURCES   CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/intf/os_posix/*.c")
file(GLOB_RECURSE NS_WINDOWS_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/intf/os_windows/*.c")

# Start with all sources, strip apps and both platform dirs
set(NS_SOURCES ${NS_ALL_SOURCES})
list(REMOVE_ITEM NS_SOURCES ${NS_APP_SOURCES})      # Don't include apps in sources
list(REMOVE_ITEM NS_SOURCES ${NS_POSIX_SOURCES})    # Don't include platform
list(REMOVE_ITEM NS_SOURCES ${NS_WINDOWS_SOURCES})  # Don't include platform
list(REMOVE_ITEM NS_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/numstore/types/literal_eval.c)
list(REMOVE_ITEM NS_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/numstore/types/literal.c)

# Add back the correct platform
if(WIN32)
    list(APPEND NS_SOURCES ${NS_WINDOWS_SOURCES})
else()
    list(APPEND NS_SOURCES ${NS_POSIX_SOURCES})
endif()

# If tests are disabled (NTEST defined), remove test sources
if(ENABLE_NTEST)
    list(REMOVE_ITEM NS_SOURCES ${NS_TEST_SOURCES})
    list(REMOVE_ITEM NS_APP_SOURCES ${NS_TEST_SOURCES})
endif()

# The main library
add_library(numstore STATIC ${NS_SOURCES})
target_include_directories(numstore
    PUBLIC  "${CMAKE_SOURCE_DIR}/include"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}"
)
if(MSVC)
    target_compile_options(numstore PRIVATE /std:c11)
else()
    target_compile_features(numstore PRIVATE c_std_11)
endif()

if(WIN32)
    target_include_directories(numstore PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/intf/os_windows")
    target_link_libraries(numstore PUBLIC ws2_32)
else()
    target_include_directories(numstore PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/intf/os_posix")
    target_link_libraries(numstore PUBLIC pthread)
endif()
set_target_properties(numstore PROPERTIES
    OUTPUT_NAME "numstore"
    VERSION     "${PROJECT_VERSION}"
)

foreach(APP_SRC ${NS_APP_SOURCES})
    get_filename_component(APP_NAME ${APP_SRC} NAME_WE)
    string(REGEX REPLACE "_app$" "" APP_NAME ${APP_NAME})
    add_executable(${APP_NAME} ${APP_SRC})
    if(APP_NAME STREQUAL "test")
        target_link_libraries(${APP_NAME} PRIVATE numstore)
        if(NOT WIN32)
            target_link_libraries(${APP_NAME} PRIVATE pthread)
        endif()
        if(APPLE)
            target_link_options(${APP_NAME} PRIVATE
                -Wl,-force_load,$<TARGET_FILE:numstore>)
        elseif(MSVC)
            target_link_options(${APP_NAME} PRIVATE
                /WHOLEARCHIVE:$<TARGET_FILE:numstore>)
        else()
            # GCC/Clang on Linux, MinGW on Windows, etc.
            target_link_options(${APP_NAME} PRIVATE
                -Wl,--whole-archive
                $<TARGET_FILE:numstore>
                -Wl,--no-whole-archive)
        endif()
    else()
        target_link_libraries(${APP_NAME} PRIVATE numstore)
    endif()
    target_include_directories(${APP_NAME}
        PUBLIC  "${CMAKE_SOURCE_DIR}/include"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}"
    )
    install(TARGETS ${APP_NAME}
        RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
        COMPONENT runtime
    )
endforeach()

install(TARGETS numstore
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    COMPONENT development
)
install(DIRECTORY "${CMAKE_SOURCE_DIR}/include/"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
    COMPONENT   development
    FILES_MATCHING PATTERN "*.h"
)
install(
    FILES
        "${CMAKE_SOURCE_DIR}/README.md"
        "${CMAKE_SOURCE_DIR}/LICENSE.md"
    DESTINATION "${CMAKE_INSTALL_DOCDIR}"
    COMPONENT   runtime
    OPTIONAL
)
