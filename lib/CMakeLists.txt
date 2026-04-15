# Gather base sources
file(GLOB_RECURSE NS_ALL_SOURCES  CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.c")
file(GLOB_RECURSE NS_APP_SOURCES  CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/apps/*.c")

# Start with all sources, strip apps and both platform dirs
set(NS_SOURCES ${NS_ALL_SOURCES})
list(REMOVE_ITEM NS_SOURCES ${NS_APP_SOURCES})      

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
    target_link_libraries(numstore PUBLIC ws2_32)
else()
    target_link_libraries(numstore PUBLIC pthread)
endif()

set_target_properties(numstore PROPERTIES
    OUTPUT_NAME "numstore"
    VERSION     "${PROJECT_VERSION}"
)

foreach(APP_SRC ${NS_APP_SOURCES})
    get_filename_component(APP_NAME ${APP_SRC} NAME_WE)
    add_executable(${APP_NAME} ${APP_SRC})
    target_link_libraries(${APP_NAME} PRIVATE numstore)
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
