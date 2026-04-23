# Gather base sources
file(GLOB_RECURSE NS_SOURCES  CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.c")

# The main library
add_library(smartfiles STATIC ${NS_SOURCES})
target_sources(smartfiles PRIVATE $<TARGET_OBJECTS:c_specx>)
target_include_directories(smartfiles PRIVATE $<TARGET_PROPERTY:c_specx,INTERFACE_INCLUDE_DIRECTORIES>)
target_include_directories(smartfiles
    PUBLIC  "${CMAKE_SOURCE_DIR}/include"
    PRIVATE "${CMAKE_SOURCE_DIR}/lib"
    PRIVATE "${CMAKE_SOURCE_DIR}/thirdparty/c_specx/include"
)

if(WIN32)
    target_link_libraries(smartfiles PUBLIC ws2_32)
else()
    target_link_libraries(smartfiles PUBLIC pthread)
endif()

set_target_properties(smartfiles PROPERTIES
    OUTPUT_NAME "smartfiles"
    VERSION     "${PROJECT_VERSION}"
)

install(TARGETS smartfiles
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
