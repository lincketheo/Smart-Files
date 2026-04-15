# Gather base sources
file(GLOB_RECURSE NS_APP_SOURCES  CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.c")

foreach(APP_SRC ${NS_APP_SOURCES})
    get_filename_component(APP_NAME ${APP_SRC} NAME_WE)
    add_executable(${APP_NAME} ${APP_SRC})
    target_link_libraries(${APP_NAME} PRIVATE numstore tlclib)
    target_include_directories(${APP_NAME}
        PUBLIC  "${CMAKE_SOURCE_DIR}/include"
        PUBLIC  "${CMAKE_SOURCE_DIR}/thirdparty/tlclib/include"
        PRIVATE "${CMAKE_SOURCE_DIR}/lib"
    )
    install(TARGETS ${APP_NAME}
        RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
        COMPONENT runtime
    )
endforeach()
