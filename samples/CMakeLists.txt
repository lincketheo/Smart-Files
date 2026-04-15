# All the source files
file(GLOB_RECURSE SAMPLES_SRCS CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/rope/*.c")

foreach(APP_SRC ${SAMPLES_SRCS})
    get_filename_component(APP_NAME ${APP_SRC} NAME_WE)
    string(REGEX REPLACE "_app$" "" APP_NAME ${APP_NAME})

    add_executable(${APP_NAME} ${APP_SRC})

    if(APP_NAME STREQUAL "test")
        # Link numstore as a proper target so CMake tracks the dependency
        # and re-links test whenever numstore changes.
        # Force-load ensures all object files (including constructor-registered
        # test functions) are pulled in even if not directly referenced.
        target_link_libraries(${APP_NAME} PRIVATE numstore pthread)
        if(APPLE)
            target_link_options(${APP_NAME} PRIVATE
                -Wl,-force_load,$<TARGET_FILE:numstore>)
        else()
            target_link_options(${APP_NAME} PRIVATE
                -Wl,--whole-archive
                $<TARGET_FILE:numstore>
                -Wl,--no-whole-archive)
        endif()
    else()
        target_link_libraries(${APP_NAME} PRIVATE numstore)
    endif()

    target_include_directories(${APP_NAME} PUBLIC  "${CMAKE_SOURCE_DIR}/include")
    install(TARGETS ${APP_NAME}
        RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
        COMPONENT runtime
    )
endforeach()
