# All the source files
file(GLOB_RECURSE SAMPLES_SRCS CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/rope/*.c")

foreach(APP_SRC ${SAMPLES_SRCS})
    get_filename_component(APP_NAME ${APP_SRC} NAME_WE)
    add_executable(${APP_NAME} ${APP_SRC})
    target_link_libraries(${APP_NAME} PRIVATE numstore tlclib)
    target_include_directories(${APP_NAME} PUBLIC  
      PUBLIC "${CMAKE_SOURCE_DIR}/include"
      PUBLIC "${CMAKE_SOURCE_DIR}/thirdparty/tlclib/include"
    )
endforeach()
