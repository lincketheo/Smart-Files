# Sanitizers 
# This file is in charge of adding optional address sanitizers if asan is enabled.

if(ENABLE_ASAN)
    if(MSVC)
        add_compile_options(/fsanitize=address /Zi) 
        add_link_options(/INCREMENTAL:NO) # ASan is incompatible with incremental linking
        message(STATUS "Sanitizers: ASan (MSVC) - Ensure /MD is used")
    else()
        # Linux/macOS Flags: Use -fsanitize=address
        # -fno-omit-frame-pointer: Ensures nice, readable stack traces
        add_compile_options(
          -fsanitize=address          # Add address sanitizer
          -fno-omit-frame-pointer     # Readable stack traces
          -g                          # Keep symbols
        )
        
        # Linker must also have the flag to pull in the ASan library
        add_link_options(-fsanitize=address)

        # Add leak sanitizer for linux too
        # Apple disables it - because they use the apple command leak
        if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
            add_compile_options(-fsanitize=leak)
            add_link_options(-fsanitize=leak)
        endif()
    endif()
endif()

