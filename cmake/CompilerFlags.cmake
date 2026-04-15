if(MSVC)
    add_compile_options(
      /W3                       # Include all warnings
      /wd4100                   # Unreferenced formal parameter
      /wd4101                   # Unused local variable
      /wd4244                   # Convert - possible loss of data
      /wd4267                   # Convert from size_t to type
      /experimental:c11atomics  # Support C11 atomic operations
    )

    # For debug build:
    #   - Disable optimizations (equivalent to -O0)
    #   - Create Symbols (equivalent to -g)
    add_compile_options("$<$<CONFIG:Debug>:/Od;/Zi>")
else()
    add_compile_options(
        -Wall                         # Enable standard warnings
        -Wextra                       # Enable even more warnings
        -Werror                       # Treat all warnings as errors
        -Wshadow                      # Warn when a variable shadows another
        -Wsign-compare                # Warn on signed/unsigned comparisons
        -Wstrict-prototypes           # Force function prototypes in C
        -Wmissing-prototypes          # Warn if global function lacks prototype
        -Wmissing-declarations        # Warn if global function lacks declaration
        -pedantic-errors              # Strict ISO C/C++ compliance
        -Wno-unused-parameter         # Silence "unused parameter" warnings
        -Wno-unused-variable          # Silence "unused variable" warnings
        -Wno-unused-but-set-variable # Silence "assigned but never used" warnings
    )

    if(CMAKE_C_COMPILER_ID MATCHES "Clang")
        add_compile_options(
          -Wno-gnu-zero-variadic-macro-arguments # I use __VA_ARGS__ a lot in TEST and LOG - I might be able to refactor this but for now disable it
          -Wno-static-in-inline                  # Silence Clang warnings on static inline functions
        )
    endif()

    # For debug build:
    #   - Disable Optimizations
    #   - Create symbols
    add_compile_options("$<$<CONFIG:Debug>:-O0;-g>") 

    if(ENABLE_PORTABLE)
        # Tune output to baseline 64 bit or armv8 
        # standard and don't use newer features 
        # like AVX-512
        if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|AMD64)$")
            add_compile_options(-march=x86-64 -mtune=generic) 
        elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)$")
            add_compile_options(-march=armv8-a -mtune=generic) 
        endif()
    else()
        # You can tune this build for this specific machine
        add_compile_options(-march=native -mtune=native) 
    endif()
endif()

# Enable profiling
if(ENABLE_GPROF)
  if(MSVC)
    add_link_options(/PROFILE)
  else()
    add_compile_options(-pg)    
    add_link_options(-pg)      
  endif()
endif()

# Strip symbols
if(ENABLE_STRIP)
    set(CMAKE_INSTALL_DO_STRIP TRUE) 
endif()
