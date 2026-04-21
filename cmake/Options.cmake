# Build options (default to debug profile values)
option(ENABLE_NDEBUG   "Disable assertions (NDEBUG)"                              OFF)
option(ENABLE_NTEST    "Exclude test code at compile time (NTEST)"                OFF)
option(ENABLE_NLOG     "Disable all logging (NLOG)"                               OFF)
option(ENABLE_GPROF    "Enable gprof profiling support"                           OFF)
option(ENABLE_PORTABLE "Portable build: no -march=native (for packages)"          OFF)
option(ENABLE_STRIP    "Strip debug symbols from installed binaries"               OFF)
option(ENABLE_ASAN     "Enable AddressSanitizer (default ON for non-release)"      ON)

# Translates ENABLE_* options into preprocessor defines.
foreach(_flag NDEBUG NTEST NLOG)
    if(ENABLE_${_flag})
        add_compile_definitions(${_flag})
    endif()
endforeach()

# Disable all the warnings for things like strncpy to strncpy_s.
# SmartFiles makes a point to make strncpy safe
if(MSVC)
    add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
endif()
