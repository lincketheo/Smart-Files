# Include standard runtime libraries (like msvcp140.dll on Windows) in the package
include(InstallRequiredSystemLibraries)

# Common Information
set(CPACK_PACKAGE_NAME                  "tlclib")
set(CPACK_PACKAGE_VENDOR                "TLCLib Project")
set(CPACK_PACKAGE_CONTACT               "lincketheo@gmail.com")
set(CPACK_PACKAGE_HOMEPAGE_URL          "https://github.com/lincketheo/tlclib")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY   "Theo Lincke's standard library")

set(CPACK_PACKAGE_VERSION               "${PROJECT_VERSION}")

# Legal documentation
set(CPACK_RESOURCE_FILE_LICENSE         "${CMAKE_SOURCE_DIR}/LICENSE.md")
set(CPACK_RESOURCE_FILE_README          "${CMAKE_SOURCE_DIR}/README.md")

# Remove debug symbols from installed binaries
set(CPACK_STRIP_FILES                   TRUE)  

# Set default install path
set(CPACK_PACKAGING_INSTALL_PREFIX      "${CMAKE_INSTALL_PREFIX}") 

# Debian (.deb) 
set(CPACK_DEBIAN_PACKAGE_MAINTAINER     "Theo Lincke <lincketheo@gmail.com>")
set(CPACK_DEBIAN_PACKAGE_SECTION        "c")
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS      ON) 
set(CPACK_DEBIAN_FILE_NAME              "DEB-DEFAULT")

# RPM (.rpm) 
set(CPACK_RPM_PACKAGE_LICENSE           "Apache 2.0")
set(CPACK_RPM_PACKAGE_GROUP             "Languages/C")
set(CPACK_RPM_PACKAGE_AUTOREQ           YES)    
set(CPACK_RPM_FILE_NAME                 "RPM-DEFAULT")

# Generator Selection 
if(NOT CPACK_GENERATOR)
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        # Generate Debian, RedHat, and compressed tarball packages
        set(CPACK_GENERATOR "DEB;RPM;TGZ")
    elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
        # Generate macOS compressed tarball
        set(CPACK_GENERATOR "TGZ")
    elseif(CMAKE_SYSTEM_NAME STREQUAL "Windows")
        # Generate Windows ZIP (Consider "NSIS" if you want a .exe installer)
        set(CPACK_GENERATOR "ZIP")
    else()
        set(CPACK_GENERATOR "TGZ")
    endif()
endif()

# Must be included LAST to apply all the 'set' commands above
include(CPack)
