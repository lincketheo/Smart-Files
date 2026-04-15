.PHONY: all build comprehensive \
        test run-tests run-tests-no-asan \
        nstypes-tests nspager-tests numstore-tests nsusecase-tests \
        package package-deb package-rpm package-tar package-zip \
        package-python package-java package-all \
        homebrew-formula install clean format tidy

NPROC  = $(shell nproc 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo 1)
target ?= debug

all: build

######################################################## Main Build Target

build:
	cmake --preset $(target)
	cmake --build --preset $(target) -j$(NPROC)
	@if [ "$(target)" = "debug" ]; then cp build/debug/compile_commands.json .; fi

######################################################## Packaging

package:
	$(MAKE) build target=package-release
	cd build/package-release && cpack

package-deb:
	$(MAKE) build target=package-release
	cd build/package-release && cpack -G DEB

package-rpm:
	$(MAKE) build target=package-release
	cd build/package-release && cpack -G RPM

package-tar:
	$(MAKE) build target=package-release
	cd build/package-release && cpack -G TGZ

package-zip:
	$(MAKE) build target=package-release
	cd build/package-release && cpack -G ZIP

package-all: package package-python package-java
	cd build/package-release && cpack -G DEB
	cd build/package-release && cpack -G RPM
	cd build/package-release && cpack -G TGZ
	cd build/package-release && cpack -G ZIP

clean:
	rm -rf build
	rm -f compile_commands.json

format:
	./scripts/format.py
	clang-format -i $(shell find . \( -name '*.c' -o -name '*.h' \))
