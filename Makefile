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

dockcross/dockcross-%:
	@mkdir -p $(DOCKER_DIR)
	docker run --rm dockcross/$* > $@
	@chmod +x $@

ifdef platform
build: $(DOCKER_DIR)/dockcross-$(platform)
	./$< bash -c 'make build target=$(target)'
else
build:
	cmake --preset $(target)
	cmake --build --preset $(target) -j$(NPROC)
	@if [ "$(target)" = "debug" ]; then cp build/debug/compile_commands.json .; fi
endif

comprehensive:
	@python3 -c \
	  "import json; [print(p['name']) for p in json.load(open('CMakePresets.json'))['configurePresets'] if not p.get('hidden')]" \
	| xargs -I{} $(MAKE) build target={}

######################################################## Running tests

test:
	$(MAKE) build target=$(target)

run-tests: test
	cd build/$(target)/lib/ && ./test

run-tests-no-asan:
	$(MAKE) build target=$(target)
ifeq ($(shell uname),Darwin)
	@set -o pipefail; \
	leaks --atExit -- build/$(target)/lib/test pager_fill_ht 2>&1 | tee /tmp/leaks-$(target).txt; \
	grep -q "0 leaks for 0 total leaked bytes" /tmp/leaks-$(target).txt \
		|| (echo "FAIL: leaks detected in preset '$(target)'"; exit 1)
else
	valgrind \
		--tool=memcheck \
		--leak-check=full \
		--leak-resolution=high \
		--show-leak-kinds=all \
		--track-origins=yes \
		--track-fds=yes \
		--error-exitcode=1 \
		--errors-for-leak-kinds=all \
		--undef-value-errors=yes \
		--partial-loads-ok=no \
		--expensive-definedness-checks=yes \
		--show-mismatched-frees=yes \
		--show-reachable=yes \
		--num-callers=40 -- \
		build/$(target)/apps/test pgr_fill_ht
endif

comprehensive-tests:
	$(MAKE) run-tests target=debug
	$(MAKE) run-tests-no-asan target=debug-no-asan
	$(MAKE) run-tests-no-asan target=release-tests

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

package-python:
	@mkdir -p build/pynumstore
	python3 -m build --outdir build/pynumstore
	@echo "Python wheel ready in build/pynumstore/"

package-java:
	$(MAKE) build target=release
	cmake --build build/release --target jnumstore-jar
	@mkdir -p build/jnumstore
	@cp bindings/java/jnumstore/build/libs/jnumstore-*.jar build/jnumstore/
	@echo "Java jar ready in build/jnumstore/"

package-all: package package-python package-java
	cd build/package-release && cpack -G DEB
	cd build/package-release && cpack -G RPM
	cd build/package-release && cpack -G TGZ
	cd build/package-release && cpack -G ZIP

install:
	$(MAKE) build target=package-release
	cmake --install build/package-release --prefix /usr/local

######################################################## Maintenence

clean:
	rm -rf build
	find . -name '*.db' -o -name '*.wal' -o -name '*.nsdb' | xargs rm -f
	rm -f compile_commands.json

format:
	./scripts/format.py lib
	./scripts/format.py apps
	./scripts/format.py samples
	clang-format -i $(shell find lib \( -name '*.c' -o -name '*.h' \))

tidy:
	@if [ ! -f compile_commands.json ]; then \
		echo "Error: compile_commands.json not found. Running build first..."; \
		$(MAKE) build target=$(target); \
	fi
	/opt/homebrew/opt/llvm/bin/clang-tidy -p . $(shell find lib -name '*.c' -o -name '*.h') \
