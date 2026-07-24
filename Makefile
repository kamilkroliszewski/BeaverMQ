# ============================================================================
# BeaverMQ - Makefile
#
#   make            Build everything (binary once src/main.c exists, else the
#                   library objects + test binaries).
#   make test       Build and run every test in tests/.
#   make run        Build and run the broker.
#   make clean      Remove all build artifacts.
#   make debug      Build with sanitizers (ASan + UBSan) and -O0.
#
# Source files are auto-discovered, so adding a new src/*.c needs no Makefile
# edits. src/main.c (the entry point) is linked only into the broker binary,
# letting test binaries link every other object without symbol clashes.
# ============================================================================

CC        ?= cc

CSTD      := -std=c11
WARN      := -Wall -Wextra -Wpedantic -Wshadow -Wstrict-prototypes \
             -Wmissing-prototypes -Wno-unused-parameter
OPT       ?= -O2 -g
DEFS      := -D_GNU_SOURCE
INCLUDES  := -Iinclude

CFLAGS    := $(CSTD) $(WARN) $(OPT) $(DEFS) $(INCLUDES) -pthread -MMD -MP
LDFLAGS   := -pthread
LDLIBS    := -luv -ljansson

SRC_DIR   := src
TEST_DIR  := tests
BUILD_DIR := build
OBJ_DIR   := $(BUILD_DIR)/obj

# --- Source / object discovery ---------------------------------------------
SRCS      := $(wildcard $(SRC_DIR)/*.c)
MAIN_SRC  := $(wildcard $(SRC_DIR)/main.c)
LIB_SRCS  := $(filter-out $(MAIN_SRC),$(SRCS))
LIB_OBJS  := $(patsubst $(SRC_DIR)/%.c,$(OBJ_DIR)/%.o,$(LIB_SRCS))
MAIN_OBJ  := $(patsubst $(SRC_DIR)/%.c,$(OBJ_DIR)/%.o,$(MAIN_SRC))

BIN       := $(BUILD_DIR)/beavermq

TEST_SRCS := $(wildcard $(TEST_DIR)/*.c)
TEST_BINS := $(patsubst $(TEST_DIR)/%.c,$(BUILD_DIR)/%,$(TEST_SRCS))

# All generated dependency files (from -MMD).
DEPS      := $(LIB_OBJS:.o=.d) $(MAIN_OBJ:.o=.d)

.PHONY: all test run clean debug
.DEFAULT_GOAL := all

# When src/main.c exists, the default build produces the broker binary.
# Until then (early phases), it builds the library objects and test binaries.
ifeq ($(MAIN_SRC),)
all: $(LIB_OBJS) $(TEST_BINS)
	@echo "  (src/main.c not present yet - built library objects + tests)"
else
all: $(BIN)
endif

# --- Build stamp -------------------------------------------------------------
# Regenerated at every link so `beavermq --version`, the startup log and
# /api/healthz identify EXACTLY which build a process is running - a stale
# binary/process after a deploy is then immediately visible.
VERSION_SRC := $(BUILD_DIR)/version.c
VERSION_OBJ := $(OBJ_DIR)/version.o

.PHONY: version_src
version_src:
$(VERSION_OBJ): version_src | $(OBJ_DIR)
	@printf 'const char beaver_build_stamp[] = "%s";\n' \
		"$$(date '+%Y-%m-%d %H:%M:%S')" > $(VERSION_SRC)
	@$(CC) $(CFLAGS) -c $(VERSION_SRC) -o $@

# --- Linking ---------------------------------------------------------------
$(BIN): $(LIB_OBJS) $(MAIN_OBJ) $(VERSION_OBJ)
	@echo "  LD    $@"
	@$(CC) $^ $(LDFLAGS) $(LDLIBS) -o $@

# --- Compilation -----------------------------------------------------------
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c | $(OBJ_DIR)
	@echo "  CC    $<"
	@$(CC) $(CFLAGS) -c $< -o $@

# Each test binary links against all non-main library objects.
$(BUILD_DIR)/%: $(TEST_DIR)/%.c $(LIB_OBJS) $(VERSION_OBJ) | $(BUILD_DIR)
	@echo "  CCLD  $@"
	@$(CC) $(CFLAGS) $< $(LIB_OBJS) $(VERSION_OBJ) $(LDFLAGS) $(LDLIBS) -o $@

# --- Convenience targets ---------------------------------------------------
# Runs every compiled tests/*.c binary AND tests/test_supervisor.sh (an
# end-to-end process-level check that can't be a plain unit test). Reports
# an honest count of what actually ran, and - critically - FAILS (nonzero
# exit) if there is nothing to run: an empty $(TEST_BINS) used to make this
# target print "All tests passed" having verified precisely zero tests, the
# same silent-lie failure mode as skipping a check entirely.
test: $(TEST_BINS) $(BIN)
	@echo "=== Running tests ==="
	@if [ -z "$(strip $(TEST_BINS))" ]; then \
		echo "=== FAIL: no test binaries found in $(TEST_DIR)/*.c - nothing was verified ==="; \
		exit 1; \
	fi; \
	fail=0; ran=0; \
	for t in $(TEST_BINS); do \
		echo "--- $$t ---"; \
		if $$t; then ran=$$((ran + 1)); else fail=1; fi; \
	done; \
	if [ -x $(TEST_DIR)/test_supervisor.sh ]; then \
		echo "--- $(TEST_DIR)/test_supervisor.sh ---"; \
		if bash $(TEST_DIR)/test_supervisor.sh; then ran=$$((ran + 1)); else fail=1; fi; \
	else \
		echo "=== FAIL: $(TEST_DIR)/test_supervisor.sh is missing or not executable ==="; \
		fail=1; \
	fi; \
	if [ $$fail -eq 0 ]; then echo "=== All $$ran test(s) passed ==="; \
	else echo "=== Some tests FAILED ==="; exit 1; fi

run: $(BIN)
	@$(BIN)

# End-to-end integration tests against a live broker (AMQP via pika + the
# management HTTP API). Separate from `make test` because it needs python3
# (and pika, which it installs into a venv or SKIPs if offline).
.PHONY: integration
integration: $(BIN)
	@bash $(TEST_DIR)/integration/run.sh

debug: OPT := -O0 -g3 -fsanitize=address,undefined -fno-omit-frame-pointer
debug: LDFLAGS += -fsanitize=address,undefined
debug: clean all

# ThreadSanitizer build for race detection (understands C11 atomics + pthreads).
tsan: OPT := -O1 -g -fsanitize=thread -fno-omit-frame-pointer
tsan: LDFLAGS += -fsanitize=thread
tsan: clean all

# Build EVERY tests/*.c binary with ASan+UBSan and run them. The plain `test`
# target runs the same binaries in release; this one turns any out-of-bounds
# read / UB (e.g. in the frame fuzzer) into a hard failure. Used in CI.
.PHONY: test-asan
test-asan: OPT := -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer
test-asan: LDFLAGS += -fsanitize=address,undefined
test-asan: clean $(TEST_BINS)
	@echo "=== Running tests under ASan/UBSan ==="
	@fail=0; for t in $(TEST_BINS); do \
		echo "--- $$t (asan+ubsan) ---"; \
		ASAN_OPTIONS=detect_leaks=1 UBSAN_OPTIONS=halt_on_error=1 $$t || fail=1; \
	done; \
	if [ $$fail -eq 0 ]; then echo "=== All sanitized tests passed ==="; \
	else echo "=== Some sanitized tests FAILED ==="; exit 1; fi

# Coverage-guided fuzzing of the frame parser via libFuzzer (clang only). Builds
# a standalone fuzz binary; run it with an optional corpus dir + time budget:
#   make fuzz && ./build/fuzz_frame -max_total_time=60
.PHONY: fuzz
fuzz: | $(BUILD_DIR)
	@echo "  FUZZ  build/fuzz_frame"
	@clang -D_GNU_SOURCE -DFUZZ_LIBFUZZER $(INCLUDES) \
		-g -O1 -fsanitize=fuzzer,address,undefined \
		$(TEST_DIR)/test_fuzz_frame.c $(SRC_DIR)/frame.c -o $(BUILD_DIR)/fuzz_frame

clean:
	@echo "  CLEAN"
	@rm -rf $(BUILD_DIR)

$(OBJ_DIR) $(BUILD_DIR):
	@mkdir -p $@

# Include auto-generated header dependencies (silent if absent).
-include $(DEPS)
