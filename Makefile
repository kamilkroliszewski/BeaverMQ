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
test: $(TEST_BINS)
	@echo "=== Running tests ==="
	@fail=0; for t in $(TEST_BINS); do \
		echo "--- $$t ---"; \
		$$t || fail=1; \
	done; \
	if [ $$fail -eq 0 ]; then echo "=== All tests passed ==="; \
	else echo "=== Some tests FAILED ==="; exit 1; fi

run: $(BIN)
	@$(BIN)

debug: OPT := -O0 -g3 -fsanitize=address,undefined -fno-omit-frame-pointer
debug: LDFLAGS += -fsanitize=address,undefined
debug: clean all

# ThreadSanitizer build for race detection (understands C11 atomics + pthreads).
tsan: OPT := -O1 -g -fsanitize=thread -fno-omit-frame-pointer
tsan: LDFLAGS += -fsanitize=thread
tsan: clean all

clean:
	@echo "  CLEAN"
	@rm -rf $(BUILD_DIR)

$(OBJ_DIR) $(BUILD_DIR):
	@mkdir -p $@

# Include auto-generated header dependencies (silent if absent).
-include $(DEPS)
