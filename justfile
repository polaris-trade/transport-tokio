set shell := ["bash", "-uc"]
set dotenv-load := true

# Default target = list all recipes.
default:
    @just --list --unsorted

# ---------------------------------------------------------------------------
# Build variants — profiles defined in .cargo/config.toml
# ---------------------------------------------------------------------------

# Debug build (default cargo profile).
build:
    cargo build --workspace

# Optimised release build.
release:
    cargo build --workspace --release

# Fully optimised, stripped, LTO'd. Slow to compile. Ship this.
production:
    cargo build --workspace --profile production

# Same as production but keeps debug info. For prod incidents you need to attach a debugger to.
production-debug:
    cargo build --workspace --profile production-with-debug

# CI-tuned dev build. Faster than plain `dev`, still un-optimised.
ci-build:
    cargo build --workspace --profile ci

# Debug + optimisation. Loads .cargo/profiling.toml so the strip/dead-strip
# linker flags in the base config are replaced with frame-pointer preserving
# ones — samply/perf/instruments get usable symbols.
profile:
    cargo --config @.cargo/profiling.toml build --workspace --profile profiling

# Same as `profile` but with codegen-units=1 for sharper hotspot names.
profile-sharp:
    cargo --config @.cargo/profiling.toml build --workspace --profile profiling-sharp

# Production + line-table debug info. For flamegraphs. Loads flamegraph.toml
# so link-time symbol stripping is skipped.
flamegraph-build:
    cargo --config @.cargo/flamegraph.toml build --workspace --profile flamegraph

# ---------------------------------------------------------------------------
# Test / lint
# ---------------------------------------------------------------------------

# All tests via cargo-nextest.
test:
    cargo nextest run --workspace --no-fail-fast

# Integration tests only (behind #[ignore]).
test-ignored:
    cargo nextest run --workspace --no-fail-fast --run-ignored ignored-only

# Doctests (nextest doesn't run these).
doctest:
    cargo test --workspace --doc

# Full test suite: unit + doc + integration.
test-all: test doctest test-ignored

# Clippy with warnings-as-errors, all targets.
clippy:
    cargo clippy --workspace --all-targets -- -D warnings

# rustfmt check.
fmt-check:
    cargo fmt --all --check

# rustfmt apply.
fmt:
    cargo fmt --all

# Pre-commit gate: fmt + clippy + test.
check: fmt-check clippy test

# ---------------------------------------------------------------------------
# Profiling helpers
# ---------------------------------------------------------------------------

# Run a binary under samply (macOS/Linux). Usage: just samply <bin-name>
samply BIN:
    cargo --config @.cargo/profiling.toml build --profile profiling --bin 
    samply record ./target/profiling/

# Generate a flamegraph via cargo-flamegraph. Usage: just flame <bin-name>
flame BIN:
    cargo --config @.cargo/flamegraph.toml flamegraph --profile flamegraph --bin 

# ---------------------------------------------------------------------------
# Release / changelog (release-plz)
# ---------------------------------------------------------------------------

# Preview what release-plz would do without touching git or crates.io.
release-preview:
    release-plz update --dry-run

# Apply local version + changelog bumps (no push, no publish).
release-local:
    release-plz update

# ---------------------------------------------------------------------------
# Housekeeping
# ---------------------------------------------------------------------------

# Wipe target/ and lat caches.
clean:
    cargo clean
    rm -rf .lat .release-plz

# Validate lat.md links + code refs.
lat-check:
    lat check

# Install local git hooks (lefthook.yml). Run once per clone.
hooks:
    lefthook install

# Update rust-toolchain.toml to a new MSRV. Usage: just msrv 1.98.0
msrv VERSION:
    sed -i.bak -E 's/^(\s*channel\s*=\s*)"[^"]+"/\1""/' rust-toolchain.toml
    sed -i.bak -E 's/^(\s*rust-version\s*=\s*)"[^"]+"/\1""/' Cargo.toml
    rm -f rust-toolchain.toml.bak Cargo.toml.bak
    @echo "MSRV updated to . Commit the change and CI will re-resolve the matrix."
