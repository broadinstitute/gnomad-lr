# gnomad-lr build and development targets

CARGO_FEATURES ?= clickhouse
CLICKHOUSE_URL ?= http://127.0.0.1:8123
GENOHYPE_GIT := https://github.com/broadinstitute/genohype.git
GENOHYPE_REV := 39b2f2a99f191acb49cf18d9a96a26fb62cd29d4
GIT_SHA := $(shell git rev-parse --verify HEAD)
GIT_DIRTY_SUFFIX := $(shell test -z "$$(git status --porcelain --untracked-files=no)" || printf '%s' '-dirty')
SOURCE_BUILD_ID := $(GIT_SHA)$(GIT_DIRTY_SUFFIX)

.PHONY: all release worker test verify install-genohype clickhouse-up clickhouse-down \
	clickhouse-reset init smoke clean

# Default: build the host CLI and Linux worker with ClickHouse enabled.
all: release worker

release:
	GNOMAD_LR_GIT_SHA="$(GIT_SHA)" \
	GNOMAD_LR_BUILD_IDENTITY="gnomad-lr/$(SOURCE_BUILD_ID)/host-release/features-$(CARGO_FEATURES)" \
	cargo build --locked --release --features $(CARGO_FEATURES)

# Cross-compile the custom worker used by genohype pools on GCP.
worker:
	@echo "Building Linux worker binary with features: $(CARGO_FEATURES)"
	@ulimit -n 16384 2>/dev/null || ulimit -n 8192 2>/dev/null || true; \
	GNOMAD_LR_GIT_SHA="$(GIT_SHA)" \
	GNOMAD_LR_BUILD_IDENTITY="gnomad-lr/$(SOURCE_BUILD_ID)/x86_64-linux-release/features-$(CARGO_FEATURES)" \
	cargo zigbuild --locked --target x86_64-unknown-linux-gnu --release --features $(CARGO_FEATURES)
	@mkdir -p target/release
	@cp target/x86_64-unknown-linux-gnu/release/gnomad-lr target/release/gnomad-lr-worker
	@echo "Installed: target/release/gnomad-lr-worker"

test:
	GNOMAD_LR_GIT_SHA="$(GIT_SHA)" \
	GNOMAD_LR_BUILD_IDENTITY="gnomad-lr/$(SOURCE_BUILD_ID)/host-test/features-$(CARGO_FEATURES)" \
	cargo test --locked --features $(CARGO_FEATURES)

verify:
	./scripts/verify.sh

# Install the pool/orchestration CLI at the same revision as the Rust libraries.
install-genohype:
	cargo install --locked --force --git $(GENOHYPE_GIT) --rev $(GENOHYPE_REV) \
		--features clickhouse genohype-cli

clickhouse-up:
	./scripts/clickhouse-local.sh up

clickhouse-down:
	./scripts/clickhouse-local.sh down

clickhouse-reset:
	./scripts/clickhouse-local.sh reset

init:
	cargo run --locked --features $(CARGO_FEATURES) -- init --clickhouse-url "$(CLICKHOUSE_URL)"

# Runs the bounded legacy-v1 plumbing profile; this is not Y1 acceptance.
smoke: clickhouse-up
	python3 scripts/smoke.py --clickhouse-url "$(CLICKHOUSE_URL)"

clean:
	cargo clean
