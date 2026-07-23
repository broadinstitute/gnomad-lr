# gnomad-lr build and development targets

CARGO_FEATURES ?= clickhouse
CLICKHOUSE_URL ?= http://127.0.0.1:8123
GENOHYPE_GIT := https://github.com/broadinstitute/genohype.git
GENOHYPE_REV := 15ea8c387d53b150449cf109ab0005a7d8d655ca

.PHONY: all release worker test verify install-genohype clickhouse-up clickhouse-down \
	clickhouse-reset init smoke clean

# Default: build the host CLI and Linux worker with ClickHouse enabled.
all: release worker

release:
	cargo build --locked --release --features $(CARGO_FEATURES)

# Cross-compile the custom worker used by genohype pools on GCP.
worker:
	@echo "Building Linux worker binary with features: $(CARGO_FEATURES)"
	@ulimit -n 16384 2>/dev/null || ulimit -n 8192 2>/dev/null || true; \
	cargo zigbuild --locked --target x86_64-unknown-linux-gnu --release --features $(CARGO_FEATURES)
	@mkdir -p target/release
	@cp target/x86_64-unknown-linux-gnu/release/gnomad-lr target/release/gnomad-lr-worker
	@echo "Installed: target/release/gnomad-lr-worker"

test:
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

# Starts local ClickHouse and runs all loaders against bounded source subsets.
smoke: clickhouse-up
	python3 scripts/smoke.py --clickhouse-url "$(CLICKHOUSE_URL)"

clean:
	cargo clean
