# gnomad-lr build targets

CARGO_FEATURES ?= clickhouse
GENOHYPE_GIT := https://github.com/broadinstitute/genohype.git
GENOHYPE_REV := 15ea8c387d53b150449cf109ab0005a7d8d655ca

.PHONY: all release worker test install-genohype clean

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

# Install the pool/orchestration CLI at the same revision as the Rust libraries.
install-genohype:
	cargo install --locked --force --git $(GENOHYPE_GIT) --rev $(GENOHYPE_REV) \
		--features clickhouse genohype-cli

clean:
	cargo clean
