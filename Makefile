# gnomad-lr Build Targets

.PHONY: all release worker clean

# Default: build macOS CLI and Linux worker
all: release worker

# Build macOS release binary
release:
	cargo build --release

# Build Linux worker binary (cross-compile for GCP VMs)
worker:
	@echo "Building Linux worker binary..."
	@ulimit -n 16384 2>/dev/null || ulimit -n 8192 2>/dev/null || true; \
	cargo zigbuild --target x86_64-unknown-linux-gnu --release
	@mkdir -p target/release
	@cp target/x86_64-unknown-linux-gnu/release/gnomad-lr target/release/gnomad-lr-worker
	@echo "Installed: target/release/gnomad-lr-worker"

clean:
	cargo clean
