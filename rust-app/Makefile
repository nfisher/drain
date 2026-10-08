CARGO ?= cargo

.PHONY: build release run check test fmt lint clean

build:
	$(CARGO) build

release:
	$(CARGO) build --release

run:
	$(CARGO) run

check:
	$(CARGO) check --all-targets

test:
	$(CARGO) test

fmt:
	$(CARGO) fmt --all -- --check

lint:
	$(CARGO) clippy --all-targets -- -D warnings

clean:
	$(CARGO) clean
