SHELL := /bin/bash
GO ?= go

.PHONY: test lint

## test: every unit suite (no cluster); the e2e suite needs one and is skipped here
test:
	$(GO) tool ginkgo --no-color -r --skip-package=e2e --label-filter='!toolchain'

## lint: go vet, gofmt, and the toolchain-labelled specs (compiled-code checks)
lint:
	$(GO) vet ./...
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi
	$(GO) tool ginkgo --no-color --fail-on-empty --label-filter=toolchain ./pkg/verdict
