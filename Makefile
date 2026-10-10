SHELL := /bin/bash
GO ?= go

.PHONY: test lint

## test: every unit suite (no cluster); the e2e suite needs one and is skipped here
test:
	$(GO) tool ginkgo --no-color -r --skip-package=e2e

## lint: go vet and gofmt
lint:
	$(GO) vet ./...
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi
