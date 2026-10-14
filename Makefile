SHELL := /bin/bash
GO ?= go
SCHEDULER ?= all
FOCUS ?=
RESULTS ?= $(CURDIR)/results
RUN_ID ?= $(shell date -u +%Y%m%dT%H%M%SZ)-$(shell git rev-parse --short HEAD 2>/dev/null)$(shell [ -z "$$(git status --porcelain --untracked-files=normal 2>/dev/null)" ] || echo -dirty)

.PHONY: test lint e2e report

## test: every unit suite (no cluster); the e2e suite needs one and is skipped here
test:
	$(GO) tool ginkgo --no-color -r --skip-package=e2e --label-filter='!toolchain && !live'

## lint: go vet, gofmt, and the verdict package's toolchain specs (no fused multiply-add)
lint:
	$(GO) vet ./...
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi
	$(GO) tool ginkgo --no-color --fail-on-empty --label-filter=toolchain ./pkg/verdict

## e2e: SCHEDULER=vanilla|kueue|kai|all, FOCUS=<ginkgo label filter> for one probe
e2e:
	bash hack/e2e.sh "$(SCHEDULER)" "$(RUN_ID)" "$(RESULTS)" "$(FOCUS)"

## report: rebuild verdicts and summary of RUN into OUT (default: in place)
report:
	$(GO) run ./cmd/scs-report $(if $(OUT),-out $(OUT)) $(RUN)
