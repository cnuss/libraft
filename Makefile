.PHONY: all check fmt fmt-check vet build windows test e2e e2e-aws run

# The saml.to role used by the real-AWS e2e target. Override on the command
# line if your role name differs: make e2e-aws SAML_ROLE=you@example.com
SAML_ROLE ?= github+aws@cnuss.com

# The library is pure Go. Forcing CGO off keeps every build identical across
# hosts and sidesteps broken toolchains (e.g. windows-11-arm runners ship an
# x86_64 gcc that can't assemble runtime/cgo's arm64 stubs).
export CGO_ENABLED = 0

# Default: everything CI runs except the auto-bump release step.
all: fmt-check vet build windows test e2e

# Compose the common pre-push checklist. Mirrors the CI matrix.
check: fmt-check vet windows test e2e

# gofmt the tree in place.
fmt:
	gofmt -w .

# Fail if anything in the tree is not gofmt-clean.
fmt-check:
	@out=$$(gofmt -l .); \
	if [ -n "$$out" ]; then echo "gofmt found unformatted files:"; echo "$$out"; exit 1; fi

# Static analysis across every package. -unsafeptr=false disables the one
# analyzer that false-positives on v3/reflect's monkey-patcher: it converts a
# text-segment code address (uintptr) to unsafe.Pointer to overwrite a function
# prologue — never a GC-managed pointer, which is the misuse the check guards.
vet:
	go vet -unsafeptr=false ./...
	go vet -C e2e -unsafeptr=false ./...

# Build the whole module for the host platform. e2e is its own module (so the
# harness's docker SDK and etcdmain dep trees stay out of the library's graph)
# and ./... never descends into a nested module, hence the explicit -C e2e legs
# here and in vet/windows.
build:
	go build ./...
	go build -C e2e ./...

# Cross-compile + vet for Windows. A build-only smoke so a host-only library
# doesn't quietly stop building on the other major target.
windows:
	GOOS=windows go vet -unsafeptr=false ./...
	GOOS=windows go vet -C e2e -unsafeptr=false ./...
	GOOS=windows go build ./...
	GOOS=windows go build -C e2e ./...

# Library unit + fuzz tests (v1alpha1) plus the godoc examples (v1).
test:
	go test ./...

# End-to-end: the harness builds and drives every example binary. -count=1 disables
# go test caching, since the harness builds the example binaries at runtime and the
# cache key wouldn't otherwise pick up example source changes. Without AWS_REGION
# the harness starts a throwaway MinIO container for the libraft legs (skipping
# them where no linux-container docker daemon is reachable).
e2e:
	go test -C e2e -count=1 -v .

# End-to-end against REAL AWS S3 instead of a throwaway MinIO container — the
# local equivalent of CI's reflect-aws leg. Assumes $(SAML_ROLE) via saml-to
# (headless), points the libraft legs at a unique per-run prefix in the
# per-account bucket (libraft-e2e-<account>), runs the same e2e module against
# it, then purges that prefix. The harness routes to this store because
# AWS_REGION is set (see e2e/internal/harness). Real S3 is ~100ms/op, so this
# is much slower than `make e2e`.
#
# Requires: saml-to (brew install saml-to/tap/saml-to) and the aws CLI, both
# able to assume $(SAML_ROLE). Exercises the same conditional-write CAS the
# hijacked binary uses; note real S3 supports If-None-Match:* natively, so the
# client runs in conditional mode (etagChain off), unlike community MinIO.
e2e-aws:
	@command -v saml-to >/dev/null 2>&1 || { echo "e2e-aws: saml-to not found (brew install saml-to/tap/saml-to)"; exit 1; }
	@command -v aws     >/dev/null 2>&1 || { echo "e2e-aws: aws CLI not found"; exit 1; }
	@eval "$$(saml-to assume $(SAML_ROLE) --headless)"; \
	  test -n "$$AWS_ACCESS_KEY_ID" || { echo "e2e-aws: could not assume $(SAML_ROLE)"; exit 1; }; \
	  account=$$(aws sts get-caller-identity --query Account --output text); \
	  bucket="libraft-e2e-$$account"; \
	  prefix="local-$$$$-$$(git rev-parse --short HEAD 2>/dev/null || echo nogit)"; \
	  export ETCD_S3LOG_URL="https://s3.$$AWS_DEFAULT_REGION.amazonaws.com/$$bucket/$$prefix"; \
	  echo "e2e-aws store: $$ETCD_S3LOG_URL"; \
	  go test -C e2e -count=1 -v .; rc=$$?; \
	  echo "e2e-aws: purging s3://$$bucket/$$prefix"; \
	  aws s3 rm --recursive "s3://$$bucket/$$prefix" >/dev/null 2>&1 || true; \
	  exit $$rc

# Run an example by name, forwarding any trailing words as args:
#   make run basic
run:
	cd examples/$(word 2,$(MAKECMDGOALS)) && go run . $(wordlist 3,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))

# Swallow the example name and forwarded args (extra goals) so make doesn't error.
%:
	@:
