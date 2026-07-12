.PHONY: all check fmt fmt-check vet build windows test e2e run

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

# Run an example by name, forwarding any trailing words as args:
#   make run basic
run:
	cd examples/$(word 2,$(MAKECMDGOALS)) && go run . $(wordlist 3,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))

# Swallow the example name and forwarded args (extra goals) so make doesn't error.
%:
	@:
