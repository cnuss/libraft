// This file installs s3raft as a drop-in replacement for the raft
// algorithm WITHOUT modifying any etcd source: the only change to the tree
// is a blank import of this package. When ETCD_S3LOG_URL is set, init()
// rewrites the machine-code prologue of raft.StartNode and raft.RestartNode
// with an unconditional jump to our replacements.
//
// Why not a cleaner seam? etcd calls raft.StartNode/RestartNode directly
// from an unexported method (bootstrappedRaft.newRaftNode); with the source
// frozen there is no function variable, interface, or build tag to hook.
// Pure reflection cannot rewrite a compiled function body, so the honest
// mechanism is a classic monkey patch: reflect supplies the code addresses,
// unsafe + mprotect performs the overwrite. We patch the EXPORTED raft
// entry points (not the unexported etcd method) so the replacements are
// expressible with exported types alone and reconstruct everything they
// need — member id, peer set, MemoryStorage — from *raft.Config.
//
// This is a proof-of-concept technique. It is architecture-specific
// (amd64, arm64), depends on the process being able to mprotect its own
// text pages (true for standard `go build` binaries; may fail under a
// hardened runtime), and permanently redirects the raft entry points for
// the lifetime of the process.

package reflect

import (
	"fmt"
	"os"
	"reflect"
	"runtime"
	"unsafe"

	v3 "github.com/cnuss/libraft/v3"
	"go.uber.org/zap"

	serverstorage "go.etcd.io/etcd/server/v3/storage"
	"go.etcd.io/raft/v3"
)

func init() {
	rawurl := os.Getenv(v3.EnvURL)
	if rawurl == "" {
		return
	}
	lg := v3.Logger()
	defer func() {
		if r := recover(); r != nil {
			lg.Fatal("s3raft: failed to install raft hijack",
				zap.Any("panic", r),
				zap.String("arch", runtime.GOARCH))
		}
	}()

	// Redirect the raft constructors. StartNode(cfg, peers) is the
	// fresh-cluster path; RestartNode(cfg) is the recover-from-WAL path.
	patchFunc(reflect.ValueOf(raft.StartNode).Pointer(), reflect.ValueOf(s3StartNode).Pointer())
	patchFunc(reflect.ValueOf(raft.RestartNode).Pointer(), reflect.ValueOf(s3RestartNode).Pointer())

	// Redirect the backend opener so we can capture the bbolt backend for
	// checkpointing and restore it from the bucket on a disk-wiped start.
	patchFunc(reflect.ValueOf(serverstorage.OpenBackend).Pointer(), reflect.ValueOf(v3.S3OpenBackend).Pointer())

	lg.Info("s3raft: hijacked raft.StartNode/RestartNode and OpenBackend",
		zap.String("url", rawurl),
		zap.String("arch", runtime.GOARCH))
}

// s3StartNode replaces raft.StartNode for the fresh-cluster path. Signature
// MUST match raft.StartNode exactly so the jumped-to ABI lines up.
func s3StartNode(c *raft.Config, peers []raft.Peer) raft.Node {
	return s3Node(c, peers)
}

// s3RestartNode replaces raft.RestartNode for the WAL-recovery path. There
// are no peers here; the member set is recovered from the ConfState that
// bootstrap seeded into the MemoryStorage.
func s3RestartNode(c *raft.Config) raft.Node {
	return s3Node(c, nil)
}

// s3Node is the shared body: hand the bucket namespace the OpenBackend hijack
// derived (activeNS) off to start(). OpenBackend and StartNode/RestartNode are
// patched together, so OpenBackend always runs first and sets activeNS; an
// empty activeNS (only when the backend was not hijacked, e.g. a unit test
// exercising the node directly) means the bucket/prefix root is the log.
func s3Node(c *raft.Config, peers []raft.Peer) raft.Node {
	ms, ok := c.Storage.(*raft.MemoryStorage)
	if !ok {
		panic(fmt.Sprintf("s3raft: expected *raft.MemoryStorage, got %T", c.Storage))
	}
	n, err := v3.Start(v3.Logger(), os.Getenv(v3.EnvURL), c.ID, v3.ActiveNS, peers, ms)
	if err != nil {
		panic(fmt.Sprintf("s3raft: start: %v", err))
	}
	return n
}

// patchFunc overwrites the prologue of the function at `from` with an
// unconditional jump to `to`. Both must be Go functions with identical
// signatures; the original is never called again.
func patchFunc(from, to uintptr) {
	jmp := jumpTo(to)
	writeText(from, jmp)
}

// jumpTo returns machine code that transfers control to addr, preserving
// all argument registers/stack (a raw branch, no prologue).
func jumpTo(addr uintptr) []byte {
	switch runtime.GOARCH {
	case "amd64":
		// MOVABS RAX, addr ; JMP RAX
		b := []byte{
			0x48, 0xB8, 0, 0, 0, 0, 0, 0, 0, 0, // mov rax, imm64
			0xFF, 0xE0, // jmp rax
		}
		putUint64(b[2:10], uint64(addr))
		return b
	case "arm64":
		// LDR X17, #8 ; BR X17 ; .quad addr
		b := []byte{
			0x51, 0x00, 0x00, 0x58, // ldr x17, #8
			0x20, 0x02, 0x1F, 0xD6, // br x17
			0, 0, 0, 0, 0, 0, 0, 0, // .quad addr
		}
		putUint64(b[8:16], uint64(addr))
		return b
	default:
		panic("s3raft: unsupported GOARCH " + runtime.GOARCH)
	}
}

func putUint64(b []byte, v uint64) {
	for i := 0; i < 8; i++ {
		b[i] = byte(v >> (8 * i))
	}
}

// writeText copies code into executable memory at addr, temporarily making
// the page(s) writable, then flushing the instruction cache for the patched
// range. The setWritable / setExecutable / flushICache primitives are
// platform-specific (see hijack_darwin.go and hijack_other.go): W^X targets
// like Apple Silicon forbid a writable+executable mapping and refuse a
// plain mprotect on signed text, so darwin uses mach_vm_protect with
// VM_PROT_COPY to break copy-on-write into a private writable page.
func writeText(addr uintptr, code []byte) {
	pageSize := uintptr(pageSize())
	start := addr &^ (pageSize - 1)
	end := addr + uintptr(len(code))
	span := ((end - start) + pageSize - 1) &^ (pageSize - 1)

	if err := setWritable(start, span); err != nil {
		panic(fmt.Sprintf("s3raft: make text writable failed: %v", err))
	}
	dst := unsafe.Slice((*byte)(unsafe.Pointer(addr)), len(code))
	copy(dst, code)
	flushICache(addr, len(code))
	if err := setExecutable(start, span); err != nil {
		panic(fmt.Sprintf("s3raft: restore text executable failed: %v", err))
	}
}
