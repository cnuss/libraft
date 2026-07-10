// This file installs s3raft as a drop-in replacement for the raft
// algorithm WITHOUT modifying any etcd source: the only change to the tree
// is a blank import of this package. When ETCD_S3LOG_URL is set, init()
// rewrites the machine-code prologue of two functions with an unconditional
// jump to our replacements:
//
//   - (*bootstrappedRaft).newRaftNode — the sole raft-construction site.
//     Our replacement builds the raft.Node from the S3 log (v3.Start) instead
//     of raft.StartNode/RestartNode. Because this call site also carries the
//     snapshotter, WAL, and *membership.RaftCluster, the replacement reaches
//     the etcd cluster ID (cl.ID()) that the raft.StartNode seam cannot see.
//   - serverstorage.OpenBackend — captures the bbolt backend for checkpointing
//     and restores it from the bucket on a disk-wiped start; it also resolves
//     v3.ActiveNS (the per-cluster namespace) before newRaftNode runs.
//
// Why a monkey patch? etcd constructs the raft node from an unexported method
// with the source frozen: no function variable, interface, or build tag to
// hook. Pure reflection cannot rewrite a compiled function body, so reflect
// supplies the code addresses and unsafe + mprotect performs the overwrite.
// newRaftNode is unexported, so its address is reached via //go:linkname and
// its unexported argument/return types are reconstructed as byte-identical
// layout mirrors (see experiment_newraftnode.go).
//
// This is a proof-of-concept technique. It is architecture-specific
// (amd64, arm64), depends on the process being able to mprotect its own
// text pages (true for standard `go build` binaries; may fail under a
// hardened runtime), and permanently redirects the entry points for the
// lifetime of the process.

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

	// Redirect the backend opener first so it captures the bbolt backend and
	// resolves v3.ActiveNS before the node seam fires. (init patches run in
	// source order; keep OpenBackend ahead of newRaftNode.)
	patchFunc(reflect.ValueOf(serverstorage.OpenBackend).Pointer(), reflect.ValueOf(v3.S3OpenBackend).Pointer())

	// Redirect the single raft-construction seam. v3.NewRaftNode builds the
	// raft.Node from the S3 log via v3.Start.
	patchFunc(reflect.ValueOf(etcdNewRaftNode).Pointer(), reflect.ValueOf(v3.NewRaftNode).Pointer())

	lg.Info("s3raft: hijacked (*bootstrappedRaft).newRaftNode and OpenBackend",
		zap.String("url", rawurl),
		zap.String("arch", runtime.GOARCH))
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
		// JMP [RIP+0] ; .quad addr — an absolute indirect jump through the
		// address stored immediately after the instruction. This clobbers NO
		// registers, which is essential: Go's internal ABI passes the first
		// integer arguments in RAX, RBX, RCX, RDI, ... so a `MOVABS RAX, addr;
		// JMP RAX` trampoline (the obvious form) would destroy the callee's
		// first argument (a method receiver lands in RAX) before it is read —
		// a nil-deref crash on amd64 that a scratch-register arm64 branch hides.
		b := []byte{
			0xFF, 0x25, 0x00, 0x00, 0x00, 0x00, // jmp qword ptr [rip+0]
			0, 0, 0, 0, 0, 0, 0, 0, // .quad addr
		}
		putUint64(b[6:14], uint64(addr))
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
// platform-specific (see init_darwin.go and init_other.go): W^X targets
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
