//go:build windows

// Windows text-patching primitives.
//
// Windows does not enforce W^X on ordinary image text the way Apple Silicon
// does, so VirtualProtect can flip a .text page to PAGE_EXECUTE_READWRITE
// directly, we patch, then restore PAGE_EXECUTE_READ. VirtualProtect,
// GetCurrentProcess and FlushInstructionCache live in kernel32.dll and are
// reached with the standard library's LazyDLL loader — no cgo and no module
// dependency, mirroring how init_darwin.go reaches libSystem.

package reflect

import (
	"fmt"
	"syscall"
	"unsafe"
)

// Windows memory-protection constants (memoryapi.h).
const (
	pageExecuteRead      = 0x20
	pageExecuteReadWrite = 0x40
)

var (
	kernel32                  = syscall.NewLazyDLL("kernel32.dll")
	procVirtualProtect        = kernel32.NewProc("VirtualProtect")
	procGetCurrentProcess     = kernel32.NewProc("GetCurrentProcess")
	procFlushInstructionCache = kernel32.NewProc("FlushInstructionCache")
)

func pageSize() int { return syscall.Getpagesize() }

// virtualProtect changes the protection of [addr, addr+length) to prot,
// discarding the previous protection (VirtualProtect requires a non-nil
// out-param even when the caller does not read it back).
func virtualProtect(addr, length uintptr, prot uint32) error {
	var old uint32
	r, _, err := procVirtualProtect.Call(addr, length, uintptr(prot), uintptr(unsafe.Pointer(&old)))
	if r == 0 {
		return fmt.Errorf("VirtualProtect: %w", err)
	}
	return nil
}

func setWritable(addr, length uintptr) error {
	return virtualProtect(addr, length, pageExecuteReadWrite)
}

func setExecutable(addr, length uintptr) error {
	return virtualProtect(addr, length, pageExecuteRead)
}

func flushICache(addr uintptr, length int) {
	proc, _, _ := procGetCurrentProcess.Call()
	procFlushInstructionCache.Call(proc, addr, uintptr(length))
}
