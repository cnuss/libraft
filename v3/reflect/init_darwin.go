//go:build darwin

// Darwin text-patching primitives.
//
// A plain mprotect cannot add write permission to the binary's __TEXT
// segment on macOS: it is mapped with max_protection r-x, and on Apple
// Silicon that is strictly enforced, so mprotect returns EACCES. The way
// through is mach_vm_protect with VM_PROT_COPY, which forces a
// copy-on-write break of the signed page into a private, writable page —
// after which the patch persists at the same virtual address.
//
// mach_vm_protect, task_self_trap and sys_icache_invalidate live in
// libSystem, not the BSD syscall table, so they are reached the same way
// the standard library reaches darwin libc: //go:cgo_import_dynamic pulls
// each symbol in, a NOSPLIT assembly trampoline (hijack_darwin_*.s) jumps
// to it, and runtime.syscall_syscall6 invokes it on the system stack. This
// needs no cgo and no module dependency.

package reflect

import (
	"fmt"
	"syscall"
	"unsafe"
)

// Mach vm_prot_t flags.
const (
	vmProtRead    = 0x1
	vmProtWrite   = 0x2
	vmProtExecute = 0x4
	vmProtCopy    = 0x10 // force copy-on-write, bypassing signed max_protection
)

func pageSize() int { return syscall.Getpagesize() }

func setWritable(addr, length uintptr) error {
	task := taskSelf()
	if kr := machVMProtect(task, uint64(addr), uint64(length), 0, vmProtRead|vmProtWrite|vmProtCopy); kr != 0 {
		return fmt.Errorf("mach_vm_protect RW|COPY: kern_return_t=%d", kr)
	}
	return nil
}

func setExecutable(addr, length uintptr) error {
	task := taskSelf()
	if kr := machVMProtect(task, uint64(addr), uint64(length), 0, vmProtRead|vmProtExecute); kr != 0 {
		return fmt.Errorf("mach_vm_protect RX: kern_return_t=%d", kr)
	}
	return nil
}

func flushICache(addr uintptr, length int) {
	syscall6(funcPC(sysIcacheInvalidateTrampoline), addr, uintptr(length), 0, 0, 0, 0)
}

func taskSelf() uint32 {
	r1, _, _ := syscall6(funcPC(taskSelfTrapTrampoline), 0, 0, 0, 0, 0, 0)
	return uint32(r1)
}

func machVMProtect(task uint32, addr, size uint64, setMax, prot int32) int32 {
	r1, _, _ := syscall6(funcPC(machVMProtectTrampoline),
		uintptr(task), uintptr(addr), uintptr(size), uintptr(setMax), uintptr(prot), 0)
	return int32(r1)
}

// funcPC extracts the entry PC from a (top-level) func value.
func funcPC(f func()) uintptr {
	return **(**uintptr)(unsafe.Pointer(&f))
}

// syscall6 invokes a libc function pointer on the system stack. It aliases
// the runtime's darwin libc-call entry point.
//
//go:linkname syscall6 syscall.syscall6
func syscall6(fn, a1, a2, a3, a4, a5, a6 uintptr) (r1, r2, err uintptr)

// Assembly trampolines defined in hijack_darwin_<arch>.s. Each jumps to the
// dynamically imported libSystem symbol declared below.
func machVMProtectTrampoline()
func taskSelfTrapTrampoline()
func sysIcacheInvalidateTrampoline()

//go:cgo_import_dynamic libc_mach_vm_protect mach_vm_protect "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_task_self_trap task_self_trap "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_sys_icache_invalidate sys_icache_invalidate "/usr/lib/libSystem.B.dylib"
