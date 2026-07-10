// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !darwin && !windows

// Non-darwin, non-windows text-patching primitives. On Linux and friends the
// binary's text pages can be made writable with a plain mprotect (drop EXEC
// while writing, restore afterward to honor W^X), and x86 keeps the
// instruction and data caches coherent so no explicit flush is required.
// Windows uses VirtualProtect instead (see init_windows.go).

package reflect

import (
	"syscall"
	"unsafe"
)

func pageSize() int { return syscall.Getpagesize() }

func setWritable(addr, length uintptr) error {
	return syscall.Mprotect(regionSlice(addr, length), syscall.PROT_READ|syscall.PROT_WRITE)
}

func setExecutable(addr, length uintptr) error {
	return syscall.Mprotect(regionSlice(addr, length), syscall.PROT_READ|syscall.PROT_EXEC)
}

func flushICache(addr uintptr, length int) {}

func regionSlice(addr, length uintptr) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(addr)), length)
}
