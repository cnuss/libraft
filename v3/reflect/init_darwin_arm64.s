//go:build darwin && arm64

#include "textflag.h"

TEXT ·machVMProtectTrampoline(SB), NOSPLIT, $0-0
	JMP	libc_mach_vm_protect(SB)

TEXT ·taskSelfTrapTrampoline(SB), NOSPLIT, $0-0
	JMP	libc_task_self_trap(SB)

TEXT ·sysIcacheInvalidateTrampoline(SB), NOSPLIT, $0-0
	JMP	libc_sys_icache_invalidate(SB)
