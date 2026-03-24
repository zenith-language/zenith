// x86-64 System V ABI fiber context switch.
//
// void fiber_switch(usize *from_sp, usize *to_sp)
//
// Saves callee-saved registers (rbx, rbp, r12-r15) and the stack pointer
// to *from_sp, then restores them from *to_sp and returns to the
// new context's return address.
//
// This follows the System V AMD64 ABI: rdi = first arg, rsi = second arg.
// Callee-saved registers: rbx, rbp, r12, r13, r14, r15.

.global _fiber_switch
.global fiber_switch

_fiber_switch:
fiber_switch:
    // Save callee-saved registers onto the current stack.
    push %rbp
    push %rbx
    push %r12
    push %r13
    push %r14
    push %r15

    // Save current stack pointer to *from_sp (rdi = first argument).
    mov %rsp, (%rdi)

    // Load new stack pointer from *to_sp (rsi = second argument).
    mov (%rsi), %rsp

    // Restore callee-saved registers from the new stack.
    pop %r15
    pop %r14
    pop %r13
    pop %r12
    pop %rbx
    pop %rbp

    // Return to the new context's return address (on top of stack).
    ret
