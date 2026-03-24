// ARM64 AAPCS64 fiber context switch.
//
// void fiber_switch(usize *from_sp, usize *to_sp)
//
// Saves callee-saved registers (x19-x29, x30/lr, d8-d15) and the
// stack pointer to *from_sp, then restores them from *to_sp and
// returns to the new context's link register address.
//
// AAPCS64: x0 = first arg, x1 = second arg.
// Callee-saved general: x19-x28, x29 (frame pointer), x30 (link register).
// Callee-saved SIMD: d8-d15.

.global _fiber_switch
.global fiber_switch

.align 4
_fiber_switch:
fiber_switch:
    // Save callee-saved general and SIMD registers (10 pairs = 160 bytes).
    stp x19, x20, [sp, #-16]!
    stp x21, x22, [sp, #-16]!
    stp x23, x24, [sp, #-16]!
    stp x25, x26, [sp, #-16]!
    stp x27, x28, [sp, #-16]!
    stp x29, x30, [sp, #-16]!
    stp d8,  d9,  [sp, #-16]!
    stp d10, d11, [sp, #-16]!
    stp d12, d13, [sp, #-16]!
    stp d14, d15, [sp, #-16]!

    // Save current stack pointer to *from_sp (x0 = first argument).
    mov x2, sp
    str x2, [x0]

    // Load new stack pointer from *to_sp (x1 = second argument).
    ldr x2, [x1]
    mov sp, x2

    // Restore SIMD and general registers (reverse order).
    ldp d14, d15, [sp], #16
    ldp d12, d13, [sp], #16
    ldp d10, d11, [sp], #16
    ldp d8,  d9,  [sp], #16
    ldp x29, x30, [sp], #16
    ldp x27, x28, [sp], #16
    ldp x25, x26, [sp], #16
    ldp x23, x24, [sp], #16
    ldp x21, x22, [sp], #16
    ldp x19, x20, [sp], #16

    // Return to new context (x30/lr was restored above).
    ret
