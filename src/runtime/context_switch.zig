/// Platform-specific fiber context switch wrapper.
///
/// Selects the appropriate assembly implementation (x86-64 or ARM64)
/// at comptime via `@import("builtin").cpu.arch`.
///
/// The assembly `fiber_switch` saves callee-saved registers and swaps
/// stack pointers, enabling cooperative context switching between fibers.

const std = @import("std");
const builtin = @import("builtin");

// Import the assembly symbol. The linker resolves this to the
// platform-specific assembly file added via addAssemblyFile in build.zig.
extern fn fiber_switch(from_sp: *usize, to_sp: *usize) void;

/// Switch execution context from the current fiber to a target fiber.
///
/// Saves callee-saved registers and stack pointer to `from_sp`,
/// then restores them from `to_sp`. When this function returns,
/// execution continues in the target fiber's context.
///
/// Both pointers must point to the `sp` field of a PlatformContext.
pub fn switchContext(from_sp: *usize, to_sp: *usize) void {
    fiber_switch(from_sp, to_sp);
}

/// Initialize a fiber's native stack so that when `switchContext` is called
/// with it as the target, execution begins at `entry_fn(arg)`.
///
/// Sets up the initial stack frame to match what `fiber_switch` expects
/// to pop when restoring context:
///
/// x86-64: 6 registers (r15, r14, r13, r12, rbx, rbp) + return address
///   - rbx = arg pointer, r12 = entry function pointer
///   - Return address = trampoline that calls entry_fn(arg)
///
/// ARM64: 10 register pairs (160 bytes)
///   - x19 = entry function pointer, x20 = arg pointer
///   - x30 (lr) = trampoline that calls entry_fn(arg)
///
/// The trampoline extracts the function pointer and argument from the
/// callee-saved registers that fiber_switch restores, then calls the
/// entry function. The entry function must not return (the fiber
/// should yield or complete via the scheduler).
pub fn initContext(
    stack_base: [*]u8,
    stack_size: usize,
    entry_fn: *const fn (*anyopaque) callconv(.c) noreturn,
    arg: *anyopaque,
) usize {
    // Stack grows downward. Start at the top of the usable region,
    // aligned to 16 bytes (required by both x86-64 and ARM64 ABIs).
    var sp = @intFromPtr(stack_base) + stack_size;
    sp = sp & ~@as(usize, 15); // 16-byte align

    if (builtin.cpu.arch == .x86_64) {
        // x86-64 layout (grows downward):
        // [return addr] = trampoline (popped by ret in fiber_switch)
        // [rbp]         = 0
        // [rbx]         = arg
        // [r12]         = entry_fn
        // [r13]         = 0
        // [r14]         = 0
        // [r15]         = 0  <-- sp points here after setup
        sp -= @sizeOf(usize); // return address: trampoline
        writeUsize(sp, @intFromPtr(&fiberTrampolineX86));
        sp -= @sizeOf(usize); // rbp = 0
        writeUsize(sp, 0);
        sp -= @sizeOf(usize); // rbx = arg
        writeUsize(sp, @intFromPtr(arg));
        sp -= @sizeOf(usize); // r12 = entry_fn
        writeUsize(sp, @intFromPtr(entry_fn));
        sp -= @sizeOf(usize); // r13 = 0
        writeUsize(sp, 0);
        sp -= @sizeOf(usize); // r14 = 0
        writeUsize(sp, 0);
        sp -= @sizeOf(usize); // r15 = 0
        writeUsize(sp, 0);
    } else if (builtin.cpu.arch == .aarch64) {
        // ARM64 layout (grows downward, 10 pairs = 160 bytes):
        // fiber_switch pops in order:
        //   [d14,d15] [d12,d13] [d10,d11] [d8,d9]
        //   [x29,x30] [x27,x28] [x25,x26] [x23,x24] [x21,x22] [x19,x20]
        //
        // We set: x19=entry_fn, x20=arg, x30(lr)=trampoline
        // All others = 0
        //
        // Memory layout from high to low (sp starts at top):
        // pair 0 (first pushed, last popped): x19, x20
        // pair 1: x21, x22
        // pair 2: x23, x24
        // pair 3: x25, x26
        // pair 4: x27, x28
        // pair 5: x29, x30
        // pair 6: d8, d9
        // pair 7: d10, d11
        // pair 8: d12, d13
        // pair 9: d14, d15

        // Allocate 160 bytes (10 pairs of 16 bytes).
        sp -= 10 * 16;
        const base = sp;

        // Zero everything first.
        const region: [*]u8 = @ptrFromInt(base);
        @memset(region[0 .. 10 * 16], 0);

        // pair 9 (first popped = highest address): x19=entry_fn, x20=arg
        // ldp x19, x20 pops from [sp], #16 -- this is the LAST ldp, so
        // it reads from the highest address (base + 9*16).
        writeUsize(base + 9 * 16, @intFromPtr(entry_fn)); // x19
        writeUsize(base + 9 * 16 + 8, @intFromPtr(arg)); // x20

        // pair 4 (x29, x30): x29=0 (frame pointer), x30=trampoline (link register)
        // ldp x29, x30 is the 5th ldp from the bottom. It reads from base + 4*16.
        writeUsize(base + 4 * 16, 0); // x29
        writeUsize(base + 4 * 16 + 8, @intFromPtr(&fiberTrampolineArm64)); // x30
    }

    return sp;
}

/// Helper: write a usize value at the given address.
inline fn writeUsize(addr: usize, value: usize) void {
    const ptr: *usize = @ptrFromInt(addr);
    ptr.* = value;
}

/// x86-64 trampoline: called when a new fiber is first switched to.
/// fiber_switch has restored: rbx=arg, r12=entry_fn.
/// We call entry_fn(arg) using a naked function to read registers directly.
fn fiberTrampolineX86() callconv(.naked) noreturn {
    // Move r12 (entry_fn) and rbx (arg) into the calling convention registers.
    // rdi = first arg (System V), then call the entry function.
    asm volatile (
        \\mov %%rbx, %%rdi
        \\call *%%r12
        \\ud2
    );
}

/// ARM64 trampoline: called when a new fiber is first switched to.
/// fiber_switch has restored: x19=entry_fn, x20=arg.
/// We call entry_fn(arg) via blr.
fn fiberTrampolineArm64() callconv(.naked) noreturn {
    // Move x20 (arg) to x0 (first argument register), then branch to x19 (entry_fn).
    asm volatile (
        \\mov x0, x20
        \\blr x19
        \\brk #1
    );
}

// ── Tests ──────────────────────────────────────────────────────────────

test "switchContext function exists and is callable" {
    // We can't easily test actual context switching in unit tests
    // (it requires valid stacks and would switch execution flow),
    // but we verify the symbol links correctly.
    const func_ptr = &switchContext;
    try std.testing.expect(@intFromPtr(func_ptr) != 0);
}

test "initContext sets up stack for host architecture" {
    // Allocate a small stack region for testing.
    const stack_size: usize = 4096;
    var stack_memory: [stack_size]u8 align(16) = undefined;
    const base: [*]u8 = &stack_memory;

    // Dummy entry function.
    const dummy_entry = struct {
        fn entry(_: *anyopaque) callconv(.c) noreturn {
            unreachable;
        }
    }.entry;

    const entry_ptr: *const fn (*anyopaque) callconv(.c) noreturn = &dummy_entry;
    var dummy_arg: u64 = 0;
    const sp = initContext(base, stack_size, entry_ptr, @ptrCast(&dummy_arg));

    // sp should be within the stack region.
    const base_addr = @intFromPtr(base);
    try std.testing.expect(sp >= base_addr);
    try std.testing.expect(sp < base_addr + stack_size);

    // sp should be 16-byte aligned (after our setup, might not be
    // exactly 16-aligned due to register saves, but base should be).
    // The important thing is it's within bounds and set up.
    if (builtin.cpu.arch == .x86_64) {
        // 7 pushes of 8 bytes each = 56 bytes below the top.
        const expected_offset = stack_size - 7 * @sizeOf(usize);
        try std.testing.expectEqual(base_addr + expected_offset, sp);
    } else if (builtin.cpu.arch == .aarch64) {
        // 10 pairs of 16 bytes = 160 bytes below the aligned top.
        const aligned_top = (base_addr + stack_size) & ~@as(usize, 15);
        try std.testing.expectEqual(aligned_top - 160, sp);
    }
}

test "initContext places entry_fn and arg in correct positions" {
    const stack_size: usize = 4096;
    var stack_memory: [stack_size]u8 align(16) = undefined;
    const base: [*]u8 = &stack_memory;

    const dummy_entry = struct {
        fn entry(_: *anyopaque) callconv(.c) noreturn {
            unreachable;
        }
    }.entry;

    const entry_ptr: *const fn (*anyopaque) callconv(.c) noreturn = &dummy_entry;
    var dummy_arg: u64 = 42;
    const sp = initContext(base, stack_size, entry_ptr, @ptrCast(&dummy_arg));

    if (builtin.cpu.arch == .x86_64) {
        // After initContext, the stack (from sp upward) should contain:
        // [sp+0]  = r15 = 0
        // [sp+8]  = r14 = 0
        // [sp+16] = r13 = 0
        // [sp+24] = r12 = entry_fn
        // [sp+32] = rbx = arg
        // [sp+40] = rbp = 0
        // [sp+48] = return address = trampoline
        const r12_val: *const usize = @ptrFromInt(sp + 3 * @sizeOf(usize));
        const rbx_val: *const usize = @ptrFromInt(sp + 4 * @sizeOf(usize));
        try std.testing.expectEqual(@intFromPtr(entry_ptr), r12_val.*);
        try std.testing.expectEqual(@intFromPtr(&dummy_arg), rbx_val.*);
    } else if (builtin.cpu.arch == .aarch64) {
        // x19 at base + 9*16, x20 at base + 9*16 + 8.
        const x19_val: *const usize = @ptrFromInt(sp + 9 * 16);
        const x20_val: *const usize = @ptrFromInt(sp + 9 * 16 + 8);
        try std.testing.expectEqual(@intFromPtr(entry_ptr), x19_val.*);
        try std.testing.expectEqual(@intFromPtr(&dummy_arg), x20_val.*);
    }
}
