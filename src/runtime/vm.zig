const std = @import("std");
const Allocator = std.mem.Allocator;
const chunk_mod = @import("chunk");
const Chunk = chunk_mod.Chunk;
const OpCode = chunk_mod.OpCode;
const value_mod = @import("value");
const Value = value_mod.Value;
const obj_mod = @import("obj");
const ObjString = obj_mod.ObjString;
const ObjInt = obj_mod.ObjInt;
const ObjFunction = obj_mod.ObjFunction;
const ObjClosure = obj_mod.ObjClosure;
const ObjUpvalue = obj_mod.ObjUpvalue;
const error_mod = @import("error");
const Diagnostic = error_mod.Diagnostic;
const ErrorCode = error_mod.ErrorCode;
const Label = error_mod.Label;
const builtins_mod = @import("builtins");
const NativeFn = builtins_mod.NativeFn;

const STACK_MAX: u32 = 65536;
const FRAMES_MAX: u32 = 256;

/// Call frame for function calls.
const CallFrame = struct {
    closure: *ObjClosure,
    ip: u32,
    base_slot: u32,
};

/// Runtime error type.
pub const RuntimeError = error{
    RuntimeErr,
    StackOverflow,
    StackUnderflow,
} || Allocator.Error;

/// Stack-based virtual machine with closure-based call frames.
pub const VM = struct {
    // Legacy chunk pointer for backward compatibility with Phase 1 raw-chunk VM tests.
    chunk: ?*const Chunk,
    legacy_ip: u32,
    stack: [STACK_MAX]Value,
    stack_top: u32,
    frames: [FRAMES_MAX]CallFrame,
    frame_count: u32,
    objects: ?*obj_mod.Obj, // Head of allocated objects linked list
    open_upvalues: ?*ObjUpvalue, // Head of sorted open upvalue list
    atom_names: std.ArrayListUnmanaged([]const u8),
    allocator: Allocator,
    errors: std.ArrayListUnmanaged(Diagnostic),
    // Output writer for tests (captures print output instead of stdout).
    output_buf: ?*std.ArrayListUnmanaged(u8),

    const Self = @This();

    /// Initialize a VM to execute a raw chunk (legacy Phase 1 mode).
    pub fn init(chunk: *const Chunk, allocator: Allocator) VM {
        return .{
            .chunk = chunk,
            .legacy_ip = 0,
            .stack = undefined,
            .stack_top = 0,
            .frames = undefined,
            .frame_count = 0,
            .objects = null,
            .open_upvalues = null,
            .atom_names = .empty,
            .allocator = allocator,
            .errors = .empty,
            .output_buf = null,
        };
    }

    /// Initialize VM with an ObjClosure (Phase 2 mode).
    pub fn initWithClosure(closure: *ObjClosure, allocator: Allocator) VM {
        var vm = VM{
            .chunk = null,
            .legacy_ip = 0,
            .stack = undefined,
            .stack_top = 0,
            .frames = undefined,
            .frame_count = 0,
            .objects = null,
            .open_upvalues = null,
            .atom_names = .empty,
            .allocator = allocator,
            .errors = .empty,
            .output_buf = null,
        };
        // Push the script closure as slot 0.
        vm.stack[0] = Value.fromObj(&closure.obj);
        vm.stack_top = 1;
        // Set up the initial call frame.
        vm.frames[0] = .{
            .closure = closure,
            .ip = 0,
            .base_slot = 0,
        };
        vm.frame_count = 1;
        return vm;
    }

    /// Initialize VM and push a placeholder for slot 0 (script function).
    /// Used by the pipeline when running compiler output via raw chunk.
    pub fn initForScript(chunk: *const Chunk, allocator: Allocator) VM {
        var vm = init(chunk, allocator);
        vm.stack[0] = Value.nil;
        vm.stack_top = 1;
        return vm;
    }

    /// Free VM resources.
    pub fn deinit(self: *VM) void {
        // Free allocated objects.
        self.freeObjects();
        self.atom_names.deinit(self.allocator);
        self.errors.deinit(self.allocator);
    }

    /// Set atom name mapping (from compiler's atom table).
    pub fn setAtomNames(self: *VM, names: []const []const u8, allocator: Allocator) !void {
        for (names) |name| {
            try self.atom_names.append(allocator, name);
        }
    }

    /// Register a heap-allocated object for cleanup.
    fn trackObject(self: *VM, obj: *obj_mod.Obj) void {
        obj.next = self.objects;
        self.objects = obj;
    }

    /// Free all tracked heap objects.
    fn freeObjects(self: *VM) void {
        var obj = self.objects;
        while (obj) |o| {
            const next = o.next;
            o.destroy(self.allocator);
            obj = next;
        }
        self.objects = null;
    }

    // ── Stack operations ──────────────────────────────────────────────

    fn push(self: *Self, val: Value) RuntimeError!void {
        if (self.stack_top >= STACK_MAX) return error.StackOverflow;
        self.stack[self.stack_top] = val;
        self.stack_top += 1;
    }

    fn pop(self: *Self) RuntimeError!Value {
        if (self.stack_top == 0) return error.StackUnderflow;
        self.stack_top -= 1;
        return self.stack[self.stack_top];
    }

    fn peek(self: *const Self, distance: u32) Value {
        return self.stack[self.stack_top - 1 - distance];
    }

    // ── Reading from bytecode stream (legacy mode) ───────────────────

    fn readByteLegacy(self: *Self) u8 {
        const b = self.chunk.?.code.items[self.legacy_ip];
        self.legacy_ip += 1;
        return b;
    }

    fn readU16Legacy(self: *Self) u16 {
        const hi: u16 = self.chunk.?.code.items[self.legacy_ip];
        const lo: u16 = self.chunk.?.code.items[self.legacy_ip + 1];
        self.legacy_ip += 2;
        return (hi << 8) | lo;
    }

    fn readConstantLegacy(self: *Self) Value {
        const idx = self.readByteLegacy();
        return self.chunk.?.constants.items[idx];
    }

    fn readConstantLongLegacy(self: *Self) Value {
        const idx = self.readU16Legacy();
        return self.chunk.?.constants.items[idx];
    }

    // ── Reading from current frame's bytecode ────────────────────────

    fn currentFrame(self: *Self) *CallFrame {
        return &self.frames[self.frame_count - 1];
    }

    fn frameChunk(self: *Self) *const Chunk {
        return &self.currentFrame().closure.function.chunk;
    }

    fn readByteFrame(self: *Self) u8 {
        const frame = self.currentFrame();
        const b = frame.closure.function.chunk.code.items[frame.ip];
        frame.ip += 1;
        return b;
    }

    fn readU16Frame(self: *Self) u16 {
        const frame = self.currentFrame();
        const code = frame.closure.function.chunk.code.items;
        const hi: u16 = code[frame.ip];
        const lo: u16 = code[frame.ip + 1];
        frame.ip += 2;
        return (hi << 8) | lo;
    }

    fn readConstantFrame(self: *Self) Value {
        const idx = self.readByteFrame();
        return self.frameChunk().constants.items[idx];
    }

    fn readConstantLongFrame(self: *Self) Value {
        const idx = self.readU16Frame();
        return self.frameChunk().constants.items[idx];
    }

    // ── Main dispatch loop ────────────────────────────────────────────

    /// Execute the loaded chunk/closure. Returns the final value on the stack.
    pub fn run(self: *Self) RuntimeError!Value {
        // If we have frames (closure mode), use frame-based dispatch.
        if (self.frame_count > 0) {
            return self.runFrames();
        }
        // Legacy mode: run from raw chunk.
        return self.runLegacy();
    }

    /// Legacy dispatch loop for raw chunk execution (Phase 1 VM tests).
    fn runLegacy(self: *Self) RuntimeError!Value {
        const code = self.chunk.?.code.items;
        if (code.len == 0) return Value.nil;

        while (self.legacy_ip < code.len) {
            const opcode: OpCode = @enumFromInt(code[self.legacy_ip]);
            self.legacy_ip += 1;

            switch (opcode) {
                .op_constant => {
                    const val = self.readConstantLegacy();
                    try self.push(val);
                },
                .op_constant_long => {
                    const val = self.readConstantLongLegacy();
                    try self.push(val);
                },
                .op_nil => try self.push(Value.nil),
                .op_true => try self.push(Value.true_val),
                .op_false => try self.push(Value.false_val),

                .op_add => try self.binaryAdd(),
                .op_subtract => try self.binarySub(),
                .op_multiply => try self.binaryMul(),
                .op_divide => try self.binaryDiv(),
                .op_modulo => try self.binaryMod(),

                .op_negate => try self.execNegate(),
                .op_not => {
                    const val = try self.pop();
                    try self.push(Value.fromBool(builtins_mod.isFalsy(val)));
                },

                .op_equal => {
                    const b = try self.pop();
                    const a = try self.pop();
                    try self.push(Value.fromBool(Value.eql(a, b)));
                },
                .op_not_equal => {
                    const b = try self.pop();
                    const a = try self.pop();
                    try self.push(Value.fromBool(!Value.eql(a, b)));
                },

                .op_less => try self.binaryCompare(.lt),
                .op_greater => try self.binaryCompare(.gt),
                .op_less_equal => try self.binaryCompare(.le),
                .op_greater_equal => try self.binaryCompare(.ge),

                .op_concat => try self.stringConcat(),

                .op_pop => {
                    _ = try self.pop();
                },

                .op_get_local => {
                    const slot = self.readByteLegacy();
                    try self.push(self.stack[slot]);
                },
                .op_set_local => {
                    const slot = self.readByteLegacy();
                    self.stack[slot] = self.peek(0);
                },
                .op_get_global, .op_set_global, .op_define_global => {
                    try self.runtimeErrorLegacy(.E002, "globals not supported");
                    return error.RuntimeErr;
                },

                .op_jump => {
                    const offset = self.readU16Legacy();
                    self.legacy_ip += offset;
                },
                .op_jump_if_false => {
                    const offset = self.readU16Legacy();
                    if (builtins_mod.isFalsy(self.peek(0))) {
                        self.legacy_ip += offset;
                    }
                },
                .op_loop => {
                    const offset = self.readU16Legacy();
                    self.legacy_ip -= offset;
                },

                .op_print => {
                    const val = try self.pop();
                    try self.printValue(val);
                },

                .op_get_builtin => {
                    const idx = self.readByteLegacy();
                    try self.push(Value.fromAtom(BUILTIN_BASE + @as(u32, idx)));
                },

                .op_call => {
                    const arg_count = self.readByteLegacy();
                    try self.callValue(arg_count);
                },

                .op_return => {
                    if (self.stack_top > 0) {
                        return try self.pop();
                    }
                    return Value.nil;
                },

                .op_atom => {
                    const val = self.readConstantLegacy();
                    try self.push(val);
                },

                .op_closure, .op_get_upvalue, .op_set_upvalue, .op_close_upvalue, .op_close_upvalue_at, .op_tail_call => {
                    try self.runtimeErrorLegacy(.E001, "closure opcodes require frame mode");
                    return error.RuntimeErr;
                },

                .op_for_iter => {
                    const jump_offset = self.readU16Legacy();
                    try self.execForIter(jump_offset, &self.legacy_ip);
                },

                // Phase 3 opcodes -- not yet implemented in legacy mode.
                .op_list,
                .op_map,
                .op_tuple,
                .op_record,
                .op_record_spread,
                .op_adt_construct,
                .op_adt_get_field,
                .op_get_field,
                .op_get_index,
                .op_check_tag,
                .op_list_len,
                .op_list_slice,
                .op_dup,
                => {
                    try self.runtimeErrorLegacy(.E001, "Phase 3 opcodes not yet implemented");
                    return error.RuntimeErr;
                },
            }
        }

        return Value.nil;
    }

    /// Frame-based dispatch loop for closure execution (Phase 2).
    fn runFrames(self: *Self) RuntimeError!Value {
        while (self.frame_count > 0) {
            const frame = self.currentFrame();
            const code = frame.closure.function.chunk.code.items;

            if (frame.ip >= code.len) {
                // End of function code without explicit return.
                return Value.nil;
            }

            const opcode: OpCode = @enumFromInt(code[frame.ip]);
            frame.ip += 1;

            switch (opcode) {
                .op_constant => {
                    const val = self.readConstantFrame();
                    try self.push(val);
                },
                .op_constant_long => {
                    const val = self.readConstantLongFrame();
                    try self.push(val);
                },
                .op_nil => try self.push(Value.nil),
                .op_true => try self.push(Value.true_val),
                .op_false => try self.push(Value.false_val),

                .op_add => try self.binaryAdd(),
                .op_subtract => try self.binarySub(),
                .op_multiply => try self.binaryMul(),
                .op_divide => try self.binaryDiv(),
                .op_modulo => try self.binaryMod(),

                .op_negate => try self.execNegate(),
                .op_not => {
                    const val = try self.pop();
                    try self.push(Value.fromBool(builtins_mod.isFalsy(val)));
                },

                .op_equal => {
                    const b = try self.pop();
                    const a = try self.pop();
                    try self.push(Value.fromBool(Value.eql(a, b)));
                },
                .op_not_equal => {
                    const b = try self.pop();
                    const a = try self.pop();
                    try self.push(Value.fromBool(!Value.eql(a, b)));
                },

                .op_less => try self.binaryCompare(.lt),
                .op_greater => try self.binaryCompare(.gt),
                .op_less_equal => try self.binaryCompare(.le),
                .op_greater_equal => try self.binaryCompare(.ge),

                .op_concat => try self.stringConcat(),

                .op_pop => {
                    _ = try self.pop();
                },

                .op_get_local => {
                    const slot = self.readByteFrame();
                    try self.push(self.stack[frame.base_slot + slot]);
                },
                .op_set_local => {
                    const slot = self.readByteFrame();
                    self.stack[frame.base_slot + slot] = self.peek(0);
                },
                .op_get_global, .op_set_global, .op_define_global => {
                    try self.runtimeErrorFrame(.E002, "globals not supported");
                    return error.RuntimeErr;
                },

                .op_jump => {
                    const offset = self.readU16Frame();
                    self.currentFrame().ip += offset;
                },
                .op_jump_if_false => {
                    const offset = self.readU16Frame();
                    if (builtins_mod.isFalsy(self.peek(0))) {
                        self.currentFrame().ip += offset;
                    }
                },
                .op_loop => {
                    const offset = self.readU16Frame();
                    self.currentFrame().ip -= offset;
                },

                .op_print => {
                    const val = try self.pop();
                    try self.printValue(val);
                },

                .op_get_builtin => {
                    const idx = self.readByteFrame();
                    try self.push(Value.fromAtom(BUILTIN_BASE + @as(u32, idx)));
                },

                .op_call => {
                    const arg_count = self.readByteFrame();
                    try self.callValue(arg_count);
                },

                .op_closure => {
                    const const_idx = self.readByteFrame();
                    const func_val = self.frameChunk().constants.items[const_idx];
                    const func = ObjFunction.fromObj(func_val.asObj());
                    const closure = try ObjClosure.create(self.allocator, func);
                    self.trackObject(&closure.obj);

                    // Populate upvalues from descriptors.
                    for (0..func.upvalue_count) |i| {
                        const is_local = self.readByteFrame();
                        const index = self.readByteFrame();
                        if (is_local == 1) {
                            closure.upvalues[i] = try self.captureUpvalue(frame.base_slot + index);
                        } else {
                            closure.upvalues[i] = frame.closure.upvalues[index];
                        }
                    }

                    try self.push(Value.fromObj(&closure.obj));
                },

                .op_get_upvalue => {
                    const slot = self.readByteFrame();
                    if (frame.closure.upvalues[slot]) |uv| {
                        try self.push(uv.location.*);
                    } else {
                        try self.push(Value.nil);
                    }
                },

                .op_set_upvalue => {
                    const slot = self.readByteFrame();
                    if (frame.closure.upvalues[slot]) |uv| {
                        uv.location.* = self.peek(0);
                    }
                },

                .op_close_upvalue => {
                    self.closeUpvalues(self.stack_top - 1);
                    _ = try self.pop();
                },

                .op_close_upvalue_at => {
                    const slot = self.readByteFrame();
                    self.closeUpvalues(frame.base_slot + slot);
                },

                .op_return => {
                    const result = try self.pop();
                    // Close upvalues in the returning frame.
                    self.closeUpvalues(frame.base_slot);
                    self.frame_count -= 1;
                    if (self.frame_count == 0) {
                        // Top-level script returning.
                        return result;
                    }
                    // Discard the frame's slots.
                    self.stack_top = frame.base_slot;
                    try self.push(result);
                },

                .op_tail_call => {
                    const arg_count = self.readByteFrame();
                    try self.execTailCall(arg_count);
                },

                .op_atom => {
                    const val = self.readConstantFrame();
                    try self.push(val);
                },

                .op_for_iter => {
                    const jump_offset = self.readU16Frame();
                    try self.execForIterFrame(jump_offset);
                },

                // Phase 3 opcodes -- not yet implemented in frame mode.
                .op_list,
                .op_map,
                .op_tuple,
                .op_record,
                .op_record_spread,
                .op_adt_construct,
                .op_adt_get_field,
                .op_get_field,
                .op_get_index,
                .op_check_tag,
                .op_list_len,
                .op_list_slice,
                .op_dup,
                => {
                    try self.runtimeErrorFrame(.E001, "Phase 3 opcodes not yet implemented");
                    return error.RuntimeErr;
                },
            }
        }

        return Value.nil;
    }

    // Sentinel base for builtin function atoms.
    const BUILTIN_BASE: u32 = 0xFFFF_0000;

    // ── Negate ────────────────────────────────────────────────────────

    fn execNegate(self: *Self) RuntimeError!void {
        const val = try self.pop();
        if (val.isInt()) {
            const n = val.asInt();
            if (n == std.math.minInt(i32)) {
                try self.runtimeErrorAny(.E003, "integer overflow: cannot negate minimum integer");
                return error.RuntimeErr;
            }
            try self.push(Value.fromInt(-n));
        } else if (val.isFloat()) {
            try self.push(Value.fromFloat(-val.asFloat()));
        } else if (val.isObjType(.int_big)) {
            const big_val = ObjInt.fromObj(val.asObj()).value;
            const result = try Value.fromI64(-big_val, self.allocator);
            if (result.isObj()) self.trackObject(result.asObj());
            try self.push(result);
        } else {
            try self.runtimeErrorAny(.E001, "cannot negate non-numeric value");
            return error.RuntimeErr;
        }
    }

    // ── Arithmetic operations ─────────────────────────────────────────

    fn binaryAdd(self: *Self) RuntimeError!void {
        const b = try self.pop();
        const a = try self.pop();

        if (a.isInt() and b.isInt()) {
            const ai: i64 = a.asInt();
            const bi: i64 = b.asInt();
            const result = ai + bi;
            if (result > std.math.maxInt(i32) or result < std.math.minInt(i32)) {
                try self.runtimeErrorAny(.E003, "integer overflow");
                return error.RuntimeErr;
            }
            try self.push(Value.fromInt(@intCast(result)));
        } else if (a.isFloat() and b.isFloat()) {
            try self.push(Value.fromFloat(a.asFloat() + b.asFloat()));
        } else if (a.isInt() and b.isFloat()) {
            try self.push(Value.fromFloat(@as(f64, @floatFromInt(a.asInt())) + b.asFloat()));
        } else if (a.isFloat() and b.isInt()) {
            try self.push(Value.fromFloat(a.asFloat() + @as(f64, @floatFromInt(b.asInt()))));
        } else {
            try self.runtimeErrorAny(.E001, "operands must be numbers for '+'");
            return error.RuntimeErr;
        }
    }

    fn binarySub(self: *Self) RuntimeError!void {
        const b = try self.pop();
        const a = try self.pop();

        if (a.isInt() and b.isInt()) {
            const ai: i64 = a.asInt();
            const bi: i64 = b.asInt();
            const result = ai - bi;
            if (result > std.math.maxInt(i32) or result < std.math.minInt(i32)) {
                try self.runtimeErrorAny(.E003, "integer overflow");
                return error.RuntimeErr;
            }
            try self.push(Value.fromInt(@intCast(result)));
        } else if (a.isFloat() and b.isFloat()) {
            try self.push(Value.fromFloat(a.asFloat() - b.asFloat()));
        } else if (a.isInt() and b.isFloat()) {
            try self.push(Value.fromFloat(@as(f64, @floatFromInt(a.asInt())) - b.asFloat()));
        } else if (a.isFloat() and b.isInt()) {
            try self.push(Value.fromFloat(a.asFloat() - @as(f64, @floatFromInt(b.asInt()))));
        } else {
            try self.runtimeErrorAny(.E001, "operands must be numbers for '-'");
            return error.RuntimeErr;
        }
    }

    fn binaryMul(self: *Self) RuntimeError!void {
        const b = try self.pop();
        const a = try self.pop();

        if (a.isInt() and b.isInt()) {
            const ai: i64 = a.asInt();
            const bi: i64 = b.asInt();
            const result = ai * bi;
            if (result > std.math.maxInt(i32) or result < std.math.minInt(i32)) {
                try self.runtimeErrorAny(.E003, "integer overflow");
                return error.RuntimeErr;
            }
            try self.push(Value.fromInt(@intCast(result)));
        } else if (a.isFloat() and b.isFloat()) {
            try self.push(Value.fromFloat(a.asFloat() * b.asFloat()));
        } else if (a.isInt() and b.isFloat()) {
            try self.push(Value.fromFloat(@as(f64, @floatFromInt(a.asInt())) * b.asFloat()));
        } else if (a.isFloat() and b.isInt()) {
            try self.push(Value.fromFloat(a.asFloat() * @as(f64, @floatFromInt(b.asInt()))));
        } else {
            try self.runtimeErrorAny(.E001, "operands must be numbers for '*'");
            return error.RuntimeErr;
        }
    }

    fn binaryDiv(self: *Self) RuntimeError!void {
        const b = try self.pop();
        const a = try self.pop();

        if (a.isInt() and b.isInt()) {
            if (b.asInt() == 0) { try self.runtimeErrorAny(.E004, "division by zero"); return error.RuntimeErr; }
            try self.push(Value.fromInt(@intCast(@divTrunc(@as(i64, a.asInt()), @as(i64, b.asInt())))));
        } else if (a.isFloat() and b.isFloat()) {
            if (b.asFloat() == 0.0) { try self.runtimeErrorAny(.E004, "division by zero"); return error.RuntimeErr; }
            try self.push(Value.fromFloat(a.asFloat() / b.asFloat()));
        } else if (a.isInt() and b.isFloat()) {
            if (b.asFloat() == 0.0) { try self.runtimeErrorAny(.E004, "division by zero"); return error.RuntimeErr; }
            try self.push(Value.fromFloat(@as(f64, @floatFromInt(a.asInt())) / b.asFloat()));
        } else if (a.isFloat() and b.isInt()) {
            if (b.asInt() == 0) { try self.runtimeErrorAny(.E004, "division by zero"); return error.RuntimeErr; }
            try self.push(Value.fromFloat(a.asFloat() / @as(f64, @floatFromInt(b.asInt()))));
        } else {
            try self.runtimeErrorAny(.E001, "operands must be numbers for '/'");
            return error.RuntimeErr;
        }
    }

    fn binaryMod(self: *Self) RuntimeError!void {
        const b = try self.pop();
        const a = try self.pop();

        if (a.isInt() and b.isInt()) {
            if (b.asInt() == 0) { try self.runtimeErrorAny(.E004, "division by zero"); return error.RuntimeErr; }
            try self.push(Value.fromInt(@intCast(@rem(@as(i64, a.asInt()), @as(i64, b.asInt())))));
        } else {
            try self.runtimeErrorAny(.E001, "operands must be integers for '%'");
            return error.RuntimeErr;
        }
    }

    // ── Comparison operations ─────────────────────────────────────────

    const CompareOp = enum { lt, gt, le, ge };

    fn binaryCompare(self: *Self, op: CompareOp) RuntimeError!void {
        const b = try self.pop();
        const a = try self.pop();
        const result = try self.compareValues(a, b, op);
        try self.push(Value.fromBool(result));
    }

    fn compareValues(self: *Self, a: Value, b: Value, op: CompareOp) RuntimeError!bool {
        if (a.isInt() and b.isInt()) {
            const ai = a.asInt();
            const bi = b.asInt();
            return switch (op) {
                .lt => ai < bi, .gt => ai > bi, .le => ai <= bi, .ge => ai >= bi,
            };
        }

        var fa: f64 = undefined;
        var fb: f64 = undefined;

        if (a.isFloat()) { fa = a.asFloat(); } else if (a.isInt()) { fa = @floatFromInt(a.asInt()); } else {
            try self.runtimeErrorAny(.E001, "operands must be numbers for comparison");
            return error.RuntimeErr;
        }

        if (b.isFloat()) { fb = b.asFloat(); } else if (b.isInt()) { fb = @floatFromInt(b.asInt()); } else {
            try self.runtimeErrorAny(.E001, "operands must be numbers for comparison");
            return error.RuntimeErr;
        }

        return switch (op) {
            .lt => fa < fb, .gt => fa > fb, .le => fa <= fb, .ge => fa >= fb,
        };
    }

    // ── String concatenation ──────────────────────────────────────────

    fn stringConcat(self: *Self) RuntimeError!void {
        const b = try self.pop();
        const a = try self.pop();

        if (!a.isString() or !b.isString()) {
            try self.runtimeErrorAny(.E001, "operands must be strings for '++'");
            return error.RuntimeErr;
        }

        const sa = ObjString.fromObj(a.asObj());
        const sb = ObjString.fromObj(b.asObj());

        const new_len = sa.bytes.len + sb.bytes.len;
        const new_bytes = try self.allocator.alloc(u8, new_len);
        @memcpy(new_bytes[0..sa.bytes.len], sa.bytes);
        @memcpy(new_bytes[sa.bytes.len..], sb.bytes);

        const result = try self.allocator.create(ObjString);
        result.* = .{
            .obj = .{ .obj_type = .string },
            .bytes = new_bytes,
            .hash = 0,
        };
        self.trackObject(&result.obj);
        try self.push(Value.fromObj(&result.obj));
    }

    // ── Function calls ────────────────────────────────────────────────

    fn callValue(self: *Self, arg_count: u8) RuntimeError!void {
        const callee = self.stack[self.stack_top - 1 - @as(u32, arg_count)];

        // Check if it's a closure (user-defined function).
        if (callee.isObj() and callee.asObj().obj_type == .closure) {
            const closure = ObjClosure.fromObj(callee.asObj());
            return self.callClosure(closure, arg_count);
        }

        // Check if it's a builtin function (atom with BUILTIN_BASE).
        if (callee.isAtom()) {
            const atom_id = callee.asAtom();
            if (atom_id >= BUILTIN_BASE and atom_id < BUILTIN_BASE + builtins_mod.builtins.len) {
                return self.callBuiltin(atom_id - BUILTIN_BASE, arg_count);
            }
        }

        try self.runtimeErrorAny(.E001, "value is not callable");
        return error.RuntimeErr;
    }

    fn callClosure(self: *Self, closure: *ObjClosure, arg_count: u8) RuntimeError!void {
        const func = closure.function;

        // Validate arity.
        if (arg_count < func.arity or arg_count > func.arity_max) {
            try self.runtimeErrorAny(.E012, "wrong number of arguments");
            return error.RuntimeErr;
        }

        // Fill in default values for missing optional params.
        if (arg_count < func.arity_max and func.param_defaults != null) {
            const defaults = func.param_defaults.?;
            const missing_start = arg_count - func.arity; // how many optional args were provided
            var i: u8 = @intCast(missing_start);
            while (i < defaults.len) : (i += 1) {
                try self.push(defaults[i]);
            }
        }

        if (self.frame_count >= FRAMES_MAX) {
            try self.runtimeErrorAny(.E001, "stack overflow: too many nested calls");
            return error.StackOverflow;
        }

        const total_args = if (arg_count < func.arity_max and func.param_defaults != null)
            func.arity_max
        else
            arg_count;

        self.frames[self.frame_count] = .{
            .closure = closure,
            .ip = 0,
            .base_slot = self.stack_top - @as(u32, total_args) - 1,
        };
        self.frame_count += 1;
    }

    fn callBuiltin(self: *Self, builtin_idx: u32, arg_count: u8) RuntimeError!void {
        const builtin = builtins_mod.builtins[builtin_idx];

        if (arg_count < builtin.arity_min or arg_count > builtin.arity_max) {
            try self.runtimeErrorAny(.E012, "wrong number of arguments");
            return error.RuntimeErr;
        }

        const args_start = self.stack_top - @as(u32, arg_count);
        const args = self.stack[args_start..self.stack_top];

        // Special-case print builtin for atom name formatting.
        if (builtin_idx == 0) {
            try self.printValue(args[0]);
            self.stack_top -= (@as(u32, arg_count) + 1);
            try self.push(Value.nil);
            return;
        }

        var err_msg: []const u8 = "";
        const result = builtin.func(args, self.allocator, &err_msg) catch |err| {
            switch (err) {
                error.RuntimeError => {
                    try self.runtimeErrorAny(.E001, err_msg);
                    return error.RuntimeErr;
                },
                else => return error.OutOfMemory,
            }
        };

        if (result.isObj()) {
            self.trackObject(result.asObj());
        }

        self.stack_top -= (@as(u32, arg_count) + 1);
        try self.push(result);
    }

    // ── Tail call dispatch ────────────────────────────────────────────

    fn execTailCall(self: *Self, arg_count: u8) RuntimeError!void {
        const callee = self.stack[self.stack_top - 1 - @as(u32, arg_count)];

        // If it's a closure, reuse the current frame.
        if (callee.isObj() and callee.asObj().obj_type == .closure) {
            const closure = ObjClosure.fromObj(callee.asObj());
            const func = closure.function;

            if (arg_count < func.arity or arg_count > func.arity_max) {
                try self.runtimeErrorAny(.E012, "wrong number of arguments");
                return error.RuntimeErr;
            }

            const frame = self.currentFrame();

            // Close upvalues in the current frame.
            self.closeUpvalues(frame.base_slot);

            // Slide the callee + arguments over the current frame's slots.
            const src_start = self.stack_top - @as(u32, arg_count) - 1;
            var j: u32 = 0;
            while (j <= arg_count) : (j += 1) {
                self.stack[frame.base_slot + j] = self.stack[src_start + j];
            }

            self.stack_top = frame.base_slot + @as(u32, arg_count) + 1;
            frame.closure = closure;
            frame.ip = 0;
            // frame_count unchanged -- frame is reused.
            return;
        }

        // If it's a builtin, just call it normally (builtins don't need tail optimization).
        if (callee.isAtom()) {
            const atom_id = callee.asAtom();
            if (atom_id >= BUILTIN_BASE and atom_id < BUILTIN_BASE + builtins_mod.builtins.len) {
                return self.callBuiltin(atom_id - BUILTIN_BASE, arg_count);
            }
        }

        try self.runtimeErrorAny(.E001, "value is not callable");
        return error.RuntimeErr;
    }

    // ── Upvalue management ────────────────────────────────────────────

    fn captureUpvalue(self: *Self, stack_slot: u32) !*ObjUpvalue {
        const slot_ptr: *Value = &self.stack[stack_slot];

        // Walk the open upvalues list to find an existing one for this slot.
        var prev: ?*ObjUpvalue = null;
        var current = self.open_upvalues;
        while (current) |uv| {
            // Open upvalues are sorted by stack position (highest first).
            if (@intFromPtr(uv.location) == @intFromPtr(slot_ptr)) {
                return uv; // Already captured.
            }
            if (@intFromPtr(uv.location) < @intFromPtr(slot_ptr)) {
                break; // Past our slot -- insert here.
            }
            prev = uv;
            current = uv.next;
        }

        // Create a new upvalue.
        const new_uv = try ObjUpvalue.create(self.allocator, slot_ptr);
        self.trackObject(&new_uv.obj);
        new_uv.next = current;
        if (prev) |p| {
            p.next = new_uv;
        } else {
            self.open_upvalues = new_uv;
        }
        return new_uv;
    }

    fn closeUpvalues(self: *Self, last_slot: u32) void {
        const threshold: usize = @intFromPtr(&self.stack[last_slot]);
        while (self.open_upvalues) |uv| {
            if (@intFromPtr(uv.location) < threshold) break;
            // Close: copy value from stack to closed field.
            uv.closed = uv.location.*;
            uv.location = &uv.closed;
            self.open_upvalues = uv.next;
        }
    }

    // ── For-in iteration ──────────────────────────────────────────────

    fn execForIterFrame(self: *Self, jump_offset: u16) RuntimeError!void {
        const iterable = self.stack[self.stack_top - 3];
        if (iterable.isObjType(.range)) {
            const ObjRange = obj_mod.ObjRange;
            const r = ObjRange.fromObj(iterable.asObj());
            const idx = self.stack[self.stack_top - 2].asInt();
            const current = r.start + idx * r.step;
            const done = if (r.step > 0) current >= r.end else current <= r.end;
            if (done) {
                self.currentFrame().ip += jump_offset;
            } else {
                self.stack[self.stack_top - 1] = Value.fromInt(current);
                self.stack[self.stack_top - 2] = Value.fromInt(idx + 1);
            }
        } else {
            try self.runtimeErrorAny(.E001, "value is not iterable");
            return error.RuntimeErr;
        }
    }

    fn execForIter(self: *Self, jump_offset: u16, ip_ptr: *u32) RuntimeError!void {
        const iterable = self.stack[self.stack_top - 3];
        if (iterable.isObjType(.range)) {
            const ObjRange = obj_mod.ObjRange;
            const r = ObjRange.fromObj(iterable.asObj());
            const idx = self.stack[self.stack_top - 2].asInt();
            const current = r.start + idx * r.step;
            const done = if (r.step > 0) current >= r.end else current <= r.end;
            if (done) {
                ip_ptr.* += jump_offset;
            } else {
                self.stack[self.stack_top - 1] = Value.fromInt(current);
                self.stack[self.stack_top - 2] = Value.fromInt(idx + 1);
            }
        } else {
            try self.runtimeErrorAny(.E001, "value is not iterable");
            return error.RuntimeErr;
        }
    }

    // ── Output ────────────────────────────────────────────────────────

    fn printValue(self: *Self, val: Value) !void {
        const text = try builtins_mod.formatValue(val, self.allocator, if (self.atom_names.items.len > 0) self.atom_names.items else null);
        defer self.allocator.free(text);

        if (self.output_buf) |buf| {
            try buf.appendSlice(self.allocator, text);
            try buf.append(self.allocator, '\n');
        } else {
            const stdout = std.fs.File.stdout();
            stdout.writeAll(text) catch {};
            stdout.writeAll("\n") catch {};
        }
    }

    // ── Error reporting ───────────────────────────────────────────────

    fn runtimeErrorLegacy(self: *Self, code: ErrorCode, message: []const u8) !void {
        const line = if (self.chunk) |c| c.getLine(if (self.legacy_ip > 0) self.legacy_ip - 1 else 0) else 0;
        _ = line;
        try self.errors.append(self.allocator, .{
            .error_code = code,
            .severity = .@"error",
            .message = message,
            .span = .{ .start = 0, .end = 0 },
            .labels = &[_]Label{},
            .help = null,
        });
    }

    fn runtimeErrorFrame(self: *Self, code: ErrorCode, message: []const u8) !void {
        _ = code;
        _ = message;
        // In frame mode, get line from current frame's chunk.
        try self.runtimeErrorAny(.E001, "runtime error");
    }

    fn runtimeErrorAny(self: *Self, code: ErrorCode, message: []const u8) !void {
        try self.errors.append(self.allocator, .{
            .error_code = code,
            .severity = .@"error",
            .message = message,
            .span = .{ .start = 0, .end = 0 },
            .labels = &[_]Label{},
            .help = null,
        });
    }
};

// ═══════════════════════════════════════════════════════════════════════
// ── Test helpers ──────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════

/// Build a chunk from a sequence of opcodes/operands and run it.
fn buildAndRun(ops: []const u8, constants: []const Value, allocator: Allocator) !struct { result: Value, errors: []const Diagnostic, vm: *VM } {
    _ = ops;
    _ = constants;
    _ = allocator;
    unreachable; // Not used; individual test helpers below.
}

/// Create a chunk, add opcodes and constants, run the VM.
const TestChunk = struct {
    chunk: Chunk,
    allocator: Allocator,

    fn init(allocator: Allocator) TestChunk {
        return .{ .chunk = .{}, .allocator = allocator };
    }

    fn addConst(self: *TestChunk, val: Value) !u8 {
        const idx = try self.chunk.addConstant(val, self.allocator);
        return @intCast(idx);
    }

    fn emit(self: *TestChunk, byte: u8) !void {
        try self.chunk.write(byte, 1, self.allocator);
    }

    fn emitOp(self: *TestChunk, op: OpCode) !void {
        try self.emit(@intFromEnum(op));
    }

    fn emitConstant(self: *TestChunk, val: Value) !void {
        const idx = try self.addConst(val);
        try self.emitOp(.op_constant);
        try self.emit(idx);
    }

    fn emitReturn(self: *TestChunk) !void {
        try self.emitOp(.op_return);
    }

    fn run(self: *TestChunk) !Value {
        var vm = VM.init(&self.chunk, self.allocator);
        defer vm.deinit();
        return vm.run();
    }

    fn runWithVM(self: *TestChunk) !VM {
        var vm = VM.init(&self.chunk, self.allocator);
        const result = vm.run();
        _ = result catch {};
        return vm;
    }

    fn deinit(self: *TestChunk) void {
        self.chunk.deinit(self.allocator);
    }
};

// ═══════════════════════════════════════════════════════════════════════
// ── Tests ──────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════

// Test 1: VM executes op_constant(42), op_return and returns 42
test "vm: constant 42 return" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();

    try tc.emitConstant(Value.fromInt(42));
    try tc.emitReturn();

    const result = try tc.run();
    try std.testing.expect(result.isInt());
    try std.testing.expectEqual(@as(i32, 42), result.asInt());
}

// Test 2: VM executes 1 + 2 = 3
test "vm: integer addition" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();

    try tc.emitConstant(Value.fromInt(1));
    try tc.emitConstant(Value.fromInt(2));
    try tc.emitOp(.op_add);
    try tc.emitReturn();

    const result = try tc.run();
    try std.testing.expectEqual(@as(i32, 3), result.asInt());
}

test "vm: integer subtraction" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();
    try tc.emitConstant(Value.fromInt(3));
    try tc.emitConstant(Value.fromInt(2));
    try tc.emitOp(.op_subtract);
    try tc.emitReturn();
    const result = try tc.run();
    try std.testing.expectEqual(@as(i32, 1), result.asInt());
}

test "vm: integer multiplication" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();
    try tc.emitConstant(Value.fromInt(6));
    try tc.emitConstant(Value.fromInt(2));
    try tc.emitOp(.op_multiply);
    try tc.emitReturn();
    const result = try tc.run();
    try std.testing.expectEqual(@as(i32, 12), result.asInt());
}

test "vm: integer division truncating" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();
    try tc.emitConstant(Value.fromInt(7));
    try tc.emitConstant(Value.fromInt(2));
    try tc.emitOp(.op_divide);
    try tc.emitReturn();
    const result = try tc.run();
    try std.testing.expectEqual(@as(i32, 3), result.asInt());
}

test "vm: integer modulo" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();
    try tc.emitConstant(Value.fromInt(7));
    try tc.emitConstant(Value.fromInt(2));
    try tc.emitOp(.op_modulo);
    try tc.emitReturn();
    const result = try tc.run();
    try std.testing.expectEqual(@as(i32, 1), result.asInt());
}

test "vm: float addition" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();
    try tc.emitConstant(Value.fromFloat(3.14));
    try tc.emitConstant(Value.fromFloat(2.0));
    try tc.emitOp(.op_add);
    try tc.emitReturn();
    const result = try tc.run();
    try std.testing.expect(result.isFloat());
    try std.testing.expectApproxEqAbs(@as(f64, 5.14), result.asFloat(), 1e-10);
}

test "vm: integer comparison less" {
    const allocator = std.testing.allocator;
    {
        var tc = TestChunk.init(allocator);
        defer tc.deinit();
        try tc.emitConstant(Value.fromInt(1));
        try tc.emitConstant(Value.fromInt(2));
        try tc.emitOp(.op_less);
        try tc.emitReturn();
        const result = try tc.run();
        try std.testing.expect(result.asBool() == true);
    }
    {
        var tc = TestChunk.init(allocator);
        defer tc.deinit();
        try tc.emitConstant(Value.fromInt(2));
        try tc.emitConstant(Value.fromInt(1));
        try tc.emitOp(.op_less);
        try tc.emitReturn();
        const result = try tc.run();
        try std.testing.expect(result.asBool() == false);
    }
}

test "vm: equality comparison" {
    const allocator = std.testing.allocator;
    {
        var tc = TestChunk.init(allocator);
        defer tc.deinit();
        try tc.emitConstant(Value.fromInt(42));
        try tc.emitConstant(Value.fromInt(42));
        try tc.emitOp(.op_equal);
        try tc.emitReturn();
        const result = try tc.run();
        try std.testing.expect(result.asBool() == true);
    }
    {
        var tc = TestChunk.init(allocator);
        defer tc.deinit();
        try tc.emitConstant(Value.fromInt(42));
        try tc.emitConstant(Value.fromInt(43));
        try tc.emitOp(.op_equal);
        try tc.emitReturn();
        const result = try tc.run();
        try std.testing.expect(result.asBool() == false);
    }
}

test "vm: logical not" {
    const allocator = std.testing.allocator;
    {
        var tc = TestChunk.init(allocator);
        defer tc.deinit();
        try tc.emitOp(.op_true);
        try tc.emitOp(.op_not);
        try tc.emitReturn();
        const result = try tc.run();
        try std.testing.expect(result.asBool() == false);
    }
    {
        var tc = TestChunk.init(allocator);
        defer tc.deinit();
        try tc.emitOp(.op_false);
        try tc.emitOp(.op_not);
        try tc.emitReturn();
        const result = try tc.run();
        try std.testing.expect(result.asBool() == true);
    }
    {
        var tc = TestChunk.init(allocator);
        defer tc.deinit();
        try tc.emitOp(.op_nil);
        try tc.emitOp(.op_not);
        try tc.emitReturn();
        const result = try tc.run();
        try std.testing.expect(result.asBool() == true);
    }
}

test "vm: negate int and float" {
    const allocator = std.testing.allocator;
    {
        var tc = TestChunk.init(allocator);
        defer tc.deinit();
        try tc.emitConstant(Value.fromInt(42));
        try tc.emitOp(.op_negate);
        try tc.emitReturn();
        const result = try tc.run();
        try std.testing.expectEqual(@as(i32, -42), result.asInt());
    }
    {
        var tc = TestChunk.init(allocator);
        defer tc.deinit();
        try tc.emitConstant(Value.fromFloat(3.14));
        try tc.emitOp(.op_negate);
        try tc.emitReturn();
        const result = try tc.run();
        try std.testing.expectApproxEqAbs(@as(f64, -3.14), result.asFloat(), 1e-10);
    }
}

test "vm: type mismatch error" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();
    const s = try ObjString.create(allocator, "hello");
    defer s.obj.destroy(allocator);
    try tc.emitConstant(Value.fromInt(1));
    try tc.emitConstant(Value.fromObj(&s.obj));
    try tc.emitOp(.op_add);
    try tc.emitReturn();
    var vm = VM.init(&tc.chunk, allocator);
    defer vm.deinit();
    const result = vm.run();
    try std.testing.expectError(error.RuntimeErr, result);
    try std.testing.expect(vm.errors.items.len > 0);
    try std.testing.expectEqual(ErrorCode.E001, vm.errors.items[0].error_code);
}

test "vm: integer overflow error" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();
    try tc.emitConstant(Value.fromInt(std.math.maxInt(i32)));
    try tc.emitConstant(Value.fromInt(1));
    try tc.emitOp(.op_add);
    try tc.emitReturn();
    var vm = VM.init(&tc.chunk, allocator);
    defer vm.deinit();
    const result = vm.run();
    try std.testing.expectError(error.RuntimeErr, result);
    try std.testing.expectEqual(ErrorCode.E003, vm.errors.items[0].error_code);
}

test "vm: division by zero error" {
    const allocator = std.testing.allocator;
    {
        var tc = TestChunk.init(allocator);
        defer tc.deinit();
        try tc.emitConstant(Value.fromInt(42));
        try tc.emitConstant(Value.fromInt(0));
        try tc.emitOp(.op_divide);
        try tc.emitReturn();
        var vm = VM.init(&tc.chunk, allocator);
        defer vm.deinit();
        const result = vm.run();
        try std.testing.expectError(error.RuntimeErr, result);
        try std.testing.expectEqual(ErrorCode.E004, vm.errors.items[0].error_code);
    }
    {
        var tc = TestChunk.init(allocator);
        defer tc.deinit();
        try tc.emitConstant(Value.fromFloat(42.0));
        try tc.emitConstant(Value.fromFloat(0.0));
        try tc.emitOp(.op_divide);
        try tc.emitReturn();
        var vm = VM.init(&tc.chunk, allocator);
        defer vm.deinit();
        const result = vm.run();
        try std.testing.expectError(error.RuntimeErr, result);
        try std.testing.expectEqual(ErrorCode.E004, vm.errors.items[0].error_code);
    }
}

test "vm: conditional jump" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();
    try tc.emitOp(.op_true);
    try tc.emitOp(.op_jump_if_false);
    try tc.emit(0); try tc.emit(5);
    try tc.emitOp(.op_pop);
    try tc.emitConstant(Value.fromInt(1));
    try tc.emitOp(.op_jump);
    try tc.emit(0); try tc.emit(3);
    try tc.emitOp(.op_pop);
    try tc.emitConstant(Value.fromInt(2));
    try tc.emitReturn();
    const result = try tc.run();
    try std.testing.expectEqual(@as(i32, 1), result.asInt());
}

test "vm: loop with backward jump" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();
    try tc.emitConstant(Value.fromInt(3));
    try tc.emitOp(.op_get_local); try tc.emit(0);
    try tc.emitConstant(Value.fromInt(0));
    try tc.emitOp(.op_greater);
    try tc.emitOp(.op_jump_if_false); try tc.emit(0); try tc.emit(12);
    try tc.emitOp(.op_pop);
    try tc.emitOp(.op_get_local); try tc.emit(0);
    try tc.emitConstant(Value.fromInt(1));
    try tc.emitOp(.op_subtract);
    try tc.emitOp(.op_set_local); try tc.emit(0);
    try tc.emitOp(.op_pop);
    try tc.emitOp(.op_loop); try tc.emit(0); try tc.emit(20);
    try tc.emitOp(.op_pop);
    try tc.emitOp(.op_get_local); try tc.emit(0);
    try tc.emitReturn();
    const result = try tc.run();
    try std.testing.expectEqual(@as(i32, 0), result.asInt());
}

test "vm: local variable get and set" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();
    try tc.emitConstant(Value.fromInt(10));
    try tc.emitConstant(Value.fromInt(20));
    try tc.emitOp(.op_set_local); try tc.emit(0);
    try tc.emitOp(.op_pop);
    try tc.emitOp(.op_get_local); try tc.emit(0);
    try tc.emitReturn();
    const result = try tc.run();
    try std.testing.expectEqual(@as(i32, 20), result.asInt());
}

test "vm: print builtin" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();
    try tc.emitOp(.op_get_builtin); try tc.emit(0);
    try tc.emitConstant(Value.fromInt(42));
    try tc.emitOp(.op_call); try tc.emit(1);
    try tc.emitReturn();
    var output: std.ArrayListUnmanaged(u8) = .empty;
    defer output.deinit(allocator);
    var vm = VM.init(&tc.chunk, allocator);
    vm.output_buf = &output;
    defer vm.deinit();
    _ = try vm.run();
}

test "vm: str builtin" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();
    try tc.emitOp(.op_get_builtin); try tc.emit(1);
    try tc.emitConstant(Value.fromInt(42));
    try tc.emitOp(.op_call); try tc.emit(1);
    try tc.emitReturn();
    var vm = VM.init(&tc.chunk, allocator);
    defer vm.deinit();
    const result = try vm.run();
    try std.testing.expect(result.isString());
}

test "vm: type_of builtin" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();
    try tc.emitOp(.op_get_builtin); try tc.emit(3);
    try tc.emitConstant(Value.fromInt(42));
    try tc.emitOp(.op_call); try tc.emit(1);
    try tc.emitReturn();
    var vm = VM.init(&tc.chunk, allocator);
    defer vm.deinit();
    const result = try vm.run();
    try std.testing.expect(result.isAtom());
    try std.testing.expectEqual(@as(u32, 0), result.asAtom());
}

test "vm: assert builtin" {
    const allocator = std.testing.allocator;
    {
        var tc = TestChunk.init(allocator);
        defer tc.deinit();
        try tc.emitOp(.op_get_builtin); try tc.emit(4);
        try tc.emitOp(.op_true);
        try tc.emitOp(.op_call); try tc.emit(1);
        try tc.emitReturn();
        var vm = VM.init(&tc.chunk, allocator);
        defer vm.deinit();
        const result = try vm.run();
        try std.testing.expect(result.isNil());
    }
    {
        var tc = TestChunk.init(allocator);
        defer tc.deinit();
        try tc.emitOp(.op_get_builtin); try tc.emit(4);
        try tc.emitOp(.op_false);
        try tc.emitOp(.op_call); try tc.emit(1);
        try tc.emitReturn();
        var vm = VM.init(&tc.chunk, allocator);
        defer vm.deinit();
        const result = vm.run();
        try std.testing.expectError(error.RuntimeErr, result);
    }
}

test "vm: range builtin" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();
    try tc.emitOp(.op_get_builtin); try tc.emit(6);
    try tc.emitConstant(Value.fromInt(10));
    try tc.emitOp(.op_call); try tc.emit(1);
    try tc.emitReturn();
    var vm = VM.init(&tc.chunk, allocator);
    defer vm.deinit();
    const result = try vm.run();
    try std.testing.expect(result.isObjType(.range));
    const r = obj_mod.ObjRange.fromObj(result.asObj());
    try std.testing.expectEqual(@as(i32, 0), r.start);
    try std.testing.expectEqual(@as(i32, 10), r.end);
}

test "vm: len builtin" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();
    const s = try ObjString.create(allocator, "hello");
    try tc.emitOp(.op_get_builtin); try tc.emit(2);
    try tc.emitConstant(Value.fromObj(&s.obj));
    try tc.emitOp(.op_call); try tc.emit(1);
    try tc.emitReturn();
    var vm = VM.init(&tc.chunk, allocator);
    defer vm.deinit();
    const result = try vm.run();
    try std.testing.expectEqual(@as(i32, 5), result.asInt());
    s.obj.destroy(allocator);
}

test "vm: NaN not equal to NaN" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();
    const nan = std.math.nan(f64);
    try tc.emitConstant(Value.fromFloat(nan));
    try tc.emitConstant(Value.fromFloat(nan));
    try tc.emitOp(.op_equal);
    try tc.emitReturn();
    const result = try tc.run();
    try std.testing.expect(result.asBool() == false);
}

test "vm: string concatenation" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();
    const s1 = try ObjString.create(allocator, "hello");
    const s2 = try ObjString.create(allocator, " world");
    try tc.emitConstant(Value.fromObj(&s1.obj));
    try tc.emitConstant(Value.fromObj(&s2.obj));
    try tc.emitOp(.op_concat);
    try tc.emitReturn();
    var vm = VM.init(&tc.chunk, allocator);
    defer vm.deinit();
    const result = try vm.run();
    try std.testing.expect(result.isString());
    const str = ObjString.fromObj(result.asObj());
    try std.testing.expectEqualStrings("hello world", str.bytes);
    s1.obj.destroy(allocator);
    s2.obj.destroy(allocator);
}

test "vm: mixed int float promotion" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();
    try tc.emitConstant(Value.fromInt(1));
    try tc.emitConstant(Value.fromFloat(2.0));
    try tc.emitOp(.op_add);
    try tc.emitReturn();
    const result = try tc.run();
    try std.testing.expect(result.isFloat());
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), result.asFloat(), 1e-10);
}

test "vm: greater, less_equal, greater_equal" {
    const allocator = std.testing.allocator;
    {
        var tc = TestChunk.init(allocator);
        defer tc.deinit();
        try tc.emitConstant(Value.fromInt(5));
        try tc.emitConstant(Value.fromInt(3));
        try tc.emitOp(.op_greater);
        try tc.emitReturn();
        const result = try tc.run();
        try std.testing.expect(result.asBool());
    }
    {
        var tc = TestChunk.init(allocator);
        defer tc.deinit();
        try tc.emitConstant(Value.fromInt(3));
        try tc.emitConstant(Value.fromInt(3));
        try tc.emitOp(.op_less_equal);
        try tc.emitReturn();
        const result = try tc.run();
        try std.testing.expect(result.asBool());
    }
    {
        var tc = TestChunk.init(allocator);
        defer tc.deinit();
        try tc.emitConstant(Value.fromInt(3));
        try tc.emitConstant(Value.fromInt(5));
        try tc.emitOp(.op_greater_equal);
        try tc.emitReturn();
        const result = try tc.run();
        try std.testing.expect(!result.asBool());
    }
}
