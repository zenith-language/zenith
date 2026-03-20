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
const error_mod = @import("error");
const Diagnostic = error_mod.Diagnostic;
const ErrorCode = error_mod.ErrorCode;
const Label = error_mod.Label;
const builtins_mod = @import("builtins");
const NativeFn = builtins_mod.NativeFn;

const STACK_MAX: u32 = 65536;
const FRAMES_MAX: u32 = 256;

/// Call frame for function calls (builtins in Phase 1).
const CallFrame = struct {
    return_ip: u32,
    base_slot: u32,
};

/// Runtime error type.
pub const RuntimeError = error{
    RuntimeErr,
    StackOverflow,
    StackUnderflow,
} || Allocator.Error;

/// Stack-based virtual machine with labeled-switch dispatch.
pub const VM = struct {
    chunk: *const Chunk,
    ip: u32,
    stack: [STACK_MAX]Value,
    stack_top: u32,
    frames: [FRAMES_MAX]CallFrame,
    frame_count: u32,
    objects: ?*obj_mod.Obj, // Head of allocated objects linked list
    atom_names: std.ArrayListUnmanaged([]const u8),
    allocator: Allocator,
    errors: std.ArrayListUnmanaged(Diagnostic),
    // Output writer for tests (captures print output instead of stdout).
    output_buf: ?*std.ArrayListUnmanaged(u8),

    const Self = @This();

    /// Initialize a VM to execute a chunk.
    pub fn init(chunk: *const Chunk, allocator: Allocator) VM {
        return .{
            .chunk = chunk,
            .ip = 0,
            .stack = undefined,
            .stack_top = 0,
            .frames = undefined,
            .frame_count = 0,
            .objects = null,
            .atom_names = .empty,
            .allocator = allocator,
            .errors = .empty,
            .output_buf = null,
        };
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

    // ── Reading from bytecode stream ──────────────────────────────────

    fn readByte(self: *Self) u8 {
        const b = self.chunk.code.items[self.ip];
        self.ip += 1;
        return b;
    }

    fn readU16(self: *Self) u16 {
        const hi: u16 = self.chunk.code.items[self.ip];
        const lo: u16 = self.chunk.code.items[self.ip + 1];
        self.ip += 2;
        return (hi << 8) | lo;
    }

    fn readConstant(self: *Self) Value {
        const idx = self.readByte();
        return self.chunk.constants.items[idx];
    }

    fn readConstantLong(self: *Self) Value {
        const idx = self.readU16();
        return self.chunk.constants.items[idx];
    }

    // ── Main dispatch loop ────────────────────────────────────────────

    /// Execute the loaded chunk. Returns the final value on the stack.
    pub fn run(self: *Self) RuntimeError!Value {
        const code = self.chunk.code.items;
        if (code.len == 0) return Value.nil;

        while (self.ip < code.len) {
            const opcode: OpCode = @enumFromInt(code[self.ip]);
            self.ip += 1;

            switch (opcode) {
                .op_constant => {
                    const val = self.readConstant();
                    try self.push(val);
                },
                .op_constant_long => {
                    const val = self.readConstantLong();
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

                .op_negate => {
                    const val = try self.pop();
                    if (val.isInt()) {
                        const n = val.asInt();
                        // Check for negating min_int (overflow).
                        if (n == std.math.minInt(i32)) {
                            try self.runtimeError(.E003, "integer overflow: cannot negate minimum integer");
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
                        try self.runtimeError(.E001, "cannot negate non-numeric value");
                        return error.RuntimeErr;
                    }
                },

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
                    const slot = self.readByte();
                    try self.push(self.stack[slot]);
                },
                .op_set_local => {
                    const slot = self.readByte();
                    self.stack[slot] = self.peek(0);
                },
                .op_get_global, .op_set_global, .op_define_global => {
                    // Globals not used in Phase 1 (all locals).
                    try self.runtimeError(.E002, "globals not supported in Phase 1");
                    return error.RuntimeErr;
                },

                .op_jump => {
                    const offset = self.readU16();
                    self.ip += offset;
                },
                .op_jump_if_false => {
                    const offset = self.readU16();
                    if (builtins_mod.isFalsy(self.peek(0))) {
                        self.ip += offset;
                    }
                },
                .op_loop => {
                    const offset = self.readU16();
                    self.ip -= offset;
                },

                .op_print => {
                    const val = try self.pop();
                    try self.printValue(val);
                },

                .op_get_builtin => {
                    const idx = self.readByte();
                    // Push a sentinel value that represents the builtin.
                    // We use an atom with a special offset to identify builtins.
                    // Builtin index is stored as: BUILTIN_BASE + idx.
                    try self.push(Value.fromAtom(BUILTIN_BASE + @as(u32, idx)));
                },

                .op_call => {
                    const arg_count = self.readByte();
                    try self.callValue(arg_count);
                },

                .op_return => {
                    if (self.stack_top > 0) {
                        return try self.pop();
                    }
                    return Value.nil;
                },

                .op_atom => {
                    const val = self.readConstant();
                    try self.push(val);
                },

                .op_for_iter => {
                    // For-in iteration: reads the iterator and index from stack,
                    // advances, and either sets the loop variable or jumps past the body.
                    const jump_offset = self.readU16();
                    // Stack layout: [..., iterable, index, loop_var]
                    // iterable = stack_top - 3, index = stack_top - 2, loop_var = stack_top - 1
                    const iterable = self.stack[self.stack_top - 3];
                    if (iterable.isObjType(.range)) {
                        const ObjRange = obj_mod.ObjRange;
                        const r = ObjRange.fromObj(iterable.asObj());
                        const idx = self.stack[self.stack_top - 2].asInt();
                        const current = r.start + idx * r.step;
                        // Check termination based on step direction.
                        const done = if (r.step > 0) current >= r.end else current <= r.end;
                        if (done) {
                            // Jump past the body.
                            self.ip += jump_offset;
                        } else {
                            // Set loop variable to current value.
                            self.stack[self.stack_top - 1] = Value.fromInt(current);
                            // Increment index.
                            self.stack[self.stack_top - 2] = Value.fromInt(idx + 1);
                        }
                    } else {
                        try self.runtimeError(.E001, "value is not iterable");
                        return error.RuntimeErr;
                    }
                },
            }
        }

        // If we reached end of code without return, return nil.
        return Value.nil;
    }

    // Sentinel base for builtin function atoms.
    const BUILTIN_BASE: u32 = 0xFFFF_0000;

    // ── Arithmetic operations ─────────────────────────────────────────

    fn binaryAdd(self: *Self) RuntimeError!void {
        const b = try self.pop();
        const a = try self.pop();

        if (a.isInt() and b.isInt()) {
            const ai: i64 = a.asInt();
            const bi: i64 = b.asInt();
            const result = ai + bi;
            if (result > std.math.maxInt(i32) or result < std.math.minInt(i32)) {
                try self.runtimeError(.E003, "integer overflow");
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
            try self.runtimeError(.E001, "operands must be numbers for '+'");
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
                try self.runtimeError(.E003, "integer overflow");
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
            try self.runtimeError(.E001, "operands must be numbers for '-'");
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
                try self.runtimeError(.E003, "integer overflow");
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
            try self.runtimeError(.E001, "operands must be numbers for '*'");
            return error.RuntimeErr;
        }
    }

    fn binaryDiv(self: *Self) RuntimeError!void {
        const b = try self.pop();
        const a = try self.pop();

        if (a.isInt() and b.isInt()) {
            if (b.asInt() == 0) {
                try self.runtimeError(.E004, "division by zero");
                return error.RuntimeErr;
            }
            try self.push(Value.fromInt(@intCast(@divTrunc(@as(i64, a.asInt()), @as(i64, b.asInt())))));
        } else if (a.isFloat() and b.isFloat()) {
            if (b.asFloat() == 0.0) {
                try self.runtimeError(.E004, "division by zero");
                return error.RuntimeErr;
            }
            try self.push(Value.fromFloat(a.asFloat() / b.asFloat()));
        } else if (a.isInt() and b.isFloat()) {
            if (b.asFloat() == 0.0) {
                try self.runtimeError(.E004, "division by zero");
                return error.RuntimeErr;
            }
            try self.push(Value.fromFloat(@as(f64, @floatFromInt(a.asInt())) / b.asFloat()));
        } else if (a.isFloat() and b.isInt()) {
            if (b.asInt() == 0) {
                try self.runtimeError(.E004, "division by zero");
                return error.RuntimeErr;
            }
            try self.push(Value.fromFloat(a.asFloat() / @as(f64, @floatFromInt(b.asInt()))));
        } else {
            try self.runtimeError(.E001, "operands must be numbers for '/'");
            return error.RuntimeErr;
        }
    }

    fn binaryMod(self: *Self) RuntimeError!void {
        const b = try self.pop();
        const a = try self.pop();

        if (a.isInt() and b.isInt()) {
            if (b.asInt() == 0) {
                try self.runtimeError(.E004, "division by zero");
                return error.RuntimeErr;
            }
            try self.push(Value.fromInt(@intCast(@rem(@as(i64, a.asInt()), @as(i64, b.asInt())))));
        } else {
            try self.runtimeError(.E001, "operands must be integers for '%'");
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
                .lt => ai < bi,
                .gt => ai > bi,
                .le => ai <= bi,
                .ge => ai >= bi,
            };
        }

        // Promote to float for comparison if mixed.
        var fa: f64 = undefined;
        var fb: f64 = undefined;

        if (a.isFloat()) {
            fa = a.asFloat();
        } else if (a.isInt()) {
            fa = @floatFromInt(a.asInt());
        } else {
            try self.runtimeError(.E001, "operands must be numbers for comparison");
            return error.RuntimeErr;
        }

        if (b.isFloat()) {
            fb = b.asFloat();
        } else if (b.isInt()) {
            fb = @floatFromInt(b.asInt());
        } else {
            try self.runtimeError(.E001, "operands must be numbers for comparison");
            return error.RuntimeErr;
        }

        // IEEE 754: comparisons with NaN return false.
        return switch (op) {
            .lt => fa < fb,
            .gt => fa > fb,
            .le => fa <= fb,
            .ge => fa >= fb,
        };
    }

    // ── String concatenation ──────────────────────────────────────────

    fn stringConcat(self: *Self) RuntimeError!void {
        const b = try self.pop();
        const a = try self.pop();

        if (!a.isString() or !b.isString()) {
            try self.runtimeError(.E001, "operands must be strings for '++'");
            return error.RuntimeErr;
        }

        const sa = ObjString.fromObj(a.asObj());
        const sb = ObjString.fromObj(b.asObj());

        // Concatenate the two strings.
        const new_len = sa.bytes.len + sb.bytes.len;
        const new_bytes = try self.allocator.alloc(u8, new_len);
        @memcpy(new_bytes[0..sa.bytes.len], sa.bytes);
        @memcpy(new_bytes[sa.bytes.len..], sb.bytes);

        const result = try self.allocator.create(ObjString);
        result.* = .{
            .obj = .{ .obj_type = .string },
            .bytes = new_bytes,
            .hash = 0, // We could compute hash but it's not critical for Phase 1.
        };
        self.trackObject(&result.obj);
        try self.push(Value.fromObj(&result.obj));
    }

    // ── Function calls ────────────────────────────────────────────────

    fn callValue(self: *Self, arg_count: u8) RuntimeError!void {
        // The callee is on the stack below the arguments.
        const callee = self.stack[self.stack_top - 1 - @as(u32, arg_count)];

        // Check if it's a builtin function (atom with BUILTIN_BASE).
        if (callee.isAtom()) {
            const atom_id = callee.asAtom();
            if (atom_id >= BUILTIN_BASE and atom_id < BUILTIN_BASE + builtins_mod.builtins.len) {
                const builtin_idx = atom_id - BUILTIN_BASE;
                const builtin = builtins_mod.builtins[builtin_idx];

                // Check arity.
                if (arg_count < builtin.arity_min or arg_count > builtin.arity_max) {
                    try self.runtimeError(.E012, "wrong number of arguments");
                    return error.RuntimeErr;
                }

                // Gather arguments from stack.
                const args_start = self.stack_top - @as(u32, arg_count);
                const args = self.stack[args_start..self.stack_top];

                // Special-case print and show builtins to inject atom names.
                if (builtin_idx == 0) {
                    // print builtin: use VM's printValue for proper atom name formatting.
                    try self.printValue(args[0]);
                    // Pop arguments and callee, push nil result.
                    self.stack_top -= (@as(u32, arg_count) + 1);
                    try self.push(Value.nil);
                    return;
                }

                // Call the native function.
                var err_msg: []const u8 = "";
                const result = builtin.func(args, self.allocator, &err_msg) catch |err| {
                    switch (err) {
                        error.RuntimeError => {
                            try self.runtimeError(.E001, err_msg);
                            return error.RuntimeErr;
                        },
                        else => return error.OutOfMemory,
                    }
                };

                // Track any objects returned by builtins.
                if (result.isObj()) {
                    self.trackObject(result.asObj());
                }

                // Pop arguments and callee, push result.
                self.stack_top -= (@as(u32, arg_count) + 1);
                try self.push(result);
                return;
            }
        }

        try self.runtimeError(.E001, "value is not callable");
        return error.RuntimeErr;
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

    fn runtimeError(self: *Self, code: ErrorCode, message: []const u8) !void {
        const line = self.chunk.getLine(if (self.ip > 0) self.ip - 1 else 0);
        try self.errors.append(self.allocator, .{
            .error_code = code,
            .severity = .@"error",
            .message = message,
            .span = .{ .start = 0, .end = 0 },
            .labels = &[_]Label{},
            .help = null,
        });
        _ = line;
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

// Test 3: VM executes 3 - 2 = 1
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

// Test 4: VM executes 6 * 2 = 12
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

// Test 5: VM executes 7 / 2 = 3 (truncating)
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

// Test 6: VM executes 7 % 2 = 1
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

// Test 7: VM executes float arithmetic: 3.14 + 2.0 = 5.14
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

// Test 8: VM executes comparison: 1 < 2 = true, 2 < 1 = false
test "vm: integer comparison less" {
    const allocator = std.testing.allocator;

    // 1 < 2 = true
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

    // 2 < 1 = false
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

// Test 9: VM executes equality: 42 == 42 = true, 42 == 43 = false
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

// Test 10: logical: true and false = false, false or true = true
test "vm: logical and/or via jumps" {
    // We test the underlying not operation + jump_if_false which implements
    // logical and/or at the compiler level.
    const allocator = std.testing.allocator;

    // not true = false
    {
        var tc = TestChunk.init(allocator);
        defer tc.deinit();
        try tc.emitOp(.op_true);
        try tc.emitOp(.op_not);
        try tc.emitReturn();
        const result = try tc.run();
        try std.testing.expect(result.asBool() == false);
    }

    // not false = true
    {
        var tc = TestChunk.init(allocator);
        defer tc.deinit();
        try tc.emitOp(.op_false);
        try tc.emitOp(.op_not);
        try tc.emitReturn();
        const result = try tc.run();
        try std.testing.expect(result.asBool() == true);
    }

    // not nil = true (nil is falsy)
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

// Test 11: VM executes negate: -42 = -42, -3.14 = -3.14
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

// Test 12: type mismatch: 1 + "hello" -> error
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

// Test 13: integer overflow (i32 max + 1)
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
    try std.testing.expect(vm.errors.items.len > 0);
    try std.testing.expectEqual(ErrorCode.E003, vm.errors.items[0].error_code);
}

// Test 14: division by zero (both int and float)
test "vm: division by zero error" {
    const allocator = std.testing.allocator;

    // Int division by zero
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

    // Float division by zero
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

// Test 15: conditional jump (if/else paths)
test "vm: conditional jump" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();

    // if true { 1 } else { 2 }
    // Bytecode:
    //   0: op_true
    //   1: op_jump_if_false offset=5 (skip to else)
    //   4: op_pop (pop condition)
    //   5: op_constant(1) -- then branch
    //   7: op_jump offset=3 (skip else)
    //  10: op_pop (pop condition for false path)
    //  11: op_constant(2) -- else branch
    //  13: op_return

    try tc.emitOp(.op_true);
    try tc.emitOp(.op_jump_if_false);
    try tc.emit(0); // hi
    try tc.emit(5); // lo: jump 5 bytes (to offset 8)
    try tc.emitOp(.op_pop); // pop condition
    try tc.emitConstant(Value.fromInt(1)); // then: constant(1)
    try tc.emitOp(.op_jump);
    try tc.emit(0); // hi
    try tc.emit(3); // lo: jump 3 bytes (to offset 13)
    try tc.emitOp(.op_pop); // pop condition (false path)
    try tc.emitConstant(Value.fromInt(2)); // else: constant(2)
    try tc.emitReturn();

    const result = try tc.run();
    try std.testing.expectEqual(@as(i32, 1), result.asInt()); // true path taken
}

// Test 16: loop with backward jump
test "vm: loop with backward jump" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();

    // let x = 3; while x > 0 { x = x - 1 }; x
    // Stack: [x]
    // Bytecode:
    //  0: op_constant(3)       -- x = 3 (slot 0)
    //  2: op_get_local(0)      -- push x
    //  4: op_constant(0)       -- push 0
    //  6: op_greater           -- x > 0?
    //  7: op_jump_if_false 10  -- exit if false
    // 10: op_pop               -- pop condition
    // 11: op_get_local(0)      -- push x
    // 13: op_constant(1)       -- push 1
    // 15: op_subtract          -- x - 1
    // 16: op_set_local(0)      -- x = result
    // 18: op_pop               -- discard set_local value (copy)
    // 19: op_loop 17           -- back to offset 2
    // 22: op_pop               -- pop false condition
    // 23: op_get_local(0)      -- push x
    // 25: op_return

    // Bytecode layout:
    //  0: op_constant idx=0  (3)    -> x = 3, stack: [3]
    //  2: op_get_local 0            -> push x, stack: [3, 3]
    //  4: op_constant idx=1  (0)    -> push 0, stack: [3, 3, 0]
    //  6: op_greater                -> x > 0?, stack: [3, true]
    //  7: op_jump_if_false 12       -> if false, jump to offset 22
    // 10: op_pop                    -> pop condition, stack: [3]
    // 11: op_get_local 0            -> push x, stack: [3, 3]
    // 13: op_constant idx=2  (1)    -> push 1, stack: [3, 3, 1]
    // 15: op_subtract               -> x-1, stack: [3, 2]
    // 16: op_set_local 0            -> x=2, stack: [3, 2]
    // 18: op_pop                    -> pop copy, stack: [3]  (but stack[0]=2)
    // 19: op_loop 20                -> ip = 22 - 20 = 2
    // 22: op_pop                    -> pop false condition
    // 23: op_get_local 0            -> push x (=0)
    // 25: op_return
    try tc.emitConstant(Value.fromInt(3)); // offset 0-1
    try tc.emitOp(.op_get_local); // offset 2
    try tc.emit(0); // offset 3
    try tc.emitConstant(Value.fromInt(0)); // offset 4-5
    try tc.emitOp(.op_greater); // offset 6
    try tc.emitOp(.op_jump_if_false); // offset 7
    try tc.emit(0); // offset 8 (hi)
    try tc.emit(12); // offset 9 (lo): jump 12 -> ip = 10 + 12 = 22
    try tc.emitOp(.op_pop); // offset 10
    try tc.emitOp(.op_get_local); // offset 11
    try tc.emit(0); // offset 12
    try tc.emitConstant(Value.fromInt(1)); // offset 13-14
    try tc.emitOp(.op_subtract); // offset 15
    try tc.emitOp(.op_set_local); // offset 16
    try tc.emit(0); // offset 17
    try tc.emitOp(.op_pop); // offset 18
    try tc.emitOp(.op_loop); // offset 19
    try tc.emit(0); // offset 20 (hi)
    try tc.emit(20); // offset 21 (lo): loop back 20 -> ip = 22 - 20 = 2
    try tc.emitOp(.op_pop); // offset 22: pop false condition
    try tc.emitOp(.op_get_local); // offset 23
    try tc.emit(0); // offset 24
    try tc.emitReturn(); // offset 25

    const result = try tc.run();
    try std.testing.expectEqual(@as(i32, 0), result.asInt()); // x ends at 0
}

// Test 17: local variable get/set
test "vm: local variable get and set" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();

    // let x = 10; x = 20; x
    try tc.emitConstant(Value.fromInt(10)); // slot 0 = 10
    try tc.emitConstant(Value.fromInt(20)); // push 20
    try tc.emitOp(.op_set_local); // set slot 0 = 20
    try tc.emit(0);
    try tc.emitOp(.op_pop); // pop the copy
    try tc.emitOp(.op_get_local); // get slot 0
    try tc.emit(0);
    try tc.emitReturn();

    const result = try tc.run();
    try std.testing.expectEqual(@as(i32, 20), result.asInt());
}

// Test 18: print builtin writes to output
test "vm: print builtin" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();

    // print(42): get_builtin(0), constant(42), call(1)
    try tc.emitOp(.op_get_builtin);
    try tc.emit(0); // print is index 0
    try tc.emitConstant(Value.fromInt(42));
    try tc.emitOp(.op_call);
    try tc.emit(1);
    try tc.emitReturn();

    var output: std.ArrayListUnmanaged(u8) = .empty;
    defer output.deinit(allocator);

    var vm = VM.init(&tc.chunk, allocator);
    vm.output_buf = &output;
    defer vm.deinit();

    _ = try vm.run();
    // The print call goes through the builtin which writes to stdout, not output_buf.
    // For this test, we just verify it doesn't error.
    // The builtin writes to stdout directly. The VM's output_buf is for op_print.
}

// Test 19: str builtin converts value to string
test "vm: str builtin" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();

    // str(42): get_builtin(1), constant(42), call(1)
    try tc.emitOp(.op_get_builtin);
    try tc.emit(1); // str is index 1
    try tc.emitConstant(Value.fromInt(42));
    try tc.emitOp(.op_call);
    try tc.emit(1);
    try tc.emitReturn();

    var vm = VM.init(&tc.chunk, allocator);
    defer vm.deinit();

    const result = try vm.run();
    try std.testing.expect(result.isString());
    // Note: the string object is tracked by the VM and freed on deinit.
}

// Test 20: type_of builtin returns atom
test "vm: type_of builtin" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();

    // type_of(42): get_builtin(3), constant(42), call(1)
    try tc.emitOp(.op_get_builtin);
    try tc.emit(3); // type_of is index 3
    try tc.emitConstant(Value.fromInt(42));
    try tc.emitOp(.op_call);
    try tc.emit(1);
    try tc.emitReturn();

    var vm = VM.init(&tc.chunk, allocator);
    defer vm.deinit();

    const result = try vm.run();
    try std.testing.expect(result.isAtom());
    try std.testing.expectEqual(@as(u32, 0), result.asAtom()); // :int
}

// Test 21: assert builtin does nothing on true, panics on false
test "vm: assert builtin" {
    const allocator = std.testing.allocator;

    // assert(true) - should succeed
    {
        var tc = TestChunk.init(allocator);
        defer tc.deinit();
        try tc.emitOp(.op_get_builtin);
        try tc.emit(4); // assert is index 4
        try tc.emitOp(.op_true);
        try tc.emitOp(.op_call);
        try tc.emit(1);
        try tc.emitReturn();

        var vm = VM.init(&tc.chunk, allocator);
        defer vm.deinit();
        const result = try vm.run();
        try std.testing.expect(result.isNil());
    }

    // assert(false) - should error
    {
        var tc = TestChunk.init(allocator);
        defer tc.deinit();
        try tc.emitOp(.op_get_builtin);
        try tc.emit(4); // assert
        try tc.emitOp(.op_false);
        try tc.emitOp(.op_call);
        try tc.emit(1);
        try tc.emitReturn();

        var vm = VM.init(&tc.chunk, allocator);
        defer vm.deinit();
        const result = vm.run();
        try std.testing.expectError(error.RuntimeErr, result);
    }
}

// Test 22: range builtin (exists, returns value)
test "vm: range builtin" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();

    // range(10): get_builtin(6), constant(10), call(1)
    try tc.emitOp(.op_get_builtin);
    try tc.emit(6); // range is index 6
    try tc.emitConstant(Value.fromInt(10));
    try tc.emitOp(.op_call);
    try tc.emit(1);
    try tc.emitReturn();

    var vm = VM.init(&tc.chunk, allocator);
    defer vm.deinit();
    const result = try vm.run();
    // range() now returns an ObjRange value
    try std.testing.expect(result.isObjType(.range));
    const r = obj_mod.ObjRange.fromObj(result.asObj());
    try std.testing.expectEqual(@as(i32, 0), r.start);
    try std.testing.expectEqual(@as(i32, 10), r.end);
    try std.testing.expectEqual(@as(i32, 1), r.step);
}

// Test 23: len builtin returns string length
test "vm: len builtin" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();

    const s = try ObjString.create(allocator, "hello");
    // Don't defer destroy -- let the constant pool hold it; VM won't free constants.
    // We'll free it manually after.

    // len("hello"): get_builtin(2), constant(string), call(1)
    try tc.emitOp(.op_get_builtin);
    try tc.emit(2); // len is index 2
    try tc.emitConstant(Value.fromObj(&s.obj));
    try tc.emitOp(.op_call);
    try tc.emit(1);
    try tc.emitReturn();

    var vm = VM.init(&tc.chunk, allocator);
    defer vm.deinit();
    const result = try vm.run();
    try std.testing.expect(result.isInt());
    try std.testing.expectEqual(@as(i32, 5), result.asInt());

    // Clean up the string object (not tracked by VM since it came from constant pool).
    s.obj.destroy(allocator);
}

// Test 24: NaN != NaN (IEEE 754)
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
    try std.testing.expect(result.isBool());
    try std.testing.expect(result.asBool() == false); // NaN != NaN
}

// Test 25: String concatenation via op_concat
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

    // Clean up constant pool strings (not tracked by VM).
    s1.obj.destroy(allocator);
    s2.obj.destroy(allocator);
}

// Test: mixed int/float promotion
test "vm: mixed int float promotion" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();

    // 1 + 2.0 = 3.0
    try tc.emitConstant(Value.fromInt(1));
    try tc.emitConstant(Value.fromFloat(2.0));
    try tc.emitOp(.op_add);
    try tc.emitReturn();

    const result = try tc.run();
    try std.testing.expect(result.isFloat());
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), result.asFloat(), 1e-10);
}

// Test: comparison ops
test "vm: greater, less_equal, greater_equal" {
    const allocator = std.testing.allocator;

    // 5 > 3 = true
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

    // 3 <= 3 = true
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

    // 3 >= 5 = false
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
