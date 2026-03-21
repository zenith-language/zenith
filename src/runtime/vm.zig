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
const gc_mod = @import("gc");
const GC = gc_mod.GC;

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
    objects: ?*obj_mod.Obj, // Head of allocated objects linked list (legacy mode)
    open_upvalues: ?*ObjUpvalue, // Head of sorted open upvalue list
    atom_names: std.ArrayListUnmanaged([]const u8),
    adt_type_info: ?[]const builtins_mod.AdtTypeInfo,
    allocator: Allocator, // Heap allocator (GC allocator in GC mode)
    /// Infrastructure allocator for non-GC bookkeeping (errors, atom_names).
    /// Same as allocator in legacy mode; raw backing allocator in GC mode.
    infra_allocator: Allocator,
    gc: ?*GC, // GC instance (null for legacy raw-chunk mode)
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
            .adt_type_info = null,
            .allocator = allocator,
            .infra_allocator = allocator,
            .gc = null,
            .errors = .empty,
            .output_buf = null,
        };
    }

    /// Initialize VM with an ObjClosure (Phase 2+ mode).
    /// When `gc` is non-null, all object tracking goes through the GC
    /// and nursery collection is triggered automatically on allocation.
    /// `heap_allocator` is the GC-aware allocator for object creation.
    /// `infra_alloc` is the raw allocator for bookkeeping (errors, atom names).
    pub fn initWithClosure(closure: *ObjClosure, heap_allocator: Allocator, gc: ?*GC, infra_alloc: Allocator) VM {
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
            .adt_type_info = null,
            .allocator = heap_allocator,
            .infra_allocator = infra_alloc,
            .gc = gc,
            .errors = .empty,
            .output_buf = null,
        };
        // Register VM with GC for root scanning.
        if (gc) |g| {
            g.vm = &vm;
        }
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
    /// Used by the pipeline when running compiler output via raw chunk (legacy).
    pub fn initForScript(chunk: *const Chunk, allocator: Allocator) VM {
        var vm = init(chunk, allocator);
        // Legacy mode: gc is already null from init.
        vm.stack[0] = Value.nil;
        vm.stack_top = 1;
        return vm;
    }

    /// Free VM resources.
    pub fn deinit(self: *VM) void {
        if (self.gc != null) {
            // GC mode: disconnect VM from GC (GC owns the objects).
            if (self.gc) |g| {
                g.vm = null;
            }
        } else {
            // Legacy mode: free allocated objects directly.
            self.freeObjects();
        }
        self.atom_names.deinit(self.infra_allocator);
        self.errors.deinit(self.infra_allocator);
        // Clear module-level ADT type info to avoid stale references.
        if (self.adt_type_info != null) {
            builtins_mod.clearAdtTypes();
            self.adt_type_info = null;
        }
    }

    /// Set atom name mapping (from compiler's atom table).
    pub fn setAtomNames(self: *VM, names: []const []const u8, allocator: Allocator) !void {
        for (names) |name| {
            try self.atom_names.append(allocator, name);
        }
    }

    /// Set ADT type info for pretty-printing ADT values.
    pub fn setAdtTypes(self: *VM, info: []const builtins_mod.AdtTypeInfo) void {
        self.adt_type_info = info;
        builtins_mod.setAdtTypes(info);
    }

    /// Register a heap-allocated object for cleanup.
    /// In GC mode, delegates to gc.trackObject (nursery list).
    /// In legacy mode, uses the VM's own linked list.
    fn trackObject(self: *VM, obj: *obj_mod.Obj) void {
        if (self.gc) |g| {
            g.trackObject(obj);
        } else {
            obj.next = self.objects;
            self.objects = obj;
        }
    }

    /// Register all heap objects from the compiled constant pools so the VM
    /// owns them for cleanup. Must be called once after initWithClosure.
    /// In GC mode, these are tracked as old-gen objects (long-lived).
    pub fn trackCompilerObjects(self: *Self, closure: *ObjClosure) void {
        self.trackConstantsRecursive(closure.function);
    }

    fn trackConstantsRecursive(self: *Self, func: *ObjFunction) void {
        for (func.chunk.constants.items) |val| {
            if (val.isObj()) {
                const obj_ptr = val.asObj();
                if (obj_ptr.obj_type == .function) {
                    // Recurse into nested functions but don't track the function
                    // object itself (CompileResult.deinit frees ObjFunction structs).
                    self.trackConstantsRecursive(ObjFunction.fromObj(obj_ptr));
                } else {
                    if (self.gc) |g| {
                        // GC mode: track as old-gen (long-lived compiler constants).
                        if (!obj_ptr.isOldGen()) {
                            g.trackOldObject(obj_ptr);
                        }
                    } else {
                        // Legacy mode: track in VM's linked list.
                        var cur = self.objects;
                        var found = false;
                        while (cur) |o| {
                            if (o == obj_ptr) {
                                found = true;
                                break;
                            }
                            cur = o.next;
                        }
                        if (!found) self.trackObject(obj_ptr);
                    }
                }
            }
        }
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

                // Phase 3 collection opcodes -- legacy mode.
                .op_list => {
                    const count = self.readU16Legacy();
                    try self.execOpList(count);
                },
                .op_map => {
                    const count = self.readU16Legacy();
                    try self.execOpMap(count);
                },
                .op_tuple => {
                    const count = self.readU16Legacy();
                    try self.execOpTuple(count);
                },
                .op_record => {
                    const count = self.readU16Legacy();
                    try self.execOpRecordLegacy(count);
                },
                .op_record_spread => {
                    const override_count = self.readByteLegacy();
                    try self.execOpRecordSpreadLegacy(override_count);
                },
                .op_get_field => {
                    const const_idx = self.readU16Legacy();
                    const field_val = self.chunk.?.constants.items[const_idx];
                    try self.execOpGetField(field_val);
                },
                .op_dup => {
                    const top = self.peek(0);
                    try self.push(top);
                },

                // Phase 3 ADT/pattern matching opcodes -- legacy mode.
                .op_adt_construct => {
                    const type_id = self.readU16Legacy();
                    const variant_idx = self.readU16Legacy();
                    const arity = self.readByteLegacy();
                    try self.execOpAdtConstruct(type_id, variant_idx, arity);
                },
                .op_adt_get_field => {
                    const field_idx = self.readByteLegacy();
                    try self.execOpAdtGetField(field_idx);
                },
                .op_check_tag => {
                    const type_id = self.readU16Legacy();
                    const variant_idx = self.readU16Legacy();
                    try self.execOpCheckTag(type_id, variant_idx);
                },
                .op_get_index => {
                    const index = self.readU16Legacy();
                    try self.execOpGetIndex(index);
                },
                .op_list_len => {
                    try self.execOpListLen();
                },
                .op_list_slice => {
                    const start = self.readU16Legacy();
                    try self.execOpListSlice(start);
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
                        const val = self.peek(0);
                        uv.location.* = val;
                        // Write barrier: upvalue (possibly old-gen) stores a new value.
                        if (self.gc) |g| {
                            g.writeBarrier(&uv.obj, val);
                        }
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

                // Phase 3 collection opcodes -- frame mode.
                .op_list => {
                    const count = self.readU16Frame();
                    try self.execOpList(count);
                },
                .op_map => {
                    const count = self.readU16Frame();
                    try self.execOpMap(count);
                },
                .op_tuple => {
                    const count = self.readU16Frame();
                    try self.execOpTuple(count);
                },
                .op_record => {
                    const count = self.readU16Frame();
                    try self.execOpRecordFrame(count);
                },
                .op_record_spread => {
                    const override_count = self.readByteFrame();
                    try self.execOpRecordSpreadFrame(override_count);
                },
                .op_get_field => {
                    const const_idx = self.readU16Frame();
                    const field_val = self.frameChunk().constants.items[const_idx];
                    try self.execOpGetField(field_val);
                },
                .op_dup => {
                    const top = self.peek(0);
                    try self.push(top);
                },

                // Phase 3 ADT/pattern matching opcodes -- frame mode.
                .op_adt_construct => {
                    const type_id = self.readU16Frame();
                    const variant_idx = self.readU16Frame();
                    const arity = self.readByteFrame();
                    try self.execOpAdtConstruct(type_id, variant_idx, arity);
                },
                .op_adt_get_field => {
                    const field_idx = self.readByteFrame();
                    try self.execOpAdtGetField(field_idx);
                },
                .op_check_tag => {
                    const type_id = self.readU16Frame();
                    const variant_idx = self.readU16Frame();
                    try self.execOpCheckTag(type_id, variant_idx);
                },
                .op_get_index => {
                    const index = self.readU16Frame();
                    try self.execOpGetIndex(index);
                },
                .op_list_len => {
                    try self.execOpListLen();
                },
                .op_list_slice => {
                    const start = self.readU16Frame();
                    try self.execOpListSlice(start);
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

        // Set VM pointer for higher-order builtins that need to invoke closures
        // and track intermediate heap objects.
        builtins_mod.setVM(@ptrCast(self), &callClosureFromBuiltin, &trackObjectFromBuiltin);
        defer builtins_mod.clearVM();

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
            // Only track if not already tracked (closures called from builtins
            // may return objects already registered via the VM's normal execution).
            const obj_ptr = result.asObj();
            var already_tracked = false;
            if (self.gc) |g| {
                var cur: ?*obj_mod.Obj = g.nursery_objects;
                while (cur) |o| {
                    if (o == obj_ptr) { already_tracked = true; break; }
                    cur = o.next;
                }
                if (!already_tracked) {
                    cur = g.old_objects;
                    while (cur) |o| {
                        if (o == obj_ptr) { already_tracked = true; break; }
                        cur = o.next;
                    }
                }
            } else {
                var cur = self.objects;
                while (cur) |o| {
                    if (o == obj_ptr) { already_tracked = true; break; }
                    cur = o.next;
                }
            }
            if (!already_tracked) self.trackObject(obj_ptr);
        }

        self.stack_top -= (@as(u32, arg_count) + 1);
        try self.push(result);
    }

    /// Callback function for builtins to register intermediate heap objects.
    fn trackObjectFromBuiltin(vm_ptr: *anyopaque, obj: *obj_mod.Obj) void {
        const vm: *Self = @ptrCast(@alignCast(vm_ptr));
        if (vm.gc) |g| {
            // GC mode: check nursery + old lists.
            var cur: ?*obj_mod.Obj = g.nursery_objects;
            while (cur) |o| {
                if (o == obj) return;
                cur = o.next;
            }
            cur = g.old_objects;
            while (cur) |o| {
                if (o == obj) return;
                cur = o.next;
            }
            g.trackObject(obj);
        } else {
            // Legacy mode: check VM's list.
            var cur = vm.objects;
            while (cur) |o| {
                if (o == obj) return;
                cur = o.next;
            }
            vm.trackObject(obj);
        }
    }

    /// Callback function for builtins to invoke user closures through the VM.
    /// This is passed to builtins via setVM() and called when List.map, etc.
    /// need to apply a user-provided function to values.
    fn callClosureFromBuiltin(vm_ptr: *anyopaque, closure_val: Value, cb_args: []const Value) ?Value {
        const vm: *Self = @ptrCast(@alignCast(vm_ptr));

        // Validate: must be a closure.
        if (!closure_val.isObj() or closure_val.asObj().obj_type != .closure) {
            vm.runtimeErrorAny(.E001, "expected a function argument") catch {};
            return null;
        }
        const closure = ObjClosure.fromObj(closure_val.asObj());

        // Save current frame count to detect when the closure returns.
        const saved_frame_count = vm.frame_count;

        // Push closure (callee slot) + arguments onto the stack.
        vm.push(closure_val) catch return null;
        for (cb_args) |arg| {
            vm.push(arg) catch return null;
        }

        // Set up the call frame.
        vm.callClosure(closure, @intCast(cb_args.len)) catch return null;

        // Run the VM until this frame returns.
        while (vm.frame_count > saved_frame_count) {
            const frame = vm.currentFrame();
            const code = frame.closure.function.chunk.code.items;

            if (frame.ip >= code.len) {
                // End of function code: implicit nil return.
                vm.frame_count -= 1;
                if (vm.frame_count > saved_frame_count) {
                    // Inner frame returned; push nil and continue.
                    const base = frame.base_slot;
                    vm.stack_top = base;
                    vm.push(Value.nil) catch return null;
                    continue;
                }
                // Our target frame returned.
                vm.stack_top = frame.base_slot;
                return Value.nil;
            }

            const opcode: OpCode = @enumFromInt(code[frame.ip]);
            frame.ip += 1;

            // Re-dispatch the single instruction.
            // For op_return, intercept to stop at our saved frame level.
            if (opcode == .op_return) {
                const ret = vm.pop() catch return null;
                vm.closeUpvalues(frame.base_slot);
                vm.frame_count -= 1;
                if (vm.frame_count <= saved_frame_count) {
                    // Our closure has returned. Restore stack.
                    vm.stack_top = frame.base_slot;
                    return ret;
                }
                // Nested function returned.
                vm.stack_top = frame.base_slot;
                vm.push(ret) catch return null;
                continue;
            }

            // For all other opcodes, rewind ip and dispatch via runSingleOp.
            frame.ip -= 1;
            vm.runSingleFrameOp() catch return null;
        }

        return Value.nil;
    }

    /// Execute a single opcode from the current frame (used by callClosureFromBuiltin).
    fn runSingleFrameOp(self: *Self) RuntimeError!void {
        const frame = self.currentFrame();
        const code = frame.closure.function.chunk.code.items;
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
                try self.runtimeErrorAny(.E002, "globals not supported");
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
                    const val = self.peek(0);
                    uv.location.* = val;
                    // Write barrier: upvalue (possibly old-gen) stores a new value.
                    if (self.gc) |g| {
                        g.writeBarrier(&uv.obj, val);
                    }
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
                // This should be handled by callClosureFromBuiltin, but just in case:
                const ret = try self.pop();
                self.closeUpvalues(frame.base_slot);
                self.frame_count -= 1;
                self.stack_top = frame.base_slot;
                try self.push(ret);
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
            .op_list => {
                const count = self.readU16Frame();
                try self.execOpList(count);
            },
            .op_map => {
                const count = self.readU16Frame();
                try self.execOpMap(count);
            },
            .op_tuple => {
                const count = self.readU16Frame();
                try self.execOpTuple(count);
            },
            .op_record => {
                const count = self.readU16Frame();
                try self.execOpRecordFrame(count);
            },
            .op_record_spread => {
                const override_count = self.readByteFrame();
                try self.execOpRecordSpreadFrame(override_count);
            },
            .op_get_field => {
                const const_idx = self.readU16Frame();
                const field_val = self.frameChunk().constants.items[const_idx];
                try self.execOpGetField(field_val);
            },
            .op_dup => {
                const top = self.peek(0);
                try self.push(top);
            },
            .op_adt_construct => {
                const type_id = self.readU16Frame();
                const variant_idx = self.readU16Frame();
                const arity = self.readByteFrame();
                try self.execOpAdtConstruct(type_id, variant_idx, arity);
            },
            .op_adt_get_field => {
                const field_idx = self.readByteFrame();
                try self.execOpAdtGetField(field_idx);
            },
            .op_check_tag => {
                const type_id = self.readU16Frame();
                const variant_idx = self.readU16Frame();
                try self.execOpCheckTag(type_id, variant_idx);
            },
            .op_get_index => {
                const index = self.readU16Frame();
                try self.execOpGetIndex(index);
            },
            .op_list_len => {
                try self.execOpListLen();
            },
            .op_list_slice => {
                const start = self.readU16Frame();
                try self.execOpListSlice(start);
            },
        }
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
            // Write barrier: upvalue (possibly old-gen) now stores a value
            // that may reference a nursery object.
            if (self.gc) |g| {
                g.writeBarrier(&uv.obj, uv.closed);
            }
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

    // ── Collection opcode execution (Phase 3) ─────────────────────────

    const ObjList = obj_mod.ObjList;
    const ObjMap = obj_mod.ObjMap;
    const ObjTuple = obj_mod.ObjTuple;
    const ObjRecord = obj_mod.ObjRecord;

    fn execOpList(self: *Self, count: u16) RuntimeError!void {
        const lst = try ObjList.create(self.allocator);
        self.trackObject(&lst.obj);

        // Reserve capacity and fill from stack in order (first pushed = index 0).
        try lst.items.ensureTotalCapacity(self.allocator, count);
        const base = self.stack_top - count;
        var i: u16 = 0;
        while (i < count) : (i += 1) {
            lst.items.appendAssumeCapacity(self.stack[base + i]);
        }
        self.stack_top -= count;

        try self.push(Value.fromObj(&lst.obj));
    }

    fn execOpMap(self: *Self, pair_count: u16) RuntimeError!void {
        const m = try ObjMap.create(self.allocator);
        self.trackObject(&m.obj);

        // Stack has: key0, val0, key1, val1, ... (2*pair_count values).
        const total: u16 = pair_count * 2;
        const base = self.stack_top - total;
        var i: u16 = 0;
        while (i < pair_count) : (i += 1) {
            const key = self.stack[base + i * 2];
            const val = self.stack[base + i * 2 + 1];
            try m.entries.put(self.allocator, key, val);
        }
        self.stack_top -= total;

        try self.push(Value.fromObj(&m.obj));
    }

    fn execOpTuple(self: *Self, count: u16) RuntimeError!void {
        // Collect values from stack.
        const values = try self.allocator.alloc(Value, count);
        var i: u16 = 0;
        while (i < count) : (i += 1) {
            values[i] = self.stack[self.stack_top - count + i];
        }
        self.stack_top -= count;

        const t = try ObjTuple.create(self.allocator, values);
        self.allocator.free(values); // ObjTuple.create copies the slice
        self.trackObject(&t.obj);

        try self.push(Value.fromObj(&t.obj));
    }

    /// Execute op_record in legacy mode: read field name const indices from chunk.
    fn execOpRecordLegacy(self: *Self, count: u16) RuntimeError!void {
        // Read field name constant indices from bytecode.
        const names = try self.allocator.alloc([]const u8, count);
        defer self.allocator.free(names);

        var i: u16 = 0;
        while (i < count) : (i += 1) {
            const ci = self.readU16Legacy();
            const name_val = self.chunk.?.constants.items[ci];
            if (name_val.isObj() and name_val.asObj().obj_type == .string) {
                names[i] = ObjString.fromObj(name_val.asObj()).bytes;
            } else {
                names[i] = "<unknown>";
            }
        }

        try self.createRecord(count, names);
    }

    /// Execute op_record in frame mode: read field name const indices from frame chunk.
    fn execOpRecordFrame(self: *Self, count: u16) RuntimeError!void {
        const names = try self.allocator.alloc([]const u8, count);
        defer self.allocator.free(names);

        var i: u16 = 0;
        while (i < count) : (i += 1) {
            const ci = self.readU16Frame();
            const name_val = self.frameChunk().constants.items[ci];
            if (name_val.isObj() and name_val.asObj().obj_type == .string) {
                names[i] = ObjString.fromObj(name_val.asObj()).bytes;
            } else {
                names[i] = "<unknown>";
            }
        }

        try self.createRecord(count, names);
    }

    /// Shared record creation helper.
    fn createRecord(self: *Self, count: u16, names: []const []const u8) RuntimeError!void {
        // Pop field values from stack (in order).
        const values = try self.allocator.alloc(Value, count);
        defer self.allocator.free(values);

        var i: u16 = 0;
        while (i < count) : (i += 1) {
            values[i] = self.stack[self.stack_top - count + i];
        }
        self.stack_top -= count;

        const rec = try ObjRecord.create(self.allocator, names, values);
        self.trackObject(&rec.obj);

        try self.push(Value.fromObj(&rec.obj));
    }

    /// Execute op_record_spread in legacy mode.
    fn execOpRecordSpreadLegacy(self: *Self, override_count: u8) RuntimeError!void {
        const names = try self.allocator.alloc([]const u8, override_count);
        defer self.allocator.free(names);

        var i: u8 = 0;
        while (i < override_count) : (i += 1) {
            const ci = self.readU16Legacy();
            const name_val = self.chunk.?.constants.items[ci];
            if (name_val.isObj() and name_val.asObj().obj_type == .string) {
                names[i] = ObjString.fromObj(name_val.asObj()).bytes;
            } else {
                names[i] = "<unknown>";
            }
        }

        try self.execRecordSpread(override_count, names);
    }

    /// Execute op_record_spread in frame mode.
    fn execOpRecordSpreadFrame(self: *Self, override_count: u8) RuntimeError!void {
        const names = try self.allocator.alloc([]const u8, override_count);
        defer self.allocator.free(names);

        var i: u8 = 0;
        while (i < override_count) : (i += 1) {
            const ci = self.readU16Frame();
            const name_val = self.frameChunk().constants.items[ci];
            if (name_val.isObj() and name_val.asObj().obj_type == .string) {
                names[i] = ObjString.fromObj(name_val.asObj()).bytes;
            } else {
                names[i] = "<unknown>";
            }
        }

        try self.execRecordSpread(override_count, names);
    }

    /// Shared record spread execution.
    fn execRecordSpread(self: *Self, override_count: u8, override_names: []const []const u8) RuntimeError!void {
        // Stack: [base_record, override_val_0, override_val_1, ...]
        // Pop override values.
        const override_values = try self.allocator.alloc(Value, override_count);
        defer self.allocator.free(override_values);

        var i: u8 = 0;
        while (i < override_count) : (i += 1) {
            override_values[override_count - 1 - i] = try self.pop();
        }

        // Pop base record.
        const base_val = try self.pop();
        if (!base_val.isObj() or base_val.asObj().obj_type != .record) {
            try self.runtimeErrorAny(.E001, "spread base must be a record");
            return error.RuntimeErr;
        }
        const base = ObjRecord.fromObj(base_val.asObj());

        // Build new record: start with base fields, override matching ones, add new ones.
        // Compute total field count.
        var total: usize = base.field_count;
        for (override_names) |oname| {
            var found = false;
            for (base.field_names[0..base.field_count]) |bname| {
                if (std.mem.eql(u8, oname, bname)) {
                    found = true;
                    break;
                }
            }
            if (!found) total += 1;
        }

        const new_names = try self.allocator.alloc([]const u8, total);
        defer self.allocator.free(new_names);
        const new_values = try self.allocator.alloc(Value, total);
        defer self.allocator.free(new_values);

        // Copy base fields.
        var idx: usize = 0;
        for (0..base.field_count) |fi| {
            new_names[idx] = base.field_names[fi];
            new_values[idx] = base.field_values[fi];
            // Check if this field is overridden.
            for (override_names, 0..) |oname, oi| {
                if (std.mem.eql(u8, oname, base.field_names[fi])) {
                    new_values[idx] = override_values[oi];
                    break;
                }
            }
            idx += 1;
        }

        // Add new fields not in base.
        for (override_names, 0..) |oname, oi| {
            var found = false;
            for (base.field_names[0..base.field_count]) |bname| {
                if (std.mem.eql(u8, oname, bname)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                new_names[idx] = oname;
                new_values[idx] = override_values[oi];
                idx += 1;
            }
        }

        const rec = try ObjRecord.create(self.allocator, new_names[0..total], new_values[0..total]);
        self.trackObject(&rec.obj);

        try self.push(Value.fromObj(&rec.obj));
    }

    /// Execute op_get_field: pop object, look up field by name, push result.
    fn execOpGetField(self: *Self, field_val: Value) RuntimeError!void {
        const obj_val = try self.pop();

        // Get field name string from the constant.
        const field_name = if (field_val.isObj() and field_val.asObj().obj_type == .string)
            ObjString.fromObj(field_val.asObj()).bytes
        else
            return error.RuntimeErr;

        if (obj_val.isObj()) {
            const obj_ptr = obj_val.asObj();
            switch (obj_ptr.obj_type) {
                .record => {
                    const rec = ObjRecord.fromObj(obj_ptr);
                    for (rec.field_names[0..rec.field_count], 0..) |name, idx| {
                        if (std.mem.eql(u8, name, field_name)) {
                            try self.push(rec.field_values[idx]);
                            return;
                        }
                    }
                    // Field not found: push nil.
                    try self.push(Value.nil);
                },
                .map => {
                    const m = ObjMap.fromObj(obj_ptr);
                    if (m.entries.get(field_val)) |val| {
                        try self.push(val);
                    } else {
                        try self.push(Value.nil);
                    }
                },
                else => {
                    try self.runtimeErrorAny(.E001, "field access on non-record/map value");
                    return error.RuntimeErr;
                },
            }
        } else {
            try self.runtimeErrorAny(.E001, "field access on non-object value");
            return error.RuntimeErr;
        }
    }

    // ── ADT and pattern matching opcodes ─────────────────────────────

    const ObjAdt = obj_mod.ObjAdt;

    fn execOpAdtConstruct(self: *Self, type_id: u16, variant_idx: u16, arity: u8) RuntimeError!void {
        // Pop arity values from stack into payload.
        const payload = try self.allocator.alloc(Value, arity);
        defer self.allocator.free(payload);

        // Pop in reverse order to match push order.
        var i: u8 = arity;
        while (i > 0) {
            i -= 1;
            payload[i] = try self.pop();
        }

        const adt = try ObjAdt.create(self.allocator, type_id, variant_idx, payload);
        self.trackObject(&adt.obj);
        try self.push(Value.fromObj(&adt.obj));
    }

    fn execOpAdtGetField(self: *Self, field_idx: u8) RuntimeError!void {
        const val = try self.pop();
        if (!val.isObj() or val.asObj().obj_type != .adt) {
            try self.runtimeErrorAny(.E001, "op_adt_get_field: expected ADT value");
            return error.RuntimeErr;
        }
        const adt = ObjAdt.fromObj(val.asObj());
        if (field_idx >= adt.payload.len) {
            try self.runtimeErrorAny(.E001, "ADT field index out of bounds");
            return error.RuntimeErr;
        }
        try self.push(adt.payload[field_idx]);
    }

    fn execOpCheckTag(self: *Self, type_id: u16, variant_idx: u16) RuntimeError!void {
        // Peek top of stack (don't pop).
        const val = self.peek(0);
        if (val.isObj() and val.asObj().obj_type == .adt) {
            const adt = ObjAdt.fromObj(val.asObj());
            if (adt.type_id == type_id and adt.variant_idx == variant_idx) {
                try self.push(Value.true_val);
                return;
            }
        }
        try self.push(Value.false_val);
    }

    fn execOpGetIndex(self: *Self, index: u16) RuntimeError!void {
        const val = try self.pop();
        if (val.isObj()) {
            const obj_ptr = val.asObj();
            switch (obj_ptr.obj_type) {
                .list => {
                    const lst = ObjList.fromObj(obj_ptr);
                    if (index >= lst.items.items.len) {
                        try self.push(Value.nil);
                    } else {
                        try self.push(lst.items.items[index]);
                    }
                },
                .tuple => {
                    const t = ObjTuple.fromObj(obj_ptr);
                    if (index >= t.fields.len) {
                        try self.push(Value.nil);
                    } else {
                        try self.push(t.fields[index]);
                    }
                },
                else => {
                    try self.runtimeErrorAny(.E001, "op_get_index: expected list or tuple");
                    return error.RuntimeErr;
                },
            }
        } else {
            try self.runtimeErrorAny(.E001, "op_get_index: expected object");
            return error.RuntimeErr;
        }
    }

    fn execOpListLen(self: *Self) RuntimeError!void {
        const val = try self.pop();
        if (!val.isObj() or val.asObj().obj_type != .list) {
            try self.runtimeErrorAny(.E001, "op_list_len: expected list");
            return error.RuntimeErr;
        }
        const lst = ObjList.fromObj(val.asObj());
        try self.push(Value.fromInt(@intCast(lst.items.items.len)));
    }

    fn execOpListSlice(self: *Self, start: u16) RuntimeError!void {
        const val = try self.pop();
        if (!val.isObj() or val.asObj().obj_type != .list) {
            try self.runtimeErrorAny(.E001, "op_list_slice: expected list");
            return error.RuntimeErr;
        }
        const src = ObjList.fromObj(val.asObj());
        const new_list = try ObjList.create(self.allocator);
        self.trackObject(&new_list.obj);

        if (start < src.items.items.len) {
            const slice = src.items.items[start..];
            try new_list.items.appendSlice(self.allocator, slice);
        }

        try self.push(Value.fromObj(&new_list.obj));
    }

    // ── Output ────────────────────────────────────────────────────────

    fn printValue(self: *Self, val: Value) !void {
        const text = try builtins_mod.formatValue(val, self.infra_allocator, if (self.atom_names.items.len > 0) self.atom_names.items else null);
        defer self.infra_allocator.free(text);

        if (self.output_buf) |buf| {
            try buf.appendSlice(self.infra_allocator, text);
            try buf.append(self.infra_allocator, '\n');
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
        try self.errors.append(self.infra_allocator, .{
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
        try self.errors.append(self.infra_allocator, .{
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
    const s = try ObjString.create(allocator, "hello", null);
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
    const s = try ObjString.create(allocator, "hello", null);
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
    const s1 = try ObjString.create(allocator, "hello", null);
    const s2 = try ObjString.create(allocator, " world", null);
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

// ── Phase 3: ADT and pattern matching opcode tests ────────────────────

test "vm: op_adt_construct creates ADT value" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();

    // Construct an ADT: type_id=2, variant_idx=0, arity=0 (nullary)
    try tc.emitOp(.op_adt_construct);
    try tc.emit(0); try tc.emit(2); // type_id = 2
    try tc.emit(0); try tc.emit(0); // variant_idx = 0
    try tc.emit(0); // arity = 0
    try tc.emitReturn();

    var vm = VM.init(&tc.chunk, allocator);
    defer vm.deinit();
    const result = try vm.run();
    try std.testing.expect(result.isObjType(.adt));
    const adt = obj_mod.ObjAdt.fromObj(result.asObj());
    try std.testing.expectEqual(@as(u16, 2), adt.type_id);
    try std.testing.expectEqual(@as(u16, 0), adt.variant_idx);
    try std.testing.expectEqual(@as(usize, 0), adt.payload.len);
}

test "vm: op_adt_construct with payload" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();

    // Push payload value, then construct ADT with arity=1
    try tc.emitConstant(Value.fromInt(42));
    try tc.emitOp(.op_adt_construct);
    try tc.emit(0); try tc.emit(0); // type_id = 0 (Option)
    try tc.emit(0); try tc.emit(0); // variant_idx = 0 (Some)
    try tc.emit(1); // arity = 1
    try tc.emitReturn();

    var vm = VM.init(&tc.chunk, allocator);
    defer vm.deinit();
    const result = try vm.run();
    try std.testing.expect(result.isObjType(.adt));
    const adt = obj_mod.ObjAdt.fromObj(result.asObj());
    try std.testing.expectEqual(@as(u16, 0), adt.type_id);
    try std.testing.expectEqual(@as(u16, 0), adt.variant_idx);
    try std.testing.expectEqual(@as(usize, 1), adt.payload.len);
    try std.testing.expectEqual(@as(i32, 42), adt.payload[0].asInt());
}

test "vm: op_check_tag matches correctly" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();

    // Construct ADT type_id=1, variant=0, then check tag
    try tc.emitOp(.op_adt_construct);
    try tc.emit(0); try tc.emit(1); // type_id = 1
    try tc.emit(0); try tc.emit(0); // variant_idx = 0
    try tc.emit(0); // arity = 0

    // Check for matching tag
    try tc.emitOp(.op_check_tag);
    try tc.emit(0); try tc.emit(1); // type_id = 1
    try tc.emit(0); try tc.emit(0); // variant_idx = 0
    try tc.emitReturn();

    var vm = VM.init(&tc.chunk, allocator);
    defer vm.deinit();
    const result = try vm.run();
    try std.testing.expect(result.asBool());
}

test "vm: op_check_tag rejects wrong variant" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();

    // Construct ADT type_id=1, variant=0
    try tc.emitOp(.op_adt_construct);
    try tc.emit(0); try tc.emit(1);
    try tc.emit(0); try tc.emit(0);
    try tc.emit(0);

    // Check for variant 1 (should fail)
    try tc.emitOp(.op_check_tag);
    try tc.emit(0); try tc.emit(1);
    try tc.emit(0); try tc.emit(1); // variant_idx = 1 (different)
    try tc.emitReturn();

    var vm = VM.init(&tc.chunk, allocator);
    defer vm.deinit();
    const result = try vm.run();
    try std.testing.expect(!result.asBool());
}

test "vm: op_adt_get_field extracts payload" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();

    // Construct ADT with payload [42, 99]
    try tc.emitConstant(Value.fromInt(42));
    try tc.emitConstant(Value.fromInt(99));
    try tc.emitOp(.op_adt_construct);
    try tc.emit(0); try tc.emit(0);
    try tc.emit(0); try tc.emit(0);
    try tc.emit(2); // arity = 2

    // Get field 1
    try tc.emitOp(.op_adt_get_field);
    try tc.emit(1);
    try tc.emitReturn();

    var vm = VM.init(&tc.chunk, allocator);
    defer vm.deinit();
    const result = try vm.run();
    try std.testing.expectEqual(@as(i32, 99), result.asInt());
}

test "vm: op_dup duplicates stack top" {
    const allocator = std.testing.allocator;
    var tc = TestChunk.init(allocator);
    defer tc.deinit();

    try tc.emitConstant(Value.fromInt(7));
    try tc.emitOp(.op_dup);
    try tc.emitOp(.op_add);
    try tc.emitReturn();

    const result = try tc.run();
    try std.testing.expectEqual(@as(i32, 14), result.asInt());
}
