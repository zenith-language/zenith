const std = @import("std");
const Allocator = std.mem.Allocator;
const ast_mod = @import("ast");
const Ast = ast_mod.Ast;
const Node = Ast.Node;
const token_mod = @import("token");
const Token = token_mod.Token;
const Tag = token_mod.Tag;
const chunk_mod = @import("chunk");
const Chunk = chunk_mod.Chunk;
const OpCode = chunk_mod.OpCode;
const value_mod = @import("value");
const Value = value_mod.Value;
const obj_mod = @import("obj");
const ObjString = obj_mod.ObjString;
const ObjFunction = obj_mod.ObjFunction;
const ObjClosure = obj_mod.ObjClosure;
const error_mod = @import("error");
const Diagnostic = error_mod.Diagnostic;
const ErrorCode = error_mod.ErrorCode;
const Label = error_mod.Label;

/// Result of compilation.
pub const CompileResult = struct {
    closure: *ObjClosure,
    errors: std.ArrayListUnmanaged(Diagnostic),
    atom_table: std.StringHashMapUnmanaged(u32),
    atom_count: u32,

    pub fn deinit(self: *CompileResult, allocator: Allocator) void {
        // Recursively destroy the closure and all nested functions.
        destroyFunction(self.closure.function, allocator);
        allocator.free(self.closure.upvalues);
        allocator.destroy(self.closure);
        self.errors.deinit(allocator);
        self.atom_table.deinit(allocator);
    }

    /// Recursively destroy an ObjFunction and its nested function constants.
    fn destroyFunction(func: *ObjFunction, allocator: Allocator) void {
        // First, recursively destroy any nested ObjFunction constants.
        for (func.chunk.constants.items) |val| {
            if (val.isObj()) {
                const obj_ptr = val.asObj();
                if (obj_ptr.obj_type == .function) {
                    destroyFunction(ObjFunction.fromObj(obj_ptr), allocator);
                } else {
                    obj_ptr.destroy(allocator);
                }
            }
        }
        // Deinit the chunk (code, constants list, lines, etc.) but constants already freed above.
        func.chunk.code.deinit(allocator);
        func.chunk.constants.deinit(allocator);
        func.chunk.lines.deinit(allocator);
        for (func.chunk.owned_strings.items) |s| allocator.free(s);
        func.chunk.owned_strings.deinit(allocator);
        if (func.chunk.owns_atom_names) {
            for (func.chunk.atom_names.items) |n| allocator.free(n);
        }
        func.chunk.atom_names.deinit(allocator);
        if (func.chunk.owns_name) allocator.free(func.chunk.name);
        // Free param metadata.
        if (func.param_names) |names| allocator.free(names);
        if (func.param_defaults) |defaults| {
            for (defaults) |val| {
                if (val.isObj()) val.asObj().destroy(allocator);
            }
            allocator.free(defaults);
        }
        allocator.destroy(func);
    }

    pub fn hasErrors(self: *const CompileResult) bool {
        for (self.errors.items) |d| {
            if (d.severity == .@"error") return true;
        }
        return false;
    }

    /// Access the top-level chunk (convenience for backward-compat paths).
    pub fn getChunk(self: *const CompileResult) *const Chunk {
        return &self.closure.function.chunk;
    }
};

/// Function type: script (top-level), function (named fn), lambda (anonymous).
const FnType = enum { script, function, lambda };

/// Upvalue descriptor -- compile-time tracking of captured variables.
const UpvalueDesc = struct {
    index: u8, // local slot (if is_local) or parent upvalue index
    is_local: bool, // true = captures parent local; false = captures parent upvalue
};

/// Local variable tracking.
const Local = struct {
    name: []const u8,
    depth: i32,
    is_captured: bool,
};

/// Bytecode compiler -- walks AST nodes and emits opcodes into a Chunk.
/// Supports nested compilation for function bodies with upvalue resolution.
pub const Compiler = struct {
    function: *ObjFunction,
    fn_type: FnType,
    ast: *const Ast,
    locals: [256]Local,
    local_count: u8,
    scope_depth: i32,
    upvalues: [256]UpvalueDesc,
    parent: ?*Compiler,
    is_tail_position: bool,
    atom_table: std.StringHashMapUnmanaged(u32),
    atom_count: u32,
    errors: std.ArrayListUnmanaged(Diagnostic),
    allocator: Allocator,

    const Self = @This();

    /// Explicit error set for recursive compilation functions (Zig 0.15 requirement).
    pub const Error = error{Overflow} || Allocator.Error;

    // Names of built-in functions.
    const builtin_names = [_][]const u8{
        "print", "str", "len", "type_of", "assert", "panic", "range", "show",
    };

    // Builtin type atom names, pre-registered at IDs 0-6 to match
    // the hardcoded return values from type_of().
    const type_atom_names = [_][]const u8{
        "int", "float", "bool", "nil", "string", "bytes", "atom",
    };

    /// Compile an AST into bytecodes wrapped in an ObjClosure.
    pub fn compile(ast_ptr: *const Ast, allocator: Allocator) !CompileResult {
        // Create the top-level script function.
        const func = try ObjFunction.create(allocator);
        func.name = null; // script has no name

        var self = Self{
            .function = func,
            .fn_type = .script,
            .ast = ast_ptr,
            .locals = undefined,
            .local_count = 0,
            .scope_depth = 0,
            .upvalues = undefined,
            .parent = null,
            .is_tail_position = false,
            .atom_table = .{},
            .atom_count = 0,
            .errors = .empty,
            .allocator = allocator,
        };

        // Reserve slot 0 for the script function itself.
        self.locals[0] = .{ .name = "", .depth = 0, .is_captured = false };
        self.local_count = 1;

        // Pre-register builtin type atom names at fixed IDs 0-6.
        for (type_atom_names) |name| {
            try self.atom_table.put(allocator, name, self.atom_count);
            self.atom_count += 1;
        }

        // Find and compile the root node (last node in the AST).
        const root_idx: u32 = @intCast(ast_ptr.nodes.len - 1);
        const root_tag = ast_ptr.nodes.items(.tag)[root_idx];
        if (root_tag == .root) {
            const data = ast_ptr.nodes.items(.data)[root_idx];
            const stmts = ast_ptr.extra_data.items[data.lhs..data.rhs];
            try self.compileStatements(stmts);
        }

        // Emit nil + return at end of script.
        try self.emitOp(.op_nil, 0);
        try self.emitOp(.op_return, 0);

        // Wrap in ObjClosure.
        const closure = try ObjClosure.create(allocator, func);

        return .{
            .closure = closure,
            .errors = self.errors,
            .atom_table = self.atom_table,
            .atom_count = self.atom_count,
        };
    }

    /// Get the current chunk being compiled (the function's chunk).
    fn currentChunk(self: *Self) *Chunk {
        return &self.function.chunk;
    }

    // ── Statement compilation ─────────────────────────────────────────

    fn compileStatements(self: *Self, stmts: []const u32) Error!void {
        for (stmts, 0..) |stmt_idx, i| {
            try self.compileNode(stmt_idx);
            // Pop intermediate statement values (all except the last in a block).
            const tag = self.ast.nodes.items(.tag)[stmt_idx];
            if (i < stmts.len - 1) {
                // Expression statements that leave a value on stack need pop.
                if (tag == .expr_stmt) {
                    // expr_stmt already does not leave a value (it pops internally)
                } else if (tag == .while_stmt or tag == .for_stmt) {
                    // while and for emit op_nil as their result -- pop it when not last.
                    try self.emitOp(.op_pop, self.getLine(stmt_idx));
                } else if (tag != .let_decl and tag != .assign_stmt and tag != .fn_decl) {
                    try self.emitOp(.op_pop, self.getLine(stmt_idx));
                }
            }
        }
    }

    fn compileNode(self: *Self, node_idx: u32) Error!void {
        const tags = self.ast.nodes.items(.tag);
        const data = self.ast.nodes.items(.data);
        const tag = tags[node_idx];
        const node_data = data[node_idx];

        switch (tag) {
            .int_literal => try self.compileIntLiteral(node_idx),
            .float_literal => try self.compileFloatLiteral(node_idx),
            .string_literal => try self.compileStringLiteral(node_idx),
            .bool_literal => try self.compileBoolLiteral(node_idx),
            .nil_literal => try self.emitOp(.op_nil, self.getLine(node_idx)),
            .atom_literal => try self.compileAtomLiteral(node_idx),
            .identifier => try self.compileIdentifier(node_idx),

            .negate => {
                try self.compileNode(node_data.lhs);
                try self.emitOp(.op_negate, self.getLine(node_idx));
            },
            .logical_not => {
                try self.compileNode(node_data.lhs);
                try self.emitOp(.op_not, self.getLine(node_idx));
            },

            .add => try self.compileBinaryOp(node_data, .op_add, node_idx),
            .subtract => try self.compileBinaryOp(node_data, .op_subtract, node_idx),
            .multiply => try self.compileBinaryOp(node_data, .op_multiply, node_idx),
            .divide => try self.compileBinaryOp(node_data, .op_divide, node_idx),
            .modulo => try self.compileBinaryOp(node_data, .op_modulo, node_idx),
            .equal => try self.compileBinaryOp(node_data, .op_equal, node_idx),
            .not_equal => try self.compileBinaryOp(node_data, .op_not_equal, node_idx),
            .less => try self.compileBinaryOp(node_data, .op_less, node_idx),
            .greater => try self.compileBinaryOp(node_data, .op_greater, node_idx),
            .less_equal => try self.compileBinaryOp(node_data, .op_less_equal, node_idx),
            .greater_equal => try self.compileBinaryOp(node_data, .op_greater_equal, node_idx),
            .concat => try self.compileBinaryOp(node_data, .op_concat, node_idx),

            .logical_and => try self.compileLogicalAnd(node_data, node_idx),
            .logical_or => try self.compileLogicalOr(node_data, node_idx),

            .let_decl => try self.compileLetDecl(node_data, node_idx),
            .assign_stmt => try self.compileAssignStmt(node_data, node_idx),
            .expr_stmt => {
                try self.compileNode(node_data.lhs);
                try self.emitOp(.op_pop, self.getLine(node_idx));
            },
            .if_expr => try self.compileIfExpr(node_data, node_idx),
            .while_stmt => try self.compileWhileStmt(node_data, node_idx),
            .for_stmt => try self.compileForStmt(node_data, node_idx),
            .block_expr => try self.compileBlockExpr(node_data, node_idx),
            .call_expr => try self.compileCallExpr(node_data, node_idx),
            .grouped_expr => try self.compileNode(node_data.lhs),

            // Phase 2 function/closure nodes.
            .fn_decl => try self.compileFnDecl(node_data, node_idx),
            .lambda_expr => try self.compileLambdaExpr(node_data, node_idx),
            .pipe_expr => try self.compilePipeExpr(node_data, node_idx),
            .return_expr => try self.compileReturnExpr(node_data, node_idx),
            .named_arg => {
                // Named args are handled inside compileCallExpr; should not appear standalone.
                try self.emitError(node_idx, .E005, "unexpected named argument outside of function call");
            },

            // Phase 3 collection literals and dot access.
            .list_literal => try self.compileListLiteral(node_data, node_idx),
            .map_literal => try self.compileMapLiteral(node_data, node_idx),
            .tuple_literal => try self.compileTupleLiteral(node_data, node_idx),
            .record_literal => try self.compileRecordLiteral(node_data, node_idx),
            .record_spread => try self.compileRecordSpread(node_data, node_idx),
            .field_access => try self.compileFieldAccess(node_data, node_idx),

            // Phase 3 ADT/pattern matching nodes -- compilation implemented in later plans.
            .type_decl,
            .adt_constructor,
            .match_expr,
            .match_arm,
            .match_arm_guarded,
            .pattern_wildcard,
            .pattern_literal,
            .pattern_binding,
            .pattern_adt,
            .pattern_list,
            .pattern_tuple,
            .pattern_record,
            .pattern_rest,
            => {
                try self.emitError(node_idx, .E005, "not yet implemented");
            },

            .root => {}, // handled above
        }
    }

    // ── Function declaration compilation ──────────────────────────────

    fn compileFnDecl(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        const extra_idx = node_data.lhs;
        const ed = self.ast.extra_data.items;
        const name_tok = ed[extra_idx];
        const param_start = ed[extra_idx + 1];
        const param_end = ed[extra_idx + 2];
        const body_node = ed[extra_idx + 3];
        const defaults_start = ed[extra_idx + 4];
        const defaults_end = ed[extra_idx + 5];

        const param_tokens = ed[param_start..param_end];
        const default_nodes = ed[defaults_start..defaults_end];

        // Determine function name.
        const fn_name: ?[]const u8 = if (name_tok != Node.null_node)
            self.ast.tokenSlice(name_tok)
        else
            null;

        // Create the ObjFunction.
        const func = try ObjFunction.create(self.allocator);
        func.name = fn_name;

        const param_count: u8 = @intCast(param_tokens.len);
        const required_params: u8 = @intCast(param_tokens.len - default_nodes.len);
        func.arity = required_params;
        func.arity_max = param_count;

        // Store param names in ObjFunction.
        if (param_count > 0) {
            const names = try self.allocator.alloc([]const u8, param_count);
            for (param_tokens, 0..) |ptok, i| {
                names[i] = self.ast.tokenSlice(ptok);
            }
            func.param_names = names;
        }

        // Compile default values into constants and store in ObjFunction.
        if (default_nodes.len > 0) {
            const defaults = try self.allocator.alloc(Value, default_nodes.len);
            for (default_nodes, 0..) |def_node, i| {
                defaults[i] = try self.evaluateConstantExpr(def_node);
            }
            func.param_defaults = defaults;
        }

        // Create child compiler.
        var child = Self{
            .function = func,
            .fn_type = .function,
            .ast = self.ast,
            .locals = undefined,
            .local_count = 0,
            .scope_depth = 0,
            .upvalues = undefined,
            .parent = self,
            .is_tail_position = false,
            .atom_table = self.atom_table,
            .atom_count = self.atom_count,
            .errors = self.errors,
            .allocator = self.allocator,
        };

        // Reserve slot 0 for the function itself (enables recursion).
        child.locals[0] = .{ .name = fn_name orelse "", .depth = 0, .is_captured = false };
        child.local_count = 1;

        // Add parameters as locals (slots 1..N).
        child.beginScope();
        for (param_tokens) |ptok| {
            const pname = self.ast.tokenSlice(ptok);
            child.locals[child.local_count] = .{ .name = pname, .depth = child.scope_depth, .is_captured = false };
            child.local_count += 1;
        }

        // Compile body -- the body is a block_expr node.
        // Set tail position for the last expression in the body.
        try child.compileNodeInTailPosition(body_node);

        // If the last instruction is not a return, emit implicit return.
        // The body's last expression value is already on the stack.
        if (!child.lastInstructionIsReturn()) {
            try child.emitOp(.op_return, self.getLine(node_idx));
        }

        // Copy state back to parent.
        // func.upvalue_count is already maintained by addUpvalue.
        self.atom_table = child.atom_table;
        self.atom_count = child.atom_count;
        self.errors = child.errors;

        // In parent: emit op_closure with function constant.
        const const_idx = try self.currentChunk().addConstant(Value.fromObj(&func.obj), self.allocator);
        try self.emitOp(.op_closure, self.getLine(node_idx));
        try self.emitByte(@intCast(const_idx), self.getLine(node_idx));

        // Emit upvalue descriptors.
        var i: u8 = 0;
        while (i < func.upvalue_count) : (i += 1) {
            try self.emitByte(if (child.upvalues[i].is_local) 1 else 0, self.getLine(node_idx));
            try self.emitByte(child.upvalues[i].index, self.getLine(node_idx));
        }

        // Add function name as a local in parent scope (like let binding).
        if (fn_name) |fname| {
            if (self.local_count >= 255) {
                try self.emitError(node_idx, .E009, "too many local variables in scope");
                return;
            }
            self.locals[self.local_count] = .{ .name = fname, .depth = self.scope_depth, .is_captured = false };
            self.local_count += 1;
        }
    }

    // ── Lambda expression compilation ─────────────────────────────────

    fn compileLambdaExpr(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        const extra_idx = node_data.lhs;
        const ed = self.ast.extra_data.items;
        const param_start = ed[extra_idx];
        const param_end = ed[extra_idx + 1];
        const body_node = ed[extra_idx + 2];

        const param_tokens = ed[param_start..param_end];

        // Create the ObjFunction for the lambda.
        const func = try ObjFunction.create(self.allocator);
        func.name = null; // lambdas are anonymous
        const param_count: u8 = @intCast(param_tokens.len);
        func.arity = param_count;
        func.arity_max = param_count;

        // Store param names.
        if (param_count > 0) {
            const names = try self.allocator.alloc([]const u8, param_count);
            for (param_tokens, 0..) |ptok, i| {
                names[i] = self.ast.tokenSlice(ptok);
            }
            func.param_names = names;
        }

        // Create child compiler.
        var child = Self{
            .function = func,
            .fn_type = .lambda,
            .ast = self.ast,
            .locals = undefined,
            .local_count = 0,
            .scope_depth = 0,
            .upvalues = undefined,
            .parent = self,
            .is_tail_position = false,
            .atom_table = self.atom_table,
            .atom_count = self.atom_count,
            .errors = self.errors,
            .allocator = self.allocator,
        };

        // Slot 0 for the lambda itself (unnamed).
        child.locals[0] = .{ .name = "", .depth = 0, .is_captured = false };
        child.local_count = 1;

        // Add parameters as locals.
        child.beginScope();
        for (param_tokens) |ptok| {
            const pname = self.ast.tokenSlice(ptok);
            child.locals[child.local_count] = .{ .name = pname, .depth = child.scope_depth, .is_captured = false };
            child.local_count += 1;
        }

        // Compile body expression -- single expression with implicit return.
        try child.compileNode(body_node);
        try child.emitOp(.op_return, self.getLine(node_idx));

        // Copy state back.
        // func.upvalue_count is already maintained by addUpvalue.
        self.atom_table = child.atom_table;
        self.atom_count = child.atom_count;
        self.errors = child.errors;

        // In parent: emit op_closure.
        const const_idx = try self.currentChunk().addConstant(Value.fromObj(&func.obj), self.allocator);
        try self.emitOp(.op_closure, self.getLine(node_idx));
        try self.emitByte(@intCast(const_idx), self.getLine(node_idx));

        // Emit upvalue descriptors.
        var i: u8 = 0;
        while (i < func.upvalue_count) : (i += 1) {
            try self.emitByte(if (child.upvalues[i].is_local) 1 else 0, self.getLine(node_idx));
            try self.emitByte(child.upvalues[i].index, self.getLine(node_idx));
        }
        // Lambda is an expression -- closure value is left on the stack.
    }

    // ── Pipe expression compilation ───────────────────────────────────

    fn compilePipeExpr(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        const lhs = node_data.lhs; // value being piped
        const rhs = node_data.rhs; // function or call_expr

        const rhs_tag = self.ast.nodes.items(.tag)[rhs];

        if (rhs_tag == .call_expr) {
            // x |> f(y) desugars to f(x, y)
            const rhs_data = self.ast.nodes.items(.data)[rhs];
            const callee = rhs_data.lhs;
            const extra = rhs_data.rhs;
            const arg_start = self.ast.extra_data.items[extra];
            const arg_end = self.ast.extra_data.items[extra + 1];
            const existing_args = self.ast.extra_data.items[arg_start..arg_end];

            // Compile: callee, piped value (first arg), then remaining args.
            try self.compileNode(callee);
            try self.compileNode(lhs);
            for (existing_args) |arg_idx| {
                const arg_tag = self.ast.nodes.items(.tag)[arg_idx];
                if (arg_tag == .named_arg) {
                    // For named args, compile just the value part.
                    const arg_data = self.ast.nodes.items(.data)[arg_idx];
                    try self.compileNode(arg_data.rhs);
                } else {
                    try self.compileNode(arg_idx);
                }
            }

            const total_args: u8 = @intCast(1 + existing_args.len);
            // Check tail position.
            if (self.is_tail_position) {
                try self.emitOp(.op_tail_call, self.getLine(node_idx));
            } else {
                try self.emitOp(.op_call, self.getLine(node_idx));
            }
            try self.emitByte(total_args, self.getLine(node_idx));
        } else {
            // x |> f desugars to f(x) -- bare identifier or grouped expr (lambda)
            // Compile: callee, piped value.
            try self.compileNode(rhs);
            try self.compileNode(lhs);
            // Swap: we need [callee, arg] but compiled [callee, arg] -- that's correct!
            // Actually for op_call, callee must be below args on stack.
            // We compiled rhs (function) first, then lhs (arg). Stack: [..., fn, arg].
            // op_call expects: [..., fn, arg1, arg2, ...]. That's correct.
            if (self.is_tail_position) {
                try self.emitOp(.op_tail_call, self.getLine(node_idx));
            } else {
                try self.emitOp(.op_call, self.getLine(node_idx));
            }
            try self.emitByte(1, self.getLine(node_idx));
        }
    }

    // ── Return expression compilation ─────────────────────────────────

    fn compileReturnExpr(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        if (self.fn_type == .script) {
            try self.emitError(node_idx, .E005, "'return' outside of function");
            return;
        }

        if (node_data.lhs != Node.null_node) {
            // Check if the return value is a call -- if so, it's a tail call.
            const val_tag = self.ast.nodes.items(.tag)[node_data.lhs];
            if (val_tag == .call_expr) {
                // Compile the call as a tail call.
                const saved_tail = self.is_tail_position;
                self.is_tail_position = true;
                try self.compileNode(node_data.lhs);
                self.is_tail_position = saved_tail;
                // The tail call already handles the return.
                // But we still need op_return for non-tail-call path fallback.
                // Actually, op_tail_call reuses the frame but doesn't return.
                // We need op_return after in case it wasn't actually a tail call
                // (e.g., if op_tail_call is for closures only and it was a builtin).
                try self.emitOp(.op_return, self.getLine(node_idx));
            } else {
                try self.compileNode(node_data.lhs);
                try self.emitOp(.op_return, self.getLine(node_idx));
            }
        } else {
            try self.emitOp(.op_nil, self.getLine(node_idx));
            try self.emitOp(.op_return, self.getLine(node_idx));
        }
    }

    /// Compile a node in tail position (used for function bodies).
    fn compileNodeInTailPosition(self: *Self, node_idx: u32) Error!void {
        const tag = self.ast.nodes.items(.tag)[node_idx];
        const node_data = self.ast.nodes.items(.data)[node_idx];

        if (tag == .block_expr) {
            // For block expressions, the last statement is in tail position.
            try self.compileBlockExprTail(node_data, node_idx);
        } else if (tag == .call_expr) {
            const saved = self.is_tail_position;
            self.is_tail_position = true;
            try self.compileCallExpr(node_data, node_idx);
            self.is_tail_position = saved;
        } else if (tag == .if_expr) {
            try self.compileIfExprTail(node_data, node_idx);
        } else if (tag == .return_expr) {
            try self.compileReturnExpr(node_data, node_idx);
        } else {
            try self.compileNode(node_idx);
        }
    }

    // ── Literal compilation ───────────────────────────────────────────

    fn compileIntLiteral(self: *Self, node_idx: u32) Error!void {
        const main_tokens = self.ast.nodes.items(.main_token);
        const tok_idx = main_tokens[node_idx];
        const text = self.ast.tokenSlice(tok_idx);
        const val = std.fmt.parseInt(i32, text, 10) catch {
            try self.emitError(node_idx, .E007, "invalid integer literal");
            return;
        };
        try self.emitConstant(Value.fromInt(val), self.getLine(node_idx));
    }

    fn compileFloatLiteral(self: *Self, node_idx: u32) Error!void {
        const main_tokens = self.ast.nodes.items(.main_token);
        const tok_idx = main_tokens[node_idx];
        const text = self.ast.tokenSlice(tok_idx);
        const val = std.fmt.parseFloat(f64, text) catch {
            try self.emitError(node_idx, .E007, "invalid float literal");
            return;
        };
        try self.emitConstant(Value.fromFloat(val), self.getLine(node_idx));
    }

    fn compileStringLiteral(self: *Self, node_idx: u32) Error!void {
        const main_tokens = self.ast.nodes.items(.main_token);
        const tok_idx = main_tokens[node_idx];
        const text = self.ast.tokenSlice(tok_idx);
        // Strip quotes from the token text.
        if (text.len >= 2 and text[0] == '"' and text[text.len - 1] == '"') {
            const content = text[1 .. text.len - 1];
            const str_obj = try ObjString.create(self.allocator, content);
            try self.emitConstant(Value.fromObj(&str_obj.obj), self.getLine(node_idx));
        } else {
            try self.emitError(node_idx, .E007, "invalid string literal");
        }
    }

    fn compileBoolLiteral(self: *Self, node_idx: u32) Error!void {
        const main_tokens = self.ast.nodes.items(.main_token);
        const tok_idx = main_tokens[node_idx];
        const tok_tag = self.ast.tokens[tok_idx].tag;
        if (tok_tag == .kw_true) {
            try self.emitOp(.op_true, self.getLine(node_idx));
        } else {
            try self.emitOp(.op_false, self.getLine(node_idx));
        }
    }

    fn compileAtomLiteral(self: *Self, node_idx: u32) Error!void {
        const main_tokens = self.ast.nodes.items(.main_token);
        const tok_idx = main_tokens[node_idx];
        const text = self.ast.tokenSlice(tok_idx);
        // Atom token text is `:name` -- strip the leading colon.
        const name = if (text.len > 0 and text[0] == ':') text[1..] else text;

        const gop = try self.atom_table.getOrPut(self.allocator, name);
        if (!gop.found_existing) {
            gop.value_ptr.* = self.atom_count;
            self.atom_count += 1;
        }
        const atom_id = gop.value_ptr.*;
        try self.emitConstant(Value.fromAtom(atom_id), self.getLine(node_idx));
    }

    // ── Identifier resolution ─────────────────────────────────────────

    fn compileIdentifier(self: *Self, node_idx: u32) Error!void {
        const main_tokens = self.ast.nodes.items(.main_token);
        const tok_idx = main_tokens[node_idx];
        const name = self.ast.tokenSlice(tok_idx);

        // Try to resolve as local variable.
        if (self.resolveLocal(name)) |slot| {
            try self.emitOp(.op_get_local, self.getLine(node_idx));
            try self.emitByte(slot, self.getLine(node_idx));
            return;
        }

        // Try to resolve as upvalue.
        if (self.resolveUpvalue(name)) |slot| {
            try self.emitOp(.op_get_upvalue, self.getLine(node_idx));
            try self.emitByte(slot, self.getLine(node_idx));
            return;
        }

        // Try to resolve as builtin.
        if (self.resolveBuiltin(name)) |builtin_idx| {
            try self.emitOp(.op_get_builtin, self.getLine(node_idx));
            try self.emitByte(builtin_idx, self.getLine(node_idx));
            return;
        }

        // Undefined variable.
        try self.emitError(node_idx, .E002, "undefined variable");
    }

    fn resolveLocal(self: *const Self, name: []const u8) ?u8 {
        if (self.local_count == 0) return null;
        var i: u8 = self.local_count;
        while (i > 0) {
            i -= 1;
            if (std.mem.eql(u8, self.locals[i].name, name)) {
                return i;
            }
        }
        return null;
    }

    fn resolveUpvalue(self: *Self, name: []const u8) ?u8 {
        if (self.parent == null) return null;
        const parent_ptr = self.parent.?;

        // Try local in immediate parent.
        if (parent_ptr.resolveLocal(name)) |slot| {
            parent_ptr.locals[slot].is_captured = true;
            return self.addUpvalue(slot, true);
        }

        // Try upvalue in parent (recursive).
        if (parent_ptr.resolveUpvalue(name)) |idx| {
            return self.addUpvalue(idx, false);
        }

        return null;
    }

    fn addUpvalue(self: *Self, index: u8, is_local: bool) ?u8 {
        const upvalue_count = self.function.upvalue_count;

        // Check for existing upvalue with same index and locality.
        var i: u8 = 0;
        while (i < upvalue_count) : (i += 1) {
            if (self.upvalues[i].index == index and self.upvalues[i].is_local == is_local) {
                return i;
            }
        }

        if (upvalue_count >= 255) return null;

        self.upvalues[upvalue_count] = .{ .index = index, .is_local = is_local };
        self.function.upvalue_count = upvalue_count + 1;
        return upvalue_count;
    }

    fn resolveBuiltin(_: *const Self, name: []const u8) ?u8 {
        for (builtin_names, 0..) |bname, idx| {
            if (std.mem.eql(u8, bname, name)) {
                return @intCast(idx);
            }
        }
        return null;
    }

    // ── Collection literal compilation (Phase 3) ──────────────────────

    /// Known module names for compile-time dot access resolution.
    const module_names = [_][]const u8{
        "List", "Map", "String", "Result", "Option", "Tuple",
    };

    fn isKnownModule(name: []const u8) bool {
        for (module_names) |m| {
            if (std.mem.eql(u8, m, name)) return true;
        }
        return false;
    }

    fn compileListLiteral(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        const elements = self.ast.extra_data.items[node_data.lhs..node_data.rhs];
        // Compile each element (pushes values onto stack).
        for (elements) |elem_idx| {
            try self.compileNode(elem_idx);
        }
        // Emit op_list with element count as u16.
        const count: u16 = @intCast(elements.len);
        const line = self.getLine(node_idx);
        try self.emitOp(.op_list, line);
        try self.emitByte(@intCast((count >> 8) & 0xFF), line);
        try self.emitByte(@intCast(count & 0xFF), line);
    }

    fn compileMapLiteral(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        const pairs = self.ast.extra_data.items[node_data.lhs..node_data.rhs];
        // pairs contains alternating key_node/value_node indices.
        const pair_count = pairs.len / 2;
        var i: usize = 0;
        while (i < pairs.len) : (i += 2) {
            try self.compileNode(pairs[i]); // key
            try self.compileNode(pairs[i + 1]); // value
        }
        // Emit op_map with pair count as u16.
        const count: u16 = @intCast(pair_count);
        const line = self.getLine(node_idx);
        try self.emitOp(.op_map, line);
        try self.emitByte(@intCast((count >> 8) & 0xFF), line);
        try self.emitByte(@intCast(count & 0xFF), line);
    }

    fn compileTupleLiteral(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        const elements = self.ast.extra_data.items[node_data.lhs..node_data.rhs];
        for (elements) |elem_idx| {
            try self.compileNode(elem_idx);
        }
        const count: u16 = @intCast(elements.len);
        const line = self.getLine(node_idx);
        try self.emitOp(.op_tuple, line);
        try self.emitByte(@intCast((count >> 8) & 0xFF), line);
        try self.emitByte(@intCast(count & 0xFF), line);
    }

    fn compileRecordLiteral(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        const pairs = self.ast.extra_data.items[node_data.lhs..node_data.rhs];
        // pairs contains alternating name_token/value_node indices.
        const field_count = pairs.len / 2;

        // First, compile all field values (push onto stack).
        var i: usize = 0;
        while (i < pairs.len) : (i += 2) {
            try self.compileNode(pairs[i + 1]); // value
        }

        // Collect constant pool indices for field names.
        var name_const_indices: std.ArrayListUnmanaged(u16) = .empty;
        defer name_const_indices.deinit(self.allocator);

        i = 0;
        while (i < pairs.len) : (i += 2) {
            const name_tok = pairs[i];
            const name = self.ast.tokenSlice(name_tok);
            // Add field name string to constant pool.
            const str_obj = try ObjString.create(self.allocator, name);
            const const_idx = try self.currentChunk().addConstant(Value.fromObj(&str_obj.obj), self.allocator);
            try name_const_indices.append(self.allocator, @intCast(const_idx));
        }

        // Emit op_record with field count as u16.
        const count: u16 = @intCast(field_count);
        const line = self.getLine(node_idx);
        try self.emitOp(.op_record, line);
        try self.emitByte(@intCast((count >> 8) & 0xFF), line);
        try self.emitByte(@intCast(count & 0xFF), line);

        // Emit field name constant indices (u16 each).
        for (name_const_indices.items) |ci| {
            try self.emitByte(@intCast((ci >> 8) & 0xFF), line);
            try self.emitByte(@intCast(ci & 0xFF), line);
        }
    }

    fn compileRecordSpread(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        // data.lhs = base record expression node
        // data.rhs = extra_idx pointing to {override_start, override_end} in extra_data
        try self.compileNode(node_data.lhs); // push base record

        const extra_idx = node_data.rhs;
        const override_start = self.ast.extra_data.items[extra_idx];
        const override_end = self.ast.extra_data.items[extra_idx + 1];
        const override_pairs = self.ast.extra_data.items[override_start..override_end];
        const override_count = override_pairs.len / 2;

        // Compile override values (push onto stack).
        var i: usize = 0;
        while (i < override_pairs.len) : (i += 2) {
            try self.compileNode(override_pairs[i + 1]); // value
        }

        // Collect constant pool indices for override field names.
        var name_const_indices: std.ArrayListUnmanaged(u16) = .empty;
        defer name_const_indices.deinit(self.allocator);

        i = 0;
        while (i < override_pairs.len) : (i += 2) {
            const name_tok = override_pairs[i];
            const name = self.ast.tokenSlice(name_tok);
            const str_obj = try ObjString.create(self.allocator, name);
            const const_idx = try self.currentChunk().addConstant(Value.fromObj(&str_obj.obj), self.allocator);
            try name_const_indices.append(self.allocator, @intCast(const_idx));
        }

        const line = self.getLine(node_idx);
        try self.emitOp(.op_record_spread, line);
        try self.emitByte(@intCast(override_count), line);

        // Emit override field name constant indices (u16 each).
        for (name_const_indices.items) |ci| {
            try self.emitByte(@intCast((ci >> 8) & 0xFF), line);
            try self.emitByte(@intCast(ci & 0xFF), line);
        }
    }

    fn compileFieldAccess(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        // data.lhs = object expression node, data.rhs = field name token index
        const field_tok = node_data.rhs;
        const field_name = self.ast.tokenSlice(field_tok);

        // Check if left side is an identifier that is a known module name.
        const left_tag = self.ast.nodes.items(.tag)[node_data.lhs];
        if (left_tag == .identifier) {
            const left_tok = self.ast.nodes.items(.main_token)[node_data.lhs];
            const left_name = self.ast.tokenSlice(left_tok);

            if (isKnownModule(left_name)) {
                // Build the dotted name (e.g., "List.get").
                // Look up in builtins table.
                const dotted = [_][]const u8{ left_name, ".", field_name };
                var dotted_buf: [128]u8 = undefined;
                var pos: usize = 0;
                for (dotted) |part| {
                    @memcpy(dotted_buf[pos..][0..part.len], part);
                    pos += part.len;
                }
                const dotted_name = dotted_buf[0..pos];

                if (self.resolveBuiltin(dotted_name)) |builtin_idx| {
                    try self.emitOp(.op_get_builtin, self.getLine(node_idx));
                    try self.emitByte(builtin_idx, self.getLine(node_idx));
                    return;
                }

                // Module-qualified but not a known builtin -- emit error.
                try self.emitError(node_idx, .E002, "unknown module function");
                return;
            }
        }

        // Runtime field access: compile left side, emit op_get_field.
        try self.compileNode(node_data.lhs);
        const str_obj = try ObjString.create(self.allocator, field_name);
        const const_idx = try self.currentChunk().addConstant(Value.fromObj(&str_obj.obj), self.allocator);
        const line = self.getLine(node_idx);
        try self.emitOp(.op_get_field, line);
        try self.emitByte(@intCast((const_idx >> 8) & 0xFF), line);
        try self.emitByte(@intCast(const_idx & 0xFF), line);
    }

    // ── Binary operations ─────────────────────────────────────────────

    fn compileBinaryOp(self: *Self, node_data: Node.Data, op: OpCode, node_idx: u32) Error!void {
        try self.compileNode(node_data.lhs);
        try self.compileNode(node_data.rhs);
        try self.emitOp(op, self.getLine(node_idx));
    }

    fn compileLogicalAnd(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        // Short-circuit: if lhs is false, skip rhs.
        try self.compileNode(node_data.lhs);
        const jump_offset = try self.emitJump(.op_jump_if_false, self.getLine(node_idx));
        try self.emitOp(.op_pop, self.getLine(node_idx));
        try self.compileNode(node_data.rhs);
        try self.patchJump(jump_offset);
    }

    fn compileLogicalOr(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        // Short-circuit: if lhs is true, skip rhs.
        try self.compileNode(node_data.lhs);
        // Jump over the "skip" if false:
        const else_jump = try self.emitJump(.op_jump_if_false, self.getLine(node_idx));
        const end_jump = try self.emitJump(.op_jump, self.getLine(node_idx));
        try self.patchJump(else_jump);
        try self.emitOp(.op_pop, self.getLine(node_idx));
        try self.compileNode(node_data.rhs);
        try self.patchJump(end_jump);
    }

    // ── Let declaration ───────────────────────────────────────────────

    fn compileLetDecl(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        // Compile the initializer -- its value stays on the stack as the local.
        try self.compileNode(node_data.rhs);

        // Record the local variable.
        const name_tok_idx = node_data.lhs;
        const name = self.ast.tokenSlice(name_tok_idx);

        if (self.local_count >= 255) {
            try self.emitError(node_idx, .E009, "too many local variables in scope");
            return;
        }

        self.locals[self.local_count] = .{
            .name = name,
            .depth = self.scope_depth,
            .is_captured = false,
        };
        self.local_count += 1;
    }

    // ── Assignment ────────────────────────────────────────────────────

    fn compileAssignStmt(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        // Compile the value expression.
        try self.compileNode(node_data.rhs);

        // Resolve the target variable.
        const target_idx = node_data.lhs;
        const target_main_tok = self.ast.nodes.items(.main_token)[target_idx];
        const name = self.ast.tokenSlice(target_main_tok);

        if (self.resolveLocal(name)) |slot| {
            try self.emitOp(.op_set_local, self.getLine(node_idx));
            try self.emitByte(slot, self.getLine(node_idx));
            // Pop the stale copy -- assignment is a statement, not an expression.
            try self.emitOp(.op_pop, self.getLine(node_idx));
        } else if (self.resolveUpvalue(name)) |slot| {
            try self.emitOp(.op_set_upvalue, self.getLine(node_idx));
            try self.emitByte(slot, self.getLine(node_idx));
            // Pop the stale copy.
            try self.emitOp(.op_pop, self.getLine(node_idx));
        } else {
            try self.emitError(node_idx, .E002, "undefined variable");
        }
    }

    // ── If expression ─────────────────────────────────────────────────

    fn compileIfExpr(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        // Compile condition.
        try self.compileNode(node_data.lhs);

        // Emit jump-if-false (to else branch).
        const then_jump = try self.emitJump(.op_jump_if_false, self.getLine(node_idx));
        try self.emitOp(.op_pop, self.getLine(node_idx)); // pop condition

        // Extract then/else branches from extra_data.
        const extra_idx = node_data.rhs;
        const then_branch = self.ast.extra_data.items[extra_idx];
        const else_branch = self.ast.extra_data.items[extra_idx + 1];

        // Compile then-branch.
        try self.compileNode(then_branch);

        // Jump over else-branch.
        const else_jump = try self.emitJump(.op_jump, self.getLine(node_idx));

        // Patch then_jump to land here (start of else).
        try self.patchJump(then_jump);
        try self.emitOp(.op_pop, self.getLine(node_idx)); // pop condition (for false path)

        // Compile else-branch (or emit nil if none).
        if (else_branch != Node.null_node) {
            try self.compileNode(else_branch);
        } else {
            try self.emitOp(.op_nil, self.getLine(node_idx));
        }

        try self.patchJump(else_jump);
    }

    /// Compile if expression with tail position propagation.
    fn compileIfExprTail(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        // Compile condition.
        try self.compileNode(node_data.lhs);

        const then_jump = try self.emitJump(.op_jump_if_false, self.getLine(node_idx));
        try self.emitOp(.op_pop, self.getLine(node_idx));

        const extra_idx = node_data.rhs;
        const then_branch = self.ast.extra_data.items[extra_idx];
        const else_branch = self.ast.extra_data.items[extra_idx + 1];

        // Both branches are in tail position.
        try self.compileNodeInTailPosition(then_branch);

        const else_jump = try self.emitJump(.op_jump, self.getLine(node_idx));

        try self.patchJump(then_jump);
        try self.emitOp(.op_pop, self.getLine(node_idx));

        if (else_branch != Node.null_node) {
            try self.compileNodeInTailPosition(else_branch);
        } else {
            try self.emitOp(.op_nil, self.getLine(node_idx));
        }

        try self.patchJump(else_jump);
    }

    // ── While statement ───────────────────────────────────────────────

    fn compileWhileStmt(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        const loop_start: u32 = @intCast(self.currentChunk().code.items.len);

        // Compile condition.
        try self.compileNode(node_data.lhs);

        // Jump out if false.
        const exit_jump = try self.emitJump(.op_jump_if_false, self.getLine(node_idx));
        try self.emitOp(.op_pop, self.getLine(node_idx)); // pop condition

        // Compile body.
        try self.compileNode(node_data.rhs);
        try self.emitOp(.op_pop, self.getLine(node_idx)); // discard body value

        // Loop back.
        try self.emitLoop(loop_start, self.getLine(node_idx));

        try self.patchJump(exit_jump);
        try self.emitOp(.op_pop, self.getLine(node_idx)); // pop condition (false path)

        // While produces nil.
        try self.emitOp(.op_nil, self.getLine(node_idx));
    }

    // ── For statement ─────────────────────────────────────────────────

    fn compileForStmt(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        // for i in iterable { body }
        // Extra data: [var_tok, body_node]
        const extra_idx = node_data.rhs;
        const var_tok = self.ast.extra_data.items[extra_idx];
        const body_node = self.ast.extra_data.items[extra_idx + 1];
        const line = self.getLine(node_idx);

        // Compile iterable -- leaves it on stack.
        try self.compileNode(node_data.lhs);

        // Push initial index (0) for iteration.
        try self.emitConstant(Value.fromInt(0), line);

        // Record the loop variable and index as locals.
        self.beginScope();
        const var_name = self.ast.tokenSlice(var_tok);

        // Local for the iterable (hidden).
        if (self.local_count >= 254) {
            try self.emitError(node_idx, .E009, "too many local variables in scope");
            return;
        }
        self.locals[self.local_count] = .{ .name = "__iter__", .depth = self.scope_depth, .is_captured = false };
        self.local_count += 1;

        // Local for the index (hidden).
        self.locals[self.local_count] = .{ .name = "__idx__", .depth = self.scope_depth, .is_captured = false };
        self.local_count += 1;

        // Push placeholder for loop variable value.
        try self.emitOp(.op_nil, line);
        self.locals[self.local_count] = .{ .name = var_name, .depth = self.scope_depth, .is_captured = false };
        self.local_count += 1;

        const loop_start: u32 = @intCast(self.currentChunk().code.items.len);

        // Emit for_iter: checks if iteration complete, sets loop var, or jumps past body.
        const exit_jump = try self.emitJump(.op_for_iter, line);

        // Compile body.
        try self.compileNode(body_node);
        try self.emitOp(.op_pop, line); // discard body result

        // Loop back.
        try self.emitLoop(loop_start, line);

        try self.patchJump(exit_jump);

        // End scope: pop the 3 for-in locals (loop_var, __idx__, __iter__).
        // Check if any are captured and emit close_upvalue if needed.
        self.endScopeWithPops(3, line);

        // For produces nil.
        try self.emitOp(.op_nil, line);
    }

    // ── Block expression ──────────────────────────────────────────────

    fn compileBlockExpr(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        const stmts = self.ast.extra_data.items[node_data.lhs..node_data.rhs];
        if (stmts.len == 0) {
            try self.emitOp(.op_nil, self.getLine(node_idx));
            return;
        }

        self.beginScope();

        const last_stmt_idx = stmts[stmts.len - 1];
        const last_tag = self.ast.nodes.items(.tag)[last_stmt_idx];

        // Compile all statements. The last one should leave its value on stack.
        for (stmts, 0..) |stmt_idx, i| {
            const is_last = (i == stmts.len - 1);

            if (is_last and last_tag == .expr_stmt) {
                // For the last item in a block, if it's an expr_stmt, compile
                // just the inner expression WITHOUT the pop, so its value
                // remains on the stack as the block's result.
                const inner_data = self.ast.nodes.items(.data)[stmt_idx];
                try self.compileNode(inner_data.lhs);
            } else {
                try self.compileNode(stmt_idx);
            }

            if (!is_last) {
                const stmt_tag = self.ast.nodes.items(.tag)[stmt_idx];
                // Statements that don't produce values (let, while, for, assign, fn_decl) don't need pop.
                if (stmt_tag == .expr_stmt) {
                    // expr_stmt already pops its value
                } else if (stmt_tag != .let_decl and stmt_tag != .while_stmt and stmt_tag != .for_stmt and stmt_tag != .assign_stmt and stmt_tag != .fn_decl) {
                    try self.emitOp(.op_pop, self.getLine(stmt_idx));
                }
            }
        }

        // End scope: pop locals but we need to keep the last expression value.
        const last_is_value = (last_tag == .expr_stmt or
            (last_tag != .let_decl and last_tag != .while_stmt and
            last_tag != .for_stmt and last_tag != .assign_stmt and last_tag != .fn_decl));

        // Count locals to pop.
        var pop_count: u32 = 0;
        var local_idx = self.local_count;
        while (local_idx > 0 and self.locals[local_idx - 1].depth > self.scope_depth - 1) {
            pop_count += 1;
            local_idx -= 1;
        }

        if (last_is_value) {
            if (pop_count > 0) {
                const base_slot: u8 = @intCast(self.local_count - @as(u8, @intCast(pop_count)));
                // Close captured upvalues BEFORE saving the return value to base_slot,
                // so the upvalue gets the original value, not the overwritten one.
                try self.emitCloseUpvaluesInScope(pop_count, self.getLine(node_idx));
                try self.emitOp(.op_set_local, self.getLine(node_idx));
                try self.emitByte(base_slot, self.getLine(node_idx));
                // Pop locals (all upvalues already closed, just pop everything).
                self.endScopePopOnly(pop_count, self.getLine(node_idx));
                try self.emitOp(.op_get_local, self.getLine(node_idx));
                try self.emitByte(base_slot, self.getLine(node_idx));
            } else {
                self.scope_depth -= 1;
            }
        } else {
            self.endScopeWithPops(pop_count, self.getLine(node_idx));
            try self.emitOp(.op_nil, self.getLine(node_idx));
        }
    }

    /// Compile block expression with tail position for the last statement.
    fn compileBlockExprTail(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        const stmts = self.ast.extra_data.items[node_data.lhs..node_data.rhs];
        if (stmts.len == 0) {
            try self.emitOp(.op_nil, self.getLine(node_idx));
            return;
        }

        self.beginScope();

        const last_stmt_idx = stmts[stmts.len - 1];
        const last_tag = self.ast.nodes.items(.tag)[last_stmt_idx];

        for (stmts, 0..) |stmt_idx, i| {
            const is_last = (i == stmts.len - 1);

            if (is_last) {
                if (last_tag == .expr_stmt) {
                    const inner_data = self.ast.nodes.items(.data)[stmt_idx];
                    try self.compileNodeInTailPosition(inner_data.lhs);
                } else {
                    try self.compileNodeInTailPosition(stmt_idx);
                }
            } else {
                try self.compileNode(stmt_idx);
                const stmt_tag = self.ast.nodes.items(.tag)[stmt_idx];
                if (stmt_tag == .expr_stmt) {
                    // already popped
                } else if (stmt_tag != .let_decl and stmt_tag != .while_stmt and stmt_tag != .for_stmt and stmt_tag != .assign_stmt and stmt_tag != .fn_decl) {
                    try self.emitOp(.op_pop, self.getLine(stmt_idx));
                }
            }
        }

        // End scope.
        const last_is_value = (last_tag == .expr_stmt or
            (last_tag != .let_decl and last_tag != .while_stmt and
            last_tag != .for_stmt and last_tag != .assign_stmt and last_tag != .fn_decl));

        var pop_count: u32 = 0;
        var local_idx2 = self.local_count;
        while (local_idx2 > 0 and self.locals[local_idx2 - 1].depth > self.scope_depth - 1) {
            pop_count += 1;
            local_idx2 -= 1;
        }

        if (last_is_value) {
            if (pop_count > 0) {
                const base_slot: u8 = @intCast(self.local_count - @as(u8, @intCast(pop_count)));
                // Close captured upvalues BEFORE saving the return value to base_slot.
                try self.emitCloseUpvaluesInScope(pop_count, self.getLine(node_idx));
                try self.emitOp(.op_set_local, self.getLine(node_idx));
                try self.emitByte(base_slot, self.getLine(node_idx));
                self.endScopePopOnly(pop_count, self.getLine(node_idx));
                try self.emitOp(.op_get_local, self.getLine(node_idx));
                try self.emitByte(base_slot, self.getLine(node_idx));
            } else {
                self.scope_depth -= 1;
            }
        } else {
            self.endScopeWithPops(pop_count, self.getLine(node_idx));
            try self.emitOp(.op_nil, self.getLine(node_idx));
        }
    }

    // ── Call expression ───────────────────────────────────────────────

    fn compileCallExpr(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        // Save tail position -- arguments are never in tail position.
        const saved_tail = self.is_tail_position;
        self.is_tail_position = false;

        // Compile callee.
        try self.compileNode(node_data.lhs);

        // Get argument range from extra_data.
        const extra_idx = node_data.rhs;
        const arg_start = self.ast.extra_data.items[extra_idx];
        const arg_end = self.ast.extra_data.items[extra_idx + 1];
        const arg_indices = self.ast.extra_data.items[arg_start..arg_end];

        // Check for named arguments -- if any exist, we need special handling.
        var has_named = false;
        for (arg_indices) |arg_idx| {
            if (self.ast.nodes.items(.tag)[arg_idx] == .named_arg) {
                has_named = true;
                break;
            }
        }

        if (has_named) {
            // Restore tail position for the call itself.
            self.is_tail_position = saved_tail;
            try self.compileCallWithNamedArgs(node_data, node_idx, arg_indices);
        } else {
            // Simple case: all positional arguments.
            for (arg_indices) |arg_idx| {
                try self.compileNode(arg_idx);
            }

            // Restore tail position for the call instruction.
            self.is_tail_position = saved_tail;
            const arg_count: u8 = @intCast(arg_indices.len);
            if (self.is_tail_position) {
                try self.emitOp(.op_tail_call, self.getLine(node_idx));
            } else {
                try self.emitOp(.op_call, self.getLine(node_idx));
            }
            try self.emitByte(arg_count, self.getLine(node_idx));
        }
    }

    /// Compile a call with named arguments. We need to reorder arguments
    /// into positional slots, filling defaults for missing optional params.
    fn compileCallWithNamedArgs(self: *Self, node_data: Node.Data, node_idx: u32, arg_indices: []const u32) Error!void {
        _ = node_data;
        // We compile arguments as-is and let the VM handle the reordering
        // since we may not know the callee's param metadata at compile time.
        // However, for known local functions, we could optimize.
        // For now, compile all positional args first, then named args.
        // The VM will need to handle this at runtime.

        // Simple approach: compile all args in order (positional then named).
        // For named args, push the name atom + value pairs.
        // The VM can use function metadata to reorder.

        // Actually, the simpler approach from the plan: compile positional args in order,
        // then for named args, try to resolve at compile time if the callee is known.
        // For dynamic callees, just emit in order.
        for (arg_indices) |arg_idx| {
            const arg_tag = self.ast.nodes.items(.tag)[arg_idx];
            if (arg_tag == .named_arg) {
                // Just compile the value part of the named arg.
                const arg_data = self.ast.nodes.items(.data)[arg_idx];
                try self.compileNode(arg_data.rhs);
            } else {
                try self.compileNode(arg_idx);
            }
        }

        const arg_count: u8 = @intCast(arg_indices.len);
        if (self.is_tail_position) {
            try self.emitOp(.op_tail_call, self.getLine(node_idx));
        } else {
            try self.emitOp(.op_call, self.getLine(node_idx));
        }
        try self.emitByte(arg_count, self.getLine(node_idx));
    }

    // ── Scope management ──────────────────────────────────────────────

    fn beginScope(self: *Self) void {
        self.scope_depth += 1;
    }

    fn endScope(self: *Self) void {
        // Pop all locals at current depth, emitting close_upvalue for captured ones.
        while (self.local_count > 0 and self.locals[self.local_count - 1].depth >= self.scope_depth) {
            // We can't emit here because we need Error return.
            // Just adjust the count; caller handles op_pop/op_close_upvalue.
            self.local_count -= 1;
        }
        self.scope_depth -= 1;
    }

    /// End scope and emit pops/close_upvalues for the specified number of locals.
    fn endScopeWithPops(self: *Self, count: u32, line: u32) void {
        var remaining = count;
        while (remaining > 0 and self.local_count > 0) {
            self.local_count -= 1;
            if (self.locals[self.local_count].is_captured) {
                self.emitOp(.op_close_upvalue, line) catch {};
            } else {
                self.emitOp(.op_pop, line) catch {};
            }
            remaining -= 1;
        }
        self.scope_depth -= 1;
    }

    /// End scope emitting only op_pop (no close_upvalue). Used when upvalues
    /// have already been closed via emitCloseUpvaluesInScope.
    fn endScopePopOnly(self: *Self, count: u32, line: u32) void {
        var remaining = count;
        while (remaining > 0 and self.local_count > 0) {
            self.local_count -= 1;
            self.emitOp(.op_pop, line) catch {};
            remaining -= 1;
        }
        self.scope_depth -= 1;
    }

    /// Emit op_close_upvalue_at for each captured local in the current scope.
    /// This closes upvalues in-place (without popping) so that a subsequent
    /// op_set_local to the base slot doesn't corrupt upvalue values.
    fn emitCloseUpvaluesInScope(self: *Self, pop_count: u32, line: u32) Error!void {
        const base_idx: u8 = @intCast(self.local_count - @as(u8, @intCast(pop_count)));
        var i: u8 = base_idx;
        while (i < self.local_count) : (i += 1) {
            if (self.locals[i].is_captured) {
                try self.emitOp(.op_close_upvalue_at, line);
                try self.emitByte(i, line);
            }
        }
    }

    // ── Bytecode emission helpers ─────────────────────────────────────

    fn emitOp(self: *Self, op: OpCode, line: u32) Error!void {
        try self.currentChunk().write(@intFromEnum(op), line, self.allocator);
    }

    fn emitByte(self: *Self, byte: u8, line: u32) Error!void {
        try self.currentChunk().write(byte, line, self.allocator);
    }

    fn emitConstant(self: *Self, val: Value, line: u32) Error!void {
        const idx = try self.currentChunk().addConstant(val, self.allocator);
        if (idx <= 255) {
            try self.emitOp(.op_constant, line);
            try self.emitByte(@intCast(idx), line);
        } else {
            try self.emitOp(.op_constant_long, line);
            try self.emitByte(@intCast((idx >> 8) & 0xFF), line);
            try self.emitByte(@intCast(idx & 0xFF), line);
        }
    }

    fn emitJump(self: *Self, op: OpCode, line: u32) Error!u32 {
        try self.emitOp(op, line);
        try self.emitByte(0xFF, line); // placeholder hi
        try self.emitByte(0xFF, line); // placeholder lo
        return @intCast(self.currentChunk().code.items.len - 2);
    }

    fn patchJump(self: *Self, offset: u32) Error!void {
        const jump: u32 = @intCast(self.currentChunk().code.items.len - offset - 2);
        if (jump > 0xFFFF) {
            // Jump too large -- would need a long jump instruction.
            return error.Overflow;
        }
        self.currentChunk().code.items[offset] = @intCast((jump >> 8) & 0xFF);
        self.currentChunk().code.items[offset + 1] = @intCast(jump & 0xFF);
    }

    fn emitLoop(self: *Self, loop_start: u32, line: u32) Error!void {
        try self.emitOp(.op_loop, line);
        const offset: u32 = @intCast(self.currentChunk().code.items.len - loop_start + 2);
        if (offset > 0xFFFF) {
            return error.Overflow;
        }
        try self.emitByte(@intCast((offset >> 8) & 0xFF), line);
        try self.emitByte(@intCast(offset & 0xFF), line);
    }

    // ── Helpers ───────────────────────────────────────────────────────

    fn getLine(self: *const Self, node_idx: u32) u32 {
        const main_tokens = self.ast.nodes.items(.main_token);
        const tok_idx = main_tokens[node_idx];
        if (tok_idx < self.ast.tokens.len) {
            return self.ast.tokens[tok_idx].line;
        }
        return 0;
    }

    fn emitError(self: *Self, node_idx: u32, code: ErrorCode, message: []const u8) Error!void {
        const main_tokens = self.ast.nodes.items(.main_token);
        const tok_idx = main_tokens[node_idx];
        const tok = if (tok_idx < self.ast.tokens.len) self.ast.tokens[tok_idx] else self.ast.tokens[self.ast.tokens.len - 1];
        try self.errors.append(self.allocator, .{
            .error_code = code,
            .severity = .@"error",
            .message = message,
            .span = .{ .start = tok.start, .end = tok.end },
            .labels = &[_]Label{},
            .help = null,
        });
    }

    /// Check if the last emitted instruction is op_return.
    fn lastInstructionIsReturn(self: *const Self) bool {
        const code = self.currentChunkConst().code.items;
        if (code.len == 0) return false;
        return @as(OpCode, @enumFromInt(code[code.len - 1])) == .op_return;
    }

    /// Const access to current chunk.
    fn currentChunkConst(self: *const Self) *const Chunk {
        return &self.function.chunk;
    }

    /// Evaluate a constant expression at compile time (for default parameter values).
    /// Only supports simple literals (int, float, string, bool, nil, atom).
    fn evaluateConstantExpr(self: *Self, node_idx: u32) Error!Value {
        const tag = self.ast.nodes.items(.tag)[node_idx];
        const main_tokens = self.ast.nodes.items(.main_token);
        const tok_idx = main_tokens[node_idx];
        const text = self.ast.tokenSlice(tok_idx);

        switch (tag) {
            .int_literal => {
                const val = std.fmt.parseInt(i32, text, 10) catch return Value.nil;
                return Value.fromInt(val);
            },
            .float_literal => {
                const val = std.fmt.parseFloat(f64, text) catch return Value.nil;
                return Value.fromFloat(val);
            },
            .string_literal => {
                if (text.len >= 2 and text[0] == '"' and text[text.len - 1] == '"') {
                    const content = text[1 .. text.len - 1];
                    const str_obj = try ObjString.create(self.allocator, content);
                    return Value.fromObj(&str_obj.obj);
                }
                return Value.nil;
            },
            .bool_literal => {
                const tok_tag = self.ast.tokens[tok_idx].tag;
                return Value.fromBool(tok_tag == .kw_true);
            },
            .nil_literal => return Value.nil,
            .atom_literal => {
                const name = if (text.len > 0 and text[0] == ':') text[1..] else text;
                const gop = try self.atom_table.getOrPut(self.allocator, name);
                if (!gop.found_existing) {
                    gop.value_ptr.* = self.atom_count;
                    self.atom_count += 1;
                }
                return Value.fromAtom(gop.value_ptr.*);
            },
            else => return Value.nil,
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════
// ── Test helpers ──────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════

const lexer_mod = @import("lexer");
const Lexer = lexer_mod.Lexer;
const parser_mod = @import("parser");
const Parser = parser_mod.Parser;

const TestCompileResult = struct {
    result: CompileResult,
    token_buf: std.ArrayListUnmanaged(Token),
    ast: Ast,

    fn deinit(self: *TestCompileResult, allocator: Allocator) void {
        self.result.deinit(allocator);
        self.ast.deinit(allocator);
        self.token_buf.deinit(allocator);
    }

    fn getChunk(self: *const TestCompileResult) *const Chunk {
        return &self.result.closure.function.chunk;
    }
};

fn testCompile(source: []const u8, allocator: Allocator) !TestCompileResult {
    var lex = Lexer.init(source);
    try lex.tokenize(allocator);
    lex.errors.deinit(allocator);

    var ast = try Parser.parse(lex.tokens.items, source, allocator);
    var result = try Compiler.compile(&ast, allocator);
    _ = &result;

    return .{
        .result = result,
        .ast = ast,
        .token_buf = lex.tokens,
    };
}

/// Get the opcode at a given offset in the chunk.
fn opAt(chunk: *const Chunk, offset: usize) OpCode {
    return @enumFromInt(chunk.code.items[offset]);
}

/// Get the byte operand at a given offset.
fn byteAt(chunk: *const Chunk, offset: usize) u8 {
    return chunk.code.items[offset];
}

// ═══════════════════════════════════════════════════════════════════════
// ── Tests ──────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════

// Test 1: Compiling `42` emits [op_constant(42), op_pop, op_nil, op_return]
test "compile: integer literal 42" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("42", allocator);
    defer tc.deinit(allocator);

    const chunk = tc.getChunk();
    try std.testing.expect(!tc.result.hasErrors());

    try std.testing.expectEqual(OpCode.op_constant, opAt(chunk, 0));
    const const_idx = byteAt(chunk, 1);
    try std.testing.expectEqual(@as(i32, 42), chunk.constants.items[const_idx].asInt());
    try std.testing.expectEqual(OpCode.op_pop, opAt(chunk, 2));
}

// Test 2: Compiling `1 + 2` emits [op_constant(1), op_constant(2), op_add, ...]
test "compile: addition 1 + 2" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("1 + 2", allocator);
    defer tc.deinit(allocator);

    const chunk = tc.getChunk();
    try std.testing.expect(!tc.result.hasErrors());

    try std.testing.expectEqual(OpCode.op_constant, opAt(chunk, 0));
    try std.testing.expectEqual(@as(i32, 1), chunk.constants.items[byteAt(chunk, 1)].asInt());
    try std.testing.expectEqual(OpCode.op_constant, opAt(chunk, 2));
    try std.testing.expectEqual(@as(i32, 2), chunk.constants.items[byteAt(chunk, 3)].asInt());
    try std.testing.expectEqual(OpCode.op_add, opAt(chunk, 4));
}

// Test 3: Precedence: 1 + 2 * 3
test "compile: precedence 1 + 2 * 3" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("1 + 2 * 3", allocator);
    defer tc.deinit(allocator);

    const chunk = tc.getChunk();
    try std.testing.expect(!tc.result.hasErrors());

    try std.testing.expectEqual(OpCode.op_constant, opAt(chunk, 0)); // 1
    try std.testing.expectEqual(OpCode.op_constant, opAt(chunk, 2)); // 2
    try std.testing.expectEqual(OpCode.op_constant, opAt(chunk, 4)); // 3
    try std.testing.expectEqual(OpCode.op_multiply, opAt(chunk, 6));
    try std.testing.expectEqual(OpCode.op_add, opAt(chunk, 7));
}

// Test 4: Unary negate
test "compile: unary negate" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("-42", allocator);
    defer tc.deinit(allocator);

    const chunk = tc.getChunk();
    try std.testing.expect(!tc.result.hasErrors());

    try std.testing.expectEqual(OpCode.op_constant, opAt(chunk, 0));
    try std.testing.expectEqual(@as(i32, 42), chunk.constants.items[byteAt(chunk, 1)].asInt());
    try std.testing.expectEqual(OpCode.op_negate, opAt(chunk, 2));
}

// Test 4b: Negate a local variable
test "compile: negate local in block" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("let x = 5\nlet y = -x", allocator);
    defer tc.deinit(allocator);

    const chunk = tc.getChunk();
    try std.testing.expect(!tc.result.hasErrors());

    var found_negate = false;
    var found_get_local_before_negate = false;
    var i: usize = 0;
    while (i < chunk.code.items.len) {
        const op = opAt(chunk, i);
        if (op == .op_get_local) {
            if (i + 2 < chunk.code.items.len and opAt(chunk, i + 2) == .op_negate) {
                found_get_local_before_negate = true;
                found_negate = true;
            }
            i += 2;
        } else if (op == .op_constant or op == .op_set_local or op == .op_get_builtin or op == .op_call or op == .op_atom or op == .op_get_upvalue or op == .op_set_upvalue or op == .op_tail_call) {
            i += 2;
        } else if (op == .op_jump or op == .op_jump_if_false or op == .op_loop or op == .op_constant_long or op == .op_for_iter) {
            i += 3;
        } else if (op == .op_closure) {
            i += 2; // skip const idx, then upvalue descriptors handled separately
        } else {
            i += 1;
        }
    }
    try std.testing.expect(found_get_local_before_negate);
    try std.testing.expect(found_negate);
}

// Test 5: Let binding
test "compile: let binding creates local" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("let x = 42", allocator);
    defer tc.deinit(allocator);

    const chunk = tc.getChunk();
    try std.testing.expect(!tc.result.hasErrors());

    try std.testing.expectEqual(OpCode.op_constant, opAt(chunk, 0));
    try std.testing.expectEqual(@as(i32, 42), chunk.constants.items[byteAt(chunk, 1)].asInt());
}

// Test 6: Multiple locals and access
test "compile: multiple locals and access" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("let x = 1\nlet y = 2\nx + y", allocator);
    defer tc.deinit(allocator);

    const chunk = tc.getChunk();
    try std.testing.expect(!tc.result.hasErrors());

    var get_local_slots: std.ArrayListUnmanaged(u8) = .empty;
    defer get_local_slots.deinit(allocator);

    var i: usize = 0;
    while (i < chunk.code.items.len) {
        const op = opAt(chunk, i);
        if (op == .op_get_local) {
            try get_local_slots.append(allocator, byteAt(chunk, i + 1));
            i += 2;
        } else if (op == .op_constant or op == .op_set_local or op == .op_get_builtin or op == .op_call or op == .op_atom or op == .op_get_upvalue or op == .op_set_upvalue or op == .op_tail_call) {
            i += 2;
        } else if (op == .op_jump or op == .op_jump_if_false or op == .op_loop or op == .op_constant_long or op == .op_for_iter) {
            i += 3;
        } else if (op == .op_closure) {
            i += 2;
        } else {
            i += 1;
        }
    }

    try std.testing.expect(get_local_slots.items.len >= 2);
    // Slot 0 is reserved for the script function, so locals start at 1.
    try std.testing.expectEqual(@as(u8, 1), get_local_slots.items[0]); // x at slot 1
    try std.testing.expectEqual(@as(u8, 2), get_local_slots.items[1]); // y at slot 2
}

// Test 7: If-else expression
test "compile: if-else expression" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("if true { 1 } else { 2 }", allocator);
    defer tc.deinit(allocator);

    const chunk = tc.getChunk();
    try std.testing.expect(!tc.result.hasErrors());

    var found_true = false;
    var found_jif = false;
    var found_jump = false;
    var i: usize = 0;
    while (i < chunk.code.items.len) {
        const op = opAt(chunk, i);
        switch (op) {
            .op_true => {
                found_true = true;
                i += 1;
            },
            .op_jump_if_false => {
                found_jif = true;
                i += 3;
            },
            .op_jump => {
                found_jump = true;
                i += 3;
            },
            .op_constant, .op_set_local, .op_get_local, .op_get_builtin, .op_call, .op_atom, .op_get_upvalue, .op_set_upvalue, .op_tail_call => {
                i += 2;
            },
            .op_loop, .op_constant_long, .op_for_iter => {
                i += 3;
            },
            .op_closure => {
                i += 2;
            },
            else => i += 1,
        }
    }
    try std.testing.expect(found_true);
    try std.testing.expect(found_jif);
    try std.testing.expect(found_jump);
}

// Test 8: While loop
test "compile: while loop" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("let x = 3\nwhile x > 0 { x = x - 1 }", allocator);
    defer tc.deinit(allocator);

    const chunk = tc.getChunk();
    try std.testing.expect(!tc.result.hasErrors());

    var found_loop = false;
    var found_jif = false;
    var i: usize = 0;
    while (i < chunk.code.items.len) {
        const op = opAt(chunk, i);
        switch (op) {
            .op_loop => {
                found_loop = true;
                i += 3;
            },
            .op_jump_if_false => {
                found_jif = true;
                i += 3;
            },
            .op_constant, .op_set_local, .op_get_local, .op_get_builtin, .op_call, .op_atom, .op_get_upvalue, .op_set_upvalue, .op_tail_call => i += 2,
            .op_jump, .op_constant_long, .op_for_iter => i += 3,
            .op_closure => i += 2,
            else => i += 1,
        }
    }
    try std.testing.expect(found_loop);
    try std.testing.expect(found_jif);
}

// Test 9: Atom literal
test "compile: atom literal" {
    const allocator = std.testing.allocator;
    var tc = try testCompile(":ok", allocator);
    defer tc.deinit(allocator);

    try std.testing.expect(!tc.result.hasErrors());
    try std.testing.expectEqual(@as(u32, 8), tc.result.atom_count);
    try std.testing.expect(tc.result.atom_table.contains("ok"));
}

// Test 10: Block expression
test "compile: block expression with local scope" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("{ let x = 1\nx + 2 }", allocator);
    defer tc.deinit(allocator);

    const chunk = tc.getChunk();
    try std.testing.expect(!tc.result.hasErrors());

    var found_add = false;
    var i: usize = 0;
    while (i < chunk.code.items.len) {
        const op = opAt(chunk, i);
        switch (op) {
            .op_add => {
                found_add = true;
                i += 1;
            },
            .op_constant, .op_set_local, .op_get_local, .op_get_builtin, .op_call, .op_atom, .op_get_upvalue, .op_set_upvalue, .op_tail_call => i += 2,
            .op_jump, .op_jump_if_false, .op_loop, .op_constant_long, .op_for_iter => i += 3,
            .op_closure => i += 2,
            else => i += 1,
        }
    }
    try std.testing.expect(found_add);
}

// Test 11: Builtin call
test "compile: builtin function call" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("print(42)", allocator);
    defer tc.deinit(allocator);

    const chunk = tc.getChunk();
    try std.testing.expect(!tc.result.hasErrors());

    var found_get_builtin = false;
    var found_call = false;
    var call_arg_count: u8 = 0;
    var i: usize = 0;
    while (i < chunk.code.items.len) {
        const op = opAt(chunk, i);
        switch (op) {
            .op_get_builtin => {
                found_get_builtin = true;
                try std.testing.expectEqual(@as(u8, 0), byteAt(chunk, i + 1));
                i += 2;
            },
            .op_call => {
                found_call = true;
                call_arg_count = byteAt(chunk, i + 1);
                i += 2;
            },
            .op_constant, .op_set_local, .op_get_local, .op_atom, .op_get_upvalue, .op_set_upvalue, .op_tail_call => i += 2,
            .op_jump, .op_jump_if_false, .op_loop, .op_constant_long, .op_for_iter => i += 3,
            .op_closure => i += 2,
            else => i += 1,
        }
    }
    try std.testing.expect(found_get_builtin);
    try std.testing.expect(found_call);
    try std.testing.expectEqual(@as(u8, 1), call_arg_count);
}

// Test 12: Variable shadowing
test "compile: variable shadowing" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("let x = 1\n{ let x = 2\nx }", allocator);
    defer tc.deinit(allocator);

    const chunk = tc.getChunk();
    try std.testing.expect(!tc.result.hasErrors());

    var get_local_slots: std.ArrayListUnmanaged(u8) = .empty;
    defer get_local_slots.deinit(allocator);

    var i: usize = 0;
    while (i < chunk.code.items.len) {
        const op = opAt(chunk, i);
        if (op == .op_get_local) {
            try get_local_slots.append(allocator, byteAt(chunk, i + 1));
            i += 2;
        } else if (op == .op_constant or op == .op_set_local or op == .op_get_builtin or op == .op_call or op == .op_atom or op == .op_get_upvalue or op == .op_set_upvalue or op == .op_tail_call) {
            i += 2;
        } else if (op == .op_jump or op == .op_jump_if_false or op == .op_loop or op == .op_constant_long or op == .op_for_iter) {
            i += 3;
        } else if (op == .op_closure) {
            i += 2;
        } else {
            i += 1;
        }
    }

    // Inner x should be at slot 2 (slot 0 = script, slot 1 = outer x, slot 2 = inner x)
    var found_slot_2 = false;
    for (get_local_slots.items) |slot| {
        if (slot == 2) found_slot_2 = true;
    }
    try std.testing.expect(found_slot_2);
}

// Test 13: String concatenation
test "compile: string concatenation ++" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("\"hello\" ++ \" world\"", allocator);
    defer tc.deinit(allocator);

    const chunk = tc.getChunk();
    try std.testing.expect(!tc.result.hasErrors());

    var found_concat = false;
    var i: usize = 0;
    while (i < chunk.code.items.len) {
        const op = opAt(chunk, i);
        if (op == .op_concat) {
            found_concat = true;
            i += 1;
        } else if (op == .op_constant or op == .op_set_local or op == .op_get_local or op == .op_get_builtin or op == .op_call or op == .op_atom or op == .op_get_upvalue or op == .op_set_upvalue or op == .op_tail_call) {
            i += 2;
        } else if (op == .op_jump or op == .op_jump_if_false or op == .op_loop or op == .op_constant_long or op == .op_for_iter) {
            i += 3;
        } else if (op == .op_closure) {
            i += 2;
        } else {
            i += 1;
        }
    }
    try std.testing.expect(found_concat);
}

// Test 14: Undefined variable error
test "compile: error on undefined variable" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("unknown_var", allocator);
    defer tc.deinit(allocator);

    try std.testing.expect(tc.result.hasErrors());
    try std.testing.expectEqual(@as(usize, 1), tc.result.errors.items.len);
    try std.testing.expectEqual(ErrorCode.E002, tc.result.errors.items[0].error_code);
}

// ── Phase 2 function/closure tests ─────────────────────────────────

// Test: Simple fn compiles to closure bytecode
test "compile: simple fn produces op_closure" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("fn add(a, b) { a + b }", allocator);
    defer tc.deinit(allocator);

    const chunk = tc.getChunk();
    try std.testing.expect(!tc.result.hasErrors());

    // Find op_closure in the script chunk.
    var found_closure = false;
    var i: usize = 0;
    while (i < chunk.code.items.len) {
        const op = opAt(chunk, i);
        if (op == .op_closure) {
            found_closure = true;
            // The constant should be an ObjFunction.
            const const_idx = byteAt(chunk, i + 1);
            const val = chunk.constants.items[const_idx];
            try std.testing.expect(val.isObj());
            try std.testing.expectEqual(obj_mod.ObjType.function, val.asObj().obj_type);
            const func = ObjFunction.fromObj(val.asObj());
            try std.testing.expectEqual(@as(u8, 2), func.arity);
            try std.testing.expectEqualStrings("add", func.name.?);
            break;
        }
        if (op == .op_constant or op == .op_set_local or op == .op_get_local or op == .op_get_builtin or op == .op_call or op == .op_atom or op == .op_get_upvalue or op == .op_set_upvalue or op == .op_tail_call) {
            i += 2;
        } else if (op == .op_jump or op == .op_jump_if_false or op == .op_loop or op == .op_constant_long or op == .op_for_iter) {
            i += 3;
        } else {
            i += 1;
        }
    }
    try std.testing.expect(found_closure);
}

// Test: Upvalue resolution across one nesting level
test "compile: upvalue resolution one level" {
    const allocator = std.testing.allocator;
    var tc = try testCompile(
        \\fn outer() {
        \\  let x = 1
        \\  fn inner() { x }
        \\  inner
        \\}
    , allocator);
    defer tc.deinit(allocator);

    try std.testing.expect(!tc.result.hasErrors());

    // The outer function should have its 'x' captured.
    // Inner function should have upvalue_count > 0.
    const chunk = tc.getChunk();
    // Find the outer function constant.
    for (chunk.constants.items) |val| {
        if (val.isObj() and val.asObj().obj_type == .function) {
            const func = ObjFunction.fromObj(val.asObj());
            if (func.name != null and std.mem.eql(u8, func.name.?, "outer")) {
                // Check that inner function within outer's chunk has upvalues.
                for (func.chunk.constants.items) |inner_val| {
                    if (inner_val.isObj() and inner_val.asObj().obj_type == .function) {
                        const inner_func = ObjFunction.fromObj(inner_val.asObj());
                        if (inner_func.name != null and std.mem.eql(u8, inner_func.name.?, "inner")) {
                            try std.testing.expect(inner_func.upvalue_count > 0);
                        }
                    }
                }
            }
        }
    }
}

// Test: Pipe desugaring: x |> f compiles to call bytecode
test "compile: pipe x |> f desugars to f(x)" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("fn f(x) { x }\nlet v = 5\nv |> f", allocator);
    defer tc.deinit(allocator);

    try std.testing.expect(!tc.result.hasErrors());

    // Should find an op_call with arg_count=1 for the pipe.
    const chunk = tc.getChunk();
    var found_call = false;
    var i: usize = 0;
    while (i < chunk.code.items.len) {
        const op = opAt(chunk, i);
        if (op == .op_call) {
            const arg_count = byteAt(chunk, i + 1);
            if (arg_count == 1) found_call = true;
            i += 2;
        } else if (op == .op_closure) {
            const const_idx = byteAt(chunk, i + 1);
            i += 2;
            // Skip upvalue descriptors.
            const val = chunk.constants.items[const_idx];
            if (val.isObj() and val.asObj().obj_type == .function) {
                const func = ObjFunction.fromObj(val.asObj());
                i += @as(usize, func.upvalue_count) * 2;
            }
        } else if (op == .op_constant or op == .op_set_local or op == .op_get_local or op == .op_get_builtin or op == .op_atom or op == .op_get_upvalue or op == .op_set_upvalue or op == .op_tail_call) {
            i += 2;
        } else if (op == .op_jump or op == .op_jump_if_false or op == .op_loop or op == .op_constant_long or op == .op_for_iter) {
            i += 3;
        } else {
            i += 1;
        }
    }
    try std.testing.expect(found_call);
}

// Test: Tail call: last call in fn body emits op_tail_call
test "compile: tail call in return position" {
    const allocator = std.testing.allocator;
    var tc = try testCompile(
        \\fn f(x) { x }
        \\fn g(x) { f(x) }
    , allocator);
    defer tc.deinit(allocator);

    try std.testing.expect(!tc.result.hasErrors());

    // Find function 'g' and check its chunk for op_tail_call.
    const chunk = tc.getChunk();
    var found_tail_call = false;
    for (chunk.constants.items) |val| {
        if (val.isObj() and val.asObj().obj_type == .function) {
            const func = ObjFunction.fromObj(val.asObj());
            if (func.name != null and std.mem.eql(u8, func.name.?, "g")) {
                for (func.chunk.code.items) |byte| {
                    if (@as(OpCode, @enumFromInt(byte)) == .op_tail_call) {
                        found_tail_call = true;
                    }
                }
            }
        }
    }
    try std.testing.expect(found_tail_call);
}

// Test: Non-tail call: call in let binding emits op_call
test "compile: non-tail call in let binding" {
    const allocator = std.testing.allocator;
    var tc = try testCompile(
        \\fn f(x) { x }
        \\fn g(x) { let y = f(x)
        \\y }
    , allocator);
    defer tc.deinit(allocator);

    try std.testing.expect(!tc.result.hasErrors());

    // Find function 'g' and check its chunk has op_call (not op_tail_call) for the let binding.
    const chunk = tc.getChunk();
    var found_regular_call = false;
    for (chunk.constants.items) |val| {
        if (val.isObj() and val.asObj().obj_type == .function) {
            const func = ObjFunction.fromObj(val.asObj());
            if (func.name != null and std.mem.eql(u8, func.name.?, "g")) {
                for (func.chunk.code.items) |byte| {
                    if (@as(OpCode, @enumFromInt(byte)) == .op_call) {
                        found_regular_call = true;
                    }
                }
            }
        }
    }
    try std.testing.expect(found_regular_call);
}

// Test: CompileResult now returns ObjClosure
test "compile: result contains ObjClosure" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("42", allocator);
    defer tc.deinit(allocator);

    try std.testing.expect(!tc.result.hasErrors());
    try std.testing.expectEqual(obj_mod.ObjType.closure, tc.result.closure.obj.obj_type);
    try std.testing.expectEqual(obj_mod.ObjType.function, tc.result.closure.function.obj.obj_type);
}
