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
const error_mod = @import("error");
const Diagnostic = error_mod.Diagnostic;
const ErrorCode = error_mod.ErrorCode;
const Label = error_mod.Label;

/// Result of compilation.
pub const CompileResult = struct {
    chunk: Chunk,
    errors: std.ArrayListUnmanaged(Diagnostic),
    atom_table: std.StringHashMapUnmanaged(u32),
    atom_count: u32,

    pub fn deinit(self: *CompileResult, allocator: Allocator) void {
        // Free any ObjString constants in the chunk.
        for (self.chunk.constants.items) |val| {
            if (val.isObj()) {
                val.asObj().destroy(allocator);
            }
        }
        self.chunk.deinit(allocator);
        self.errors.deinit(allocator);
        // StringHashMap keys are slices into the AST source -- we don't own them.
        self.atom_table.deinit(allocator);
    }

    pub fn hasErrors(self: *const CompileResult) bool {
        for (self.errors.items) |d| {
            if (d.severity == .@"error") return true;
        }
        return false;
    }
};

/// Local variable tracking.
const Local = struct {
    name: []const u8,
    depth: i32,
};

/// Bytecode compiler -- walks AST nodes and emits opcodes into a Chunk.
pub const Compiler = struct {
    chunk: Chunk,
    ast: *const Ast,
    locals: [256]Local,
    local_count: u8,
    scope_depth: i32,
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

    /// Compile an AST into bytecodes.
    pub fn compile(ast_ptr: *const Ast, allocator: Allocator) !CompileResult {
        var self = Self{
            .chunk = .{},
            .ast = ast_ptr,
            .locals = undefined,
            .local_count = 0,
            .scope_depth = 0,
            .atom_table = .{},
            .atom_count = 0,
            .errors = .empty,
            .allocator = allocator,
        };

        // Find and compile the root node (last node in the AST).
        const root_idx: u32 = @intCast(ast_ptr.nodes.len - 1);
        const root_tag = ast_ptr.nodes.items(.tag)[root_idx];
        if (root_tag == .root) {
            const data = ast_ptr.nodes.items(.data)[root_idx];
            const stmts = ast_ptr.extra_data.items[data.lhs..data.rhs];
            try self.compileStatements(stmts);
        }

        // Emit return at end.
        try self.emitOp(.op_return, 0);

        return .{
            .chunk = self.chunk,
            .errors = self.errors,
            .atom_table = self.atom_table,
            .atom_count = self.atom_count,
        };
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
                } else if (tag != .let_decl and tag != .while_stmt and tag != .for_stmt and tag != .assign_stmt) {
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
            .root => {}, // handled above
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

    fn resolveBuiltin(_: *const Self, name: []const u8) ?u8 {
        for (builtin_names, 0..) |bname, idx| {
            if (std.mem.eql(u8, bname, name)) {
                return @intCast(idx);
            }
        }
        return null;
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

    // ── While statement ───────────────────────────────────────────────

    fn compileWhileStmt(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        const loop_start: u32 = @intCast(self.chunk.code.items.len);

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
        self.locals[self.local_count] = .{ .name = "__iter__", .depth = self.scope_depth };
        self.local_count += 1;

        // Local for the index (hidden).
        self.locals[self.local_count] = .{ .name = "__idx__", .depth = self.scope_depth };
        self.local_count += 1;

        // Push placeholder for loop variable value.
        try self.emitOp(.op_nil, line);
        self.locals[self.local_count] = .{ .name = var_name, .depth = self.scope_depth };
        self.local_count += 1;

        const loop_start: u32 = @intCast(self.chunk.code.items.len);

        // Emit for_iter: checks if iteration complete, sets loop var, or jumps past body.
        const exit_jump = try self.emitJump(.op_for_iter, line);

        // Compile body.
        try self.compileNode(body_node);
        try self.emitOp(.op_pop, line); // discard body result

        // Loop back.
        try self.emitLoop(loop_start, line);

        try self.patchJump(exit_jump);

        // End scope: pop loop locals.
        self.endScope();

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

        // Compile all statements. The last one should leave its value on stack.
        for (stmts, 0..) |stmt_idx, i| {
            try self.compileNode(stmt_idx);
            if (i < stmts.len - 1) {
                const stmt_tag = self.ast.nodes.items(.tag)[stmt_idx];
                // Statements that don't produce values (let, while, for, assign) don't need pop.
                if (stmt_tag == .expr_stmt) {
                    // expr_stmt already pops its value
                } else if (stmt_tag != .let_decl and stmt_tag != .while_stmt and stmt_tag != .for_stmt and stmt_tag != .assign_stmt) {
                    try self.emitOp(.op_pop, self.getLine(stmt_idx));
                }
            }
        }

        // End scope: pop locals but we need to keep the last expression value.
        // Count how many locals were added in this scope.
        const last_stmt_idx = stmts[stmts.len - 1];
        const last_tag = self.ast.nodes.items(.tag)[last_stmt_idx];

        // Count locals to pop.
        var pop_count: u32 = 0;
        while (self.local_count > 0 and self.locals[self.local_count - 1].depth > self.scope_depth - 1) {
            pop_count += 1;
            self.local_count -= 1;
        }
        self.scope_depth -= 1;

        // If the last item is an expression (not a statement), we need to
        // preserve its value while popping locals from underneath it.
        if (last_tag != .let_decl and last_tag != .while_stmt and last_tag != .for_stmt and last_tag != .assign_stmt and last_tag != .expr_stmt) {
            // The result is on top of stack. Locals are below it.
            // We need to pop the locals beneath the result.
            // Strategy: set_local to slot 0 (temp), pop locals, get_local 0.
            // Actually, simpler: for each local to pop, we emit a pop BEFORE the result.
            // But the result is already on top...
            // Best approach: track the base slot, and after leaving the scope, emit pops.
            // The result value is on top. We need to slide it down past the locals.
            // For now, use a simple approach: stash the result, pop locals, push result back.
            // Actually the simplest and most common approach: just pop the locals from under.
            // Since we can't easily do that, we'll reorganize.
            // Better approach: result is on top of stack. Locals are the N slots below it.
            // We can use set_local to the base slot, pop everything, then get_local base.
            // ... this is getting complex. Let's just use the standard approach:
            // Pop locals that are ABOVE the result. Since locals are below, we actually
            // need a different strategy.
            //
            // Standard Lox approach for blocks: the result is on top. Pop all locals
            // from the scope, which means popping from below the result. Most VMs handle
            // this by just noting that the VM should pop N items from under the top.
            //
            // Simple approach for now: emit OP_POPs. The result on top stays, and we
            // pop the locals from underneath... but standard OP_POP removes the top.
            //
            // Actually: re-think. When we exit a block like { let x = 1; x + 2 }:
            //   Stack: [..., 1 (x), 3 (result)]
            // We need to end up with: [..., 3]
            // So we need to swap top with underneath, then pop. Or we can use a
            // rotate/swap operation. Since we don't have that, let's do:
            //   1. For each local, emit a pop to remove it from under the result.
            //      But op_pop removes the TOP item, not underneath.
            //
            // Real solution: emit the pops BETWEEN the last-but-one statement and
            // the last expression. But we already compiled everything.
            //
            // Practical solution used in Crafting Interpreters: for blocks that
            // produce a value, we handle it by not popping the last expression.
            // The scope locals get popped explicitly. The trick is: the locals ARE
            // the stack slots. When we pop locals, the result stays because it's
            // on top of them.
            //
            // Wait -- that's exactly what happens:
            //   Stack: [local_x=1, result=3]
            //   Pop locals (1 pop): removes top -> removes result!
            //
            // Hmm. The issue is that locals AND the result are interleaved.
            // Actually no: locals are assigned during `let`, and the result
            // expression is evaluated after all lets. So:
            //   let x = 1   -> stack: [1]
            //   x + 2       -> stack: [1, 3]
            //   end scope   -> need to pop local x, keep 3
            //
            // So we need to pop 1 element from UNDER the top. This requires
            // either a swap+pop or a special opcode.
            //
            // For simplicity, I'll handle this at the block level:
            // After evaluating all statements including the final expression,
            // if there are locals to pop, we need to slide the result down.
            // We can do this by emitting set_local to the base_slot position,
            // then popping the remaining locals, then getting from base_slot.
            //
            // Actually even simpler: just pop the excess. The result is on top.
            // Pop count locals from underneath. Since we can't pop from underneath,
            // we'll just pop from top (destroying result), but save/restore it.
            // We don't have save/restore... Let's just emit pop_count OP_POPs
            // and note that for block expressions, the compiler "swaps" before popping.

            // Practical approach: we need some mechanism.
            // Simplest: just emit extra pops. The result on top gets destroyed.
            // Then emit the result again? No, we can't replay.
            //
            // OK: cleanest approach for Phase 1. We don't have closures or complex
            // scoping yet. Let's just handle it with a simple pattern:
            // - Before the last expression, note the base slot.
            // - After compiling the last expression (result on top),
            //   set it into the base_slot, pop all locals above base, then get base_slot.
            // But this changes the slot...
            //
            // SIMPLEST correct approach: after compiling block contents, the result
            // is on top and locals are below. To remove locals while keeping result:
            // If pop_count > 0, we set_local to the first local slot of this scope,
            // then pop (pop_count - 1) times, and the result now sits in that slot
            // where it'll be used as the "value" of the block expression.
            // BUT: this slot is about to go out of scope.
            //
            // ACTUALLY: the simplest correct pattern used by many compilers is:
            // Pop count is the number of locals. Each local IS a stack slot.
            // The last value (block result) is at stack_top. The locals are below it.
            // We need pop_count "swaps" or one rotate. Since we don't have rotate,
            // let's just do: for each pop, we emit op_pop but NOTE: op_pop removes
            // the TOP. So if we do pop_count op_pops, we remove the result + (pop_count-1) locals.
            // That's wrong.
            //
            // Final approach: I'll use a two-step strategy:
            // 1. When there's a block result and locals to clean up,
            //    rewrite the compilation so we endScope BEFORE the last expression.
            //    This way locals are popped, then the last expression is evaluated
            //    with access to parent scope only.
            //
            // But the last expression might reference block locals (like `x + 2` where x is local).
            // So we can't pop locals before evaluating it.
            //
            // THE ACTUAL STANDARD APPROACH: Don't pop at compile time. Instead,
            // the endScope emits pops for every local EXCEPT that we account for
            // the block result being on top. So we:
            //   1. Compile last expression -> result on top of locals
            //   2. For each local, copy the top value down by one slot:
            //      set_local(base_slot), pop remaining locals.
            //   Actually even simpler for a single-pass compiler:
            //   The result is on top. We can do pop_count OP_POPs *below* it by
            //   just accepting that the VM maintains stack_top correctly and we
            //   emit a sequence of instructions that effectively does:
            //   temp = pop(); for N: pop(); push(temp);
            //
            // For Phase 1 correctness and simplicity, I'll use this approach:
            // After compiling all block stmts, if there are locals AND the last
            // item leaves a value:
            //   - Store result in a temporary (set_local to first scope slot).
            //   - Pop the remaining scope locals.
            //   - Get the temporary back (get_local first scope slot).
            //   Actually this still leaves the temp local on stack.
            //
            // CLEANEST: Let me just use pop_count op_pop AFTER the result.
            // This means the block result gets popped too. But then we need
            // to recompute the result. That's not possible.
            //
            // OK, I'm overcomplicating this. Let me use the approach from Crafting
            // Interpreters: endScope pops ALL locals, and block expressions
            // handle the result by setting the result into slot[base], popping
            // the rest. Actually CI doesn't have block expressions.
            //
            // FINAL APPROACH for Phase 1:
            // I will NOT emit pops for scope cleanup in block expressions when
            // the last element is an expression producing a value. Instead:
            // - The last expression value is on top of the stack.
            // - I record the base_slot at scope entry.
            // - At scope exit, I know how many slots to reclaim.
            // - I emit OP_POP for (local_count - base) - but FIRST I need to
            //   rotate the value down. Since I don't have rotate, I'll use:
            //   Emit nothing. Just adjust local_count. The VM stack slots
            //   for locals become "dead" but aren't explicitly popped.
            //   Then on the next scope/function entry, they'll be overwritten.
            //   This "leaks" stack slots but is correct for Phase 1 where we
            //   don't have deep nesting and stack overflow checks will catch issues.
            //
            // WAIT. Actually the simplest and CORRECT approach is: pop EVERYTHING
            // including the result, and note that we need the result. Before popping,
            // stash it in a local slot.
            //
            // Let me just use the "pop under" approach:
            //   result is at stack[top]
            //   locals are at stack[base..top-1]
            //   We want stack[base-1] = result, top = base
            //   So: conceptually set stack[base_before_scope] = stack[top], top = base_before_scope + 1
            //   But we can't do that with just set_local and pop.
            //
            //   set_local(base_before_scope) -> stack[base] = result (top unchanged)
            //   Then pop (pop_count) times -> removes top, top, top...
            //   After pop_count pops: top = original_top - pop_count
            //   original_top = base + pop_count + 1 (locals + result)
            //   After pops: top = base + 1, stack[base] = result. CORRECT!

            if (pop_count > 0) {
                // base_slot is the first local in this scope
                const base_slot: u8 = self.local_count; // after decrementing
                try self.emitOp(.op_set_local, self.getLine(node_idx));
                try self.emitByte(base_slot, self.getLine(node_idx));
                // Pop the remaining (pop_count) slots including the duplicated result
                for (0..pop_count) |_| {
                    try self.emitOp(.op_pop, self.getLine(node_idx));
                }
                // Now get the result back from base_slot
                try self.emitOp(.op_get_local, self.getLine(node_idx));
                try self.emitByte(base_slot, self.getLine(node_idx));
            }
        } else {
            // Last statement is a statement (let, while, etc.) that doesn't leave a value.
            // Pop all locals and emit nil as the block's value.
            for (0..pop_count) |_| {
                try self.emitOp(.op_pop, self.getLine(node_idx));
            }
            try self.emitOp(.op_nil, self.getLine(node_idx));
        }
    }

    // ── Call expression ───────────────────────────────────────────────

    fn compileCallExpr(self: *Self, node_data: Node.Data, node_idx: u32) Error!void {
        // Compile callee.
        try self.compileNode(node_data.lhs);

        // Get argument range from extra_data.
        const extra_idx = node_data.rhs;
        const arg_start = self.ast.extra_data.items[extra_idx];
        const arg_end = self.ast.extra_data.items[extra_idx + 1];
        const arg_indices = self.ast.extra_data.items[arg_start..arg_end];

        // Compile arguments left-to-right.
        for (arg_indices) |arg_idx| {
            try self.compileNode(arg_idx);
        }

        // Emit call with arg count.
        const arg_count: u8 = @intCast(arg_indices.len);
        try self.emitOp(.op_call, self.getLine(node_idx));
        try self.emitByte(arg_count, self.getLine(node_idx));
    }

    // ── Scope management ──────────────────────────────────────────────

    fn beginScope(self: *Self) void {
        self.scope_depth += 1;
    }

    fn endScope(self: *Self) void {
        // Pop all locals at current depth.
        while (self.local_count > 0 and self.locals[self.local_count - 1].depth >= self.scope_depth) {
            self.local_count -= 1;
        }
        self.scope_depth -= 1;
    }

    // ── Bytecode emission helpers ─────────────────────────────────────

    fn emitOp(self: *Self, op: OpCode, line: u32) Error!void {
        try self.chunk.write(@intFromEnum(op), line, self.allocator);
    }

    fn emitByte(self: *Self, byte: u8, line: u32) Error!void {
        try self.chunk.write(byte, line, self.allocator);
    }

    fn emitConstant(self: *Self, val: Value, line: u32) Error!void {
        const idx = try self.chunk.addConstant(val, self.allocator);
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
        return @intCast(self.chunk.code.items.len - 2);
    }

    fn patchJump(self: *Self, offset: u32) Error!void {
        const jump: u32 = @intCast(self.chunk.code.items.len - offset - 2);
        if (jump > 0xFFFF) {
            // Jump too large -- would need a long jump instruction.
            return error.Overflow;
        }
        self.chunk.code.items[offset] = @intCast((jump >> 8) & 0xFF);
        self.chunk.code.items[offset + 1] = @intCast(jump & 0xFF);
    }

    fn emitLoop(self: *Self, loop_start: u32, line: u32) Error!void {
        try self.emitOp(.op_loop, line);
        const offset: u32 = @intCast(self.chunk.code.items.len - loop_start + 2);
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

// Test 1: Compiling `42` emits [op_constant(42), op_return]
test "compile: integer literal 42" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("42", allocator);
    defer tc.deinit(allocator);

    const chunk = &tc.result.chunk;
    try std.testing.expect(!tc.result.hasErrors());

    // Should be: expr_stmt wraps the literal, so:
    // op_constant(42), op_pop (expr_stmt), op_return
    // Actually for a single top-level expression: the root compiles statements.
    // The top-level "42" becomes expr_stmt which compiles the int then op_pop.
    // Then op_return at the end.
    // But wait: if there's only one statement at root level, compileStatements
    // doesn't pop it because it's the last one. But expr_stmt ALWAYS emits pop.
    // So: op_constant, op_pop, op_return.
    try std.testing.expectEqual(OpCode.op_constant, opAt(chunk, 0));
    // constant index
    const const_idx = byteAt(chunk, 1);
    try std.testing.expectEqual(@as(i32, 42), chunk.constants.items[const_idx].asInt());
    // expr_stmt emits pop
    try std.testing.expectEqual(OpCode.op_pop, opAt(chunk, 2));
    try std.testing.expectEqual(OpCode.op_return, opAt(chunk, 3));
}

// Test 2: Compiling `1 + 2` emits [op_constant(1), op_constant(2), op_add, op_return]
test "compile: addition 1 + 2" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("1 + 2", allocator);
    defer tc.deinit(allocator);

    const chunk = &tc.result.chunk;
    try std.testing.expect(!tc.result.hasErrors());

    try std.testing.expectEqual(OpCode.op_constant, opAt(chunk, 0));
    try std.testing.expectEqual(@as(i32, 1), chunk.constants.items[byteAt(chunk, 1)].asInt());
    try std.testing.expectEqual(OpCode.op_constant, opAt(chunk, 2));
    try std.testing.expectEqual(@as(i32, 2), chunk.constants.items[byteAt(chunk, 3)].asInt());
    try std.testing.expectEqual(OpCode.op_add, opAt(chunk, 4));
}

// Test 3: Compiling `1 + 2 * 3` emits correct order (multiply before add)
test "compile: precedence 1 + 2 * 3" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("1 + 2 * 3", allocator);
    defer tc.deinit(allocator);

    const chunk = &tc.result.chunk;
    try std.testing.expect(!tc.result.hasErrors());

    // Should be: const(1), const(2), const(3), multiply, add (postfix order)
    try std.testing.expectEqual(OpCode.op_constant, opAt(chunk, 0)); // 1
    try std.testing.expectEqual(OpCode.op_constant, opAt(chunk, 2)); // 2
    try std.testing.expectEqual(OpCode.op_constant, opAt(chunk, 4)); // 3
    try std.testing.expectEqual(OpCode.op_multiply, opAt(chunk, 6));
    try std.testing.expectEqual(OpCode.op_add, opAt(chunk, 7));
}

// Test 4: Compiling `-42` emits [op_constant(42), op_negate]
test "compile: unary negate" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("-42", allocator);
    defer tc.deinit(allocator);

    const chunk = &tc.result.chunk;
    try std.testing.expect(!tc.result.hasErrors());

    // -42: op_constant(42), op_negate, op_pop (expr_stmt), op_return
    try std.testing.expectEqual(OpCode.op_constant, opAt(chunk, 0));
    try std.testing.expectEqual(@as(i32, 42), chunk.constants.items[byteAt(chunk, 1)].asInt());
    try std.testing.expectEqual(OpCode.op_negate, opAt(chunk, 2));
}

// Test 4b: Negate a local variable via expression
test "compile: negate local in block" {
    const allocator = std.testing.allocator;
    // Use a block so let and -x are clearly separate
    var tc = try testCompile("let x = 5\nlet y = -x", allocator);
    defer tc.deinit(allocator);

    const chunk = &tc.result.chunk;
    try std.testing.expect(!tc.result.hasErrors());

    // Find the negate instruction - it should follow a get_local
    var found_negate = false;
    var found_get_local_before_negate = false;
    var i: usize = 0;
    while (i < chunk.code.items.len) {
        const op = opAt(chunk, i);
        if (op == .op_get_local) {
            // Check if next instruction is negate
            if (i + 2 < chunk.code.items.len and opAt(chunk, i + 2) == .op_negate) {
                found_get_local_before_negate = true;
                found_negate = true;
            }
            i += 2;
        } else if (op == .op_constant or op == .op_set_local or op == .op_get_builtin or op == .op_call or op == .op_atom) {
            i += 2;
        } else if (op == .op_jump or op == .op_jump_if_false or op == .op_loop or op == .op_constant_long or op == .op_for_iter) {
            i += 3;
        } else {
            i += 1;
        }
    }
    try std.testing.expect(found_get_local_before_negate);
    try std.testing.expect(found_negate);
}

// Test 5: Compiling `let x = 42` records x at local slot 0
test "compile: let binding creates local" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("let x = 42", allocator);
    defer tc.deinit(allocator);

    const chunk = &tc.result.chunk;
    try std.testing.expect(!tc.result.hasErrors());

    // let x = 42: op_constant(42). The value stays on stack as the local.
    try std.testing.expectEqual(OpCode.op_constant, opAt(chunk, 0));
    try std.testing.expectEqual(@as(i32, 42), chunk.constants.items[byteAt(chunk, 1)].asInt());
}

// Test 6: Compiling `let x = 1\nlet y = 2\nx + y`
test "compile: multiple locals and access" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("let x = 1\nlet y = 2\nx + y", allocator);
    defer tc.deinit(allocator);

    const chunk = &tc.result.chunk;
    try std.testing.expect(!tc.result.hasErrors());

    // Find the get_local instructions for x + y
    // x should be slot 0, y should be slot 1
    var get_local_slots: std.ArrayListUnmanaged(u8) = .empty;
    defer get_local_slots.deinit(allocator);

    var i: usize = 0;
    while (i < chunk.code.items.len) {
        const op = opAt(chunk, i);
        if (op == .op_get_local) {
            try get_local_slots.append(allocator, byteAt(chunk, i + 1));
            i += 2;
        } else if (op == .op_constant or op == .op_set_local or op == .op_get_builtin or op == .op_call or op == .op_atom) {
            i += 2;
        } else if (op == .op_jump or op == .op_jump_if_false or op == .op_loop or op == .op_constant_long or op == .op_for_iter) {
            i += 3;
        } else {
            i += 1;
        }
    }

    try std.testing.expect(get_local_slots.items.len >= 2);
    try std.testing.expectEqual(@as(u8, 0), get_local_slots.items[0]); // x at slot 0
    try std.testing.expectEqual(@as(u8, 1), get_local_slots.items[1]); // y at slot 1
}

// Test 7: Compiling `if true { 1 } else { 2 }` emits conditional jump
test "compile: if-else expression" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("if true { 1 } else { 2 }", allocator);
    defer tc.deinit(allocator);

    const chunk = &tc.result.chunk;
    try std.testing.expect(!tc.result.hasErrors());

    // Should contain: op_true, op_jump_if_false, ..., op_jump, ..., op_return
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
            .op_constant, .op_set_local, .op_get_local, .op_get_builtin, .op_call, .op_atom => {
                i += 2;
            },
            .op_loop, .op_constant_long, .op_for_iter => {
                i += 3;
            },
            else => i += 1,
        }
    }
    try std.testing.expect(found_true);
    try std.testing.expect(found_jif);
    try std.testing.expect(found_jump);
}

// Test 8: Compiling `while x > 0 { x = x - 1 }` emits loop
test "compile: while loop" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("let x = 3\nwhile x > 0 { x = x - 1 }", allocator);
    defer tc.deinit(allocator);

    const chunk = &tc.result.chunk;
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
            .op_constant, .op_set_local, .op_get_local, .op_get_builtin, .op_call, .op_atom => i += 2,
            .op_jump, .op_constant_long, .op_for_iter => i += 3,
            else => i += 1,
        }
    }
    try std.testing.expect(found_loop);
    try std.testing.expect(found_jif);
}

// Test 9: Compiling `:ok` emits op_atom
test "compile: atom literal" {
    const allocator = std.testing.allocator;
    var tc = try testCompile(":ok", allocator);
    defer tc.deinit(allocator);

    try std.testing.expect(!tc.result.hasErrors());
    // Atom should be interned with id 0.
    try std.testing.expectEqual(@as(u32, 1), tc.result.atom_count);
    try std.testing.expect(tc.result.atom_table.contains("ok"));
}

// Test 10: Block expression with local scope
test "compile: block expression with local scope" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("{ let x = 1\nx + 2 }", allocator);
    defer tc.deinit(allocator);

    const chunk = &tc.result.chunk;
    try std.testing.expect(!tc.result.hasErrors());

    // Should have at least op_constant(1), op_get_local, op_constant(2), op_add
    var found_add = false;
    var i: usize = 0;
    while (i < chunk.code.items.len) {
        const op = opAt(chunk, i);
        switch (op) {
            .op_add => {
                found_add = true;
                i += 1;
            },
            .op_constant, .op_set_local, .op_get_local, .op_get_builtin, .op_call, .op_atom => i += 2,
            .op_jump, .op_jump_if_false, .op_loop, .op_constant_long, .op_for_iter => i += 3,
            else => i += 1,
        }
    }
    try std.testing.expect(found_add);
}

// Test 11: Compiling `print(42)` emits get_builtin + call
test "compile: builtin function call" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("print(42)", allocator);
    defer tc.deinit(allocator);

    const chunk = &tc.result.chunk;
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
                // builtin index 0 = print
                try std.testing.expectEqual(@as(u8, 0), byteAt(chunk, i + 1));
                i += 2;
            },
            .op_call => {
                found_call = true;
                call_arg_count = byteAt(chunk, i + 1);
                i += 2;
            },
            .op_constant, .op_set_local, .op_get_local, .op_atom => i += 2,
            .op_jump, .op_jump_if_false, .op_loop, .op_constant_long, .op_for_iter => i += 3,
            else => i += 1,
        }
    }
    try std.testing.expect(found_get_builtin);
    try std.testing.expect(found_call);
    try std.testing.expectEqual(@as(u8, 1), call_arg_count);
}

// Test 12: Shadowing -- inner block redefines variable
test "compile: variable shadowing" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("let x = 1\n{ let x = 2\nx }", allocator);
    defer tc.deinit(allocator);

    const chunk = &tc.result.chunk;
    try std.testing.expect(!tc.result.hasErrors());

    // Inner x should be at slot 1, outer x at slot 0
    // The get_local inside the block should reference slot 1 (inner x)
    var get_local_slots: std.ArrayListUnmanaged(u8) = .empty;
    defer get_local_slots.deinit(allocator);

    var i: usize = 0;
    while (i < chunk.code.items.len) {
        const op = opAt(chunk, i);
        if (op == .op_get_local) {
            try get_local_slots.append(allocator, byteAt(chunk, i + 1));
            i += 2;
        } else if (op == .op_constant or op == .op_set_local or op == .op_get_builtin or op == .op_call or op == .op_atom) {
            i += 2;
        } else if (op == .op_jump or op == .op_jump_if_false or op == .op_loop or op == .op_constant_long or op == .op_for_iter) {
            i += 3;
        } else {
            i += 1;
        }
    }

    // Should find at least one get_local for slot 1 (inner x)
    var found_slot_1 = false;
    for (get_local_slots.items) |slot| {
        if (slot == 1) found_slot_1 = true;
    }
    try std.testing.expect(found_slot_1);
}

// Test 13: String concatenation operator
test "compile: string concatenation ++" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("\"hello\" ++ \" world\"", allocator);
    defer tc.deinit(allocator);

    const chunk = &tc.result.chunk;
    try std.testing.expect(!tc.result.hasErrors());

    var found_concat = false;
    var i: usize = 0;
    while (i < chunk.code.items.len) {
        const op = opAt(chunk, i);
        if (op == .op_concat) {
            found_concat = true;
            i += 1;
        } else if (op == .op_constant or op == .op_set_local or op == .op_get_local or op == .op_get_builtin or op == .op_call or op == .op_atom) {
            i += 2;
        } else if (op == .op_jump or op == .op_jump_if_false or op == .op_loop or op == .op_constant_long or op == .op_for_iter) {
            i += 3;
        } else {
            i += 1;
        }
    }
    try std.testing.expect(found_concat);
}

// Test: error accumulation
test "compile: error on undefined variable" {
    const allocator = std.testing.allocator;
    var tc = try testCompile("unknown_var", allocator);
    defer tc.deinit(allocator);

    try std.testing.expect(tc.result.hasErrors());
    try std.testing.expectEqual(@as(usize, 1), tc.result.errors.items.len);
    try std.testing.expectEqual(ErrorCode.E002, tc.result.errors.items[0].error_code);
}
