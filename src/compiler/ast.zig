const std = @import("std");
const Allocator = std.mem.Allocator;
const token_mod = @import("token");
const Token = token_mod.Token;
const error_mod = @import("error");
const Diagnostic = error_mod.Diagnostic;

/// Abstract Syntax Tree for Zenith programs.
///
/// Uses a flat `MultiArrayList(Node)` pool where nodes reference each other
/// by `u32` index.  Nodes with more than 2 children store overflow indices
/// in `extra_data`.  This struct-of-arrays layout follows the same pattern
/// as Zig's own compiler for cache-friendly traversal.
pub const Ast = struct {
    nodes: std.MultiArrayList(Node),
    extra_data: std.ArrayListUnmanaged(u32),
    tokens: []const Token,
    source: []const u8,
    errors: error_mod.DiagnosticList,

    pub const Node = struct {
        tag: Tag,
        main_token: u32,
        data: Data,

        pub const Tag = enum(u8) {
            // ── Literals ────────────────────────────────────────────────
            int_literal,
            float_literal,
            string_literal,
            bool_literal,
            nil_literal,
            atom_literal,
            identifier,

            // ── Unary ───────────────────────────────────────────────────
            negate, // data.lhs = operand
            logical_not, // data.lhs = operand

            // ── Binary ──────────────────────────────────────────────────
            add,
            subtract,
            multiply,
            divide,
            modulo,
            equal,
            not_equal,
            less,
            greater,
            less_equal,
            greater_equal,
            logical_and,
            logical_or,
            concat, // ++ string concatenation

            // ── Expressions ─────────────────────────────────────────────
            grouped_expr, // data.lhs = inner expression
            block_expr, // data.lhs = extra_data start, data.rhs = extra_data end
            if_expr, // data.lhs = condition, data.rhs = extra_data index -> {then, else}
            call_expr, // data.lhs = callee, data.rhs = extra_data index -> {arg_start, arg_end}

            // ── Statements ──────────────────────────────────────────────
            let_decl, // data.lhs = name token index, data.rhs = initializer expression node
            assign_stmt, // data.lhs = target, data.rhs = value expression
            while_stmt, // data.lhs = condition, data.rhs = body
            for_stmt, // data.lhs = iterable, data.rhs = extra_data index -> {var_token, body}
            expr_stmt, // data.lhs = expression

            // ── Root ────────────────────────────────────────────────────
            root, // data.lhs = extra_data start, data.rhs = extra_data end
        };

        pub const Data = struct {
            lhs: u32,
            rhs: u32,
        };

        pub const Index = u32;

        /// Sentinel value meaning "no node".
        pub const null_node: Index = std.math.maxInt(u32);
    };

    /// Create an empty AST.
    pub fn init(tokens: []const Token, source: []const u8) Ast {
        return .{
            .nodes = .{},
            .extra_data = .empty,
            .tokens = tokens,
            .source = source,
            .errors = .{},
        };
    }

    /// Add a new AST node.  Returns the index of the added node.
    pub fn addNode(self: *Ast, node: Node, allocator: Allocator) !Node.Index {
        const idx: Node.Index = @intCast(self.nodes.len);
        try self.nodes.append(allocator, node);
        return idx;
    }

    /// Add a value to the extra_data array. Returns the index.
    pub fn addExtra(self: *Ast, data: u32, allocator: Allocator) !u32 {
        const idx: u32 = @intCast(self.extra_data.items.len);
        try self.extra_data.append(allocator, data);
        return idx;
    }

    /// Get the source text slice for a token by index.
    pub fn tokenSlice(self: *const Ast, token_index: u32) []const u8 {
        const tok = self.tokens[token_index];
        return self.source[tok.start..tok.end];
    }

    /// Release all allocated memory.
    pub fn deinit(self: *Ast, allocator: Allocator) void {
        self.nodes.deinit(allocator);
        self.extra_data.deinit(allocator);
        self.errors.deinit(allocator);
    }
};

// ═══════════════════════════════════════════════════════════════════════
// ── Tests ──────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════

test "Ast: addNode and index" {
    const allocator = std.testing.allocator;
    var ast = Ast.init(&.{}, "");
    defer ast.deinit(allocator);

    const idx = try ast.addNode(.{
        .tag = .int_literal,
        .main_token = 0,
        .data = .{ .lhs = 0, .rhs = 0 },
    }, allocator);
    try std.testing.expectEqual(@as(Ast.Node.Index, 0), idx);

    const idx2 = try ast.addNode(.{
        .tag = .add,
        .main_token = 1,
        .data = .{ .lhs = 0, .rhs = 0 },
    }, allocator);
    try std.testing.expectEqual(@as(Ast.Node.Index, 1), idx2);
}

test "Ast: addExtra and index" {
    const allocator = std.testing.allocator;
    var ast = Ast.init(&.{}, "");
    defer ast.deinit(allocator);

    const idx = try ast.addExtra(42, allocator);
    try std.testing.expectEqual(@as(u32, 0), idx);

    const idx2 = try ast.addExtra(99, allocator);
    try std.testing.expectEqual(@as(u32, 1), idx2);

    try std.testing.expectEqual(@as(u32, 42), ast.extra_data.items[0]);
    try std.testing.expectEqual(@as(u32, 99), ast.extra_data.items[1]);
}

test "Ast: Node.null_node sentinel" {
    try std.testing.expectEqual(std.math.maxInt(u32), Ast.Node.null_node);
}

test "Ast: all Tag variants exist" {
    // Compile-time check: referencing all tags ensures they exist.
    const tags = [_]Ast.Node.Tag{
        .int_literal, .float_literal, .string_literal, .bool_literal,
        .nil_literal,  .atom_literal,  .identifier,
        .negate,       .logical_not,
        .add,          .subtract,      .multiply,       .divide,
        .modulo,       .equal,         .not_equal,      .less,
        .greater,      .less_equal,    .greater_equal,
        .logical_and,  .logical_or,    .concat,
        .grouped_expr, .block_expr,    .if_expr,        .call_expr,
        .let_decl,     .assign_stmt,   .while_stmt,     .for_stmt,
        .expr_stmt,    .root,
    };
    try std.testing.expect(tags.len > 0);
}
