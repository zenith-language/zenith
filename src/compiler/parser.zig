const std = @import("std");
const Allocator = std.mem.Allocator;
const token_mod = @import("token");
const Token = token_mod.Token;
const Tag = token_mod.Tag;
const ast_mod = @import("ast");
const Ast = ast_mod.Ast;
const Node = Ast.Node;
const error_mod = @import("error");
const Diagnostic = error_mod.Diagnostic;
const ErrorCode = error_mod.ErrorCode;
const Label = error_mod.Label;

/// Recursive descent parser with Pratt operator precedence.
///
/// Consumes the flat token array produced by the lexer and builds an AST
/// using MultiArrayList(Node) storage.  Error recovery uses panic-mode
/// synchronisation: on a parse error, the parser skips tokens until it
/// reaches a statement boundary (`let`, `if`, `while`, `for`, `}`, EOF).
pub const Parser = struct {
    tokens: []const Token,
    source: []const u8,
    pos: u32,
    ast: Ast,
    allocator: Allocator,
    /// Tracks nesting depth inside function bodies (fn/lambda).
    /// Used to validate that `return` only appears inside functions.
    fn_depth: u32 = 0,

    pub const Error = error{ParseError} || Allocator.Error;

    /// Parse a token stream into an AST.
    pub fn parse(tokens: []const Token, source: []const u8, allocator: Allocator) Error!Ast {
        var self = Parser{
            .tokens = tokens,
            .source = source,
            .pos = 0,
            .ast = Ast.init(tokens, source),
            .allocator = allocator,
        };

        // Parse top-level statements into a root node.
        var stmts: std.ArrayListUnmanaged(u32) = .empty;
        defer stmts.deinit(allocator);

        while (!self.atEnd()) {
            if (self.parseStatement()) |stmt| {
                try stmts.append(allocator, stmt);
            } else |_| {
                // Error recovery: synchronize to next statement boundary.
                self.synchronize();
            }
        }

        // Create root node with extra_data range for statement list.
        const extra_start: u32 = @intCast(self.ast.extra_data.items.len);
        for (stmts.items) |s| {
            _ = try self.ast.addExtra(s, allocator);
        }
        const extra_end: u32 = @intCast(self.ast.extra_data.items.len);

        _ = try self.ast.addNode(.{
            .tag = .root,
            .main_token = 0,
            .data = .{ .lhs = extra_start, .rhs = extra_end },
        }, allocator);

        return self.ast;
    }

    // ── Statement parsing ──────────────────────────────────────────────

    fn parseStatement(self: *Parser) Error!Node.Index {
        return switch (self.peekTag()) {
            .kw_let => self.parseLetDecl(),
            .kw_while => self.parseWhileStmt(),
            .kw_for => self.parseForStmt(),
            .kw_fn => self.parseFnDecl(),
            .kw_return => self.parseReturnStmt(),
            .kw_type => self.parseTypeDecl(),
            .kw_select => self.parseSelectExpr(),
            else => self.parseExprStmt(),
        };
    }

    fn parseLetDecl(self: *Parser) Error!Node.Index {
        const let_tok = self.pos;
        self.advance(); // consume 'let'

        // Expect identifier.
        if (self.peekTag() != .identifier) {
            try self.emitError("expected identifier after 'let'");
            return error.ParseError;
        }
        const name_tok = self.pos;
        self.advance(); // consume identifier

        // Expect '='.
        if (self.peekTag() != .equal) {
            try self.emitError("expected '=' after variable name");
            return error.ParseError;
        }
        self.advance(); // consume '='

        // Parse initializer expression.
        const initializer = try self.parseExpression();

        return self.ast.addNode(.{
            .tag = .let_decl,
            .main_token = let_tok,
            .data = .{ .lhs = name_tok, .rhs = initializer },
        }, self.allocator);
    }

    fn parseWhileStmt(self: *Parser) Error!Node.Index {
        const while_tok = self.pos;
        self.advance(); // consume 'while'

        // Parse condition.
        const condition = try self.parseExpression();

        // Parse body (block).
        const body = try self.parseBlockExpr();

        return self.ast.addNode(.{
            .tag = .while_stmt,
            .main_token = while_tok,
            .data = .{ .lhs = condition, .rhs = body },
        }, self.allocator);
    }

    fn parseForStmt(self: *Parser) Error!Node.Index {
        const for_tok = self.pos;
        self.advance(); // consume 'for'

        // Expect loop variable identifier.
        if (self.peekTag() != .identifier) {
            try self.emitError("expected identifier after 'for'");
            return error.ParseError;
        }
        const var_tok = self.pos;
        self.advance(); // consume identifier

        // Expect 'in'.
        if (self.peekTag() != .kw_in) {
            try self.emitError("expected 'in' after loop variable");
            return error.ParseError;
        }
        self.advance(); // consume 'in'

        // Parse iterable expression.
        const iterable = try self.parseExpression();

        // Parse body (block).
        const body = try self.parseBlockExpr();

        // Store var_tok and body in extra_data.
        const extra_idx = try self.ast.addExtra(var_tok, self.allocator);
        _ = try self.ast.addExtra(body, self.allocator);

        return self.ast.addNode(.{
            .tag = .for_stmt,
            .main_token = for_tok,
            .data = .{ .lhs = iterable, .rhs = extra_idx },
        }, self.allocator);
    }

    fn parseExprStmt(self: *Parser) Error!Node.Index {
        const expr = try self.parseExpression();

        // Check for assignment: if next token is '=' and expr is an identifier.
        if (self.peekTag() == .equal) {
            self.advance(); // consume '='
            const value = try self.parseExpression();
            const tags = self.ast.nodes.items(.tag);
            const main_token = self.ast.nodes.items(.main_token)[expr];
            if (tags[expr] == .identifier) {
                return self.ast.addNode(.{
                    .tag = .assign_stmt,
                    .main_token = main_token,
                    .data = .{ .lhs = expr, .rhs = value },
                }, self.allocator);
            }
        }

        return self.ast.addNode(.{
            .tag = .expr_stmt,
            .main_token = self.ast.nodes.items(.main_token)[expr],
            .data = .{ .lhs = expr, .rhs = 0 },
        }, self.allocator);
    }

    // ── Expression parsing (Pratt) ─────────────────────────────────────

    const Precedence = enum(u8) {
        none,
        or_prec, // or
        pipe_prec, // |>
        and_prec, // and
        equality, // == !=
        comparison, // < > <= >=
        additive, // + - ++
        multiplicative, // * / %
        unary, // - not
        call, // f(x)
        primary, // literals, identifiers, grouped
    };

    fn parseExpression(self: *Parser) Error!Node.Index {
        return self.parsePrecedence(.or_prec);
    }

    fn parsePrecedence(self: *Parser, min_prec: Precedence) Error!Node.Index {
        // Prefix (unary / primary).
        var left = try self.parsePrefix();

        // Infix (binary operators, left-associative).
        while (true) {
            const prec = self.infixPrecedence(self.peekTag());
            if (@intFromEnum(prec) < @intFromEnum(min_prec)) break;

            left = try self.parseInfix(left, prec);
        }

        return left;
    }

    fn parsePrefix(self: *Parser) Error!Node.Index {
        return switch (self.peekTag()) {
            .minus => self.parseUnaryNegate(),
            .kw_not => self.parseUnaryNot(),
            .left_paren => self.parseGroupedOrTuple(),
            .left_brace => self.parseBraceExpr(),
            .left_bracket => self.parseListLiteral(),
            .kw_if => self.parseIfExpr(),
            .kw_fn => self.parseFnExpr(),
            .pipe => self.parseLambda(),
            .int_literal => self.parseLiteral(.int_literal),
            .float_literal => self.parseLiteral(.float_literal),
            .string_literal => self.parseLiteral(.string_literal),
            .atom_literal => self.parseLiteral(.atom_literal),
            .kw_true => self.parseLiteral(.bool_literal),
            .kw_false => self.parseLiteral(.bool_literal),
            .kw_nil => self.parseLiteral(.nil_literal),
            .identifier => self.parseIdentifier(),
            .kw_match => self.parseMatchExpr(),
            else => {
                try self.emitError("expected expression");
                return error.ParseError;
            },
        };
    }

    fn parseInfix(self: *Parser, left: Node.Index, prec: Precedence) Error!Node.Index {
        const op_tag = self.peekTag();
        const op_tok = self.pos;

        // Check if this is a call expression.
        if (op_tag == .left_paren) {
            return self.parseCall(left);
        }

        // Dot access (field_access): infix at call precedence.
        if (op_tag == .dot) {
            return self.parseDotAccess(left);
        }

        // Pipe operator: special handling for left-associative pipe chains.
        if (op_tag == .pipe_greater) {
            return self.parsePipe(left);
        }

        const node_tag = self.tokenToNodeTag(op_tag);
        self.advance(); // consume operator

        // Left-associative: next precedence is one higher.
        const next_prec: Precedence = @enumFromInt(@intFromEnum(prec) + 1);
        const right = try self.parsePrecedence(next_prec);

        return self.ast.addNode(.{
            .tag = node_tag,
            .main_token = op_tok,
            .data = .{ .lhs = left, .rhs = right },
        }, self.allocator);
    }

    fn parseCall(self: *Parser, callee: Node.Index) Error!Node.Index {
        const paren_tok = self.pos;
        self.advance(); // consume '('

        var args: std.ArrayListUnmanaged(u32) = .empty;
        defer args.deinit(self.allocator);
        var seen_named = false;

        // Parse arguments.
        if (self.peekTag() != .right_paren) {
            const first_arg = try self.parseCallArg(&seen_named);
            try args.append(self.allocator, first_arg);

            while (self.peekTag() == .comma) {
                self.advance(); // consume ','
                const arg = try self.parseCallArg(&seen_named);
                try args.append(self.allocator, arg);
            }
        }

        if (self.peekTag() != .right_paren) {
            try self.emitError("expected ')' after arguments");
            return error.ParseError;
        }
        self.advance(); // consume ')'

        // Store args in extra_data.
        const arg_start: u32 = @intCast(self.ast.extra_data.items.len);
        for (args.items) |a| {
            _ = try self.ast.addExtra(a, self.allocator);
        }
        const arg_end: u32 = @intCast(self.ast.extra_data.items.len);

        // Store arg range in extra_data.
        const extra_idx = try self.ast.addExtra(arg_start, self.allocator);
        _ = try self.ast.addExtra(arg_end, self.allocator);

        return self.ast.addNode(.{
            .tag = .call_expr,
            .main_token = paren_tok,
            .data = .{ .lhs = callee, .rhs = extra_idx },
        }, self.allocator);
    }

    /// Parse a single call argument, which may be a named argument (identifier: expr).
    fn parseCallArg(self: *Parser, seen_named: *bool) Error!Node.Index {
        // Check if this is a named argument: identifier followed by ':'
        if (self.peekTag() == .identifier) {
            // Look ahead: if the next token after identifier is ':', it's a named arg.
            if (self.pos + 1 < self.tokens.len and self.tokens[self.pos + 1].tag == .colon) {
                const name_tok = self.pos;
                self.advance(); // consume identifier
                self.advance(); // consume ':'
                const value = try self.parseExpression();
                seen_named.* = true;
                return self.ast.addNode(.{
                    .tag = .named_arg,
                    .main_token = name_tok,
                    .data = .{ .lhs = name_tok, .rhs = value },
                }, self.allocator);
            }
        }

        // Positional argument.
        if (seen_named.*) {
            try self.emitError("positional argument cannot follow named argument");
            return error.ParseError;
        }
        return self.parseExpression();
    }

    fn parseLiteral(self: *Parser, tag: Node.Tag) Error!Node.Index {
        const tok = self.pos;
        self.advance();
        return self.ast.addNode(.{
            .tag = tag,
            .main_token = tok,
            .data = .{ .lhs = 0, .rhs = 0 },
        }, self.allocator);
    }

    fn parseIdentifier(self: *Parser) Error!Node.Index {
        const tok = self.pos;
        self.advance();
        return self.ast.addNode(.{
            .tag = .identifier,
            .main_token = tok,
            .data = .{ .lhs = 0, .rhs = 0 },
        }, self.allocator);
    }

    fn parseUnaryNegate(self: *Parser) Error!Node.Index {
        const op_tok = self.pos;
        self.advance(); // consume '-'
        const operand = try self.parsePrecedence(.unary);
        return self.ast.addNode(.{
            .tag = .negate,
            .main_token = op_tok,
            .data = .{ .lhs = operand, .rhs = 0 },
        }, self.allocator);
    }

    fn parseUnaryNot(self: *Parser) Error!Node.Index {
        const op_tok = self.pos;
        self.advance(); // consume 'not'
        const operand = try self.parsePrecedence(.unary);
        return self.ast.addNode(.{
            .tag = .logical_not,
            .main_token = op_tok,
            .data = .{ .lhs = operand, .rhs = 0 },
        }, self.allocator);
    }

    /// Parse `(expr)` as grouped expression OR `(expr, ...)` / `(expr,)` as tuple literal.
    fn parseGroupedOrTuple(self: *Parser) Error!Node.Index {
        const paren_tok = self.pos;
        self.advance(); // consume '('

        // Empty parens `()` -- parse as zero-element tuple.
        if (self.peekTag() == .right_paren) {
            self.advance(); // consume ')'
            const extra_start: u32 = @intCast(self.ast.extra_data.items.len);
            return self.ast.addNode(.{
                .tag = .tuple_literal,
                .main_token = paren_tok,
                .data = .{ .lhs = extra_start, .rhs = extra_start },
            }, self.allocator);
        }

        const first = try self.parseExpression();

        // Single element with closing paren and no comma -> grouped_expr.
        if (self.peekTag() == .right_paren) {
            self.advance(); // consume ')'
            return self.ast.addNode(.{
                .tag = .grouped_expr,
                .main_token = paren_tok,
                .data = .{ .lhs = first, .rhs = 0 },
            }, self.allocator);
        }

        // Comma found -> this is a tuple literal.
        if (self.peekTag() != .comma) {
            try self.emitError("expected ')' or ',' in tuple/grouped expression");
            return error.ParseError;
        }

        var elements: std.ArrayListUnmanaged(u32) = .empty;
        defer elements.deinit(self.allocator);
        try elements.append(self.allocator, first);

        // Consume comma and parse remaining elements.
        while (self.peekTag() == .comma) {
            self.advance(); // consume ','
            // Trailing comma: `(expr,)`
            if (self.peekTag() == .right_paren) break;
            const elem = try self.parseExpression();
            try elements.append(self.allocator, elem);
        }

        if (self.peekTag() != .right_paren) {
            try self.emitError("expected ')' to close tuple");
            return error.ParseError;
        }
        self.advance(); // consume ')'

        // Store element indices in extra_data.
        const extra_start: u32 = @intCast(self.ast.extra_data.items.len);
        for (elements.items) |e| {
            _ = try self.ast.addExtra(e, self.allocator);
        }
        const extra_end: u32 = @intCast(self.ast.extra_data.items.len);

        return self.ast.addNode(.{
            .tag = .tuple_literal,
            .main_token = paren_tok,
            .data = .{ .lhs = extra_start, .rhs = extra_end },
        }, self.allocator);
    }

    /// Parse `[expr, expr, ...]` as list literal.
    fn parseListLiteral(self: *Parser) Error!Node.Index {
        const bracket_tok = self.pos;
        self.advance(); // consume '['

        var elements: std.ArrayListUnmanaged(u32) = .empty;
        defer elements.deinit(self.allocator);

        if (self.peekTag() != .right_bracket) {
            const first = try self.parseExpression();
            try elements.append(self.allocator, first);

            while (self.peekTag() == .comma) {
                self.advance(); // consume ','
                // Allow trailing comma.
                if (self.peekTag() == .right_bracket) break;
                const elem = try self.parseExpression();
                try elements.append(self.allocator, elem);
            }
        }

        if (self.peekTag() != .right_bracket) {
            try self.emitError("expected ']' to close list literal");
            return error.ParseError;
        }
        self.advance(); // consume ']'

        const extra_start: u32 = @intCast(self.ast.extra_data.items.len);
        for (elements.items) |e| {
            _ = try self.ast.addExtra(e, self.allocator);
        }
        const extra_end: u32 = @intCast(self.ast.extra_data.items.len);

        return self.ast.addNode(.{
            .tag = .list_literal,
            .main_token = bracket_tok,
            .data = .{ .lhs = extra_start, .rhs = extra_end },
        }, self.allocator);
    }

    /// Disambiguation: `{` can start a block_expr, record literal, map literal, or record spread.
    /// - `{}` -> empty map
    /// - `{..expr, ...}` -> record_spread
    /// - `{identifier: expr, ...}` -> record literal
    /// - `{string_literal: expr, ...}` -> map literal
    /// - Otherwise -> block expression
    fn parseBraceExpr(self: *Parser) Error!Node.Index {
        // Look ahead to disambiguate without consuming the `{`.
        const after_brace = self.pos + 1;

        // Empty `{}` -> empty map literal.
        if (after_brace < self.tokens.len and self.tokens[after_brace].tag == .right_brace) {
            return self.parseMapLiteral();
        }

        // `{..` -> record spread.
        if (after_brace < self.tokens.len and self.tokens[after_brace].tag == .dot_dot) {
            return self.parseRecordSpread();
        }

        // `{identifier :` -> record literal (but NOT `{identifier =` which is assignment in block).
        if (after_brace < self.tokens.len and self.tokens[after_brace].tag == .identifier) {
            const after_ident = after_brace + 1;
            if (after_ident < self.tokens.len and self.tokens[after_ident].tag == .colon) {
                return self.parseRecordLiteral();
            }
        }

        // `{string_literal :` -> map literal.
        if (after_brace < self.tokens.len and self.tokens[after_brace].tag == .string_literal) {
            const after_str = after_brace + 1;
            if (after_str < self.tokens.len and self.tokens[after_str].tag == .colon) {
                return self.parseMapLiteral();
            }
        }

        // Default: block expression.
        return self.parseBlockExpr();
    }

    /// Parse `{name: expr, name: expr, ...}` as record literal.
    fn parseRecordLiteral(self: *Parser) Error!Node.Index {
        const brace_tok = self.pos;
        self.advance(); // consume '{'

        var pairs: std.ArrayListUnmanaged(u32) = .empty;
        defer pairs.deinit(self.allocator);

        // Parse first field.
        if (self.peekTag() == .identifier) {
            const name_tok = self.pos;
            self.advance(); // consume identifier
            if (self.peekTag() != .colon) {
                try self.emitError("expected ':' after record field name");
                return error.ParseError;
            }
            self.advance(); // consume ':'
            const value = try self.parseExpression();
            try pairs.append(self.allocator, name_tok);
            try pairs.append(self.allocator, value);
        }

        // Parse remaining fields.
        while (self.peekTag() == .comma) {
            self.advance(); // consume ','
            if (self.peekTag() == .right_brace) break; // trailing comma
            if (self.peekTag() != .identifier) {
                try self.emitError("expected field name in record literal");
                return error.ParseError;
            }
            const name_tok = self.pos;
            self.advance(); // consume identifier
            if (self.peekTag() != .colon) {
                try self.emitError("expected ':' after record field name");
                return error.ParseError;
            }
            self.advance(); // consume ':'
            const value = try self.parseExpression();
            try pairs.append(self.allocator, name_tok);
            try pairs.append(self.allocator, value);
        }

        if (self.peekTag() != .right_brace) {
            try self.emitError("expected '}' to close record literal");
            return error.ParseError;
        }
        self.advance(); // consume '}'

        // Store alternating name_token/value_node in extra_data.
        const extra_start: u32 = @intCast(self.ast.extra_data.items.len);
        for (pairs.items) |p| {
            _ = try self.ast.addExtra(p, self.allocator);
        }
        const extra_end: u32 = @intCast(self.ast.extra_data.items.len);

        return self.ast.addNode(.{
            .tag = .record_literal,
            .main_token = brace_tok,
            .data = .{ .lhs = extra_start, .rhs = extra_end },
        }, self.allocator);
    }

    /// Parse `{"key": expr, ...}` or `{}` as map literal.
    fn parseMapLiteral(self: *Parser) Error!Node.Index {
        const brace_tok = self.pos;
        self.advance(); // consume '{'

        var pairs: std.ArrayListUnmanaged(u32) = .empty;
        defer pairs.deinit(self.allocator);

        if (self.peekTag() != .right_brace) {
            // Parse first key-value pair.
            const key = try self.parseExpression();
            if (self.peekTag() != .colon) {
                try self.emitError("expected ':' after map key");
                return error.ParseError;
            }
            self.advance(); // consume ':'
            const value = try self.parseExpression();
            try pairs.append(self.allocator, key);
            try pairs.append(self.allocator, value);

            // Parse remaining pairs.
            while (self.peekTag() == .comma) {
                self.advance(); // consume ','
                if (self.peekTag() == .right_brace) break; // trailing comma
                const k = try self.parseExpression();
                if (self.peekTag() != .colon) {
                    try self.emitError("expected ':' after map key");
                    return error.ParseError;
                }
                self.advance(); // consume ':'
                const v = try self.parseExpression();
                try pairs.append(self.allocator, k);
                try pairs.append(self.allocator, v);
            }
        }

        if (self.peekTag() != .right_brace) {
            try self.emitError("expected '}' to close map literal");
            return error.ParseError;
        }
        self.advance(); // consume '}'

        // Store alternating key_node/value_node in extra_data.
        const extra_start: u32 = @intCast(self.ast.extra_data.items.len);
        for (pairs.items) |p| {
            _ = try self.ast.addExtra(p, self.allocator);
        }
        const extra_end: u32 = @intCast(self.ast.extra_data.items.len);

        return self.ast.addNode(.{
            .tag = .map_literal,
            .main_token = brace_tok,
            .data = .{ .lhs = extra_start, .rhs = extra_end },
        }, self.allocator);
    }

    /// Parse `{..expr, field: val, ...}` as record spread.
    fn parseRecordSpread(self: *Parser) Error!Node.Index {
        const brace_tok = self.pos;
        self.advance(); // consume '{'

        // Expect `..`
        if (self.peekTag() != .dot_dot) {
            try self.emitError("expected '..' for record spread");
            return error.ParseError;
        }
        self.advance(); // consume '..'

        // Parse base record expression.
        const base = try self.parseExpression();

        // Parse override fields.
        var overrides: std.ArrayListUnmanaged(u32) = .empty;
        defer overrides.deinit(self.allocator);

        while (self.peekTag() == .comma) {
            self.advance(); // consume ','
            if (self.peekTag() == .right_brace) break; // trailing comma
            if (self.peekTag() != .identifier) {
                try self.emitError("expected field name in record spread override");
                return error.ParseError;
            }
            const name_tok = self.pos;
            self.advance(); // consume identifier
            if (self.peekTag() != .colon) {
                try self.emitError("expected ':' after override field name");
                return error.ParseError;
            }
            self.advance(); // consume ':'
            const value = try self.parseExpression();
            try overrides.append(self.allocator, name_tok);
            try overrides.append(self.allocator, value);
        }

        if (self.peekTag() != .right_brace) {
            try self.emitError("expected '}' to close record spread");
            return error.ParseError;
        }
        self.advance(); // consume '}'

        // Store override pairs in extra_data: alternating name_token/value_node.
        const extra_start: u32 = @intCast(self.ast.extra_data.items.len);
        for (overrides.items) |o| {
            _ = try self.ast.addExtra(o, self.allocator);
        }
        const extra_end: u32 = @intCast(self.ast.extra_data.items.len);

        // extra_idx stores start..end range of override pairs.
        const extra_idx = try self.ast.addExtra(extra_start, self.allocator);
        _ = try self.ast.addExtra(extra_end, self.allocator);

        return self.ast.addNode(.{
            .tag = .record_spread,
            .main_token = brace_tok,
            .data = .{ .lhs = base, .rhs = extra_idx },
        }, self.allocator);
    }

    /// Parse dot access: `expr.identifier` -> field_access node.
    fn parseDotAccess(self: *Parser, left: Node.Index) Error!Node.Index {
        const dot_tok = self.pos;
        self.advance(); // consume '.'

        if (self.peekTag() != .identifier) {
            try self.emitError("expected identifier after '.'");
            return error.ParseError;
        }
        const field_tok = self.pos;
        self.advance(); // consume identifier

        return self.ast.addNode(.{
            .tag = .field_access,
            .main_token = dot_tok,
            .data = .{ .lhs = left, .rhs = field_tok },
        }, self.allocator);
    }

    // ── Select expression parsing (Phase 7) ────────────────────────────

    /// Parse `select { | recv(ch) -> |val| body | send(ch, v) -> body | after(ms) -> body }`
    ///
    /// Each arm is stored as a select_arm node:
    ///   data.lhs = kind (0=recv, 1=send, 2=after)
    ///   data.rhs = extra_idx into extra_data:
    ///     For recv: [channel_expr, body_expr, binding_token (0 if none)]
    ///     For send: [channel_expr, value_expr, body_expr]
    ///     For after: [timeout_expr, body_expr]
    fn parseSelectExpr(self: *Parser) Error!Node.Index {
        const select_tok = self.pos;
        self.advance(); // consume 'select'

        if (self.peekTag() != .left_brace) {
            try self.emitError("expected '{' after 'select'");
            return error.ParseError;
        }
        self.advance(); // consume '{'

        var arms: std.ArrayListUnmanaged(u32) = .empty;
        defer arms.deinit(self.allocator);

        // Parse arms: | recv(ch) -> |val| body or | send(ch, v) -> body or | after(ms) -> body
        while (self.peekTag() == .pipe) {
            self.advance(); // consume '|'

            // Determine arm kind from identifier.
            if (self.peekTag() != .identifier) {
                try self.emitError("expected 'recv', 'send', or 'after' in select arm");
                return error.ParseError;
            }
            const kind_name = self.tokenSlice(self.pos);

            if (std.mem.eql(u8, kind_name, "recv")) {
                self.advance(); // consume 'recv'
                // recv(ch) -> |val| body
                if (self.peekTag() != .left_paren) {
                    try self.emitError("expected '(' after 'recv'");
                    return error.ParseError;
                }
                self.advance(); // consume '('
                const ch_expr = try self.parseExpression();
                if (self.peekTag() != .right_paren) {
                    try self.emitError("expected ')' after channel expression");
                    return error.ParseError;
                }
                self.advance(); // consume ')'

                if (self.peekTag() != .arrow) {
                    try self.emitError("expected '->' after recv(ch)");
                    return error.ParseError;
                }
                self.advance(); // consume '->'

                // Optional binding: |val|
                var binding_token: u32 = 0;
                if (self.peekTag() == .pipe) {
                    self.advance(); // consume '|'
                    if (self.peekTag() != .identifier) {
                        try self.emitError("expected binding name after '|'");
                        return error.ParseError;
                    }
                    binding_token = self.pos;
                    self.advance(); // consume identifier
                    if (self.peekTag() != .pipe) {
                        try self.emitError("expected '|' after binding name");
                        return error.ParseError;
                    }
                    self.advance(); // consume '|'
                }

                const body_expr = try self.parseExpression();

                // Store in extra_data: [channel_expr, body_expr, binding_token]
                const extra_idx: u32 = @intCast(self.ast.extra_data.items.len);
                _ = try self.ast.addExtra(ch_expr, self.allocator);
                _ = try self.ast.addExtra(body_expr, self.allocator);
                _ = try self.ast.addExtra(binding_token, self.allocator);

                const arm_node = try self.ast.addNode(.{
                    .tag = .select_arm,
                    .main_token = select_tok,
                    .data = .{ .lhs = 0, .rhs = extra_idx },
                }, self.allocator);
                try arms.append(self.allocator, arm_node);
            } else if (std.mem.eql(u8, kind_name, "send")) {
                self.advance(); // consume 'send'
                // send(ch, value) -> body
                if (self.peekTag() != .left_paren) {
                    try self.emitError("expected '(' after 'send'");
                    return error.ParseError;
                }
                self.advance(); // consume '('
                const ch_expr = try self.parseExpression();
                if (self.peekTag() != .comma) {
                    try self.emitError("expected ',' after channel in send");
                    return error.ParseError;
                }
                self.advance(); // consume ','
                const val_expr = try self.parseExpression();
                if (self.peekTag() != .right_paren) {
                    try self.emitError("expected ')' after send arguments");
                    return error.ParseError;
                }
                self.advance(); // consume ')'

                if (self.peekTag() != .arrow) {
                    try self.emitError("expected '->' after send(ch, v)");
                    return error.ParseError;
                }
                self.advance(); // consume '->'

                const body_expr = try self.parseExpression();

                // Store in extra_data: [channel_expr, value_expr, body_expr]
                const extra_idx: u32 = @intCast(self.ast.extra_data.items.len);
                _ = try self.ast.addExtra(ch_expr, self.allocator);
                _ = try self.ast.addExtra(val_expr, self.allocator);
                _ = try self.ast.addExtra(body_expr, self.allocator);

                const arm_node = try self.ast.addNode(.{
                    .tag = .select_arm,
                    .main_token = select_tok,
                    .data = .{ .lhs = 1, .rhs = extra_idx },
                }, self.allocator);
                try arms.append(self.allocator, arm_node);
            } else if (std.mem.eql(u8, kind_name, "after")) {
                self.advance(); // consume 'after'
                // after(ms) -> body
                if (self.peekTag() != .left_paren) {
                    try self.emitError("expected '(' after 'after'");
                    return error.ParseError;
                }
                self.advance(); // consume '('
                const timeout_expr = try self.parseExpression();
                if (self.peekTag() != .right_paren) {
                    try self.emitError("expected ')' after timeout expression");
                    return error.ParseError;
                }
                self.advance(); // consume ')'

                if (self.peekTag() != .arrow) {
                    try self.emitError("expected '->' after after(ms)");
                    return error.ParseError;
                }
                self.advance(); // consume '->'

                const body_expr = try self.parseExpression();

                // Store in extra_data: [timeout_expr, body_expr]
                const extra_idx: u32 = @intCast(self.ast.extra_data.items.len);
                _ = try self.ast.addExtra(timeout_expr, self.allocator);
                _ = try self.ast.addExtra(body_expr, self.allocator);

                const arm_node = try self.ast.addNode(.{
                    .tag = .select_arm,
                    .main_token = select_tok,
                    .data = .{ .lhs = 2, .rhs = extra_idx },
                }, self.allocator);
                try arms.append(self.allocator, arm_node);
            } else {
                try self.emitError("expected 'recv', 'send', or 'after' in select arm");
                return error.ParseError;
            }
        }

        if (self.peekTag() != .right_brace) {
            try self.emitError("expected '}' after select arms");
            return error.ParseError;
        }
        self.advance(); // consume '}'

        // Store arm indices in extra_data.
        const extra_start: u32 = @intCast(self.ast.extra_data.items.len);
        for (arms.items) |arm_idx| {
            _ = try self.ast.addExtra(arm_idx, self.allocator);
        }
        const extra_end: u32 = @intCast(self.ast.extra_data.items.len);

        return self.ast.addNode(.{
            .tag = .select_expr,
            .main_token = select_tok,
            .data = .{ .lhs = extra_start, .rhs = extra_end },
        }, self.allocator);
    }

    // ── Type declaration parsing ──────────────────────────────────────

    /// Parse `type Name = | Variant1 | Variant2(T1, T2) | ...`
    fn parseTypeDecl(self: *Parser) Error!Node.Index {
        const type_tok = self.pos;
        self.advance(); // consume 'type'

        // Expect type name (uppercase identifier).
        if (self.peekTag() != .identifier) {
            try self.emitError("expected type name after 'type'");
            return error.ParseError;
        }
        const name_tok = self.pos;
        self.advance(); // consume type name

        // Expect '='.
        if (self.peekTag() != .equal) {
            try self.emitError("expected '=' after type name");
            return error.ParseError;
        }
        self.advance(); // consume '='

        // Parse variants: | Variant1 | Variant2(T1, T2) | ...
        var variant_data: std.ArrayListUnmanaged(u32) = .empty;
        defer variant_data.deinit(self.allocator);

        // First variant must start with '|'.
        if (self.peekTag() != .pipe) {
            try self.emitError("expected '|' before variant");
            return error.ParseError;
        }

        var variant_count: u32 = 0;
        while (self.peekTag() == .pipe) {
            self.advance(); // consume '|'

            if (self.peekTag() != .identifier) {
                try self.emitError("expected variant name after '|'");
                return error.ParseError;
            }
            const variant_tok = self.pos;
            self.advance(); // consume variant name

            // Check for payload: `(Type1, Type2)`
            var arity: u32 = 0;
            if (self.peekTag() == .left_paren) {
                self.advance(); // consume '('
                if (self.peekTag() != .right_paren) {
                    // Count params (type names or identifiers for arity tracking).
                    arity = 1;
                    if (self.peekTag() == .identifier) {
                        self.advance(); // consume first type name
                    } else {
                        try self.emitError("expected type name in variant payload");
                        return error.ParseError;
                    }
                    while (self.peekTag() == .comma) {
                        self.advance(); // consume ','
                        if (self.peekTag() == .identifier) {
                            self.advance(); // consume type name
                        } else {
                            try self.emitError("expected type name in variant payload");
                            return error.ParseError;
                        }
                        arity += 1;
                    }
                }
                if (self.peekTag() != .right_paren) {
                    try self.emitError("expected ')' to close variant payload");
                    return error.ParseError;
                }
                self.advance(); // consume ')'
            }

            try variant_data.append(self.allocator, variant_tok);
            try variant_data.append(self.allocator, arity);
            variant_count += 1;
        }

        // Store in extra_data: [name_token, variant_count, then per variant: name_token, arity]
        const extra_idx = try self.ast.addExtra(name_tok, self.allocator);
        _ = try self.ast.addExtra(variant_count, self.allocator);
        for (variant_data.items) |v| {
            _ = try self.ast.addExtra(v, self.allocator);
        }

        return self.ast.addNode(.{
            .tag = .type_decl,
            .main_token = type_tok,
            .data = .{ .lhs = extra_idx, .rhs = 0 },
        }, self.allocator);
    }

    // ── Match expression parsing ──────────────────────────────────────

    /// Parse `match scrutinee | pattern -> body | pattern when guard -> body | ...`
    fn parseMatchExpr(self: *Parser) Error!Node.Index {
        const match_tok = self.pos;
        self.advance(); // consume 'match'

        // Parse the scrutinee expression -- stop before '|' (pipe at arm start).
        // We parse at a precedence that stops at pipe (which starts match arms).
        // Since pipe_prec is used for |>, and bare | is used for lambdas/match arms,
        // we need to parse the scrutinee as a full expression but stop at bare '|'.
        // Strategy: parse expression, but since | has no infix precedence for bare pipe,
        // parseExpression will stop naturally at '|'.
        const scrutinee = try self.parseExpression();

        // Parse arms.
        var arm_nodes: std.ArrayListUnmanaged(u32) = .empty;
        defer arm_nodes.deinit(self.allocator);

        if (self.peekTag() != .pipe) {
            try self.emitError("expected '|' to start match arm");
            return error.ParseError;
        }

        while (self.peekTag() == .pipe) {
            self.advance(); // consume '|'
            const arm = try self.parseMatchArm();
            try arm_nodes.append(self.allocator, arm);
        }

        // Store arm node indices in extra_data.
        const extra_start: u32 = @intCast(self.ast.extra_data.items.len);
        for (arm_nodes.items) |a| {
            _ = try self.ast.addExtra(a, self.allocator);
        }
        const extra_end: u32 = @intCast(self.ast.extra_data.items.len);

        // Store start/end in extra_data.
        const arms_extra = try self.ast.addExtra(extra_start, self.allocator);
        _ = try self.ast.addExtra(extra_end, self.allocator);

        return self.ast.addNode(.{
            .tag = .match_expr,
            .main_token = match_tok,
            .data = .{ .lhs = scrutinee, .rhs = arms_extra },
        }, self.allocator);
    }

    /// Parse a single match arm: `pattern -> body` or `pattern when guard -> body`
    fn parseMatchArm(self: *Parser) Error!Node.Index {
        const arm_tok = self.pos;
        const pattern = try self.parsePattern();

        // Check for guard: `when guard_expr`
        if (self.peekTag() == .kw_when) {
            self.advance(); // consume 'when'
            const guard = try self.parseExpression();

            if (self.peekTag() != .arrow) {
                try self.emitError("expected '->' after guard expression");
                return error.ParseError;
            }
            self.advance(); // consume '->'

            const body = try self.parseExpression();

            // Store pattern, guard, body in extra_data.
            const extra_idx = try self.ast.addExtra(pattern, self.allocator);
            _ = try self.ast.addExtra(guard, self.allocator);
            _ = try self.ast.addExtra(body, self.allocator);

            return self.ast.addNode(.{
                .tag = .match_arm_guarded,
                .main_token = arm_tok,
                .data = .{ .lhs = extra_idx, .rhs = 0 },
            }, self.allocator);
        }

        // No guard.
        if (self.peekTag() != .arrow) {
            try self.emitError("expected '->' after pattern");
            return error.ParseError;
        }
        self.advance(); // consume '->'

        const body = try self.parseExpression();

        return self.ast.addNode(.{
            .tag = .match_arm,
            .main_token = arm_tok,
            .data = .{ .lhs = pattern, .rhs = body },
        }, self.allocator);
    }

    /// Parse a pattern (wildcard, literal, binding, ADT, list, tuple, record).
    fn parsePattern(self: *Parser) Error!Node.Index {
        const tag = self.peekTag();

        switch (tag) {
            // `_` -> wildcard (but could also be identifier binding)
            .identifier => {
                const name = self.tokenSlice(self.pos);

                // Check if it's `_` (wildcard).
                if (std.mem.eql(u8, name, "_")) {
                    const tok = self.pos;
                    self.advance(); // consume '_'
                    return self.ast.addNode(.{
                        .tag = .pattern_wildcard,
                        .main_token = tok,
                        .data = .{ .lhs = 0, .rhs = 0 },
                    }, self.allocator);
                }

                // Check if first char is uppercase -> ADT pattern: TypeName.Variant(...)
                if (name.len > 0 and name[0] >= 'A' and name[0] <= 'Z') {
                    return self.parseAdtPattern();
                }

                // Lowercase identifier -> binding pattern.
                const tok = self.pos;
                self.advance(); // consume identifier
                return self.ast.addNode(.{
                    .tag = .pattern_binding,
                    .main_token = tok,
                    .data = .{ .lhs = tok, .rhs = 0 },
                }, self.allocator);
            },

            // Integer/float/string/bool/nil/atom literal -> pattern_literal
            .int_literal, .float_literal, .string_literal, .atom_literal => {
                const lit = try self.parseLiteral(switch (tag) {
                    .int_literal => .int_literal,
                    .float_literal => .float_literal,
                    .string_literal => .string_literal,
                    .atom_literal => .atom_literal,
                    else => unreachable,
                });
                return self.ast.addNode(.{
                    .tag = .pattern_literal,
                    .main_token = self.ast.nodes.items(.main_token)[lit],
                    .data = .{ .lhs = lit, .rhs = 0 },
                }, self.allocator);
            },
            .kw_true, .kw_false => {
                const lit = try self.parseLiteral(.bool_literal);
                return self.ast.addNode(.{
                    .tag = .pattern_literal,
                    .main_token = self.ast.nodes.items(.main_token)[lit],
                    .data = .{ .lhs = lit, .rhs = 0 },
                }, self.allocator);
            },
            .kw_nil => {
                const lit = try self.parseLiteral(.nil_literal);
                return self.ast.addNode(.{
                    .tag = .pattern_literal,
                    .main_token = self.ast.nodes.items(.main_token)[lit],
                    .data = .{ .lhs = lit, .rhs = 0 },
                }, self.allocator);
            },

            // `[` -> list pattern
            .left_bracket => return self.parseListPattern(),

            // `(` -> tuple pattern
            .left_paren => return self.parseTuplePattern(),

            // `{` -> record pattern
            .left_brace => return self.parseRecordPattern(),

            else => {
                try self.emitError("expected pattern");
                return error.ParseError;
            },
        }
    }

    /// Parse ADT pattern: `TypeName.Variant` or `TypeName.Variant(sub_pat, ...)`
    fn parseAdtPattern(self: *Parser) Error!Node.Index {
        const type_tok = self.pos;
        self.advance(); // consume TypeName

        if (self.peekTag() != .dot) {
            try self.emitError("expected '.' after type name in pattern");
            return error.ParseError;
        }
        self.advance(); // consume '.'

        if (self.peekTag() != .identifier) {
            try self.emitError("expected variant name after '.'");
            return error.ParseError;
        }
        const variant_tok = self.pos;
        self.advance(); // consume VariantName

        // Store type_token and variant_token in extra_data.
        const type_extra = try self.ast.addExtra(type_tok, self.allocator);
        _ = try self.ast.addExtra(variant_tok, self.allocator);

        // Check for sub-patterns: `(sub_pat, ...)`
        var sub_pats_start: u32 = @intCast(self.ast.extra_data.items.len);
        var sub_pats_end: u32 = sub_pats_start;

        if (self.peekTag() == .left_paren) {
            self.advance(); // consume '('

            var sub_pats: std.ArrayListUnmanaged(u32) = .empty;
            defer sub_pats.deinit(self.allocator);

            if (self.peekTag() != .right_paren) {
                const first = try self.parsePattern();
                try sub_pats.append(self.allocator, first);

                while (self.peekTag() == .comma) {
                    self.advance(); // consume ','
                    if (self.peekTag() == .right_paren) break;
                    const sub = try self.parsePattern();
                    try sub_pats.append(self.allocator, sub);
                }
            }

            if (self.peekTag() != .right_paren) {
                try self.emitError("expected ')' to close ADT sub-patterns");
                return error.ParseError;
            }
            self.advance(); // consume ')'

            sub_pats_start = @intCast(self.ast.extra_data.items.len);
            for (sub_pats.items) |sp| {
                _ = try self.ast.addExtra(sp, self.allocator);
            }
            sub_pats_end = @intCast(self.ast.extra_data.items.len);
        }

        // Store sub-pattern range in extra_data.
        const sub_extra = try self.ast.addExtra(sub_pats_start, self.allocator);
        _ = try self.ast.addExtra(sub_pats_end, self.allocator);

        return self.ast.addNode(.{
            .tag = .pattern_adt,
            .main_token = type_tok,
            .data = .{ .lhs = type_extra, .rhs = sub_extra },
        }, self.allocator);
    }

    /// Parse list pattern: `[p1, p2, ..rest]`
    fn parseListPattern(self: *Parser) Error!Node.Index {
        const bracket_tok = self.pos;
        self.advance(); // consume '['

        var elements: std.ArrayListUnmanaged(u32) = .empty;
        defer elements.deinit(self.allocator);

        if (self.peekTag() != .right_bracket) {
            // Check for `..rest` at start.
            if (self.peekTag() == .dot_dot) {
                const rest = try self.parseRestPattern();
                try elements.append(self.allocator, rest);
            } else {
                const first = try self.parsePattern();
                try elements.append(self.allocator, first);
            }

            while (self.peekTag() == .comma) {
                self.advance(); // consume ','
                if (self.peekTag() == .right_bracket) break;
                if (self.peekTag() == .dot_dot) {
                    const rest = try self.parseRestPattern();
                    try elements.append(self.allocator, rest);
                    break; // rest must be last
                }
                const elem = try self.parsePattern();
                try elements.append(self.allocator, elem);
            }
        }

        if (self.peekTag() != .right_bracket) {
            try self.emitError("expected ']' to close list pattern");
            return error.ParseError;
        }
        self.advance(); // consume ']'

        const extra_start: u32 = @intCast(self.ast.extra_data.items.len);
        for (elements.items) |e| {
            _ = try self.ast.addExtra(e, self.allocator);
        }
        const extra_end: u32 = @intCast(self.ast.extra_data.items.len);

        return self.ast.addNode(.{
            .tag = .pattern_list,
            .main_token = bracket_tok,
            .data = .{ .lhs = extra_start, .rhs = extra_end },
        }, self.allocator);
    }

    /// Parse tuple pattern: `(p1, p2)`
    fn parseTuplePattern(self: *Parser) Error!Node.Index {
        const paren_tok = self.pos;
        self.advance(); // consume '('

        var elements: std.ArrayListUnmanaged(u32) = .empty;
        defer elements.deinit(self.allocator);

        if (self.peekTag() != .right_paren) {
            const first = try self.parsePattern();
            try elements.append(self.allocator, first);

            while (self.peekTag() == .comma) {
                self.advance(); // consume ','
                if (self.peekTag() == .right_paren) break;
                const elem = try self.parsePattern();
                try elements.append(self.allocator, elem);
            }
        }

        if (self.peekTag() != .right_paren) {
            try self.emitError("expected ')' to close tuple pattern");
            return error.ParseError;
        }
        self.advance(); // consume ')'

        const extra_start: u32 = @intCast(self.ast.extra_data.items.len);
        for (elements.items) |e| {
            _ = try self.ast.addExtra(e, self.allocator);
        }
        const extra_end: u32 = @intCast(self.ast.extra_data.items.len);

        return self.ast.addNode(.{
            .tag = .pattern_tuple,
            .main_token = paren_tok,
            .data = .{ .lhs = extra_start, .rhs = extra_end },
        }, self.allocator);
    }

    /// Parse record pattern: `{field: pattern, ...}`
    fn parseRecordPattern(self: *Parser) Error!Node.Index {
        const brace_tok = self.pos;
        self.advance(); // consume '{'

        var pairs: std.ArrayListUnmanaged(u32) = .empty;
        defer pairs.deinit(self.allocator);

        if (self.peekTag() != .right_brace) {
            // Parse first field pattern.
            if (self.peekTag() != .identifier) {
                try self.emitError("expected field name in record pattern");
                return error.ParseError;
            }
            const name_tok = self.pos;
            self.advance(); // consume identifier
            if (self.peekTag() != .colon) {
                try self.emitError("expected ':' after field name in record pattern");
                return error.ParseError;
            }
            self.advance(); // consume ':'
            const pat = try self.parsePattern();
            try pairs.append(self.allocator, name_tok);
            try pairs.append(self.allocator, pat);

            while (self.peekTag() == .comma) {
                self.advance(); // consume ','
                if (self.peekTag() == .right_brace) break;
                if (self.peekTag() != .identifier) {
                    try self.emitError("expected field name in record pattern");
                    return error.ParseError;
                }
                const fname_tok = self.pos;
                self.advance(); // consume identifier
                if (self.peekTag() != .colon) {
                    try self.emitError("expected ':' after field name in record pattern");
                    return error.ParseError;
                }
                self.advance(); // consume ':'
                const fpat = try self.parsePattern();
                try pairs.append(self.allocator, fname_tok);
                try pairs.append(self.allocator, fpat);
            }
        }

        if (self.peekTag() != .right_brace) {
            try self.emitError("expected '}' to close record pattern");
            return error.ParseError;
        }
        self.advance(); // consume '}'

        const extra_start: u32 = @intCast(self.ast.extra_data.items.len);
        for (pairs.items) |p| {
            _ = try self.ast.addExtra(p, self.allocator);
        }
        const extra_end: u32 = @intCast(self.ast.extra_data.items.len);

        return self.ast.addNode(.{
            .tag = .pattern_record,
            .main_token = brace_tok,
            .data = .{ .lhs = extra_start, .rhs = extra_end },
        }, self.allocator);
    }

    /// Parse rest pattern: `..rest`
    fn parseRestPattern(self: *Parser) Error!Node.Index {
        const dot_tok = self.pos;
        self.advance(); // consume '..'

        if (self.peekTag() != .identifier) {
            try self.emitError("expected identifier after '..' in rest pattern");
            return error.ParseError;
        }
        const name_tok = self.pos;
        self.advance(); // consume identifier

        return self.ast.addNode(.{
            .tag = .pattern_rest,
            .main_token = dot_tok,
            .data = .{ .lhs = name_tok, .rhs = 0 },
        }, self.allocator);
    }

    fn parseBlockExpr(self: *Parser) Error!Node.Index {
        if (self.peekTag() != .left_brace) {
            try self.emitError("expected '{'");
            return error.ParseError;
        }
        const brace_tok = self.pos;
        self.advance(); // consume '{'

        var stmts: std.ArrayListUnmanaged(u32) = .empty;
        defer stmts.deinit(self.allocator);

        while (self.peekTag() != .right_brace and !self.atEnd()) {
            if (self.parseStatement()) |stmt| {
                try stmts.append(self.allocator, stmt);
            } else |_| {
                self.synchronize();
            }
        }

        if (self.peekTag() != .right_brace) {
            try self.emitError("expected '}' to close block");
            return error.ParseError;
        }
        self.advance(); // consume '}'

        // Store statement indices in extra_data.
        const extra_start: u32 = @intCast(self.ast.extra_data.items.len);
        for (stmts.items) |s| {
            _ = try self.ast.addExtra(s, self.allocator);
        }
        const extra_end: u32 = @intCast(self.ast.extra_data.items.len);

        return self.ast.addNode(.{
            .tag = .block_expr,
            .main_token = brace_tok,
            .data = .{ .lhs = extra_start, .rhs = extra_end },
        }, self.allocator);
    }

    fn parseIfExpr(self: *Parser) Error!Node.Index {
        const if_tok = self.pos;
        self.advance(); // consume 'if'

        // Parse condition.
        const condition = try self.parseExpression();

        // Parse then-branch (block).
        const then_branch = try self.parseBlockExpr();

        // Optional else branch.
        var else_branch: Node.Index = Node.null_node;
        if (self.peekTag() == .kw_else) {
            self.advance(); // consume 'else'
            // else can be followed by another if (else if) or a block.
            if (self.peekTag() == .kw_if) {
                else_branch = try self.parseIfExpr();
            } else {
                else_branch = try self.parseBlockExpr();
            }
        }

        // Store then/else in extra_data.
        const extra_idx = try self.ast.addExtra(then_branch, self.allocator);
        _ = try self.ast.addExtra(else_branch, self.allocator);

        return self.ast.addNode(.{
            .tag = .if_expr,
            .main_token = if_tok,
            .data = .{ .lhs = condition, .rhs = extra_idx },
        }, self.allocator);
    }

    // ── Function / Lambda / Pipe / Return parsing ──────────────────────

    /// Parse a named function declaration in statement position:
    /// `fn name(params) { body }`
    /// Also binds the name as a local (acts like `let name = fn ...`).
    fn parseFnDecl(self: *Parser) Error!Node.Index {
        const fn_tok = self.pos;
        self.advance(); // consume 'fn'

        // Expect function name.
        if (self.peekTag() != .identifier) {
            try self.emitError("expected function name after 'fn'");
            return error.ParseError;
        }
        const name_tok = self.pos;
        self.advance(); // consume name

        // Expect '('.
        if (self.peekTag() != .left_paren) {
            try self.emitError("expected '(' after function name");
            return error.ParseError;
        }
        self.advance(); // consume '('

        // Parse parameter list.
        var params: std.ArrayListUnmanaged(u32) = .empty;
        defer params.deinit(self.allocator);
        var defaults: std.ArrayListUnmanaged(u32) = .empty;
        defer defaults.deinit(self.allocator);
        var seen_named_param = false;

        if (self.peekTag() != .right_paren) {
            try self.parseFnParam(&params, &defaults, &seen_named_param);
            while (self.peekTag() == .comma) {
                self.advance(); // consume ','
                try self.parseFnParam(&params, &defaults, &seen_named_param);
            }
        }

        if (self.peekTag() != .right_paren) {
            try self.emitError("expected ')' after parameters");
            return error.ParseError;
        }
        self.advance(); // consume ')'

        // Parse body (block expression).
        self.fn_depth += 1;
        const body = try self.parseBlockExpr();
        self.fn_depth -= 1;

        // Store in extra_data: name_token, param_start, param_end, body_node, defaults_start, defaults_end
        const param_start: u32 = @intCast(self.ast.extra_data.items.len);
        for (params.items) |p| {
            _ = try self.ast.addExtra(p, self.allocator);
        }
        const param_end: u32 = @intCast(self.ast.extra_data.items.len);

        const defaults_start: u32 = @intCast(self.ast.extra_data.items.len);
        for (defaults.items) |d| {
            _ = try self.ast.addExtra(d, self.allocator);
        }
        const defaults_end: u32 = @intCast(self.ast.extra_data.items.len);

        const extra_idx = try self.ast.addExtra(name_tok, self.allocator);
        _ = try self.ast.addExtra(param_start, self.allocator);
        _ = try self.ast.addExtra(param_end, self.allocator);
        _ = try self.ast.addExtra(body, self.allocator);
        _ = try self.ast.addExtra(defaults_start, self.allocator);
        _ = try self.ast.addExtra(defaults_end, self.allocator);

        return self.ast.addNode(.{
            .tag = .fn_decl,
            .main_token = fn_tok,
            .data = .{ .lhs = extra_idx, .rhs = 0 },
        }, self.allocator);
    }

    /// Parse a function expression in expression position:
    /// `fn(params) { body }` (anonymous) or `fn name(params) { body }` (self-recursive)
    fn parseFnExpr(self: *Parser) Error!Node.Index {
        const fn_tok = self.pos;
        self.advance(); // consume 'fn'

        // Check if we have a name (for self-recursion) or go straight to '('.
        var name_tok: u32 = Node.null_node;
        if (self.peekTag() == .identifier) {
            // Check if next token is '(' -- that means this is a named fn.
            // If not, this fn is anonymous and the identifier is something else.
            if (self.pos + 1 < self.tokens.len and self.tokens[self.pos + 1].tag == .left_paren) {
                name_tok = self.pos;
                self.advance(); // consume name
            }
        }

        // Expect '('.
        if (self.peekTag() != .left_paren) {
            try self.emitError("expected '(' for function expression");
            return error.ParseError;
        }
        self.advance(); // consume '('

        // Parse parameter list.
        var params: std.ArrayListUnmanaged(u32) = .empty;
        defer params.deinit(self.allocator);
        var defaults: std.ArrayListUnmanaged(u32) = .empty;
        defer defaults.deinit(self.allocator);
        var seen_named_param = false;

        if (self.peekTag() != .right_paren) {
            try self.parseFnParam(&params, &defaults, &seen_named_param);
            while (self.peekTag() == .comma) {
                self.advance(); // consume ','
                try self.parseFnParam(&params, &defaults, &seen_named_param);
            }
        }

        if (self.peekTag() != .right_paren) {
            try self.emitError("expected ')' after parameters");
            return error.ParseError;
        }
        self.advance(); // consume ')'

        // Parse body.
        self.fn_depth += 1;
        const body = try self.parseBlockExpr();
        self.fn_depth -= 1;

        // Store in extra_data: name_token, param_start, param_end, body_node, defaults_start, defaults_end
        const param_start: u32 = @intCast(self.ast.extra_data.items.len);
        for (params.items) |p| {
            _ = try self.ast.addExtra(p, self.allocator);
        }
        const param_end: u32 = @intCast(self.ast.extra_data.items.len);

        const defaults_start: u32 = @intCast(self.ast.extra_data.items.len);
        for (defaults.items) |d| {
            _ = try self.ast.addExtra(d, self.allocator);
        }
        const defaults_end: u32 = @intCast(self.ast.extra_data.items.len);

        const extra_idx = try self.ast.addExtra(name_tok, self.allocator);
        _ = try self.ast.addExtra(param_start, self.allocator);
        _ = try self.ast.addExtra(param_end, self.allocator);
        _ = try self.ast.addExtra(body, self.allocator);
        _ = try self.ast.addExtra(defaults_start, self.allocator);
        _ = try self.ast.addExtra(defaults_end, self.allocator);

        return self.ast.addNode(.{
            .tag = .fn_decl,
            .main_token = fn_tok,
            .data = .{ .lhs = extra_idx, .rhs = 0 },
        }, self.allocator);
    }

    /// Parse a single function parameter. May have a default value (`:` expr).
    fn parseFnParam(
        self: *Parser,
        params: *std.ArrayListUnmanaged(u32),
        defaults: *std.ArrayListUnmanaged(u32),
        seen_named_param: *bool,
    ) Error!void {
        if (self.peekTag() != .identifier) {
            try self.emitError("expected parameter name");
            return error.ParseError;
        }
        const param_tok = self.pos;
        self.advance(); // consume identifier

        try params.append(self.allocator, param_tok);

        // Check for default value.
        if (self.peekTag() == .colon) {
            self.advance(); // consume ':'
            const default_expr = try self.parseExpression();
            try defaults.append(self.allocator, default_expr);
            seen_named_param.* = true;
        } else {
            // Positional param after a named param is an error.
            if (seen_named_param.*) {
                try self.emitError("positional parameter cannot follow named parameter with default");
                return error.ParseError;
            }
        }
    }

    /// Parse a lambda expression: `|params| body_expr`
    /// `|_| expr` is zero-arg lambda.
    fn parseLambda(self: *Parser) Error!Node.Index {
        const pipe_tok = self.pos;
        self.advance(); // consume '|'

        var params: std.ArrayListUnmanaged(u32) = .empty;
        defer params.deinit(self.allocator);

        // Check for zero-arg: |_|
        if (self.peekTag() == .identifier) {
            const slice = self.tokenSlice(self.pos);
            if (std.mem.eql(u8, slice, "_")) {
                // Zero-arg lambda: |_|
                self.advance(); // consume '_'
            } else {
                // Parse params.
                try params.append(self.allocator, self.pos);
                self.advance(); // consume first param

                while (self.peekTag() == .comma) {
                    self.advance(); // consume ','
                    if (self.peekTag() != .identifier) {
                        try self.emitError("expected parameter name in lambda");
                        return error.ParseError;
                    }
                    try params.append(self.allocator, self.pos);
                    self.advance(); // consume param
                }
            }
        }

        // Expect closing '|'.
        if (self.peekTag() != .pipe) {
            try self.emitError("expected '|' to close lambda parameters");
            return error.ParseError;
        }
        self.advance(); // consume '|'

        // Parse body as a single expression.
        self.fn_depth += 1;
        const body = try self.parseExpression();
        self.fn_depth -= 1;

        // Store in extra_data: param_start, param_end, body_node
        const param_start: u32 = @intCast(self.ast.extra_data.items.len);
        for (params.items) |p| {
            _ = try self.ast.addExtra(p, self.allocator);
        }
        const param_end: u32 = @intCast(self.ast.extra_data.items.len);

        const extra_idx = try self.ast.addExtra(param_start, self.allocator);
        _ = try self.ast.addExtra(param_end, self.allocator);
        _ = try self.ast.addExtra(body, self.allocator);

        return self.ast.addNode(.{
            .tag = .lambda_expr,
            .main_token = pipe_tok,
            .data = .{ .lhs = extra_idx, .rhs = 0 },
        }, self.allocator);
    }

    /// Parse the pipe operator (`|>`). Called as an infix handler.
    /// Left-associative: `x |> f |> g` parses as `pipe(pipe(x, f), g)`.
    fn parsePipe(self: *Parser, left: Node.Index) Error!Node.Index {
        const op_tok = self.pos;
        self.advance(); // consume '|>'

        // Right side at one higher precedence (left-associative).
        const next_prec: Precedence = @enumFromInt(@intFromEnum(Precedence.pipe_prec) + 1);
        const right = try self.parsePrecedence(next_prec);

        return self.ast.addNode(.{
            .tag = .pipe_expr,
            .main_token = op_tok,
            .data = .{ .lhs = left, .rhs = right },
        }, self.allocator);
    }

    /// Parse a return statement: `return` or `return expr`
    fn parseReturnStmt(self: *Parser) Error!Node.Index {
        const ret_tok = self.pos;
        self.advance(); // consume 'return'

        // Validate we're inside a function.
        if (self.fn_depth == 0) {
            try self.emitError("'return' outside of function");
            return error.ParseError;
        }

        // Check if there's a value expression or this is a bare return.
        var value: Node.Index = Node.null_node;
        const next = self.peekTag();
        if (next != .right_brace and next != .eof and
            next != .kw_let and next != .kw_if and next != .kw_while and
            next != .kw_for and next != .kw_fn and next != .kw_return)
        {
            value = try self.parseExpression();
        }

        return self.ast.addNode(.{
            .tag = .return_expr,
            .main_token = ret_tok,
            .data = .{ .lhs = value, .rhs = 0 },
        }, self.allocator);
    }

    /// Get token source text for the given token index.
    fn tokenSlice(self: *const Parser, token_index: u32) []const u8 {
        const tok = self.tokens[token_index];
        return self.source[tok.start..tok.end];
    }

    // ── Operator precedence table ─────────────────────────────────────

    fn infixPrecedence(self: *const Parser, tag: Tag) Precedence {
        _ = self;
        return switch (tag) {
            .kw_or => .or_prec,
            .pipe_greater => .pipe_prec,
            .kw_and => .and_prec,
            .equal_equal, .bang_equal => .equality,
            .less, .greater, .less_equal, .greater_equal => .comparison,
            .plus, .minus, .plus_plus => .additive,
            .star, .slash, .percent => .multiplicative,
            .left_paren, .dot => .call,
            else => .none,
        };
    }

    fn tokenToNodeTag(self: *const Parser, tag: Tag) Node.Tag {
        _ = self;
        return switch (tag) {
            .plus => .add,
            .minus => .subtract,
            .star => .multiply,
            .slash => .divide,
            .percent => .modulo,
            .equal_equal => .equal,
            .bang_equal => .not_equal,
            .less => .less,
            .greater => .greater,
            .less_equal => .less_equal,
            .greater_equal => .greater_equal,
            .kw_and => .logical_and,
            .kw_or => .logical_or,
            .plus_plus => .concat,
            else => .expr_stmt, // should not happen
        };
    }

    // ── Token access helpers ──────────────────────────────────────────

    fn peekTag(self: *const Parser) Tag {
        if (self.pos >= self.tokens.len) return .eof;
        return self.tokens[self.pos].tag;
    }

    fn advance(self: *Parser) void {
        if (self.pos < self.tokens.len) {
            self.pos += 1;
        }
    }

    fn atEnd(self: *const Parser) bool {
        return self.peekTag() == .eof;
    }

    // ── Error handling ───────────────────────────────────────────────

    fn emitError(self: *Parser, message: []const u8) Error!void {
        const tok = if (self.pos < self.tokens.len) self.tokens[self.pos] else self.tokens[self.tokens.len - 1];
        try self.ast.errors.append(.{
            .error_code = .E005,
            .severity = .@"error",
            .message = message,
            .span = .{ .start = tok.start, .end = tok.end },
            .labels = &[_]Label{},
            .help = null,
        }, self.allocator);
    }

    fn synchronize(self: *Parser) void {
        while (!self.atEnd()) {
            // Stop at tokens that can start a new statement.
            switch (self.peekTag()) {
                .kw_let, .kw_if, .kw_while, .kw_for, .kw_fn, .kw_return, .kw_type, .kw_match, .right_brace => return,
                else => self.advance(),
            }
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════
// ── Test helpers ──────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════

const lexer_mod = @import("lexer");
const Lexer = lexer_mod.Lexer;

/// Lex + parse source, return AST (caller must deinit).
fn testParse(source: []const u8, allocator: Allocator) !Ast {
    var lex = Lexer.init(source);
    try lex.tokenize(allocator);
    // We must keep the tokens alive for the AST's lifetime.
    // Store them by moving ownership -- the lexer errors can be freed.
    const tokens = lex.tokens.items;

    var ast = try Parser.parse(tokens, source, allocator);
    // Transfer lexer errors -- not needed for test assertions.
    // Clean up lexer error list but keep token array alive (owned by ast.tokens pointer).
    lex.errors.deinit(allocator);
    // Do NOT deinit lex.tokens here -- ownership transferred to `ast.tokens` slice reference.
    // We need to track the token list so we can free it.  Store the raw list in a var
    // so the caller frees it.  Actually, let's use a wrapper.

    // Problem: we must not free the token array until the AST is freed.
    // Solution: we leak the token memory into the AST.  For tests, the testing
    // allocator will catch actual leaks.  Let's create a helper struct.
    _ = &ast;

    // Actually let's just return ast and have the caller also deinit the lexer tokens.
    // We'll use a different approach: return a struct.
    // For simplicity in tests: we won't deinit lexer.tokens here, and rely on
    // TestResult below.
    return ast;
}

const TestResult = struct {
    ast: Ast,
    token_buf: std.ArrayListUnmanaged(Token),

    fn deinit(self: *TestResult, allocator: Allocator) void {
        self.ast.deinit(allocator);
        self.token_buf.deinit(allocator);
    }
};

fn testParseOwned(source: []const u8, allocator: Allocator) !TestResult {
    var lex = Lexer.init(source);
    try lex.tokenize(allocator);
    lex.errors.deinit(allocator);

    var ast = try Parser.parse(lex.tokens.items, source, allocator);
    _ = &ast;

    return .{
        .ast = ast,
        .token_buf = lex.tokens,
    };
}

/// Get the tag of the node at index.
fn nodeTag(ast: *const Ast, idx: Node.Index) Node.Tag {
    return ast.nodes.items(.tag)[idx];
}

/// Get the data of the node at index.
fn nodeData(ast: *const Ast, idx: Node.Index) Node.Data {
    return ast.nodes.items(.data)[idx];
}

/// Get the root node's statement list.
fn rootStmts(ast: *const Ast) []const u32 {
    // Root is always the last node.
    const root_idx: u32 = @intCast(ast.nodes.len - 1);
    const data = ast.nodes.items(.data)[root_idx];
    return ast.extra_data.items[data.lhs..data.rhs];
}

// ═══════════════════════════════════════════════════════════════════════
// ── Tests ──────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════

// Test 1: Parse `42` produces AST with single int_literal node
test "parser: int literal" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("42", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    try std.testing.expectEqual(@as(usize, 1), stmts.len);
    // stmt is an expr_stmt wrapping int_literal
    try std.testing.expectEqual(Node.Tag.expr_stmt, nodeTag(&r.ast, stmts[0]));
    const inner = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.int_literal, nodeTag(&r.ast, inner));
}

// Test 2: Parse `1 + 2` produces binary_op(add) node
test "parser: simple addition" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("1 + 2", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    try std.testing.expectEqual(@as(usize, 1), stmts.len);
    const stmt = stmts[0];
    try std.testing.expectEqual(Node.Tag.expr_stmt, nodeTag(&r.ast, stmt));
    const expr = nodeData(&r.ast, stmt).lhs;
    try std.testing.expectEqual(Node.Tag.add, nodeTag(&r.ast, expr));

    // Children should be int_literal nodes.
    const data = nodeData(&r.ast, expr);
    try std.testing.expectEqual(Node.Tag.int_literal, nodeTag(&r.ast, data.lhs));
    try std.testing.expectEqual(Node.Tag.int_literal, nodeTag(&r.ast, data.rhs));
}

// Test 3: Parse `1 + 2 * 3` respects precedence: add(1, mul(2, 3))
test "parser: operator precedence mul before add" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("1 + 2 * 3", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.add, nodeTag(&r.ast, expr));

    const add_data = nodeData(&r.ast, expr);
    try std.testing.expectEqual(Node.Tag.int_literal, nodeTag(&r.ast, add_data.lhs)); // 1
    try std.testing.expectEqual(Node.Tag.multiply, nodeTag(&r.ast, add_data.rhs)); // 2 * 3

    const mul_data = nodeData(&r.ast, add_data.rhs);
    try std.testing.expectEqual(Node.Tag.int_literal, nodeTag(&r.ast, mul_data.lhs)); // 2
    try std.testing.expectEqual(Node.Tag.int_literal, nodeTag(&r.ast, mul_data.rhs)); // 3
}

// Test 4: Parse `-x` produces negate(identifier)
test "parser: unary negate" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("-x", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.negate, nodeTag(&r.ast, expr));

    const operand = nodeData(&r.ast, expr).lhs;
    try std.testing.expectEqual(Node.Tag.identifier, nodeTag(&r.ast, operand));
}

// Test 5: Parse `not true` produces logical_not(bool_literal)
test "parser: unary not" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("not true", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.logical_not, nodeTag(&r.ast, expr));

    const operand = nodeData(&r.ast, expr).lhs;
    try std.testing.expectEqual(Node.Tag.bool_literal, nodeTag(&r.ast, operand));
}

// Test 6: Parse `let x = 42` produces let_decl node
test "parser: let declaration" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("let x = 42", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    try std.testing.expectEqual(@as(usize, 1), stmts.len);
    try std.testing.expectEqual(Node.Tag.let_decl, nodeTag(&r.ast, stmts[0]));

    const data = nodeData(&r.ast, stmts[0]);
    // data.rhs should be an int_literal node.
    try std.testing.expectEqual(Node.Tag.int_literal, nodeTag(&r.ast, data.rhs));
}

// Test 7: Parse `if x > 0 { x } else { -x }` produces if_expr
test "parser: if else expression" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("if x > 0 { x } else { -x }", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.if_expr, nodeTag(&r.ast, expr));

    // condition is x > 0
    const if_data = nodeData(&r.ast, expr);
    try std.testing.expectEqual(Node.Tag.greater, nodeTag(&r.ast, if_data.lhs));

    // then/else stored in extra_data
    const then_branch = r.ast.extra_data.items[if_data.rhs];
    const else_branch = r.ast.extra_data.items[if_data.rhs + 1];
    try std.testing.expectEqual(Node.Tag.block_expr, nodeTag(&r.ast, then_branch));
    try std.testing.expectEqual(Node.Tag.block_expr, nodeTag(&r.ast, else_branch));
    try std.testing.expect(else_branch != Node.null_node);
}

// Test 8: Parse `while x > 0 { x = x - 1 }` produces while_stmt
test "parser: while statement" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("while x > 0 { x = x - 1 }", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    try std.testing.expectEqual(@as(usize, 1), stmts.len);
    try std.testing.expectEqual(Node.Tag.while_stmt, nodeTag(&r.ast, stmts[0]));

    const data = nodeData(&r.ast, stmts[0]);
    // condition: x > 0
    try std.testing.expectEqual(Node.Tag.greater, nodeTag(&r.ast, data.lhs));
    // body: block_expr
    try std.testing.expectEqual(Node.Tag.block_expr, nodeTag(&r.ast, data.rhs));
}

// Test 9: Parse `for i in range(10) { print(i) }` produces for_stmt
test "parser: for-in statement" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("for i in range(10) { print(i) }", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    try std.testing.expectEqual(@as(usize, 1), stmts.len);
    try std.testing.expectEqual(Node.Tag.for_stmt, nodeTag(&r.ast, stmts[0]));

    const data = nodeData(&r.ast, stmts[0]);
    // lhs = iterable (call_expr: range(10))
    try std.testing.expectEqual(Node.Tag.call_expr, nodeTag(&r.ast, data.lhs));
}

// Test 10: Parse `{ let x = 1\n x + 2 }` produces block_expr
test "parser: block expression" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("{ let x = 1\n x + 2 }", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.block_expr, nodeTag(&r.ast, expr));

    // Block should contain 2 statements: let_decl and expr_stmt
    const block_data = nodeData(&r.ast, expr);
    const block_stmts = r.ast.extra_data.items[block_data.lhs..block_data.rhs];
    try std.testing.expectEqual(@as(usize, 2), block_stmts.len);
    try std.testing.expectEqual(Node.Tag.let_decl, nodeTag(&r.ast, block_stmts[0]));
    try std.testing.expectEqual(Node.Tag.expr_stmt, nodeTag(&r.ast, block_stmts[1]));
}

// Test 11: Parse `:ok` produces atom_literal node
test "parser: atom literal" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned(":ok", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.atom_literal, nodeTag(&r.ast, expr));
}

// Test 12: Parse `x and y or z` produces correct precedence: or(and(x, y), z)
test "parser: and/or precedence" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("x and y or z", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.logical_or, nodeTag(&r.ast, expr));

    const or_data = nodeData(&r.ast, expr);
    try std.testing.expectEqual(Node.Tag.logical_and, nodeTag(&r.ast, or_data.lhs));
    try std.testing.expectEqual(Node.Tag.identifier, nodeTag(&r.ast, or_data.rhs)); // z

    const and_data = nodeData(&r.ast, or_data.lhs);
    try std.testing.expectEqual(Node.Tag.identifier, nodeTag(&r.ast, and_data.lhs)); // x
    try std.testing.expectEqual(Node.Tag.identifier, nodeTag(&r.ast, and_data.rhs)); // y
}

// Test 13: Parse `a == b` produces equal node
test "parser: equality" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("a == b", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.equal, nodeTag(&r.ast, expr));
}

// Test 14: Parse multiple statements produces root with all
test "parser: multiple statements" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("let x = 1\nlet y = 2\nx + y", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    try std.testing.expectEqual(@as(usize, 3), stmts.len);
    try std.testing.expectEqual(Node.Tag.let_decl, nodeTag(&r.ast, stmts[0]));
    try std.testing.expectEqual(Node.Tag.let_decl, nodeTag(&r.ast, stmts[1]));
    try std.testing.expectEqual(Node.Tag.expr_stmt, nodeTag(&r.ast, stmts[2]));
}

// Test 15: Parse error produces diagnostic, parser recovers
test "parser: error recovery" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("let = 42\nlet y = 10", allocator);
    defer r.deinit(allocator);

    // Should have at least one error diagnostic.
    try std.testing.expect(r.ast.errors.items.items.len > 0);

    // Should still have parsed the second statement.
    const stmts = rootStmts(&r.ast);
    try std.testing.expect(stmts.len >= 1);
    // Find the let_decl for y.
    var found_y = false;
    for (stmts) |s| {
        if (nodeTag(&r.ast, s) == .let_decl) {
            found_y = true;
        }
    }
    try std.testing.expect(found_y);
}

// Test 16: Operator precedence: unary > multiplicative > additive > comparison > equality > and > or
test "parser: full precedence chain" {
    const allocator = std.testing.allocator;
    // -1 * 2 + 3 < 4 == true and false or true
    // Should parse as: or(and(equal(less(add(mul(negate(1), 2), 3), 4), true), false), true)
    var r = try testParseOwned("-1 * 2 + 3 < 4 == true and false or true", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    // Top level should be 'or'
    try std.testing.expectEqual(Node.Tag.logical_or, nodeTag(&r.ast, expr));
}

// Test 17: Parse `x ++ y` produces concat node
test "parser: string concatenation" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("x ++ y", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.concat, nodeTag(&r.ast, expr));
}

// Test 18: Parse `print(x)` produces call_expr
test "parser: function call" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("print(x)", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.call_expr, nodeTag(&r.ast, expr));

    // Callee should be identifier
    const call_data = nodeData(&r.ast, expr);
    try std.testing.expectEqual(Node.Tag.identifier, nodeTag(&r.ast, call_data.lhs));
}

// Additional tests:

test "parser: nil literal" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("nil", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.nil_literal, nodeTag(&r.ast, expr));
}

test "parser: bool literal" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("true", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.bool_literal, nodeTag(&r.ast, expr));
}

test "parser: string literal" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("\"hello\"", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.string_literal, nodeTag(&r.ast, expr));
}

test "parser: float literal" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("3.14", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.float_literal, nodeTag(&r.ast, expr));
}

test "parser: grouped expression" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("(1 + 2) * 3", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    // Top: multiply
    try std.testing.expectEqual(Node.Tag.multiply, nodeTag(&r.ast, expr));
    // lhs: grouped_expr
    const mul_data = nodeData(&r.ast, expr);
    try std.testing.expectEqual(Node.Tag.grouped_expr, nodeTag(&r.ast, mul_data.lhs));
}

test "parser: call with multiple args" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("add(1, 2, 3)", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.call_expr, nodeTag(&r.ast, expr));

    // Check arg count via extra_data.
    const call_data = nodeData(&r.ast, expr);
    const extra_idx = call_data.rhs;
    const arg_start = r.ast.extra_data.items[extra_idx];
    const arg_end = r.ast.extra_data.items[extra_idx + 1];
    try std.testing.expectEqual(@as(u32, 3), arg_end - arg_start);
}

test "parser: not equal" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("a != b", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.not_equal, nodeTag(&r.ast, expr));
}

test "parser: all comparison operators" {
    const allocator = std.testing.allocator;

    const ops = [_]struct { src: []const u8, tag: Node.Tag }{
        .{ .src = "a < b", .tag = .less },
        .{ .src = "a > b", .tag = .greater },
        .{ .src = "a <= b", .tag = .less_equal },
        .{ .src = "a >= b", .tag = .greater_equal },
    };

    for (ops) |op| {
        var r = try testParseOwned(op.src, allocator);
        defer r.deinit(allocator);

        const stmts = rootStmts(&r.ast);
        const expr = nodeData(&r.ast, stmts[0]).lhs;
        try std.testing.expectEqual(op.tag, nodeTag(&r.ast, expr));
    }
}

test "parser: assignment statement" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("x = 42", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    try std.testing.expectEqual(@as(usize, 1), stmts.len);
    try std.testing.expectEqual(Node.Tag.assign_stmt, nodeTag(&r.ast, stmts[0]));
}

test "parser: modulo operator" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("10 % 3", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.modulo, nodeTag(&r.ast, expr));
}

test "parser: if without else" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("if x { 1 }", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.if_expr, nodeTag(&r.ast, expr));

    // else_branch should be null_node
    const if_data = nodeData(&r.ast, expr);
    const else_branch = r.ast.extra_data.items[if_data.rhs + 1];
    try std.testing.expectEqual(Node.null_node, else_branch);
}

// ═══════════════════════════════════════════════════════════════════════
// ── Phase 2: Function, Lambda, Pipe, Return, Named Arg Tests ─────────
// ═══════════════════════════════════════════════════════════════════════

test "parser: fn declaration with 2 params" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("fn add(a, b) { a + b }", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    try std.testing.expectEqual(@as(usize, 1), stmts.len);
    try std.testing.expectEqual(Node.Tag.fn_decl, nodeTag(&r.ast, stmts[0]));

    // Extract extra_data for fn_decl.
    const fn_data = nodeData(&r.ast, stmts[0]);
    const extra_idx = fn_data.lhs;
    const name_tok = r.ast.extra_data.items[extra_idx];
    const param_start = r.ast.extra_data.items[extra_idx + 1];
    const param_end = r.ast.extra_data.items[extra_idx + 2];
    const body_node = r.ast.extra_data.items[extra_idx + 3];

    // Name should be "add".
    const name_slice = r.ast.source[r.ast.tokens[name_tok].start..r.ast.tokens[name_tok].end];
    try std.testing.expectEqualStrings("add", name_slice);

    // 2 params.
    try std.testing.expectEqual(@as(u32, 2), param_end - param_start);

    // Body is a block_expr.
    try std.testing.expectEqual(Node.Tag.block_expr, nodeTag(&r.ast, body_node));
}

test "parser: fn with named param and default" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("fn greet(name, greeting: \"hello\") { name }", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    try std.testing.expectEqual(Node.Tag.fn_decl, nodeTag(&r.ast, stmts[0]));

    const fn_data = nodeData(&r.ast, stmts[0]);
    const extra_idx = fn_data.lhs;
    const param_start = r.ast.extra_data.items[extra_idx + 1];
    const param_end = r.ast.extra_data.items[extra_idx + 2];
    const defaults_start = r.ast.extra_data.items[extra_idx + 4];
    const defaults_end = r.ast.extra_data.items[extra_idx + 5];

    // 2 params total.
    try std.testing.expectEqual(@as(u32, 2), param_end - param_start);
    // 1 default (for "greeting").
    try std.testing.expectEqual(@as(u32, 1), defaults_end - defaults_start);
}

test "parser: lambda with one param" {
    const allocator = std.testing.allocator;
    // Wrap lambda in parentheses to ensure it's parsed as expression.
    var r = try testParseOwned("(|x| x + 1)", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    // expr_stmt -> grouped_expr -> lambda_expr
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.grouped_expr, nodeTag(&r.ast, expr));
    const inner = nodeData(&r.ast, expr).lhs;
    try std.testing.expectEqual(Node.Tag.lambda_expr, nodeTag(&r.ast, inner));

    // Extract lambda extra_data.
    const lam_data = nodeData(&r.ast, inner);
    const lam_extra = lam_data.lhs;
    const pstart = r.ast.extra_data.items[lam_extra];
    const pend = r.ast.extra_data.items[lam_extra + 1];
    const body_node = r.ast.extra_data.items[lam_extra + 2];

    // 1 param.
    try std.testing.expectEqual(@as(u32, 1), pend - pstart);
    // Body is an add expression.
    try std.testing.expectEqual(Node.Tag.add, nodeTag(&r.ast, body_node));
}

test "parser: lambda with two params" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("(|a, b| a + b)", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    const inner = nodeData(&r.ast, expr).lhs;
    try std.testing.expectEqual(Node.Tag.lambda_expr, nodeTag(&r.ast, inner));

    const lam_data = nodeData(&r.ast, inner);
    const lam_extra = lam_data.lhs;
    const pstart = r.ast.extra_data.items[lam_extra];
    const pend = r.ast.extra_data.items[lam_extra + 1];

    // 2 params.
    try std.testing.expectEqual(@as(u32, 2), pend - pstart);
}

test "parser: zero-arg lambda |_|" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("(|_| 42)", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    const inner = nodeData(&r.ast, expr).lhs;
    try std.testing.expectEqual(Node.Tag.lambda_expr, nodeTag(&r.ast, inner));

    const lam_data = nodeData(&r.ast, inner);
    const lam_extra = lam_data.lhs;
    const pstart = r.ast.extra_data.items[lam_extra];
    const pend = r.ast.extra_data.items[lam_extra + 1];

    // 0 params (wildcard).
    try std.testing.expectEqual(@as(u32, 0), pend - pstart);
}

test "parser: pipe expression x |> f" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("x |> f", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.pipe_expr, nodeTag(&r.ast, expr));

    const pipe_data = nodeData(&r.ast, expr);
    try std.testing.expectEqual(Node.Tag.identifier, nodeTag(&r.ast, pipe_data.lhs)); // x
    try std.testing.expectEqual(Node.Tag.identifier, nodeTag(&r.ast, pipe_data.rhs)); // f
}

test "parser: pipe chain left-associative" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("x |> f |> g", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    // Top level should be pipe_expr(pipe_expr(x, f), g)
    try std.testing.expectEqual(Node.Tag.pipe_expr, nodeTag(&r.ast, expr));

    const outer = nodeData(&r.ast, expr);
    try std.testing.expectEqual(Node.Tag.pipe_expr, nodeTag(&r.ast, outer.lhs)); // pipe(x, f)
    try std.testing.expectEqual(Node.Tag.identifier, nodeTag(&r.ast, outer.rhs)); // g

    const inner = nodeData(&r.ast, outer.lhs);
    try std.testing.expectEqual(Node.Tag.identifier, nodeTag(&r.ast, inner.lhs)); // x
    try std.testing.expectEqual(Node.Tag.identifier, nodeTag(&r.ast, inner.rhs)); // f
}

test "parser: pipe with call expression" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("x |> f(y)", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.pipe_expr, nodeTag(&r.ast, expr));

    const pipe_data = nodeData(&r.ast, expr);
    try std.testing.expectEqual(Node.Tag.identifier, nodeTag(&r.ast, pipe_data.lhs)); // x
    try std.testing.expectEqual(Node.Tag.call_expr, nodeTag(&r.ast, pipe_data.rhs)); // f(y)
}

test "parser: return with value" {
    const allocator = std.testing.allocator;
    // return inside fn
    var r = try testParseOwned("fn foo() { return 42 }", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    try std.testing.expectEqual(Node.Tag.fn_decl, nodeTag(&r.ast, stmts[0]));

    // Get the body block.
    const fn_data = nodeData(&r.ast, stmts[0]);
    const extra_idx = fn_data.lhs;
    const body_node = r.ast.extra_data.items[extra_idx + 3];
    try std.testing.expectEqual(Node.Tag.block_expr, nodeTag(&r.ast, body_node));

    // Block should contain a return_expr statement.
    const block_data = nodeData(&r.ast, body_node);
    const block_stmts = r.ast.extra_data.items[block_data.lhs..block_data.rhs];
    try std.testing.expectEqual(@as(usize, 1), block_stmts.len);
    try std.testing.expectEqual(Node.Tag.return_expr, nodeTag(&r.ast, block_stmts[0]));

    // Return value should be int_literal.
    const ret_data = nodeData(&r.ast, block_stmts[0]);
    try std.testing.expectEqual(Node.Tag.int_literal, nodeTag(&r.ast, ret_data.lhs));
}

test "parser: bare return" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("fn foo() { return }", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const fn_data = nodeData(&r.ast, stmts[0]);
    const extra_idx = fn_data.lhs;
    const body_node = r.ast.extra_data.items[extra_idx + 3];
    const block_data = nodeData(&r.ast, body_node);
    const block_stmts = r.ast.extra_data.items[block_data.lhs..block_data.rhs];

    try std.testing.expectEqual(@as(usize, 1), block_stmts.len);
    try std.testing.expectEqual(Node.Tag.return_expr, nodeTag(&r.ast, block_stmts[0]));
    // Bare return has null_node as value.
    const ret_data = nodeData(&r.ast, block_stmts[0]);
    try std.testing.expectEqual(Node.null_node, ret_data.lhs);
}

test "parser: named argument in call" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("f(x, name: val)", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.call_expr, nodeTag(&r.ast, expr));

    // Check args: first is positional (identifier), second is named_arg.
    const call_data = nodeData(&r.ast, expr);
    const extra_idx = call_data.rhs;
    const arg_start = r.ast.extra_data.items[extra_idx];
    const arg_end = r.ast.extra_data.items[extra_idx + 1];
    const args = r.ast.extra_data.items[arg_start..arg_end];

    try std.testing.expectEqual(@as(usize, 2), args.len);
    try std.testing.expectEqual(Node.Tag.identifier, nodeTag(&r.ast, args[0]));
    try std.testing.expectEqual(Node.Tag.named_arg, nodeTag(&r.ast, args[1]));
}

test "parser: multiple named arguments" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("f(x, b: 2, a: 1)", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.call_expr, nodeTag(&r.ast, expr));

    const call_data = nodeData(&r.ast, expr);
    const extra_idx = call_data.rhs;
    const arg_start = r.ast.extra_data.items[extra_idx];
    const arg_end = r.ast.extra_data.items[extra_idx + 1];
    const args = r.ast.extra_data.items[arg_start..arg_end];

    try std.testing.expectEqual(@as(usize, 3), args.len);
    try std.testing.expectEqual(Node.Tag.identifier, nodeTag(&r.ast, args[0])); // x (positional)
    try std.testing.expectEqual(Node.Tag.named_arg, nodeTag(&r.ast, args[1])); // b: 2
    try std.testing.expectEqual(Node.Tag.named_arg, nodeTag(&r.ast, args[2])); // a: 1
}

test "parser: pipe precedence lower than call, higher than or" {
    const allocator = std.testing.allocator;
    // `a or b |> f` should parse as `a or (b |> f)` because pipe > or.
    var r = try testParseOwned("a or b |> f", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    // Top level should be 'or'
    try std.testing.expectEqual(Node.Tag.logical_or, nodeTag(&r.ast, expr));

    const or_data = nodeData(&r.ast, expr);
    try std.testing.expectEqual(Node.Tag.identifier, nodeTag(&r.ast, or_data.lhs)); // a
    try std.testing.expectEqual(Node.Tag.pipe_expr, nodeTag(&r.ast, or_data.rhs)); // b |> f
}

test "parser: fn expression (anonymous)" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("let f = fn(x) { x + 1 }", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    try std.testing.expectEqual(Node.Tag.let_decl, nodeTag(&r.ast, stmts[0]));

    // The initializer should be fn_decl.
    const let_data = nodeData(&r.ast, stmts[0]);
    try std.testing.expectEqual(Node.Tag.fn_decl, nodeTag(&r.ast, let_data.rhs));
}

test "parser: return outside function is error" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("return 42", allocator);
    defer r.deinit(allocator);

    // Should have parse errors.
    try std.testing.expect(r.ast.errors.items.items.len > 0);
}

test "parser: fn with zero params" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("fn noop() { nil }", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    try std.testing.expectEqual(Node.Tag.fn_decl, nodeTag(&r.ast, stmts[0]));

    const fn_data = nodeData(&r.ast, stmts[0]);
    const extra_idx = fn_data.lhs;
    const param_start = r.ast.extra_data.items[extra_idx + 1];
    const param_end = r.ast.extra_data.items[extra_idx + 2];
    try std.testing.expectEqual(@as(u32, 0), param_end - param_start);
}

// ── Phase 3 Collection Literal Parsing Tests ─────────────────────────

test "parser: empty list literal []" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("[]", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.list_literal, nodeTag(&r.ast, expr));
    const data = nodeData(&r.ast, expr);
    try std.testing.expectEqual(data.lhs, data.rhs); // zero elements
}

test "parser: list literal [1, 2, 3]" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("[1, 2, 3]", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.list_literal, nodeTag(&r.ast, expr));
    const data = nodeData(&r.ast, expr);
    const elements = r.ast.extra_data.items[data.lhs..data.rhs];
    try std.testing.expectEqual(@as(usize, 3), elements.len);
    try std.testing.expectEqual(Node.Tag.int_literal, nodeTag(&r.ast, elements[0]));
    try std.testing.expectEqual(Node.Tag.int_literal, nodeTag(&r.ast, elements[1]));
    try std.testing.expectEqual(Node.Tag.int_literal, nodeTag(&r.ast, elements[2]));
}

test "parser: list literal with trailing comma" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("[1, 2,]", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.list_literal, nodeTag(&r.ast, expr));
    const data = nodeData(&r.ast, expr);
    const elements = r.ast.extra_data.items[data.lhs..data.rhs];
    try std.testing.expectEqual(@as(usize, 2), elements.len);
}

test "parser: empty map literal {}" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("{}", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.map_literal, nodeTag(&r.ast, expr));
    const data = nodeData(&r.ast, expr);
    try std.testing.expectEqual(data.lhs, data.rhs); // zero pairs
}

test "parser: map literal with string keys" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned(
        \\{"a": 1, "b": 2}
    , allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.map_literal, nodeTag(&r.ast, expr));
    const data = nodeData(&r.ast, expr);
    const pairs = r.ast.extra_data.items[data.lhs..data.rhs];
    // 2 pairs * 2 (key + value) = 4
    try std.testing.expectEqual(@as(usize, 4), pairs.len);
    try std.testing.expectEqual(Node.Tag.string_literal, nodeTag(&r.ast, pairs[0])); // key "a"
    try std.testing.expectEqual(Node.Tag.int_literal, nodeTag(&r.ast, pairs[1])); // value 1
}

test "parser: record literal {name: expr}" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned(
        \\{name: "alice", age: 30}
    , allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.record_literal, nodeTag(&r.ast, expr));
    const data = nodeData(&r.ast, expr);
    const pairs = r.ast.extra_data.items[data.lhs..data.rhs];
    // 2 fields * 2 (name_token + value_node) = 4
    try std.testing.expectEqual(@as(usize, 4), pairs.len);
}

test "parser: record spread {..base, field: val}" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("{..r, name: 42}", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.record_spread, nodeTag(&r.ast, expr));
    const data = nodeData(&r.ast, expr);
    // lhs = base record node
    try std.testing.expectEqual(Node.Tag.identifier, nodeTag(&r.ast, data.lhs));
    // rhs = extra_idx pointing to override start/end
    const extra_idx = data.rhs;
    const override_start = r.ast.extra_data.items[extra_idx];
    const override_end = r.ast.extra_data.items[extra_idx + 1];
    // 1 override * 2 (name_token + value_node) = 2
    try std.testing.expectEqual(@as(u32, 2), override_end - override_start);
}

test "parser: grouped expression (expr) still works" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("(42)", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.grouped_expr, nodeTag(&r.ast, expr));
    const inner = nodeData(&r.ast, expr).lhs;
    try std.testing.expectEqual(Node.Tag.int_literal, nodeTag(&r.ast, inner));
}

test "parser: tuple literal (1, 2)" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("(1, 2)", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.tuple_literal, nodeTag(&r.ast, expr));
    const data = nodeData(&r.ast, expr);
    const elements = r.ast.extra_data.items[data.lhs..data.rhs];
    try std.testing.expectEqual(@as(usize, 2), elements.len);
    try std.testing.expectEqual(Node.Tag.int_literal, nodeTag(&r.ast, elements[0]));
    try std.testing.expectEqual(Node.Tag.int_literal, nodeTag(&r.ast, elements[1]));
}

test "parser: single-element tuple with trailing comma (expr,)" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("(42,)", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.tuple_literal, nodeTag(&r.ast, expr));
    const data = nodeData(&r.ast, expr);
    const elements = r.ast.extra_data.items[data.lhs..data.rhs];
    try std.testing.expectEqual(@as(usize, 1), elements.len);
}

test "parser: dot access expr.field" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("x.y", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.field_access, nodeTag(&r.ast, expr));
    const data = nodeData(&r.ast, expr);
    try std.testing.expectEqual(Node.Tag.identifier, nodeTag(&r.ast, data.lhs)); // x
    // data.rhs is the token index for "y"
    const field_name = r.ast.tokenSlice(data.rhs);
    try std.testing.expectEqualStrings("y", field_name);
}

test "parser: chained dot access a.b.c" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("a.b.c", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    // Top level: field_access(field_access(a, b), c)
    try std.testing.expectEqual(Node.Tag.field_access, nodeTag(&r.ast, expr));
    const outer = nodeData(&r.ast, expr);
    try std.testing.expectEqual(Node.Tag.field_access, nodeTag(&r.ast, outer.lhs));
    const field_name = r.ast.tokenSlice(outer.rhs);
    try std.testing.expectEqualStrings("c", field_name);
}

test "parser: block expression still works with { statements }" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("{ let x = 1 }", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.block_expr, nodeTag(&r.ast, expr));
}

// ── Phase 3 ADT and Pattern Matching Parsing Tests ────────────────────

test "parser: type declaration with variants" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("type Color = | Red | Green | Blue | Hex(String)", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    try std.testing.expectEqual(@as(usize, 1), stmts.len);
    try std.testing.expectEqual(Node.Tag.type_decl, nodeTag(&r.ast, stmts[0]));

    // Extract extra_data: [name_token, variant_count, then per variant: name_token, arity]
    const data = nodeData(&r.ast, stmts[0]);
    const extra_idx = data.lhs;
    const ed = r.ast.extra_data.items;
    const variant_count = ed[extra_idx + 1];
    try std.testing.expectEqual(@as(u32, 4), variant_count);

    // Red: arity 0
    try std.testing.expectEqual(@as(u32, 0), ed[extra_idx + 3]); // Red arity
    // Green: arity 0
    try std.testing.expectEqual(@as(u32, 0), ed[extra_idx + 5]); // Green arity
    // Blue: arity 0
    try std.testing.expectEqual(@as(u32, 0), ed[extra_idx + 7]); // Blue arity
    // Hex: arity 1
    try std.testing.expectEqual(@as(u32, 1), ed[extra_idx + 9]); // Hex arity
}

test "parser: type declaration with multi-field variant" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("type Shape = | Circle(Float) | Rect(Float, Float)", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    try std.testing.expectEqual(Node.Tag.type_decl, nodeTag(&r.ast, stmts[0]));

    const data = nodeData(&r.ast, stmts[0]);
    const extra_idx = data.lhs;
    const ed = r.ast.extra_data.items;
    const variant_count = ed[extra_idx + 1];
    try std.testing.expectEqual(@as(u32, 2), variant_count);

    // Circle: arity 1
    try std.testing.expectEqual(@as(u32, 1), ed[extra_idx + 3]);
    // Rect: arity 2
    try std.testing.expectEqual(@as(u32, 2), ed[extra_idx + 5]);
}

test "parser: match expression with literal patterns" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("match x | 1 -> \"one\" | 2 -> \"two\" | _ -> \"other\"", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.match_expr, nodeTag(&r.ast, expr));

    // Scrutinee is identifier.
    const match_data = nodeData(&r.ast, expr);
    try std.testing.expectEqual(Node.Tag.identifier, nodeTag(&r.ast, match_data.lhs));

    // 3 arms stored in extra_data.
    const arms_extra = match_data.rhs;
    const arms_start = r.ast.extra_data.items[arms_extra];
    const arms_end = r.ast.extra_data.items[arms_extra + 1];
    const arms = r.ast.extra_data.items[arms_start..arms_end];
    try std.testing.expectEqual(@as(usize, 3), arms.len);

    // First arm: match_arm with pattern_literal
    try std.testing.expectEqual(Node.Tag.match_arm, nodeTag(&r.ast, arms[0]));
    const arm0_data = nodeData(&r.ast, arms[0]);
    try std.testing.expectEqual(Node.Tag.pattern_literal, nodeTag(&r.ast, arm0_data.lhs));

    // Third arm: match_arm with pattern_wildcard
    try std.testing.expectEqual(Node.Tag.match_arm, nodeTag(&r.ast, arms[2]));
    const arm2_data = nodeData(&r.ast, arms[2]);
    try std.testing.expectEqual(Node.Tag.pattern_wildcard, nodeTag(&r.ast, arm2_data.lhs));
}

test "parser: match with guarded arm" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("match x | n when n > 0 -> n | _ -> 0", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.match_expr, nodeTag(&r.ast, expr));

    const match_data = nodeData(&r.ast, expr);
    const arms_extra = match_data.rhs;
    const arms_start = r.ast.extra_data.items[arms_extra];
    const arms_end = r.ast.extra_data.items[arms_extra + 1];
    const arms = r.ast.extra_data.items[arms_start..arms_end];
    try std.testing.expectEqual(@as(usize, 2), arms.len);

    // First arm is guarded.
    try std.testing.expectEqual(Node.Tag.match_arm_guarded, nodeTag(&r.ast, arms[0]));
    // Second arm is unguarded wildcard.
    try std.testing.expectEqual(Node.Tag.match_arm, nodeTag(&r.ast, arms[1]));
}

test "parser: match with ADT pattern" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("match v | Option.Some(x) -> x | Option.None -> 0", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.match_expr, nodeTag(&r.ast, expr));

    const match_data = nodeData(&r.ast, expr);
    const arms_extra = match_data.rhs;
    const arms_start = r.ast.extra_data.items[arms_extra];
    const arms_end = r.ast.extra_data.items[arms_extra + 1];
    const arms = r.ast.extra_data.items[arms_start..arms_end];
    try std.testing.expectEqual(@as(usize, 2), arms.len);

    // First arm has ADT pattern.
    const arm0_data = nodeData(&r.ast, arms[0]);
    try std.testing.expectEqual(Node.Tag.pattern_adt, nodeTag(&r.ast, arm0_data.lhs));
}

test "parser: match with binding pattern" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("match x | val -> val", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.match_expr, nodeTag(&r.ast, expr));

    const match_data = nodeData(&r.ast, expr);
    const arms_extra = match_data.rhs;
    const arms_start = r.ast.extra_data.items[arms_extra];
    const arms_end = r.ast.extra_data.items[arms_extra + 1];
    const arms = r.ast.extra_data.items[arms_start..arms_end];

    const arm_data = nodeData(&r.ast, arms[0]);
    try std.testing.expectEqual(Node.Tag.pattern_binding, nodeTag(&r.ast, arm_data.lhs));
}

test "parser: list pattern [a, b, ..rest]" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("match xs | [a, b, ..rest] -> a", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    try std.testing.expectEqual(Node.Tag.match_expr, nodeTag(&r.ast, expr));

    const match_data = nodeData(&r.ast, expr);
    const arms_extra = match_data.rhs;
    const arms_start = r.ast.extra_data.items[arms_extra];
    const arms_end = r.ast.extra_data.items[arms_extra + 1];
    const arms = r.ast.extra_data.items[arms_start..arms_end];

    const arm_data = nodeData(&r.ast, arms[0]);
    try std.testing.expectEqual(Node.Tag.pattern_list, nodeTag(&r.ast, arm_data.lhs));

    // List pattern should have 3 elements: binding a, binding b, rest.
    const list_data = nodeData(&r.ast, arm_data.lhs);
    const elements = r.ast.extra_data.items[list_data.lhs..list_data.rhs];
    try std.testing.expectEqual(@as(usize, 3), elements.len);
    try std.testing.expectEqual(Node.Tag.pattern_binding, nodeTag(&r.ast, elements[0]));
    try std.testing.expectEqual(Node.Tag.pattern_binding, nodeTag(&r.ast, elements[1]));
    try std.testing.expectEqual(Node.Tag.pattern_rest, nodeTag(&r.ast, elements[2]));
}

test "parser: tuple pattern (a, b)" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("match p | (x, y) -> x", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    const match_data = nodeData(&r.ast, expr);
    const arms_extra = match_data.rhs;
    const arms_start = r.ast.extra_data.items[arms_extra];
    const arms_end = r.ast.extra_data.items[arms_extra + 1];
    const arms = r.ast.extra_data.items[arms_start..arms_end];

    const arm_data = nodeData(&r.ast, arms[0]);
    try std.testing.expectEqual(Node.Tag.pattern_tuple, nodeTag(&r.ast, arm_data.lhs));
}

test "parser: record pattern {field: pat}" {
    const allocator = std.testing.allocator;
    var r = try testParseOwned("match r | {name: n, age: a} -> n", allocator);
    defer r.deinit(allocator);

    const stmts = rootStmts(&r.ast);
    const expr = nodeData(&r.ast, stmts[0]).lhs;
    const match_data = nodeData(&r.ast, expr);
    const arms_extra = match_data.rhs;
    const arms_start = r.ast.extra_data.items[arms_extra];
    const arms_end = r.ast.extra_data.items[arms_extra + 1];
    const arms = r.ast.extra_data.items[arms_start..arms_end];

    const arm_data = nodeData(&r.ast, arms[0]);
    try std.testing.expectEqual(Node.Tag.pattern_record, nodeTag(&r.ast, arm_data.lhs));

    // Record pattern should have 4 entries (2 fields * 2).
    const rec_data = nodeData(&r.ast, arm_data.lhs);
    const pairs = r.ast.extra_data.items[rec_data.lhs..rec_data.rhs];
    try std.testing.expectEqual(@as(usize, 4), pairs.len);
}
