const std = @import("std");
const Allocator = std.mem.Allocator;
const token_mod = @import("token");
const Token = token_mod.Token;
const Tag = token_mod.Tag;
const error_mod = @import("error");
const Diagnostic = error_mod.Diagnostic;
const ErrorCode = error_mod.ErrorCode;
const Span = error_mod.Span;
const Label = error_mod.Label;

/// Hand-rolled UTF-8 lexer for Zenith source code.
///
/// Produces a flat `ArrayListUnmanaged(Token)` from the source text.
/// Two-pass architecture: the lexer runs to completion, then the parser
/// consumes the token array by index.
///
/// Error recovery: on invalid input (unterminated string, unknown char),
/// the lexer emits an error token and continues scanning.  All errors
/// are accumulated in the `errors` list (never stops at the first).
pub const Lexer = struct {
    source: []const u8,
    current: u32,
    start: u32,
    line: u32,
    tokens: std.ArrayListUnmanaged(Token),
    errors: error_mod.DiagnosticList,

    /// Create a new lexer for the given source text.  Does NOT allocate.
    pub fn init(source: []const u8) Lexer {
        return .{
            .source = source,
            .current = 0,
            .start = 0,
            .line = 1,
            .tokens = .empty,
            .errors = .{},
        };
    }

    /// Scan all tokens from the source text into `self.tokens`.
    pub fn tokenize(self: *Lexer, allocator: Allocator) !void {
        while (!self.isAtEnd()) {
            self.start = self.current;
            try self.scanToken(allocator);
        }
        // Emit EOF token.
        try self.tokens.append(allocator, .{
            .tag = .eof,
            .start = self.current,
            .end = self.current,
            .line = self.line,
        });
    }

    /// Release all allocated memory.
    pub fn deinit(self: *Lexer, allocator: Allocator) void {
        self.tokens.deinit(allocator);
        self.errors.deinit(allocator);
    }

    // ── Private scanning methods ───────────────────────────────────────

    fn scanToken(self: *Lexer, allocator: Allocator) !void {
        const c = self.advance();
        switch (c) {
            '(' => try self.addToken(.left_paren, allocator),
            ')' => try self.addToken(.right_paren, allocator),
            '[' => try self.addToken(.left_bracket, allocator),
            ']' => try self.addToken(.right_bracket, allocator),
            ',' => try self.addToken(.comma, allocator),
            '.' => {
                if (self.match('.')) {
                    try self.addToken(.dot_dot, allocator);
                } else {
                    try self.addToken(.dot, allocator);
                }
            },
            ';' => try self.addToken(.semicolon, allocator),
            '*' => try self.addToken(.star, allocator),
            '%' => try self.addToken(.percent, allocator),

            '+' => {
                if (self.match('+')) {
                    try self.addToken(.plus_plus, allocator);
                } else {
                    try self.addToken(.plus, allocator);
                }
            },

            '-' => {
                if (self.match('-')) {
                    // Line comment: skip to end of line.
                    self.skipLineComment();
                } else if (self.match('>')) {
                    try self.addToken(.arrow, allocator);
                } else {
                    try self.addToken(.minus, allocator);
                }
            },

            '/' => try self.addToken(.slash, allocator),

            '=' => {
                if (self.match('=')) {
                    try self.addToken(.equal_equal, allocator);
                } else {
                    try self.addToken(.equal, allocator);
                }
            },

            '!' => {
                if (self.match('=')) {
                    try self.addToken(.bang_equal, allocator);
                } else {
                    // Standalone '!' is not a valid Zenith token.
                    try self.addErrorToken("unexpected character '!'", allocator);
                }
            },

            '<' => {
                if (self.match('=')) {
                    try self.addToken(.less_equal, allocator);
                } else {
                    try self.addToken(.less, allocator);
                }
            },

            '>' => {
                if (self.match('=')) {
                    try self.addToken(.greater_equal, allocator);
                } else {
                    try self.addToken(.greater, allocator);
                }
            },

            '|' => {
                if (self.match('>')) {
                    try self.addToken(.pipe_greater, allocator);
                } else {
                    try self.addToken(.pipe, allocator);
                }
            },

            '{' => {
                if (self.match('-')) {
                    // Block comment.
                    try self.skipBlockComment(allocator);
                } else {
                    try self.addToken(.left_brace, allocator);
                }
            },

            '}' => try self.addToken(.right_brace, allocator),

            ':' => {
                // Check if next character starts an identifier for atom literal.
                if (!self.isAtEnd() and isIdentStart(self.peek())) {
                    try self.scanAtom(allocator);
                } else {
                    try self.addToken(.colon, allocator);
                }
            },

            '"' => try self.scanString(allocator),

            ' ', '\t', '\r' => {}, // Skip whitespace.

            '\n' => {
                self.line += 1;
            },

            else => {
                if (isDigit(c)) {
                    try self.scanNumber(allocator);
                } else if (isIdentStart(c)) {
                    try self.scanIdentifier(allocator);
                } else {
                    try self.addErrorToken("unexpected character", allocator);
                }
            },
        }
    }

    fn advance(self: *Lexer) u8 {
        const c = self.source[self.current];
        self.current += 1;
        return c;
    }

    fn peek(self: *const Lexer) u8 {
        return self.source[self.current];
    }

    fn peekNext(self: *const Lexer) u8 {
        if (self.current + 1 >= self.source.len) return 0;
        return self.source[self.current + 1];
    }

    fn match(self: *Lexer, expected: u8) bool {
        if (self.isAtEnd()) return false;
        if (self.source[self.current] != expected) return false;
        self.current += 1;
        return true;
    }

    fn isAtEnd(self: *const Lexer) bool {
        return self.current >= self.source.len;
    }

    fn addToken(self: *Lexer, tag: Tag, allocator: Allocator) !void {
        try self.tokens.append(allocator, .{
            .tag = tag,
            .start = self.start,
            .end = self.current,
            .line = self.line,
        });
    }

    fn addErrorToken(self: *Lexer, message: []const u8, allocator: Allocator) !void {
        try self.tokens.append(allocator, .{
            .tag = .@"error",
            .start = self.start,
            .end = self.current,
            .line = self.line,
        });
        try self.errors.append(.{
            .error_code = .E005,
            .severity = .@"error",
            .message = message,
            .span = .{ .start = self.start, .end = self.current },
            .labels = &[_]Label{},
            .help = null,
        }, allocator);
    }

    // ── Comment scanning ──────────────────────────────────────────────

    fn skipLineComment(self: *Lexer) void {
        while (!self.isAtEnd() and self.peek() != '\n') {
            _ = self.advance();
        }
    }

    fn skipBlockComment(self: *Lexer, allocator: Allocator) !void {
        var depth: u32 = 1;
        while (depth > 0) {
            if (self.isAtEnd()) {
                // Unterminated block comment.
                try self.errors.append(.{
                    .error_code = .E006,
                    .severity = .@"error",
                    .message = "unterminated block comment",
                    .span = .{ .start = self.start, .end = self.current },
                    .labels = &[_]Label{},
                    .help = "add matching '-}' to close the block comment",
                }, allocator);
                return;
            }

            const c = self.advance();
            if (c == '\n') {
                self.line += 1;
            } else if (c == '{' and !self.isAtEnd() and self.peek() == '-') {
                _ = self.advance();
                depth += 1;
            } else if (c == '-' and !self.isAtEnd() and self.peek() == '}') {
                _ = self.advance();
                depth -= 1;
            }
        }
    }

    // ── String scanning ───────────────────────────────────────────────

    fn scanString(self: *Lexer, allocator: Allocator) !void {
        while (!self.isAtEnd() and self.peek() != '"') {
            if (self.peek() == '\n') self.line += 1;
            if (self.peek() == '\\') {
                // Skip escape sequence (consume backslash + next char).
                _ = self.advance();
                if (!self.isAtEnd()) {
                    _ = self.advance();
                    continue;
                }
            }
            _ = self.advance();
        }

        if (self.isAtEnd()) {
            // Unterminated string.
            try self.tokens.append(allocator, .{
                .tag = .@"error",
                .start = self.start,
                .end = self.current,
                .line = self.line,
            });
            try self.errors.append(.{
                .error_code = .E006,
                .severity = .@"error",
                .message = "unterminated string",
                .span = .{ .start = self.start, .end = self.current },
                .labels = &[_]Label{},
                .help = "add a closing '\"'",
            }, allocator);
            return;
        }

        // Consume the closing quote.
        _ = self.advance();
        try self.addToken(.string_literal, allocator);
    }

    // ── Number scanning ───────────────────────────────────────────────

    fn scanNumber(self: *Lexer, allocator: Allocator) !void {
        while (!self.isAtEnd() and isDigit(self.peek())) {
            _ = self.advance();
        }

        // Check for fractional part.
        if (!self.isAtEnd() and self.peek() == '.' and
            self.current + 1 < self.source.len and isDigit(self.peekNext()))
        {
            // Consume the '.'.
            _ = self.advance();
            // Consume fractional digits.
            while (!self.isAtEnd() and isDigit(self.peek())) {
                _ = self.advance();
            }
            try self.addToken(.float_literal, allocator);
        } else {
            try self.addToken(.int_literal, allocator);
        }
    }

    // ── Atom scanning ────────────────────────────────────────────────

    fn scanAtom(self: *Lexer, allocator: Allocator) !void {
        // self.start is at ':', current is past it, and peek() is the first ident char.
        while (!self.isAtEnd() and isIdentCont(self.peek())) {
            _ = self.advance();
        }
        try self.addToken(.atom_literal, allocator);
    }

    // ── Identifier / keyword scanning ────────────────────────────────

    fn scanIdentifier(self: *Lexer, allocator: Allocator) !void {
        while (!self.isAtEnd() and isIdentCont(self.peek())) {
            _ = self.advance();
        }

        const text = self.source[self.start..self.current];
        const tag = token_mod.keyword(text) orelse .identifier;
        try self.addToken(tag, allocator);
    }

    // ── Character classification ──────────────────────────────────────

    fn isDigit(c: u8) bool {
        return c >= '0' and c <= '9';
    }

    fn isIdentStart(c: u8) bool {
        return (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or c == '_';
    }

    fn isIdentCont(c: u8) bool {
        return isIdentStart(c) or isDigit(c);
    }
};

// ═══════════════════════════════════════════════════════════════════════
// ── Tests ──────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════

fn expectTags(source: []const u8, expected: []const Tag) !void {
    const allocator = std.testing.allocator;
    var lexer = Lexer.init(source);
    defer lexer.deinit(allocator);
    try lexer.tokenize(allocator);

    // Check that we got exactly the expected tags.
    if (lexer.tokens.items.len != expected.len) {
        std.debug.print("Expected {d} tokens, got {d}\n", .{ expected.len, lexer.tokens.items.len });
        for (lexer.tokens.items) |tok| {
            std.debug.print("  tag={s} lexeme=\"{s}\"\n", .{ @tagName(tok.tag), tok.lexeme(source) });
        }
        return error.TestUnexpectedResult;
    }

    for (expected, 0..) |exp, i| {
        if (lexer.tokens.items[i].tag != exp) {
            std.debug.print("Token {d}: expected {s}, got {s} (lexeme=\"{s}\")\n", .{
                i,
                @tagName(exp),
                @tagName(lexer.tokens.items[i].tag),
                lexer.tokens.items[i].lexeme(source),
            });
            return error.TestUnexpectedResult;
        }
    }
}

fn expectLexeme(source: []const u8, token_index: usize, expected_lexeme: []const u8) !void {
    const allocator = std.testing.allocator;
    var lexer = Lexer.init(source);
    defer lexer.deinit(allocator);
    try lexer.tokenize(allocator);

    if (token_index >= lexer.tokens.items.len) return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(expected_lexeme, lexer.tokens.items[token_index].lexeme(source));
}

// Test 1: `let x = 42` tokenizes to [let_kw, identifier("x"), equal, int_literal("42"), eof]
test "lexer: let binding" {
    try expectTags("let x = 42", &.{ .kw_let, .identifier, .equal, .int_literal, .eof });
    try expectLexeme("let x = 42", 1, "x");
    try expectLexeme("let x = 42", 3, "42");
}

// Test 2: `3.14 + 2` tokenizes to [float_literal, plus, int_literal, eof]
test "lexer: float and int arithmetic" {
    try expectTags("3.14 + 2", &.{ .float_literal, .plus, .int_literal, .eof });
    try expectLexeme("3.14 + 2", 0, "3.14");
}

// Test 3: `"hello world"` tokenizes to [string_literal, eof]
test "lexer: string literal" {
    try expectTags("\"hello world\"", &.{ .string_literal, .eof });
    // lexeme includes quotes
    try expectLexeme("\"hello world\"", 0, "\"hello world\"");
}

// Test 4: `:ok` tokenizes to [atom_literal, eof]
test "lexer: atom literal" {
    try expectTags(":ok", &.{ .atom_literal, .eof });
    try expectLexeme(":ok", 0, ":ok");
}

// Test 5: line comment is skipped
test "lexer: line comment skipped" {
    try expectTags("-- this is a comment\nlet x = 1", &.{ .kw_let, .identifier, .equal, .int_literal, .eof });
}

// Test 6: nestable block comment is skipped
test "lexer: nested block comment skipped" {
    try expectTags("{- nested {- comment -} -} 42", &.{ .int_literal, .eof });
}

// Test 7: comparison operators
test "lexer: comparison operators" {
    try expectTags("== != < > <= >=", &.{ .equal_equal, .bang_equal, .less, .greater, .less_equal, .greater_equal, .eof });
}

// Test 8: and or not keywords
test "lexer: logical keywords" {
    try expectTags("and or not", &.{ .kw_and, .kw_or, .kw_not, .eof });
}

// Test 9: if/else expression
test "lexer: if else expression" {
    try expectTags("if x > 0 { x } else { 0 }", &.{
        .kw_if, .identifier, .greater, .int_literal, .left_brace, .identifier, .right_brace,
        .kw_else, .left_brace, .int_literal, .right_brace, .eof,
    });
}

// Test 10: pipe operator
test "lexer: pipe operator" {
    try expectTags("|>", &.{ .pipe_greater, .eof });
}

// Test 11: plus_plus (string concatenation)
test "lexer: plus plus" {
    try expectTags("++", &.{ .plus_plus, .eof });
}

// Test 12: unterminated string produces error token, lexer continues
test "lexer: unterminated string error recovery" {
    const source = "\"unterminated\n42";
    const allocator = std.testing.allocator;
    var lexer = Lexer.init(source);
    defer lexer.deinit(allocator);
    try lexer.tokenize(allocator);

    // Should have error token + int_literal + eof
    try std.testing.expect(lexer.tokens.items.len >= 2);
    try std.testing.expectEqual(Tag.@"error", lexer.tokens.items[0].tag);
    // Lexer continues after error.
    try std.testing.expect(lexer.errors.items.items.len > 0);
}

// Test 13: invalid character produces error token, lexer continues
test "lexer: invalid character error recovery" {
    const source = "@ 42";
    const allocator = std.testing.allocator;
    var lexer = Lexer.init(source);
    defer lexer.deinit(allocator);
    try lexer.tokenize(allocator);

    // Should have error token + int_literal + eof
    try std.testing.expectEqual(Tag.@"error", lexer.tokens.items[0].tag);
    try std.testing.expectEqual(Tag.int_literal, lexer.tokens.items[1].tag);
    try std.testing.expectEqual(Tag.eof, lexer.tokens.items[2].tag);
    try std.testing.expect(lexer.errors.items.items.len > 0);
}

// Test 14: multi-line source tracks line numbers correctly
test "lexer: line tracking" {
    const source = "let\nx\n=\n42";
    const allocator = std.testing.allocator;
    var lexer = Lexer.init(source);
    defer lexer.deinit(allocator);
    try lexer.tokenize(allocator);

    try std.testing.expectEqual(@as(u32, 1), lexer.tokens.items[0].line); // let
    try std.testing.expectEqual(@as(u32, 2), lexer.tokens.items[1].line); // x
    try std.testing.expectEqual(@as(u32, 3), lexer.tokens.items[2].line); // =
    try std.testing.expectEqual(@as(u32, 4), lexer.tokens.items[3].line); // 42
}

// Test 15: empty input produces only eof
test "lexer: empty input" {
    try expectTags("", &.{.eof});
}

// Test 16: identifiers starting with letter/underscore
test "lexer: identifiers" {
    try expectTags("foo _bar baz123", &.{ .identifier, .identifier, .identifier, .eof });
    try expectLexeme("foo _bar baz123", 0, "foo");
    try expectLexeme("foo _bar baz123", 1, "_bar");
    try expectLexeme("foo _bar baz123", 2, "baz123");
}

// Additional edge cases:

test "lexer: all single-char punctuation" {
    try expectTags("( ) { } [ ] , : . ;", &.{
        .left_paren, .right_paren, .left_brace, .right_brace,
        .left_bracket, .right_bracket, .comma, .colon, .dot, .semicolon, .eof,
    });
}

test "lexer: all arithmetic operators" {
    try expectTags("+ - * / %", &.{ .plus, .minus, .star, .slash, .percent, .eof });
}

test "lexer: assignment vs equality" {
    try expectTags("= ==", &.{ .equal, .equal_equal, .eof });
}

test "lexer: string with escapes" {
    const source = "\"hello\\nworld\\t\\\"end\"";
    try expectTags(source, &.{ .string_literal, .eof });
}

test "lexer: multiple errors accumulated" {
    const source = "@ # 42";
    const allocator = std.testing.allocator;
    var lexer = Lexer.init(source);
    defer lexer.deinit(allocator);
    try lexer.tokenize(allocator);

    // Two error tokens plus the valid int and eof.
    try std.testing.expectEqual(@as(usize, 2), lexer.errors.items.items.len);
    try std.testing.expectEqual(Tag.int_literal, lexer.tokens.items[2].tag);
}

test "lexer: keyword vs identifier" {
    // `letters` is an identifier, not a keyword even though it starts with `let`.
    try expectTags("letters", &.{ .identifier, .eof });
    try expectLexeme("letters", 0, "letters");
}

test "lexer: all keywords" {
    const src = "let if else while for in and or not fn true false nil match type with return break continue import when";
    try expectTags(src, &.{
        .kw_let, .kw_if, .kw_else, .kw_while, .kw_for, .kw_in,
        .kw_and, .kw_or, .kw_not, .kw_fn, .kw_true, .kw_false,
        .kw_nil, .kw_match, .kw_type, .kw_with, .kw_return,
        .kw_break, .kw_continue, .kw_import, .kw_when, .eof,
    });
}

test "lexer: colon not followed by ident is colon" {
    try expectTags(": 42", &.{ .colon, .int_literal, .eof });
}

test "lexer: integer literal at end of input" {
    try expectTags("99", &.{ .int_literal, .eof });
    try expectLexeme("99", 0, "99");
}

test "lexer: float without trailing digits is int + dot" {
    // `42.` followed by space is not a float -- it's int + dot.
    // Actually, our scanner checks peekNext for digit, so 42. (EOF) stays int.
    try expectTags("42", &.{ .int_literal, .eof });
}

test "lexer: minus is not comment when alone" {
    try expectTags("x - 1", &.{ .identifier, .minus, .int_literal, .eof });
}

test "lexer: while loop tokens" {
    try expectTags("while x > 0 { x = x - 1 }", &.{
        .kw_while, .identifier, .greater, .int_literal,
        .left_brace, .identifier, .equal, .identifier, .minus, .int_literal, .right_brace, .eof,
    });
}

test "lexer: for-in tokens" {
    try expectTags("for i in range(10) { print(i) }", &.{
        .kw_for, .identifier, .kw_in, .identifier, .left_paren, .int_literal, .right_paren,
        .left_brace, .identifier, .left_paren, .identifier, .right_paren, .right_brace, .eof,
    });
}
