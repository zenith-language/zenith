const std = @import("std");

/// All Zenith token types for Phase 1.
pub const Tag = enum(u8) {
    // ── Literals ───────────────────────────────────────────────────────
    int_literal,
    float_literal,
    string_literal,
    atom_literal,

    // ── Identifiers ────────────────────────────────────────────────────
    identifier,

    // ── Operators ──────────────────────────────────────────────────────
    plus, // +
    minus, // -
    star, // *
    slash, // /
    percent, // %
    plus_plus, // ++  (string/list concatenation)
    equal, // =
    equal_equal, // ==
    bang_equal, // !=
    less, // <
    greater, // >
    less_equal, // <=
    greater_equal, // >=
    pipe, // | (lambda param delimiter)
    pipe_greater, // |>
    arrow, // ->
    dot_dot, // ..

    // ── Punctuation ────────────────────────────────────────────────────
    left_paren, // (
    right_paren, // )
    left_brace, // {
    right_brace, // }
    left_bracket, // [
    right_bracket, // ]
    comma, // ,
    colon, // :
    dot, // .
    semicolon, // ;

    // ── Keywords ───────────────────────────────────────────────────────
    kw_let,
    kw_if,
    kw_else,
    kw_while,
    kw_for,
    kw_in,
    kw_and,
    kw_or,
    kw_not,
    kw_fn,
    kw_true,
    kw_false,
    kw_nil,
    kw_match,
    kw_type,
    kw_with,
    kw_return,
    kw_break,
    kw_continue,
    kw_import,
    kw_when,

    // ── Comments ───────────────────────────────────────────────────────
    line_comment, // -- ...
    block_comment, // {- ... -}

    // ── Special ────────────────────────────────────────────────────────
    eof,
    @"error",
};

/// A single token produced by the lexer.
pub const Token = struct {
    tag: Tag,
    /// Byte offset of the first character in the source.
    start: u32,
    /// Byte offset one-past the last character in the source.
    end: u32,
    /// 1-based source line number.
    line: u32,

    /// Return the source text slice for this token.
    pub fn lexeme(self: Token, source: []const u8) []const u8 {
        return source[self.start..self.end];
    }
};

/// Keyword lookup table.
/// Given an identifier string, returns the corresponding keyword `Tag`
/// or `null` if the text is not a keyword.
pub fn keyword(text: []const u8) ?Tag {
    const map = std.StaticStringMap(Tag).initComptime(.{
        .{ "let", .kw_let },
        .{ "if", .kw_if },
        .{ "else", .kw_else },
        .{ "while", .kw_while },
        .{ "for", .kw_for },
        .{ "in", .kw_in },
        .{ "and", .kw_and },
        .{ "or", .kw_or },
        .{ "not", .kw_not },
        .{ "fn", .kw_fn },
        .{ "true", .kw_true },
        .{ "false", .kw_false },
        .{ "nil", .kw_nil },
        .{ "match", .kw_match },
        .{ "type", .kw_type },
        .{ "with", .kw_with },
        .{ "return", .kw_return },
        .{ "break", .kw_break },
        .{ "continue", .kw_continue },
        .{ "import", .kw_import },
        .{ "when", .kw_when },
    });
    return map.get(text);
}

// ── Tests ──────────────────────────────────────────────────────────────

test "keyword lookup resolves all keywords" {
    const cases = .{
        .{ "let", Tag.kw_let },
        .{ "if", Tag.kw_if },
        .{ "else", Tag.kw_else },
        .{ "while", Tag.kw_while },
        .{ "for", Tag.kw_for },
        .{ "in", Tag.kw_in },
        .{ "and", Tag.kw_and },
        .{ "or", Tag.kw_or },
        .{ "not", Tag.kw_not },
        .{ "fn", Tag.kw_fn },
        .{ "true", Tag.kw_true },
        .{ "false", Tag.kw_false },
        .{ "nil", Tag.kw_nil },
        .{ "match", Tag.kw_match },
        .{ "type", Tag.kw_type },
        .{ "with", Tag.kw_with },
        .{ "return", Tag.kw_return },
        .{ "break", Tag.kw_break },
        .{ "continue", Tag.kw_continue },
        .{ "import", Tag.kw_import },
        .{ "when", Tag.kw_when },
    };

    inline for (cases) |case| {
        const result = keyword(case[0]);
        try std.testing.expect(result != null);
        try std.testing.expectEqual(case[1], result.?);
    }
}

test "keyword lookup returns null for non-keywords" {
    const non_keywords = [_][]const u8{
        "foo",
        "bar",
        "x",
        "myVar",
        "Let", // case-sensitive
        "IF",
        "TRUE",
        "False",
        "Nil",
        "",
    };

    for (non_keywords) |text| {
        try std.testing.expectEqual(@as(?Tag, null), keyword(text));
    }
}

test "Token struct has expected fields and lexeme works" {
    const source = "let x = 42";
    const tok = Token{
        .tag = .kw_let,
        .start = 0,
        .end = 3,
        .line = 1,
    };
    try std.testing.expectEqualStrings("let", tok.lexeme(source));
    try std.testing.expectEqual(Tag.kw_let, tok.tag);
    try std.testing.expectEqual(@as(u32, 0), tok.start);
    try std.testing.expectEqual(@as(u32, 3), tok.end);
    try std.testing.expectEqual(@as(u32, 1), tok.line);
}

test "Tag enum has all required token types" {
    // Verify the key tokens exist by referencing them.
    // If any is missing, compilation fails.
    const tags = [_]Tag{
        .int_literal,
        .float_literal,
        .string_literal,
        .atom_literal,
        .identifier,
        .plus,
        .minus,
        .star,
        .slash,
        .percent,
        .plus_plus,
        .equal,
        .equal_equal,
        .bang_equal,
        .less,
        .greater,
        .less_equal,
        .greater_equal,
        .pipe,
        .pipe_greater,
        .arrow,
        .dot_dot,
        .left_paren,
        .right_paren,
        .left_brace,
        .right_brace,
        .left_bracket,
        .right_bracket,
        .comma,
        .colon,
        .dot,
        .semicolon,
        .kw_let,
        .kw_if,
        .kw_else,
        .kw_while,
        .kw_for,
        .kw_in,
        .kw_and,
        .kw_or,
        .kw_not,
        .kw_fn,
        .kw_true,
        .kw_false,
        .kw_nil,
        .kw_match,
        .kw_type,
        .kw_with,
        .kw_return,
        .kw_break,
        .kw_continue,
        .kw_import,
        .kw_when,
        .line_comment,
        .block_comment,
        .eof,
        .@"error",
    };
    // All tags are distinct (enum guarantees this), just verify count.
    try std.testing.expect(tags.len > 0);
}
