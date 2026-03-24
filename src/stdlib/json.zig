/// Hand-rolled JSON parser and emitter for Zenith Values.
///
/// Maps directly between JSON values and Zenith runtime types:
///   JSON object  -> ObjMap(String keys, Value values)
///   JSON array   -> ObjList
///   JSON string  -> ObjString (via Value)
///   JSON number  -> Int (i32 if fits) or Float
///   JSON true/false -> Bool
///   JSON null    -> Nil

const std = @import("std");
const Allocator = std.mem.Allocator;
const value_mod = @import("value");
const Value = value_mod.Value;
const obj_mod = @import("obj");
const Obj = obj_mod.Obj;
const ObjString = obj_mod.ObjString;
const ObjList = obj_mod.ObjList;
const ObjMap = obj_mod.ObjMap;
const ObjTuple = obj_mod.ObjTuple;
const ObjRecord = obj_mod.ObjRecord;
const ObjAdt = obj_mod.ObjAdt;

// ── Track Object Callback ─────────────────────────────────────────────
// JSON parser creates heap objects (ObjString, ObjMap, ObjList) that must
// be tracked by the GC. Uses the same callback pattern as stream.zig.

pub const TrackObjFn = *const fn (vm_ptr: *anyopaque, o: *Obj) void;

var current_vm: ?*anyopaque = null;
var track_obj_fn: ?TrackObjFn = null;
/// Atom names for encoding atoms to JSON strings.
var atom_names_ptr: ?[]const []const u8 = null;

pub fn setVM(vm_ptr: *anyopaque, track_fn: TrackObjFn) void {
    current_vm = vm_ptr;
    track_obj_fn = track_fn;
}

pub fn setAtomNames(names: []const []const u8) void {
    atom_names_ptr = names;
}

pub fn clearVM() void {
    current_vm = null;
    track_obj_fn = null;
    atom_names_ptr = null;
}

fn trackObj(o: *Obj) void {
    if (current_vm) |vm_ptr| {
        if (track_obj_fn) |f| {
            f(vm_ptr, o);
        }
    }
}

// ── Parse Result ──────────────────────────────────────────────────────

pub const ParseErrorInfo = struct {
    message: []const u8,
    position: usize,
    line: usize,
};

pub const ParseResult = union(enum) {
    ok: Value,
    err: ParseErrorInfo,
};

// ── Emit Result ───────────────────────────────────────────────────────

pub const EmitResult = union(enum) {
    ok: []const u8,
    err: []const u8,
};

// ── JSON Parser ───────────────────────────────────────────────────────

const Parser = struct {
    text: []const u8,
    pos: usize,
    allocator: Allocator,
    err_msg: []const u8,
    err_pos: usize,

    fn init(text: []const u8, allocator: Allocator) Parser {
        return .{
            .text = text,
            .pos = 0,
            .allocator = allocator,
            .err_msg = "",
            .err_pos = 0,
        };
    }

    fn setError(self: *Parser, msg: []const u8) void {
        self.err_msg = msg;
        self.err_pos = self.pos;
    }

    fn peek(self: *const Parser) ?u8 {
        if (self.pos < self.text.len) return self.text[self.pos];
        return null;
    }

    fn advance(self: *Parser) ?u8 {
        if (self.pos < self.text.len) {
            const c = self.text[self.pos];
            self.pos += 1;
            return c;
        }
        return null;
    }

    fn skipWhitespace(self: *Parser) void {
        while (self.pos < self.text.len) {
            const c = self.text[self.pos];
            if (c == ' ' or c == '\t' or c == '\n' or c == '\r') {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn expect(self: *Parser, expected: u8) bool {
        if (self.pos < self.text.len and self.text[self.pos] == expected) {
            self.pos += 1;
            return true;
        }
        return false;
    }

    fn computeLine(self: *const Parser) usize {
        var line: usize = 1;
        for (self.text[0..@min(self.err_pos, self.text.len)]) |c| {
            if (c == '\n') line += 1;
        }
        return line;
    }

    // ── Recursive descent parser ──────────────────────────────────────

    fn parseValue(self: *Parser) ?Value {
        self.skipWhitespace();
        const c = self.peek() orelse {
            self.setError("unexpected end of input");
            return null;
        };

        return switch (c) {
            '{' => self.parseObject(),
            '[' => self.parseArray(),
            '"' => self.parseStringValue(),
            't' => self.parseTrue(),
            'f' => self.parseFalse(),
            'n' => self.parseNull(),
            '-', '0'...'9' => self.parseNumber(),
            else => {
                self.setError("unexpected character");
                return null;
            },
        };
    }

    fn parseObject(self: *Parser) ?Value {
        _ = self.advance(); // consume '{'
        self.skipWhitespace();

        const map = ObjMap.create(self.allocator) catch return null;
        trackObj(&map.obj);

        // Empty object.
        if (self.peek() == @as(u8, '}')) {
            _ = self.advance();
            return Value.fromObj(&map.obj);
        }

        while (true) {
            self.skipWhitespace();

            // Parse key (must be a string).
            if (self.peek() != @as(u8, '"')) {
                self.setError("expected string key in object");
                return null;
            }
            const key_val = self.parseStringValue() orelse return null;

            self.skipWhitespace();
            if (!self.expect(':')) {
                self.setError("expected ':' after object key");
                return null;
            }

            // Parse value.
            const val = self.parseValue() orelse return null;

            // Insert into map (key is a Value-wrapped ObjString).
            map.entries.put(self.allocator, key_val, val) catch return null;

            self.skipWhitespace();
            if (self.expect(',')) {
                continue;
            }
            if (self.expect('}')) {
                return Value.fromObj(&map.obj);
            }
            self.setError("expected ',' or '}' in object");
            return null;
        }
    }

    fn parseArray(self: *Parser) ?Value {
        _ = self.advance(); // consume '['
        self.skipWhitespace();

        const list = ObjList.create(self.allocator) catch return null;
        trackObj(&list.obj);

        // Empty array.
        if (self.peek() == @as(u8, ']')) {
            _ = self.advance();
            return Value.fromObj(&list.obj);
        }

        while (true) {
            const val = self.parseValue() orelse return null;
            list.items.append(self.allocator, val) catch return null;

            self.skipWhitespace();
            if (self.expect(',')) {
                continue;
            }
            if (self.expect(']')) {
                return Value.fromObj(&list.obj);
            }
            self.setError("expected ',' or ']' in array");
            return null;
        }
    }

    fn parseStringValue(self: *Parser) ?Value {
        const str_bytes = self.parseStringRaw() orelse return null;
        const str_obj = ObjString.create(self.allocator, str_bytes, null) catch {
            self.allocator.free(str_bytes);
            return null;
        };
        // If interning returned a different string, free our copy.
        if (str_obj.bytes.ptr != str_bytes.ptr) {
            self.allocator.free(str_bytes);
        }
        trackObj(&str_obj.obj);
        return Value.fromObj(&str_obj.obj);
    }

    /// Parse a JSON string and return the unescaped bytes (caller owns).
    fn parseStringRaw(self: *Parser) ?[]const u8 {
        if (!self.expect('"')) {
            self.setError("expected '\"'");
            return null;
        }

        var buf = std.ArrayListUnmanaged(u8){};
        while (self.pos < self.text.len) {
            const c = self.text[self.pos];
            self.pos += 1;

            if (c == '"') {
                // End of string.
                return buf.toOwnedSlice(self.allocator) catch return null;
            }

            if (c == '\\') {
                // Escape sequence.
                const esc = self.advance() orelse {
                    self.setError("unexpected end of string escape");
                    buf.deinit(self.allocator);
                    return null;
                };
                switch (esc) {
                    '"' => buf.append(self.allocator, '"') catch {
                        buf.deinit(self.allocator);
                        return null;
                    },
                    '\\' => buf.append(self.allocator, '\\') catch {
                        buf.deinit(self.allocator);
                        return null;
                    },
                    '/' => buf.append(self.allocator, '/') catch {
                        buf.deinit(self.allocator);
                        return null;
                    },
                    'b' => buf.append(self.allocator, '\x08') catch {
                        buf.deinit(self.allocator);
                        return null;
                    },
                    'f' => buf.append(self.allocator, '\x0C') catch {
                        buf.deinit(self.allocator);
                        return null;
                    },
                    'n' => buf.append(self.allocator, '\n') catch {
                        buf.deinit(self.allocator);
                        return null;
                    },
                    'r' => buf.append(self.allocator, '\r') catch {
                        buf.deinit(self.allocator);
                        return null;
                    },
                    't' => buf.append(self.allocator, '\t') catch {
                        buf.deinit(self.allocator);
                        return null;
                    },
                    'u' => {
                        const cp = self.parseUnicodeEscape() orelse {
                            buf.deinit(self.allocator);
                            return null;
                        };
                        self.writeCodepoint(&buf, cp) catch {
                            buf.deinit(self.allocator);
                            return null;
                        };
                    },
                    else => {
                        self.setError("invalid escape sequence");
                        buf.deinit(self.allocator);
                        return null;
                    },
                }
            } else if (c < 0x20) {
                self.setError("control character in string");
                buf.deinit(self.allocator);
                return null;
            } else {
                buf.append(self.allocator, c) catch {
                    buf.deinit(self.allocator);
                    return null;
                };
            }
        }

        self.setError("unterminated string");
        buf.deinit(self.allocator);
        return null;
    }

    fn parseUnicodeEscape(self: *Parser) ?u21 {
        const high = self.readHex4() orelse return null;

        // Check for surrogate pair.
        if (high >= 0xD800 and high <= 0xDBFF) {
            // High surrogate -- must be followed by \uXXXX low surrogate.
            if (self.pos + 1 < self.text.len and
                self.text[self.pos] == '\\' and
                self.text[self.pos + 1] == 'u')
            {
                self.pos += 2; // skip \u
                const low = self.readHex4() orelse return null;
                if (low >= 0xDC00 and low <= 0xDFFF) {
                    return @intCast((@as(u21, high - 0xD800) << 10) + (low - 0xDC00) + 0x10000);
                }
                self.setError("invalid surrogate pair");
                return null;
            }
            self.setError("expected low surrogate after high surrogate");
            return null;
        }
        if (high >= 0xDC00 and high <= 0xDFFF) {
            self.setError("unexpected low surrogate");
            return null;
        }

        return @intCast(high);
    }

    fn readHex4(self: *Parser) ?u16 {
        if (self.pos + 4 > self.text.len) {
            self.setError("incomplete unicode escape");
            return null;
        }
        const hex = self.text[self.pos .. self.pos + 4];
        self.pos += 4;
        return std.fmt.parseInt(u16, hex, 16) catch {
            self.setError("invalid hex in unicode escape");
            return null;
        };
    }

    fn writeCodepoint(self: *Parser, buf: *std.ArrayListUnmanaged(u8), cp: u21) !void {
        var utf8_buf: [4]u8 = undefined;
        const len = std.unicode.utf8Encode(cp, &utf8_buf) catch return error.OutOfMemory;
        try buf.appendSlice(self.allocator, utf8_buf[0..len]);
    }

    fn parseTrue(self: *Parser) ?Value {
        if (self.pos + 4 <= self.text.len and std.mem.eql(u8, self.text[self.pos .. self.pos + 4], "true")) {
            self.pos += 4;
            return Value.fromBool(true);
        }
        self.setError("invalid literal (expected 'true')");
        return null;
    }

    fn parseFalse(self: *Parser) ?Value {
        if (self.pos + 5 <= self.text.len and std.mem.eql(u8, self.text[self.pos .. self.pos + 5], "false")) {
            self.pos += 5;
            return Value.fromBool(false);
        }
        self.setError("invalid literal (expected 'false')");
        return null;
    }

    fn parseNull(self: *Parser) ?Value {
        if (self.pos + 4 <= self.text.len and std.mem.eql(u8, self.text[self.pos .. self.pos + 4], "null")) {
            self.pos += 4;
            return Value.nil;
        }
        self.setError("invalid literal (expected 'null')");
        return null;
    }

    fn parseNumber(self: *Parser) ?Value {
        const start = self.pos;

        // Optional negative sign.
        if (self.peek() == @as(u8, '-')) self.pos += 1;

        // Integer part.
        if (self.pos >= self.text.len or !isDigit(self.text[self.pos])) {
            self.setError("expected digit");
            return null;
        }

        // Leading zero check.
        if (self.text[self.pos] == '0' and self.pos + 1 < self.text.len and isDigit(self.text[self.pos + 1])) {
            self.setError("leading zeros not allowed");
            return null;
        }

        while (self.pos < self.text.len and isDigit(self.text[self.pos])) {
            self.pos += 1;
        }

        var is_float = false;

        // Fractional part.
        if (self.pos < self.text.len and self.text[self.pos] == '.') {
            is_float = true;
            self.pos += 1;
            if (self.pos >= self.text.len or !isDigit(self.text[self.pos])) {
                self.setError("expected digit after decimal point");
                return null;
            }
            while (self.pos < self.text.len and isDigit(self.text[self.pos])) {
                self.pos += 1;
            }
        }

        // Exponent part.
        if (self.pos < self.text.len and (self.text[self.pos] == 'e' or self.text[self.pos] == 'E')) {
            is_float = true;
            self.pos += 1;
            if (self.pos < self.text.len and (self.text[self.pos] == '+' or self.text[self.pos] == '-')) {
                self.pos += 1;
            }
            if (self.pos >= self.text.len or !isDigit(self.text[self.pos])) {
                self.setError("expected digit in exponent");
                return null;
            }
            while (self.pos < self.text.len and isDigit(self.text[self.pos])) {
                self.pos += 1;
            }
        }

        const num_text = self.text[start..self.pos];

        if (is_float) {
            const f = std.fmt.parseFloat(f64, num_text) catch {
                self.setError("invalid number");
                return null;
            };
            return Value.fromFloat(f);
        }

        // Try integer path.
        if (std.fmt.parseInt(i64, num_text, 10)) |i| {
            if (i >= std.math.minInt(i32) and i <= std.math.maxInt(i32)) {
                return Value.fromInt(@intCast(i));
            }
            // Fits in i64 but not i32 -> store as float (NaN-boxing only supports inline i32).
            return Value.fromFloat(@floatFromInt(i));
        } else |_| {
            // Exceeds i64 range -> parse as float.
            const f = std.fmt.parseFloat(f64, num_text) catch {
                self.setError("invalid number");
                return null;
            };
            return Value.fromFloat(f);
        }
    }

    fn isDigit(c: u8) bool {
        return c >= '0' and c <= '9';
    }
};

// ── Public parse API ──────────────────────────────────────────────────

pub fn parse(text: []const u8, allocator: Allocator) ParseResult {
    var parser = Parser.init(text, allocator);
    const val = parser.parseValue() orelse {
        return .{ .err = .{
            .message = parser.err_msg,
            .position = parser.err_pos,
            .line = parser.computeLine(),
        } };
    };

    // Ensure no trailing content (except whitespace).
    parser.skipWhitespace();
    if (parser.pos < parser.text.len) {
        parser.setError("unexpected content after value");
        return .{ .err = .{
            .message = parser.err_msg,
            .position = parser.err_pos,
            .line = parser.computeLine(),
        } };
    }

    return .{ .ok = val };
}

// ── JSON Emitter ──────────────────────────────────────────────────────

pub fn emit(value: Value, allocator: Allocator) EmitResult {
    var buf = std.ArrayListUnmanaged(u8){};
    if (emitValue(value, allocator, &buf)) {
        return .{ .ok = buf.toOwnedSlice(allocator) catch return .{ .err = "out of memory" } };
    } else |_| {
        // On error, clean up the buffer.
        buf.deinit(allocator);
        return .{ .err = emit_error_msg };
    }
}

/// Module-level error message for emit failures (avoids allocating error strings).
var emit_error_msg: []const u8 = "";

fn emitValue(value: Value, allocator: Allocator, buf: *std.ArrayListUnmanaged(u8)) !void {
    const writer = buf.writer(allocator);

    if (value.isInt()) {
        try writer.print("{d}", .{value.asInt()});
        return;
    }

    if (value.isFloat()) {
        const f = value.asFloat();
        if (std.math.isNan(f) or std.math.isInf(f)) {
            emit_error_msg = "cannot encode NaN or Infinity to JSON";
            return error.OutOfMemory; // signal error
        }
        // Format with sufficient precision. Use a fixed format that
        // always includes decimal if the number is an integer value.
        if (f == @trunc(f) and @abs(f) < 1e15) {
            // Integer-valued float, format without decimals for clean JSON.
            try writer.print("{d:.0}", .{f});
        } else {
            try writer.print("{d}", .{f});
        }
        return;
    }

    if (value.isBool()) {
        try writer.writeAll(if (value.asBool()) "true" else "false");
        return;
    }

    if (value.isNil()) {
        try writer.writeAll("null");
        return;
    }

    if (value.isAtom()) {
        // Atoms encode as JSON strings.
        if (atom_names_ptr) |names| {
            const id = value.asAtom();
            if (id < names.len) {
                try emitJsonString(names[id], allocator, buf);
                return;
            }
        }
        emit_error_msg = "cannot encode unknown atom to JSON";
        return error.OutOfMemory;
    }

    if (value.isObj()) {
        const obj_ptr = value.asObj();
        switch (obj_ptr.obj_type) {
            .string => {
                const str = ObjString.fromObj(obj_ptr);
                try emitJsonString(str.bytes, allocator, buf);
            },
            .list => {
                const list = ObjList.fromObj(obj_ptr);
                try writer.writeByte('[');
                for (list.items.items, 0..) |item, i| {
                    if (i > 0) try writer.writeByte(',');
                    try emitValue(item, allocator, buf);
                }
                try writer.writeByte(']');
            },
            .map => {
                const map = ObjMap.fromObj(obj_ptr);
                try writer.writeByte('{');
                var it = map.entries.iterator();
                var first = true;
                while (it.next()) |entry| {
                    if (!first) try writer.writeByte(',');
                    first = false;

                    // Keys must be strings (or atoms -> string).
                    const key = entry.key_ptr.*;
                    if (key.isObj() and key.asObj().obj_type == .string) {
                        const key_str = ObjString.fromObj(key.asObj());
                        try emitJsonString(key_str.bytes, allocator, buf);
                    } else if (key.isAtom()) {
                        if (atom_names_ptr) |names| {
                            const id = key.asAtom();
                            if (id < names.len) {
                                try emitJsonString(names[id], allocator, buf);
                            } else {
                                emit_error_msg = "cannot encode map with unknown atom key to JSON";
                                return error.OutOfMemory;
                            }
                        } else {
                            emit_error_msg = "cannot encode map with atom key without atom names";
                            return error.OutOfMemory;
                        }
                    } else {
                        emit_error_msg = "cannot encode map with non-string key to JSON";
                        return error.OutOfMemory;
                    }
                    try writer.writeByte(':');
                    try emitValue(entry.value_ptr.*, allocator, buf);
                }
                try writer.writeByte('}');
            },
            .tuple => {
                // Tuples encode as JSON arrays (per CONTEXT.md).
                const t = ObjTuple.fromObj(obj_ptr);
                try writer.writeByte('[');
                for (t.fields, 0..) |field, i| {
                    if (i > 0) try writer.writeByte(',');
                    try emitValue(field, allocator, buf);
                }
                try writer.writeByte(']');
            },
            .record => {
                // Records encode as JSON objects.
                const rec = ObjRecord.fromObj(obj_ptr);
                try writer.writeByte('{');
                for (0..rec.field_count) |i| {
                    if (i > 0) try writer.writeByte(',');
                    try emitJsonString(rec.field_names[i], allocator, buf);
                    try writer.writeByte(':');
                    try emitValue(rec.field_values[i], allocator, buf);
                }
                try writer.writeByte('}');
            },
            .adt => {
                // ADTs encode as JSON objects: {"variant": idx, "payload": [...]}
                const adt = ObjAdt.fromObj(obj_ptr);
                try writer.writeAll("{\"variant\":");
                try writer.print("{d}", .{adt.variant_idx});
                try writer.writeAll(",\"type_id\":");
                try writer.print("{d}", .{adt.type_id});
                if (adt.payload.len > 0) {
                    try writer.writeAll(",\"payload\":[");
                    for (adt.payload, 0..) |p, i| {
                        if (i > 0) try writer.writeByte(',');
                        try emitValue(p, allocator, buf);
                    }
                    try writer.writeByte(']');
                }
                try writer.writeByte('}');
            },
            .int_big => {
                // Big integers stored on heap. Convert i64 to float for JSON.
                const big = obj_mod.ObjInt.fromObj(obj_ptr);
                // Format as integer if in safe integer range for doubles.
                const v = big.value;
                if (v >= -9007199254740992 and v <= 9007199254740992) {
                    try writer.print("{d}", .{v});
                } else {
                    try writer.print("{d}", .{@as(f64, @floatFromInt(v))});
                }
            },
            .function, .closure => {
                emit_error_msg = "cannot encode function to JSON";
                return error.OutOfMemory;
            },
            .stream => {
                emit_error_msg = "cannot encode stream to JSON";
                return error.OutOfMemory;
            },
            .bytes => {
                emit_error_msg = "cannot encode bytes to JSON";
                return error.OutOfMemory;
            },
            .range => {
                // Encode range as an array [start, end, step].
                const r = obj_mod.ObjRange.fromObj(obj_ptr);
                try writer.print("[{d},{d},{d}]", .{ r.start, r.end, r.step });
            },
            .upvalue => {
                emit_error_msg = "cannot encode upvalue to JSON";
                return error.OutOfMemory;
            },
            .fiber => {
                emit_error_msg = "cannot encode fiber to JSON";
                return error.OutOfMemory;
            },
            .channel => {
                emit_error_msg = "cannot encode channel to JSON";
                return error.OutOfMemory;
            },
        }
        return;
    }

    emit_error_msg = "cannot encode unknown value type to JSON";
    return error.OutOfMemory;
}

/// Emit a JSON-escaped string with surrounding quotes.
fn emitJsonString(bytes: []const u8, allocator: Allocator, buf: *std.ArrayListUnmanaged(u8)) !void {
    const writer = buf.writer(allocator);
    try writer.writeByte('"');
    for (bytes) |c| {
        switch (c) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            0x08 => try writer.writeAll("\\b"),
            0x0C => try writer.writeAll("\\f"),
            else => {
                if (c < 0x20) {
                    // Control character -> \u00XX.
                    try writer.print("\\u{x:0>4}", .{@as(u16, c)});
                } else {
                    try writer.writeByte(c);
                }
            },
        }
    }
    try writer.writeByte('"');
}

// ── Test Helpers ──────────────────────────────────────────────────────

/// Recursively destroy a parsed JSON value and all nested objects.
/// In production, the GC handles this. For tests, we need manual cleanup.
fn destroyJsonValue(val: Value, allocator: Allocator) void {
    if (!val.isObj()) return;
    const obj_ptr = val.asObj();
    switch (obj_ptr.obj_type) {
        .map => {
            const map = ObjMap.fromObj(obj_ptr);
            var it = map.entries.iterator();
            while (it.next()) |entry| {
                destroyJsonValue(entry.key_ptr.*, allocator);
                destroyJsonValue(entry.value_ptr.*, allocator);
            }
            map.obj.destroy(allocator);
        },
        .list => {
            const list = ObjList.fromObj(obj_ptr);
            for (list.items.items) |item| {
                destroyJsonValue(item, allocator);
            }
            list.obj.destroy(allocator);
        },
        .string => obj_ptr.destroy(allocator),
        else => obj_ptr.destroy(allocator),
    }
}

// ── Unit Tests ────────────────────────────────────────────────────────

test "parse string" {
    const allocator = std.testing.allocator;
    const result = parse("\"hello\"", allocator);
    switch (result) {
        .ok => |val| {
            defer destroyJsonValue(val, allocator);
            try std.testing.expect(val.isString());
            const str = ObjString.fromObj(val.asObj());
            try std.testing.expectEqualStrings("hello", str.bytes);
        },
        .err => |e| {
            std.debug.print("Parse error: {s} at {d}\n", .{ e.message, e.position });
            return error.TestUnexpectedResult;
        },
    }
}

test "parse integer 42" {
    const allocator = std.testing.allocator;
    const result = parse("42", allocator);
    switch (result) {
        .ok => |val| {
            try std.testing.expect(val.isInt());
            try std.testing.expectEqual(@as(i32, 42), val.asInt());
        },
        .err => return error.TestUnexpectedResult,
    }
}

test "parse float 3.14" {
    const allocator = std.testing.allocator;
    const result = parse("3.14", allocator);
    switch (result) {
        .ok => |val| {
            try std.testing.expect(val.isFloat());
            try std.testing.expectApproxEqAbs(@as(f64, 3.14), val.asFloat(), 0.001);
        },
        .err => return error.TestUnexpectedResult,
    }
}

test "parse true, false, null" {
    const allocator = std.testing.allocator;
    {
        const result = parse("true", allocator);
        try std.testing.expect(result == .ok);
        try std.testing.expect(result.ok.isBool());
        try std.testing.expectEqual(true, result.ok.asBool());
    }
    {
        const result = parse("false", allocator);
        try std.testing.expect(result == .ok);
        try std.testing.expect(result.ok.isBool());
        try std.testing.expectEqual(false, result.ok.asBool());
    }
    {
        const result = parse("null", allocator);
        try std.testing.expect(result == .ok);
        try std.testing.expect(result.ok.isNil());
    }
}

test "parse object" {
    const allocator = std.testing.allocator;
    const result = parse("{\"a\": 1}", allocator);
    switch (result) {
        .ok => |val| {
            defer destroyJsonValue(val, allocator);
            try std.testing.expect(val.isObjType(.map));
            const map = ObjMap.fromObj(val.asObj());
            try std.testing.expectEqual(@as(u32, 1), map.entries.count());
        },
        .err => return error.TestUnexpectedResult,
    }
}

test "parse array" {
    const allocator = std.testing.allocator;
    const result = parse("[1, 2, 3]", allocator);
    switch (result) {
        .ok => |val| {
            defer destroyJsonValue(val, allocator);
            try std.testing.expect(val.isObjType(.list));
            const list = ObjList.fromObj(val.asObj());
            try std.testing.expectEqual(@as(usize, 3), list.items.items.len);
            try std.testing.expectEqual(@as(i32, 1), list.items.items[0].asInt());
            try std.testing.expectEqual(@as(i32, 2), list.items.items[1].asInt());
            try std.testing.expectEqual(@as(i32, 3), list.items.items[2].asInt());
        },
        .err => return error.TestUnexpectedResult,
    }
}

test "parse escape sequences" {
    const allocator = std.testing.allocator;
    const result = parse("\"\\n\\t\\\\\"", allocator);
    switch (result) {
        .ok => |val| {
            defer destroyJsonValue(val, allocator);
            try std.testing.expect(val.isString());
            const str = ObjString.fromObj(val.asObj());
            try std.testing.expectEqualStrings("\n\t\\", str.bytes);
        },
        .err => return error.TestUnexpectedResult,
    }
}

test "parse error: unterminated object" {
    // Use page_allocator: parser creates partial objects on error paths
    // that can't be freed without GC (no trackObj in unit tests).
    const allocator = std.heap.page_allocator;
    const result = parse("{", allocator);
    try std.testing.expect(result == .err);
}

test "parse error: unterminated array" {
    const allocator = std.heap.page_allocator;
    const result = parse("[1,", allocator);
    try std.testing.expect(result == .err);
}

test "parse error: unterminated string" {
    const allocator = std.testing.allocator;
    const result = parse("\"unterminated", allocator);
    try std.testing.expect(result == .err);
}

test "parse negative integer" {
    const allocator = std.testing.allocator;
    const result = parse("-42", allocator);
    switch (result) {
        .ok => |val| {
            try std.testing.expect(val.isInt());
            try std.testing.expectEqual(@as(i32, -42), val.asInt());
        },
        .err => return error.TestUnexpectedResult,
    }
}

test "parse exponent number as float" {
    const allocator = std.testing.allocator;
    const result = parse("1e10", allocator);
    switch (result) {
        .ok => |val| {
            try std.testing.expect(val.isFloat());
            try std.testing.expectApproxEqAbs(@as(f64, 1e10), val.asFloat(), 1.0);
        },
        .err => return error.TestUnexpectedResult,
    }
}

test "parse empty object" {
    const allocator = std.testing.allocator;
    const result = parse("{}", allocator);
    switch (result) {
        .ok => |val| {
            defer destroyJsonValue(val, allocator);
            try std.testing.expect(val.isObjType(.map));
            const map = ObjMap.fromObj(val.asObj());
            try std.testing.expectEqual(@as(u32, 0), map.entries.count());
        },
        .err => return error.TestUnexpectedResult,
    }
}

test "parse empty array" {
    const allocator = std.testing.allocator;
    const result = parse("[]", allocator);
    switch (result) {
        .ok => |val| {
            defer destroyJsonValue(val, allocator);
            try std.testing.expect(val.isObjType(.list));
            const list = ObjList.fromObj(val.asObj());
            try std.testing.expectEqual(@as(usize, 0), list.items.items.len);
        },
        .err => return error.TestUnexpectedResult,
    }
}

test "parse zero" {
    const allocator = std.testing.allocator;
    const result = parse("0", allocator);
    switch (result) {
        .ok => |val| {
            try std.testing.expect(val.isInt());
            try std.testing.expectEqual(@as(i32, 0), val.asInt());
        },
        .err => return error.TestUnexpectedResult,
    }
}

test "parse nested object and array" {
    const allocator = std.testing.allocator;
    const result = parse("{\"nums\": [1, 2], \"flag\": true}", allocator);
    switch (result) {
        .ok => |val| {
            defer destroyJsonValue(val, allocator);
            try std.testing.expect(val.isObjType(.map));
        },
        .err => return error.TestUnexpectedResult,
    }
}

test "emit integer" {
    const allocator = std.testing.allocator;
    const result = emit(Value.fromInt(42), allocator);
    switch (result) {
        .ok => |s| {
            try std.testing.expectEqualStrings("42", s);
            allocator.free(s);
        },
        .err => return error.TestUnexpectedResult,
    }
}

test "emit float" {
    const allocator = std.testing.allocator;
    const result = emit(Value.fromFloat(3.14), allocator);
    switch (result) {
        .ok => |s| {
            // Should contain 3.14.
            try std.testing.expect(std.mem.indexOf(u8, s, "3.14") != null);
            allocator.free(s);
        },
        .err => return error.TestUnexpectedResult,
    }
}

test "emit bool and null" {
    const allocator = std.testing.allocator;
    {
        const result = emit(Value.fromBool(true), allocator);
        try std.testing.expect(result == .ok);
        try std.testing.expectEqualStrings("true", result.ok);
        allocator.free(result.ok);
    }
    {
        const result = emit(Value.fromBool(false), allocator);
        try std.testing.expect(result == .ok);
        try std.testing.expectEqualStrings("false", result.ok);
        allocator.free(result.ok);
    }
    {
        const result = emit(Value.nil, allocator);
        try std.testing.expect(result == .ok);
        try std.testing.expectEqualStrings("null", result.ok);
        allocator.free(result.ok);
    }
}

test "emit string with escapes" {
    const allocator = std.testing.allocator;
    const str_obj = try ObjString.create(allocator, "hello\nworld", null);
    defer str_obj.obj.destroy(allocator);
    const result = emit(Value.fromObj(&str_obj.obj), allocator);
    switch (result) {
        .ok => |s| {
            try std.testing.expectEqualStrings("\"hello\\nworld\"", s);
            allocator.free(s);
        },
        .err => return error.TestUnexpectedResult,
    }
}

test "emit list" {
    const allocator = std.testing.allocator;
    const list = try ObjList.create(allocator);
    defer list.obj.destroy(allocator);
    try list.items.append(allocator, Value.fromInt(1));
    try list.items.append(allocator, Value.fromInt(2));
    const result = emit(Value.fromObj(&list.obj), allocator);
    switch (result) {
        .ok => |s| {
            try std.testing.expectEqualStrings("[1,2]", s);
            allocator.free(s);
        },
        .err => return error.TestUnexpectedResult,
    }
}

test "emit map" {
    const allocator = std.testing.allocator;
    const map = try ObjMap.create(allocator);
    const key_obj = try ObjString.create(allocator, "a", null);
    try map.entries.put(allocator, Value.fromObj(&key_obj.obj), Value.fromInt(1));
    // destroyJsonValue handles both key objects and the map itself.
    defer destroyJsonValue(Value.fromObj(&map.obj), allocator);
    const result = emit(Value.fromObj(&map.obj), allocator);
    switch (result) {
        .ok => |s| {
            try std.testing.expectEqualStrings("{\"a\":1}", s);
            allocator.free(s);
        },
        .err => return error.TestUnexpectedResult,
    }
}

test "round-trip parse-emit integer" {
    const allocator = std.testing.allocator;
    const emit_result = emit(Value.fromInt(42), allocator);
    try std.testing.expect(emit_result == .ok);
    defer allocator.free(emit_result.ok);
    const parse_result = parse(emit_result.ok, allocator);
    switch (parse_result) {
        .ok => |val| {
            try std.testing.expect(val.isInt());
            try std.testing.expectEqual(@as(i32, 42), val.asInt());
        },
        .err => return error.TestUnexpectedResult,
    }
}

test "parse error: invalid literal" {
    const allocator = std.testing.allocator;
    const result = parse("invalid", allocator);
    try std.testing.expect(result == .err);
    try std.testing.expectEqual(@as(usize, 0), result.err.position);
}

test "parse trailing content is error" {
    const allocator = std.testing.allocator;
    const result = parse("42 extra", allocator);
    try std.testing.expect(result == .err);
}
