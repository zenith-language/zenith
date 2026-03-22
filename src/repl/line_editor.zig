/// Raw terminal line editor with history, cursor movement, and escape
/// sequence handling for interactive REPL input.
const std = @import("std");
const posix = std.posix;
const Allocator = std.mem.Allocator;

pub const LineEditor = struct {
    orig_termios: posix.termios,
    history: std.ArrayListUnmanaged([]const u8),
    history_index: usize,
    line_buf: std.ArrayListUnmanaged(u8),
    cursor_pos: usize,
    allocator: Allocator,
    /// Saved line buffer content when navigating history.
    saved_line: ?[]const u8,

    pub fn init(allocator: Allocator) !LineEditor {
        const orig = try enableRawMode();
        return .{
            .orig_termios = orig,
            .history = .empty,
            .history_index = 0,
            .line_buf = .empty,
            .cursor_pos = 0,
            .allocator = allocator,
            .saved_line = null,
        };
    }

    pub fn deinit(self: *LineEditor) void {
        disableRawMode(self.orig_termios);
        for (self.history.items) |h| {
            self.allocator.free(h);
        }
        self.history.deinit(self.allocator);
        self.line_buf.deinit(self.allocator);
        if (self.saved_line) |s| self.allocator.free(s);
    }

    /// Read one line of input with line editing support.
    /// Returns the line content, or null on EOF (Ctrl+D on empty line).
    /// Caller must free the returned slice.
    pub fn readLine(self: *LineEditor, prompt: []const u8) !?[]const u8 {
        // Reset state for new line.
        self.line_buf.clearRetainingCapacity();
        self.cursor_pos = 0;
        self.history_index = self.history.items.len;
        if (self.saved_line) |s| {
            self.allocator.free(s);
            self.saved_line = null;
        }

        // Write prompt.
        const stdout = std.fs.File.stdout();
        stdout.writeAll(prompt) catch {};

        while (true) {
            const byte = readByte() orelse return null; // EOF

            switch (byte) {
                0x0D => { // Enter
                    stdout.writeAll("\r\n") catch {};
                    const line = try self.allocator.dupe(u8, self.line_buf.items);
                    // Add to history if non-empty and different from last entry.
                    if (line.len > 0) {
                        const dominated = if (self.history.items.len > 0)
                            std.mem.eql(u8, self.history.items[self.history.items.len - 1], line)
                        else
                            false;
                        if (!dominated) {
                            try self.history.append(self.allocator, try self.allocator.dupe(u8, line));
                        }
                    }
                    return line;
                },
                0x7F, 0x08 => { // Backspace
                    if (self.cursor_pos > 0) {
                        _ = self.line_buf.orderedRemove(self.cursor_pos - 1);
                        self.cursor_pos -= 1;
                        self.refreshLine(prompt);
                    }
                },
                0x03 => { // Ctrl+C -- cancel current line
                    stdout.writeAll("^C\r\n") catch {};
                    self.line_buf.clearRetainingCapacity();
                    self.cursor_pos = 0;
                    // Re-write prompt for new line.
                    stdout.writeAll(prompt) catch {};
                },
                0x04 => { // Ctrl+D
                    if (self.line_buf.items.len == 0) {
                        stdout.writeAll("\r\n") catch {};
                        return null; // EOF
                    }
                    // Non-empty line: ignore
                },
                0x01 => { // Ctrl+A -- home
                    self.cursor_pos = 0;
                    self.refreshLine(prompt);
                },
                0x05 => { // Ctrl+E -- end
                    self.cursor_pos = self.line_buf.items.len;
                    self.refreshLine(prompt);
                },
                0x0C => { // Ctrl+L -- clear screen
                    stdout.writeAll("\x1b[2J\x1b[H") catch {};
                    self.refreshLine(prompt);
                },
                0x1B => { // Escape sequence
                    self.handleEscape(prompt);
                },
                else => {
                    // Printable ASCII
                    if (byte >= 0x20 and byte <= 0x7E) {
                        self.line_buf.insert(self.allocator, self.cursor_pos, byte) catch {};
                        self.cursor_pos += 1;
                        self.refreshLine(prompt);
                    }
                    // Ignore other control characters and non-ASCII for now.
                },
            }
        }
    }

    /// Handle escape sequences (arrow keys, etc.)
    fn handleEscape(self: *LineEditor, prompt: []const u8) void {
        // Read next byte with timeout. If nothing comes, it was a bare escape.
        const next = readByteTimeout() orelse return;
        if (next != '[') return;

        const code = readByteTimeout() orelse return;
        switch (code) {
            'A' => self.historyUp(prompt), // Up arrow
            'B' => self.historyDown(prompt), // Down arrow
            'C' => { // Right arrow
                if (self.cursor_pos < self.line_buf.items.len) {
                    self.cursor_pos += 1;
                    self.refreshLine(prompt);
                }
            },
            'D' => { // Left arrow
                if (self.cursor_pos > 0) {
                    self.cursor_pos -= 1;
                    self.refreshLine(prompt);
                }
            },
            'H' => { // Home
                self.cursor_pos = 0;
                self.refreshLine(prompt);
            },
            'F' => { // End
                self.cursor_pos = self.line_buf.items.len;
                self.refreshLine(prompt);
            },
            '3' => { // Delete key: ESC [ 3 ~
                const tilde = readByteTimeout() orelse return;
                if (tilde == '~' and self.cursor_pos < self.line_buf.items.len) {
                    _ = self.line_buf.orderedRemove(self.cursor_pos);
                    self.refreshLine(prompt);
                }
            },
            else => {}, // Unknown sequence, ignore
        }
    }

    /// Navigate history backward (older entries).
    fn historyUp(self: *LineEditor, prompt: []const u8) void {
        if (self.history.items.len == 0) return;
        if (self.history_index == 0) return;

        // Save current line when first navigating away.
        if (self.history_index == self.history.items.len) {
            if (self.saved_line) |s| self.allocator.free(s);
            self.saved_line = self.allocator.dupe(u8, self.line_buf.items) catch return;
        }

        self.history_index -= 1;
        self.line_buf.clearRetainingCapacity();
        self.line_buf.appendSlice(self.allocator, self.history.items[self.history_index]) catch return;
        self.cursor_pos = self.line_buf.items.len;
        self.refreshLine(prompt);
    }

    /// Navigate history forward (newer entries).
    fn historyDown(self: *LineEditor, prompt: []const u8) void {
        if (self.history_index >= self.history.items.len) return;

        self.history_index += 1;
        self.line_buf.clearRetainingCapacity();

        if (self.history_index == self.history.items.len) {
            // Restore saved line.
            if (self.saved_line) |s| {
                self.line_buf.appendSlice(self.allocator, s) catch {};
                self.allocator.free(s);
                self.saved_line = null;
            }
        } else {
            self.line_buf.appendSlice(self.allocator, self.history.items[self.history_index]) catch {};
        }
        self.cursor_pos = self.line_buf.items.len;
        self.refreshLine(prompt);
    }

    /// Refresh the current line display.
    fn refreshLine(self: *LineEditor, prompt: []const u8) void {
        const stdout = std.fs.File.stdout();
        var buf: [256]u8 = undefined;

        // Move cursor to start of line and clear it.
        stdout.writeAll("\r") catch {};
        stdout.writeAll(prompt) catch {};
        stdout.writeAll(self.line_buf.items) catch {};
        // Clear to end of line.
        stdout.writeAll("\x1b[K") catch {};

        // Position cursor.
        const cursor_col = prompt.len + self.cursor_pos;
        if (cursor_col == 0) {
            stdout.writeAll("\r") catch {};
        } else {
            const seq = std.fmt.bufPrint(&buf, "\r\x1b[{d}C", .{cursor_col}) catch return;
            stdout.writeAll(seq) catch {};
        }
    }

    /// Read a single byte from stdin, blocking.
    fn readByte() ?u8 {
        var buf: [1]u8 = undefined;
        const n = posix.read(posix.STDIN_FILENO, &buf) catch return null;
        if (n == 0) return null;
        return buf[0];
    }

    /// Read a single byte with a short timeout for escape sequence detection.
    /// Returns null if no byte arrives within ~100ms.
    fn readByteTimeout() ?u8 {
        const stdin_fd = posix.STDIN_FILENO;

        // Save current termios, set timeout mode.
        var tio = posix.tcgetattr(stdin_fd) catch return null;
        const saved_cc_min = tio.cc[@intFromEnum(posix.V.MIN)];
        const saved_cc_time = tio.cc[@intFromEnum(posix.V.TIME)];
        tio.cc[@intFromEnum(posix.V.MIN)] = 0;
        tio.cc[@intFromEnum(posix.V.TIME)] = 1; // 100ms timeout
        posix.tcsetattr(stdin_fd, .NOW, tio) catch return null;

        defer {
            // Restore blocking mode.
            tio.cc[@intFromEnum(posix.V.MIN)] = saved_cc_min;
            tio.cc[@intFromEnum(posix.V.TIME)] = saved_cc_time;
            posix.tcsetattr(stdin_fd, .NOW, tio) catch {};
        }

        var buf: [1]u8 = undefined;
        const n = posix.read(stdin_fd, &buf) catch return null;
        if (n == 0) return null;
        return buf[0];
    }
};

/// Enable raw terminal mode. Returns the original termios for restoration.
fn enableRawMode() !posix.termios {
    const fd = posix.STDIN_FILENO;
    const orig = try posix.tcgetattr(fd);
    var raw = orig;

    // Input flags: disable CR-to-NL, flow control.
    raw.iflag.ICRNL = false;
    raw.iflag.IXON = false;

    // Output flags: disable output processing.
    raw.oflag.OPOST = false;

    // Local flags: disable echo, canonical mode, signals, extended processing.
    raw.lflag.ECHO = false;
    raw.lflag.ICANON = false;
    raw.lflag.IEXTEN = false;
    raw.lflag.ISIG = false;

    // Control chars: read returns after 1 byte, no timeout.
    raw.cc[@intFromEnum(posix.V.MIN)] = 1;
    raw.cc[@intFromEnum(posix.V.TIME)] = 0;

    try posix.tcsetattr(fd, .FLUSH, raw);
    return orig;
}

/// Restore original terminal mode.
fn disableRawMode(orig: posix.termios) void {
    posix.tcsetattr(posix.STDIN_FILENO, .FLUSH, orig) catch {};
}
