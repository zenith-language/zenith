/// URI parser for Zenith source/sink transport dispatch.
/// Parses URI strings into scheme, host, port, and path components.
/// All returned slices point into the original string (zero allocation).

pub const UriScheme = enum {
    file,
    http,
    https,
    s3,
    gs,
    az,
};

pub const ParsedUri = struct {
    scheme: UriScheme,
    host: ?[]const u8, // bucket for s3/gs/az, hostname for http(s)
    port: ?u16, // for http(s) only
    path: []const u8, // object key, file path, or URL path
    raw: []const u8, // original string
};

pub const ParseError = error{UnsupportedScheme};

/// Parse a URI string into its components.
///
/// Supported schemes:
///   file://path, fs://path  →  .file (fs:// is an alias)
///   s3://bucket/key          →  .s3
///   gs://bucket/key          →  .gs
///   az://container/path      →  .az
///   http://host[:port]/path  →  .http
///   https://host[:port]/path →  .https
///
/// Bare paths (./foo, /foo, foo) resolve to .file with path = original.
pub fn parse(raw: []const u8) ParseError!ParsedUri {
    // Check for scheme prefix: look for "://"
    if (indexOfSchemeEnd(raw)) |scheme_end| {
        const scheme_str = raw[0..scheme_end];
        const after_scheme = raw[scheme_end + 3 ..]; // skip "://"

        if (eql(scheme_str, "file") or eql(scheme_str, "fs")) {
            // file:///absolute or file://relative
            // After "file://", the rest is the path.
            // For file:///abs, after_scheme starts with "/abs".
            return .{
                .scheme = .file,
                .host = null,
                .port = null,
                .path = after_scheme,
                .raw = raw,
            };
        }

        if (eql(scheme_str, "s3")) {
            return parseBucketUri(.s3, after_scheme, raw);
        }

        if (eql(scheme_str, "gs")) {
            return parseBucketUri(.gs, after_scheme, raw);
        }

        if (eql(scheme_str, "az")) {
            return parseBucketUri(.az, after_scheme, raw);
        }

        if (eql(scheme_str, "http")) {
            return parseHttpUri(.http, after_scheme, raw);
        }

        if (eql(scheme_str, "https")) {
            return parseHttpUri(.https, after_scheme, raw);
        }

        return error.UnsupportedScheme;
    }

    // No scheme — treat as local file path.
    return .{
        .scheme = .file,
        .host = null,
        .port = null,
        .path = raw,
        .raw = raw,
    };
}

/// Parse bucket-style URI: scheme://bucket/key
fn parseBucketUri(scheme: UriScheme, after_scheme: []const u8, raw: []const u8) ParsedUri {
    // Find first '/' after bucket name.
    if (indexOf(after_scheme, '/')) |slash_pos| {
        return .{
            .scheme = scheme,
            .host = after_scheme[0..slash_pos],
            .port = null,
            .path = after_scheme[slash_pos + 1 ..],
            .raw = raw,
        };
    }
    // No slash — bucket only, empty path.
    return .{
        .scheme = scheme,
        .host = after_scheme,
        .port = null,
        .path = "",
        .raw = raw,
    };
}

/// Parse HTTP(S) URI: scheme://host[:port]/path
fn parseHttpUri(scheme: UriScheme, after_scheme: []const u8, raw: []const u8) ParsedUri {
    // Find end of host[:port] — first '/' after scheme://
    const host_end = indexOf(after_scheme, '/') orelse after_scheme.len;
    const host_port = after_scheme[0..host_end];
    const path = if (host_end < after_scheme.len) after_scheme[host_end..] else "/";

    // Check for port separator.
    if (indexOf(host_port, ':')) |colon_pos| {
        const host = host_port[0..colon_pos];
        const port_str = host_port[colon_pos + 1 ..];
        const port = parsePort(port_str);
        return .{
            .scheme = scheme,
            .host = host,
            .port = port,
            .path = path,
            .raw = raw,
        };
    }

    return .{
        .scheme = scheme,
        .host = host_port,
        .port = null,
        .path = path,
        .raw = raw,
    };
}

/// Find the position of "://" in the string, returning the index of ':'.
fn indexOfSchemeEnd(s: []const u8) ?usize {
    if (s.len < 4) return null; // minimum: "x://"
    var i: usize = 0;
    while (i + 2 < s.len) : (i += 1) {
        if (s[i] == ':' and s[i + 1] == '/' and s[i + 2] == '/') {
            if (i == 0) return null; // "://" with no scheme
            return i;
        }
    }
    return null;
}

/// Find first occurrence of a byte in a slice.
fn indexOf(s: []const u8, needle: u8) ?usize {
    for (s, 0..) |c, i| {
        if (c == needle) return i;
    }
    return null;
}

/// Parse a port number string. Returns null if invalid.
fn parsePort(s: []const u8) ?u16 {
    if (s.len == 0 or s.len > 5) return null;
    var result: u32 = 0;
    for (s) |c| {
        if (c < '0' or c > '9') return null;
        result = result * 10 + (c - '0');
        if (result > 65535) return null;
    }
    return @intCast(result);
}

/// Case-sensitive string equality.
fn eql(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}

const std = @import("std");
