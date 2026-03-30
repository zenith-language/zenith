/// Azure Blob Storage Shared Key authorization.
/// Signs requests using HMAC-SHA256 with Azure's canonical format.

const std = @import("std");
const Allocator = std.mem.Allocator;
const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;

/// Sign an Azure Blob Storage request and return the Authorization header value.
/// Returns an allocated string: "SharedKey {account}:{base64(HMAC-SHA256(key, stringToSign))}"
pub fn signRequest(
    method: []const u8,
    content_length: usize,
    content_type: []const u8,
    x_ms_date: []const u8,
    x_ms_version: []const u8,
    canonicalized_resource: []const u8,
    account: []const u8,
    account_key_b64: []const u8,
    allocator: Allocator,
) ![]const u8 {
    // Decode the Base64 account key.
    const key_len = std.base64.standard.Decoder.calcSizeUpperBound(account_key_b64.len) catch
        return error.OutOfMemory;
    const decoded_key = try allocator.alloc(u8, key_len);
    defer allocator.free(decoded_key);
    std.base64.standard.Decoder.decode(decoded_key, account_key_b64) catch
        return error.OutOfMemory;
    const key = decoded_key[0..key_len];

    // Build the string to sign.
    // Format:
    //   VERB\n
    //   Content-Encoding\n  (empty)
    //   Content-Language\n  (empty)
    //   Content-Length\n     (empty for GET, value for PUT)
    //   Content-MD5\n       (empty)
    //   Content-Type\n
    //   Date\n              (empty — we use x-ms-date)
    //   If-Modified-Since\n (empty)
    //   If-Match\n          (empty)
    //   If-None-Match\n     (empty)
    //   If-Unmodified-Since\n (empty)
    //   Range\n             (empty)
    //   CanonicalizedHeaders\n
    //   CanonicalizedResource
    var sts_buf = std.ArrayListUnmanaged(u8){};
    defer sts_buf.deinit(allocator);
    const w = sts_buf.writer(allocator);

    try w.writeAll(method);
    try w.writeAll("\n\n\n"); // Content-Encoding, Content-Language empty
    // Content-Length: empty for GET, value for PUT
    if (content_length > 0) {
        try w.print("{d}", .{content_length});
    }
    try w.writeByte('\n');
    try w.writeAll("\n"); // Content-MD5 empty
    try w.writeAll(content_type);
    try w.writeAll("\n\n\n\n\n\n"); // Date, If-Modified-Since, If-Match, If-None-Match, If-Unmodified-Since, Range — all empty
    // Canonicalized headers (sorted x-ms-* headers)
    try w.writeAll("x-ms-date:");
    try w.writeAll(x_ms_date);
    try w.writeByte('\n');
    try w.writeAll("x-ms-version:");
    try w.writeAll(x_ms_version);
    try w.writeByte('\n');
    // Canonicalized resource
    try w.writeAll(canonicalized_resource);

    // HMAC-SHA256 sign.
    var signature: [32]u8 = undefined;
    HmacSha256.create(&signature, sts_buf.items, key);

    // Base64 encode the signature.
    const b64_len = std.base64.standard.Encoder.calcSize(32);
    var sig_b64: [44]u8 = undefined; // ceil(32/3)*4 = 44
    _ = std.base64.standard.Encoder.encode(&sig_b64, &signature);
    const sig_str = sig_b64[0..b64_len];

    // Build Authorization header: "SharedKey account:signature"
    var auth_buf = std.ArrayListUnmanaged(u8){};
    errdefer auth_buf.deinit(allocator);
    const aw = auth_buf.writer(allocator);
    try aw.writeAll("SharedKey ");
    try aw.writeAll(account);
    try aw.writeByte(':');
    try aw.writeAll(sig_str);

    return auth_buf.toOwnedSlice(allocator);
}

/// Format current UTC time as RFC 1123: "Sun, 01 Jan 2024 12:00:00 GMT"
pub fn formatRfc1123(secs: u64) [29]u8 {
    const es = std.time.epoch.EpochSeconds{ .secs = secs };
    const epoch_day = es.getEpochDay();
    const day_secs = es.getDaySeconds();
    const yd = epoch_day.calculateYearDay();
    const md = yd.calculateMonthDay();

    // Day of week
    const dow = @as(u3, @intCast((epoch_day.day + 4) % 7)); // epoch was Thursday
    const day_names = [7][]const u8{ "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" };
    const month_names = [12][]const u8{ "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };

    var buf: [29]u8 = undefined;
    _ = std.fmt.bufPrint(&buf, "{s}, {d:0>2} {s} {d:0>4} {d:0>2}:{d:0>2}:{d:0>2} GMT", .{
        day_names[dow],
        @as(u6, md.day_index) + 1,
        month_names[md.month.numeric() - 1],
        yd.year,
        day_secs.getHoursIntoDay(),
        day_secs.getMinutesIntoHour(),
        day_secs.getSecondsIntoMinute(),
    }) catch unreachable;
    return buf;
}

pub fn currentRfc1123() [29]u8 {
    const ts = std.time.timestamp();
    return formatRfc1123(@intCast(@max(ts, 0)));
}

// Azure Blob Storage API version
pub const API_VERSION = "2024-11-04";
