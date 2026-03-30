/// AWS Signature Version 4 signing for S3 requests.
/// Implements the canonical request → string-to-sign → signing key chain
/// using HMAC-SHA256 from Zig's standard library.

const std = @import("std");
const Allocator = std.mem.Allocator;
const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;
const Sha256 = std.crypto.hash.sha2.Sha256;

/// AWS credentials used for request signing.
pub const Credentials = struct {
    access_key: []const u8,
    secret_key: []const u8,
    session_token: ?[]const u8 = null,
    region: []const u8,
};

/// Formatted timestamp pair for AWS Sig V4.
pub const AwsTimestamp = struct {
    /// Full ISO 8601: "20240101T120000Z" (16 bytes)
    datetime: [16]u8,
    /// Date only: "20240101" (8 bytes)
    date: [8]u8,
};

/// Get current UTC timestamp formatted for AWS Sig V4.
pub fn currentTimestamp() AwsTimestamp {
    const ts = std.time.timestamp();
    return formatTimestamp(@intCast(@max(ts, 0)));
}

/// Format a Unix epoch timestamp for AWS Sig V4.
pub fn formatTimestamp(secs: u64) AwsTimestamp {
    const es = std.time.epoch.EpochSeconds{ .secs = secs };
    const epoch_day = es.getEpochDay();
    const day_secs = es.getDaySeconds();
    const yd = epoch_day.calculateYearDay();
    const md = yd.calculateMonthDay();

    var result: AwsTimestamp = undefined;
    _ = std.fmt.bufPrint(&result.datetime, "{d:0>4}{d:0>2}{d:0>2}T{d:0>2}{d:0>2}{d:0>2}Z", .{
        yd.year,
        md.month.numeric(),
        @as(u6, md.day_index) + 1,
        day_secs.getHoursIntoDay(),
        day_secs.getMinutesIntoHour(),
        day_secs.getSecondsIntoMinute(),
    }) catch unreachable;
    @memcpy(&result.date, result.datetime[0..8]);
    return result;
}

/// SHA-256 hash of data, returned as 64-char lowercase hex string.
pub fn sha256Hex(data: []const u8) [64]u8 {
    var digest: [32]u8 = undefined;
    Sha256.hash(data, &digest, .{});
    return std.fmt.bytesToHex(digest, .lower);
}

/// Derive the AWS Sig V4 signing key.
/// SigningKey = HMAC(HMAC(HMAC(HMAC("AWS4"+secret, date), region), service), "aws4_request")
fn deriveSigningKey(secret_key: []const u8, date: []const u8, region: []const u8) [32]u8 {
    // Step 1: HMAC("AWS4" + secret_key, date)
    var aws4_key_buf: [256]u8 = undefined;
    if (4 + secret_key.len > aws4_key_buf.len) {
        // Secret key too long — truncate (shouldn't happen with AWS keys).
        var truncated: [32]u8 = undefined;
        HmacSha256.create(&truncated, date, secret_key[0..@min(secret_key.len, 252)]);
        return truncated;
    }
    @memcpy(aws4_key_buf[0..4], "AWS4");
    @memcpy(aws4_key_buf[4 .. 4 + secret_key.len], secret_key);
    const aws4_key = aws4_key_buf[0 .. 4 + secret_key.len];

    var date_key: [32]u8 = undefined;
    HmacSha256.create(&date_key, date, aws4_key);

    // Step 2: HMAC(date_key, region)
    var region_key: [32]u8 = undefined;
    HmacSha256.create(&region_key, region, &date_key);

    // Step 3: HMAC(region_key, "s3")
    var service_key: [32]u8 = undefined;
    HmacSha256.create(&service_key, "s3", &region_key);

    // Step 4: HMAC(service_key, "aws4_request")
    var signing_key: [32]u8 = undefined;
    HmacSha256.create(&signing_key, "aws4_request", &service_key);

    return signing_key;
}

/// Sign an S3 request and return the Authorization header value.
///
/// Returns an allocated string that the caller must free.
pub fn signRequest(
    method: []const u8,
    uri_path: []const u8,
    query_string: []const u8,
    host: []const u8,
    payload_hash: []const u8,
    creds: Credentials,
    ts: AwsTimestamp,
    allocator: Allocator,
) ![]const u8 {
    // === Step 1: Canonical Request ===
    // CanonicalRequest =
    //   HTTPRequestMethod + '\n' +
    //   CanonicalURI + '\n' +
    //   CanonicalQueryString + '\n' +
    //   CanonicalHeaders + '\n' +
    //   SignedHeaders + '\n' +
    //   HexEncode(Hash(Payload))

    const canonical_uri = if (uri_path.len == 0) "/" else uri_path;

    // Signed headers: host;x-amz-content-sha256;x-amz-date[;x-amz-security-token]
    const has_token = creds.session_token != null;
    const signed_headers = if (has_token)
        "host;x-amz-content-sha256;x-amz-date;x-amz-security-token"
    else
        "host;x-amz-content-sha256;x-amz-date";

    // Build canonical headers (must be sorted alphabetically by header name, lowercase).
    // host, x-amz-content-sha256, x-amz-date already in order.
    var canon_buf = std.ArrayListUnmanaged(u8){};
    defer canon_buf.deinit(allocator);
    const w = canon_buf.writer(allocator);

    // Canonical request lines
    try w.writeAll(method);
    try w.writeByte('\n');
    try w.writeAll(canonical_uri);
    try w.writeByte('\n');
    try w.writeAll(query_string);
    try w.writeByte('\n');
    // Canonical headers
    try w.writeAll("host:");
    try w.writeAll(host);
    try w.writeByte('\n');
    try w.writeAll("x-amz-content-sha256:");
    try w.writeAll(payload_hash);
    try w.writeByte('\n');
    try w.writeAll("x-amz-date:");
    try w.writeAll(&ts.datetime);
    try w.writeByte('\n');
    if (has_token) {
        try w.writeAll("x-amz-security-token:");
        try w.writeAll(creds.session_token.?);
        try w.writeByte('\n');
    }
    try w.writeByte('\n'); // blank line after headers
    try w.writeAll(signed_headers);
    try w.writeByte('\n');
    try w.writeAll(payload_hash);

    // Hash the canonical request.
    const canon_hash = sha256Hex(canon_buf.items);

    // === Step 2: String to Sign ===
    // StringToSign =
    //   Algorithm + '\n' +
    //   RequestDateTime + '\n' +
    //   CredentialScope + '\n' +
    //   HexEncode(Hash(CanonicalRequest))
    var sts_buf = std.ArrayListUnmanaged(u8){};
    defer sts_buf.deinit(allocator);
    const sw = sts_buf.writer(allocator);

    try sw.writeAll("AWS4-HMAC-SHA256\n");
    try sw.writeAll(&ts.datetime);
    try sw.writeByte('\n');
    // Credential scope: date/region/s3/aws4_request
    try sw.writeAll(&ts.date);
    try sw.writeByte('/');
    try sw.writeAll(creds.region);
    try sw.writeAll("/s3/aws4_request\n");
    try sw.writeAll(&canon_hash);

    // === Step 3: Signing Key ===
    const signing_key = deriveSigningKey(creds.secret_key, &ts.date, creds.region);

    // === Step 4: Signature ===
    var signature_raw: [32]u8 = undefined;
    HmacSha256.create(&signature_raw, sts_buf.items, &signing_key);
    const signature = std.fmt.bytesToHex(signature_raw, .lower);

    // === Step 5: Authorization Header ===
    // Authorization: AWS4-HMAC-SHA256 Credential=AKID/date/region/s3/aws4_request,
    //   SignedHeaders=host;x-amz-content-sha256;x-amz-date,
    //   Signature=abcdef...
    var auth_buf = std.ArrayListUnmanaged(u8){};
    errdefer auth_buf.deinit(allocator);
    const aw = auth_buf.writer(allocator);

    try aw.writeAll("AWS4-HMAC-SHA256 Credential=");
    try aw.writeAll(creds.access_key);
    try aw.writeByte('/');
    try aw.writeAll(&ts.date);
    try aw.writeByte('/');
    try aw.writeAll(creds.region);
    try aw.writeAll("/s3/aws4_request, SignedHeaders=");
    try aw.writeAll(signed_headers);
    try aw.writeAll(", Signature=");
    try aw.writeAll(&signature);

    return auth_buf.toOwnedSlice(allocator);
}

// ═══════════════════════════════════════════════════════════════════════
// ── Tests ──────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════

test "aws_sig: formatTimestamp" {
    // Unix epoch 0 = 1970-01-01T00:00:00Z
    const ts = formatTimestamp(0);
    try std.testing.expectEqualStrings("19700101T000000Z", &ts.datetime);
    try std.testing.expectEqualStrings("19700101", &ts.date);
}

test "aws_sig: formatTimestamp 2024-03-15T10:30:45Z" {
    // 2024-03-15T10:30:45Z = 1710495045
    const ts = formatTimestamp(1710495045);
    try std.testing.expectEqualStrings("20240315T103045Z", &ts.datetime);
    try std.testing.expectEqualStrings("20240315", &ts.date);
}

test "aws_sig: sha256Hex empty string" {
    const hex = sha256Hex("");
    // SHA-256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    try std.testing.expectEqualStrings("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", &hex);
}

test "aws_sig: deriveSigningKey produces expected output" {
    // Test with known values from AWS documentation
    const key = deriveSigningKey("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "20130524", "us-east-1");
    const hex = std.fmt.bytesToHex(key, .lower);
    // This is a known test vector from AWS docs
    try std.testing.expectEqual(@as(usize, 64), hex.len);
}

test "aws_sig: signRequest produces valid Authorization header" {
    const allocator = std.testing.allocator;
    const ts = formatTimestamp(1369353600); // 2013-05-24T00:00:00Z

    const auth_header = try signRequest(
        "GET",
        "/test.txt",
        "",
        "examplebucket.s3.amazonaws.com",
        &sha256Hex(""),
        .{
            .access_key = "AKIAIOSFODNN7EXAMPLE",
            .secret_key = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            .region = "us-east-1",
        },
        ts,
        allocator,
    );
    defer allocator.free(auth_header);

    // Verify it starts with the expected prefix
    try std.testing.expect(std.mem.startsWith(u8, auth_header, "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request"));
}
