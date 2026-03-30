/// Credential resolution for cloud transports.
/// Three-tier model:
///   1. Environment variables (zero config)
///   2. Named profile from ~/.aws/credentials
///   3. Explicit credential record from Zenith code
///
/// For S3, resolves to an aws_sig.Credentials struct.

const std = @import("std");
const Allocator = std.mem.Allocator;
const value_mod = @import("value");
const Value = value_mod.Value;
const obj_mod = @import("obj");
const ObjString = obj_mod.ObjString;
const ObjRecord = obj_mod.ObjRecord;

pub const AuthError = error{
    MissingCredentials,
    InvalidAuthValue,
    InvalidProfile,
    ConfigReadError,
};

/// Resolved AWS credentials. All slices point to either env vars or
/// allocated memory (caller-owned via `owned_buf`).
pub const AwsCredentials = struct {
    access_key: []const u8,
    secret_key: []const u8,
    session_token: ?[]const u8 = null,
    region: []const u8,
    /// If non-null, the caller must free this buffer when done.
    owned_buf: ?[]u8 = null,

    pub fn deinit(self: *AwsCredentials, allocator: Allocator) void {
        if (self.owned_buf) |buf| {
            allocator.free(buf);
        }
    }
};

/// Resolve AWS credentials from the three-tier model.
///
/// auth_val interpretation:
///   - nil / not present → environment credential chain
///   - atom → named profile from ~/.aws/credentials
///   - record → explicit credentials {access_key:, secret_key:, region:, ...}
pub fn resolveAwsCredentials(
    auth_val: ?Value,
    default_region: ?[]const u8,
    atom_name_fn: *const fn (Value) ?[]const u8,
    allocator: Allocator,
) AuthError!AwsCredentials {
    if (auth_val) |val| {
        if (val.isNil()) {
            return resolveFromEnv(default_region);
        }
        if (val.isAtom()) {
            // Named profile
            const profile_name = atom_name_fn(val) orelse
                return error.InvalidAuthValue;
            return resolveFromProfile(profile_name, default_region, allocator);
        }
        if (val.isObj()) {
            const obj = val.asObj();
            if (obj.obj_type == .record) {
                return resolveFromRecord(ObjRecord.fromObj(obj), default_region);
            }
        }
        return error.InvalidAuthValue;
    }
    return resolveFromEnv(default_region);
}

/// Tier 1: Resolve credentials from environment variables.
/// Checks: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN,
///         AWS_DEFAULT_REGION / AWS_REGION
fn resolveFromEnv(default_region: ?[]const u8) AuthError!AwsCredentials {
    const access_key = std.posix.getenv("AWS_ACCESS_KEY_ID") orelse
        return error.MissingCredentials;
    const secret_key = std.posix.getenv("AWS_SECRET_ACCESS_KEY") orelse
        return error.MissingCredentials;
    const session_token = std.posix.getenv("AWS_SESSION_TOKEN");
    const region = std.posix.getenv("AWS_DEFAULT_REGION") orelse
        std.posix.getenv("AWS_REGION") orelse
        (default_region orelse "us-east-1");

    return .{
        .access_key = access_key,
        .secret_key = secret_key,
        .session_token = session_token,
        .region = region,
    };
}

/// Tier 2: Resolve credentials from ~/.aws/credentials file using a named profile.
fn resolveFromProfile(
    profile_name: []const u8,
    default_region: ?[]const u8,
    allocator: Allocator,
) AuthError!AwsCredentials {
    // Try AWS_SHARED_CREDENTIALS_FILE env var first, then ~/.aws/credentials
    const creds_path = std.posix.getenv("AWS_SHARED_CREDENTIALS_FILE");

    var path_buf: [512]u8 = undefined;
    const file_path = if (creds_path) |p|
        p
    else blk: {
        const home = std.posix.getenv("HOME") orelse
            return error.ConfigReadError;
        const len = std.fmt.bufPrint(&path_buf, "{s}/.aws/credentials", .{home}) catch
            return error.ConfigReadError;
        break :blk len;
    };

    const content = std.fs.cwd().readFileAlloc(allocator, file_path, 256 * 1024) catch
        return error.ConfigReadError;

    // Parse INI-format credentials file.
    var access_key: ?[]const u8 = null;
    var secret_key: ?[]const u8 = null;
    var session_token: ?[]const u8 = null;
    var region: ?[]const u8 = null;
    var in_target_section = false;

    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, &[_]u8{ ' ', '\t', '\r' });
        if (line.len == 0 or line[0] == '#' or line[0] == ';') continue;

        // Section header: [profile_name]
        if (line[0] == '[' and line[line.len - 1] == ']') {
            const section = std.mem.trim(u8, line[1 .. line.len - 1], &[_]u8{ ' ', '\t' });
            in_target_section = std.mem.eql(u8, section, profile_name);
            continue;
        }

        if (!in_target_section) continue;

        // Key = value
        if (std.mem.indexOfScalar(u8, line, '=')) |eq_pos| {
            const key = std.mem.trim(u8, line[0..eq_pos], &[_]u8{ ' ', '\t' });
            const val = std.mem.trim(u8, line[eq_pos + 1 ..], &[_]u8{ ' ', '\t' });

            if (std.mem.eql(u8, key, "aws_access_key_id")) {
                access_key = val;
            } else if (std.mem.eql(u8, key, "aws_secret_access_key")) {
                secret_key = val;
            } else if (std.mem.eql(u8, key, "aws_session_token")) {
                session_token = val;
            } else if (std.mem.eql(u8, key, "region")) {
                region = val;
            }
        }
    }

    if (access_key == null or secret_key == null) {
        allocator.free(content);
        return error.InvalidProfile;
    }

    // Region fallback: profile → env → default
    const resolved_region = region orelse
        std.posix.getenv("AWS_DEFAULT_REGION") orelse
        std.posix.getenv("AWS_REGION") orelse
        (default_region orelse "us-east-1");

    return .{
        .access_key = access_key.?,
        .secret_key = secret_key.?,
        .session_token = session_token,
        .region = resolved_region,
        .owned_buf = content, // keep content alive for slices
    };
}

/// Tier 3: Resolve credentials from an explicit Zenith record.
/// Expected fields: access_key, secret_key, region, [session_token]
fn resolveFromRecord(rec: *ObjRecord, default_region: ?[]const u8) AuthError!AwsCredentials {
    var access_key: ?[]const u8 = null;
    var secret_key: ?[]const u8 = null;
    var session_token: ?[]const u8 = null;
    var region: ?[]const u8 = null;

    for (0..rec.field_count) |i| {
        const name = rec.field_names[i];
        const val = rec.field_values[i];

        if (!val.isString()) continue;
        const str = ObjString.fromObj(val.asObj()).bytes;

        if (std.mem.eql(u8, name, "access_key")) {
            access_key = str;
        } else if (std.mem.eql(u8, name, "secret_key")) {
            secret_key = str;
        } else if (std.mem.eql(u8, name, "session_token")) {
            session_token = str;
        } else if (std.mem.eql(u8, name, "region")) {
            region = str;
        }
    }

    if (access_key == null or secret_key == null) {
        return error.MissingCredentials;
    }

    return .{
        .access_key = access_key.?,
        .secret_key = secret_key.?,
        .session_token = session_token,
        .region = region orelse
            std.posix.getenv("AWS_DEFAULT_REGION") orelse
            std.posix.getenv("AWS_REGION") orelse
            (default_region orelse "us-east-1"),
    };
}

// ═══════════════════════════════════════════════════════════════════════
// ── GCS Credentials ──────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════

pub const GcsAuthMode = enum { hmac, bearer };

/// Resolved GCS credentials. Supports HMAC (S3-compat) and Bearer token modes.
pub const GcsCredentials = struct {
    mode: GcsAuthMode,
    // HMAC fields (S3-compatible Sig V4)
    access_key: []const u8 = "",
    secret_key: []const u8 = "",
    // Bearer token field
    token: []const u8 = "",
    // Region for HMAC signing (default "auto")
    region: []const u8 = "auto",
    /// If non-null, the caller must free this buffer when done.
    owned_buf: ?[]u8 = null,

    pub fn deinit(self: *GcsCredentials, allocator: Allocator) void {
        if (self.owned_buf) |buf| {
            allocator.free(buf);
        }
    }
};

/// Resolve GCS credentials from the three-tier model.
///
/// auth_val interpretation:
///   - nil / not present → environment credential chain
///   - atom → named profile
///   - record → explicit credentials
///     {access_key:, secret_key:} → HMAC mode
///     {token:} → Bearer mode
pub fn resolveGcsCredentials(
    auth_val: ?Value,
    atom_name_fn: *const fn (Value) ?[]const u8,
    allocator: Allocator,
) AuthError!GcsCredentials {
    if (auth_val) |val| {
        if (val.isNil()) {
            return resolveGcsFromEnv();
        }
        if (val.isAtom()) {
            const profile_name = atom_name_fn(val) orelse
                return error.InvalidAuthValue;
            return resolveGcsFromProfile(profile_name, allocator);
        }
        if (val.isObj()) {
            const obj = val.asObj();
            if (obj.obj_type == .record) {
                return resolveGcsFromRecord(ObjRecord.fromObj(obj));
            }
        }
        return error.InvalidAuthValue;
    }
    return resolveGcsFromEnv();
}

/// Tier 1: Resolve GCS credentials from environment variables.
fn resolveGcsFromEnv() AuthError!GcsCredentials {
    // Try HMAC keys first.
    const hmac_access = std.posix.getenv("GCS_HMAC_ACCESS_KEY");
    const hmac_secret = std.posix.getenv("GCS_HMAC_SECRET_KEY");
    if (hmac_access != null and hmac_secret != null) {
        return .{
            .mode = .hmac,
            .access_key = hmac_access.?,
            .secret_key = hmac_secret.?,
            .region = std.posix.getenv("GCS_REGION") orelse "auto",
        };
    }

    // Try bearer token.
    const token = std.posix.getenv("GOOGLE_BEARER_TOKEN") orelse
        std.posix.getenv("GCS_TOKEN");
    if (token) |t| {
        return .{
            .mode = .bearer,
            .token = t,
        };
    }

    return error.MissingCredentials;
}

/// Tier 2: Resolve GCS credentials from a named profile in ~/.aws/credentials.
fn resolveGcsFromProfile(
    profile_name: []const u8,
    allocator: Allocator,
) AuthError!GcsCredentials {
    // Reuse AWS credentials file — GCS HMAC keys can be stored there too.
    const creds_path = std.posix.getenv("AWS_SHARED_CREDENTIALS_FILE");

    var path_buf: [512]u8 = undefined;
    const file_path = if (creds_path) |p|
        p
    else blk: {
        const home = std.posix.getenv("HOME") orelse
            return error.ConfigReadError;
        const len = std.fmt.bufPrint(&path_buf, "{s}/.aws/credentials", .{home}) catch
            return error.ConfigReadError;
        break :blk len;
    };

    const content = std.fs.cwd().readFileAlloc(allocator, file_path, 256 * 1024) catch
        return error.ConfigReadError;

    var access_key: ?[]const u8 = null;
    var secret_key: ?[]const u8 = null;
    var token: ?[]const u8 = null;
    var region: ?[]const u8 = null;
    var in_target_section = false;

    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, &[_]u8{ ' ', '\t', '\r' });
        if (line.len == 0 or line[0] == '#' or line[0] == ';') continue;

        if (line[0] == '[' and line[line.len - 1] == ']') {
            const section = std.mem.trim(u8, line[1 .. line.len - 1], &[_]u8{ ' ', '\t' });
            in_target_section = std.mem.eql(u8, section, profile_name);
            continue;
        }

        if (!in_target_section) continue;

        if (std.mem.indexOfScalar(u8, line, '=')) |eq_pos| {
            const key = std.mem.trim(u8, line[0..eq_pos], &[_]u8{ ' ', '\t' });
            const val = std.mem.trim(u8, line[eq_pos + 1 ..], &[_]u8{ ' ', '\t' });

            if (std.mem.eql(u8, key, "gcs_hmac_access_key") or std.mem.eql(u8, key, "aws_access_key_id")) {
                access_key = val;
            } else if (std.mem.eql(u8, key, "gcs_hmac_secret_key") or std.mem.eql(u8, key, "aws_secret_access_key")) {
                secret_key = val;
            } else if (std.mem.eql(u8, key, "gcs_token") or std.mem.eql(u8, key, "token")) {
                token = val;
            } else if (std.mem.eql(u8, key, "region")) {
                region = val;
            }
        }
    }

    // Prefer HMAC if both keys found.
    if (access_key != null and secret_key != null) {
        return .{
            .mode = .hmac,
            .access_key = access_key.?,
            .secret_key = secret_key.?,
            .region = region orelse "auto",
            .owned_buf = content,
        };
    }

    // Fall back to bearer token.
    if (token) |t| {
        return .{
            .mode = .bearer,
            .token = t,
            .owned_buf = content,
        };
    }

    allocator.free(content);
    return error.InvalidProfile;
}

/// Tier 3: Resolve GCS credentials from an explicit Zenith record.
fn resolveGcsFromRecord(rec: *ObjRecord) AuthError!GcsCredentials {
    var access_key: ?[]const u8 = null;
    var secret_key: ?[]const u8 = null;
    var token: ?[]const u8 = null;
    var region: ?[]const u8 = null;

    for (0..rec.field_count) |i| {
        const name = rec.field_names[i];
        const val = rec.field_values[i];

        if (!val.isString()) continue;
        const str = ObjString.fromObj(val.asObj()).bytes;

        if (std.mem.eql(u8, name, "access_key")) {
            access_key = str;
        } else if (std.mem.eql(u8, name, "secret_key")) {
            secret_key = str;
        } else if (std.mem.eql(u8, name, "token")) {
            token = str;
        } else if (std.mem.eql(u8, name, "region")) {
            region = str;
        }
    }

    if (access_key != null and secret_key != null) {
        return .{
            .mode = .hmac,
            .access_key = access_key.?,
            .secret_key = secret_key.?,
            .region = region orelse "auto",
        };
    }

    if (token) |t| {
        return .{ .mode = .bearer, .token = t };
    }

    return error.MissingCredentials;
}

// ═══════════════════════════════════════════════════════════════════════
// ── Azure Blob Credentials ──────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════

/// Resolved Azure Blob Storage credentials (Shared Key).
pub const AzureCredentials = struct {
    account: []const u8,
    account_key: []const u8, // Base64-encoded account key
    owned_buf: ?[]u8 = null,

    pub fn deinit(self: *AzureCredentials, allocator: Allocator) void {
        if (self.owned_buf) |buf| allocator.free(buf);
    }
};

/// Resolve Azure Blob Storage credentials.
pub fn resolveAzureCredentials(
    auth_val: ?Value,
    atom_name_fn: *const fn (Value) ?[]const u8,
    allocator: Allocator,
) AuthError!AzureCredentials {
    if (auth_val) |val| {
        if (val.isNil()) return resolveAzureFromEnv();
        if (val.isAtom()) {
            const profile_name = atom_name_fn(val) orelse return error.InvalidAuthValue;
            return resolveAzureFromProfile(profile_name, allocator);
        }
        if (val.isObj()) {
            const obj = val.asObj();
            if (obj.obj_type == .record) return resolveAzureFromRecord(ObjRecord.fromObj(obj));
        }
        return error.InvalidAuthValue;
    }
    return resolveAzureFromEnv();
}

fn resolveAzureFromEnv() AuthError!AzureCredentials {
    const account = std.posix.getenv("AZURE_STORAGE_ACCOUNT") orelse return error.MissingCredentials;
    const key = std.posix.getenv("AZURE_STORAGE_KEY") orelse return error.MissingCredentials;
    return .{ .account = account, .account_key = key };
}

fn resolveAzureFromProfile(profile_name: []const u8, allocator: Allocator) AuthError!AzureCredentials {
    const creds_path = std.posix.getenv("AWS_SHARED_CREDENTIALS_FILE");
    var path_buf: [512]u8 = undefined;
    const file_path = if (creds_path) |p| p else blk: {
        const home = std.posix.getenv("HOME") orelse return error.ConfigReadError;
        break :blk std.fmt.bufPrint(&path_buf, "{s}/.aws/credentials", .{home}) catch return error.ConfigReadError;
    };
    const content = std.fs.cwd().readFileAlloc(allocator, file_path, 256 * 1024) catch return error.ConfigReadError;

    var account: ?[]const u8 = null;
    var account_key: ?[]const u8 = null;
    var in_target = false;

    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, &[_]u8{ ' ', '\t', '\r' });
        if (line.len == 0 or line[0] == '#' or line[0] == ';') continue;
        if (line[0] == '[' and line[line.len - 1] == ']') {
            in_target = std.mem.eql(u8, std.mem.trim(u8, line[1 .. line.len - 1], &[_]u8{ ' ', '\t' }), profile_name);
            continue;
        }
        if (!in_target) continue;
        if (std.mem.indexOfScalar(u8, line, '=')) |eq| {
            const k = std.mem.trim(u8, line[0..eq], &[_]u8{ ' ', '\t' });
            const v = std.mem.trim(u8, line[eq + 1 ..], &[_]u8{ ' ', '\t' });
            if (std.mem.eql(u8, k, "azure_storage_account")) account = v
            else if (std.mem.eql(u8, k, "azure_storage_key")) account_key = v;
        }
    }

    if (account == null or account_key == null) {
        allocator.free(content);
        return error.InvalidProfile;
    }
    return .{ .account = account.?, .account_key = account_key.?, .owned_buf = content };
}

fn resolveAzureFromRecord(rec: *ObjRecord) AuthError!AzureCredentials {
    var account: ?[]const u8 = null;
    var account_key: ?[]const u8 = null;

    for (0..rec.field_count) |i| {
        const val = rec.field_values[i];
        if (!val.isString()) continue;
        const str = ObjString.fromObj(val.asObj()).bytes;
        if (std.mem.eql(u8, rec.field_names[i], "account")) account = str
        else if (std.mem.eql(u8, rec.field_names[i], "account_key")) account_key = str;
    }

    if (account == null or account_key == null) return error.MissingCredentials;
    return .{ .account = account.?, .account_key = account_key.? };
}
