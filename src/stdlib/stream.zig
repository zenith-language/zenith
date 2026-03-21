const std = @import("std");
const Allocator = std.mem.Allocator;
const value_mod = @import("value");
const Value = value_mod.Value;
const obj_mod = @import("obj");
const Obj = obj_mod.Obj;
const ObjAdt = obj_mod.ObjAdt;
const ObjList = obj_mod.ObjList;
const ObjRange = obj_mod.ObjRange;
const ObjTuple = obj_mod.ObjTuple;
const ObjStream = obj_mod.ObjStream;
const ObjString = obj_mod.ObjString;
const ObjRecord = obj_mod.ObjRecord;
const json_mod = @import("json");

/// Error type matching builtins.zig NativeError.
pub const NativeError = error{
    RuntimeError,
} || Allocator.Error;

/// Pull-based stream state. Each variant represents a different stream
/// operator (generator, transform, or terminal helper). The `next()`
/// function pulls one element at a time, returning Some(val) or None
/// (as ObjAdt Option type).
pub const StreamState = union(enum) {
    range_iter: RangeIter,
    repeat_iter: RepeatIter,
    iterate_iter: IterateIter,
    map_op: MapOp,
    filter_op: FilterOp,
    take_op: TakeOp,
    drop_op: DropOp,
    flat_map_op: FlatMapOp,
    filter_map_op: FilterMapOp,
    scan_op: ScanOp,
    distinct_op: DistinctOp,
    zip_op: ZipOp,
    flatten_op: FlattenOp,
    tap_op: TapOp,
    batch_op: BatchOp,
    partition_ok: PartitionOp,
    partition_err: PartitionOp,
    file_reader: FileReaderOp,
    jsonl_reader: JsonlReaderOp,
    stdin_reader: StdinReaderOp,

    pub const RangeIter = struct {
        current: i32,
        end: i32,
        step: i32,
    };

    pub const RepeatIter = struct {
        value: Value,
    };

    pub const IterateIter = struct {
        current: Value,
        fn_val: Value,
        started: bool,
    };

    pub const MapOp = struct {
        upstream: Value, // NaN-boxed ObjStream pointer
        fn_val: Value,
    };

    pub const FilterOp = struct {
        upstream: Value, // NaN-boxed ObjStream pointer
        fn_val: Value,
    };

    pub const TakeOp = struct {
        upstream: Value, // NaN-boxed ObjStream pointer
        remaining: i32,
    };

    pub const DropOp = struct {
        upstream: Value, // NaN-boxed ObjStream pointer
        remaining: i32,
        started: bool,
    };

    pub const FlatMapOp = struct {
        upstream: Value, // NaN-boxed ObjStream pointer
        fn_val: Value,
        inner: Value, // NaN-boxed inner ObjStream or nil
    };

    pub const FilterMapOp = struct {
        upstream: Value, // NaN-boxed ObjStream pointer
        fn_val: Value,
    };

    pub const ScanOp = struct {
        upstream: Value, // NaN-boxed ObjStream pointer
        acc: Value,
        fn_val: Value,
    };

    pub const DistinctOp = struct {
        upstream: Value, // NaN-boxed ObjStream pointer
        seen: std.AutoArrayHashMapUnmanaged(u64, void),
    };

    pub const ZipOp = struct {
        upstream_a: Value, // NaN-boxed ObjStream pointer
        upstream_b: Value, // NaN-boxed ObjStream pointer
    };

    pub const FlattenOp = struct {
        upstream: Value, // NaN-boxed ObjStream pointer
        inner_list: Value, // NaN-boxed ObjList or nil
        inner_idx: usize,
        inner_stream: Value, // NaN-boxed inner ObjStream or nil (flatten stream of streams)
    };

    pub const TapOp = struct {
        upstream: Value, // NaN-boxed ObjStream pointer
        fn_val: Value,
    };

    pub const BatchOp = struct {
        upstream: Value, // NaN-boxed ObjStream pointer
        size: i32,
        exhausted: bool,
    };

    pub const PartitionOp = struct {
        shared: *PartitionState,
    };

    /// Shared state for partition_result: both ok and err streams reference this.
    /// ref_count tracks how many streams still hold a reference (starts at 2).
    pub const PartitionState = struct {
        upstream: Value, // NaN-boxed ObjStream pointer
        ok_queue: std.ArrayListUnmanaged(Value),
        err_queue: std.ArrayListUnmanaged(Value),
        ref_count: u8,
    };

    /// Heap-allocated state for file-based readers. Contains the read buffer
    /// and File.Reader so that the reader's internal pointer to the buffer
    /// remains stable (the buffer must outlive the reader).
    pub const FileReaderState = struct {
        file: std.fs.File,
        read_buf: *[READ_BUF_SIZE]u8,
        reader: std.fs.File.Reader,
        done: bool,
        line_buf: std.ArrayListUnmanaged(u8), // accumulator for StreamTooLong
        is_stdin: bool, // if true, do NOT close file on deinit
        line_number: usize, // tracks line count for JSONL error reporting

        pub fn create(allocator: Allocator, file: std.fs.File, is_stdin: bool) !*FileReaderState {
            const buf = try allocator.create([READ_BUF_SIZE]u8);
            const state = try allocator.create(FileReaderState);
            state.* = .{
                .file = file,
                .read_buf = buf,
                .reader = file.reader(buf),
                .done = false,
                .line_buf = .{},
                .is_stdin = is_stdin,
                .line_number = 0,
            };
            return state;
        }

        pub fn deinit(self: *FileReaderState, allocator: Allocator) void {
            self.line_buf.deinit(allocator);
            if (!self.is_stdin) {
                self.file.close();
            }
            allocator.destroy(self.read_buf);
            allocator.destroy(self);
        }

        /// Read one line from the file. Returns the line bytes (owned by line_buf
        /// or by the reader's internal buffer), or null on EOF.
        /// File I/O errors are mapped to RuntimeError.
        pub fn readLine(self: *FileReaderState, allocator: Allocator) NativeError!?[]const u8 {
            if (self.done) return null;

            // Reset accumulator for this line.
            self.line_buf.clearRetainingCapacity();

            while (true) {
                const line_with_delim = self.reader.interface.takeDelimiterInclusive('\n') catch |err| switch (err) {
                    error.EndOfStream => {
                        // Check for remaining data in buffer (last line without trailing newline).
                        const remaining = self.reader.interface.buffer[self.reader.interface.seek..self.reader.interface.end];
                        if (remaining.len > 0 or self.line_buf.items.len > 0) {
                            // Accumulate remaining into line_buf if needed.
                            if (remaining.len > 0) {
                                self.line_buf.appendSlice(allocator, remaining) catch return error.OutOfMemory;
                            }
                            self.done = true;
                            self.line_number += 1;
                            return self.line_buf.items;
                        }
                        self.done = true;
                        return null;
                    },
                    error.StreamTooLong => {
                        // Line exceeds buffer -- accumulate what we have and continue.
                        const partial = self.reader.interface.buffer[self.reader.interface.seek..self.reader.interface.end];
                        self.line_buf.appendSlice(allocator, partial) catch return error.OutOfMemory;
                        // Toss the buffered data so the reader refills.
                        self.reader.interface.toss(@intCast(self.reader.interface.end - self.reader.interface.seek));
                        continue;
                    },
                    else => return error.RuntimeError, // File I/O error
                };

                // Got a line (possibly with trailing \n). Strip it.
                const line = if (line_with_delim.len > 0 and line_with_delim[line_with_delim.len - 1] == '\n')
                    line_with_delim[0 .. line_with_delim.len - 1]
                else
                    line_with_delim;

                // Also strip \r for Windows line endings.
                const clean_line = if (line.len > 0 and line[line.len - 1] == '\r')
                    line[0 .. line.len - 1]
                else
                    line;

                self.line_number += 1;

                // If we had accumulated partial data, append this final part.
                if (self.line_buf.items.len > 0) {
                    try self.line_buf.appendSlice(allocator, clean_line);
                    return self.line_buf.items;
                }
                return clean_line;
            }
        }
    };

    pub const READ_BUF_SIZE = 256 * 1024; // 256KB read buffer

    pub const FileReaderOp = struct {
        frs: *FileReaderState,
    };

    pub const JsonlReaderOp = struct {
        frs: *FileReaderState,
    };

    pub const StdinReaderOp = struct {
        frs: *FileReaderState,
    };

    /// Pull the next element from this stream.
    /// Returns Some(val) or None as ObjAdt Option values.
    pub fn next(self: *StreamState, allocator: Allocator) NativeError!Value {
        switch (self.*) {
            .range_iter => |*s| {
                if (s.step > 0) {
                    if (s.current >= s.end) return makeNone(allocator);
                } else {
                    if (s.current <= s.end) return makeNone(allocator);
                }
                const val = Value.fromInt(s.current);
                s.current += s.step;
                return makeSome(val, allocator);
            },
            .repeat_iter => |s| {
                return makeSome(s.value, allocator);
            },
            .iterate_iter => |*s| {
                if (!s.started) {
                    s.started = true;
                    return makeSome(s.current, allocator);
                }
                // Apply function to current value to get next.
                const result = try callClosure(s.fn_val, &[_]Value{s.current});
                s.current = result;
                return makeSome(result, allocator);
            },
            .map_op => |s| {
                const upstream_stream = ObjStream.fromObj(s.upstream.asObj());
                const upstream_val = try upstream_stream.state.next(allocator);
                // If upstream returned None, pass it through.
                if (isNone(upstream_val)) return upstream_val;
                // Extract the value from Some(val).
                const inner = adtPayload(upstream_val, 0);
                const mapped = try callClosure(s.fn_val, &[_]Value{inner});
                return makeSome(mapped, allocator);
            },
            .filter_op => |s| {
                const upstream_stream = ObjStream.fromObj(s.upstream.asObj());
                // Keep pulling until we find a matching element or upstream is exhausted.
                while (true) {
                    const upstream_val = try upstream_stream.state.next(allocator);
                    if (isNone(upstream_val)) return upstream_val;
                    const inner = adtPayload(upstream_val, 0);
                    const predicate_result = try callClosure(s.fn_val, &[_]Value{inner});
                    if (!isFalsy(predicate_result)) {
                        return makeSome(inner, allocator);
                    }
                }
            },
            .take_op => |*s| {
                if (s.remaining <= 0) return makeNone(allocator);
                const upstream_stream = ObjStream.fromObj(s.upstream.asObj());
                const upstream_val = try upstream_stream.state.next(allocator);
                if (isNone(upstream_val)) return upstream_val;
                s.remaining -= 1;
                return upstream_val;
            },
            .drop_op => |*s| {
                const upstream_stream = ObjStream.fromObj(s.upstream.asObj());
                if (!s.started) {
                    // Discard `remaining` elements.
                    var dropped: i32 = 0;
                    while (dropped < s.remaining) : (dropped += 1) {
                        const upstream_val = try upstream_stream.state.next(allocator);
                        if (isNone(upstream_val)) return upstream_val;
                    }
                    s.started = true;
                }
                return upstream_stream.state.next(allocator);
            },
            .flat_map_op => |*s| {
                while (true) {
                    // If we have an active inner stream, pull from it.
                    if (!s.inner.isNil()) {
                        const inner_stream = ObjStream.fromObj(s.inner.asObj());
                        const inner_val = try inner_stream.state.next(allocator);
                        if (!isNone(inner_val)) return inner_val;
                        // Inner stream exhausted, clear it.
                        s.inner = Value.nil;
                    }
                    // Pull from upstream.
                    const upstream_stream = ObjStream.fromObj(s.upstream.asObj());
                    const upstream_val = try upstream_stream.state.next(allocator);
                    if (isNone(upstream_val)) return upstream_val;
                    const elem = adtPayload(upstream_val, 0);
                    // Apply function to get new inner stream or list.
                    const result = try callClosure(s.fn_val, &[_]Value{elem});
                    if (result.isObjType(.stream)) {
                        s.inner = result;
                    } else if (result.isObjType(.list)) {
                        // Auto-wrap list into a stream (list_iter).
                        const list_obj = ObjList.fromObj(result.asObj());
                        const items = list_obj.items.items;
                        // Create a range_iter-like stream over the list items by
                        // wrapping each element. Use a flatten approach: store
                        // the list as a flatten_op with inner_list.
                        const state = try allocator.create(StreamState);
                        state.* = .{ .flatten_op = .{
                            .upstream = Value.nil, // no upstream; single-list flatten
                            .inner_list = result,
                            .inner_idx = 0,
                            .inner_stream = Value.nil,
                        } };
                        _ = items; // already captured via result
                        const wrapped = try ObjStream.create(allocator, state);
                        trackObj(&wrapped.obj);
                        s.inner = Value.fromObj(&wrapped.obj);
                    } else {
                        // If fn returns a range, auto-wrap to stream.
                        if (result.isObjType(.range)) {
                            const r = ObjRange.fromObj(result.asObj());
                            const state = try allocator.create(StreamState);
                            state.* = .{ .range_iter = .{
                                .current = r.start,
                                .end = r.end,
                                .step = r.step,
                            } };
                            const wrapped = try ObjStream.create(allocator, state);
                            trackObj(&wrapped.obj);
                            s.inner = Value.fromObj(&wrapped.obj);
                        } else {
                            // Not a stream/list/range -- treat as single-element.
                            return makeSome(result, allocator);
                        }
                    }
                }
            },
            .filter_map_op => |s| {
                const upstream_stream = ObjStream.fromObj(s.upstream.asObj());
                while (true) {
                    const upstream_val = try upstream_stream.state.next(allocator);
                    if (isNone(upstream_val)) return upstream_val;
                    const elem = adtPayload(upstream_val, 0);
                    const result = try callClosure(s.fn_val, &[_]Value{elem});
                    // Check if result is Some(v) (type_id=0, variant=0).
                    if (result.isObjType(.adt)) {
                        const adt = ObjAdt.fromObj(result.asObj());
                        if (adt.type_id == 0 and adt.variant_idx == 0) {
                            // Some(v) -- extract and yield.
                            return makeSome(adt.payload[0], allocator);
                        }
                        // None -- skip and continue.
                    }
                    // Non-Option values are skipped (filter_map contract: fn must return Option).
                }
            },
            .scan_op => |*s| {
                const upstream_stream = ObjStream.fromObj(s.upstream.asObj());
                const upstream_val = try upstream_stream.state.next(allocator);
                if (isNone(upstream_val)) return upstream_val;
                const elem = adtPayload(upstream_val, 0);
                const result = try callClosure(s.fn_val, &[_]Value{ s.acc, elem });
                s.acc = result;
                return makeSome(result, allocator);
            },
            .distinct_op => |*s| {
                const upstream_stream = ObjStream.fromObj(s.upstream.asObj());
                while (true) {
                    const upstream_val = try upstream_stream.state.next(allocator);
                    if (isNone(upstream_val)) return upstream_val;
                    const elem = adtPayload(upstream_val, 0);
                    const key = elem.bits;
                    const entry = try s.seen.getOrPut(allocator, key);
                    if (!entry.found_existing) {
                        return makeSome(elem, allocator);
                    }
                    // Duplicate -- skip.
                }
            },
            .zip_op => |s| {
                const stream_a = ObjStream.fromObj(s.upstream_a.asObj());
                const stream_b = ObjStream.fromObj(s.upstream_b.asObj());
                const val_a = try stream_a.state.next(allocator);
                if (isNone(val_a)) return val_a;
                const val_b = try stream_b.state.next(allocator);
                if (isNone(val_b)) return val_b;
                const elem_a = adtPayload(val_a, 0);
                const elem_b = adtPayload(val_b, 0);
                const tuple = try ObjTuple.create(allocator, &[_]Value{ elem_a, elem_b });
                trackObj(&tuple.obj);
                return makeSome(Value.fromObj(&tuple.obj), allocator);
            },
            .flatten_op => |*s| {
                while (true) {
                    // If we have an active inner stream, pull from it.
                    if (!s.inner_stream.isNil()) {
                        const inner_s = ObjStream.fromObj(s.inner_stream.asObj());
                        const inner_val = try inner_s.state.next(allocator);
                        if (!isNone(inner_val)) return inner_val;
                        s.inner_stream = Value.nil;
                    }
                    // If we have an active inner list, yield from it.
                    if (!s.inner_list.isNil()) {
                        const list_obj = ObjList.fromObj(s.inner_list.asObj());
                        if (s.inner_idx < list_obj.items.items.len) {
                            const elem = list_obj.items.items[s.inner_idx];
                            s.inner_idx += 1;
                            return makeSome(elem, allocator);
                        }
                        // Inner list exhausted.
                        s.inner_list = Value.nil;
                        s.inner_idx = 0;
                    }
                    // If no upstream (single-list flatten from flat_map auto-wrap), we are done.
                    if (s.upstream.isNil()) return makeNone(allocator);
                    // Pull from upstream.
                    const upstream_stream = ObjStream.fromObj(s.upstream.asObj());
                    const upstream_val = try upstream_stream.state.next(allocator);
                    if (isNone(upstream_val)) return upstream_val;
                    const elem = adtPayload(upstream_val, 0);
                    if (elem.isObjType(.list)) {
                        s.inner_list = elem;
                        s.inner_idx = 0;
                    } else if (elem.isObjType(.stream)) {
                        s.inner_stream = elem;
                    } else {
                        // Non-list, non-stream element -- yield directly.
                        return makeSome(elem, allocator);
                    }
                }
            },
            .tap_op => |s| {
                const upstream_stream = ObjStream.fromObj(s.upstream.asObj());
                const upstream_val = try upstream_stream.state.next(allocator);
                if (isNone(upstream_val)) return upstream_val;
                const elem = adtPayload(upstream_val, 0);
                // Invoke side-effect function, ignore result.
                _ = try callClosure(s.fn_val, &[_]Value{elem});
                return makeSome(elem, allocator);
            },
            .batch_op => |*s| {
                if (s.exhausted) return makeNone(allocator);
                const upstream_stream = ObjStream.fromObj(s.upstream.asObj());
                const batch_list = try ObjList.create(allocator);
                trackObj(&batch_list.obj);
                var count: i32 = 0;
                while (count < s.size) : (count += 1) {
                    const upstream_val = try upstream_stream.state.next(allocator);
                    if (isNone(upstream_val)) {
                        s.exhausted = true;
                        break;
                    }
                    const elem = adtPayload(upstream_val, 0);
                    try batch_list.items.append(allocator, elem);
                }
                if (batch_list.items.items.len == 0) return makeNone(allocator);
                return makeSome(Value.fromObj(&batch_list.obj), allocator);
            },
            .partition_ok => |s| {
                return partitionNext(s.shared, true, allocator);
            },
            .partition_err => |s| {
                return partitionNext(s.shared, false, allocator);
            },
            .file_reader => |s| {
                return fileReaderNext(s.frs, allocator);
            },
            .stdin_reader => |s| {
                return fileReaderNext(s.frs, allocator);
            },
            .jsonl_reader => |s| {
                return jsonlReaderNext(s.frs, allocator);
            },
        }
    }

    /// Free any owned memory for this stream state.
    pub fn deinit(self: *StreamState, allocator: Allocator) void {
        switch (self.*) {
            .distinct_op => |*s| {
                s.seen.deinit(allocator);
            },
            .partition_ok, .partition_err => |*s| {
                s.shared.ref_count -= 1;
                if (s.shared.ref_count == 0) {
                    s.shared.ok_queue.deinit(allocator);
                    s.shared.err_queue.deinit(allocator);
                    allocator.destroy(s.shared);
                }
            },
            .file_reader => |*s| {
                s.frs.deinit(allocator);
            },
            .jsonl_reader => |*s| {
                s.frs.deinit(allocator);
            },
            .stdin_reader => |*s| {
                s.frs.deinit(allocator);
            },
            else => {},
        }
    }

    /// Trace GC references in this stream state for nursery collection.
    /// All Value fields that might hold object references must be traced.
    pub fn traceGCRefs(self: *StreamState, nursery: anytype, gc: anytype) !void {
        switch (self.*) {
            .range_iter, .file_reader, .jsonl_reader, .stdin_reader => {},
            .repeat_iter => |*s| {
                try nursery.processValue(&s.value, gc);
            },
            .iterate_iter => |*s| {
                try nursery.processValue(&s.current, gc);
                try nursery.processValue(&s.fn_val, gc);
            },
            .map_op => |*s| {
                try nursery.processValue(&s.upstream, gc);
                try nursery.processValue(&s.fn_val, gc);
            },
            .filter_op => |*s| {
                try nursery.processValue(&s.upstream, gc);
                try nursery.processValue(&s.fn_val, gc);
            },
            .take_op => |*s| {
                try nursery.processValue(&s.upstream, gc);
            },
            .drop_op => |*s| {
                try nursery.processValue(&s.upstream, gc);
            },
            .flat_map_op => |*s| {
                try nursery.processValue(&s.upstream, gc);
                try nursery.processValue(&s.fn_val, gc);
                try nursery.processValue(&s.inner, gc);
            },
            .filter_map_op => |*s| {
                try nursery.processValue(&s.upstream, gc);
                try nursery.processValue(&s.fn_val, gc);
            },
            .scan_op => |*s| {
                try nursery.processValue(&s.upstream, gc);
                try nursery.processValue(&s.acc, gc);
                try nursery.processValue(&s.fn_val, gc);
            },
            .distinct_op => |*s| {
                try nursery.processValue(&s.upstream, gc);
            },
            .zip_op => |*s| {
                try nursery.processValue(&s.upstream_a, gc);
                try nursery.processValue(&s.upstream_b, gc);
            },
            .flatten_op => |*s| {
                try nursery.processValue(&s.upstream, gc);
                try nursery.processValue(&s.inner_list, gc);
                try nursery.processValue(&s.inner_stream, gc);
            },
            .tap_op => |*s| {
                try nursery.processValue(&s.upstream, gc);
                try nursery.processValue(&s.fn_val, gc);
            },
            .batch_op => |*s| {
                try nursery.processValue(&s.upstream, gc);
            },
            .partition_ok, .partition_err => |*s| {
                try nursery.processValue(&s.shared.upstream, gc);
                for (s.shared.ok_queue.items) |*v| {
                    try nursery.processValue(v, gc);
                }
                for (s.shared.err_queue.items) |*v| {
                    try nursery.processValue(v, gc);
                }
            },
        }
    }

    /// Trace GC references for old-gen collection.
    pub fn traceGCRefsOldGen(self: *StreamState, oldgen: anytype, gc: anytype) !void {
        switch (self.*) {
            .range_iter, .file_reader, .jsonl_reader, .stdin_reader => {},
            .repeat_iter => |*s| {
                try oldgen.processValue(&s.value, gc);
            },
            .iterate_iter => |*s| {
                try oldgen.processValue(&s.current, gc);
                try oldgen.processValue(&s.fn_val, gc);
            },
            .map_op => |*s| {
                try oldgen.processValue(&s.upstream, gc);
                try oldgen.processValue(&s.fn_val, gc);
            },
            .filter_op => |*s| {
                try oldgen.processValue(&s.upstream, gc);
                try oldgen.processValue(&s.fn_val, gc);
            },
            .take_op => |*s| {
                try oldgen.processValue(&s.upstream, gc);
            },
            .drop_op => |*s| {
                try oldgen.processValue(&s.upstream, gc);
            },
            .flat_map_op => |*s| {
                try oldgen.processValue(&s.upstream, gc);
                try oldgen.processValue(&s.fn_val, gc);
                try oldgen.processValue(&s.inner, gc);
            },
            .filter_map_op => |*s| {
                try oldgen.processValue(&s.upstream, gc);
                try oldgen.processValue(&s.fn_val, gc);
            },
            .scan_op => |*s| {
                try oldgen.processValue(&s.upstream, gc);
                try oldgen.processValue(&s.acc, gc);
                try oldgen.processValue(&s.fn_val, gc);
            },
            .distinct_op => |*s| {
                try oldgen.processValue(&s.upstream, gc);
            },
            .zip_op => |*s| {
                try oldgen.processValue(&s.upstream_a, gc);
                try oldgen.processValue(&s.upstream_b, gc);
            },
            .flatten_op => |*s| {
                try oldgen.processValue(&s.upstream, gc);
                try oldgen.processValue(&s.inner_list, gc);
                try oldgen.processValue(&s.inner_stream, gc);
            },
            .tap_op => |*s| {
                try oldgen.processValue(&s.upstream, gc);
                try oldgen.processValue(&s.fn_val, gc);
            },
            .batch_op => |*s| {
                try oldgen.processValue(&s.upstream, gc);
            },
            .partition_ok, .partition_err => |*s| {
                try oldgen.processValue(&s.shared.upstream, gc);
                for (s.shared.ok_queue.items) |*v| {
                    try oldgen.processValue(v, gc);
                }
                for (s.shared.err_queue.items) |*v| {
                    try oldgen.processValue(v, gc);
                }
            },
        }
    }
};

/// Helper for file_reader and stdin_reader next(): read one line as ObjString.
fn fileReaderNext(frs: *StreamState.FileReaderState, allocator: Allocator) NativeError!Value {
    const line = try frs.readLine(allocator);
    if (line) |line_bytes| {
        const str = try ObjString.create(allocator, line_bytes, null);
        trackObj(&str.obj);
        return makeSome(Value.fromObj(&str.obj), allocator);
    }
    return makeNone(allocator);
}

/// Helper for jsonl_reader next(): read one line, parse as JSON, wrap in Result.
fn jsonlReaderNext(frs: *StreamState.FileReaderState, allocator: Allocator) NativeError!Value {
    while (true) {
        const line = try frs.readLine(allocator);
        if (line) |line_bytes| {
            // Skip empty lines between records.
            if (line_bytes.len == 0) continue;

            // Set JSON module callbacks for tracking.
            if (current_vm) |vm_ptr| {
                if (track_obj_fn) |tfn| {
                    json_mod.setVM(vm_ptr, tfn);
                }
            }
            defer json_mod.clearVM();

            // Parse the line as JSON.
            const parse_result = json_mod.parse(line_bytes, allocator);
            switch (parse_result) {
                .ok => |val| {
                    // Wrap in Result.Ok (type_id=1, variant_idx=0).
                    const ok_adt = try ObjAdt.create(allocator, 1, 0, &[_]Value{val});
                    trackObj(&ok_adt.obj);
                    return makeSome(Value.fromObj(&ok_adt.obj), allocator);
                },
                .err => |e| {
                    // Create ParseError record {message: String, line: Int}.
                    const msg_str = try ObjString.create(allocator, e.message, null);
                    trackObj(&msg_str.obj);
                    const field_names = [_][]const u8{ "message", "line" };
                    const field_values = [_]Value{
                        Value.fromObj(&msg_str.obj),
                        Value.fromInt(@intCast(@min(frs.line_number, @as(usize, @intCast(std.math.maxInt(i32)))))),
                    };
                    const record = try ObjRecord.create(allocator, &field_names, &field_values);
                    trackObj(&record.obj);
                    // Wrap in Result.Err (type_id=1, variant_idx=1).
                    const err_adt = try ObjAdt.create(allocator, 1, 1, &[_]Value{Value.fromObj(&record.obj)});
                    trackObj(&err_adt.obj);
                    return makeSome(Value.fromObj(&err_adt.obj), allocator);
                },
            }
        }
        return makeNone(allocator);
    }
}

/// Shared helper for partition_ok and partition_err next() logic.
/// If `want_ok` is true, returns from the ok_queue; otherwise from err_queue.
fn partitionNext(shared: *StreamState.PartitionState, want_ok: bool, allocator: Allocator) NativeError!Value {
    // Check our queue first.
    const my_queue = if (want_ok) &shared.ok_queue else &shared.err_queue;
    if (my_queue.items.len > 0) {
        const val = my_queue.orderedRemove(0);
        return makeSome(val, allocator);
    }
    // Pull from upstream and classify.
    const other_queue = if (want_ok) &shared.err_queue else &shared.ok_queue;
    const upstream_stream = ObjStream.fromObj(shared.upstream.asObj());
    while (true) {
        const upstream_val = try upstream_stream.state.next(allocator);
        if (isNone(upstream_val)) return upstream_val;
        const elem = adtPayload(upstream_val, 0);
        // Classify: Result type_id=1, Ok=variant 0, Err=variant 1.
        if (elem.isObjType(.adt)) {
            const adt = ObjAdt.fromObj(elem.asObj());
            if (adt.type_id == 1) {
                const payload_val = if (adt.payload.len > 0) adt.payload[0] else Value.nil;
                if (adt.variant_idx == 0) {
                    // Ok variant.
                    if (want_ok) return makeSome(payload_val, allocator);
                    try other_queue.append(allocator, payload_val);
                } else {
                    // Err variant.
                    if (!want_ok) return makeSome(payload_val, allocator);
                    try other_queue.append(allocator, payload_val);
                }
                continue;
            }
        }
        // Non-Result values go to ok queue by default.
        if (want_ok) return makeSome(elem, allocator);
        try other_queue.append(allocator, elem);
    }
}

// ── VM Callback Interface ─────────────────────────────────────────────
// Stream.next() needs to invoke closures (for iterate, map, filter).
// Uses the same callback pattern as builtins.zig -- the VM sets these
// before running any stream terminal.

/// Callback type: invoke a closure Value with given arguments.
pub const CallClosureFn = *const fn (vm_ptr: *anyopaque, closure_val: Value, args: []const Value) ?Value;

/// Callback type: register a heap object with the VM for cleanup.
pub const TrackObjFn = *const fn (vm_ptr: *anyopaque, o: *Obj) void;

/// Module-level callback state (set by builtins.zig before terminal execution).
var current_vm: ?*anyopaque = null;
var call_closure_fn: ?CallClosureFn = null;
var track_obj_fn: ?TrackObjFn = null;

/// Set VM callbacks for stream operations.
pub fn setVM(vm_ptr: *anyopaque, closure_fn: CallClosureFn, track_fn: TrackObjFn) void {
    current_vm = vm_ptr;
    call_closure_fn = closure_fn;
    track_obj_fn = track_fn;
}

/// Clear VM callbacks.
pub fn clearVM() void {
    current_vm = null;
    call_closure_fn = null;
    track_obj_fn = null;
}

/// Track an intermediate heap object with the VM.
fn trackObj(o: *Obj) void {
    if (current_vm) |vm_ptr| {
        if (track_obj_fn) |f| {
            f(vm_ptr, o);
        }
    }
}

/// Internal helper: invoke a closure from within a stream operation.
fn callClosure(closure_val: Value, args: []const Value) NativeError!Value {
    const vm_ptr = current_vm orelse return error.RuntimeError;
    const fn_ptr = call_closure_fn orelse return error.RuntimeError;
    return fn_ptr(vm_ptr, closure_val, args) orelse return error.RuntimeError;
}

// ── ADT Helper Functions ──────────────────────────────────────────────
// Option: type_id=0, Some=variant_idx 0 (arity 1), None=variant_idx 1 (arity 0)

fn makeNone(allocator: Allocator) NativeError!Value {
    const adt = try ObjAdt.create(allocator, 0, 1, &[_]Value{});
    trackObj(&adt.obj);
    return Value.fromObj(&adt.obj);
}

fn makeSome(val: Value, allocator: Allocator) NativeError!Value {
    const adt = try ObjAdt.create(allocator, 0, 0, &[_]Value{val});
    trackObj(&adt.obj);
    return Value.fromObj(&adt.obj);
}

/// Check if a value is None (Option type_id=0, variant_idx=1).
fn isNone(val: Value) bool {
    if (!val.isObjType(.adt)) return false;
    const adt = ObjAdt.fromObj(val.asObj());
    return adt.type_id == 0 and adt.variant_idx == 1;
}

/// Get payload from ADT at given index.
fn adtPayload(val: Value, idx: usize) Value {
    const adt = ObjAdt.fromObj(val.asObj());
    return adt.payload[idx];
}

/// Check if value is falsy (nil or false).
fn isFalsy(val: Value) bool {
    if (val.isNil()) return true;
    if (val.isBool()) return !val.asBool();
    return false;
}
