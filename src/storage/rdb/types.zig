const std = @import("std");
const stream_store = @import("../stream.zig");

pub const EntryId = std.meta.FieldType(stream_store.StreamEntry, .id);

pub const RdbError = error{
    InvalidHeader,
    InvalidChecksum,
    UnsupportedEncoding,
    UnexpectedEOF,
    InvalidLengthEncoding,
    InvalidListpackEncoding,
    ExpectedInteger,
    LzfOutputTooSmall,
    LzfInputTruncated,
    LzfInvalidReference,
    LzfSizeMismatch,
};
