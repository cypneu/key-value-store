const std = @import("std");
const db = @import("../data_structures/mod.zig");

pub const EntryId = std.meta.FieldType(db.StreamEntry, .id);

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
