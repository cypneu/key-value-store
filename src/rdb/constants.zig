pub const DEFAULT_DB_INDEX: u8 = 0;
pub const RDB_VERSION = "REDIS0012";

pub const ValueType = enum(u8) {
    string = 0,
    list_quicklist_2 = 18,
    stream_listpacks_3 = 21,
    _,
};
