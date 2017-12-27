// Constants taken from https://github.com/antirez/redis/blob/unstable/src/rdb.h
pub mod version {
    pub const SUPPORTED_MINIMUM : u32 = 1;
    pub const SUPPORTED_MAXIMUM : u32 = 8;
}

pub mod constant {
    pub const RDB_6BITLEN : u8 = 0;
    pub const RDB_14BITLEN : u8 = 1;
    pub const RDB_32BITLEN : u8 = 0x80;
    pub const RDB_64BITLEN : u8 = 0x81;
    /// Special encoding, where the next 6 bits determine the format
    pub const RDB_ENCVAL : u8 = 3;
    pub const RDB_MAGIC : &'static [u8] = b"REDIS";
}

pub mod op_code {
    pub const AUX : u8 = 250;
    pub const RESIZEDB : u8 = 251;
    pub const EXPIRETIME_MS : u8 = 252;
    pub const EXPIRETIME : u8 = 253;
    pub const SELECTDB   : u8 = 254;
    pub const EOF : u8 = 255;
}

pub mod encoding {
    pub const INT8 : u32 = 0;
    pub const INT16 : u32 = 1;
    pub const INT32 : u32 = 2;
    pub const LZF : u32 = 3;
}
