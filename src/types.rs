use std::time::{Instant, Duration, SystemTime};
use std::collections::{HashMap, HashSet, BTreeMap, BTreeSet};
use failure::Error;
use nom;

pub type ParseResult<I,O> = nom::IResult<I,O,Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RDB {
    pub version: u32,
    pub aux_codes: HashMap<RedisString, RedisString>,
    pub databases: Vec<Database>,
    pub crc64_checksum: Option<u64>
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Database {
    pub database_number: u64,
    pub database_size: Option<DatabaseSize>,
    pub entries: Vec<KeyValuePair>
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseSize {
    pub db_size: u64,
    pub expire_size: u64
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValuePair {
    pub expiry: Option<Duration>,
    pub key: RedisString,
    pub value: RedisValue
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisValue {
    String(RedisString),
    List(Vec<RedisString>),
    Hash(HashMap<RedisString, RedisString>),
    Set(HashSet<RedisString>),
    SortedSet(BTreeSet<(RedisString, u64)>)
}

impl ::std::fmt::Display for RedisValue {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match self {
            &RedisValue::String(ref vec) => write!(f, "{}", String::from_utf8_lossy(&vec)),
            &RedisValue::List(ref vec) => write!(f, "[{}]", vec.iter()
                .map(|s| String::from_utf8_lossy(&s))
                .collect::<Vec<_>>()
                .join(", ")),
            &RedisValue::Hash(ref hash) => write!(f,"{:?}", hash.iter()
                .map(|(key, value)| format!("({},{})", String::from_utf8_lossy(key), String::from_utf8_lossy(value)))
                .collect::<Vec<_>>()
                .join(", ")),
            &RedisValue::Set(_) => write!(f,"{:?}", self),
            &RedisValue::SortedSet(_) => write!(f,"{:?}", self),
        }
    }
}

pub type RedisString = Vec<u8>;



#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodingType {
    String,
    LinkedList,
    Hashtable,
    Intset(u64),
    Ziplist(u64),
    Zipmap(u64),
    Quicklist,
    Module
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Type {
    String,
    List,
    Set,
    SortedSet,
    Hash,
    Module
}

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodedType {
    STRING = 0,
    LIST = 1,
    SET = 2,
    ZSET = 3,
    HASH = 4,
    ZSET_2 = 5,
    MODULE = 6,
    MODULE_2 = 7,
    HASH_ZIPMAP = 9,
    LIST_ZIPLIST = 10,
    SET_INTSET = 11,
    ZSET_ZIPLIST = 12,
    HASH_ZIPLIST = 13,
    LIST_QUICKLIST = 14
}
impl EncodedType {
    pub fn to_type(&self) -> Type {
        match *self {
            EncodedType::STRING => Type::String,
            EncodedType::LIST | EncodedType::LIST_ZIPLIST | EncodedType::LIST_QUICKLIST=> Type::List,
            EncodedType::SET | EncodedType::SET_INTSET => Type::Set,
            EncodedType::ZSET | EncodedType::ZSET_2 | EncodedType::ZSET_ZIPLIST => Type::SortedSet,
            EncodedType::HASH | EncodedType::HASH_ZIPMAP | EncodedType::HASH_ZIPLIST => Type::Hash,
            EncodedType::MODULE | EncodedType::MODULE_2 => Type::Module
        }
    }
    pub fn from_u8(u8: u8) -> Option<Self> {
        match u8 {
            0 => Some(EncodedType::STRING),
            1 => Some(EncodedType::LIST),
            2 => Some(EncodedType::SET),
            3 => Some(EncodedType::ZSET),
            4 => Some(EncodedType::HASH),
            5 => Some(EncodedType::ZSET_2),
            6 => Some(EncodedType::MODULE),
            7 => Some(EncodedType::MODULE_2),
            9 => Some(EncodedType::HASH_ZIPMAP),
            10 => Some(EncodedType::LIST_ZIPLIST),
            11 => Some(EncodedType::SET_INTSET),
            12 => Some(EncodedType::ZSET_ZIPLIST),
            13 => Some(EncodedType::HASH_ZIPLIST),
            14 => Some(EncodedType::LIST_QUICKLIST),
            _ => None
        }
    }
}

