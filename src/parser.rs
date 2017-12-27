use super::types::*;
use super::constants::{constant, op_code, encoding};
use failure::Error;
use std::str::FromStr;
use std::collections::{HashMap, HashSet, BTreeSet};
use nom::{IResult, HexDisplay,
          be_u64, be_u32, be_u24, be_u16, be_u8,
          be_i64, be_i32, be_i24, be_i16, be_i8,
          le_u64, le_u32, le_u24, le_u16, le_u8,
          le_i64, le_i32, le_i24, le_i16, le_i8
};
use byteorder::{BigEndian, LittleEndian, ReadBytesExt, WriteBytesExt};
use std::time::Duration;
use std::iter::FromIterator;
use lzf;

/* RDB related parsers */
named!(rdb<RDB>,
    do_parse!(
        version: rdb_header_version >>
        aux_codes: rdb_auxes >>
        databases: redis_databases >>
        crc64_checksum: rdb_ending_checksum >>
        (RDB {
            version,
            aux_codes,
            databases,//: Vec::new(),
            crc64_checksum//: None
        })
));
named!(rdb_header_version<u32>, preceded!(tag!(constant::RDB_MAGIC), rdb_version));
named!(rdb_version<u32>, map_res!(take_str!(4), u32::from_str));
named!(rdb_auxes<HashMap<RedisString, RedisString>>, map!(many0!(rdb_aux), |v| v.into_iter().collect::<HashMap<_,_>>()));
named!(rdb_aux<(RedisString, RedisString)>, preceded!(tag!(&[op_code::AUX]), do_parse!(
    key: read_string >>
    value: read_string >>
    (key, value)
)));
named!(redis_databases<Vec<Database>>, many0!(redis_database));
named!(rdb_ending_checksum<Option<u64>>, do_parse!(
    tag!(&[op_code::EOF]) >>
    checksum: opt!(complete!(be_u64)) >>
    eof!() >>
    (checksum)
));


/* Database related parsers */
named!(redis_database<Database>,
    do_parse!(
        database_number: database_header_number >>
        database_size: database_size >>
        entries: key_value_pairs >>
        ({
            println!("Parsed database-{}, size: {:?}, entries-len: {}", database_number, database_size, entries.len());
            Database {
                database_number,
                database_size,
                entries
            }
        })
));
named!(database_header_number<u64>, preceded!(tag!(&[op_code::SELECTDB]), read_length));
named!(database_size<Option<DatabaseSize>>, opt!(preceded!(tag!(&[op_code::RESIZEDB]), do_parse!(
    db_size: read_length >>
    expire_size: read_length >>
    (DatabaseSize {
        db_size, expire_size
    })
))));

// If we encounter a SELECTDB or an EOF, it means we've finished with our current DB so we
// shouldn't parse the upcoming input as another key-value pair
named!(should_continue_parsing_db_entries, not!(alt!(
    tag!(&[op_code::SELECTDB]) | tag!(&[op_code::EOF])
)));


/* Key value related parsers */
named!(key_value_pairs<Vec<KeyValuePair>>, many0!(key_value_pair));

named!(key_value_pair<KeyValuePair>, do_parse!(
    should_continue_parsing_db_entries >>
    expiry: expiry >>
    value_type: take!(1) >>
    key: read_string >>
    value: call!(read_value, value_type[0]) >>
    ({
        println!("Parsed kvp, key: {}, value_type: {:?}, value: {}, expiry: {:?}",
            String::from_utf8_lossy(&key), EncodedType::from_u8(value_type[0]), value, expiry);
        KeyValuePair {
            expiry,
            key,
            value
        }
    })
));

named!(expiry_s<Duration>, preceded!(
    tag!(&[op_code::EXPIRETIME]), // read_unsigned_int
    map!(le_u32, |s| Duration::from_secs(s as u64))));
named!(expiry_ms<Duration>, preceded!(
    tag!(&[op_code::EXPIRETIME_MS]), // read_unsigned_long
    map!(le_u64, |ms| Duration::from_millis(ms))));

named!(expiry<Option<Duration>>, opt!(alt!(
    expiry_s | expiry_ms
)));


/* Value parsers */
fn read_value(input: &[u8], value_type: u8) -> IResult<&[u8], RedisValue> {
    let encoded_type = EncodedType::from_u8(value_type).unwrap_or_else(|| panic!("unknown value type '{}'", value_type));
    let redis_type = encoded_type.to_type();
    println!("  read_value called with encoded type {:?}, type {:?}", encoded_type, redis_type);
    let value = match encoded_type {
        EncodedType::STRING => map!(input, read_string, RedisValue::String),
        EncodedType::LIST => map!(input, read_list, RedisValue::List),
        EncodedType::SET => map!(input, read_set, RedisValue::Set),
        EncodedType::ZSET => unimplemented!("ZSET"),
        EncodedType::HASH => map!(input, read_hash, RedisValue::Hash),
        EncodedType::ZSET_2 => unimplemented!("ZSET_2"),
        EncodedType::MODULE => unimplemented!("MODULE"),
        EncodedType::MODULE_2 => unimplemented!("MODULE_2"),
        EncodedType::HASH_ZIPMAP => unimplemented!("HASH_ZIPMAP"),
        EncodedType::LIST_ZIPLIST => map!(input, read_ziplist, RedisValue::List),
        EncodedType::SET_INTSET => unimplemented!("SET_INTSET"),
        EncodedType::ZSET_ZIPLIST => unimplemented!("ZSET_ZIPLIST"),
        EncodedType::HASH_ZIPLIST => map!(input, read_hash_ziplist, RedisValue::Hash),
        EncodedType::LIST_QUICKLIST => unimplemented!("LIST_QUICKLIST"),
    };
    value
}

fn read_list(input: &[u8]) -> IResult<&[u8], Vec<RedisString>> {
    let (input, size) = try_parse!(input, read_length);
    let (input, strings) = try_parse!(input, many_m_n!(size as usize, size as usize, read_string));
    IResult::Done(input, strings)
}

fn read_set(input: &[u8]) -> IResult<&[u8], HashSet<RedisString>> {
    let (input, strings) = try_parse!(input, read_list);
    IResult::Done(input, HashSet::from_iter(strings.into_iter()))
}

fn read_hash(input: &[u8]) -> IResult<&[u8], HashMap<RedisString, RedisString>> {
    let (input, entries_size) = try_parse!(input, read_length);
    println!("making a hash with {} elements", entries_size);
    let mut hash = HashMap::with_capacity(entries_size as usize);
    let mut input_iter = input;
    for _i in 0..entries_size {
        let (input, key) = try_parse!(input_iter, read_string);
        let (input, value) = try_parse!(input, read_string);
        hash.insert(key, value);
        input_iter = input;
    }
    IResult::Done(input_iter, hash)
}


fn read_ziplist(input: &[u8]) -> IResult<&[u8], Vec<RedisString>> {
    let (input, ziplist_buf) = try_parse!(input, read_string);
    let res = read_ziplist_string(&ziplist_buf);
    match res {
        IResult::Done(_, entries) => IResult::Done(input, entries),
        IResult::Incomplete(needed) => IResult::Incomplete(needed),
        IResult::Error(_e) => IResult::Error(unimplemented!("todo error"))
    }
}

fn read_hash_ziplist(input: &[u8]) -> IResult<&[u8], HashMap<RedisString, RedisString>> {
    let (input, entries) = try_parse!(input, read_ziplist);
    let hash = entries.chunks(2).map(|chunk| (chunk[0].clone(), chunk[1].clone())).collect::<HashMap<_,_>>();
    IResult::Done(input, hash)
}


fn read_ziplist_string(ziplist: &[u8]) -> IResult<&[u8], Vec<RedisString>> {
    let mut entries = Vec::new();
    let (ziplist, _zlbytes)= try_parse!(ziplist, le_u32);
    let (ziplist, _zltail) = try_parse!(ziplist, le_u32);
    let (ziplist, zllen) = try_parse!(ziplist, le_u16);
    println!("beginning parse of ziplist of len {}: '{}'", zllen, String::from_utf8_lossy(ziplist));
    let mut ziplist_iter = ziplist;
    for i in 0..zllen {
        let (ziplist, prev_len) = try_parse!(ziplist_iter, be_u8);
        let (ziplist, prev_len) = if prev_len < 254 {(ziplist, prev_len as u32)} else {try_parse!(ziplist, be_u32)};
        println!("  parsing entry no {}, prev-len: {}, flag-byte: {:08b}", i, prev_len, ziplist[0]);
        let (ziplist, entry) = try_parse!(ziplist, alt!(
            special_flag_6bit_len_string | special_flag_14bit_len_string | special_flag_4byte_len_string |
            special_flag_64bit | special_flag_32bit | special_flag_24bit | special_flag_16bit | special_flag_8bit |
            special_flag_4bit
        ));
        println!("    found ziplist entry: {}", String::from_utf8_lossy(&entry));
        entries.push(entry);
        ziplist_iter = ziplist;
    }
    let (ziplist, _tag) = try_parse!(ziplist_iter, tag!(&[255u8])); // end of ziplist

    IResult::Done(ziplist, entries)
}

fn special_flag_6bit_len_string(input: &[u8]) -> IResult<&[u8], RedisString> {
    let (input, len) = try_parse!(input, bits!(do_parse!(
        tag_bits!(u8, 2, 0b00) >>
        len: take_bits!(u8, 6) >>
        (len)
    )));
    println!("    special flag indicates 6bit-len string, len is {} = {:08b}", len, len);
    let (input, st) = try_parse!(input, take!(len));
    IResult::Done(input, st.to_vec())
}

fn special_flag_14bit_len_string(input: &[u8]) -> IResult<&[u8], RedisString> {
    let (input, len) = try_parse!(input, bits!(do_parse!(
        tag_bits!(u8, 2, 0b01) >>
        len: take_bits!(u16, 14) >>
        (len)
    )));
    println!("    special flag indicates 14bit-len string, len is {} = {:08b}", len, len);
    let (input, st) = try_parse!(input, take!(len));
    IResult::Done(input, st.to_vec())
}

named!(special_flag_4byte_len_string<RedisString>, do_parse!(
    bits!(tag_bits!(u8, 2, 0b10)) >>
    len: be_u32 >>
    st: take!(len) >>
    (st.to_vec())
));


fn i64_to_bytes(i: i64) -> Vec<u8> {
    let mut vec = Vec::new();
    vec.write_i64::<LittleEndian>(i).unwrap();
    vec
}

fn i32_to_bytes(i: i32) -> Vec<u8> {
    let mut vec = Vec::new();
    vec.write_i32::<LittleEndian>(i).unwrap();
    vec
}

fn i24_to_bytes(i: i32) -> Vec<u8> {
    let mut vec = Vec::new();
    vec.write_i24::<LittleEndian>(i).unwrap();
    vec
}

fn i16_to_bytes(i: i16) -> Vec<u8> {
    let mut vec = Vec::new();
    vec.write_i16::<LittleEndian>(i).unwrap();
    vec
}

fn i8_to_bytes(i: i8) -> Vec<u8> {
    vec![i as u8]
}

named!(special_flag_64bit<RedisString>, map!(preceded!(bits!(tag_bits!(u8, 4, 0b1110)), le_i64), i64_to_bytes));
named!(special_flag_32bit<RedisString>, map!(preceded!(bits!(tag_bits!(u8, 4, 0b1101)), le_i32), i32_to_bytes));
named!(special_flag_16bit<RedisString>, map!(preceded!(bits!(tag_bits!(u8, 4, 0b1100)), le_i16), i16_to_bytes));
named!(special_flag_24bit<RedisString>, map!(preceded!(bits!(tag_bits!(u8, 8, 0b11110000)), le_i24), i24_to_bytes));
named!(special_flag_8bit<RedisString>, map!(preceded!(bits!(tag_bits!(u8, 8, 0b11111110)), le_i8), i8_to_bytes));
// make sure to put this after the 24bit and 8bit parsers, otherwise it'll always match
named!(special_flag_4bit<RedisString>, map!(verify!(bits!(do_parse!(
    tag_bits!(u8, 4, 0b1111) >>
    val: take_bits!(u8, 4) >>
    (val as i8 - 1)
)),|val| val > 0b0000 && val <= 0b1101), i8_to_bytes));

#[test]
fn test_6bit_len_string() {
    let data = [0b0000011u8, 19, 37, 160, 182];
    assert_eq!(special_flag_6bit_len_string(&data[..]), IResult::Done(&data[4..], vec![19, 37, 160]));
}
#[test]
fn test_14bit_len_string() {
    let data = vec![0b01000000, 0b00000010, 27, 32, 125];
    assert_eq!(special_flag_14bit_len_string(&data[..]), IResult::Done(&data[4..], vec![27, 32]));

}

#[test]
fn test_4byte_len_string() {
    let mut data = vec![];
    data.write_u8(0b10000000).unwrap();
    data.write_u32::<BigEndian>(3).unwrap();
    data.extend_from_slice(&[10,20,30, 77]);
    assert_eq!(special_flag_4byte_len_string(&data[..]), IResult::Done(&data[data.len()-1..], vec![10, 20, 30]));
}

/* More general parsers */

/// Parses a redis read_length, tupled with whether it indicates a special format(if so,
/// the "read_length" actually identifies the format)
fn read_length_with_encoding(input: &[u8]) -> IResult<&[u8], (u64, bool)> {
    let (after_bytes,byte0) = try_parse!(input,take!(1));
    let (_, (enc_type, six_bits)) = try_parse!(byte0, bits!(pair!(take_bits!(u8, 2),
        take_bits!(u8, 6))));
    match enc_type {
        constant::RDB_6BITLEN => {
            IResult::Done(after_bytes, (six_bits as u64, false))
        },
        constant::RDB_ENCVAL => {
            IResult::Done(after_bytes, (six_bits as u64, true))
        },
        constant::RDB_14BITLEN => {
            let (after_bytes, byte1) = try_parse!(after_bytes, take!(1));
            let mut size_buf = &[six_bits, byte1[0]][..];
            let size = size_buf.read_u16::<BigEndian>().unwrap();
            IResult::Done(after_bytes, (size as u64, false))
        },
        _ if byte0[0] == constant::RDB_32BITLEN => {
            be_u32(after_bytes).map(|u|(u as u64, false))
        },
        _ if byte0[0] == constant::RDB_64BITLEN => {
            be_u64(after_bytes).map(|u| (u, false))
        },
        _ => panic!("while reading length, unknown enc_type '{}'", enc_type)
    }
}


/// Parses a redis read_length
fn read_length(input: &[u8]) -> IResult<&[u8], u64> {
    map!(input, read_length_with_encoding, |t| {/*println!("  read length {}", t.0);*/ t.0})
}

/// Parses a redis string
fn read_string(input: &[u8]) -> IResult<&[u8], RedisString> {
    let (input, (len, custom_fmt)) = try_parse!(input, read_length_with_encoding);
    match len as u32 {
        _ if !custom_fmt =>  map!(input, take!(len), |slice| slice.to_owned()),
        encoding::INT8   =>  map!(input, le_i8, |u| u.to_string().into_bytes()),
        encoding::INT16  =>  map!(input, le_i16, |u| u.to_string().into_bytes()),
        encoding::INT32  =>  map!(input, le_i32, |u| u.to_string().into_bytes()),
        encoding::LZF    =>  do_parse!(input,
            comp_len: read_length >>
            full_len: read_length >>
            comp_bytes: take!(comp_len) >>
            (lzf::decompress(comp_bytes, full_len as usize).unwrap())
        ),
        _ => panic!("while reading string, unsupported enc_fmt '{}'", len)
    }
}

#[test]
fn can_decode_rdb_version() {
    let data = b"REDIS0008MORE";
    println!("input: {}", data.to_hex(16));
    assert_eq!(rdb_header_version(data), IResult::Done(&b"MORE"[..], 8));
}

#[test]
fn can_decode_database_header() {
    let data = &[0xFE, 0b00101101, 0x4B];
    let data_2 = &[0xFE, 0, 0x4B];
    assert_eq!(database_header_number(data), IResult::Done(&data[2..], 0b00101101));
    assert_eq!(database_header_number(data_2), IResult::Done(&data_2[2..], 0));
}

#[test]
fn can_decode_database_ending() {
    let data = &[0xFF];
    let data_2 = &[0xFF, 0x0A, 0x01, 0x0B, 0x02, 0x0C, 0x03, 0x0D, 0x04];
    let illegal = &[0xFF, 0x4C];
    let illegal_2 = &[0xFF, 0x0A, 0x01, 0x0B, 0x02, 0x0C, 0x03, 0x0D, 0x04, 0x64];
    assert_eq!(rdb_ending_checksum(data), IResult::Done(&b""[..], None));
    assert_eq!(rdb_ending_checksum(data_2), IResult::Done(&b""[..], Some(0x0A010B020C030D04)));
    assert!(rdb_ending_checksum(illegal).is_err());
    assert!(rdb_ending_checksum(illegal_2).is_err());
}

#[test]
fn can_decode_rdb() {
    let whole_rdb = include_bytes!("../rdbs/2.rdb");
    let rdb = rdb(whole_rdb).to_full_result().unwrap();
    println!();
    println!("rdb version is {}", rdb.version);
    println!("rdb has those aux codes: {}", rdb.aux_codes
        .iter()
        .map(
            |(key,value)| format!("{}:{}", String::from_utf8_lossy(key),
                                               String::from_utf8_lossy(value)))
        .collect::<Vec<String>>().join(", "));
    println!("rdb has {} databases", rdb.databases.len());
    println!("rdb has {} entries", rdb.databases.iter().map(|d| d.entries.len()).sum::<usize>());
    println!("rdb checksum is {:?}", rdb.crc64_checksum);
    // assert_eq!(true, false);
}

#[test]
fn can_decode_expiry() {
    let mut data_s = vec![0xFD];
    let mut data_ms = vec![0xFC];
    data_s.write_u32::<LittleEndian>(1337).unwrap();
    data_ms.write_u64::<LittleEndian>(1337).unwrap();
    data_s.write_u64::<LittleEndian>(0xDEADBEEF).unwrap();
    let data_neither = &[54, 101, 41, 41];
    assert_eq!(expiry(&data_s), IResult::Done(&data_s[5..], Some(Duration::from_secs(1337))));
    assert_eq!(expiry(&data_ms), IResult::Done(&data_ms[9..], Some(Duration::from_millis(1337))));
    assert_eq!(expiry(data_neither), IResult::Done(&data_neither[..], None));

}

#[test]
fn can_read_length_decode() {
    let bits_6 = vec![ 0b00100011, 50];
    let bits_enc_val= vec![0b11101010, 50];
    let bits_14 = vec![ 0b01100011, 0b00100011, 50];
    let mut bits_32= Vec::<u8>::new();
    let mut bits_64= Vec::<u8>::new();

    bits_32.write_u8(constant::RDB_32BITLEN).unwrap();
    bits_32.write_u32::<BigEndian>(1337).unwrap();
    bits_32.write_u8(78).unwrap();

    bits_64.write_u8(constant::RDB_64BITLEN).unwrap();
    bits_64.write_u64::<BigEndian>(1337).unwrap();
    bits_64.write_u8(50).unwrap();
    bits_64.write_u64::<BigEndian>(0xDEADBEEF).unwrap();

    assert_eq!(read_length_with_encoding(&bits_6), IResult::Done(&bits_6[1..], (0b00100011, false)), "6 bit read_length");
    assert_eq!(read_length_with_encoding(&bits_enc_val), IResult::Done(&bits_enc_val[1..], (0b00101010, true)), "enc-val read_length");
    assert_eq!(read_length_with_encoding(&bits_14), IResult::Done(&bits_14[2..], (0b0010001100100011, false)), "14 bit read_length");
    assert_eq!(read_length_with_encoding(&bits_32), IResult::Done(&bits_32[5..], (1337, false)), "32 bit read_length");
    assert_eq!(read_length_with_encoding(&bits_64), IResult::Done(&bits_64[9..], (1337, false)), "64 bit read_length");

}

#[test]
fn can_string_decode() {

}