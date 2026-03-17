// File Structure V2

// 2 byte: next page index
// [Pages {Free-Space, Flag, PAGE_SIZE}] fyi (0,0) is an invalid position. the cells officially start at 1
//                     (of course, the location in the page starts at zero)

// Node Layout
// - 8 Bits: Number of Keys (n)
// - 8 Bits: Flag
// - n Keys: Each key is the length of the ID type read earlier.
// - n+1 Child/Page Pointers: Each pointer is [POSITION] bytes long.
// - Next There is the Data, According to the Schema Definition

// - so the size of a Node's Header (until the rows) is: n * (Number Of Keys) + (n + 1) * POSITION_SIZE

/// Page-Flag Definition
/// - Bit 0: Indicates if the page is dirty (needs to be written to disk)
/// - Bit 2: Indicates if a page is deleted (marked for vacuum)
/// - Bit 4: Lock
#[repr(u8)]
pub enum PageFlag {
    Dirty = 0,
    Deleted = 2,
    Lock = 4,
    Data = 5,
    Overflow = 6,
}

/// Node-Flag Definition
/// - Bit 1: Indicates if the Btree Node is a Leaf
#[repr(u8)]
pub enum NodeFlag {
    Leaf = 1,
    HasExternalData = 2,
}

/// Key-Flag Definition
/// - Bit 0: Indicates if the Key is marked for deletion
/// (keys cannot be null)
#[repr(u8)]
pub enum KeyMeta {
    Tomb = 0,
}

#[repr(u8)]
pub enum FieldMeta {
    Null = 0,
    External = 1,
}

/// Main on-disk page payload size in bytes.
pub const PAGE_SIZE: usize = 4093;
/// Full persisted page size including 3-byte metadata prefix.
pub const PAGE_SIZE_WITH_META: usize = PAGE_SIZE + 3;
/// B-Tree node metadata bytes: `num_keys` + `flag`.
pub const NODE_METADATA_SIZE: usize = 2;
/// Fixed byte length for `String` values.
pub const STRING_SIZE: usize = 256;
/// Fixed byte length for `Integer` values (including flag byte).
pub const INTEGER_SIZE: usize = 5;
/// Fixed byte length for `Date` values.
pub const DATE_SIZE: usize = 5;
/// Fixed byte length for `Boolean` values.
pub const BOOLEAN_SIZE: usize = 1;
/// Fixed byte length for `Null` placeholders.
pub const NULL_SIZE: usize = 1;
/// Type-tag byte length.
pub const TYPE_SIZE: usize = 1;
/// Encoded `Position` byte length.
pub const POSITION_SIZE: usize = 4;
/// Row name size used by metadata helpers.
pub const ROW_NAME_SIZE: usize = 16;
/// Integer payload bytes excluding flag byte.
pub const INTEGER_SIZE_WITHOUT_FLAG: usize = INTEGER_SIZE - 1;
/// Maximum encoded table name bytes.
pub const TABLE_NAME_SIZE: usize = 32;
/// Byte offset where pages start in file.
pub const PAGES_START_AT: usize = 2;

/// Number of inline bytes kept for externalized string/varchar fields.
pub const INLINE_STRING_PREFIX_LEN: usize = 12;

/// Total payload-page header size in bytes.
pub const PAYLOAD_HEADER_SIZE: usize = 8;

/// Offset of `next_page` (u16) in payload-page header.
pub const PAYLOAD_NEXT_PAGE_OFFSET: usize = 0;
/// Offset of payload chunk length (u16) in payload-page header.
pub const PAYLOAD_CHUNK_LEN_OFFSET: usize = 2;
/// Offset of payload header flags byte.
pub const PAYLOAD_HEADER_FLAGS_OFFSET: usize = 4;
/// Offset of owner root page (u16).
pub const PAYLOAD_OWNER_ROOT_OFFSET: usize = 5;
/// Offset of payload header magic marker.
pub const PAYLOAD_MAGIC_OFFSET: usize = 7;

/// Magic value marking new payload-page header format.
pub const PAYLOAD_MAGIC: u8 = 0xD1;
/// Bit in payload header flags indicating deprecated payload pages.
pub const PAYLOAD_FLAG_DEPRECATED: u8 = 0;

/// Marker byte stored in externalized field metadata.
pub const EXTERNAL_MARKER: u8 = 0xA5;

/// Offset of payload pointer in externalized field metadata.
pub const EXTERNAL_PTR_OFFSET: usize = INLINE_STRING_PREFIX_LEN + 1;
/// Offset of payload tail length in externalized field metadata.
pub const EXTERNAL_LEN_OFFSET: usize = EXTERNAL_PTR_OFFSET + POSITION_SIZE;
/// Offset of metadata marker byte in externalized field metadata.
pub const EXTERNAL_MARKER_OFFSET: usize = EXTERNAL_LEN_OFFSET + 2;
/// Offset of original field flag byte in externalized field metadata.
pub const EXTERNAL_ORIG_FLAG_OFFSET: usize = EXTERNAL_MARKER_OFFSET + 1;
/// Minimum field length required to hold externalization metadata.
pub const EXTERNAL_META_MIN_FIELD_LEN: usize = EXTERNAL_ORIG_FLAG_OFFSET + 1;
