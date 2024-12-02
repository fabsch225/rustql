//will end up probably using unsafe rust -> c arrays?
//for now, just use vecs

use crate::btree::BtreeNode;
use crate::status::Status;
use crate::status::Status::{InternalExceptionIndexOutOfRange, InternalSuccess};
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::rc::Rc;
use std::sync::{Arc, RwLock};

#[derive(PartialEq, Clone)]
pub enum Type {
    Null,
    Integer,
    String,
    //Double, future feature
    Date,
    Boolean,
    //Blob    future feature
}

impl fmt::Debug for Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Type::Null => write!(f, "Null"),
            Type::Integer => write!(f, "Integer"),
            Type::String => write!(f, "String"),
            Type::Date => write!(f, "Date"),
            Type::Boolean => write!(f, "Boolean"),
            _ => write!(f, "Unknown"),
        }
        .expect("Wierd Error");
        Ok(())
    }
}

//in bit
pub const STRING_SIZE: usize = 256;
pub const INTEGER_SIZE: usize = 4;
pub const DATE_SIZE: usize = 3;
pub const BOOLEAN_SIZE: usize = 1; //why fucking not
pub const NULL_SIZE: usize = 1;
pub const TYPE_SIZE: usize = 1;
pub const POSITION_SIZE: usize = 4;//during development. should be like 16
pub const ROW_NAME_SIZE: usize = 16;

// Database Structure Overview

// Table Storage
// The database consists of a single table stored in a file with the following structure:

// Schema Information
// 16 Bits: Specifies the length of a row.
// Fields: Each field is defined as follows:
// [TYPE_SIZE - Type of Field][128 Bits - Name of Field (Ascii)] -> 8 Chars
// The first field is the ID.

// Page Storage
// Pages are nodes in a B-tree structure. Each page contains a fixed maximum of keys/rows (T - 1)
// and child nodes (T). All rows belong to the same table, and the schema is stored separately.

// Page Layout
// - 8 Bits: Number of Keys (n)
// - 8 Bits: Flag
// - n Keys: Each key is the length of the ID type read earlier.
// - n+1 Child/Page Pointers: Each pointer is 128 bits long.
// - Next There is the Data, According to the Schema Definition

// - so the size of a pages Header (until the actual data) is like this: n * (Number Of Keys) + (n + 1) * POSITION_SIZE


// Flag Definition
// - Bit 0: Indicates if the page is cached.
// - Bit 1: Indicates if the Btree Node is a Leaf
// - Remaining Bits: Specific pattern (111111)

pub type PageData = Vec<u8>;

pub struct Page {
    data: PageData,
    position: Position,
}

//first byte specify the data type
//rest must be the length of the size of the type

pub type Key = Vec<u8>;

pub type Position = i128;

#[derive(Clone)]
pub struct TableSchema {
    row_size: usize,
    row_length: usize,
    key_length: usize,
    data_length: usize,
    fields: Vec<Field>,
}

#[derive(Debug, Clone)]
struct Field {
    field_type: Type, // Assuming the type size is a single byte.
    name: String,     // The name of the field, extracted from 128 bits (16 bytes).
}

impl TableSchema {
    pub fn get_id_type(&self) -> (Status, Option<&Type>) {
        (
            Status::InternalSuccess,
            Option::from(&self.fields.get(0).unwrap().field_type),
        )
    }

    pub fn from_bytes(bytes: &[u8], row_count: usize) -> Result<Self, Status> {
        let mut fields = Vec::new();
        let mut offset = 0;
        let mut length = 0;
        let mut key_length = 0;
        while offset < bytes.len() {
            // Ensure there are enough bytes for the type (1 byte).
            if offset + 1 > bytes.len() {
                return Err(Status::InternalExceptionInvalidSchema);
            }

            // Extract the type (1 byte).
            let field_type = bytes[offset];
            offset += 1;

            // Ensure there are enough bytes for the name (16 bytes).
            if offset + 16 > bytes.len() {
                return Err(Status::InternalExceptionInvalidSchema);
            }

            // Extract the name (16 bytes).
            let name_bytes = &bytes[offset..offset + ROW_NAME_SIZE];
            offset += ROW_NAME_SIZE;

            // Convert the name bytes to a UTF-8 string, trimming null bytes.
            let name = String::from_utf8(name_bytes.iter().copied().take_while(|&b| b != 0).collect())
                .map_err(|_| Status::InternalExceptionInvalidSchema)?;

            let field_type = Serializer::parse_type(field_type).ok_or(Status::InternalExceptionInvalidSchema)?;
            let field_length = Type::get_size_of_type(&field_type).unwrap();
            if key_length == 0 {
                //the key is the first field
                key_length = field_length;
            }
            length += field_length;
            fields.push(Field {
                field_type,
                name,
            });
        }

        Ok(TableSchema {
            row_size: row_count,
            row_length: length,
            key_length,
            data_length: length - key_length,
            fields,
        })
    }
}

impl fmt::Display for TableSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableSchema")
            .field("row_size", &self.row_size)
            .field("row_length", &self.row_length)
            .field("row fields", &self.fields)
            .finish()
    }
}

impl Type {
    fn get_size_of_type(ty: &Type) -> Option<usize> {
        match ty {
            Type::String => Some(STRING_SIZE),
            Type::Integer => Some(INTEGER_SIZE),
            Type::Date => Some(DATE_SIZE),
            Type::Boolean => Some(BOOLEAN_SIZE),
            Type::Null => Some(NULL_SIZE),
            _ => None,
        }
    }
}

struct PageReader {}

impl PageReader {
    pub fn get_key(index: usize, page: &PageData, schema: TableSchema) -> (Status, Option<Key>) {
        //first 8 bits is the number of page is the number of keys / rows
        let size: usize = page[0] as usize;
        if index > size {
            return (InternalExceptionIndexOutOfRange, None);
        }
        //next 8 bits is a flag
        //next bits are the keys
        let (status, id_type) = schema.get_id_type();
        if status != InternalSuccess {
            return (status, None);
        }
        let key_size = Type::get_size_of_type(id_type.unwrap()).unwrap();
        let index_sized = 2 + key_size * index;

        (
            InternalSuccess,
            Some(page[index_sized..(index_sized + key_size)].to_owned()),
        )
    }
    pub fn get_child_position(index: usize, page: &PageData) -> (Status, Option<Position>) {
        let num_keys = page[0] as usize;
        if index > num_keys {
            return (InternalExceptionIndexOutOfRange, None);
        }

        // Child pointers start after the keys
        let start_pos = 2 + num_keys * POSITION_SIZE + index * POSITION_SIZE;
        let end_pos = start_pos + POSITION_SIZE;

        if end_pos > page.len() {
            return (InternalExceptionIndexOutOfRange, None);
        }

        (
            InternalSuccess,
            Some(Serializer::bytes_to_position(
                <&[u8; 16]>::try_from(&page[start_pos..end_pos]).unwrap(),
            )),
        )
    }
}

pub struct Pager {
    pub cache: HashMap<Position, Page>,
    pub schema: TableSchema,
    file: File,
}

#[derive(Clone)]
pub struct PagerFacade {
    pager: Arc<RwLock<Pager>>,
}

impl PagerFacade {
    // Initialize the PagerFacade with a singleton Pager
    pub fn new(pager: Pager) -> Self {
        Self {
            pager: Arc::new(RwLock::new(pager)),
        }
    }

    // Access the Pager for reading
    pub fn access_pager_read<F, T>(&self, func: F) -> T
    where
        F: FnOnce(&Pager) -> T,
    {
        let pager = self.pager.read().expect("Failed to acquire read lock on Pager");
        func(&pager)
    }

    // Access the Pager for writing
    pub fn access_pager_write<F, T>(&self, func: F) -> T
    where
        F: FnOnce(&mut Pager) -> T,
    {
        let mut pager = self.pager.write().expect("Failed to acquire write lock on Pager");
        func(&mut pager)
    }
}

impl Pager {
    pub fn init(file_path: &str) -> Result<Self, Status> {
        let mut file = File::open(file_path).map_err(|_| Status::InternalExceptionFileNotFound)?;
        let mut schema_length_bytes = [0u8; 2];
        file.read_exact(&mut schema_length_bytes)
            .map_err(|_| Status::InternalExceptionReadFailed)?;
        let row_length = u16::from_be_bytes(schema_length_bytes) as usize;
        let mut schema_data = vec![0u8; row_length * (1 + 16)];
        file.read_exact(&mut schema_data)
            .map_err(|_| Status::InternalExceptionReadFailed)?;
        let schema = TableSchema::from_bytes(&*schema_data, row_length);
        if !schema.is_ok() {
            return Err(Status::InternalExceptionInvalidSchema);
        }
        let schema = schema?;

        println!("Found Schema");
        println!("{}", schema);

        Ok(Pager {
            cache: HashMap::new(),
            schema,
            file,
        })
    }

    pub fn get_child(index: usize, parent: &BtreeNode) -> Option<BtreeNode> {
        //TODO Error handling
        let position = parent.children[index];
        //TODO minimize read accesses to pager by implementing a load method only requiring reading. then treat a potential cache miss in another method
        //TODO Handle error: lifetime may not live long enough
        let page = parent.pager_interface.access_pager_write(|p| p.load(position)).unwrap();
        Some(Serializer::create_btree_node(page, parent.pager_interface.clone()))
    }

    pub fn load(&mut self, position: Position) -> Option<&Page> {
        use std::collections::hash_map::Entry;

        match self.cache.entry(position) {
            Entry::Occupied(entry) => Some(entry.get()),
            Entry::Vacant(entry) => {
                if let Some(page) = self.read_page_from_disk(entry.key().clone()) {
                    Some(entry.insert(page))
                } else {
                    None
                }
            }
        }
    }

    pub fn read_page_from_disk(
        &mut self,
        position: Position,
    ) -> Result<Page, Status> {
        self.file
            .seek(std::io::SeekFrom::Start(position as u64))
            .map_err(|_| Status::InternalExceptionReadFailed)?;

        let mut buffer = vec![0u8; self.schema.row_size];
        self.file
            .read_exact(&mut buffer)
            .map_err(|_| Status::InternalExceptionReadFailed)?;

        Ok(Page {
            data: buffer,
            position,
        })
    }

    pub fn write_page_to_disk(&mut self, page: &Page) -> Result<(), Status> {
        self.file
            .seek(std::io::SeekFrom::Start(page.position as u64))
            .map_err(|_| Status::InternalExceptionWriteFailed)?;
        self.file
            .write_all(&page.data)
            .map_err(|_| Status::InternalExceptionWriteFailed)?;
        Ok(())
    }
}

pub struct Serializer {}

impl Serializer {
    pub fn create_btree_node(page: &Page, pager_facade: PagerFacade) -> BtreeNode {
        let num_keys = page.data[0] as usize;
        let schema = pager_facade.access_pager_read(|pager| {pager.schema.clone()});
        let flag_byte = page.data[1];
        let is_leaf = Self::byte_to_bool_at_position(flag_byte, 2);

        let (_, id_type) = schema.get_id_type();
        let key_size =
            Type::get_size_of_type(id_type.unwrap()).expect("Invalid key type in schema.");

        // Extract keys
        let mut keys = Vec::new();
        let keys_start = 2; // Start reading keys after the number of keys and flag
        for i in 0..num_keys {
            let start = keys_start + i * key_size;
            let end = start + key_size;
            let key_bits = &page.data[start..end];
            keys.push(key_bits.to_vec());
        }

        // Extract child pointers
        let mut children = Vec::new();
        let children_start = keys_start + num_keys * key_size;
        for i in 0..(num_keys + 1) {
            let start = children_start + i * POSITION_SIZE;
            let end = start + POSITION_SIZE;
            let child_bits = &page.data[start..end];
            let child_id = Serializer::bytes_to_position(
                <&[u8; 16]>::try_from(child_bits).expect("corrupted position"),
            );
            children.push(child_id);
        }

        // Construct the node
        BtreeNode {
            keys,
            children,
            is_leaf,
            pager_interface: pager_facade,
            page_position: page.position, // The node's position in the pager
        }
    }

    pub fn get_data(page: &Page, index: usize, schema: TableSchema) -> Vec<u8> {
        let num_keys = page.data[0] as usize;
        let heder_length = num_keys * schema.key_length + (num_keys + 1) * POSITION_SIZE;
        let offset = heder_length + index * schema.data_length;

        page.data[offset..offset + schema.data_length].to_vec()
    }

    pub fn write_btree_node_to_memory(
        node: &BtreeNode,
        schema: &TableSchema,
        data: Vec<u8>,
    ) -> Page {
        let mut serialized_data = Vec::new();

        // Write the number of keys (8 bits)
        serialized_data.push(node.keys.len() as u8);

        // Write the flag byte (8 bits)
        let mut flag_byte = 0;
        if node.is_leaf {
            flag_byte |= 1 << 2; // Set leaf flag
        }
        serialized_data.push(flag_byte);

        // Write the keys (n * key_size)
        let key_size = Type::get_size_of_type(schema.get_id_type().1.unwrap())
            .expect("Invalid key type in schema");
        for key in &node.keys {
            serialized_data.extend_from_slice(&key[..key_size]);
        }

        // Write the child pointers ((n+1) * POSITION_SIZE)
        for child in &node.children {
            let mut child_bytes = [0u8; POSITION_SIZE];
            let mut temp = *child;
            for byte in child_bytes.iter_mut().rev() {
                *byte = (temp & 0xFF) as u8;
                temp >>= 8;
            }
            serialized_data.extend_from_slice(&child_bytes);
        }

        // Write the data (appended directly)
        serialized_data.extend_from_slice(&data);

        // Construct and return the serialized Page
        Page {
            data: serialized_data,
            position: node.page_position,
        }
    }

    //TODO Error handling
    pub fn compare(a: &Key, b: &Key) -> Result<std::cmp::Ordering, Status> {
        let type_a_byte = a[0];
        let type_b_byte = b[0];

        let type_a = Serializer::parse_type(type_a_byte);
        let type_b = Serializer::parse_type(type_b_byte);

        if !type_a.is_some() || !type_b.is_some() {
            return Err(Status::InternalExceptionInvalidSchema);
        }

        if type_a != type_b {
            return Err(Status::InternalExceptionTypeMismatch);
        }
        let final_type = type_a.unwrap();
        let size = Type::get_size_of_type(&final_type).unwrap();
        let end_position = TYPE_SIZE + size;

        match final_type {
            Type::String => Ok(Self::compare_strings(
                <&[u8; 256]>::try_from(&a[TYPE_SIZE..end_position]).unwrap(),
                <&[u8; 256]>::try_from(&b[TYPE_SIZE..end_position]).unwrap(),
            )),
            Type::Integer => Ok(Self::compare_integers(
                <&[u8; 4]>::try_from(&a[TYPE_SIZE..end_position]).unwrap(),
                <&[u8; 4]>::try_from(&b[TYPE_SIZE..end_position]).unwrap(),
            )),
            Type::Date => Ok(Self::compare_dates(
                <&[u8; 3]>::try_from(&a[TYPE_SIZE..end_position]).unwrap(),
                <&[u8; 3]>::try_from(&b[TYPE_SIZE..end_position]).unwrap(),
            )),
            Type::Boolean => Ok(Self::compare_booleans(a[1], b[1])),
            Type::Null => Ok(std::cmp::Ordering::Equal),
        }
    }

    pub fn compare_strings(a: &[u8; STRING_SIZE], b: &[u8; STRING_SIZE]) -> std::cmp::Ordering {
        let str_a = Self::bytes_to_ascii(a);
        let str_b = Self::bytes_to_ascii(b);
        str_a.cmp(&str_b)
    }

    pub fn compare_integers(a: &[u8; INTEGER_SIZE], b: &[u8; INTEGER_SIZE]) -> std::cmp::Ordering {
        let int_a = Self::bytes_to_int(a);
        let int_b = Self::bytes_to_int(b);
        int_a.cmp(&int_b)
    }

    pub fn compare_dates(a: &[u8; DATE_SIZE], b: &[u8; DATE_SIZE]) -> std::cmp::Ordering {
        let date_a = Self::bytes_to_date(a);
        let date_b = Self::bytes_to_date(b);
        date_a.cmp(&date_b)
    }

    pub fn compare_booleans(a: u8, b: u8) -> std::cmp::Ordering {
        let bool_a = Self::byte_to_bool(a);
        let bool_b = Self::byte_to_bool(b);
        bool_a.cmp(&bool_b)
    }

    pub fn bytes_to_ascii(bytes: &[u8; STRING_SIZE]) -> String {
        bytes
            .iter()
            .map(|&byte| byte as char)
            .take_while(|&c| c != '\0')
            .collect()
    }

    pub fn bytes_to_position(bytes: &[u8; POSITION_SIZE]) -> i128 {
        let mut value = 0i128;
        for &byte in bytes {
            value = (value << 8) | (byte as i128);
        }
        value
    }

    pub fn bytes_to_int(bytes: &[u8; INTEGER_SIZE]) -> i32 {
        let mut value = 0i32;
        for &byte in bytes {
            value = (value << 8) | (byte as i32);
        }
        value
    }

    pub fn bytes_to_int_variable_length(bytes: &[u8]) -> i32 {
        let mut value = 0i32;
        for &byte in bytes {
            value = (value << 8) | (byte as i32);
        }
        value
    }

    pub fn bytes_to_date(bytes: &[u8; DATE_SIZE]) -> (i32, i32, i32) {
        let year = ((bytes[0] as i32) << 8) | (bytes[1] as i32);

        let month = ((bytes[2] >> 4) & 0b1111) as i32;
        let day = (bytes[2] & 0b1111) as i32;

        (year, month, day)
    }

    pub fn byte_to_bool(byte: u8) -> bool {
        byte & 1 != 0
    }
    pub fn byte_to_bool_at_position(byte: u8, pos: u8) -> bool {
        (byte & (1 << pos)) != 0
    }

    pub fn bytes_to_bits(bytes: &[u8]) -> Vec<bool> {
        bytes
            .iter()
            .flat_map(|byte| (0..8).rev().map(move |i| (byte & (1 << i)) != 0))
            .collect()
    }

    pub fn bits_to_bytes(bits: &[bool]) -> Vec<u8> {
        bits.chunks(8)
            .map(|chunk| {
                chunk
                    .iter()
                    .enumerate()
                    .fold(0u8, |byte, (i, &bit)| byte | ((bit as u8) << (7 - i)))
            })
            .collect()
    }

    pub fn parse_type(word: u8) -> Option<Type> {
        match word {
            0 => Some(Type::Null),
            1 => Some(Type::Integer),
            2 => Some(Type::String),
            3 => Some(Type::Date),
            4 => Some(Type::Boolean),
            _ => None,
        }
    }
}
