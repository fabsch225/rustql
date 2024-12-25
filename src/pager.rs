//will end up probably using unsafe rust -> c arrays?
//for now, just use vecs

use crate::btree::BTreeNode;
use crate::status::Status;
use crate::status::Status::{InternalExceptionIndexOutOfRange, InternalExceptionInvalidColCount, InternalExceptionInvalidRowLength, InternalExceptionInvalidSchema, InternalExceptionKeyNotFound, InternalExceptionPagerMismatch, InternalSuccess, Success};
use std::collections::HashMap;
use std::ffi::CString;
use std::fmt;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::marker::PhantomData;
use std::ops::Index;
use std::ptr::null;
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use crate::crypto::generate_random_hash;

//in byte
pub const STRING_SIZE: usize = 256;
pub const INTEGER_SIZE: usize = 4;
pub const DATE_SIZE: usize = 3;
pub const BOOLEAN_SIZE: usize = 1; //why fucking not
pub const NULL_SIZE: usize = 1;
pub const TYPE_SIZE: usize = 1;
pub const POSITION_SIZE: usize = 4; //during development. should be like 16
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

//represents a whole page except the position i.e. keys, child-position and data
pub type PageData = Vec<u8>;

#[derive(Clone)]
pub struct Page {
    data: PageData, //not like this, look at my comments in BTreeNode
    position: Position,
    size_on_disk: usize,
}

//first byte specify the data type
//rest must be the length of the size of the type
pub type Key = Vec<u8>;
pub type Flag = u8;

//TODO if this throws errors if i change it, i must abstract every implementation :) not **every** implementation
pub type Position = i32;
pub type Row = Vec<u8>;

#[derive(Clone)]
pub struct TableSchema {
    pub col_count: usize,
    pub row_length: usize,
    pub key_length: usize,
    pub key_type: Type,
    pub data_length: usize,
    pub fields: Vec<Field>,
}

#[derive(Debug, Clone)]
pub struct Field {
    pub field_type: Type, // Assuming the type size is a single byte.
    pub name: String,     // The name of the field, extracted from 128 bits (16 bytes).
}

impl fmt::Display for TableSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableSchema")
            .field("col_count", &self.col_count)
            .field("row_length", &self.row_length)
            .field("row fields", &self.fields)
            .finish()
    }
}

pub struct Pager {
    pub cache: HashMap<Position, Page>,
    pub schema: TableSchema,
    pub hash: String,
    file: File,
}

#[derive(Clone)]
pub struct PagerFacade {
    pager: Arc<RwLock<Pager>>,
    hash: String,
}

impl PagerFacade {
    // looks like dependency injection?
    pub fn new(pager: Pager) -> Self {
        let h = pager.hash.clone();
        Self {
            pager: Arc::new(RwLock::new(pager)),
            hash: h,
        }
    }

    pub fn verify(&self, pager: &Pager) -> bool {
        pager.hash == self.hash
    }

    pub fn access_pager_read<F, T>(&self, func: F) -> T
    where
        F: FnOnce(&Pager) -> T,
    {
        let pager = self
            .pager
            .read()
            .expect("Failed to acquire read lock on Pager");
        func(&pager)
    }
    pub fn access_pager_write<F, T>(&self, func: F) -> T
    where
        F: FnOnce(&mut Pager) -> T,
    {
        let mut pager = self
            .pager
            .write()
            .expect("Failed to acquire write lock on Pager");
        func(&mut pager)
    }

    //this is a bit confusing, but the page read function does still require write-access to the pager...
    //TODO Optimize reading / writing to the pager by checking cache beforehand!
    // (now we have to account for the possibility of loading a page from disk, which requires mutating the filestream)
    pub fn access_page_read<F, T>(&self, node: &BTreeNode, func: F) -> Result<T, Status>
    where
        F: FnOnce(&PageData, &TableSchema) -> Result<T, Status>,
    {
        let mut pager = self
            .pager
            .write()
            .expect("Failed to acquire write lock on Pager");
        let page = pager.access_page_read(node.page_position);
        if page.is_ok() {
            func(&page?.data, &node.schema)
        } else {
            Err(page.err().expect("there must be an err, i checked"))
        }
    }

    pub fn access_page_write<F>(&self, node: &BTreeNode, func: F) -> Status
    where
        F: FnOnce(&mut PageData, &TableSchema) -> Status,
    {
        //TODO Optimize reading / writing to the pager by checking cache beforehand!?
        let mut pager = self
            .pager
            .write()
            .expect("Failed to acquire write lock on Pager");
        let page: Result<&mut Page, Status> = pager.access_page_write(node.page_position);
        if page.is_ok() {
            func(&mut page.unwrap().data, &node.schema)
        } else {
            page.err().expect("there must be an err, i checked")
        }
    }
}

impl Pager {
    pub fn init_from_file(file_path: &str) -> Result<PagerFacade, Status> {
        let mut file = File::open(file_path).map_err(|_| Status::InternalExceptionFileNotFound)?;
        let mut schema_length_bytes = [0u8; 2];
        file.read_exact(&mut schema_length_bytes)
            .map_err(|_| Status::InternalExceptionReadFailed)?;
        let row_length = u16::from_be_bytes(schema_length_bytes) as usize;
        let mut schema_data = vec![0u8; row_length * (1 + 16)];
        file.read_exact(&mut schema_data)
            .map_err(|_| Status::InternalExceptionReadFailed)?;
        let schema = Serializer::create_table_schema_from_bytes(&*schema_data, row_length);
        if !schema.is_ok() {
            return Err(Status::InternalExceptionInvalidSchema);
        }
        let schema = schema?;

        println!("Found Schema");
        println!("{}", schema);

        Ok(PagerFacade::new(Pager {
            cache: HashMap::new(),
            schema,
            file,
            hash: generate_random_hash(16)
        }))
    }

    pub fn init_from_schema(file_path: &str, schema: TableSchema) -> Result<PagerFacade, Status> {
        if !Serializer::verify_schema(&schema).is_ok() {
            return Err(InternalExceptionInvalidSchema);
        }
        Ok(PagerFacade::new(Pager {
            cache: HashMap::new(),
            schema,
            file: File::open(file_path).unwrap(),
            hash: generate_random_hash(16)
        }))
    }

    pub fn get_child(index: usize, parent: &BTreeNode) -> Option<BTreeNode> {
        //TODO Error handling
        let parent_position = parent.page_position;
        let page = parent
            .pager_interface
            .access_pager_write(|p| p.access_page_read(parent_position))
            .unwrap();
        let position = Serializer::read_children_as_vec(&page.data, &parent.schema).unwrap()[index];

        //TODO minimize read accesses to pager by implementing a load method only requiring reading. then treat a potential cache miss in another method
        //TODO Handle error: lifetime may not live long enough
        let page = parent
            .pager_interface
            .access_pager_write(|p| p.access_page_read(position))
            .unwrap();
        Some(Serializer::create_btree_node(
            page,
            parent.pager_interface.clone(),
        ))
    }

    pub fn access_page_read(&mut self, position: Position) -> Result<Page, Status> {
        use std::collections::hash_map::Entry;

        let miss = self.cache.contains_key(&position);

        //TODO optimize this! lets hope the compiler does its magic for now
        if (miss) {
            let page = self.read_page_from_disk(position);
            if (page.is_ok()) {
                let page = page?.clone();
                self.cache.insert(position, page.clone());
                Ok(page)
            } else {
                Err(page.err().unwrap())
            }
        } else {
            Ok(self
                .cache
                .get(&position)
                .expect("This should not happen, i checked just now")
                .clone())
        }
    }

    pub fn access_page_write(&mut self, position: Position) -> Result<&mut Page, Status> {
        let miss = self.cache.contains_key(&position);
        //TODO optimize this! lets hope the compiler does its magic for now
        if (miss) {
            let page = self.read_page_from_disk(position);
            if (page.is_ok()) {
                let page = page?;
                self.cache.insert(position, page);
                Ok(self
                    .cache
                    .get_mut(&position)
                    .expect("This should not happen, i checked just now"))
            } else {
                Err(page.err().unwrap())
            }
        } else {
            Ok(self
                .cache
                .get_mut(&position)
                .expect("This should not happen, i checked just now"))
        }
    }

    pub fn create_page_at_position(
        &mut self,
        position: Position,
        keys: Vec<Key>,
        children: Vec<Position>,
        data: Vec<Row>,
        schema: &TableSchema,
        pager_facade: PagerFacade //this will surely contain a reference to itself
    ) -> Result<BTreeNode, Status> {
        if !(keys.len() as i32 >= (children.len() as i32) - 1 && keys.len() == data.len()) {
            return Err(InternalExceptionInvalidColCount);
        }
        if !(keys[0].len() == schema.key_length && data[0].len() == schema.data_length) {
            return Err(InternalExceptionInvalidSchema);
        }
        if !pager_facade.verify(&self) {
            return Err(InternalExceptionPagerMismatch);
        }
        let page_data = Serializer::createPageData(keys, schema);
        let status_insert = self.insert_page_at_position(position, page_data);
        if (status_insert != InternalSuccess) {
            return Err(status_insert);
        }
        Ok(BTreeNode {
            page_position: position,
            is_leaf: children.len() == 0,
            schema: schema.clone(),
            pager_interface: pager_facade.clone()
        })
    }

    pub fn insert_page_at_position(&mut self, position: Position, page_data: PageData) -> Status {
        let page = Page {
            data: page_data,
            position,
            size_on_disk: 0,
        };
        self.cache.insert(position, page);
        InternalSuccess
    }

    pub fn read_page_from_disk(&mut self, position: Position) -> Result<Page, Status> {
        self.file
            .seek(std::io::SeekFrom::Start(position as u64))
            .map_err(|_| Status::InternalExceptionReadFailed)?;

        let mut buffer = vec![0u8; self.schema.col_count];
        self.file
            .read_exact(&mut buffer)
            .map_err(|_| Status::InternalExceptionReadFailed)?;
        let size_on_disk = buffer.len();
        Ok(Page {
            data: buffer,
            position,
            size_on_disk,
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
    //TODO Error Handling
    //TODO think more about architecture -- where to move this for example!?
    pub fn create_table_schema_from_bytes(
        bytes: &[u8],
        row_count: usize,
    ) -> Result<TableSchema, Status> {
        let mut fields = Vec::new();
        let mut offset = 0;
        let mut length = 0;
        let mut key_length = 0;
        while offset < bytes.len() {
            if offset + 1 > bytes.len() {
                return Err(Status::InternalExceptionInvalidSchema);
            }
            let field_type = bytes[offset];
            offset += 1;
            if offset + 16 > bytes.len() {
                return Err(Status::InternalExceptionInvalidSchema);
            }
            let name_bytes = &bytes[offset..offset + ROW_NAME_SIZE];
            offset += ROW_NAME_SIZE;
            let name =
                String::from_utf8(name_bytes.iter().copied().take_while(|&b| b != 0).collect())
                    .map_err(|_| Status::InternalExceptionInvalidSchema)?;
            let field_type =
                Serializer::parse_type(field_type).ok_or(Status::InternalExceptionInvalidSchema)?;
            let field_length = Self::get_size_of_type(&field_type).unwrap();
            if key_length == 0 {
                key_length = field_length;
            }
            length += field_length;
            fields.push(Field { field_type, name });
        }

        Ok(TableSchema {
            col_count: row_count,
            row_length: length,
            key_length,
            key_type: fields[0].field_type.clone(),
            data_length: length - key_length,
            fields,
        })
    }
    pub fn createPageData(keys: Vec<Key>, schema: &TableSchema) -> PageData {
        let mut result = PageData::new();
        result.push(0);
        result.push(Serializer::create_flag(true));
        Self::expand_keys_with_vec(&keys, &mut result, schema);
        result
    }
    //the expansion methods also expand the rows and children of course.
    pub fn expand_keys_by(
        expand_size: usize,
        page: &mut PageData,
        schema: &TableSchema,
    ) -> Result<usize, Status> {
        let original_size = page[0] as usize; //old number of keys
        let original_num_children = original_size + 1;
        let key_length = schema.key_length;
        let keys_offset = 2 + key_length * original_size;
        let new_keys_offset = expand_size * key_length;
        let new_children_start =
            keys_offset + new_keys_offset + original_num_children * POSITION_SIZE;
        let new_children_offset = expand_size * POSITION_SIZE;
        page.splice(keys_offset..keys_offset, vec![0; new_keys_offset]);
        page.splice(
            new_children_start..new_children_start,
            vec![0; new_children_offset],
        );
        page[0] = (original_size + expand_size) as u8;
        Ok(keys_offset)
    }
    pub fn expand_keys_with_vec(expansion: &Vec<Key>, page: &mut PageData, schema: &TableSchema) {
        let initial_offset =
            Self::expand_keys_by(expansion.len(), page, schema).expect("handle this! #x3qnnx");
        let mut current_offset = initial_offset;
        for data in expansion {
            page.splice(current_offset..current_offset, data.iter().cloned());
            current_offset += data.len();
        }
    }
    pub fn read_key(index: usize, page: &PageData, schema: &TableSchema) -> Result<Key, Status> {
        let size: usize = page[0] as usize;
        if index > size {
            return Err(InternalExceptionIndexOutOfRange);
        }
        let key_length = schema.key_length;
        let index_sized = 2 + key_length * (index - 1);
        Ok(page[index_sized..(index_sized + key_length)].to_owned())
    }
    pub fn read_child(
        index: usize,
        page: &PageData,
        schema: &TableSchema,
    ) -> Result<Position, Status> {
        let num_children = page[0] as usize + 1;
        if index > num_children {
            return Err(InternalExceptionIndexOutOfRange);
        }
        let key_length = schema.key_length;
        let start_pos = 2 + (num_children - 1) * key_length + index * POSITION_SIZE;
        let end_pos = start_pos + POSITION_SIZE;
        if end_pos > page.len() {
            return Err(InternalExceptionIndexOutOfRange);
        }
        //TODO think about approaches to some more Serializer methods like this
        Ok(Serializer::bytes_to_position(
            <&[u8; POSITION_SIZE]>::try_from(&page[start_pos..end_pos]).unwrap(),
        ))
    }
    pub fn write_key(index: usize, key: &Key, page: &mut PageData, schema: &TableSchema) -> Status {
        let num_keys = page[0] as usize;
        if index >= num_keys {
            return InternalExceptionIndexOutOfRange;
        }
        let key_length = schema.key_length;
        let list_start_pos = 2; // Start position of keys in the page
        let start_pos = list_start_pos + index * key_length;
        let end_pos = start_pos + key_length;
        page[start_pos..end_pos].copy_from_slice(key);
        Success
    }
    pub fn write_child(
        index: usize,
        child: Position,
        page: &mut PageData,
        schema: &TableSchema,
    ) -> Status {
        let num_keys = page[0] as usize;
        let num_children = num_keys + 1;
        if index >= num_children {
            return InternalExceptionIndexOutOfRange;
        }
        let key_length = schema.key_length;
        let list_start_pos = 2 + (num_keys * key_length);
        let start_pos = list_start_pos + index * POSITION_SIZE;
        let end_pos = start_pos + POSITION_SIZE;
        let child_bytes = Serializer::position_to_bytes(child);
        page[start_pos..end_pos].copy_from_slice(&child_bytes);
        Success
    }
    pub fn read_keys_as_vec(page: &PageData, schema: &TableSchema) -> Result<Vec<Key>, Status> {
        let mut result: Vec<Key> = Vec::new();
        let num_keys = page[0] as usize;
        let key_length = schema.key_length;
        let list_start_pos = 2; //TODO: Parameterize this somehow
        for i in 0..num_keys {
            let start_pos = list_start_pos + i * key_length;
            let end_pos = start_pos + key_length;
            result.push(page[start_pos..end_pos].to_owned());
        }
        Ok(result)
    }
    pub fn read_children_as_vec(
        page: &PageData,
        schema: &TableSchema,
    ) -> Result<Vec<Position>, Status> {
        let mut result: Vec<Position> = Vec::new();
        let num_keys = page[0] as usize;
        let key_length = schema.key_length;
        let list_start_pos = 2 + (num_keys * key_length);
        for i in 0..(num_keys + 1) {
            let start_pos = list_start_pos + i * POSITION_SIZE;
            let end_pos = start_pos + POSITION_SIZE;
            result.push(Serializer::bytes_to_position(
                <&[u8; POSITION_SIZE]>::try_from(&page[start_pos..end_pos]).unwrap(),
            ));
        }
        Ok(result)
    }
    pub fn write_keys_vec(keys: &Vec<Key>, page: &mut PageData, schema: &TableSchema) -> Status {
        let num_keys = page[0] as usize;
        let key_length = schema.key_length;
        let list_start_pos = 2;
        for i in 0..num_keys {
            let start_pos = list_start_pos + i * key_length;
            let end_pos = start_pos + key_length;
            page.splice(start_pos..end_pos, keys[i].to_vec());
        }
        Success
    }
    pub fn write_children_vec(
        children: &Vec<Position>,
        page: &mut PageData,
        schema: &TableSchema,
    ) -> Status {
        let num_keys = page[0] as usize;
        let key_length = schema.key_length;
        let list_start_pos = 2 + (num_keys * key_length);
        for (i, child) in children.iter().enumerate() {
            let start_pos = list_start_pos + i * POSITION_SIZE;
            let end_pos = start_pos + POSITION_SIZE;
            page.splice(start_pos..end_pos, Serializer::position_to_bytes(*child).to_vec());
        }
        Success
    }
    pub fn read_data_by_key(
        page: &PageData,
        key: Key,
        schema: &TableSchema,
    ) -> Result<Row, Status> {
        let keys = Self::read_keys_as_vec(&page, schema)?;
        let index = keys.iter().position(|k| k == &key);
        if let Some(index) = index {
            Self::read_data_by_index(page, index, schema)
        } else {
            Err(InternalExceptionKeyNotFound)
        }
    }
    pub fn read_data_by_index(
        page: &PageData,
        index: usize,
        schema: &TableSchema,
    ) -> Result<Row, Status> {
        let num_keys = page[0] as usize;
        let key_length = schema.key_length;
        let keys_start = 2;
        let children_start = keys_start + num_keys * key_length;
        let data_start = children_start + (num_keys + 1) * POSITION_SIZE;
        let row_length = schema.row_length;
        let start = data_start + index * row_length;
        let end = start + (index + 1) * row_length;
        Ok(page[start..end].to_vec())
    }

    pub fn write_data_by_key(
        page: &mut PageData,
        key: Key,
        row: Row,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        let keys = Self::read_keys_as_vec(&page, schema)?;
        let index = keys.iter().position(|k| k == &key);
        if let Some(index) = index {
            Self::write_data_by_index(page, index, row, schema)
        } else {
            Err(InternalExceptionKeyNotFound)
        }
    }

    pub fn write_data_by_index(
        page: &mut PageData,
        index: usize,
        row: Row,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        let num_keys = page[0] as usize;
        let key_length = schema.key_length;
        let keys_start = 2;
        let children_start = keys_start + num_keys * key_length;
        let data_start = children_start + (num_keys + 1) * POSITION_SIZE;
        let row_length = schema.row_length;
        let start = data_start + index * row_length;
        let end = start + row_length;

        if row.len() != row_length {
            return Err(InternalExceptionInvalidRowLength);
        }

        if start >= page.len() || end > page.len() {
            return Err(InternalExceptionIndexOutOfRange);
        }

        page[start..end].copy_from_slice(&row);
        Ok(())
    }

    pub fn read_data_by_vec(page: &PageData, schema: &TableSchema) -> Result<Vec<Row>, Status> {
        let num_keys = page[0] as usize;
        let key_length = schema.key_length;
        let keys_start = 2;
        let children_start = keys_start + num_keys * key_length;
        let data_start = children_start + (num_keys + 1) * POSITION_SIZE;
        let row_length = schema.row_length;

        let mut rows = Vec::new();
        for index in 0..num_keys {
            let start = data_start + index * row_length;
            let end = start + row_length;
            if end > page.len() {
                return Err(InternalExceptionIndexOutOfRange);
            }
            rows.push(page[start..end].to_vec());
        }
        Ok(rows)
    }

    pub fn write_data_by_vec(
        page: &mut PageData,
        rows: Vec<Row>,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        let num_keys = page[0] as usize;
        if rows.len() != num_keys {
            return Err(InternalExceptionInvalidColCount);
        }

        let key_length = schema.key_length;
        let keys_start = 2;
        let children_start = keys_start + num_keys * key_length;
        let data_start = children_start + (num_keys + 1) * POSITION_SIZE;
        let row_length = schema.row_length;

        for (index, row) in rows.iter().enumerate() {
            if row.len() != row_length {
                return Err(InternalExceptionInvalidRowLength);
            }
            let start = data_start + index * row_length;
            let end = start + row_length;
            if start >= page.len() || end > page.len() {
                return Err(InternalExceptionIndexOutOfRange);
            }
            page[start..end].copy_from_slice(row);
        }
        Ok(())
    }

    pub fn create_btree_node(page: Page, pager_facade: PagerFacade) -> BTreeNode {
        let num_keys = page.data[0] as usize;
        let schema = pager_facade.access_pager_read(|pager| pager.schema.clone());
        let flag_byte = page.data[1];
        let is_leaf = Self::byte_to_bool_at_position(flag_byte, 2);

        //let id_type = schema.key_type;
        //let key_size = schema.key_length;
        //the current approach is to store the things only once in the pager...
        /*
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
                <&[u8; POSITION_SIZE]>::try_from(child_bits).expect("corrupted position"),
            );
            children.push(child_id);
        }

        let data_start = children_start + (num_keys + 1) * POSITION_SIZE;
        let row_length = schema.row_length;
        let mut data: Vec<Row> = vec!();

        for i in 0..num_keys {
            let start = data_start + i * row_length;
            let end = start +  (i + 1) * row_length;
            data.push(page.data[start..end].to_vec());
        }
         */

        // Construct the node
        BTreeNode {
            is_leaf,
            pager_interface: pager_facade,
            page_position: page.position, // The node's position in the pager
            schema,
        }
    }

    pub fn get_data(page: &Page, index: usize, schema: TableSchema) -> Vec<u8> {
        let num_keys = page.data[0] as usize;
        let header_length = num_keys * schema.key_length + (num_keys + 1) * POSITION_SIZE;
        let offset = header_length + index * schema.data_length;

        page.data[offset..offset + schema.data_length].to_vec()
    }

    pub fn verify_schema(schema: &TableSchema) -> Result<(), Status> {
        let computed_data_length: usize = schema.fields.iter()
            .map(|field| Self::get_size_of_type(&field.field_type).unwrap_or(0))
            .sum::<usize>() - schema.key_length;

        if computed_data_length != schema.data_length {
            return Err(InternalExceptionInvalidSchema);
        }
        if let Some(key_field) = schema.fields.get(0) {
            let key_field_size = Self::get_size_of_type(&key_field.field_type).unwrap_or(0);
            if key_field_size != schema.key_length {
                return Err(InternalExceptionInvalidSchema);
            }
        } else {
            return Err(InternalExceptionInvalidSchema);
        }

        let computed_row_length: usize = schema.fields.iter()
            .map(|field| Self::get_size_of_type(&field.field_type).unwrap_or(0))
            .sum();

        if computed_row_length != schema.row_length {
            return Err(InternalExceptionInvalidSchema);
        }
        if schema.fields.len() != schema.col_count {
            return Err(InternalExceptionInvalidSchema);
        }
        Ok(())
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
        let size = Serializer::get_size_of_type(&final_type).unwrap();
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

    pub fn create_flag(is_leaf: bool) -> Flag {
        let bits = [false, is_leaf, true, true, true, true, true, true];
        Serializer::bits_to_bytes(&bits)[0]
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

    pub fn ascii_to_bytes(ascii: &str) -> [u8; STRING_SIZE] {
        let mut bytes = [0u8; STRING_SIZE];
        for (i, &c) in ascii.as_bytes().iter().take(STRING_SIZE).enumerate() {
            bytes[i] = c;
        }
        bytes
    }

    pub fn bytes_to_position(bytes: &[u8; POSITION_SIZE]) -> Position {
        let mut value = 0i32;
        for &byte in bytes {
            value = (value << 8) | (byte as i32);
        }
        value
    }

    pub fn position_to_bytes(position: Position) -> [u8; POSITION_SIZE] {
        let mut bytes = [0u8; POSITION_SIZE];
        for i in 0..POSITION_SIZE {
            bytes[POSITION_SIZE - 1 - i] = ((position >> (i * 8)) & 0xFF) as u8;
        }
        bytes
    }

    pub fn bytes_to_int(bytes: &[u8; INTEGER_SIZE]) -> i32 {
        let mut value = 0i32;
        for &byte in bytes {
            value = (value << 8) | (byte as i32);
        }
        value
    }

    pub fn int_to_bytes(value: i32) -> [u8; INTEGER_SIZE] {
        let mut bytes = [0u8; INTEGER_SIZE];
        for i in 0..INTEGER_SIZE {
            bytes[INTEGER_SIZE - 1 - i] = ((value >> (i * 8)) & 0xFF) as u8;
        }
        bytes
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

    pub fn date_to_bytes(year: i32, month: i32, day: i32) -> [u8; DATE_SIZE] {
        let mut bytes = [0u8; DATE_SIZE];
        bytes[0] = (year >> 8) as u8;
        bytes[1] = (year & 0xFF) as u8;
        bytes[2] = ((month << 4) as u8) | (day as u8);
        bytes
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
