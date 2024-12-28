//will end up probably using unsafe rust -> c arrays?
//for now, just use vecs

use crate::btree::BTreeNode;
use crate::status::Status;
use crate::status::Status::{InternalExceptionIndexOutOfRange, InternalExceptionInvalidColCount, InternalExceptionInvalidRowLength, InternalExceptionInvalidSchema, InternalExceptionKeyNotFound, InternalExceptionPagerMismatch, InternalSuccess, Success};
use std::collections::HashMap;
use std::ffi::CString;
use std::fmt;
use std::fmt::Debug;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::marker::PhantomData;
use std::ops::Index;
use std::ptr::null;
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use crate::crypto::generate_random_hash;
use crate::serializer::Serializer;

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

#[derive(Clone, Debug)]
pub struct PageContainer {
    pub(crate) data: PageData, //not like this, look at my comments in BTreeNode
    pub(crate) position: Position,
    size_on_disk: usize,
}

//first byte specify the data type
//rest must be the length of the size of the type
pub type Key = Vec<u8>;
pub type Flag = u8;

//TODO if this throws errors if i change it, i must abstract every implementation :) not **every** implementation
pub type Position = i32;
pub type Row = Vec<u8>;

#[derive(Clone, Debug)]
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

#[derive(Debug)]
pub struct PagerCore {
    pub cache: HashMap<Position, PageContainer>,
    pub schema: TableSchema,
    pub hash: String,
    file: File,
    next_position: Position,
}

#[derive(Clone)]
pub struct PagerAccessor {
    pager: Arc<RwLock<PagerCore>>,
    hash: String,
    pub schema: Arc<TableSchema>,
}

impl Debug for PagerAccessor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[PagerAccessor]")
    }
}

impl PagerAccessor {
    // looks like dependency injection?
    pub fn new(pager: PagerCore) -> Self {
        let h = pager.hash.clone();
        let s = pager.schema.clone();
        Self {
            pager: Arc::new(RwLock::new(pager)),
            hash: h,
            schema: Arc::new(s),
        }
    }

    //this does create lots of overhead / could be removed
    pub fn verify(&self, pager: &PagerCore) -> bool {
        pager.hash == self.hash
    }

    pub fn access_pager_read<F, T>(&self, func: F) -> T
    where
        F: FnOnce(&PagerCore) -> T,
    {
        let pager = self
            .pager
            .read()
            .expect("Failed to acquire read lock on Pager");
        func(&pager)
    }
    pub fn access_pager_write<F, T>(&self, func: F) -> T
    where
        F: FnOnce(&mut PagerCore) -> T,
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
        F: FnOnce(&mut PageContainer, &TableSchema) -> Status,
    {
        //TODO Optimize reading / writing to the pager by checking cache beforehand!?
        let mut pager = self
            .pager
            .write()
            .expect("Failed to acquire write lock on Pager");
        let page: Result<&mut PageContainer, Status> = pager.access_page_write(node.page_position);
        if page.is_ok() {
            func(&mut page.unwrap(), &node.schema)
        } else {
            page.err().expect("there must be an err, i checked")
        }
    }
}

impl PagerCore {
    pub fn init_from_file(file_path: &str) -> Result<PagerAccessor, Status> {
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

        Ok(PagerAccessor::new(PagerCore {
            cache: HashMap::new(),
            schema,
            file,
            hash: generate_random_hash(16),
            next_position: (row_length * (1 + 16)) as Position
        }))
    }

    #[deprecated]
    pub fn init_from_schema(file_path: &str, schema: TableSchema) -> Result<PagerAccessor, Status> {
        if !Serializer::verify_schema(&schema).is_ok() {
            return Err(InternalExceptionInvalidSchema);
        }
        Ok(PagerAccessor::new(PagerCore {
            cache: HashMap::new(),
            schema,
            //TODO this is a workaround for development
            file: File::open(file_path).unwrap(),
            hash: generate_random_hash(16),
            next_position: 1
        }))
    }

    pub fn access_page_read(&mut self, position: Position) -> Result<PageContainer, Status> {
        let miss = !self.cache.contains_key(&position);

        //TODO optimize this! lets hope the compiler does magic for now
        if miss {
            let page = self.read_page_from_disk(position);
            if page.is_ok() {
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
                .expect("This should not happen")
                .clone())
        }
    }

    pub fn access_page_write(&mut self, position: Position) -> Result<&mut PageContainer, Status> {
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

    pub fn create_page(
        &mut self,
        keys: Vec<Key>,
        children: Vec<Position>,
        data: Vec<Row>,
        schema: &TableSchema,
        pager_facade: PagerAccessor
    ) -> Result<BTreeNode, Status> {
        let position = self.next_position;
        self.next_position += self.schema.row_length as i32;
        self.create_page_at_position(position, keys, children, data, schema, pager_facade)
    }

    fn create_page_at_position(
        &mut self,
        position: Position,
        keys: Vec<Key>,
        children: Vec<Position>,
        data: Vec<Row>,
        schema: &TableSchema,
        pager_facade: PagerAccessor //this will surely contain a reference to itself
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
        let page_data = Serializer::init_page_data(keys, data);

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

    fn insert_page_at_position(&mut self, position: Position, page_data: PageData) -> Status {
        let page = PageContainer {
            data: page_data,
            position,
            size_on_disk: 0,
        };
        self.cache.insert(position, page);
        InternalSuccess
    }

    pub fn read_page_from_disk(&mut self, position: Position) -> Result<PageContainer, Status> {
        self.file
            .seek(std::io::SeekFrom::Start(position as u64))
            .map_err(|_| Status::InternalExceptionReadFailed)?;

        let mut buffer = vec![0u8; self.schema.col_count];
        self.file
            .read_exact(&mut buffer)
            .map_err(|_| Status::InternalExceptionReadFailed)?;
        let size_on_disk = buffer.len();
        Ok(PageContainer {
            data: buffer,
            position,
            size_on_disk,
        })
    }

    pub fn write_page_to_disk(&mut self, page: &PageContainer) -> Result<(), Status> {
        self.file
            .seek(std::io::SeekFrom::Start(page.position as u64))
            .map_err(|_| Status::InternalExceptionWriteFailed)?;
        self.file
            .write_all(&page.data)
            .map_err(|_| Status::InternalExceptionWriteFailed)?;
        Ok(())
    }
}

