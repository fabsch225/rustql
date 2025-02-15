//will end up probably using unsafe rust -> c arrays?
//for now, just use vecs

use std::cmp::PartialEq;
use crate::btree::BTreeNode;
use crate::crypto::generate_random_hash;
use crate::serializer::Serializer;
use crate::status::Status;
use crate::status::Status::{InternalExceptionInvalidColCount, InternalExceptionInvalidSchema, InternalExceptionPagerMismatch, InternalSuccess};
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, RwLock};
use crate::executor::TableSchema;
//in byte

pub const PAGE_SIZE: u16 = 1024;

pub const STRING_SIZE: usize = 256; //len: 255
pub const INTEGER_SIZE: usize = 5;
pub const DATE_SIZE: usize = 5;
pub const BOOLEAN_SIZE: usize = 1; //bit 8 is NULL-Flag
pub const NULL_SIZE: usize = 1;
pub const TYPE_SIZE: usize = 1;
pub const POSITION_SIZE: usize = 4; //during development. should be like 16
pub const ROW_NAME_SIZE: usize = 16;
pub const INTEGER_SIZE_WITHOUT_FLAG: usize = INTEGER_SIZE - 1;
pub const TABLE_NAME_SIZE : usize = 32;


// File Structure V2

// 2 byte: next page index
// [Pages {PAGE_SIZE}]

//---

// Database (File) Structure Overview [deprecated]

// Meta Information
// 32 Bits: Meta Information Length (divisible by the following: TABLE_NAME_SIZE -> equal to the amount of tables * TABLE_NAME_SIZE)
// [TABLE_NAME_SIZE]: names (identifiers) of the tables

// [Table-Schema Information] tables
// 16 Bits: Specifies the length of a row.
// 8 Bits: Type of Table (Entity, Index, View, Internal Table, Virtual Table *Joined*)
// 32 Bits: Root Position
// 32 Bits: Next Position
// 32 Bits: Offset Position
// 32 Bits: Approximate Count of Entries (new)
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
// - n+1 Child/Page Pointers: Each pointer is [POSITION] bytes long.
// - Next There is the Data, According to the Schema Definition

// - so the size of a pages Header (until the actual data) is like this: n * (Number Of Keys) + (n + 1) * POSITION_SIZE

//---

// ## Each Flag is a Byte =>
// Page-Flag Definition
// - Bit 0: Indicates if the page is dirty
// - Bit 1: Indicates if the Btree Node is a Leaf
// - Bit 2: Indicates if a page is deleted
// - Bit 3: Lock

// Key-Flag Definition
// - Bit 0: Indicates if the Key is marked for deletion
// (keys cannot be null)

#[derive(PartialEq, Clone)]
pub enum Type {
    Null,
    Integer,
    String,
    //Double, future feature
    //Varchar, future feature
    Date,
    Boolean,
    //Blob    future feature
}

impl Debug for Type {
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
    size_on_disk: usize, //TODO replace with free_space (?)
}

pub type TableName = Vec<u8>;

//first byte specify the data type
//rest must be the length of the size of the type
pub type Key = Vec<u8>;

pub type Flag = u8;

//TODO if this throws errors if i change it, i must abstract every implementation :) not **every** implementation
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Position {
    pub page_index: u16,
    pub location_on_page: u16
}

impl Position {
    pub fn make_empty() -> Self {
        Position {
            page_index: 0,
            location_on_page: 0
        }
    }
}

//cache whole pages, so only lookup the page_index
impl Hash for Position {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u16(self.page_index);
    }
}

pub type Row = Vec<u8>;

impl Serializer {
    pub fn empty_key(schema: &TableSchema) -> Key {
        vec![0; schema.key_length]
    }
    pub fn empty_row(schema: &TableSchema) -> Row {
        vec![0; schema.row_length]
    }
}

#[derive(Debug)]
pub struct PagerCore {
    cache: HashMap<Position, PageContainer>,
    pub hash: String,
    file: File
}

#[derive(Clone)]
pub struct PagerAccessor {
    pager: Arc<RwLock<PagerCore>>,
    hash: String,
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
        Self {
            pager: Arc::new(RwLock::new(pager)),
            hash: h,
        }
    }

    //TODO these methods dont belong here
    pub fn set_table_schema(&self, new_schema: TableSchema) {
        if self.pager.read().expect("failed to read pager").schema.tables.len() == 0 {
            self.pager.write().expect("failed to w-lock pager").schema.tables.push(new_schema);
        } else {
            self.pager.write().expect("failed to w-lock pager").schema.tables[0] = new_schema;
        }
        self.pager.write().expect("failed to w-lock pager").invalidate_cache();
    }

    pub(crate) fn set_root_to_none(&self) -> Result<(), Status>{
        todo!()
    }

    pub fn set_root(&self, node: &BTreeNode) -> Result<(), Status> {
        self.pager.write().expect("failed to w-lock pager").schema.tables[0].root = node.page_position.clone();
        Ok(())
    }

    pub fn has_root(&self) -> bool {
        true//self.read_table_schema().root != 0
    }

    pub fn read_schema(&self) -> Schema {
        self.pager.read().unwrap().schema.clone()
    }

    #[deprecated]
    pub fn read_table_schema(&self) -> TableSchema {
        self.pager.read().unwrap().schema.tables[0].clone()
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
            .map_err(|e|{
                println!("{:?}", e);
                Status::InternalExceptionPagerWriteLock
            })?;
        let page = pager.access_page_read(&node.page_position);

        if page.is_ok() {
            func(&page?.data, &pager.schema.tables[0].clone())
        } else {
            Err(page.unwrap_err())
        }
    }

    pub fn access_page_write<F>(&self, node: &BTreeNode, func: F) -> Result<(), Status>
    where
        F: FnOnce(&mut PageContainer, &TableSchema) -> Result<(), Status>,
    {
        //TODO Optimize reading / writing to the pager by checking cache beforehand!?
        let mut pager = self
            .pager
            .write().map_err(|e|{
                eprintln!("{:?}", e);
                Status::InternalExceptionPagerWriteLock
        })?;
        let schema = pager.schema.tables[0].clone();
        let page: Result<&mut PageContainer, Status> = pager.access_page_write(&node.page_position);
        if page.is_ok() {
            func(page?, &schema)
        } else {
            Err(page.unwrap_err())
        }
    }
}

impl PagerCore {
    pub fn flush(&mut self) -> Result<(), Status> {
        let schema_bytes = Serializer::schema_to_bytes(&self.schema);
        self.file.seek(SeekFrom::Start(0)).map_err(|_| Status::InternalExceptionWriteFailed)?;
        self.file.write_all(&schema_bytes).map_err(|e| {
            eprintln!("Failed to write schema bytes to disk: {:?}", e);
            Status::InternalExceptionWriteFailed
        })?;

        let positions: Vec<Position> = self.cache.keys().cloned().collect();

        for position in positions {
            let page = self.cache[&position].clone();
            if Serializer::is_dirty(&page.data)? {
                self.write_page_to_disk(&page)?;
            }
        }

        Ok(())
    }

    pub fn init_from_file(file_path: &str, btree_width: usize) -> Result<PagerAccessor, Status> {
        let mut file = match OpenOptions::new().write(true).read(true).open(file_path) {
            Ok(f) => f,
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    return Err(Status::InternalExceptionFileNotFound);
                } else {
                    eprintln!("Error is {}", e.to_string());
                    return Err(Status::InternalExceptionFileOpenFailed);
                }
            }
        };
        let schema = Serializer::read_schema(&mut file)?;

        Ok(PagerAccessor::new(PagerCore {
            cache: HashMap::new(),
            btree_width,
            schema,
            file,
            hash: generate_random_hash(16)
        }))
    }

    pub fn invalidate_cache(&mut self) -> Status {
        self.cache.clear();
        InternalSuccess
    }

    pub fn get_node_length(&self) -> i32 {
        ((2 * self.btree_width - 1) + (2 * self.btree_width - 1) * self.schema.tables[0].whole_row_length + self.btree_width * POSITION_SIZE + 2) as i32
    }

    pub fn access_page_read(&mut self, position: &Position) -> Result<PageContainer, Status> {
        let miss = !self.cache.contains_key(&position);

        //TODO optimize this! lets hope the compiler does magic for now
        if miss {
            let page = self.read_page_from_disk(position);
            if page.is_ok() {
                let page = page?.clone();
                self.cache.insert(position.clone(), page.clone());
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

    //this should be the only function that writes to pages, so we can keep track of the dirty-flag
    pub fn access_page_write(&mut self, position: &Position) -> Result<&mut PageContainer, Status> {
        let miss = !self.cache.contains_key(&position);
        //TODO optimize this ?
        if miss {
            let page = self.read_page_from_disk(position);
            if page.is_ok() {
                let mut page = page?;
                Serializer::set_is_dirty(&mut page.data, true)?;
                self.cache.insert(position.clone(), page);
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
                .get_mut(&position).map(|pc| {
                    Serializer::set_is_dirty(&mut pc.data, true)?;
                    return Ok(pc);
                })
                .ok_or(Status::InternalExceptionCacheDenied)??
            )
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
        let position = self.schema.tables[0].next_position.clone();
        self.schema.tables[0].next_position.location_on_page += self.get_node_length() as u16;
        self.create_page_at_position(&position, keys, children, data, schema, pager_facade)
    }

    fn create_page_at_position(
        &mut self,
        position: &Position,
        keys: Vec<Key>,
        children: Vec<Position>,
        data: Vec<Row>,
        schema: &TableSchema,
        pager_facade: PagerAccessor //this will surely contain a reference to itself
    ) -> Result<BTreeNode, Status> {
        //TODO think about removing the padding (<= and <) and a cleaner implementation
        if keys.len() + 1 < children.len() || keys.len() < data.len() {
            return Err(InternalExceptionInvalidColCount);
        }
        if !(keys[0].len() == schema.key_length && (data.len() == 0 || data[0].len() == schema.row_length)) {
            return Err(InternalExceptionInvalidSchema);
        }
        if !pager_facade.verify(&self) {
            return Err(InternalExceptionPagerMismatch);
        }

        //Development
        if data.len() > 0 {
            let data_length = data.first().map_or(0, |row| row.len());
            assert_eq!(data_length, schema.row_length)
        }

        let mut data = data;
        while data.len() < keys.len() {
            data.push(vec![0; schema.row_length]);
        }
        let orig_children_len = children.len();
        let mut children = children;
        //<= is correct -- we want one more child
        while children.len() <= keys.len() {
            children.push(Position::make_empty())
        }

        let page_data = Serializer::init_page_data(keys, data);

        let status_insert = self.insert_page_at_position(position, page_data);
        if (status_insert != InternalSuccess) {
            return Err(status_insert);
        }
        Ok(BTreeNode {
            page_position: position.clone(),
            pager_accessor: pager_facade.clone()
        })
    }

    fn insert_page_at_position(&mut self, position: &Position, page_data: PageData) -> Status {
        let page = PageContainer {
            data: page_data,
            position: position.clone(),
            size_on_disk: 0,
        };
        self.cache.insert(position.clone(), page);
        InternalSuccess
    }

    fn read_page_from_disk(&mut self, position: &Position) -> Result<PageContainer, Status> {
        self.file
            .seek(SeekFrom::Start(Self::get_file_position(position)))
            .map_err(|_| Status::InternalExceptionReadFailed)?;
        let mut meta_data_buffer = vec![0u8; 2];
        self.file
            .read_exact(&mut meta_data_buffer)
            .map_err(|_| Status::InternalExceptionReadFailed)?;
        let key_count = meta_data_buffer[0] as usize;
        let mut main_buffer = vec![0u8; key_count * (self.schema.tables[0].whole_row_length + POSITION_SIZE) + POSITION_SIZE];
        self.file
            .read_exact(&mut main_buffer)
            .map_err(|e| {
                eprintln!("Cannot read File to this len: {}", e);
                Status::InternalExceptionReadFailed
            })?;
        let size_on_disk = meta_data_buffer.len() + main_buffer.len();
        let mut data = meta_data_buffer;
        data.append(&mut main_buffer);
        Ok(PageContainer {
            data,
            position: position.clone(),
            size_on_disk,
        })
    }

    fn write_page_to_disk(&mut self, page: &PageContainer) -> Result<(), Status> {
        self.file
            .seek(SeekFrom::Start(Self::get_file_position(&page.position)))
            .map_err(|_| Status::InternalExceptionWriteFailed)?;
        assert!(page.data.len() as  u16 <= PAGE_SIZE);
        self.file
            .write_all(&page.data)
            .map_err(|_| Status::InternalExceptionWriteFailed)?;
        Ok(())
    }

    fn get_file_position(position: &Position) -> u64 {
        (position.page_index * PAGE_SIZE + position.location_on_page) as u64
    }
}

