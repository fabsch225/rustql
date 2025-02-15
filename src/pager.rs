//will end up probably using unsafe rust -> c arrays?
//for now, just use vecs

use crate::btree::BTreeNode;
use crate::crypto::generate_random_hash;
use crate::executor::TableSchema;
use crate::serializer::Serializer;
use crate::status::Status;
use crate::status::Status::{
    InternalExceptionInvalidColCount, InternalExceptionInvalidSchema,
    InternalExceptionPagerMismatch, InternalSuccess,
};
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, RwLock};
//in byte

pub const PAGE_SIZE: u16 = 1024;
pub const PAGE_SIZE_WITH_META: u16 = PAGE_SIZE + 3; //2 for free space on page, 1 for flag

pub const STRING_SIZE: usize = 256; //len: 255
pub const INTEGER_SIZE: usize = 5;
pub const DATE_SIZE: usize = 5;
pub const BOOLEAN_SIZE: usize = 1; //bit 8 is NULL-Flag
pub const NULL_SIZE: usize = 1;
pub const TYPE_SIZE: usize = 1;
pub const POSITION_SIZE: usize = 4;
pub const ROW_NAME_SIZE: usize = 16;
pub const INTEGER_SIZE_WITHOUT_FLAG: usize = INTEGER_SIZE - 1;
pub const TABLE_NAME_SIZE: usize = 32;

// File Structure V2

// 2 byte: next page index
// [Pages {PAGE_SIZE}]

pub const PAGES_START_AT: usize = 2;

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
pub type PageData = Vec<u8>; //[u8; PAGE_SIZE as usize];

#[derive(Clone, Debug)]
pub struct PageContainer {
    pub data: PageData, //not like this, look at my comments in BTreeNode
    pub position: Position,
    pub free_space: u16,
    pub flag: Flag,
}

pub type TableName = Vec<u8>;

//first byte specify the data type
//rest must be the length of the size of the type
pub type Key = Vec<u8>;

pub type Flag = u8;

//TODO if this throws errors if i change it, i must abstract every implementation :) not **every** implementation
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Position {
    pub page: u16,
    pub cell: u16,
}

impl Position {
    pub fn new(page_index: u16, location_on_page: u16) -> Self {
        Position {
            page: page_index,
            cell: location_on_page,
        }
    }
    pub fn make_empty() -> Self {
        Position { page: 0, cell: 0 }
    }

    pub fn is_empty(&self) -> bool {
        self.page == 0 && self.cell == 0
    }

    fn get_file_position(&self) -> u64 {
        (self.page * PAGE_SIZE_WITH_META + self.cell) as u64
    }
}

//cache whole pages, so only lookup the page_index
impl Hash for Position {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u16(self.page);
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
    pub hash: String,
    cache: HashMap<Position, PageContainer>,
    file: File,
    next_page_index: u16,
}

#[derive(Clone)]
pub struct PagerAccessor {
    pager: Arc<RwLock<PagerCore>>,
    hash: String, //eventually remove this
                  //one could add a Arc<Priority Queue> here, for the lambdas
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

    pub(crate) fn get_next_page_index(&self) -> u16 {
        todo!()
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
        F: FnOnce(&PageData) -> Result<T, Status>,
    {
        let mut pager = self.pager.write().map_err(|e| {
            println!("{:?}", e);
            Status::InternalExceptionPagerWriteLock
        })?;
        let page = pager.access_page_read(&node.position);

        if page.is_ok() {
            func(&page?.data)
        } else {
            Err(page.unwrap_err())
        }
    }

    pub fn access_page_write<F>(&self, node: &BTreeNode, func: F) -> Result<(), Status>
    where
        F: FnOnce(&mut PageContainer) -> Result<(), Status>,
    {
        //TODO Optimize reading / writing to the pager by checking cache beforehand!?
        let mut pager = self.pager.write().map_err(|e| {
            eprintln!("{:?}", e);
            Status::InternalExceptionPagerWriteLock
        })?;
        let page: Result<&mut PageContainer, Status> = pager.access_page_write(&node.position);
        if page.is_ok() {
            func(page?)
        } else {
            Err(page.unwrap_err())
        }
    }
}

impl PagerCore {
    pub fn flush(&mut self) -> Result<(), Status> {
        let positions: Vec<Position> = self.cache.keys().cloned().collect();
        //TODO!!!!!!!!!!!!!: this will write lots of pages lots of times instead of once
        //would work
        //but Filter first TODO
        for position in positions {
            let page = self.cache[&position].clone();
            if Serializer::is_dirty(&page.data)? {
                self.write_page_to_disk(&page)?;
            }
        }

        Ok(())
    }

    pub fn init_from_file(file_path: &str) -> Result<PagerAccessor, Status> {
        let mut file = match OpenOptions::new().write(true).read(true).open(file_path) {
            Ok(f) => f,
            Err(e) => {
                return if e.kind() == ErrorKind::NotFound {
                    Err(Status::InternalExceptionFileNotFound)
                } else {
                    eprintln!("Error is {}", e.to_string());
                    Err(Status::InternalExceptionFileOpenFailed)
                }
            }
        };

        let mut next_page_index_bytes = [0u8; 2];
        let mut next_page_index;
        match file.read_exact(&mut next_page_index_bytes) {
            Err(_) => {
                next_page_index = 0;
            }
            Ok(()) => {
                next_page_index = u16::from_be_bytes(next_page_index_bytes);
            }
        }

        Ok(PagerAccessor::new(PagerCore {
            hash: generate_random_hash(16),
            cache: HashMap::new(),
            file,
            next_page_index,
        }))
    }

    pub fn invalidate_cache(&mut self) -> Status {
        self.cache.clear();
        InternalSuccess
    }
    /*
        //TODO this will be put somehwere else
        pub fn get_node_length(&self) -> i32 {
            ((2 * self.btree_width - 1) + (2 * self.btree_width - 1) * self.schema.tables[0].whole_row_length + self.btree_width * POSITION_SIZE + 2) as i32
        }
    */
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
                .get_mut(&position)
                .map(|pc| {
                    Serializer::set_is_dirty(&mut pc.data, true)?;
                    return Ok(pc);
                })
                .ok_or(Status::InternalExceptionCacheDenied)??)
        }
    }

    pub fn create_page(&mut self) -> Result<u16, Status> {
        todo!();
    }

    #[deprecated] //this is not wrong, I just don't see any use for this !?
    fn insert_page_at_position(&mut self, position: &Position, page_data: PageData) -> Status {
        let page = PageContainer {
            data: page_data,
            position: position.clone(),
            free_space: PAGE_SIZE,
            flag: 0,
        };
        self.cache.insert(position.clone(), page);
        InternalSuccess
    }

    fn read_page_from_disk(&mut self, position: &Position) -> Result<PageContainer, Status> {
        self.file
            .seek(SeekFrom::Start(position.get_file_position()))
            .map_err(|_| Status::InternalExceptionReadFailed)?;
        let mut meta_buffer = [0u8; 3];
        self.file
            .read_exact(&mut meta_buffer)
            .map_err(|_| Status::InternalExceptionReadFailed)?;
        let mut main_buffer = vec![0u8; PAGE_SIZE as usize];
        self.file.read_exact(&mut main_buffer).map_err(|e| {
            eprintln!("Cannot read File to this len: {}", e);
            Status::InternalExceptionReadFailed
        })?;
        Ok(PageContainer {
            data: main_buffer,
            position: position.clone(),
            free_space: u16::from_be_bytes([meta_buffer[0], meta_buffer[1]]),
            flag: meta_buffer[2],
        })
    }

    fn write_page_to_disk(&mut self, page: &PageContainer) -> Result<(), Status> {
        self.file
            .seek(SeekFrom::Start(page.position.get_file_position()))
            .map_err(|_| Status::InternalExceptionWriteFailed)?;
        assert!(page.data.len() as u16 <= PAGE_SIZE);
        self.file
            .write_all(&page.data)
            .map_err(|_| Status::InternalExceptionWriteFailed)?;
        Ok(())
    }
}
