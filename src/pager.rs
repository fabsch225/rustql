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
use std::{fmt, usize};
use std::fmt::{Debug, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, RwLock};

//in bytes
pub const PAGE_SIZE: usize = 4096; //16384
pub const PAGE_SIZE_WITH_META: usize = PAGE_SIZE + 3; //<2 for free space on page>, <1 for flag>
pub const NODE_METADATA_SIZE: usize = 2; //<number of keys> <flag> -- the number of keys is used to skip through the nodes on a page
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
// [Pages {Free-Space, Flag, PAGE_SIZE}] fyi (0,0) is an invalid position. the cells officially start at 1
//                     (of course, the location in the page starts at zero)

// Currently, there are no overflow pages. On a page, there can exist a fixed number of rows
// (they are assumed to have full length, although in practice they are still shifted around, because in the future there will be overflow pages)
// so, we calculate free-space as PAGE_SIZE - (Number of Rows * Row Length), and create a new page if there is not enough space

pub const PAGES_START_AT: usize = 2;

// Node Layout
// - 8 Bits: Number of Keys (n)
// - 8 Bits: Flag
// - n Keys: Each key is the length of the ID type read earlier.
// - n+1 Child/Page Pointers: Each pointer is [POSITION] bytes long.
// - Next There is the Data, According to the Schema Definition

// - so the size of a Node's Header (until the rows) is: n * (Number Of Keys) + (n + 1) * POSITION_SIZE

// ## Each Flag is a Byte =>
// Page-Flag Definition
// - Bit 0: Indicates if the page is dirty (needs to be written to disk)
// - Bit 2: Indicates if a page is deleted (marked for vacuum)
// - Bit 4: Lock

// Node-Flag Definition
// - Bit 1: Indicates if the Btree Node is a Leaf

// Key-Flag Definition
// - Bit 0: Indicates if the Key is marked for deletion
// (keys cannot be null)

#[repr(u8)]
pub enum PageFlag {
    Dirty = 0,
    Deleted = 2,
    Lock = 4,
}
#[repr(u8)]
pub enum NodeFlag {
    Leaf = 1,
}

#[repr(u8)]
pub enum KeyMeta {
    Tomb = 0
}
#[repr(u8)]
pub enum FieldMeta {
    Null = 0,
}

#[derive(PartialEq, Clone)]
pub enum Type {
    Null, //TODO remove this. this is not a type. each type can be null
    Integer,
    String,
    //Double, future featuref
    //Varchar, future feature
    Date,
    Boolean,
    //Blob    future feature, requires special treatment
    //Character future feature (?)
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
pub type PageData = [u8; PAGE_SIZE]; //[u8; PAGE_SIZE as usize]; this is possible eventually

#[derive(Clone, Debug)]
pub struct PageContainer {
    pub data: PageData,
    position: Position,
    pub free_space: usize,
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
    page: usize,
    cell: usize,
}

impl Position {
    pub fn new(page_index: usize, location_on_page: usize) -> Self {
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

    pub fn page(&self) -> usize {
        self.page
    }

    pub fn cell(&self) -> usize {
        self.cell
    }

    pub fn increase_cell(&self) -> Self {
            Position {
            page: self.page,
            cell: self.cell + 1,
        }
    }

    pub fn swap(&mut self, other: &mut Position) {
        std::mem::swap(self, other);
    }

    fn get_file_position(&self) -> u64 {
        ((self.page-1) * PAGE_SIZE_WITH_META + PAGES_START_AT) as u64
    }
}

pub type Row = Vec<u8>;

#[derive(Debug)]
pub struct PagerCore {
    pub hash: String,
    cache: HashMap<usize, PageContainer>,
    file: File,
    next_page_index: usize,
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

    pub(crate) fn get_next_page_index(&self) -> usize {
        self.access_pager_read(|p| p.next_page_index)
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
        F: FnOnce(&PageContainer) -> Result<T, Status>,
    {
        let mut pager = self.pager.write().map_err(|e| {
            println!("{:?}", e);
            Status::InternalExceptionPagerWriteLock
        })?;
        let page = pager.access_page_read(&node.position);

        if page.is_ok() {
            func(&page?)
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
        self.write_next_page_pos_to_disk()?;
        let page_indices: Vec<usize> = self.cache.keys().cloned().collect();
        for index in page_indices {
            let page_container = self.cache[&index].clone();
            if page_container.flag & 1 == 1 {
                self.write_page_to_disk(&page_container)?;
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
                next_page_index = u16::from_be_bytes(next_page_index_bytes) as usize;
            }
        }

        Ok(PagerAccessor::new(PagerCore {
            hash: generate_random_hash(16),
            cache: HashMap::new(),
            file,
            next_page_index,
        }))
    }

    pub fn write_next_page_pos_to_disk(&mut self) -> Result<(), Status> {
        self.file.seek(SeekFrom::Start(0)).map_err(|_| Status::InternalExceptionWriteFailed)?;
        let next_page_index_bytes = (self.next_page_index as u16).to_be_bytes();
        self.file.write_all(&next_page_index_bytes).map_err(|_| Status::InternalExceptionWriteFailed)?;
        Ok(())
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
        let miss = !self.cache.contains_key(&position.page());

        //TODO optimize this! lets hope the compiler does magic for now
        if miss {
            let page = self.read_page_from_disk(position);
            if page.is_ok() {
                let page = page?.clone();
                self.cache.insert(position.page(), page.clone());
                Ok(page)
            } else {
                Err(page.err().unwrap())
            }
        } else {
            Ok(self
                .cache
                .get(&position.page())
                .expect("This should not happen")
                .clone())
        }
    }

    //this should be the only function that writes to pages, so we can keep track of the dirty-flag
    pub fn access_page_write(&mut self, position: &Position) -> Result<&mut PageContainer, Status> {
        let miss = !self.cache.contains_key(&position.page());
        //TODO optimize this ?
        if miss {
            let page_container = self.read_page_from_disk(position);
            if page_container.is_ok() {
                let mut page = page_container?;
                //Serializer::set_is_dirty(&mut page_container.data, true)?;
                Serializer::write_byte_at_position(&mut page.flag, 0, true); //TODO Create a Method in Serializer for this, so less magic numbers
                self.cache.insert(position.page(), page);
                Ok(self
                    .cache
                    .get_mut(&position.page())
                    .expect("This should not happen, i checked just now"))
            } else {
                Err(page_container.err().unwrap())
            }
        } else {
            Ok(self
                .cache
                .get_mut(&position.page())
                .map(|pc| {
                    //Serializer::set_is_dirty(&mut pc.data, true)?;
                    Serializer::write_byte_at_position(&mut pc.flag, 0, true); //TODO Create a Method in Serializer for this, so less magic numbers
                    return Ok(pc);
                })
                .ok_or(Status::InternalExceptionCacheDenied)??)
        }
    }

    pub fn create_page(&mut self) -> Result<usize, Status> {
        let position = Position::new(self.next_page_index, 0);
        self.next_page_index += 1;
        let page_container = PageContainer {
            data: [0; PAGE_SIZE],
            position: position.clone(),
            free_space: PAGE_SIZE,
            flag: 0,
        };
        self.cache.insert(position.page(), page_container);
        Ok(position.page)
    }

    #[deprecated] //this is not wrong, I just don't see any use for this !?
    fn insert_page_at_position(&mut self, position: &Position, page_data: PageData) -> Status {
        let page = PageContainer {
            data: page_data,
            position: position.clone(),
            free_space: PAGE_SIZE,
            flag: 0,
        };
        self.cache.insert(position.page(), page);
        InternalSuccess
    }

    fn read_page_from_disk(&mut self, position: &Position) -> Result<PageContainer, Status> {
        self.file
            .seek(SeekFrom::Start(position.get_file_position()))
            .map_err(|_| {
                panic!("Cannot seek to this position");
                Status::InternalExceptionReadFailed
            })?;
        let mut meta_buffer = [0u8; 3];
        self.file
            .read_exact(&mut meta_buffer)
            .map_err(|_| {
                panic!("Cannot read File to this len (metadata len 3)");
                Status::InternalExceptionReadFailed
            })?;
        let mut main_buffer = [0u8; PAGE_SIZE as usize];
        self.file.read_exact(&mut main_buffer).map_err(|e| {
            panic!("Cannot read File to this len: {}", e);
            Status::InternalExceptionReadFailed
        })?;
        Ok(PageContainer {
            data: main_buffer,
            position: position.clone(),
            free_space: u16::from_be_bytes([meta_buffer[0], meta_buffer[1]]) as usize,
            flag: meta_buffer[2],
        })
    }

    fn write_page_to_disk(&mut self, page: &PageContainer) -> Result<(), Status> {
        self.file
            .seek(SeekFrom::Start(page.position.get_file_position()))
            .map_err(|_| Status::InternalExceptionWriteFailed)?;
        assert_eq!(page.data.len(), PAGE_SIZE);
        let mut meta_data = [0; 3];
        meta_data[0..2].copy_from_slice(&(page.free_space as u16).to_be_bytes());
        meta_data[2] = page.flag;
        self.file
            .write_all(&meta_data)
            .map_err(|_| Status::InternalExceptionWriteFailed)?;
        self.file
            .write_all(&page.data)
            .map_err(|_| Status::InternalExceptionWriteFailed)?;
        Ok(())
    }
}
