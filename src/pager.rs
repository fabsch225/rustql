use crate::btree::BTreeNode;
pub use crate::constants::{
    BOOLEAN_SIZE, DATE_SIZE, INTEGER_SIZE, INTEGER_SIZE_WITHOUT_FLAG, NODE_METADATA_SIZE,
    NULL_SIZE, PAGES_START_AT, PAGE_SIZE, PAGE_SIZE_WITH_META, POSITION_SIZE, ROW_NAME_SIZE,
    STRING_SIZE, TABLE_NAME_SIZE, TYPE_SIZE,
};
use crate::crypto::generate_random_hash;
use crate::serializer::Serializer;
use crate::debug::Status;
use crate::debug::Status::{
    ExceptionNoActiveTransaction, ExceptionTableLocked, ExceptionTransactionAlreadyActive,
    InternalExceptionInvalidColCount, InternalExceptionInvalidSchema,
    InternalExceptionPagerMismatch, InternalSuccess,
};
use std::cmp::PartialEq;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::sync::{Arc, RwLock};
use std::thread::ThreadId;
use std::{fmt, usize};

#[derive(PartialEq, Clone)]
pub enum Type {
    Null, //TODO remove this. this is not a type. each type can be null
    Integer,
    String,
    Varchar(usize),
    //Double, future feature
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
            Type::Varchar(max) => write!(f, "Varchar({})", max),
            Type::Date => write!(f, "Date"),
            Type::Boolean => write!(f, "Boolean"),
        }
        .expect("Wierd Error");
        Ok(())
    }
}

impl Type {
    pub fn to_sql(&self) -> String {
        match self {
            Type::Null => "Null".to_string(),
            Type::Integer => "Integer".to_string(),
            Type::String => "String".to_string(),
            Type::Varchar(len) => format!("Varchar({})", len),
            Type::Date => "Date".to_string(),
            Type::Boolean => "Boolean".to_string(),
        }
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
    //pub transaction_shards: HashMap<TransactionId, (PageData, usize)>
    //if i do this, i should put data and free_space into a struct
}

pub type TableName = Vec<u8>;

//first byte specify the data type
//rest must be the length of the size of the type
pub type Key = Vec<u8>;

pub type Flag = u8;

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
        self.page == 0
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
        //the indices are shifted by 1, so (0,0) serves as a NULL value
        ((self.page - 1) * PAGE_SIZE_WITH_META + PAGES_START_AT) as u64
    }
}

pub type Row = Vec<u8>;

pub type TransactionId = u64;

#[derive(Clone, Debug)]
struct TransactionState {
    page_overrides: HashMap<usize, PageContainer>,
    locked_tables: HashSet<String>,
}

#[derive(Clone, Debug)]
struct TableLock {
    holder_tx_id: TransactionId,
}

#[derive(Debug)]
pub struct PagerCore {
    pub hash: String,
    cache: HashMap<usize, PageContainer>,
    file: File,
    next_page_index: usize,
    transactions: HashMap<TransactionId, TransactionState>,
    current_transaction_ids: HashMap<ThreadId, TransactionId>,
    next_transaction_id: TransactionId,
    table_locks: HashMap<String, TableLock>,
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
        self.access_pager_read(|p| p.get_visible_next_page_index())
    }

    pub fn begin_transaction(&self) -> Result<(), Status> {
        self.access_pager_write(|p| p.begin_transaction())
    }

    pub fn begin_transaction_with_id(&self) -> Result<TransactionId, Status> {
        self.access_pager_write(|p| p.begin_transaction_with_id())
    }

    pub fn set_current_transaction(&self, tx_id: Option<TransactionId>) -> Result<(), Status> {
        self.access_pager_write(|p| p.set_current_transaction(tx_id))
    }

    pub fn commit_transaction(&self) -> Result<(), Status> {
        self.access_pager_write(|p| p.commit_transaction())
    }

    pub fn commit_transaction_by_id(&self, tx_id: TransactionId) -> Result<(), Status> {
        self.access_pager_write(|p| p.commit_transaction_by_id(tx_id))
    }

    pub fn rollback_transaction(&self) -> Result<(), Status> {
        self.access_pager_write(|p| p.rollback_transaction())
    }

    pub fn rollback_transaction_by_id(&self, tx_id: TransactionId) -> Result<(), Status> {
        self.access_pager_write(|p| p.rollback_transaction_by_id(tx_id))
    }

    pub fn lock_table_for_transaction(&self, table_name: &str) -> Result<(), Status> {
        self.access_pager_write(|p| p.lock_table_for_current_transaction(table_name))
    }

    pub fn lock_table_for_transaction_id(
        &self,
        tx_id: TransactionId,
        table_name: &str,
    ) -> Result<(), Status> {
        self.access_pager_write(|p| p.lock_table_for_transaction_id(tx_id, table_name))
    }

    pub fn is_transaction_active(&self) -> bool {
        self.access_pager_read(|p| p.current_transaction_id().is_some())
    }

    pub fn current_transaction_id(&self) -> Option<TransactionId> {
        self.access_pager_read(|p| p.current_transaction_id())
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
    //Implement Pager::try_read_page_from_cache -> Result<PageContainer, Status>, and if there is a cache miss, return a Status::CacheMiss,
    //    then we can request write access here and try to load from disk.
    // (now we have to account for the possibility of loading a page from disk, which requires mutating the filestream)
    pub fn access_page_read<F, T>(&self, node: &BTreeNode, func: F) -> Result<T, Status>
    where
        F: FnOnce(&PageContainer) -> Result<T, Status>,
    {
        // Fast path
        {
            let pager = self.pager.read().map_err(|e| {
                println!("{:?}", e);
                Status::InternalExceptionPagerWriteLock
            })?;

            if let Some(page) = pager.try_read_page_from_cache(&node.position) {
                return func(&page);
            }
        }

        // Slow path: cache miss requires loading from disk and mutating cache
        let mut pager = self.pager.write().map_err(|e| {
            println!("{:?}", e);
            Status::InternalExceptionPagerWriteLock
        })?;
        let page = pager.access_page_read(&node.position)?;
        func(&page)
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
    fn current_thread_id() -> ThreadId {
        std::thread::current().id()
    }

    fn current_transaction_id(&self) -> Option<TransactionId> {
        self.current_transaction_ids
            .get(&Self::current_thread_id())
            .copied()
    }

    fn get_visible_next_page_index(&self) -> usize {
        self.next_page_index
    }

    pub fn begin_transaction(&mut self) -> Result<(), Status> {
        if self.current_transaction_id().is_some() {
            return Err(ExceptionTransactionAlreadyActive);
        }

        let tx_id = self.begin_transaction_with_id()?;
        self.current_transaction_ids
            .insert(Self::current_thread_id(), tx_id);
        Ok(())
    }

    pub fn begin_transaction_with_id(&mut self) -> Result<TransactionId, Status> {
        let tx_id = self.next_transaction_id;
        self.next_transaction_id += 1;

        self.transactions.insert(
            tx_id,
            TransactionState {
                page_overrides: HashMap::new(),
                locked_tables: HashSet::new(),
            },
        );
        Ok(tx_id)
    }

    pub fn set_current_transaction(&mut self, tx_id: Option<TransactionId>) -> Result<(), Status> {
        if let Some(id) = tx_id
            && !self.transactions.contains_key(&id)
        {
            return Err(ExceptionNoActiveTransaction);
        }
        let thread_id = Self::current_thread_id();
        if let Some(id) = tx_id {
            self.current_transaction_ids.insert(thread_id, id);
        } else {
            self.current_transaction_ids.remove(&thread_id);
        }
        Ok(())
    }

    pub fn commit_transaction(&mut self) -> Result<(), Status> {
        let tx_id = self.current_transaction_id().ok_or(ExceptionNoActiveTransaction)?;
        self.commit_transaction_by_id(tx_id)
    }

    pub fn commit_transaction_by_id(&mut self, tx_id: TransactionId) -> Result<(), Status> {
        let tx = self
            .transactions
            .remove(&tx_id)
            .ok_or(ExceptionNoActiveTransaction)?;

        for (page_idx, page) in tx.page_overrides {
            self.cache.insert(page_idx, page);
        }

        for table in tx.locked_tables {
            if let Some(lock) = self.table_locks.get(&table)
                && lock.holder_tx_id == tx_id
            {
                self.table_locks.remove(&table);
            }
        }

        self.current_transaction_ids.retain(|_, bound| *bound != tx_id);
        Ok(())
    }

    pub fn rollback_transaction(&mut self) -> Result<(), Status> {
        let tx_id = self.current_transaction_id().ok_or(ExceptionNoActiveTransaction)?;
        self.rollback_transaction_by_id(tx_id)
    }

    pub fn rollback_transaction_by_id(&mut self, tx_id: TransactionId) -> Result<(), Status> {
        let tx = self
            .transactions
            .remove(&tx_id)
            .ok_or(ExceptionNoActiveTransaction)?;

        for table in tx.locked_tables {
            if let Some(lock) = self.table_locks.get(&table)
                && lock.holder_tx_id == tx_id
            {
                self.table_locks.remove(&table);
            }
        }

        self.current_transaction_ids.retain(|_, bound| *bound != tx_id);
        Ok(())
    }

    pub fn lock_table_for_current_transaction(&mut self, table_name: &str) -> Result<(), Status> {
        let Some(tx_id) = self.current_transaction_id() else {
            return Ok(());
        };

        self.lock_table_for_transaction_id(tx_id, table_name)
    }

    pub fn lock_table_for_transaction_id(
        &mut self,
        tx_id: TransactionId,
        table_name: &str,
    ) -> Result<(), Status> {
        if !self.transactions.contains_key(&tx_id) {
            return Err(ExceptionNoActiveTransaction);
        }

        if let Some(lock) = self.table_locks.get(table_name) {
            if lock.holder_tx_id != tx_id {
                return Err(ExceptionTableLocked);
            }
            return Ok(());
        }

        self.table_locks.insert(
            table_name.to_string(),
            TableLock {
                holder_tx_id: tx_id,
            },
        );

        if let Some(tx) = self.transactions.get_mut(&tx_id) {
            tx.locked_tables.insert(table_name.to_string());
        }

        Ok(())
    }

    fn read_exact_at(file: &File, mut offset: u64, mut buf: &mut [u8]) -> Result<(), Status> {
        while !buf.is_empty() {
            let bytes = file
                .read_at(buf, offset)
                .map_err(|_| Status::InternalExceptionReadFailed)?;
            if bytes == 0 {
                return Err(Status::InternalExceptionReadFailed);
            }
            offset += bytes as u64;
            let (_, rest) = buf.split_at_mut(bytes);
            buf = rest;
        }
        Ok(())
    }

    fn write_all_at(file: &File, mut offset: u64, mut buf: &[u8]) -> Result<(), Status> {
        while !buf.is_empty() {
            let bytes = file
                .write_at(buf, offset)
                .map_err(|_| Status::InternalExceptionWriteFailed)?;
            if bytes == 0 {
                return Err(Status::InternalExceptionWriteFailed);
            }
            offset += bytes as u64;
            buf = &buf[bytes..];
        }
        Ok(())
    }

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
                };
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
            transactions: HashMap::new(),
            current_transaction_ids: HashMap::new(),
            next_transaction_id: 1,
            table_locks: HashMap::new(),
        }))
    }

    pub fn write_next_page_pos_to_disk(&mut self) -> Result<(), Status> {
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|_| Status::InternalExceptionWriteFailed)?;
        let next_page_index_bytes = (self.next_page_index as u16).to_be_bytes();
        self.file
            .write_all(&next_page_index_bytes)
            .map_err(|_| Status::InternalExceptionWriteFailed)?;
        Ok(())
    }

    pub fn invalidate_cache(&mut self) -> Status {
        self.cache.clear();
        InternalSuccess
    }

    pub fn access_page_read(&mut self, position: &Position) -> Result<PageContainer, Status> {
        if let Some(tx_id) = self.current_transaction_id()
            && let Some(tx_page) = self
                .transactions
                .get(&tx_id)
                .and_then(|tx| tx.page_overrides.get(&position.page()))
                .cloned()
        {
            return Ok(tx_page);
        }

        let miss = !self.cache.contains_key(&position.page());

        //TODO optimize this!
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

    pub fn try_read_page_from_cache(&self, position: &Position) -> Option<PageContainer> {
        if let Some(tx_id) = self.current_transaction_id()
            && let Some(tx_page) = self
                .transactions
                .get(&tx_id)
                .and_then(|tx| tx.page_overrides.get(&position.page()))
                .cloned()
        {
            return Some(tx_page);
        }
        self.cache.get(&position.page()).cloned()
    }

    //this should be the only function that writes to pages, so we can keep track of the dirty-flag
    pub fn access_page_write(&mut self, position: &Position) -> Result<&mut PageContainer, Status> {
        if let Some(tx_id) = self.current_transaction_id() {
            if !self
                .transactions
                .get(&tx_id)
                .map(|tx| tx.page_overrides.contains_key(&position.page()))
                .unwrap_or(false)
            {
                let mut page = if let Some(cached) = self.cache.get(&position.page()).cloned() {
                    cached
                } else {
                    let page_from_disk = self.read_page_from_disk(position)?;
                    self.cache.insert(position.page(), page_from_disk.clone());
                    page_from_disk
                };

                Serializer::write_byte_at_position(&mut page.flag, 0, true);

                if let Some(tx) = self.transactions.get_mut(&tx_id) {
                    tx.page_overrides.insert(position.page(), page);
                }
            }

            return self
                .transactions
                .get_mut(&tx_id)
                .and_then(|tx| tx.page_overrides.get_mut(&position.page()))
                .ok_or(Status::InternalExceptionCacheDenied);
        }

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
        if let Some(tx_id) = self.current_transaction_id() {
            let page_index = self.next_page_index;
            self.next_page_index += 1;

            let position = Position::new(page_index, 0);
            let page_container = PageContainer {
                data: [0; PAGE_SIZE],
                position: position.clone(),
                free_space: PAGE_SIZE,
                flag: 0,
            };

            if let Some(tx) = self.transactions.get_mut(&tx_id) {
                tx.page_overrides.insert(position.page(), page_container);
            }

            return Ok(position.page());
        }

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

    fn read_page_from_disk(&self, position: &Position) -> Result<PageContainer, Status> {
        let offset = position.get_file_position();
        let mut meta_buffer = [0u8; 3];
        Self::read_exact_at(&self.file, offset, &mut meta_buffer)?;
        let mut main_buffer = [0u8; PAGE_SIZE as usize];
        Self::read_exact_at(&self.file, offset + 3, &mut main_buffer)?;
        Ok(PageContainer {
            data: main_buffer,
            position: position.clone(),
            free_space: u16::from_be_bytes([meta_buffer[0], meta_buffer[1]]) as usize,
            flag: meta_buffer[2],
        })
    }

    fn write_page_to_disk(&self, page: &PageContainer) -> Result<(), Status> {
        let offset = page.position.get_file_position();
        assert_eq!(page.data.len(), PAGE_SIZE);
        let mut meta_data = [0; 3];
        meta_data[0..2].copy_from_slice(&(page.free_space as u16).to_be_bytes());
        meta_data[2] = page.flag;
        Self::write_all_at(&self.file, offset, &meta_data)?;
        Self::write_all_at(&self.file, offset + 3, &page.data)?;
        Ok(())
    }
}
