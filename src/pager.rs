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
use std::io::{ErrorKind, Read};
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
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
    pub flag: Flag
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
    active: bool,
}

#[derive(Clone, Debug)]
struct TableLock {
    holder_tx_id: TransactionId,
}

#[derive(Debug)]
pub struct PagerCore {
    pub hash: String,
    commit_gate: RwLock<()>,
    cache: RwLock<HashMap<usize, PageContainer>>,
    file: Arc<File>,
    next_page_index: AtomicUsize,
    transactions: RwLock<HashMap<TransactionId, Arc<RwLock<TransactionState>>>>,
    current_transaction_ids: RwLock<HashMap<ThreadId, TransactionId>>,
    next_transaction_id: AtomicU64,
    table_locks: RwLock<HashMap<String, TableLock>>,
    io_write_lock: Mutex<()>,
}

#[derive(Clone)]
pub struct PagerAccessor {
    pager: Arc<PagerCore>,
    hash: String, //eventually remove this
                  //one could add a Arc<Priority Queue> here, for the lambdas
}

impl Debug for PagerAccessor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[PagerAccessor]")
    }
}

impl PagerAccessor {
    pub fn new(pager: PagerCore) -> Self {
        let h = pager.hash.clone();
        Self {
            pager: Arc::new(pager),
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

    pub fn verify(&self, pager: &PagerCore) -> bool {
        pager.hash == self.hash
    }

    pub fn access_pager_read<F, T>(&self, func: F) -> T
    where
        F: FnOnce(&PagerCore) -> T,
    {
        func(&self.pager)
    }

    pub fn access_pager_write<F, T>(&self, func: F) -> T
    where
        F: FnOnce(&PagerCore) -> T,
    {
        func(&self.pager)
    }

    pub fn access_page_read<F, T>(&self, node: &BTreeNode, func: F) -> Result<T, Status>
    where
        F: FnOnce(&PageContainer) -> Result<T, Status>,
    {
        if let Some(page) = self.pager.try_read_page_from_cache(&node.position) {
            return func(&page);
        }
        let page = self.pager.access_page_read(&node.position)?;
        func(&page)
    }

    pub fn access_page_write<F>(&self, node: &BTreeNode, func: F) -> Result<(), Status>
    where
        F: FnOnce(&mut PageContainer) -> Result<(), Status>,
    {
        self.pager.with_page_write(&node.position, func)
    }
}

impl PagerCore {
    // Global lock order (must be preserved whenever more than one lock is acquired):
    // commit_gate -> tx handle (from transactions map) -> tx lock -> cache -> table_locks -> current_transaction_ids
    fn current_thread_id() -> ThreadId {
        std::thread::current().id()
    }

    fn current_transaction_id(&self) -> Option<TransactionId> {
        self.current_transaction_ids
            .read()
            .ok()
            .and_then(|map| map.get(&Self::current_thread_id()).copied())
    }

    fn get_visible_next_page_index(&self) -> usize {
        self.next_page_index.load(Ordering::SeqCst)
    }

    pub fn begin_transaction(&self) -> Result<(), Status> {
        if self.current_transaction_id().is_some() {
            return Err(ExceptionTransactionAlreadyActive);
        }

        let tx_id = self.begin_transaction_with_id()?;
        self.current_transaction_ids
            .write()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?
            .insert(Self::current_thread_id(), tx_id);
        Ok(())
    }

    pub fn begin_transaction_with_id(&self) -> Result<TransactionId, Status> {
        let tx_id = self.next_transaction_id.fetch_add(1, Ordering::SeqCst);

        self.transactions
            .write()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?
            .insert(
            tx_id,
            Arc::new(RwLock::new(TransactionState {
                page_overrides: HashMap::new(),
                locked_tables: HashSet::new(),
                active: true,
            })),
        );
        Ok(tx_id)
    }

    pub fn set_current_transaction(&self, tx_id: Option<TransactionId>) -> Result<(), Status> {
        if let Some(id) = tx_id {
            let tx_handle = self
                .transactions
                .read()
                .map_err(|_| Status::InternalExceptionPagerWriteLock)?
                .get(&id)
                .cloned()
                .ok_or(ExceptionNoActiveTransaction)?;
            let tx = tx_handle
                .read()
                .map_err(|_| Status::InternalExceptionPagerWriteLock)?;
            if !tx.active {
                return Err(ExceptionNoActiveTransaction);
            }
        }
        let thread_id = Self::current_thread_id();
        let mut current_tx = self
            .current_transaction_ids
            .write()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?;
        if let Some(id) = tx_id {
            current_tx.insert(thread_id, id);
        } else {
            current_tx.remove(&thread_id);
        }
        Ok(())
    }

    pub fn commit_transaction(&self) -> Result<(), Status> {
        let tx_id = self.current_transaction_id().ok_or(ExceptionNoActiveTransaction)?;
        self.commit_transaction_by_id(tx_id)
    }

    pub fn commit_transaction_by_id(&self, tx_id: TransactionId) -> Result<(), Status> {
        let _commit_guard = self
            .commit_gate
            .write()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?;

        let tx_handle = self
            .transactions
            .read()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?
            .get(&tx_id)
            .cloned()
            .ok_or(ExceptionNoActiveTransaction)?;

        // Lock order: tx -> cache -> table_locks -> current_transaction_ids
        let mut tx = tx_handle
            .write()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?
            ;
        if !tx.active {
            return Err(ExceptionNoActiveTransaction);
        }

        let mut cache = self
            .cache
            .write()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?;
        let mut table_locks = self
            .table_locks
            .write()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?;
        let mut current_transaction_ids = self
            .current_transaction_ids
            .write()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?;

        let page_overrides = std::mem::take(&mut tx.page_overrides);
        let locked_tables = std::mem::take(&mut tx.locked_tables);
        tx.active = false;

        for (page_idx, page) in page_overrides {
            cache.insert(page_idx, page);
        }

        for table in locked_tables {
            if let Some(lock) = table_locks.get(&table)
                && lock.holder_tx_id == tx_id
            {
                table_locks.remove(&table);
            }
        }

        current_transaction_ids.retain(|_, bound| *bound != tx_id);

        // Best-effort cleanup; semantic correctness does not depend on physical removal.
        if let Ok(mut txs) = self.transactions.write() {
            txs.remove(&tx_id);
        }

        Ok(())
    }

    pub fn rollback_transaction(&self) -> Result<(), Status> {
        let tx_id = self.current_transaction_id().ok_or(ExceptionNoActiveTransaction)?;
        self.rollback_transaction_by_id(tx_id)
    }

    pub fn rollback_transaction_by_id(&self, tx_id: TransactionId) -> Result<(), Status> {
        let tx_handle = self
            .transactions
            .read()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?
            .get(&tx_id)
            .cloned()
            .ok_or(ExceptionNoActiveTransaction)?;

        // Lock order: tx -> table_locks -> current_transaction_ids
        let mut tx = tx_handle
            .write()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?;
        if !tx.active {
            return Err(ExceptionNoActiveTransaction);
        }

        let mut table_locks = self
            .table_locks
            .write()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?;
        let mut current_transaction_ids = self
            .current_transaction_ids
            .write()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?;

        let locked_tables = std::mem::take(&mut tx.locked_tables);
        tx.active = false;

        for table in locked_tables {
            if let Some(lock) = table_locks.get(&table)
                && lock.holder_tx_id == tx_id
            {
                table_locks.remove(&table);
            }
        }

        current_transaction_ids.retain(|_, bound| *bound != tx_id);

        // Best-effort cleanup; semantic correctness does not depend on physical removal.
        if let Ok(mut txs) = self.transactions.write() {
            txs.remove(&tx_id);
        }

        Ok(())
    }

    pub fn lock_table_for_current_transaction(&self, table_name: &str) -> Result<(), Status> {
        let Some(tx_id) = self.current_transaction_id() else {
            return Ok(());
        };

        self.lock_table_for_transaction_id(tx_id, table_name)
    }

    pub fn lock_table_for_transaction_id(
        &self,
        tx_id: TransactionId,
        table_name: &str,
    ) -> Result<(), Status> {
        let tx_handle = self
            .transactions
            .read()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?
            .get(&tx_id)
            .cloned()
            .ok_or(ExceptionNoActiveTransaction)?;

        // Lock order: tx -> table_locks
        let mut tx = tx_handle
            .write()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?;
        if !tx.active {
            return Err(ExceptionNoActiveTransaction);
        }

        if tx.locked_tables.contains(table_name) {
            return Ok(());
        }

        let mut table_locks = self
            .table_locks
            .write()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?;
        if let Some(lock) = table_locks.get(table_name) {
            if lock.holder_tx_id != tx_id {
                return Err(ExceptionTableLocked);
            }
        } else {
            table_locks.insert(
                table_name.to_string(),
                TableLock {
                    holder_tx_id: tx_id,
                },
            );
        }

        tx.locked_tables.insert(table_name.to_string());

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

    pub fn flush(&self) -> Result<(), Status> {
        self.write_next_page_pos_to_disk()?;
        let pages_to_write: Vec<PageContainer> = self
            .cache
            .read()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?
            .values()
            .filter(|page| page.flag & 1 == 1)
            .cloned()
            .collect();

        for page_container in pages_to_write {
            self.write_page_to_disk(&page_container)?;
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
            commit_gate: RwLock::new(()),
            cache: RwLock::new(HashMap::new()),
            file: Arc::new(file),
            next_page_index: AtomicUsize::new(next_page_index),
            transactions: RwLock::new(HashMap::new()),
            current_transaction_ids: RwLock::new(HashMap::new()),
            next_transaction_id: AtomicU64::new(1),
            table_locks: RwLock::new(HashMap::new()),
            io_write_lock: Mutex::new(()),
        }))
    }

    pub fn write_next_page_pos_to_disk(&self) -> Result<(), Status> {
        let _guard = self
            .io_write_lock
            .lock()
            .map_err(|_| Status::InternalExceptionWriteFailed)?;
        let next_page_index_bytes =
            (self.next_page_index.load(Ordering::SeqCst) as u16).to_be_bytes();
        Self::write_all_at(&self.file, 0, &next_page_index_bytes)?;
        Ok(())
    }

    pub fn invalidate_cache(&self) -> Status {
        if let Ok(mut cache) = self.cache.write() {
            cache.clear();
        }
        InternalSuccess
    }

    pub fn access_page_read(&self, position: &Position) -> Result<PageContainer, Status> {
        let _commit_guard = self
            .commit_gate
            .read()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?;

        if let Some(tx_id) = self.current_transaction_id()
            && let Some(tx_handle) = self
                .transactions
                .read()
                .map_err(|_| Status::InternalExceptionPagerWriteLock)?
                .get(&tx_id)
                .cloned()
        {
            let tx = tx_handle
                .read()
                .map_err(|_| Status::InternalExceptionPagerWriteLock)?;
            if !tx.active {
                return Err(ExceptionNoActiveTransaction);
            }
            if let Some(tx_page) = tx.page_overrides.get(&position.page()).cloned() {
                return Ok(tx_page);
            }
        }

        if let Some(cached) = self
            .cache
            .read()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?
            .get(&position.page())
            .cloned()
        {
            return Ok(cached);
        }

        let page = self.read_page_from_disk(position)?;
        self.cache
            .write()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?
            .insert(position.page(), page.clone());
        Ok(page)
    }

    pub fn try_read_page_from_cache(&self, position: &Position) -> Option<PageContainer> {
        let _commit_guard = self.commit_gate.read().ok()?;

        if let Some(tx_id) = self.current_transaction_id()
            && let Some(tx_handle) = self
                .transactions
                .read()
                .ok()?
                .get(&tx_id)
                .cloned()
        {
            let tx = tx_handle.read().ok()?;
            if tx.active {
                if let Some(tx_page) = tx.page_overrides.get(&position.page()).cloned() {
                    return Some(tx_page);
                }
            }
        }
        self.cache.read().ok()?.get(&position.page()).cloned()
    }

    //this should be the only function that writes to pages, so we can keep track of the dirty-flag
    pub fn with_page_write<F>(&self, position: &Position, func: F) -> Result<(), Status>
    where
        F: FnOnce(&mut PageContainer) -> Result<(), Status>,
    {
        if let Some(tx_id) = self.current_transaction_id() {
            let tx_handle = self
                .transactions
                .read()
                .map_err(|_| Status::InternalExceptionPagerWriteLock)?
                .get(&tx_id)
                .cloned()
                .ok_or(ExceptionNoActiveTransaction)?;

            let mut tx = tx_handle
                .write()
                .map_err(|_| Status::InternalExceptionPagerWriteLock)?;

            if !tx.active {
                return Err(ExceptionNoActiveTransaction);
            }

            if !tx.page_overrides.contains_key(&position.page()) {
                let mut page = if let Some(cached) = self
                    .cache
                    .read()
                    .map_err(|_| Status::InternalExceptionPagerWriteLock)?
                    .get(&position.page())
                    .cloned()
                {
                    cached
                } else {
                    let page_from_disk = self.read_page_from_disk(position)?;
                    self.cache
                        .write()
                        .map_err(|_| Status::InternalExceptionPagerWriteLock)?
                        .insert(position.page(), page_from_disk.clone());
                    page_from_disk
                };

                Serializer::write_byte_at_position(&mut page.flag, 0, true);
                tx.page_overrides.insert(position.page(), page);
            }

            let page = tx
                .page_overrides
                .get_mut(&position.page())
                .ok_or(Status::InternalExceptionCacheDenied)?;
            return func(page);
        }

        let mut cache = self
            .cache
            .write()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?;

        if !cache.contains_key(&position.page()) {
            let page = self.read_page_from_disk(position)?;
            cache.insert(position.page(), page);
        }

        let page = cache
            .get_mut(&position.page())
            .ok_or(Status::InternalExceptionCacheDenied)?;
        Serializer::write_byte_at_position(&mut page.flag, 0, true);
        func(page)
    }

    pub fn create_page(&self) -> Result<usize, Status> {
        // optimize: add this to freelist on rollback (journaling) 
        let page_index = self.next_page_index.fetch_add(1, Ordering::SeqCst);

        if let Some(tx_id) = self.current_transaction_id() {
            let position = Position::new(page_index, 0);
            let page_container = PageContainer {
                data: [0; PAGE_SIZE],
                position: position.clone(),
                free_space: PAGE_SIZE,
                flag: 0,
            };

            let tx_handle = self
                .transactions
                .read()
                .map_err(|_| Status::InternalExceptionPagerWriteLock)?
                .get(&tx_id)
                .cloned()
                .ok_or(ExceptionNoActiveTransaction)?;

            if let Ok(mut tx) = tx_handle.write() {
                if !tx.active {
                    return Err(ExceptionNoActiveTransaction);
                }
                tx.page_overrides.insert(position.page(), page_container);
            }

            return Ok(position.page());
        }

        let position = Position::new(page_index, 0);
        let page_container = PageContainer {
            data: [0; PAGE_SIZE],
            position: position.clone(),
            free_space: PAGE_SIZE,
            flag: 0,
        };
        self.cache
            .write()
            .map_err(|_| Status::InternalExceptionPagerWriteLock)?
            .insert(position.page(), page_container);
        Ok(position.page)
    }

    #[deprecated] //this is not wrong, I just don't see any use for this !?
    fn insert_page_at_position(&self, position: &Position, page_data: PageData) -> Status {
        let page = PageContainer {
            data: page_data,
            position: position.clone(),
            free_space: PAGE_SIZE,
            flag: 0,
        };
        if let Ok(mut cache) = self.cache.write() {
            cache.insert(position.page(), page);
        }
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
        let _guard = self
            .io_write_lock
            .lock()
            .map_err(|_| Status::InternalExceptionWriteFailed)?;
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
