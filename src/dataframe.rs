use crate::btree::Btree;
use crate::cursor::BTreeCursor;
use crate::pager::{Position, Row, Type};
use crate::parser::ParsedSetOperator;
use crate::planner::SqlStatementComparisonOperator::{Equal, Greater, GreaterOrEqual};
use crate::planner::{SqlConditionOpCode, SqlStatementComparisonOperator};
use crate::schema::{Field, TableSchema};
use crate::serializer::Serializer;
use crate::status::Status;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::hash::{DefaultHasher, Hash, Hasher};

#[derive(Debug, Clone)]
pub struct DataFrame {
    pub identifier: String,
    pub header: Vec<Field>,
    pub(crate) row_source: Source,
}

impl DataFrame {
    pub fn from_memory(identifier: String, header: Vec<Field>, data: Vec<Vec<u8>>) -> DataFrame {
        DataFrame {
            identifier,
            header,
            row_source: Source::Memory(MemorySource { data, idx: 0 }),
        }
    }

    pub fn from_table(
        identifier: String,
        header: Vec<Field>,
        btree: Btree,
        operation: SqlConditionOpCode,
        conditions: Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
    ) -> DataFrame {
        let schema = btree.table_schema.clone();
        DataFrame {
            identifier,
            header,
            row_source: Source::BTree(BTreeScanSource::new(btree, schema, operation, conditions)),
        }
    }

    //TODO fix when there is a varchar--this is wrong, a string longer than 256 would be cut off
    pub fn msg(message: &str) -> DataFrame {
        DataFrame {
            identifier: "Message to the User".to_string(),
            header: vec![Field {
                field_type: Type::String,
                name: "Message".to_string(),
                table_name: "".to_string(),
            }],
            row_source: Source::Memory(MemorySource {
                data: vec![Serializer::parse_string(message).to_vec()],
                idx: 0,
            }),
        }
    }

    pub fn fetch(mut self) -> Result<Vec<Row>, Status> {
        self.row_source.reset()?;
        let mut rows = vec![];
        while let Some(row) = self.row_source.next()? {
            rows.push(row);
        }
        Ok(rows)
    }

    pub fn filter(self, conditions: Vec<(SqlStatementComparisonOperator, Vec<u8>)>) -> DataFrame {
        let header = self.header.clone();

        DataFrame {
            identifier: format!("Filter({})", self.identifier),
            header: header.clone(),
            row_source: Source::Filter(FilterSource::new(
                Box::new(self.row_source),
                conditions,
                header,
            )),
        }
    }

    pub fn project(self, new_header: Vec<Field>, mapping_indices: Vec<usize>) -> DataFrame {
        let source_header = self.header.clone();

        DataFrame {
            identifier: format!("Project({})", self.identifier),
            header: new_header,
            row_source: Source::Project(ProjectSource::new(
                Box::new(self.row_source),
                mapping_indices,
                source_header,
            )),
        }
    }

    pub fn join(
        self,
        other: DataFrame,
        l_idx: usize,
        r_idx: usize,
        strategy: JoinStrategy,
    ) -> Result<DataFrame, Status> {
        let mut result_header = self.header.clone();
        result_header.extend(other.header.clone());

        let new_source = match strategy {
            JoinStrategy::NestedLoop => Source::NestedLoopJoin(NestedLoopJoinSource::new(
                Box::new(self.row_source),
                Box::new(other.row_source),
                self.header,
                other.header,
                vec![(l_idx, r_idx)],
            )),
            JoinStrategy::Hash => Source::HashJoin(HashJoinSource::new(
                Box::new(self.row_source),
                Box::new(other.row_source),
                l_idx,
                r_idx,
                self.header,
                other.header,
            )),
            JoinStrategy::SortMerge => Source::SortMergeJoin(SortMergeJoinSource::new(
                Box::new(self.row_source),
                Box::new(other.row_source),
                l_idx,
                r_idx,
                self.header,
                other.header,
            )),
        };

        Ok(DataFrame {
            identifier: format!("Join({}, {})", self.identifier, other.identifier),
            header: result_header,
            row_source: new_source,
        })
    }

    pub fn set_operation(
        self,
        other: DataFrame,
        op: ParsedSetOperator,
        strategy: SetOpStrategy,
    ) -> Result<DataFrame, Status> {
        if self.header.len() != other.header.len() {
            return Err(Status::DataFrameJoinError);
        }

        let new_source =
            SetOperationSource::new(Box::new(self.row_source), other.row_source, op, strategy);

        Ok(DataFrame {
            identifier: "SetOpResult".to_string(),
            header: self.header,
            row_source: Source::SetOp(new_source),
        })
    }
}

impl Display for DataFrame {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for field in &self.header {
            write!(f, "{}\t", field.name)?;
        }
        writeln!(f)?;

        let mut source = self.row_source.clone();

        if source.reset().is_err() {
            return write!(f, "<Error resetting source>");
        }

        loop {
            match source.next() {
                Ok(Some(row)) => {
                    let mut position = 0;
                    for field in &self.header {
                        let field_type = &field.field_type;
                        let field_len = Serializer::get_size_of_type(field_type).map_err(|_| fmt::Error)?;

                        let field_value = &row[position..position + field_len];
                        let formatted_value =
                            Serializer::format_field(&field_value.to_vec(), field_type).map_err(|_| fmt::Error)?;

                        write!(f, "{}\t", formatted_value)?;
                        position += field_len;
                    }
                    writeln!(f)?;
                }
                Ok(None) => break,
                Err(_) => return write!(f, "<Error fetching row>"),
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum Source {
    Memory(MemorySource),
    BTree(BTreeScanSource),
    NestedLoopJoin(NestedLoopJoinSource),
    HashJoin(HashJoinSource),
    SortMergeJoin(SortMergeJoinSource),
    SetOp(SetOperationSource),
    Filter(FilterSource),
    Project(ProjectSource),
}

pub trait RowSource {
    fn next(&mut self) -> Result<Option<Vec<u8>>, Status>;
    fn reset(&mut self) -> Result<(), Status>;
}

impl RowSource for Source {
    fn next(&mut self) -> Result<Option<Vec<u8>>, Status> {
        match self {
            Source::Memory(s) => s.next(),
            Source::BTree(s) => s.next(),
            Source::NestedLoopJoin(s) => s.next(),
            Source::HashJoin(s) => s.next(),
            Source::SortMergeJoin(s) => s.next(),
            Source::SetOp(s) => s.next(),
            Source::Filter(s) => s.next(),
            Source::Project(s) => s.next(),
        }
    }

    fn reset(&mut self) -> Result<(), Status> {
        match self {
            Source::Memory(s) => s.reset(),
            Source::BTree(s) => s.reset(),
            Source::NestedLoopJoin(s) => s.reset(),
            Source::HashJoin(s) => s.reset(),
            Source::SortMergeJoin(s) => s.reset(),
            Source::SetOp(s) => s.reset(),
            Source::Filter(s) => s.reset(),
            Source::Project(s) => s.reset(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum SetOpStrategy {
    /// T: O(N*M) S: O(1)
    NaiveNestedLoop,
    /// T: O(N) S: O(M)
    HashedMemory,
    /// T: O(N) S: O(M - but only Keys)
    HashedBTree,
    /// T: O(N+M) S: O(1) - Requires Inputs sorted by Key
    Sorted,
}

#[derive(Debug, Clone)]
enum SetOpState {
    LeftStream,
    RightStream,
    Done,
    SortedMerge {
        l_curr: Option<Vec<u8>>,
        r_curr: Option<Vec<u8>>,
        initialized: bool,
    },
}

#[derive(Debug, Clone)]
pub enum RightSideContainer {
    Raw(Box<Source>),
    HashedMem(HashedMemorySource),
    HashedBTree(HashedBTreeSource),
    Sorted(Box<Source>),
}

#[derive(Debug, Clone, Copy)]
pub enum JoinStrategy {
    /// O(N*M)
    NestedLoop,
    /// O(N+M)
    Hash,
    /// O(N+M) "Zipper"
    SortMerge,
}

fn calculate_hash(row: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    row.hash(&mut hasher);
    hasher.finish()
}

#[derive(Debug, Clone)]
pub struct HashedMemorySource {
    source: Box<Source>,
    index: Option<HashMap<u64, Vec<usize>>>,
    rows: Option<Vec<Vec<u8>>>,
}

impl HashedMemorySource {
    pub fn new(source: Source) -> Self {
        Self {
            source: Box::new(source),
            index: None,
            rows: None,
        }
    }

    fn ensure_index(&mut self) -> Result<(), Status> {
        if self.index.is_some() {
            return Ok(());
        }

        let mut rows = Vec::new();
        let mut index: HashMap<u64, Vec<usize>> = HashMap::new();

        self.source.reset();

        let mut i = 0usize;
        while let Some(row) = self.source.next()? {
            let h = calculate_hash(&row);
            index.entry(h).or_default().push(i);
            rows.push(row);
            i += 1;
        }

        self.rows = Some(rows);
        self.index = Some(index);

        Ok(())
    }

    pub fn probe(&mut self, target_row: &[u8]) -> Result<bool, Status> {
        self.ensure_index()?;

        let h = calculate_hash(target_row);
        let index = self.index.as_ref().unwrap();
        let rows = self.rows.as_ref().unwrap();

        if let Some(indices) = index.get(&h) {
            for &i in indices {
                if rows[i].as_slice() == target_row {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }
}

#[derive(Debug, Clone)]
pub struct HashedBTreeSource {
    source: BTreeScanSource,
    index: Option<HashMap<u64, Vec<Vec<u8>>>>,
}

impl HashedBTreeSource {
    pub fn new(source: BTreeScanSource) -> Self {
        Self {
            source,
            index: None,
        }
    }

    fn ensure_index(&mut self) -> Result<(), Status> {
        if self.index.is_some() {
            return Ok(());
        }

        let mut map: HashMap<u64, Vec<Vec<u8>>> = HashMap::new();

        self.source.reset()?;

        loop {
            if !self.source.cursor.is_valid() {
                break;
            }

            let (key, row_body) = self.source.cursor.current()?.ok_or(Status::CursorError)?;

            let full_row = Serializer::reconstruct_row(&key, &row_body, &self.source.schema)?;

            let h = calculate_hash(&full_row);
            map.entry(h).or_default().push(key);

            self.source.cursor.advance()?;
        }

        self.index = Some(map);
        Ok(())
    }

    pub fn probe(&mut self, target_row: &[u8]) -> Result<bool, Status> {
        self.ensure_index()?;

        let h = calculate_hash(target_row);
        let map = self.index.as_ref().unwrap();

        if let Some(keys) = map.get(&h) {
            for key in keys {
                self.source.cursor.go_to(key)?;

                if self.source.cursor.is_valid() {
                    let (k, row_body) = self.source.cursor.current()?.unwrap();
                    let fetched_row =
                        Serializer::reconstruct_row(&k, &row_body, &self.source.schema)?;

                    if fetched_row == target_row {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }
}

#[derive(Debug, Clone)]
pub struct MemorySource {
    data: Vec<Row>,
    idx: usize,
}

impl MemorySource {
    pub(crate) fn new(data: Vec<Row>) -> MemorySource {
        MemorySource { data, idx: 0 }
    }
}

impl RowSource for MemorySource {
    fn next(&mut self) -> Result<Option<Vec<u8>>, Status> {
        if self.idx >= self.data.len() {
            return Ok(None);
        }
        let row = self.data[self.idx].clone();
        self.idx += 1;
        Ok(Some(row))
    }

    fn reset(&mut self) -> Result<(), Status> {
        self.idx = 0;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct BTreeScanSource {
    btree: Btree,
    schema: TableSchema,
    conditions: Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
    op_code: SqlConditionOpCode,
    cursor: BTreeCursor,
}

impl BTreeScanSource {
    pub(crate) fn new(
        btree: Btree,
        schema: TableSchema,
        op_code: SqlConditionOpCode,
        conditions: Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
    ) -> Self {
        let mut cursor = BTreeCursor::new(btree.clone());
        let _ = Self::setup_cursor(&mut cursor, &op_code, &conditions);

        Self {
            btree,
            schema,
            conditions,
            op_code,
            cursor,
        }
    }

    pub fn setup_cursor(
        cursor: &mut BTreeCursor,
        op_code: &SqlConditionOpCode,
        conditions: &Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
    ) -> Result<(), Status> {
        match op_code {
            SqlConditionOpCode::SelectFTS => cursor.move_to_start(),
            SqlConditionOpCode::SelectKeyRange => {
                if conditions.is_empty() {
                    return cursor.move_to_start();
                }
                let (op, ref val) = conditions[0];
                match op {
                    Greater | GreaterOrEqual | Equal => cursor.go_to(&val.clone()),
                    _ => cursor.move_to_start(),
                }
            }
            SqlConditionOpCode::SelectKeyUnique => {
                if conditions.is_empty() {
                    return cursor.move_to_start();
                }
                let (_, ref val) = conditions[0];
                cursor.go_to(&val.clone())
            }
            _ => cursor.move_to_start(),
        }
    }
}

impl RowSource for BTreeScanSource {
    fn next(&mut self) -> Result<Option<Vec<u8>>, Status> {
        loop {
            if !self.cursor.is_valid() {
                return Ok(None);
            }

            let (key, row_body) = self
                .cursor
                .current()?
                .ok_or(Status::InternalExceptionIntegrityCheckFailed)?;

            if Serializer::is_tomb(&key, &self.schema)? {
                self.cursor.advance()?;
                continue;
            }

            let full_row = Serializer::reconstruct_row(&key, &row_body, &self.schema)?;

            if Serializer::check_condition_on_bytes(
                &full_row,
                &self.conditions,
                &self.schema.fields,
            )? {
                self.cursor.advance()?;
                return Ok(Some(full_row));
            }

            self.cursor.advance()?;
        }
    }

    fn reset(&mut self) -> Result<(), Status> {
        Self::setup_cursor(&mut self.cursor, &self.op_code, &self.conditions)
    }
}

#[derive(Debug, Clone)]
pub struct FilterSource {
    source: Box<Source>,
    conditions: Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
    header: Vec<Field>,
}

impl FilterSource {
    pub fn new(
        source: Box<Source>,
        conditions: Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
        header: Vec<Field>,
    ) -> Self {
        Self {
            source,
            conditions,
            header,
        }
    }
}

impl RowSource for FilterSource {
    fn next(&mut self) -> Result<Option<Vec<u8>>, Status> {
        loop {
            let row = match self.source.next()? {
                Some(r) => r,
                None => return Ok(None),
            };

            if Serializer::check_condition_on_bytes(&row, &self.conditions, &self.header)? {
                return Ok(Some(row));
            }
        }
    }

    fn reset(&mut self) -> Result<(), Status> {
        self.source.reset()
    }
}

#[derive(Debug, Clone)]
pub struct ProjectSource {
    source: Box<Source>,
    mapping_indices: Vec<usize>,
    source_header: Vec<Field>,
}

impl ProjectSource {
    pub fn new(
        source: Box<Source>,
        mapping_indices: Vec<usize>,
        source_header: Vec<Field>,
    ) -> Self {
        Self {
            source,
            mapping_indices,
            source_header,
        }
    }
}

impl RowSource for ProjectSource {
    fn next(&mut self) -> Result<Option<Vec<u8>>, Status> {
        let row = match self.source.next()? {
            Some(r) => r,
            None => return Ok(None),
        };

        let split_fields = Serializer::split_row_into_fields(&row, &self.source_header)?;

        let mut new_row_bytes = Vec::new();
        for &idx in &self.mapping_indices {
            if idx < split_fields.len() {
                new_row_bytes.extend_from_slice(&split_fields[idx]);
            }
        }

        Ok(Some(new_row_bytes))
    }

    fn reset(&mut self) -> Result<(), Status> {
        self.source.reset()
    }
}

#[derive(Debug, Clone)]
pub struct NestedLoopJoinSource {
    left: Box<Source>,
    right: Box<Source>,
    left_header: Vec<Field>,
    right_header: Vec<Field>,
    conditions: Vec<(usize, usize)>,

    current_left_row: Option<Row>,
    initialized: bool,
}

impl NestedLoopJoinSource {
    pub fn new(
        left: Box<Source>,
        right: Box<Source>,
        left_header: Vec<Field>,
        right_header: Vec<Field>,
        conditions: Vec<(usize, usize)>,
    ) -> Self {
        Self {
            left,
            right,
            left_header,
            right_header,
            conditions,
            current_left_row: None,
            initialized: false,
        }
    }
}

impl RowSource for NestedLoopJoinSource {
    fn next(&mut self) -> Result<Option<Vec<u8>>, Status> {
        if !self.initialized {
            self.left.reset()?;
            self.right.reset()?;
            self.current_left_row = self.left.next()?;
            self.initialized = true;
        }

        loop {
            let l_row = match &self.current_left_row {
                Some(r) => r,
                None => return Ok(None),
            };

            while let Some(r_row) = self.right.next()? {
                let l_fields = Serializer::split_row_into_fields(l_row, &self.left_header)?;
                let r_fields = Serializer::split_row_into_fields(&r_row, &self.right_header)?;

                let mut match_found = true;
                for (l_idx, r_idx) in &self.conditions {
                    if l_fields[*l_idx] != r_fields[*r_idx] {
                        match_found = false;
                        break;
                    }
                }

                if match_found {
                    let mut new_row = l_row.clone();
                    new_row.extend(r_row);
                    return Ok(Some(new_row));
                }
            }

            self.right.reset()?;
            self.current_left_row = self.left.next()?;
        }
    }

    fn reset(&mut self) -> Result<(), Status> {
        self.left.reset()?;
        self.right.reset()?;
        self.current_left_row = None;
        self.initialized = false;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct HashJoinSource {
    left: Box<Source>,
    right: Box<Source>,
    left_join_idx: usize,
    right_join_idx: usize,
    left_header: Vec<Field>,
    right_header: Vec<Field>,

    hash_table: Option<HashMap<Vec<u8>, Vec<Vec<u8>>>>,
    current_left_row: Option<Vec<u8>>,

    current_match_index: usize,
}

impl HashJoinSource {
    pub fn new(
        left: Box<Source>,
        right: Box<Source>,
        left_idx: usize,
        right_idx: usize,
        left_header: Vec<Field>,
        right_header: Vec<Field>,
    ) -> Self {
        Self {
            left,
            right,
            left_join_idx: left_idx,
            right_join_idx: right_idx,
            left_header,
            right_header,
            hash_table: None,
            current_left_row: None,
            current_match_index: 0,
        }
    }

    fn build_hash_table(&mut self) -> Result<(), Status> {
        let mut map: HashMap<Vec<u8>, Vec<Vec<u8>>> = HashMap::new();

        self.right.reset()?;
        while let Some(r_row) = self.right.next()? {
            let fields = Serializer::split_row_into_fields(&r_row, &self.right_header)?;
            let key = fields[self.right_join_idx].clone().to_vec();
            map.entry(key).or_default().push(r_row);
        }

        self.hash_table = Some(map);
        Ok(())
    }
}

impl RowSource for HashJoinSource {
    fn next(&mut self) -> Result<Option<Vec<u8>>, Status> {
        if self.hash_table.is_none() {
            self.build_hash_table()?;
            self.left.reset()?;
        }

        let map = self.hash_table.as_ref().unwrap();

        loop {
            if let Some(l_row) = &self.current_left_row {
                let l_fields = Serializer::split_row_into_fields(l_row, &self.left_header)?;
                let key = l_fields[self.left_join_idx].to_vec();

                if let Some(matches) = map.get(&key) {
                    if self.current_match_index < matches.len() {
                        let r_row = &matches[self.current_match_index];
                        self.current_match_index += 1;

                        let mut new_row = l_row.clone();
                        new_row.extend(r_row.clone());
                        return Ok(Some(new_row));
                    }
                }
                self.current_left_row = None;
                self.current_match_index = 0;
            }
            match self.left.next()? {
                Some(row) => {
                    self.current_left_row = Some(row);
                }
                None => return Ok(None),
            }
        }
    }

    fn reset(&mut self) -> Result<(), Status> {
        self.left.reset()?;
        self.current_left_row = None;
        self.current_match_index = 0;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SortMergeJoinSource {
    left: Box<Source>,
    right: Box<Source>,
    left_join_idx: usize,
    right_join_idx: usize,
    left_header: Vec<Field>,
    right_header: Vec<Field>,

    l_curr: Option<Vec<u8>>,
    r_curr: Option<Vec<u8>>,
    initialized: bool,
}

impl SortMergeJoinSource {
    pub fn new(
        left: Box<Source>,
        right: Box<Source>,
        left_idx: usize,
        right_idx: usize,
        left_header: Vec<Field>,
        right_header: Vec<Field>,
    ) -> Self {
        Self {
            left,
            right,
            left_join_idx: left_idx,
            right_join_idx: right_idx,
            left_header,
            right_header,
            l_curr: None,
            r_curr: None,
            initialized: false,
        }
    }

    fn get_key(&self, row: &Vec<u8>, header: &Vec<Field>, idx: usize) -> Result<Vec<u8>, Status> {
        let fields = Serializer::split_row_into_fields(row, header)?;
        Ok(fields[idx].to_vec())
    }
}

impl RowSource for SortMergeJoinSource {
    fn next(&mut self) -> Result<Option<Vec<u8>>, Status> {
        if !self.initialized {
            self.left.reset()?;
            self.right.reset()?;
            self.l_curr = self.left.next()?;
            self.r_curr = self.right.next()?;
            self.initialized = true;
        }

        loop {
            if self.l_curr.is_none() || self.r_curr.is_none() {
                return Ok(None);
            }

            let l_row = self.l_curr.as_ref().unwrap();
            let r_row = self.r_curr.as_ref().unwrap();
            let l_key_bytes = self.get_key(l_row, &self.left_header, self.left_join_idx)?;
            let r_key_bytes = self.get_key(r_row, &self.right_header, self.right_join_idx)?;
            let key_type = &self.left_header[self.left_join_idx].field_type;
            let ordering = Serializer::compare_with_type(&l_key_bytes, &r_key_bytes, key_type)?;

            match ordering {
                std::cmp::Ordering::Less => {
                    self.l_curr = self.left.next()?;
                }
                std::cmp::Ordering::Greater => {
                    self.r_curr = self.right.next()?;
                }
                std::cmp::Ordering::Equal => {
                    let mut result = l_row.clone();
                    result.extend(r_row.clone());
                    self.l_curr = self.left.next()?;
                    return Ok(Some(result));
                }
            }
        }
    }

    fn reset(&mut self) -> Result<(), Status> {
        self.initialized = false;
        self.l_curr = None;
        self.r_curr = None;
        self.left.reset()?;
        self.right.reset()?;
        Ok(())
    }
}

impl RightSideContainer {
    fn contains(&mut self, row: &[u8]) -> Result<bool, Status> {
        match self {
            RightSideContainer::HashedMem(s) => s.probe(row),
            RightSideContainer::HashedBTree(s) => s.probe(row),
            RightSideContainer::Raw(s) | RightSideContainer::Sorted(s) => {
                s.reset()?;
                while let Some(r) = s.next()? {
                    if r == row {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }

    fn next_as_source(&mut self) -> Result<Option<Vec<u8>>, Status> {
        match self {
            RightSideContainer::Raw(s) | RightSideContainer::Sorted(s) => s.next(),
            RightSideContainer::HashedMem(s) => s.source.next(),
            RightSideContainer::HashedBTree(s) => s.source.next(),
        }
    }

    fn reset(&mut self) -> Result<(), Status> {
        match self {
            RightSideContainer::Raw(s) | RightSideContainer::Sorted(s) => s.reset(),
            RightSideContainer::HashedMem(s) => s.source.reset(),
            RightSideContainer::HashedBTree(s) => s.source.reset(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SetOperationSource {
    left: Box<Source>,
    right: RightSideContainer,
    op: ParsedSetOperator,
    state: SetOpState,
    seen_rows: HashSet<Vec<u8>>,
}
impl SetOperationSource {
    pub fn new(
        left: Box<Source>,
        right_source: Source,
        op: ParsedSetOperator,
        strategy: SetOpStrategy,
    ) -> Self {
        let (right_container, state) = match strategy {
            SetOpStrategy::NaiveNestedLoop => (
                RightSideContainer::Raw(Box::new(right_source)),
                SetOpState::LeftStream,
            ),
            SetOpStrategy::HashedMemory => (
                RightSideContainer::HashedMem(HashedMemorySource::new(right_source)),
                SetOpState::LeftStream,
            ),
            SetOpStrategy::HashedBTree => {
                if let Source::BTree(bts) = right_source {
                    (
                        RightSideContainer::HashedBTree(HashedBTreeSource::new(bts)),
                        SetOpState::LeftStream,
                    )
                } else {
                    panic!()
                }
            }
            SetOpStrategy::Sorted => (
                RightSideContainer::Sorted(Box::new(right_source)),
                SetOpState::SortedMerge {
                    l_curr: None,
                    r_curr: None,
                    initialized: false,
                },
            ),
        };

        Self {
            left,
            right: right_container,
            op,
            state,
            seen_rows: HashSet::new(),
        }
    }
}

impl RowSource for SetOperationSource {
    fn next(&mut self) -> Result<Option<Vec<u8>>, Status> {
        if let SetOpState::SortedMerge {
            l_curr,
            r_curr,
            initialized,
        } = &mut self.state
        {
            if !*initialized {
                self.left.reset()?;
                self.right.reset()?;
                *l_curr = self.left.next()?;
                *r_curr = self.right.next_as_source()?;
                *initialized = true;
            }

            loop {
                match (l_curr.as_ref(), r_curr.as_ref()) {
                    (Some(l), Some(r)) => match l.cmp(r) {
                        std::cmp::Ordering::Less => match self.op {
                            ParsedSetOperator::Union | ParsedSetOperator::All => {
                                let ret = l.clone();
                                *l_curr = self.left.next()?;
                                return Ok(Some(ret));
                            }
                            ParsedSetOperator::Intersect => {
                                *l_curr = self.left.next()?;
                            }
                            ParsedSetOperator::Except | ParsedSetOperator::Minus => {
                                let ret = l.clone();
                                *l_curr = self.left.next()?;
                                return Ok(Some(ret));
                            }
                            _ => {}
                        },
                        std::cmp::Ordering::Greater => match self.op {
                            ParsedSetOperator::Union | ParsedSetOperator::All => {
                                let ret = r.clone();
                                *r_curr = self.right.next_as_source()?;
                                return Ok(Some(ret));
                            }
                            ParsedSetOperator::Intersect
                            | ParsedSetOperator::Except
                            | ParsedSetOperator::Minus => {
                                *r_curr = self.right.next_as_source()?;
                            }
                            _ => {panic!()}
                        },
                        std::cmp::Ordering::Equal => match self.op {
                            ParsedSetOperator::Union | ParsedSetOperator::Intersect => {
                                let ret = l.clone();
                                *l_curr = self.left.next()?;
                                *r_curr = self.right.next_as_source()?;
                                return Ok(Some(ret));
                            }
                            ParsedSetOperator::All => {
                                let ret = l.clone();
                                *l_curr = self.left.next()?;
                                return Ok(Some(ret));
                            }
                            ParsedSetOperator::Except | ParsedSetOperator::Minus => {
                                *l_curr = self.left.next()?;
                                *r_curr = self.right.next_as_source()?;
                            }
                            _ => {panic!()}
                        },
                    },
                    (Some(l), None) => match self.op {
                        ParsedSetOperator::Union
                        | ParsedSetOperator::All
                        | ParsedSetOperator::Except
                        | ParsedSetOperator::Minus => {
                            let ret = l.clone();
                            *l_curr = self.left.next()?;
                            return Ok(Some(ret));
                        }
                        _ => return Ok(None),
                    },
                    (None, Some(r)) => match self.op {
                        ParsedSetOperator::Union | ParsedSetOperator::All => {
                            let ret = r.clone();
                            *r_curr = self.right.next_as_source()?;
                            return Ok(Some(ret));
                        }
                        _ => return Ok(None),
                    },
                    (None, None) => return Ok(None),
                }
            }
        }

        match self.op {
            ParsedSetOperator::All => match self.state {
                SetOpState::LeftStream => {
                    let row = self.left.next()?;
                    if row.is_some() {
                        return Ok(row);
                    }
                    self.state = SetOpState::RightStream;
                    self.right.reset()?;
                    self.right.next_as_source()
                }
                SetOpState::RightStream => self.right.next_as_source(),
                SetOpState::Done => Ok(None),
                _ => Err(Status::InternalError),
            },
            ParsedSetOperator::Union => loop {
                let row_opt = match self.state {
                    SetOpState::LeftStream => {
                        let r = self.left.next()?;
                        if r.is_none() {
                            self.state = SetOpState::RightStream;
                            self.right.reset()?;
                            self.right.next_as_source()?
                        } else {
                            r
                        }
                    }
                    SetOpState::RightStream => self.right.next_as_source()?,
                    SetOpState::Done => return Ok(None),
                    _ => return Err(Status::InternalError),
                };
                match row_opt {
                    Some(row) => {
                        if self.seen_rows.insert(row.clone()) {
                            return Ok(Some(row));
                        }
                    }
                    None => {
                        self.state = SetOpState::Done;
                        return Ok(None);
                    }
                }
            },
            ParsedSetOperator::Intersect => loop {
                let row = match self.left.next()? {
                    Some(r) => r,
                    None => return Ok(None),
                };

                if self.right.contains(&row)? {
                    if self.seen_rows.insert(row.clone()) {
                        return Ok(Some(row));
                    }
                }
            },
            ParsedSetOperator::Except | ParsedSetOperator::Minus => loop {
                let row = match self.left.next()? {
                    Some(r) => r,
                    None => return Ok(None),
                };
                if !self.right.contains(&row)? {
                    if self.seen_rows.insert(row.clone()) {
                        return Ok(Some(row));
                    }
                }
            },
            ParsedSetOperator::Times => todo!(),
        }
    }

    fn reset(&mut self) -> Result<(), Status> {
        self.left.reset()?;
        self.right.reset()?;
        if let RightSideContainer::Sorted(_) = self.right {
            self.state = SetOpState::SortedMerge {
                l_curr: None,
                r_curr: None,
                initialized: false,
            };
        } else {
            self.state = SetOpState::LeftStream;
        }
        Ok(())
    }
}