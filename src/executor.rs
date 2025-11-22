use crate::btree::{Btree};
use crate::pager::{
    Key, PagerAccessor, PagerCore, Position, Row, TableName, Type, PAGE_SIZE, PAGE_SIZE_WITH_META,
};
use crate::pager_proxy::PagerProxy;
use crate::parser::{JoinType, ParsedSetOperator, Parser};
use crate::planner::SqlStatementComparisonOperator::{
    Equal, Greater, GreaterOrEqual, Lesser, LesserOrEqual,
};
use crate::planner::{CompiledCreateTableQuery, CompiledDeleteQuery, CompiledInsertQuery, CompiledQuery, CompiledSelectQuery, PlanNode, Planner, SqlConditionOpCode, SqlStatementComparisonOperator};
use crate::serializer::Serializer;
use crate::status::Status;
use crate::status::Status::ExceptionQueryMisformed;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{format, Display, Formatter};
use std::fs::OpenOptions;
use std::io::{ErrorKind, Write};
use crate::cursor::BTreeCursor;

const MASTER_TABLE_NAME: &str = "rustsql_master";

pub static MASTER_TABLE_SQL: &str = "CREATE TABLE rustsql_master (
        name STRING,
        type STRING,
        rootpage INTEGER,
        sql STRING
    )";
#[derive(Clone, Debug)]
pub struct Schema {
    pub table_index: TableIndex,
    pub tables: Vec<TableSchema>,
}

impl Schema {
    pub fn make_empty() -> Self {
        Schema {
            table_index: TableIndex { index: vec![] },
            tables: vec![],
        }
    }
}

#[derive(Clone, Debug)]
pub struct TableIndex {
    pub index: Vec<TableName>,
}

//TODO remove redundant fields, i. e. decide which to keep, implement derivation methods for the others
#[derive(Clone, Debug)]
pub struct TableSchema {
    pub next_position: Position,
    pub root: Position, //if 0 -> no tree
    //pub col_count: usize,
    //pub key_and_row_length: usize,
    //pub key_length: usize, //includes flag
    //pub key_type: Type,
    //pub row_length: usize,
    pub has_key: bool,
    pub key_position: usize,
    pub fields: Vec<Field>,
    pub table_type: u8,
    pub entry_count: i32,
}

impl TableSchema {
    pub fn get_col_count(&self) -> Result<usize, Status> {
        Ok(self.fields.len() - 1)
    }

    pub fn get_key_and_row_length(&self) -> Result<usize, Status> {
        let mut len = 0usize;
        for field in &self.fields {
            len += Serializer::get_size_of_type(&field.field_type)?;
        }
        Ok(len)
    }

    pub fn get_key_length(&self) -> Result<usize, Status> {
        if self.fields.is_empty() {
            return Err(Status::InternalExceptionCompilerError);
        }
        if self.key_position >= self.fields.len() {
            return Err(Status::InternalExceptionCompilerError);
        }
        Serializer::get_size_of_type(&self.fields[self.key_position].field_type)
    }

    pub fn get_key_type(&self) -> Result<Type, Status> {
        if self.fields.is_empty() {
            return Err(Status::InternalExceptionCompilerError);
        }
        if self.key_position >= self.fields.len() {
            return Err(Status::InternalExceptionCompilerError);
        }
        Ok(self.fields[self.key_position].field_type.clone())
    }

    pub fn get_row_length(&self) -> Result<usize, Status> {
        if self.fields.is_empty() {
            return Err(Status::InternalExceptionCompilerError);
        }
        let mut len = 0usize;
        for (idx, field) in self.fields.iter().enumerate() {
            if idx == self.key_position {
                continue;
            }
            len += Serializer::get_size_of_type(&field.field_type)?;
        }
        Ok(len)
    }
}

#[derive(Debug, Clone)]
pub struct Field {
    pub field_type: Type, // Assuming the type size is a single byte.
    pub name: String,     // The name of the field, extracted from 128 bits (16 bytes).
}

#[derive(Debug)]
pub struct QueryResult {
    pub success: bool,
    pub result: DataFrame,
    status: Status,
}

impl Display for QueryResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}\t", self.result)
    }
}

impl QueryResult {
    pub fn user_input_wrong(msg: String) -> Self {
        QueryResult {
            success: false,
            result: DataFrame::msg(msg.as_str()),
            status: ExceptionQueryMisformed,
        }
    }

    // TODO rename
    pub fn msg(str: &str) -> QueryResult {
        QueryResult {
            success: false,
            result: DataFrame::msg(str),
            status: ExceptionQueryMisformed,
        }
    }

    // TODO rename
    pub fn err(s: Status) -> Self {
        QueryResult {
            success: false,
            result: DataFrame::msg(format!("{:?}", s).as_str()),
            status: s,
        }
    }

    pub fn went_fine() -> Self {
        QueryResult {
            success: true,
            result: DataFrame::msg("Query Executed Successfully"),
            status: Status::Success,
        }
    }

    pub fn return_data(data: DataFrame) -> Self {
        QueryResult {
            success: true,
            result: data,
            status: Status::Success,
        }
    }
}

#[derive(Debug)]
pub struct DataFrame {
    pub header: Vec<Field>,
    pub data: Vec<Vec<u8>>,
}

impl DataFrame {
    pub fn new() -> Self {
        DataFrame {
            header: vec![],
            data: vec![],
        }
    }

    //TODO fix when there is a varchar--this is wrong, a string longer than 256 would be cut off
    pub fn msg(message: &str) -> Self {
        DataFrame {
            header: vec![Field {
                field_type: Type::String,
                name: "Message".to_string(),
            }],
            data: vec![Serializer::parse_string(message).to_vec()],
        }
    }
}

impl Display for DataFrame {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for field in &self.header {
            write!(f, "{}\t", field.name)?;
        }
        writeln!(f)?;

        for row in &self.data {
            let mut position = 0;
            for field in &self.header {
                let field_type = &field.field_type;
                let field_len = Serializer::get_size_of_type(field_type).unwrap();
                let field_value = &row[position..position + field_len];
                let formatted_value =
                    Serializer::format_field(&field_value.to_vec(), field_type).unwrap();
                write!(f, "{}\t", formatted_value)?;
                position += field_len;
            }
            writeln!(f)?;
        }

        Ok(())
    }
}

trait RowSource {
    fn next(&mut self) -> Result<Option<Vec<u8>>, Status>;
    fn reset(&mut self) -> Result<(), Status>;
    fn get_header(&self) -> Vec<Field>;
}

struct DataFrameSource {
    data: DataFrame,
    idx: usize,
}

impl DataFrameSource {
    fn new(data: DataFrame) -> Self {
        Self { data, idx: 0 }
    }
}

impl RowSource for DataFrameSource {
    fn next(&mut self) -> Result<Option<Vec<u8>>, Status> {
        if self.idx >= self.data.data.len() {
            return Ok(None);
        }
        let row = self.data.data[self.idx].clone();
        self.idx += 1;
        Ok(Some(row))
    }

    fn reset(&mut self) -> Result<(), Status> {
        self.idx = 0;
        Ok(())
    }

    fn get_header(&self) -> Vec<Field> {
        self.data.header.clone()
    }
}

struct BTreeScanSource {
    btree: Btree,
    schema: TableSchema,
    conditions: Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
    op_code: SqlConditionOpCode,
    cursor: BTreeCursor,
}

impl BTreeScanSource {
    fn new(
        btree: Btree,
        schema: TableSchema,
        op_code: SqlConditionOpCode,
        conditions: Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
    ) -> Self {
        let cursor = BTreeCursor::new(btree.clone());
        Self {
            btree,
            schema,
            conditions,
            op_code,
            cursor,
        }
    }
}

impl RowSource for BTreeScanSource {
    fn next(&mut self) -> Result<Option<Vec<u8>>, Status> {
        loop {
            if !self.cursor.is_valid() {
                return Ok(None);
            }

            let (key, row_body) = self.cursor.current()?.ok_or(Status::InternalExceptionIntegrityCheckFailed)?;

            if Serializer::is_tomb(&key, &self.schema)? {
                self.cursor.advance()?;
                continue;
            }

            let full_row = Executor::reconstruct_row(&key, &row_body, &self.schema)?;

            if Executor::check_condition_on_bytes(&full_row, &self.conditions, &self.schema.fields) {
                self.cursor.advance()?;
                return Ok(Some(full_row));
            }

            self.cursor.advance()?;
        }
    }

    fn reset(&mut self) -> Result<(), Status> {
        match self.op_code {
            SqlConditionOpCode::SelectFTS => {
                self.cursor.move_to_start()?;
            }
            SqlConditionOpCode::SelectKeyRange => {
                //ToDO use the key_index in Schema.TableSchema
                let (op, ref val) = self.conditions[0];
                match op {
                    Greater | GreaterOrEqual | Equal => {
                        self.cursor.go_to(&val.clone())?;
                    }
                    _ => {
                        self.cursor.move_to_start()?;
                    }
                }
            }
            SqlConditionOpCode::SelectKeyUnique => {
                // For unique, search key again
                let (_, ref val) = self.conditions[0];
                self.cursor.go_to(&val.clone())?;
            }
            _ => {
                self.cursor.move_to_start()?;
            }
        }
        Ok(())
    }

    fn get_header(&self) -> Vec<Field> {
        self.schema.fields.clone()
    }
}
/// ## Responsibilities
/// - Misc
/// - Managing a cache for queries
/// - executing compiled queries
pub struct Executor {
    pub pager_accessor: PagerAccessor,
    pub query_cache: HashMap<String, CompiledQuery>, //must be invalidated once schema is changed or in a smart way
    pub schema: Schema,
    pub btree_node_width: usize,
}

impl Executor {
    pub fn init(file_path: &str, t: usize) -> Self {
        let pager_accessor = match PagerCore::init_from_file(file_path) {
            Ok(pa) => pa,
            Err(e) => {
                println!("{:?}", e);
                match e {
                    Status::InternalExceptionFileNotFound => {
                        Self::create_database(file_path).expect("Failed to create database");
                        PagerCore::init_from_file(file_path)
                            .expect("Failed to initialise PagerCore after creating database")
                    }
                    _ => {
                        eprintln!("{:?}", e);
                        panic!("Failed to initialise Executor: {:?}", e);
                    }
                }
            }
        };

        let mut bootstrap_executor = Executor {
            pager_accessor: pager_accessor.clone(),
            query_cache: HashMap::new(),
            schema: Schema {
                table_index: TableIndex { index: vec![TableName::from(MASTER_TABLE_NAME.to_string())] },
                tables: vec![
                    Self::make_master_table_schema()
                ],
            },
            btree_node_width: t,
        };

        bootstrap_executor.schema = bootstrap_executor.load_schema();
        bootstrap_executor
    }

    pub fn debug_lite(&self, table: Option<&str>) {
        if table.is_none() {
            println!(
                "System Table: {}",
                Btree::init(
                    self.btree_node_width,
                    self.pager_accessor.clone(),
                    self.schema.tables[0].clone()
                )
                .unwrap()
            );
        } else {
            let table_name = table.unwrap();
            let table_id = Planner::find_table_id(&self.schema, table_name).unwrap();
            let table_schema = self.schema.tables[table_id].clone();
            let btree = Btree::init(
                self.btree_node_width,
                self.pager_accessor.clone(),
                table_schema.clone(),
            )
            .unwrap();
            println!("Table: {}", btree);
        }
    }

    pub fn debug(&mut self, table: Option<&str>) {
        if table.is_none() {
            println!("Cached Schema: {:?}", self.schema);
            println!(
                "System Table: {}",
                Btree::init(
                    self.btree_node_width,
                    self.pager_accessor.clone(),
                    self.schema.tables[0].clone()
                )
                .unwrap()
            );
        } else {
            let table_name = table.unwrap();
            let table_id = Planner::find_table_id(&self.schema, table_name).unwrap();
            let table_schema = self.schema.tables[table_id].clone();
            let btree = Btree::init(
                self.btree_node_width,
                self.pager_accessor.clone(),
                table_schema.clone(),
            )
            .unwrap();
            println!("Table: {}", btree);
        }
        println!(
            "Checking Integrity... Is {}",
            self.check_integrity().is_ok()
        );
        println!(
            "System Table Data: \n {}",
            self.exec("SELECT * FROM rustsql_master".to_string())
        );
    }

    pub fn exit(&self) {
        self.pager_accessor
            .access_pager_write(|p| p.flush())
            .expect("Error Flushing the Pager");
    }

    pub fn exec(&mut self, query: String) -> QueryResult {
        let result = self.exec_intern(query, false);
        if !result.is_ok() {
            result.err().unwrap()
        } else {
            result.expect("just checked")
        }
    }

    fn exec_intern(
        &mut self,
        query: String,
        allow_modification_to_system_table: bool,
    ) -> Result<QueryResult, QueryResult> {
        let mut parser = Parser::new(query.clone());
        let parsed_query = parser
            .parse_query()
            .map_err(|s| QueryResult::user_input_wrong(s))?;
        let compiled_query = Planner::plan(&self.schema, parsed_query)?;
        match compiled_query {
            CompiledQuery::CreateTable(q) => {
                //check if the table already exists
                //this could be achieved using a unique / pk constraint on the system table.
                //but there are no constraints implemented ;D
                let stripped_mame = q.table_name.trim_end_matches(|char| char == '0');
                let table_name: TableName = stripped_mame.as_bytes().to_vec();
                if self.schema.table_index.index.contains(&table_name) {
                    return Err(QueryResult::err(Status::ExceptionTableAlreadyExists));
                }

                let root_page = PagerProxy::create_empty_node_on_new_page(
                    &q.schema,
                    self.pager_accessor.clone(),
                )
                .map_err(|status| QueryResult::err(status))
                .map(|node| {
                    return node.position.page();
                })?;

                let insert_query = format!(
                    "INSERT INTO {} (name, type, rootpage, sql) VALUES ({}, {}, {}, '{}')",
                    MASTER_TABLE_NAME, q.table_name, 0, root_page, query
                );
                println!("{}", insert_query);
                self.exec_intern(insert_query, true);
                self.reload_schema()
            }
            CompiledQuery::DropTable(q) => {
                todo!()
            }
            CompiledQuery::Select(q) => {
                let result_df = self.execute_plan(&q.plan).map_err(|s| QueryResult::err(s))?;
                Ok(QueryResult::return_data(result_df))
            }
            CompiledQuery::Insert(q) => {
                let schema = &self.schema.tables[q.table_id];
                let mut btree = Btree::init(
                    self.btree_node_width,
                    self.pager_accessor.clone(),
                    schema.clone(),
                )
                .map_err(|s| QueryResult::err(s))?;
                btree
                    .insert(q.data.0, q.data.1)
                    .map_err(|s| QueryResult::err(s))?;
                Ok(QueryResult::went_fine())
            }
            CompiledQuery::Delete(q) => {
                let mut source = self.create_scan_source(q.table_id, q.operation, q.conditions.clone())
                    .map_err(|s| QueryResult::err(s))?;

                let schema = &self.schema.tables[q.table_id];
                let key_offset = {
                    let mut offset = 0;
                    for i in 0..schema.key_position {
                        offset += Serializer::get_size_of_type(&schema.fields[i].field_type).map_err(QueryResult::err)?;
                    }
                    offset
                };
                let key_len = Serializer::get_size_of_type(&schema.fields[schema.key_position].field_type).map_err(QueryResult::err)?;

                let mut keys_to_delete = Vec::new();

                source.reset().map_err(QueryResult::err)?;
                while let Some(row) = source.next().map_err(QueryResult::err)? {
                    if row.len() < key_offset + key_len {
                        return Err(QueryResult::err(Status::InternalExceptionIntegrityCheckFailed));
                    }
                    let key = row[key_offset..key_offset + key_len].to_vec();
                    keys_to_delete.push(key);
                }

                let mut btree = Btree::init(
                    self.btree_node_width,
                    self.pager_accessor.clone(),
                    schema.clone(),
                )
                    .map_err(|s| QueryResult::err(s))?;

                for key in keys_to_delete {
                    btree.delete(key).map_err(|s| QueryResult::err(s))?;
                }
                Ok(QueryResult::went_fine())
            }
        }
    }

    /// Recursive execution engine for the Query Plan Tree
    fn execute_plan(&self, plan: &PlanNode) -> Result<DataFrame, Status> {
        match plan {
            PlanNode::SeqScan { table_id, table_name, operation, conditions } => {
                let mut source = self.create_scan_source(*table_id, operation.clone(), conditions.clone())?;
                let mut result_data = Vec::new();

                source.reset()?;
                while let Some(row) = source.next()? {
                    result_data.push(row);
                }

                Ok(DataFrame {
                    header: source.get_header(),
                    data: result_data,
                })
            }

            PlanNode::Filter { source, conditions } => {
                let source_df = self.execute_plan(source)?;
                let mut filtered_data = Vec::new();

                for row in source_df.data {
                    if Self::check_condition_on_bytes(&row, conditions, &source_df.header) {
                        filtered_data.push(row);
                    }
                }

                Ok(DataFrame {
                    header: source_df.header,
                    data: filtered_data,
                })
            }

            PlanNode::Project { source, fields } => {
                let source_df = self.execute_plan(source)?;
                let mut project_data = Vec::new();
                let result_header: Vec<Field> = fields.iter().map(|(_, f)| f.clone()).collect();

                let mut mapping_indices = Vec::new();
                for (_, req_field) in fields {
                    if let Some(idx) = source_df.header.iter().position(|f| f.name == req_field.name) {
                        mapping_indices.push(idx);
                    } else {
                        return Err(Status::InternalExceptionCompilerError); // Should have been caught by Planner
                    }
                }

                for row in source_df.data {
                    let mut new_row_bytes = Vec::new();

                    let split_fields = Self::split_row_into_fields(&row, &source_df.header)?;

                    for &idx in &mapping_indices {
                        new_row_bytes.extend_from_slice(&split_fields[idx]);
                    }
                    project_data.push(new_row_bytes);
                }

                Ok(DataFrame {
                    header: result_header,
                    data: project_data,
                })
            }

            PlanNode::Join { left, right, join_type, conditions } => {
                let mut left_source = self.get_row_source(left)?;
                let mut right_source = self.get_row_source(right)?;

                let mut result_header = left_source.get_header();
                result_header.extend(right_source.get_header());

                let mut result_data = Vec::new();

                // Nested Loop Join using Cursors
                left_source.reset()?;
                while let Some(l_row) = left_source.next()? {

                    right_source.reset()?;
                    while let Some(r_row) = right_source.next()? {

                        let l_fields = Self::split_row_into_fields(&l_row, &left_source.get_header())?;
                        let r_fields = Self::split_row_into_fields(&r_row, &right_source.get_header())?;

                        let mut match_found = true;

                        for (l_col, r_col) in conditions {
                            let l_idx = left_source.get_header().iter().position(|f| f.name == *l_col).unwrap();
                            let r_idx = right_source.get_header().iter().position(|f| f.name == *r_col).unwrap();

                            if l_fields[l_idx] != r_fields[r_idx] {
                                match_found = false;
                                break;
                            }
                        }

                        if match_found {
                            let mut new_row = l_row.clone();
                            new_row.extend(r_row.clone());
                            result_data.push(new_row);
                        }
                    }
                }

                if matches!(join_type, JoinType::Left | JoinType::Full) {
                    // TODO: Implement Left Join
                }

                Ok(DataFrame {
                    header: result_header,
                    data: result_data,
                })
            }

            PlanNode::SetOperation { op, left, right } => {
                let left_df = self.execute_plan(left)?;
                let right_df = self.execute_plan(right)?;

                let mut result_data = left_df.data.clone();

                match op {
                    ParsedSetOperator::Union => {
                        // ToDo Remove Duplucates
                        result_data.extend(right_df.data.clone());
                    }
                    ParsedSetOperator::Intersect => {
                        // ToDo this is O(N*M)
                        result_data.retain(|l_row| right_df.data.contains(l_row));
                    }
                    ParsedSetOperator::Except | ParsedSetOperator::Minus => {
                        result_data.retain(|l_row| !right_df.data.contains(l_row));
                    }
                    _ => {
                        // ToDo others
                        result_data.extend(right_df.data.clone());
                    }
                }

                Ok(DataFrame {
                    header: left_df.header,
                    data: result_data,
                })
            }
        }
    }

    /// Reconstructs a single continuous byte vector from the BTree's split Key and Row.
    fn reconstruct_row(key: &Key, row: &Row, schema: &TableSchema) -> Result<Vec<u8>, Status> {
        let mut full_row = Vec::new();
        let mut row_cursor = 0;

        for (i, field) in schema.fields.iter().enumerate() {
            if i == schema.key_position {
                full_row.extend_from_slice(key);
            } else {
                let size = Serializer::get_size_of_type(&field.field_type)?;
                if row_cursor + size > row.len() {
                    return Err(Status::InternalExceptionIntegrityCheckFailed);
                }
                full_row.extend_from_slice(&row[row_cursor..row_cursor+size]);
                row_cursor += size;
            }
        }
        Ok(full_row)
    }

    fn get_row_source<'a>(&'a self, plan: &'a PlanNode) -> Result<Box<dyn RowSource + 'a>, Status> {
        match plan {
            PlanNode::SeqScan { table_id, operation, conditions, .. } => {
                self.create_scan_source(*table_id, operation.clone(), conditions.clone())
            }
            _ => {
                // Fallback: Materialize into DataFrame and iterate memory
                let df = self.execute_plan(plan)?;
                Ok(Box::new(DataFrameSource::new(df)))
            }
        }
    }

    fn create_scan_source<'a>(
        &'a self,
        table_id: usize,
        operation: SqlConditionOpCode,
        conditions: Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
    ) -> Result<Box<dyn RowSource + 'a>, Status> {
        let schema = self.schema.tables[table_id].clone();
        let btree = Btree::init(
            self.btree_node_width,
            self.pager_accessor.clone(),
            schema.clone(),
        )?;

        Ok(Box::new(BTreeScanSource::new(btree, schema, operation, conditions)))
    }

    fn split_row_into_fields<'a>(row: &'a [u8], header: &[Field]) -> Result<Vec<&'a [u8]>, Status> {
        let mut slices = Vec::new();
        let mut pos = 0;
        for field in header {
            let size = Serializer::get_size_of_type(&field.field_type)?;
            if pos + size > row.len() {
                return Err(Status::InternalExceptionIntegrityCheckFailed);
            }
            slices.push(&row[pos..pos+size]);
            pos += size;
        }
        Ok(slices)
    }

    /// Checks conditions against a byte row using the field definitions provided
    fn check_condition_on_bytes(
        row: &[u8],
        conditions: &Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
        header: &[Field],
    ) -> bool {
        let mut position = 0;

        for (i, field) in header.iter().enumerate() {
            if i >= conditions.len() { break; }

            let (op, ref target_val) = conditions[i];
            let size = Serializer::get_size_of_type(&field.field_type).unwrap_or(0);

            if op == SqlStatementComparisonOperator::None {
                position += size;
                continue;
            }

            if position + size > row.len() { return false; } // Should not happen
            let field_val = &row[position..position + size];

            let cmp_result = Serializer::compare_with_type(
                &field_val.to_vec(),
                target_val,
                &field.field_type
            ).unwrap_or(std::cmp::Ordering::Equal);

            let matched = match cmp_result {
                std::cmp::Ordering::Equal => matches!(op, Equal | LesserOrEqual | GreaterOrEqual),
                std::cmp::Ordering::Less => matches!(op, Lesser | LesserOrEqual),
                std::cmp::Ordering::Greater => matches!(op, Greater | GreaterOrEqual),
            };

            if !matched {
                return false;
            }

            position += size;
        }
        true
    }

    pub fn check_integrity(&self) -> Result<(), Status> {
        let btree = Btree::init(
            self.btree_node_width,
            self.pager_accessor.clone(),
            self.schema.tables[0].clone(),
        )?;
        let table_schema = self.schema.tables[0].clone();

        let mut cursor = BTreeCursor::new(btree);
        cursor.move_to_start()?;

        let mut last_key: Option<Key> = None;

        while cursor.is_valid() {
            if let Some((key, _)) = cursor.current()? {
                if Serializer::is_tomb(&key, &table_schema)? {
                    cursor.advance()?;
                    continue;
                }

                if let Some(ref last) = last_key {
                    if Serializer::compare_with_type(last, &key, &table_schema.get_key_type()?)?
                        != std::cmp::Ordering::Less
                    {
                        return Err(Status::InternalExceptionIntegrityCheckFailed);
                    }
                }
                last_key = Some(key);
            }
            cursor.advance()?;
        }

        Ok(())
    }


    pub fn create_database(file_name: &str) -> Result<(), Status> {
        let mut db = [0u8; 2 + PAGE_SIZE_WITH_META];
        //i think this will continue to be hardcoded here for the foreseeable future
        //where to store Next_Page??
        // [<0, 1> Next Page, <0, 1> Free Space, Flag, Num-keys, Flag]
        db[1] = 2; //next page: [0, 1] -> 2 (starts at 1)
        db[2] = ((PAGE_SIZE - 600) << 8) as u8;
        db[3] = ((PAGE_SIZE - 600) & 0xFF) as u8;
        db[6] = Serializer::create_node_flag(true); //flag: is a leaf

        match OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(file_name)
            .unwrap()
            .write(&db)
        {
            Ok(f) => Ok(()),
            _ => Err(Status::InternalExceptionDBCreationFailed),
        }
    }

    fn reload_schema(&mut self) -> Result<QueryResult, QueryResult> {
        self.schema = self.load_schema();
        Ok(QueryResult::went_fine())
    }

    fn load_schema(&self) -> Schema {
        let mut master_table_schema = Self::make_master_table_schema();
        master_table_schema.root = Position::new(1, 0);
        let mut schema = Schema {
            table_index: TableIndex {
                index: vec![TableName::from(MASTER_TABLE_NAME)],
            },
            tables: vec![master_table_schema.clone()],
        };
        let select_query = CompiledSelectQuery {
            plan: PlanNode::Project {
                source: Box::new(PlanNode::SeqScan {
                    table_id: 0,
                    table_name: MASTER_TABLE_NAME.to_string(),
                    operation: SqlConditionOpCode::SelectFTS,
                    conditions: vec![],
                }),
                fields: vec![
                    (MASTER_TABLE_NAME.to_string(), Field {
                        field_type: Type::String,
                        name: "name".to_string(),
                    }),
                    (MASTER_TABLE_NAME.to_string(), Field {
                        field_type: Type::String,
                        name: "sql".to_string(),
                    }),
                     (MASTER_TABLE_NAME.to_string(), Field {
                        field_type: Type::Integer,
                        name: "rootpage".to_string(),
                    }),
                ],
            }
        };

        let result = self.execute_plan(&select_query.plan).expect("Failed Initialisation");

        result.data.iter().for_each(|entry| {
            let name = Serializer::get_field_on_row(entry, 0, &master_table_schema)
                .expect("Failed to get field: Name");
            let sql = Serializer::format_field_on_row(entry, 1, &master_table_schema)
                .expect("Failed to format field: SQL");
            let rootpage = Serializer::bytes_to_int(
                <[u8; 5]>::try_from(
                    Serializer::get_field_on_row(entry, 2, &master_table_schema)
                        .expect("Failed to get field: Rootpage"),
                )
                .unwrap(),
            );
            let mut parser = Parser::new(sql);
            let parsed_query = parser.parse_query().expect("Failed to parse query");
            let compiled_query = Planner::plan(&Schema::make_empty(), parsed_query)
                .expect("Failed to compile query");
            match compiled_query {
                CompiledQuery::CreateTable(mut table) => {
                    let strip_pos = name.iter().rposition(|&x| x != 0).expect("cant be empty");
                    schema
                        .table_index
                        .index
                        .push(name[0..strip_pos + 1].to_vec());
                    table.schema.root = Position::new(rootpage as usize, 0);
                    schema.tables.push(table.schema);
                }
                _ => {
                    panic!("in the system table should only be create table queries")
                }
            }
        });
        schema
    }

    fn make_master_table_schema() -> TableSchema {
        let mut parser = Parser::new(MASTER_TABLE_SQL.parse().unwrap());
        let parsed_query = parser
            .parse_query()
            .expect("why would there be an error here");
        let compiled_query = Planner::plan(&Schema::make_empty(), parsed_query);
        match compiled_query {
            Ok(CompiledQuery::CreateTable(mut create)) => {
                create.schema.root = Position::new(1, 0);
                create.schema
            },
            _ => {
                panic!("wtf")
            }
        }
    }
}
