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
use crate::dataframe::{BTreeScanSource, DataFrame, MemorySource, RowSource, Source};
pub(crate) use crate::schema::{Field, Schema, TableIndex, TableSchema};


const MASTER_TABLE_NAME: &str = "rustsql_master";

pub static MASTER_TABLE_SQL: &str = "CREATE TABLE rustsql_master (
        name STRING,
        type STRING,
        rootpage INTEGER,
        sql STRING
    )";

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

                Ok(DataFrame::from_memory(table_name.clone(), plan.get_header(&self.schema), result_data))
            }

            PlanNode::Filter { source, conditions } => {
                let source_df = self.execute_plan(source)?;
                let mut filtered_data = Vec::new();

                for row in source_df.get_data()? {
                    if Self::check_condition_on_bytes(&row, conditions, &plan.get_header(&self.schema)) {
                        //ToDo potentially Expensive!
                        filtered_data.push(row.clone());
                    }
                }

                Ok(DataFrame::from_memory("".to_string(), plan.get_header(&self.schema), filtered_data))
            }

            PlanNode::Project { source, fields } => {
                let source_df = self.execute_plan(source)?;
                let mut project_data = Vec::new();
                let result_header: Vec<Field> = fields.iter().map(|(_, f)| f.clone()).collect();

                let mut mapping_indices = Vec::new();
                for (_, req_field) in fields {
                    if let Some(idx) = source_df.header.iter().position(|f| f.identifier == req_field.identifier) {
                        mapping_indices.push(idx);
                    } else {
                        return Err(Status::InternalExceptionCompilerError); // Should have been caught by Planner
                    }
                }
                let header = source_df.header.clone();
                for row in source_df.get_data()? {
                    let mut new_row_bytes = Vec::new();

                    let split_fields = Self::split_row_into_fields(&row, &header)?;

                    for &idx in &mapping_indices {
                        new_row_bytes.extend_from_slice(&split_fields[idx]);
                    }
                    project_data.push(new_row_bytes);
                }

                Ok(DataFrame::from_memory("".to_string(), result_header, project_data))
            }

            PlanNode::Join { left, right, join_type, conditions } => {
                let mut left_source = self.get_row_source(left)?;
                let mut right_source = self.get_row_source(right)?;

                Ok(left_source.join(&right_source, conditions)?)
            }

            PlanNode::SetOperation { op, left, right } => {
                let left_df = self.execute_plan(left)?;
                let right_df = self.execute_plan(right)?;
                let left_header = left_df.header.clone();
                let mut result_data = left_df.get_data()?;
                let right_data = right_df.get_data()?;
                match op {
                    ParsedSetOperator::Union => {
                        // ToDo Remove Duplucates
                        result_data.extend(right_data);
                    }
                    ParsedSetOperator::Intersect => {
                        // ToDo this is O(N*M)
                        result_data.retain(|l_row| right_data.contains(l_row));
                    }
                    ParsedSetOperator::Except | ParsedSetOperator::Minus => {
                        result_data.retain(|l_row| !right_data.contains(l_row));
                    }
                    _ => {
                        // ToDo others
                        result_data.extend(right_data);
                    }
                }

                Ok(DataFrame::from_memory("".to_string(), left_header, result_data))
            }
        }
    }

    /// Reconstructs a single continuous byte vector from the BTree's split Key and Row.
    pub(crate) fn reconstruct_row(key: &Key, row: &Row, schema: &TableSchema) -> Result<Vec<u8>, Status> {
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

    fn get_row_source(&self, plan: &PlanNode) -> Result<DataFrame, Status> {
        match plan {
            PlanNode::SeqScan { table_id, operation, conditions, .. } => {
                let scan_source = self.create_scan_source(*table_id, operation.clone(), conditions.clone())?;
                Ok(DataFrame {
                    header: plan.get_schema(&self.schema).iter().map(|tuple|{tuple.1.clone()}).collect(),
                    identifier: "".to_string(),
                    row_source: Source::BTree(scan_source)
                })
            }
            _ => {
                self.execute_plan(plan)
            }
        }
    }

    fn create_scan_source<'a>(
        &'a self,
        table_id: usize,
        operation: SqlConditionOpCode,
        conditions: Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
    ) -> Result<BTreeScanSource, Status> {
        let schema = self.schema.tables[table_id].clone();
        let btree = Btree::init(
            self.btree_node_width,
            self.pager_accessor.clone(),
            schema.clone(),
        )?;

        Ok(BTreeScanSource::new(btree, schema, operation, conditions))
    }

    pub(crate) fn split_row_into_fields<'a>(row: &'a [u8], header: &[Field]) -> Result<Vec<&'a [u8]>, Status> {
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
    pub(crate) fn check_condition_on_bytes(
        row: &[u8],
        conditions: &Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
        header: &Vec<Field>,
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
                        identifier: "name".to_string(),
                    }),
                    (MASTER_TABLE_NAME.to_string(), Field {
                        field_type: Type::String,
                        identifier: "sql".to_string(),
                    }),
                     (MASTER_TABLE_NAME.to_string(), Field {
                        field_type: Type::Integer,
                        identifier: "rootpage".to_string(),
                    }),
                ],
            }
        };

        let result = self.execute_plan(&select_query.plan).expect("Failed Initialisation");
        let data = result.get_data().expect("Failed Initialisation");
        data.iter().for_each(|entry| {
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
