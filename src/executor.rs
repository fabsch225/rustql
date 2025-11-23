use crate::btree::Btree;
use crate::cursor::BTreeCursor;
use crate::dataframe::{
    BTreeScanSource, DataFrame, JoinStrategy, MemorySource, RowSource, SetOpStrategy, Source,
};
use crate::pager::{
    Key, PAGE_SIZE, PAGE_SIZE_WITH_META, PagerAccessor, PagerCore, Position, Row, TableName, Type,
};
use crate::pager_proxy::PagerProxy;
use crate::parser::JoinType::Natural;
use crate::parser::{JoinType, ParsedSetOperator, Parser};
use crate::planner::SqlStatementComparisonOperator::{
    Equal, Greater, GreaterOrEqual, Lesser, LesserOrEqual,
};
use crate::planner::{
    CompiledCreateTableQuery, CompiledDeleteQuery, CompiledInsertQuery, CompiledQuery,
    CompiledSelectQuery, PlanNode, Planner, SqlConditionOpCode, SqlStatementComparisonOperator,
};
pub(crate) use crate::schema::{Field, Schema, TableIndex, TableSchema};
use crate::serializer::Serializer;
use crate::status::Status;
use crate::status::Status::ExceptionQueryMisformed;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Display, Formatter, format};
use std::fs::OpenOptions;
use std::io::{ErrorKind, Write};

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
                table_index: TableIndex {
                    index: vec![TableName::from(MASTER_TABLE_NAME.to_string())],
                },
                tables: vec![Self::make_master_table_schema()],
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
                let result_df = self
                    .exec_planned_tree(&q.plan)
                    .map_err(|s| QueryResult::err(s))?;
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
                let mut source = self
                    .create_scan_source(q.table_id, q.operation, q.conditions.clone())
                    .map_err(|s| QueryResult::err(s))?;

                let schema = &self.schema.tables[q.table_id];
                let key_offset = {
                    let mut offset = 0;
                    for i in 0..schema.key_position {
                        offset += Serializer::get_size_of_type(&schema.fields[i].field_type)
                            .map_err(QueryResult::err)?;
                    }
                    offset
                };
                let key_len =
                    Serializer::get_size_of_type(&schema.fields[schema.key_position].field_type)
                        .map_err(QueryResult::err)?;

                let mut keys_to_delete = Vec::new();

                source.reset().map_err(QueryResult::err)?;
                while let Some(row) = source.next().map_err(QueryResult::err)? {
                    if row.len() < key_offset + key_len {
                        return Err(QueryResult::err(
                            Status::InternalExceptionIntegrityCheckFailed,
                        ));
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

    fn exec_planned_tree(&self, plan: &PlanNode) -> Result<DataFrame, Status> {
        match plan {
            PlanNode::SeqScan {
                table_id,
                table_name,
                operation,
                conditions,
            } => {
                let schema = self.schema.tables[*table_id].clone();
                let btree = Btree::init(
                    self.btree_node_width,
                    self.pager_accessor.clone(),
                    schema.clone(),
                )?;
                Ok(DataFrame::from_table(
                    table_name.clone(),
                    plan.get_header(&self.schema)?,
                    btree,
                    operation.clone(),
                    conditions.clone(),
                ))
            }

            PlanNode::Filter { source, conditions } => {
                let source_df = self.exec_planned_tree(source)?;
                Ok(source_df.filter(conditions.clone()))
            }

            PlanNode::Project { source, fields } => {
                let source_df = self.exec_planned_tree(source)?;
                //ToDo compute these in the Planner
                let mut mapping_indices = Vec::new();
                for req_field in fields {
                    if let Some(idx) = source_df
                        .header
                        .iter()
                        .position(|f| f.name == req_field.name)
                    {
                        mapping_indices.push(idx);
                    } else {
                        return Err(Status::InternalExceptionCompilerError);
                    }
                }

                Ok(source_df.project(fields.clone(), mapping_indices))
            }

            PlanNode::Join {
                left,
                right,
                conditions,
                join_type,
                ..
            } => {
                debug_assert!(*join_type == JoinType::Inner || *join_type == Natural);
                debug_assert_eq!(conditions.len(), 1);
                let mut left_source = self.get_row_source(left)?;
                let right_source = self.get_row_source(right)?;
                let left_col = conditions[0].0.clone();
                let right_col = conditions[0].1.clone();
                let l_idx = left_source
                    .header
                    .iter()
                    .position(|f| f.name == left_col.name && f.table_name == left_col.table_name)
                    .ok_or(Status::DataFrameJoinError)?;
                let r_idx = right_source
                    .header
                    .iter()
                    .position(|f| f.name == right_col.name && f.table_name == right_col.table_name)
                    .ok_or(Status::DataFrameJoinError)?;
                Ok(left_source.join(right_source, l_idx, r_idx, JoinStrategy::Hash)?)
            }

            PlanNode::SetOperation { op, left, right } => {
                let left_df = self.exec_planned_tree(left)?;
                let right_df = self.exec_planned_tree(right)?;

                Ok(left_df.set_operation(right_df, op.clone(), SetOpStrategy::HashedMemory)?)
            }
        }
    }

    fn get_row_source(&self, plan: &PlanNode) -> Result<DataFrame, Status> {
        match plan {
            PlanNode::SeqScan {
                table_id,
                operation,
                conditions,
                ..
            } => {
                let scan_source =
                    self.create_scan_source(*table_id, operation.clone(), conditions.clone())?;
                Ok(DataFrame {
                    header: plan.get_header(&self.schema)?,
                    identifier: plan.get_schema(&self.schema)?.name,
                    row_source: Source::BTree(scan_source),
                })
            }
            _ => self.exec_planned_tree(plan),
        }
    }

    fn create_scan_source(
        &self,
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
                    Field {
                        field_type: Type::String,
                        name: "name".to_string(),
                        table_name: MASTER_TABLE_NAME.to_string(),
                    },
                    Field {
                        field_type: Type::String,
                        name: "sql".to_string(),
                        table_name: MASTER_TABLE_NAME.to_string(),
                    },
                    Field {
                        field_type: Type::Integer,
                        name: "rootpage".to_string(),
                        table_name: MASTER_TABLE_NAME.to_string(),
                    },
                ],
            },
        };

        let result = self
            .exec_planned_tree(&select_query.plan)
            .expect("Failed Initialisation");
        let data = result.fetch_data().expect("Failed Initialisation");
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
            }
            _ => {
                panic!("wtf")
            }
        }
    }
}
