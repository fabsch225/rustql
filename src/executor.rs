use crate::btree::Btree;
use crate::pager::{Key, PagerAccessor, PagerCore, Position, Row, TableName, Type};
use crate::parser::Parser;
use crate::planner::SqlStatementComparisonOperator::{
    Equal, Greater, GreaterOrEqual, Lesser, LesserOrEqual,
};
use crate::planner::{
    CompiledCreateTableQuery, CompiledDeleteQuery, CompiledQuery, CompiledSelectQuery, Planner,
    SqlConditionOpCode, SqlStatementComparisonOperator,
};
use crate::serializer::Serializer;
use crate::status::Status;
use crate::status::Status::ExceptionQueryMisformed;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{format, Display, Formatter};
use std::fs::OpenOptions;
use std::io::{ErrorKind, Write};

const MASTER_TABLE_NAME: &str = "rustsql_master";

pub static MASTER_TABLE_SQL: &str = "CREATE TABLE rustsql_master (
        name STRING,
        type STRING,
        rootpage INTEGER,
        sql STRING
    )";
#[derive(Clone, Debug)]
pub struct Schema {
    pub(crate) table_index: TableIndex,
    pub(crate) tables: Vec<TableSchema>,
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
    pub(crate) index: Vec<TableName>,
}

#[derive(Clone, Debug)]
pub struct TableSchema {
    pub next_position: Position,
    pub root: Position, //if 0 -> no tree
    pub col_count: usize,
    pub whole_row_length: usize,
    pub key_length: usize, //includes flag
    pub key_type: Type,
    pub row_length: usize,
    pub fields: Vec<Field>,
    pub table_type: u8,
    pub entry_count: i32, //Todo Fields
                          // count (of entries)
}

impl TableSchema {
    pub fn has_valid_root(&self) -> bool {
        !self.root.is_empty()
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

/// ## Responsibilities
/// - Misc
/// - Managing a cache for queries
/// - executing compiled queries
pub struct Executor {
    pub pager_accessor: PagerAccessor,
    pub query_cache: HashMap<String, CompiledQuery>, //must be invalidated once schema is changed or in a smart way
    pub schema: Schema,
    pub btree_node_width: usize
}

impl Executor {
    pub fn init(file_path: &str, t: usize) -> Self {
        let master_table_schema = Self::make_master_table_schema();
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
        Executor {
            pager_accessor: pager_accessor.clone(),
            query_cache: HashMap::new(),
            schema: Schema {
                table_index: TableIndex {
                    index: vec![TableName::from(MASTER_TABLE_NAME)],
                },
                tables: vec![master_table_schema],
            },
            btree_node_width: t
        }
    }

    pub fn debug(&self) {
        println!("Schema: {:?}", self.schema);
        println!(
            "B-Tree: {}",
            Btree::new(
                self.btree_node_width,
                self.pager_accessor.clone(),
                self.schema.tables[0].clone()
            )
            .unwrap()
        )
    }

    pub fn exit(&self) {
        self.pager_accessor
            .access_pager_write(|p| p.flush())
            .expect("Error Flushing the Pager");
    }

    pub fn exec(&self, query: String) -> QueryResult {
        let result = self.exec_intern(query, false);
        if !result.is_ok() {
            result.err().unwrap()
        } else {
            result.expect("just checked")
        }
    }

    fn exec_intern(&self, query: String, allow_modification_to_system_table: bool) -> Result<QueryResult, QueryResult> {
        let mut parser = Parser::new(query.clone());
        let parsed_query = parser
            .parse_query()
            .map_err(|s| QueryResult::user_input_wrong(s))?;
        let compiled_query = Planner::plan(&self.schema, parsed_query)?;
        match compiled_query {
            CompiledQuery::CreateTable(q) => {
                let insert_query = format!(
                    "INSERT INTO {} (name, type, rootpage, sql) VALUES ({}, {}, {}, '{}')",
                    MASTER_TABLE_NAME,
                    q.table_name,
                    0,
                    self.pager_accessor.get_next_page_index(),
                    query
                );
                println!("{}", insert_query);
                self.exec_intern(insert_query, true)
            }
            CompiledQuery::DropTable(q) => {
                todo!()
            }
            CompiledQuery::Select(q) => {
                let schema = &self.schema.tables[0];
                let btree = Btree::new(
                    self.btree_node_width,
                    self.pager_accessor.clone(),
                    schema.clone(),
                )
                .map_err(|s| QueryResult::err(s))?;
                let result = RefCell::new(DataFrame::new());
                Self::set_header(&mut result.borrow_mut(), &q);
                let action = |key: &mut Key, row: &mut Row| {
                    Executor::exec_select(key, row, &mut result.borrow_mut(), &q, schema)
                };
                Self::exec_action_with_condition(
                    &btree,
                    &schema,
                    &q.operation,
                    &q.conditions,
                    &action,
                )
                .map_err(|s| QueryResult::err(s))?;
                Ok(QueryResult::return_data(result.into_inner()))
            }
            CompiledQuery::Insert(q) => {
                let schema = &self.schema.tables[0];
                let mut btree = Btree::new(
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
                let schema = &self.schema.tables[0];
                let btree = Btree::new(
                    self.btree_node_width,
                    self.pager_accessor.clone(),
                    schema.clone(),
                )
                .map_err(|s| QueryResult::err(s))?;
                let action =
                    |key: &mut Key, row: &mut Row| Self::exec_delete(key, row, &q, &schema);
                Self::exec_action_with_condition(
                    &btree,
                    &schema,
                    &q.operation,
                    &q.conditions,
                    &action,
                )
                .map_err(|s| QueryResult::err(s))?;
                //this should be periodical, but for now
                //btree.tomb_cleanup();
                Ok(QueryResult::went_fine())
            }
        }
    }

    fn set_header(result: &mut DataFrame, query: &CompiledSelectQuery) {
        result.header = query.result.clone();
    }

    fn exec_action_with_condition<Action>(
        btree: &Btree,
        schema: &TableSchema,
        op_code: &SqlConditionOpCode,
        conditions: &Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
        action: &Action,
    ) -> Result<(), Status>
    where
        Action: Fn(&mut Key, &mut Row) -> Result<bool, Status> + Copy,
    {
        match op_code {
            SqlConditionOpCode::SelectFTS => btree.scan(action),
            SqlConditionOpCode::SelectIndexRange => {
                todo!()
            }
            SqlConditionOpCode::SelectIndexUnique => {
                todo!()
            }
            SqlConditionOpCode::SelectKeyRange => {
                let range_start;
                let range_end;
                let include_start;
                let include_end;
                let _ = match conditions[0].0 {
                    SqlStatementComparisonOperator::None => {
                        return Err(Status::InternalExceptionCompilerError);
                    }
                    Lesser => {
                        range_start = Serializer::negative_infinity(&schema.fields[0].field_type);
                        range_end = conditions[0].1.clone();
                        include_start = true;
                        include_end = false;
                    }
                    Greater => {
                        range_start = conditions[0].1.clone();
                        range_end = Serializer::infinity(&schema.fields[0].field_type);
                        include_start = false;
                        include_end = true;
                    }
                    Equal => {
                        range_start = conditions[0].1.clone();
                        range_end = conditions[0].1.clone();
                        include_start = true;
                        include_end = true;
                    }
                    LesserOrEqual => {
                        range_start = Serializer::negative_infinity(&schema.fields[0].field_type);
                        range_end = conditions[0].1.clone();
                        include_start = true;
                        include_end = true;
                    }
                    GreaterOrEqual => {
                        range_start = conditions[0].1.clone();
                        range_end = Serializer::infinity(&schema.fields[0].field_type);
                        include_start = true;
                        include_end = true;
                    }
                };
                btree.find_range(range_start, range_end, include_start, include_end, action)
            }
            SqlConditionOpCode::SelectKeyUnique => btree.find(conditions[0].1.clone(), &action),
        }
    }

    fn exec_key_collect(
        key: &mut Key,
        row: &mut Row,
        all_keys: &mut Vec<Key>,
        query: &CompiledDeleteQuery,
        schema: &TableSchema,
    ) -> Result<bool, Status> {
        if Serializer::is_tomb(key, &schema)? {
            return Ok(false);
        }
        if !Executor::exec_condition_on_row(row, &query.conditions, schema) {
            return Ok(false);
        }
        all_keys.push(key.clone());
        Ok(false)
    }

    fn exec_delete(
        key: &mut Key,
        row: &mut Row,
        query: &CompiledDeleteQuery,
        schema: &TableSchema,
    ) -> Result<bool, Status> {
        if Serializer::is_tomb(key, &schema)? {
            return Ok(false);
        }
        if !Executor::exec_condition_on_row(row, &query.conditions, schema) {
            return Ok(false);
        }
        Serializer::set_is_tomb(key, true, &schema)?;
        Ok(true)
    }

    fn exec_select(
        key: &mut Key,
        row: &mut Row,
        result: &mut DataFrame,
        query: &CompiledSelectQuery,
        schema: &TableSchema,
    ) -> Result<bool, Status> {
        if Serializer::is_tomb(key, &schema)? {
            return Ok(false);
        }
        if !Executor::exec_condition_on_row(row, &query.conditions, schema) {
            return Ok(false);
        }

        let mut data_row = Vec::new();

        for field in &query.result {
            let field_index = schema
                .fields
                .iter()
                .position(|f| f.name == field.name) //TODO optimize that
                .unwrap();
            let field_type = &schema.fields[field_index].field_type;

            if field_index == 0 {
                data_row.append(&mut key.clone());
            } else {
                let mut position = 0;
                for i in 1..field_index {
                    position += Serializer::get_size_of_type(&schema.fields[i].field_type).unwrap();
                }
                let field_len = Serializer::get_size_of_type(field_type).unwrap();
                data_row.append(&mut row[position..position + field_len].to_vec());
            }
        }

        result.data.push(data_row);
        Ok(false)
    }

    fn exec_condition_on_row(
        row: &Row,
        conditions: &Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
        schema: &TableSchema,
    ) -> bool {
        let mut position = 0;
        let mut skip = true;
        for i in 0..schema.fields.len() {
            if skip {
                skip = false;
                continue;
            } //in schema.fields, the key is listed
            let schema_field = &schema.fields[i];
            let field_condition = conditions[i].0.clone();
            let field_type = &schema_field.field_type;
            let field_len = Serializer::get_size_of_type(field_type).unwrap();
            if field_condition == SqlStatementComparisonOperator::None {
                position += field_len;
                continue;
            }
            let row_field = row[position..(position + field_len)].to_vec();
            position += field_len;
            let cmp_result =
                Serializer::compare_with_type(&row_field, &conditions[i].1, &field_type).unwrap();
            if !match cmp_result {
                std::cmp::Ordering::Equal => {
                    field_condition == LesserOrEqual
                        || field_condition == GreaterOrEqual
                        || field_condition == Equal
                }
                std::cmp::Ordering::Greater => field_condition == Greater,
                std::cmp::Ordering::Less => field_condition == Lesser,
            } {
                return false;
            }
        }
        true
    }
    pub fn check_integrity(&self) -> Result<(), Status> {
        let mut btree = Btree::new(self.btree_node_width, self.pager_accessor.clone(), self.schema.tables[0].clone())?;
        let table_schema = self.schema.tables[0].clone();
        let mut last_key: RefCell<Option<Key>> = RefCell::new(None);
        let mut valid = RefCell::new(true);
        let action = |key: &mut Key, row: &mut Row| {
            if Serializer::is_tomb(key, &table_schema)? {
                return Ok(false);
            }
            let mut last_key_mut = last_key.borrow_mut();
            if let Some(ref last_key) = *last_key_mut {
                if Serializer::compare_with_type(last_key, key, &table_schema.key_type)?
                    != std::cmp::Ordering::Less
                {
                    *valid.borrow_mut() = false;
                }
            }
            *last_key_mut = Some(key.clone());
            Ok(false)
        };

        //this wouldnt consider tombstones
        btree.scan(&action)?;

        if *valid.borrow() {
            Ok(())
        } else {
            Err(Status::InternalExceptionIntegrityCheckFailed)
        }
    }

    pub fn create_database(file_name: &str) -> Result<(), Status> {
        match OpenOptions::new().create_new(true).read(true).write(true).open(file_name).unwrap().write(&[0,0]) {
            Ok(f) => Ok(()),
            _ => Err(Status::InternalExceptionFileOpenFailed),
        }
    }

    fn make_master_table_schema() -> TableSchema {
        let mut parser = Parser::new(MASTER_TABLE_SQL.parse().unwrap());
        let parsed_query = parser
            .parse_query()
            .expect("why would there be an error here");
        let compiled_query = Planner::plan(&Schema::make_empty(), parsed_query);
        match compiled_query {
            Ok(CompiledQuery::CreateTable(create)) => create.schema,
            _ => {
                panic!("wtf")
            }
        }
    }
}
