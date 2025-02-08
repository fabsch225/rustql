use std::cell::RefCell;
use crate::btree::Btree;
use crate::compiler::{CompiledDeleteQuery, CompiledQuery, CompiledSelectQuery, Compiler, SqlConditionOpCode, SqlStatementComparisonOperator};
use crate::pager::{Field, Key, PagerAccessor, PagerCore, Row, Schema, Type};
use crate::status::Status;
use crate::status::Status::ExceptionQueryMisformed;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{format, Display, Formatter};
use crate::compiler::SqlStatementComparisonOperator::{Equal, Greater, GreaterOrEqual, LesserOrEqual, Lesser};
use crate::parser::Parser;
use crate::serializer::Serializer;

#[derive(Debug)]
pub struct QueryResult {
    pub success: bool,
    pub result: DataFrame,
    status: Status
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
            status: ExceptionQueryMisformed
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
            status: Status::Success
        }
    }

    pub fn return_data(data: DataFrame) -> Self {
        QueryResult {
            success: true,
            result: data,
            status: Status::Success
        }
    }
}

#[derive(Debug)]
pub struct DataFrame {
    pub header: Vec<Field>,
    pub data: Vec<Vec<u8>>
}

impl DataFrame {
    pub fn new() -> Self {
        DataFrame {
            header: vec!(),
            data: vec!()
        }
    }

    pub fn msg(message: &str) -> Self {
        DataFrame {
            header: vec![Field{ field_type: Type::String, name: "Message".to_string() }],
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
                let formatted_value = Serializer::format_field(&field_value.to_vec(), field_type).unwrap();
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
    pub btree_node_width: usize
}

impl Executor {
    pub fn init(file_path: &str, t: usize) -> Self {
        let pager_accessor = PagerCore::init_from_file(file_path, t).expect("Unable to open database");
        Executor {
            pager_accessor: pager_accessor.clone(),
            query_cache: HashMap::new(),
            btree_node_width: t,
        }
    }

    pub fn debug(&self) {
        println!("Schema: {:?}", self.pager_accessor.read_schema());
        println!("B-Tree: {}", Btree::new(self.btree_node_width, self.pager_accessor.clone()).unwrap())
    }

    pub fn exit(&self) {
        self.pager_accessor.access_pager_write(|p| { p.flush() }).expect("Error Flushing the Pager");
    }

    pub fn exec(&self, query: String) -> QueryResult {
        let result = self.exec_intern(query);
        if !result.is_ok() { result.err().unwrap() }
        else { result.expect("nothing") }
    }

    fn exec_intern(&self, query: String) -> Result<QueryResult, QueryResult> {
        let mut parser = Parser::new(query);
        let parsed_query = parser.parse_query().map_err(|s|QueryResult::user_input_wrong(s))?;
        let compiled_query = Compiler::compile_and_plan(&self.pager_accessor.read_schema(), parsed_query)?;

        match compiled_query {
            CompiledQuery::CreateTable(q) => {
                self.pager_accessor.set_schema(q.schema);
                Ok(QueryResult::went_fine())
            }
            CompiledQuery::DropTable(q) => { todo!() }
            CompiledQuery::Select(q) => {
                let btree = Btree::new(self.btree_node_width, self.pager_accessor.clone()).map_err(|s|QueryResult::err(s))?;
                let schema = self.pager_accessor.read_schema();
                let result = RefCell::new(DataFrame::new());
                Self::set_header(&mut result.borrow_mut(), &q);
                let action = |key: &mut Key, row: &mut Row|Executor::exec_select(key, row, &mut result.borrow_mut(), &q, &schema);
                Self::exec_action_with_condition(&btree, &schema, &q.operation, &q.conditions, &action).map_err(|s|QueryResult::err(s))?;
                Ok(QueryResult::return_data(result.into_inner()))
            }
            CompiledQuery::Insert(q) => {
                let mut btree = Btree::new(self.btree_node_width, self.pager_accessor.clone()).map_err(|s|QueryResult::err(s))?;
                btree.insert(q.data.0, q.data.1).map_err(|s|QueryResult::err(s))?;
                Ok(QueryResult::went_fine())
            }
            CompiledQuery::Delete(q) => {
                let mut btree = Btree::new(self.btree_node_width, self.pager_accessor.clone()).map_err(|s|QueryResult::err(s))?;
                let schema = self.pager_accessor.read_schema();
                println!("{}", btree);
                //current status: infinite Loop
               /* let action = |key: &mut Key, row: &mut Row|Executor::exec_delete(key, row, &q, &schema);
                Self::exec_action_with_condition(&btree, &schema, &q.operation, &q.conditions, &action).map_err(|s|QueryResult::err(s))?;

                //this should be periodical, but for debugging
                btree.tomb_cleanup();*/

                //this is for debugging:
                let result = RefCell::new(vec![]);
                let action = |key: &mut Key, row: &mut Row|Executor::exec_key_collect(key, row, &mut result.borrow_mut(), &q, &schema);
                Self::exec_action_with_condition(&btree, &schema, &q.operation, &q.conditions, &action).map_err(|s|QueryResult::err(s))?;
                for key in result.into_inner() {
                    println!("deleting {}", Serializer::format_key(&key, &schema).unwrap());
                    btree.delete(key).map_err(|s|QueryResult::err(s))?;
                    println!("{}", btree);
                }
                Ok(QueryResult::went_fine())
            }
        }
    }

    fn set_header(result: &mut DataFrame, query: &CompiledSelectQuery) {
        result.header = query.result.clone();
    }

    fn exec_action_with_condition<Action>(btree: &Btree, schema: &Schema, op_code: &SqlConditionOpCode, conditions:  &Vec<(SqlStatementComparisonOperator, Vec<u8>)>, action: &Action) -> Result<(), Status>
        where Action: Fn(&mut Key, &mut Row) -> Result<bool, Status>  + Copy
    {
        match op_code {
            SqlConditionOpCode::SelectFTS => { btree.scan(action) }
            SqlConditionOpCode::SelectFromIndex => { todo!() }
            SqlConditionOpCode::SelectAtIndex => { todo!() }
            SqlConditionOpCode::SelectFromKey => {
                let range_start;
                let range_end;
                let include_start;
                let include_end;
                let _ = match conditions[0].0 {
                    SqlStatementComparisonOperator::None => { return Err(Status::InternalExceptionCompilerError); }
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
                    Equal => { return Err(Status::InternalExceptionCompilerError); }
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
            SqlConditionOpCode::SelectAtKey => {
                btree.find(conditions[0].1.clone(), &action)
            }
        }
    }

    fn exec_key_collect(key: &mut Key, row: &mut Row, all_keys: &mut Vec<Key>,query: &CompiledDeleteQuery, schema: &Schema) -> Result<bool, Status> {
        if Serializer::is_tomb(key, &schema)? {
            return Ok(false);
        }
        if !Executor::exec_condition_on_row(row, &query.conditions, schema) {
            return Ok(false);
        }
        all_keys.push(key.clone());
        Ok(false)
    }

    fn exec_delete(key: &mut Key, row: &mut Row, query: &CompiledDeleteQuery, schema: &Schema) -> Result<bool, Status> {
        if Serializer::is_tomb(key, &schema)? {
            return Ok(false);
        }
        if !Executor::exec_condition_on_row(row,  &query.conditions, schema) {
            return Ok(false);
        }
        Serializer::set_is_tomb(key, true, &schema)?;
        Ok(true)
    }

    fn exec_select(key: &mut Key, row: &mut Row, result: &mut DataFrame, query: &CompiledSelectQuery, schema: &Schema) -> Result<bool, Status> {
        if Serializer::is_tomb(key, &schema)? {
            return Ok(false);
        }
        if !Executor::exec_condition_on_row(row, &query.conditions, schema) {
            return Ok(false);
        }

        let mut data_row = Vec::new();

        for field in &query.result {
            let field_index = schema.fields.iter().position(|f| f.name == field.name).unwrap();
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

    fn exec_condition_on_row(row: &Row, conditions: &Vec<(SqlStatementComparisonOperator, Vec<u8>)>, schema: &Schema) -> bool {
        let mut position = 0;
        let mut skip = true;
        for i in 0..schema.fields.len() {
            if skip { skip = false; continue } //in schema.fields, the key is listed
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
            let cmp_result = Serializer::compare_with_type(&row_field, &conditions[i].1, &field_type).unwrap();
            if !match cmp_result {
                std::cmp::Ordering::Equal => {
                    field_condition == LesserOrEqual || field_condition == GreaterOrEqual || field_condition == Equal
                },
                std::cmp::Ordering::Greater => { field_condition == Greater },
                std::cmp::Ordering::Less => { field_condition == Lesser },
            } { return false }
        }
        true
    }
}