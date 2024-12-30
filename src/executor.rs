use crate::btree::Btree;
use crate::compiler::{CompiledQuery, Compiler, SqlStatementComparisonOperator};
use crate::pager::{PagerAccessor, PagerCore, Row, Schema};
use crate::status::Status;
use crate::status::Status::ExceptionQueryMisformed;
use std::collections::HashMap;
use crate::compiler::SqlStatementComparisonOperator::{Equal, Greater, GreaterOrEqual, LesserOrEqual, Lesser};
use crate::parser::Parser;
use crate::serializer::Serializer;

#[derive(Debug)]
pub struct QueryResult {
    success: bool,
    result: String,
    status: Status
}

impl QueryResult {
    pub fn user_input_wrong(msg: String) -> Self {
        QueryResult {
            success: false,
            result: msg,
            status: ExceptionQueryMisformed
        }
    }

    pub fn went_fine() -> Self {
        QueryResult {
            success: false,
            result: "Query Executed Successfully".to_string(),
            status: ExceptionQueryMisformed
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
    pub btree_node_width: usize
}

impl Executor {
    pub fn init(file_path: &str, t: usize) -> Self {
        let pager_accessor = PagerCore::init_from_file(file_path).expect("Unable to open database");
        Executor {
            pager_accessor: pager_accessor.clone(),
            query_cache: HashMap::new(),
            btree_node_width: t,
        }
    }

    pub fn exec(&self, query: String) -> QueryResult {
        let result = self.exec_intern(query);
        if !result.is_ok() { result.err().unwrap() }
        else { result.expect("nothing") }
    }

    fn exec_intern(&self, query: String) -> Result<QueryResult, QueryResult> {
        let mut parser = Parser::new(query);
        let parsed_query = parser.parse_query().map_err(|s|QueryResult::user_input_wrong(s))?;
        let compiled_query = Compiler::compile(&self.pager_accessor.get_schema_read(), parsed_query)?;

        match compiled_query {
            CompiledQuery::CreateTable(q) => { todo!() }
            CompiledQuery::DropTable(q) => { todo!() }
            CompiledQuery::Select(q) => {
                let mut btree = Btree::new(self.btree_node_width, self.pager_accessor.clone());
                let schema = self.pager_accessor.get_schema_read();
                let scan_data = btree.scan();
                //let mut result_data = (vec![], vec![]);
                for i in 0..scan_data.0.len() {
                    if Self::exec_condition(&scan_data.1[i], &q.conditions, &schema) {
                        //result_data.0.push(scan_data.0[i].clone());
                        //result_data.1.push(scan_data.1[i].clone());
                        print!("{}", Serializer::format_key(&scan_data.0[i], &schema).unwrap());
                        print!("; ");
                        print!("{}", Serializer::format_row(&scan_data.1[i], &schema).unwrap());
                        println!();
                    }
                }
                //println!("{:?}", result_data);
                Ok(QueryResult::went_fine())
            }
            CompiledQuery::Insert(q) => {
                let mut btree = Btree::new(self.btree_node_width, self.pager_accessor.clone());
                btree.insert(q.data.0, q.data.1);
                println!("{}", btree);
                Ok(QueryResult::went_fine())
            }
        }
    }

    pub fn exec_condition(row: &Row, condition: &Vec<(SqlStatementComparisonOperator, Vec<u8>)>, schema: &Schema) -> bool {
        let mut position = 0;
        let mut skip = true;
        for i in 0..schema.fields.len() {
            if skip { skip = false; continue } //in schema.fields, the key is listed
            let schema_field = &schema.fields[i];
            let field_condition = condition[i].0.clone();
            let field_type = &schema_field.field_type;
            let field_len = Serializer::get_size_of_type(field_type).unwrap();
            if field_condition == SqlStatementComparisonOperator::None {
                position += field_len;
                continue;
            }
            let row_field = row[position..(position + field_len)].to_vec();
            position += field_len;
            let cmp_result = Serializer::compare_with_type(&row_field, &condition[i].1, field_type.clone()).unwrap();
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