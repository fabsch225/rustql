use crate::btree::Btree;
use crate::compiler::{CompiledQuery, Compiler};
use crate::pager::{PagerAccessor, PagerCore};
use crate::status::Status;
use crate::status::Status::ExceptionQueryMisformed;
use std::collections::HashMap;
use crate::parser::Parser;

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
            CompiledQuery::Select(q) => { todo!() }
            CompiledQuery::Insert(q) => {
                //the btree creates a new root everytime
                //we should store the tree root in the table schema!
                let mut btree = Btree::new(self.btree_node_width, self.pager_accessor.clone());
                btree.insert(q.data.0, q.data.1);
                println!("{}", btree);
                println!("{:?}", btree.scan());
                Ok(QueryResult::went_fine())
            }
        }
    }
}