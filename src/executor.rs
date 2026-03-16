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
use crate::parser::{JoinOp, JoinType, ParsedSetOperator, Parser};
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
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::{Display, Formatter, format};
use std::fs::OpenOptions;
use std::io::{ErrorKind, Write};

const MASTER_TABLE_NAME: &str = "rustsql_master";

pub static MASTER_TABLE_SQL: &str = "CREATE TABLE rustsql_master (
        name STRING,
        type STRING,
        rootpage INTEGER,
    sql STRING,
    free_list STRING
    )";

#[derive(Debug)]
pub struct QueryResult {
    pub success: bool,
    pub data: DataFrame,
    status: Status,
}

impl Display for QueryResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}\t", self.data)
    }
}

impl QueryResult {
    pub fn user_input_wrong(msg: String) -> Self {
        QueryResult {
            success: false,
            data: DataFrame::msg(msg.as_str()),
            status: ExceptionQueryMisformed,
        }
    }

    // TODO rename
    pub fn msg(str: &str) -> QueryResult {
        QueryResult {
            success: false,
            data: DataFrame::msg(str),
            status: ExceptionQueryMisformed,
        }
    }

    // TODO rename
    pub fn err(s: Status) -> Self {
        QueryResult {
            success: false,
            data: DataFrame::msg(format!("{:?}", s).as_str()),
            status: s,
        }
    }

    pub fn went_fine() -> Self {
        QueryResult {
            success: true,
            data: DataFrame::msg("Query Executed Successfully"),
            status: Status::Success,
        }
    }

    pub fn return_data(data: DataFrame) -> Self {
        QueryResult {
            success: true,
            data: data,
            status: Status::Success,
        }
    }
}

/// ## Responsibilities
/// - Misc
/// - Managing a cache for queries
/// - executing compiled queries
pub struct QueryExecutor {
    pub pager_accessor: PagerAccessor,
    pub query_cache: HashMap<String, CompiledQuery>, //must be invalidated once schema is changed or in a smart way
    pub schema: Schema,
    pub btree_node_width: usize,
    request_counter: usize,
    last_write_table_id: Option<usize>,
}

impl QueryExecutor {
    fn tree_order_for(schema: &TableSchema, fallback: usize) -> usize {
        if schema.btree_order == 0 {
            fallback
        } else {
            schema.btree_order
        }
    }

    fn encode_free_list_top_10(table: &TableSchema) -> String {
        let mut entries = table.free_list.clone();
        entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        entries.truncate(10);
        entries
            .iter()
            .map(|(page, slots)| format!("{}:{}", page, slots))
            .collect::<Vec<String>>()
            .join(",")
    }

    pub fn init(file_path: &str, t: usize) -> Self {
        let mut pager_accessor = match PagerCore::init_from_file(file_path) {
            Ok(pa) => pa,
            Err(e) => {
                println!("{:?}", e);
                match e {
                    Status::InternalExceptionFileNotFound => {
                        let _ = Self::create_database(file_path);
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

        if pager_accessor.get_next_page_index() < 2 {
            Self::initialize_database_file(file_path)
                .expect("Failed to initialize empty database file");
            pager_accessor = PagerCore::init_from_file(file_path)
                .expect("Failed to re-open pager after initializing database file");
        }

        let mut bootstrap_executor = QueryExecutor {
            pager_accessor: pager_accessor.clone(),
            query_cache: HashMap::new(),
            schema: Schema {
                table_index: TableIndex {
                    index: vec![TableName::from(MASTER_TABLE_NAME.to_string())],
                },
                tables: vec![Self::make_master_table_schema()],
            },
            btree_node_width: t,
            request_counter: 0,
            last_write_table_id: None,
        };

        bootstrap_executor.schema = bootstrap_executor.load_schema();
        bootstrap_executor
    }

    pub fn debug_lite(&self, table: Option<&str>) {
        if table.is_none() {
            println!(
                "System Table: {}",
                Btree::init(
                    Self::tree_order_for(&self.schema.tables[0], self.btree_node_width),
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
                Self::tree_order_for(&table_schema, self.btree_node_width),
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
                    Self::tree_order_for(&self.schema.tables[0], self.btree_node_width),
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
                Self::tree_order_for(&table_schema, self.btree_node_width),
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
            self.prepare("SELECT * FROM rustsql_master".to_string())
        );
    }

    pub fn exit(&self) {
        self.pager_accessor
            .access_pager_write(|p| p.flush())
            .expect("Error Flushing the Pager");
    }

    pub fn prepare(&mut self, query: String) -> QueryResult {
        self.request_counter += 1;
        self.last_write_table_id = None;
        let result = self.execute_compiled_query(query, false);

        if self.request_counter % 120 == 0 {
            if let Some(table_id) = self.last_write_table_id {
                if let Err(e) = self.refresh_table_free_list(table_id) {
                    eprintln!("Failed to refresh free-list for table {}: {:?}", table_id, e);
                } else if let Err(e) = self.persist_free_lists_snapshot_to_system_table() {
                    eprintln!("Failed to persist free-lists to system table: {:?}", e);
                }
            }
        }
        if !result.is_ok() {
            result.err().unwrap()
        } else {
            result.expect("just checked")
        }
    }

    pub fn execute_readonly(&self, query: String) -> QueryResult {
        let result = self.execute_readonly_intern(query);
        if !result.is_ok() {
            result.err().unwrap()
        } else {
            result.expect("just checked")
        }
    }

    pub fn planner_feedback_is_readonly(&self, query: &str) -> Result<bool, QueryResult> {
        let compiled_query = self.compile_query(query)?;
        Ok(Planner::is_readonly_query(&compiled_query))
    }

    fn compile_query(&self, query: &str) -> Result<CompiledQuery, QueryResult> {
        let mut parser = Parser::new(query.to_string());
        let parsed_query = parser
            .parse_query()
            .map_err(|s| QueryResult::user_input_wrong(s))?;
        Planner::plan(&self.schema, parsed_query)
    }

    fn execute_compiled_query(
        &mut self,
        query: String,
        allow_modification_to_system_table: bool,
    ) -> Result<QueryResult, QueryResult> {
        let compiled_query = self.compile_query(&query)?;
        self.exec_compiled(
            compiled_query,
            query,
            allow_modification_to_system_table,
        )
    }

    fn execute_readonly_intern(&self, query: String) -> Result<QueryResult, QueryResult> {
        let compiled_query = self.compile_query(&query)?;
        match compiled_query {
            CompiledQuery::Select(q) => {
                let result_df = self
                    .exec_planned_tree(&q.plan)
                    .map_err(|s| QueryResult::err(s))?;
                Ok(QueryResult::return_data(result_df))
            }
            _ => Err(QueryResult::user_input_wrong(
                "query is not read-only".to_string(),
            )),
        }
    }

    fn exec_compiled(
        &mut self,
        compiled_query: CompiledQuery,
        query: String,
        allow_modification_to_system_table: bool,
    ) -> Result<QueryResult, QueryResult> {
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

                let mut table_schema = q.schema.clone();
                table_schema.btree_order = self.btree_node_width;
                let page_capacity = table_schema
                    .max_nodes_per_page()
                    .map_err(QueryResult::err)?;
                let initial_free = page_capacity.saturating_sub(1);
                table_schema.free_list = vec![(root_page, initial_free)];
                let free_list_encoded = Self::encode_free_list_top_10(&table_schema);

                let insert_query = format!(
                    "INSERT INTO {} (name, type, rootpage, sql, free_list) VALUES ('{}', '{}', {}, '{}', '{}')",
                    MASTER_TABLE_NAME,
                    q.table_name.replace("'", "''"),
                    "table",
                    root_page,
                    query.replace("'", "''"),
                    free_list_encoded.replace("'", "''")
                );
                self.execute_compiled_query(insert_query, true)?;
                let result = self.reload_schema()?;
                if !allow_modification_to_system_table {
                    let created_table_id = Planner::find_table_id(&self.schema, &q.table_name)?;
                    self.last_write_table_id = Some(created_table_id);
                }
                Ok(result)
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
                if !allow_modification_to_system_table && q.table_id == 0 {
                    return Err(QueryResult::msg("You are not allowed to modify this table."));
                }

                let mut schema = self.schema.tables[q.table_id].clone();
                if allow_modification_to_system_table && q.table_id == 0 {
                    schema.free_list.clear();
                }
                let mut btree = Btree::init(
                    Self::tree_order_for(&schema, self.btree_node_width),
                    self.pager_accessor.clone(),
                    schema,
                )
                .map_err(|s| QueryResult::err(s))?;
                btree
                    .insert(q.data.0, q.data.1)
                    .map_err(|s| QueryResult::err(s))?;
                if !allow_modification_to_system_table {
                    self.last_write_table_id = Some(q.table_id);
                }
                Ok(QueryResult::went_fine())
            }
            CompiledQuery::Delete(q) => {
                if !allow_modification_to_system_table && q.table_id == 0 {
                    return Err(QueryResult::msg("You are not allowed to modify this table."));
                }

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

                let mut btree_schema = schema.clone();
                if allow_modification_to_system_table && q.table_id == 0 {
                    btree_schema.free_list.clear();
                }

                let mut btree = Btree::init(
                    Self::tree_order_for(&btree_schema, self.btree_node_width),
                    self.pager_accessor.clone(),
                    btree_schema,
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
                    Self::tree_order_for(&schema, self.btree_node_width),
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
                left_join_op,
                right_join_op,
                ..
            } => {
                debug_assert!(*join_type == JoinType::Inner || *join_type == Natural);
                debug_assert_eq!(conditions.len(), 1);
                let left_df = self.exec_planned_tree(left)?;
                let right_df = self.exec_planned_tree(right)?;
                let left_col = conditions[0].0.clone();
                let right_col = conditions[0].1.clone();
                let l_idx = left_df
                    .header
                    .iter()
                    .position(|f| f.name == left_col.name && f.table_name == left_col.table_name)
                    .ok_or(Status::DataFrameJoinError)?;
                let r_idx = right_df
                    .header
                    .iter()
                    .position(|f| f.name == right_col.name && f.table_name == right_col.table_name)
                    .ok_or(Status::DataFrameJoinError)?;
                let strategy = if *left_join_op == JoinOp::Key && *right_join_op == JoinOp::Key {
                    JoinStrategy::SortMerge
                } else {
                    JoinStrategy::Hash
                };

                println!(
                    "[JOIN] strategy={:?}, left_op={:?}, right_op={:?}, on={}.{}={}.{}",
                    strategy,
                    left_join_op,
                    right_join_op,
                    left_col.table_name,
                    left_col.name,
                    right_col.table_name,
                    right_col.name
                );

                Ok(left_df.join(right_df, l_idx, r_idx, strategy)?)
            }

            PlanNode::SetOperation { op, left, right } => {
                let left_df = self.exec_planned_tree(left)?;
                let right_df = self.exec_planned_tree(right)?;

                Ok(left_df.set_operation(right_df, op.clone(), SetOpStrategy::HashedMemory)?)
            }
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
            Self::tree_order_for(&schema, self.btree_node_width),
            self.pager_accessor.clone(),
            schema.clone(),
        )?;

        Ok(BTreeScanSource::new(btree, schema, operation, conditions))
    }

    fn count_nodes_on_page(&self, schema: &TableSchema, page_data: &[u8; PAGE_SIZE]) -> Result<usize, Status> {
        let mut count = 0usize;
        let mut offset = 0usize;
        let key_length = schema.get_key_length()?;
        let row_length = schema.get_row_length()?;

        while offset + 2 <= PAGE_SIZE {
            let num_keys = page_data[offset] as usize;
            let flag = page_data[offset + 1];

            if num_keys == 0 && flag == 0 {
                break;
            }

            let node_size = 2 + num_keys * (key_length + row_length) + (num_keys + 1) * 4;
            if offset + node_size > PAGE_SIZE {
                return Err(Status::InternalExceptionIndexOutOfRange);
            }

            count += 1;
            offset += node_size;
        }

        Ok(count)
    }

    fn collect_btree_pages(&self, node: &crate::btree::BTreeNode, pages: &mut HashSet<usize>) -> Result<(), Status> {
        if !pages.insert(node.position.page()) {
            return Ok(());
        }
        for child in PagerProxy::get_children(node)? {
            self.collect_btree_pages(&child, pages)?;
        }
        Ok(())
    }

    fn refresh_table_free_list(&mut self, table_id: usize) -> Result<(), Status> {
        if table_id >= self.schema.tables.len() {
            return Ok(());
        }
        let table_schema = self.schema.tables[table_id].clone();
        let btree = Btree::init(
            Self::tree_order_for(&table_schema, self.btree_node_width),
            self.pager_accessor.clone(),
            table_schema.clone(),
        )?;
        let root = btree.root.ok_or(Status::InternalExceptionNoRoot)?;

        let mut pages: HashSet<usize> = table_schema
            .free_list
            .iter()
            .map(|(page, _)| *page)
            .collect();
        pages.insert(root.position.page());

        let capacity = table_schema.max_nodes_per_page()?;
        let mut free_list = Vec::new();

        for page in pages {
            let page_data = self
                .pager_accessor
                .access_pager_write(|p| p.access_page_read(&Position::new(page, 0)))?;
            let used = self.count_nodes_on_page(&table_schema, &page_data.data)?;
            let free = capacity.saturating_sub(used);
            free_list.push((page, free));
        }

        free_list.sort_by_key(|(page, _)| *page);
        self.schema.tables[table_id].free_list = free_list;
        Ok(())
    }

    fn refresh_all_free_lists(&mut self) {
        for table_id in 1..self.schema.tables.len() {
            if let Err(e) = self.refresh_table_free_list(table_id) {
                eprintln!("Failed to refresh free-list for table {}: {:?}", table_id, e);
            }
        }
    }

    fn schema_to_create_sql(schema: &TableSchema) -> String {
        let fields = schema
            .fields
            .iter()
            .map(|f| {
                let type_str = match f.field_type {
                    Type::String => "String".to_string(),
                    Type::Varchar(len) => format!("Varchar({})", len),
                    Type::Integer => "Integer".to_string(),
                    Type::Date => "Date".to_string(),
                    Type::Boolean => "Boolean".to_string(),
                    Type::Null => "Null".to_string(),
                };
                format!("{} {}", f.name, type_str)
            })
            .collect::<Vec<String>>()
            .join(", ");
        format!("CREATE TABLE {} ({})", schema.name, fields)
    }

    fn persist_free_lists_to_system_table(&mut self) -> Result<(), QueryResult> {
        self.execute_compiled_query(format!("DELETE FROM {}", MASTER_TABLE_NAME), true)?;

        let tables_to_persist: Vec<TableSchema> = self.schema.tables.iter().skip(1).cloned().collect();
        for table in tables_to_persist {
            let create_sql = Self::schema_to_create_sql(&table);
            let insert_query = format!(
                "INSERT INTO {} (name, type, rootpage, sql, free_list) VALUES ('{}', '{}', {}, '{}', '{}')",
                MASTER_TABLE_NAME,
                table.name.replace("'", "''"),
                "table",
                table.root.page(),
                create_sql.replace("'", "''"),
                Self::encode_free_list_top_10(&table).replace("'", "''")
            );
            self.execute_compiled_query(insert_query, true)?;
        }

        self.reload_schema()?;
        Ok(())
    }

    fn persist_table_free_list_to_system_table(&mut self, table_id: usize) -> Result<(), QueryResult> {
        if table_id == 0 || table_id >= self.schema.tables.len() {
            return Ok(());
        }

        let table = self.schema.tables[table_id].clone();
        let create_sql = Self::schema_to_create_sql(&table);

        let insert_query = format!(
            "INSERT INTO {} (name, type, rootpage, sql, free_list) VALUES ('{}', '{}', {}, '{}', '{}')",
            MASTER_TABLE_NAME,
            table.name.replace("'", "''"),
            "table",
            table.root.page(),
            create_sql.replace("'", "''"),
            Self::encode_free_list_top_10(&table).replace("'", "''")
        );
        self.execute_compiled_query(insert_query, true)?;
        Ok(())
    }

    fn persist_free_lists_snapshot_to_system_table(&mut self) -> Result<(), QueryResult> {
        let master_schema = self.schema.tables[0].clone();
        PagerProxy::clear_table_root(&master_schema, self.pager_accessor.clone())
            .map_err(QueryResult::err)?;

        let tables_to_persist: Vec<TableSchema> = self.schema.tables.iter().skip(1).cloned().collect();
        for table in tables_to_persist {
            let create_sql = Self::schema_to_create_sql(&table);
            let insert_query = format!(
                "INSERT INTO {} (name, type, rootpage, sql, free_list) VALUES ('{}', '{}', {}, '{}', '{}')",
                MASTER_TABLE_NAME,
                table.name.replace("'", "''"),
                "table",
                table.root.page(),
                create_sql.replace("'", "''"),
                Self::encode_free_list_top_10(&table).replace("'", "''")
            );
            self.execute_compiled_query(insert_query, true)?;
        }

        Ok(())
    }

    pub fn check_integrity(&self) -> Result<(), Status> {
        let btree = Btree::init(
            Self::tree_order_for(&self.schema.tables[0], self.btree_node_width),
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
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(file_name)
            .map_err(|_| Status::InternalExceptionDBCreationFailed)?;

        let db = Self::make_initial_db_bytes();
        file.write_all(&db)
            .map_err(|_| Status::InternalExceptionDBCreationFailed)?;

        Ok(())
    }

    fn initialize_database_file(file_name: &str) -> Result<(), Status> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(file_name)
            .map_err(|_| Status::InternalExceptionDBCreationFailed)?;

        let db = Self::make_initial_db_bytes();
        file.write_all(&db)
            .map_err(|_| Status::InternalExceptionDBCreationFailed)?;
        Ok(())
    }

    fn make_initial_db_bytes() -> [u8; 2 + PAGE_SIZE_WITH_META] {
        let mut db = [0u8; 2 + PAGE_SIZE_WITH_META];
        // [<0, 1> Next Page, <0, 1> Free Space, Flag, Num-keys, Flag]
        db[1] = 2; //next page: [0, 1] -> 2 (starts at 1)
        db[2] = ((PAGE_SIZE - 600) << 8) as u8;
        db[3] = ((PAGE_SIZE - 600) & 0xFF) as u8;
        db[6] = Serializer::create_node_flag(true); //flag: is a leaf
        db
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
                    Field {
                        field_type: Type::String,
                        name: "type".to_string(),
                        table_name: MASTER_TABLE_NAME.to_string(),
                    },
                    Field {
                        field_type: Type::String,
                        name: "free_list".to_string(),
                        table_name: MASTER_TABLE_NAME.to_string(),
                    },
                ],
            },
        };

        let result = self
            .exec_planned_tree(&select_query.plan)
            .expect("Failed Initialisation");
        let data = result.fetch().expect("Failed Initialisation");
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
            let free_list_encoded = Serializer::format_field_on_row(entry, 4, &master_table_schema)
                .expect("Failed to format field: free_list");
            let mut parser = Parser::new(sql);
            let parsed_query = parser.parse_query().expect("Failed to parse query");
            let compiled_query = Planner::plan(&Schema::make_empty(), parsed_query)
                .expect("Failed to compile query");
            match compiled_query {
                CompiledQuery::CreateTable(mut table) => {
                    let strip_pos = name.iter().rposition(|&x| x != 0).expect("cant be empty");
                    let table_name = name[0..strip_pos + 1].to_vec();
                    table.schema.root = Position::new(rootpage as usize, 0);
                    table.schema.btree_order = self.btree_node_width; //ToDo Store this in the System Table
                    table.schema.free_list = TableSchema::free_list_from_string(&free_list_encoded);
                    if let Some(existing_idx) = schema
                        .table_index
                        .index
                        .iter()
                        .position(|t| t == &table_name)
                    {
                        schema.tables[existing_idx] = table.schema;
                    } else {
                        schema.table_index.index.push(table_name);
                        schema.tables.push(table.schema);
                    }
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
                create.schema.btree_order = 2;
                create.schema
            }
            _ => {
                panic!("wtf")
            }
        }
    }
}
