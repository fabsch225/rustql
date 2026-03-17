use crate::btree::Btree;
use crate::cursor::BTreeCursor;
use crate::dataframe::{
    BTreeScanSource, DataFrame, JoinStrategy, MemorySource, RowSource, SetOpStrategy, Source,
};
use crate::pager::{
    Key, PAGE_SIZE, PAGE_SIZE_WITH_META, PageData, PagerAccessor, PagerCore, Position, Row, TableName, Type
};
use crate::pager_proxy::PagerProxy;
use crate::parser::JoinType::Natural;
use crate::parser::{JoinOp, JoinType, ParsedQuery, ParsedSetOperator, Parser};
use crate::planner::SqlStatementComparisonOperator::{
    Equal, Greater, GreaterOrEqual, Lesser, LesserOrEqual,
};
use crate::planner::{
    CompiledCreateIndexQuery, CompiledCreateTableQuery, CompiledDeleteQuery, CompiledInsertQuery, CompiledQuery,
    CompiledSelectQuery, CompiledUpdateQuery, PlanNode, Planner, SqlConditionOpCode,
    SqlStatementComparisonOperator,
};
pub(crate) use crate::schema::{Field, IndexDefinition, Schema, TableIndex, TableSchema};
use crate::serializer::Serializer;
use crate::debug::Status;
use crate::debug::Status::ExceptionQueryMisformed;
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
                index_definitions: vec![],
            },
            btree_node_width: t,
            request_counter: 0,
            last_write_table_id: None,
        };

        bootstrap_executor.schema = bootstrap_executor.load_schema();
        bootstrap_executor
    }

    pub fn exit(&self) {
        self.pager_accessor
            .access_pager_write(|p| p.flush())
            .expect("Error Flushing the Pager");
    }

    pub fn prepare(&mut self, query: String) -> QueryResult {
        self.request_counter += 1;
        self.last_write_table_id = None;
        let result = self.execute(query, false);

        // Keep free-lists fresh in memory, but do not rewrite the whole system table periodically.
        // Rewriting every 30 requests caused repeated re-encoding of long SQL strings
        // (stored with external payload pages), which accumulates duplicate SQL text in the file.
        if self.request_counter % 30 == 0 {
            if let Some(table_id) = self.last_write_table_id {
                if let Err(e) = self.refresh_table_free_list(table_id) {
                    eprintln!("Failed to refresh free-list for table {}: {:?}", table_id, e);
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

    pub fn compile_query(&self, query: &str) -> Result<CompiledQuery, QueryResult> {
        let mut parser = Parser::new(query.to_string());
        let parsed_query = parser
            .parse_query()
            .map_err(|s| QueryResult::user_input_wrong(s))?;
        Planner::plan(&self.schema, parsed_query)
    }

    fn execute(
        &mut self,
        query: String,
        allow_modification_to_system_table: bool,
    ) -> Result<QueryResult, QueryResult> {
        let compiled_query = self.compile_query(&query)?;
        self.execute_compiled(
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

    fn execute_compiled(
        &mut self,
        compiled_query: CompiledQuery,
        query: String,
        allow_modification_to_system_table: bool,
    ) -> Result<QueryResult, QueryResult> {
        match compiled_query {
            CompiledQuery::CreateIndex(q) => {
                let create_index_sql = format!(
                    "CREATE INDEX {} ON {} ({})",
                    q.index_name, q.base_table_name, q.column_name
                );

                self.execute_compiled(
                    CompiledQuery::CreateTable(CompiledCreateTableQuery {
                        table_name: q.schema.name.clone(),
                        schema: q.schema,
                    }),
                    create_index_sql,
                    true,
                )?;

                self.reload_schema()?;
                let base_table_id = Planner::find_table_id(&self.schema, &q.base_table_name)?;
                self.rebuild_indices_for_table_id(base_table_id)?;
                self.last_write_table_id = Some(base_table_id);

                Ok(QueryResult::went_fine())
            }
            CompiledQuery::CreateTable(q) => {
                if !allow_modification_to_system_table && q.table_name.starts_with('_') {
                    return Err(QueryResult::user_input_wrong(
                        "Manual index tables are not allowed. Use CREATE INDEX <name> ON <table> (<column>)"
                            .to_string(),
                    ));
                }

                //check if the table already exists
                //this could be achieved using a unique / pk constraint on the system table.
                //but there are no constraints implemented ;D
                let stripped_mame = q.table_name.trim_end_matches(|char| char == '0');
                let table_name: TableName = stripped_mame.as_bytes().to_vec();
                if self.schema.table_index.index.contains(&table_name) {
                    return Err(QueryResult::err(Status::ExceptionTableAlreadyExists));
                }

                let mut table_schema = q.schema.clone();
                table_schema.btree_order = self.btree_node_width;

                let root_page = PagerProxy::create_empty_node_on_new_page(
                    &table_schema,
                    self.pager_accessor.clone(),
                )
                .map_err(|status| QueryResult::err(status))
                .map(|node| {
                    return node.position.page();
                })?;

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
                self.execute(insert_query, true)?;
                let result = self.reload_schema()?;
                if !allow_modification_to_system_table {
                    let created_table_id = Planner::find_table_id(&self.schema, &q.table_name)?;
                    self.last_write_table_id = Some(created_table_id);
                }
                Ok(result)
            }
            CompiledQuery::DropIndex(q) => self.execute_compiled(
                CompiledQuery::DropTable(crate::planner::CompiledDropTableQuery {
                    table_id: q.table_id,
                }),
                query,
                allow_modification_to_system_table,
            ),
            CompiledQuery::DropTable(q) => {
                if !allow_modification_to_system_table && q.table_id == 0 {
                    return Err(QueryResult::msg("You are not allowed to modify this table."));
                }

                if q.table_id >= self.schema.tables.len() {
                    return Err(QueryResult::user_input_wrong("Table not found".to_string()));
                }

                let dropped_table = self.schema.tables[q.table_id].clone();
                let dropped_btree = Btree::init(
                    dropped_table.btree_order,
                    self.pager_accessor.clone(),
                    dropped_table.clone(),
                )
                .map_err(QueryResult::err)?;

                let mut dropped_pages = HashSet::new();
                if let Some(root) = dropped_btree.root {
                    self.collect_btree_pages(&root, &mut dropped_pages)
                        .map_err(QueryResult::err)?;
                } else {
                    dropped_pages.insert(dropped_table.root.page());
                }
                self.mark_pages_as_deleted(&dropped_pages)
                    .map_err(QueryResult::err)?;

                PagerProxy::clear_table_root(&dropped_table, self.pager_accessor.clone())
                    .map_err(QueryResult::err)?;

                let delete_master_row = format!(
                    "DELETE FROM {} WHERE name = '{}'",
                    MASTER_TABLE_NAME,
                    dropped_table.name.replace("'", "''")
                );
                self.execute(delete_master_row, true)?;

                self.reload_schema()
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
                    schema.btree_order,
                    self.pager_accessor.clone(),
                    schema,
                )
                .map_err(|s| QueryResult::err(s))?;
                let (insert_key, insert_row) = q.data;
                btree
                    .insert(insert_key.clone(), insert_row.clone())
                    .map_err(|s| QueryResult::err(s))?;
                if !allow_modification_to_system_table {
                    self.last_write_table_id = Some(q.table_id);
                    self.insert_row_into_indices(q.table_id, &insert_key, &insert_row)?;
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
                    btree_schema.btree_order,
                    self.pager_accessor.clone(),
                    btree_schema,
                )
                .map_err(|s| QueryResult::err(s))?;

                for key in keys_to_delete {
                    btree.delete(key).map_err(|s| QueryResult::err(s))?;
                }
                if !allow_modification_to_system_table {
                    self.rebuild_indices_for_table_id(q.table_id)?;
                }
                Ok(QueryResult::went_fine())
            }
            CompiledQuery::Update(q) => {
                if !allow_modification_to_system_table && q.table_id == 0 {
                    return Err(QueryResult::msg("You are not allowed to modify this table."));
                }

                let table_id = q.table_id;
                let result = self.execute_update(q, allow_modification_to_system_table)?;
                if !allow_modification_to_system_table {
                    self.rebuild_indices_for_table_id(table_id)?;
                }
                Ok(result)
            }
        }
    }

    fn execute_update(
        &mut self,
        q: CompiledUpdateQuery,
        allow_modification_to_system_table: bool,
    ) -> Result<QueryResult, QueryResult> {
        let schema = self.schema.tables[q.table_id].clone();
        let key_len = Serializer::get_size_of_type(&schema.fields[schema.key_position].field_type)
            .map_err(QueryResult::err)?;

        let mut source = self
            .create_scan_source(q.table_id, q.operation.clone(), q.conditions.clone())
            .map_err(QueryResult::err)?;

        let mut updates_to_apply: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = Vec::new();

        source.reset().map_err(QueryResult::err)?;
        while let Some(row) = source.next().map_err(QueryResult::err)? {
            let mut updated_fields = Vec::with_capacity(schema.fields.len());
            for field_idx in 0..schema.fields.len() {
                updated_fields.push(
                    Serializer::get_field_on_row(&row, field_idx, &schema).map_err(QueryResult::err)?,
                );
            }

            let original_key = updated_fields[schema.key_position].clone();

            for (field_idx, new_value) in &q.assignments {
                updated_fields[*field_idx] = new_value.clone();
            }

            let new_key = updated_fields[schema.key_position].clone();
            if new_key.len() != key_len {
                return Err(QueryResult::err(Status::InternalExceptionTypeMismatch));
            }

            let mut new_row = Vec::new();
            for (idx, field_bytes) in updated_fields.into_iter().enumerate() {
                if idx != schema.key_position {
                    new_row.extend(field_bytes);
                }
            }

            updates_to_apply.push((original_key, new_key, new_row));
        }

        let mut btree_schema = schema.clone();
        if allow_modification_to_system_table && q.table_id == 0 {
            btree_schema.free_list.clear();
        }

        let mut btree = Btree::init(
            btree_schema.btree_order,
            self.pager_accessor.clone(),
            btree_schema,
        )
        .map_err(QueryResult::err)?;

        for (original_key, _, _) in &updates_to_apply {
            btree.delete(original_key.clone()).map_err(QueryResult::err)?;
        }

        for (_, new_key, new_row) in updates_to_apply {
            btree.insert(new_key, new_row).map_err(QueryResult::err)?;
        }

        if !allow_modification_to_system_table {
            self.last_write_table_id = Some(q.table_id);
        }

        Ok(QueryResult::went_fine())
    }

    fn exec_planned_tree(&self, plan: &PlanNode) -> Result<DataFrame, Status> {
        match plan {
            PlanNode::SeqScan {
                table_id,
                table_name,
                operation,
                conditions,
                index_table_id,
                index_on_column,
            } => {
                if matches!(
                    operation,
                    SqlConditionOpCode::SelectIndexUnique | SqlConditionOpCode::SelectIndexRange
                ) && index_table_id.is_some() && index_on_column.is_some()
                {
                    let base_schema = self.schema.tables[*table_id].clone();
                    let index_schema = self.schema.tables[index_table_id.unwrap()].clone();
                    let idx_col = index_on_column.unwrap();

                    if idx_col >= conditions.len() {
                        return Ok(DataFrame::from_memory(
                            table_name.clone(),
                            plan.get_header(&self.schema)?,
                            vec![],
                        ));
                    }

                    let (idx_cmp, idx_val) = conditions[idx_col].clone();
                    if idx_cmp == SqlStatementComparisonOperator::None {
                        return Ok(DataFrame::from_memory(
                            table_name.clone(),
                            plan.get_header(&self.schema)?,
                            vec![],
                        ));
                    }

                    let index_op = match operation {
                        SqlConditionOpCode::SelectIndexUnique => SqlConditionOpCode::SelectKeyUnique,
                        SqlConditionOpCode::SelectIndexRange => SqlConditionOpCode::SelectFTS,
                        _ => SqlConditionOpCode::SelectFTS,
                    };

                    let mut index_conditions = vec![
                        (SqlStatementComparisonOperator::None, Vec::new());
                        index_schema.fields.len()
                    ];
                    index_conditions[0] = (idx_cmp, idx_val);

                    let index_btree = Btree::init(
                        index_schema.btree_order,
                        self.pager_accessor.clone(),
                        index_schema.clone(),
                    )?;
                    let base_btree = Btree::init(
                        base_schema.btree_order,
                        self.pager_accessor.clone(),
                        base_schema.clone(),
                    )?;

                    return Ok(DataFrame::from_index_lookup(
                        table_name.clone(),
                        plan.get_header(&self.schema)?,
                        index_btree,
                        index_schema,
                        base_btree,
                        base_schema,
                        index_op,
                        index_conditions,
                        conditions.clone(),
                    ));
                }

                let schema = self.schema.tables[*table_id].clone();
                let btree = Btree::init(
                    schema.btree_order,
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

                /*println!(
                    "[JOIN] strategy={:?}, left_op={:?}, right_op={:?}, on={}.{}={}.{}",
                    strategy,
                    left_join_op,
                    right_join_op,
                    left_col.table_name,
                    left_col.name,
                    right_col.table_name,
                    right_col.name
                );*/

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
            schema.btree_order,
            self.pager_accessor.clone(),
            schema.clone(),
        )?;

        Ok(BTreeScanSource::new(btree, schema, operation, conditions))
    }

    fn index_table_name(base_table: &str, column: &str) -> String {
        format!("_{}_{}", base_table, column)
    }

    fn parse_index_table_name(index_table_name: &str) -> Option<(String, String)> {
        if !index_table_name.starts_with('_') {
            return None;
        }

        let rest = &index_table_name[1..];
        let (base, col) = rest.rsplit_once('_')?;
        if base.is_empty() || col.is_empty() {
            return None;
        }
        Some((base.to_string(), col.to_string()))
    }

    fn find_index_table_id_for_base_column(&self, base_table: &str, column: &str) -> Option<usize> {
        let index_name = self
            .schema
            .index_definitions
            .iter()
            .find(|idx| idx.base_table == base_table && idx.column_name == column)
            .map(|idx| idx.index_name.clone())?;
        Planner::find_table_id(&self.schema, &index_name).ok()
    }

    fn should_index_field(field_type: &Type) -> bool {
        matches!(
            field_type,
            Type::Integer | Type::String | Type::Varchar(_) | Type::Date
        )
    }

    fn insert_row_into_indices(
        &mut self,
        table_id: usize,
        key: &Key,
        row: &Row,
    ) -> Result<(), QueryResult> {
        if table_id == 0 || table_id >= self.schema.tables.len() {
            return Ok(());
        }

        let base = self.schema.tables[table_id].clone();
        if base.name.starts_with('_') {
            return Ok(());
        }

        let full_row = Serializer::reconstruct_row(key, row, &base).map_err(QueryResult::err)?;

        for (field_idx, field) in base.fields.iter().enumerate() {
            if field_idx == base.key_position || !Self::should_index_field(&field.field_type) {
                continue;
            }

            let index_table_id = match self.find_index_table_id_for_base_column(&base.name, &field.name) {
                Some(id) => id,
                None => continue,
            };

            let index_schema = self.schema.tables[index_table_id].clone();
            let idx_key = Serializer::get_field_on_row(&full_row, field_idx, &base)
                .map_err(QueryResult::err)?;
            let base_pk = Serializer::get_field_on_row(&full_row, base.key_position, &base)
                .map_err(QueryResult::err)?;

            let mut index_btree = Btree::init(
                index_schema.btree_order,
                self.pager_accessor.clone(),
                index_schema,
            )
            .map_err(QueryResult::err)?;
            index_btree.insert(idx_key, base_pk).map_err(QueryResult::err)?;
        }

        Ok(())
    }

    fn rebuild_indices_for_table_id(&mut self, table_id: usize) -> Result<(), QueryResult> {
        if table_id == 0 || table_id >= self.schema.tables.len() {
            return Ok(());
        }

        let base = self.schema.tables[table_id].clone();
        if base.name.starts_with('_') {
            return Ok(());
        }

        let base_key_pos = base.key_position;

        for (field_idx, field) in base.fields.iter().enumerate() {
            if field_idx == base_key_pos || !Self::should_index_field(&field.field_type) {
                continue;
            }

            let index_table_id = match self.find_index_table_id_for_base_column(&base.name, &field.name) {
                Some(id) => id,
                None => continue,
            };

            let index_schema = self.schema.tables[index_table_id].clone();
            PagerProxy::clear_table_root(&index_schema, self.pager_accessor.clone())
                .map_err(QueryResult::err)?;

            let mut base_source = self
                .create_scan_source(table_id, SqlConditionOpCode::SelectFTS, vec![])
                .map_err(QueryResult::err)?;

            let mut index_btree = Btree::init(
                index_schema.btree_order,
                self.pager_accessor.clone(),
                index_schema.clone(),
            )
            .map_err(QueryResult::err)?;

            base_source.reset().map_err(QueryResult::err)?;
            while let Some(base_row) = base_source.next().map_err(QueryResult::err)? {
                let idx_key =
                    Serializer::get_field_on_row(&base_row, field_idx, &base).map_err(QueryResult::err)?;
                let base_pk =
                    Serializer::get_field_on_row(&base_row, base_key_pos, &base).map_err(QueryResult::err)?;
                index_btree.insert(idx_key, base_pk).map_err(QueryResult::err)?;
            }
        }

        Ok(())
    }

    fn count_nodes_on_page(&self, schema: &TableSchema, page_data: PageData) -> Result<usize, Status> {
        let mut effective_schema = schema.clone();
        if effective_schema.btree_order == 0 {
            effective_schema.btree_order = self.btree_node_width;
        }

        let has_varchar = schema
            .fields
            .iter()
            .any(|f| matches!(f.field_type, Type::Varchar(_)));
        if has_varchar && effective_schema.get_node_size_in_bytes()? > PAGE_SIZE {
            return Ok(if page_data[0] == 0 && page_data[1] == 0 { 0 } else { 1 });
        }

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

    fn mark_pages_as_deleted(&self, pages: &HashSet<usize>) -> Result<(), Status> {
        for page in pages {
            let pos = Position::new(*page, 0);
            self.pager_accessor.access_pager_write(|p| {
                let page_container = p.access_page_write(&pos)?;
                Serializer::set_is_deleted(page_container, true)
            })?;
        }
        Ok(())
    }

    fn refresh_table_free_list(&mut self, table_id: usize) -> Result<(), Status> {
        if table_id >= self.schema.tables.len() {
            return Ok(());
        }
        let table_schema = self.schema.tables[table_id].clone();
        let btree = Btree::init(
            table_schema.btree_order,
            self.pager_accessor.clone(),
            table_schema.clone(),
        )?;
        let root = btree.root.ok_or(Status::InternalExceptionNoRoot)?;

        let mut pages = HashSet::new();
        self.collect_btree_pages(&root, &mut pages)?;

        let capacity = table_schema.max_nodes_per_page()?;
        let mut free_list = Vec::new();

        for page in pages {
            let page_data = self
                .pager_accessor
                .access_pager_write(|p| p.access_page_read(&Position::new(page, 0)))?;
            let used = self.count_nodes_on_page(&table_schema, page_data.data)?;
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
            .map(|f| format!("{} {}", f.name, f.field_type.to_sql()))
            .collect::<Vec<String>>()
            .join(", ");
        format!("CREATE TABLE {} ({})", schema.name, fields)
    }

    fn persist_free_lists_to_system_table(&mut self) -> Result<(), QueryResult> {
        let tables_to_persist: Vec<TableSchema> = self.schema.tables.iter().skip(1).cloned().collect();
        for table in tables_to_persist {
            let create_sql = Self::schema_to_create_sql(&table);
            let update_query = format!(
                "UPDATE {} SET type = '{}', rootpage = {}, sql = '{}', free_list = '{}' WHERE name = '{}'",
                MASTER_TABLE_NAME,
                "table",
                table.root.page(),
                create_sql.replace("'", "''"),
                Self::encode_free_list_top_10(&table).replace("'", "''"),
                table.name.replace("'", "''")
            );
            self.execute(update_query, true)?;
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

        let update_query = format!(
            "UPDATE {} SET type = '{}', rootpage = {}, sql = '{}', free_list = '{}' WHERE name = '{}'",
            MASTER_TABLE_NAME,
            "table",
            table.root.page(),
            create_sql.replace("'", "''"),
            Self::encode_free_list_top_10(&table).replace("'", "''"),
            table.name.replace("'", "''")
        );
        self.execute(update_query, true)?;
        Ok(())
    }

    fn persist_free_lists_snapshot_to_system_table(&mut self) -> Result<(), QueryResult> {
        let tables_to_persist: Vec<TableSchema> = self.schema.tables.iter().skip(1).cloned().collect();
        for table in tables_to_persist {
            let create_sql = Self::schema_to_create_sql(&table);
            let update_query = format!(
                "UPDATE {} SET type = '{}', rootpage = {}, sql = '{}', free_list = '{}' WHERE name = '{}'",
                MASTER_TABLE_NAME,
                "table",
                table.root.page(),
                create_sql.replace("'", "''"),
                Self::encode_free_list_top_10(&table).replace("'", "''"),
                table.name.replace("'", "''")
            );
            self.execute(update_query, true)?;
        }

        Ok(())
    }

    pub fn check_integrity(&self) -> Result<(), Status> {
        let btree = Btree::init(
            self.schema.tables[0].btree_order,
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
            index_definitions: vec![],
        };
        let mut pending_indices: Vec<(String, i32, String, crate::parser::ParsedCreateIndexQuery)> = vec![];
        let select_query = CompiledSelectQuery {
            plan: PlanNode::Project {
                source: Box::new(PlanNode::SeqScan {
                    table_id: 0,
                    table_name: MASTER_TABLE_NAME.to_string(),
                    operation: SqlConditionOpCode::SelectFTS,
                    conditions: vec![],
                    index_table_id: None,
                    index_on_column: None,
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
            let mut parser = Parser::new(sql.clone());
            let parsed_query = parser.parse_query().expect("Failed to parse query");
            match parsed_query {
                ParsedQuery::CreateTable(create_table_query) => {
                    let compiled_query = Planner::plan(
                        &Schema::make_empty(),
                        ParsedQuery::CreateTable(create_table_query),
                    )
                    .expect("Failed to compile query");

                    let mut table = match compiled_query {
                        CompiledQuery::CreateTable(table) => table,
                        _ => panic!("in the system table expected compiled create table"),
                    };
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
                ParsedQuery::CreateIndex(idx) => {
                    pending_indices.push((idx.index_name.clone(), rootpage, free_list_encoded, idx));
                }
                _ => {
                    panic!("in the system table should only be create table or create index queries")
                }
            }
        });

        for (index_name, rootpage, free_list_encoded, idx) in pending_indices {
            let base_table_id = Planner::find_table_id(&schema, &idx.table_name)
                .expect("Base table not found while loading index");
            let base_schema = &schema.tables[base_table_id];
            let column_name = idx
                .columns
                .first()
                .expect("Index should contain at least one column")
                .clone();
            let base_column = base_schema
                .fields
                .iter()
                .find(|f| f.name == column_name)
                .expect("Index column not found in base table");

            let index_schema = TableSchema {
                next_position: Position::make_empty(),
                root: Position::new(rootpage as usize, 0),
                has_key: true,
                key_position: 0,
                fields: vec![
                    Field {
                        field_type: base_column.field_type.clone(),
                        name: "idx_value".to_string(),
                        table_name: index_name.clone(),
                    },
                    Field {
                        field_type: base_schema.fields[base_schema.key_position].field_type.clone(),
                        name: "base_pk".to_string(),
                        table_name: index_name.clone(),
                    },
                ],
                table_type: 0,
                entry_count: 0,
                name: index_name.clone(),
                btree_order: self.btree_node_width,
                free_list: TableSchema::free_list_from_string(&free_list_encoded),
            };

            let table_name: TableName = index_name.as_bytes().to_vec();
            if let Some(existing_idx) = schema
                .table_index
                .index
                .iter()
                .position(|t| t == &table_name)
            {
                schema.tables[existing_idx] = index_schema;
            } else {
                schema.table_index.index.push(table_name);
                schema.tables.push(index_schema);
            }

            schema.index_definitions.push(IndexDefinition {
                index_name,
                base_table: idx.table_name,
                column_name,
            });
        }
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
