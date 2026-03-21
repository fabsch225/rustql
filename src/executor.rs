use crate::btree::Btree;
use crate::cursor::BTreeCursor;
use crate::dataframe::{
    BTreeScanSource, ConditionEvalContext, DataFrame, JoinStrategy, MemorySource,
    PreparedConditionExpr, PreparedInStrategy, PreparedPredicateExpr, RowSource, SetOpStrategy,
    Source,
};
use crate::debug::Status;
use crate::debug::Status::ExceptionQueryMisformed;
use crate::pager::{
    Key, PAGE_SIZE, PAGE_SIZE_WITH_META, PageData, PagerAccessor, PagerCore, Position, Row,
    TableName, TransactionId, Type,
};
use crate::pager_proxy::PagerProxy;
use crate::parser::JoinType::Natural;
use crate::parser::{JoinOp, JoinType, ParsedQuery, ParsedSetOperator, Parser};
use crate::planner::SqlStatementComparisonOperator::{
    Equal, Greater, GreaterOrEqual, Lesser, LesserOrEqual,
};
use crate::planner::{
    CompiledConditionExpr, CompiledCreateIndexQuery, CompiledCreateTableQuery, CompiledDeleteQuery,
    CompiledInStrategy, CompiledInsertQuery, CompiledLogicalOp, CompiledPredicateExpr,
    CompiledQuery,
    CompiledSelectQuery, CompiledTransactionStatement, CompiledUpdateQuery, PlanNode, Planner,
    SqlConditionOpCode, SqlStatementComparisonOperator,
};
pub(crate) use crate::schema::{Field, IndexDefinition, Schema, TableIndex, TableSchema};
use crate::serializer::Serializer;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::{Display, Formatter, format};
use std::fs::OpenOptions;
use std::io::{ErrorKind, Write};

pub(crate) const MASTER_TABLE_NAME: &str = "rustsql_master";

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

    pub fn from_pager_accessor(pager_accessor: PagerAccessor, t: usize) -> Self {
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
        let result = self.run_query_internal(&query, false);

        // Keep free-lists fresh in memory, but do not rewrite the whole system table periodically.
        // Rewriting every 30 requests caused repeated re-encoding of long SQL strings
        // (stored with external payload pages), which accumulates duplicate SQL text in the file.
        if self.request_counter % 30 == 0 {
            if let Some(table_id) = self.last_write_table_id {
                if let Err(e) = self.refresh_table_free_list(table_id) {
                    eprintln!(
                        "Failed to refresh free-list for table {}: {:?}",
                        table_id, e
                    );
                }
            }
        }
        Self::finalize_result(result)
    }

    pub fn prepare_in_transaction_context(
        &mut self,
        query: String,
        tx_id: Option<TransactionId>,
    ) -> QueryResult {
        if let Err(status) = self.pager_accessor.set_current_transaction(tx_id) {
            return QueryResult::err(status);
        }

        let result = self.prepare(query);
        let _ = self.pager_accessor.set_current_transaction(None);
        result
    }

    pub fn prepare_in_implicit_transaction(&mut self, query: String) -> QueryResult {
        let implicit_tx_id = match self.pager_accessor.begin_transaction_with_id() {
            Ok(tx_id) => tx_id,
            Err(status) => return QueryResult::err(status),
        };

        if let Err(status) = self
            .pager_accessor
            .set_current_transaction(Some(implicit_tx_id))
        {
            return QueryResult::err(status);
        }

        let statement_result = self.prepare(query);

        if let Err(status) = self
            .pager_accessor
            .set_current_transaction(Some(implicit_tx_id))
        {
            return QueryResult::err(status);
        }

        if statement_result.success {
            let commit_result = self.prepare("COMMIT".to_string());
            if commit_result.success {
                statement_result
            } else {
                commit_result
            }
        } else {
            let _ = self.prepare("ROLLBACK".to_string());
            statement_result
        }
    }

    pub fn execute_readonly(&self, query: String) -> QueryResult {
        let result = self
            .compile_query(&query)
            .and_then(|compiled_query| match compiled_query {
                CompiledQuery::Select(q) => {
                    let result_df = self
                        .exec_planned_tree(&q.plan)
                        .map_err(QueryResult::err)?;
                    Ok(QueryResult::return_data(result_df))
                }
                _ => Err(QueryResult::user_input_wrong(
                    "query is not read-only".to_string(),
                )),
            });
        Self::finalize_result(result)
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

    fn finalize_result(result: Result<QueryResult, QueryResult>) -> QueryResult {
        if !result.is_ok() {
            result.err().unwrap()
        } else {
            result.expect("just checked")
        }
    }

    pub(crate) fn run_query_internal(
        &mut self,
        query: &str,
        allow_modification_to_system_table: bool,
    ) -> Result<QueryResult, QueryResult> {
        let compiled_query = self.compile_query(query)?;
        self.execute_compiled(compiled_query, query.to_string(), allow_modification_to_system_table)
    }

    fn execute_compiled(
        &mut self,
        compiled_query: CompiledQuery,
        query: String,
        allow_modification_to_system_table: bool,
    ) -> Result<QueryResult, QueryResult> {
        match compiled_query {
            CompiledQuery::Transaction(tx) => match tx {
                CompiledTransactionStatement::Begin => {
                    self.pager_accessor
                        .begin_transaction()
                        .map_err(QueryResult::err)?;
                    Ok(QueryResult::went_fine())
                }
                CompiledTransactionStatement::Commit => {
                    self.pager_accessor
                        .commit_transaction()
                        .map_err(QueryResult::err)?;
                    self.reload_schema()
                }
                CompiledTransactionStatement::Rollback => {
                    self.pager_accessor
                        .rollback_transaction()
                        .map_err(QueryResult::err)?;
                    self.reload_schema()
                }
            },
            CompiledQuery::CreateIndex(q) => {
                if !allow_modification_to_system_table {
                    self.lock_table_if_needed(MASTER_TABLE_NAME)?;
                    self.lock_table_if_needed(&q.base_table_name)?;
                    self.lock_table_if_needed(&q.index_name)?;
                }

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
                if !allow_modification_to_system_table {
                    self.lock_table_if_needed(MASTER_TABLE_NAME)?;
                    self.lock_table_if_needed(&q.table_name)?;
                }

                if !allow_modification_to_system_table && q.table_name.starts_with('_') {
                    return Err(QueryResult::user_input_wrong(
                        "Manual index tables are not allowed. Use CREATE INDEX <name> ON <table> (<column>)"
                            .to_string(),
                    ));
                }

                //check if the table already exists
                //this could be achieved using a unique / pk constraint on the system table.
                //but there are no constraints implemented ;D
                let table_name: TableName = q.table_name.as_bytes().to_vec();
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
                self.run_query_internal(&insert_query, true)?;
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
                    return Err(QueryResult::msg(
                        "You are not allowed to modify this table.",
                    ));
                }

                if q.table_id >= self.schema.tables.len() {
                    return Err(QueryResult::user_input_wrong("Table not found".to_string()));
                }

                if !allow_modification_to_system_table {
                    self.lock_table_if_needed(MASTER_TABLE_NAME)?;
                    let dropped_name = self.schema.tables[q.table_id].name.clone();
                    self.lock_table_if_needed(&dropped_name)?;
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
                self.run_query_internal(&delete_master_row, true)?;

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
                    return Err(QueryResult::msg(
                        "You are not allowed to modify this table.",
                    ));
                }

                if !allow_modification_to_system_table {
                    let table_name = self.schema.tables[q.table_id].name.clone();
                    self.lock_table_if_needed(&table_name)?;
                }

                let mut schema = self.schema.tables[q.table_id].clone();
                if allow_modification_to_system_table && q.table_id == 0 {
                    schema.free_list.clear();
                }
                let mut btree =
                    Btree::init(schema.btree_order, self.pager_accessor.clone(), schema)
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
                    return Err(QueryResult::msg(
                        "You are not allowed to modify this table.",
                    ));
                }

                if !allow_modification_to_system_table {
                    let table_name = self.schema.tables[q.table_id].name.clone();
                    self.lock_table_if_needed(&table_name)?;
                }

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
                let prepared_condition = q
                    .condition
                    .as_ref()
                    .map(|c| self.prepare_condition_runtime(c))
                    .transpose()
                    .map_err(QueryResult::err)?;

                let table_schema = schema.clone();
                let btree = Btree::init(
                    table_schema.btree_order,
                    self.pager_accessor.clone(),
                    table_schema.clone(),
                )
                .map_err(QueryResult::err)?;

                let mut scan_df = DataFrame::from_table(
                    "DeleteScan".to_string(),
                    table_schema.fields.clone(),
                    btree,
                    q.operation.clone(),
                    q.seek_key.clone(),
                );

                if let Some(cond) = prepared_condition {
                    scan_df = scan_df.filter_prepared(
                        cond,
                        schema.clone(),
                        ConditionEvalContext {
                            pager_accessor: self.pager_accessor.clone(),
                            schemas: self.schema.tables.clone(),
                        },
                    );
                }

                let rows = scan_df.fetch().map_err(QueryResult::err)?;
                for row in rows {
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
                    return Err(QueryResult::msg(
                        "You are not allowed to modify this table.",
                    ));
                }

                if !allow_modification_to_system_table {
                    let table_name = self.schema.tables[q.table_id].name.clone();
                    self.lock_table_if_needed(&table_name)?;
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

    fn lock_table_if_needed(&self, table_name: &str) -> Result<(), QueryResult> {
        self.pager_accessor
            .lock_table_for_transaction(table_name)
            .map_err(QueryResult::err)
    }

    fn execute_update(
        &mut self,
        q: CompiledUpdateQuery,
        allow_modification_to_system_table: bool,
    ) -> Result<QueryResult, QueryResult> {
        let schema = self.schema.tables[q.table_id].clone();
        let key_len = Serializer::get_size_of_type(&schema.fields[schema.key_position].field_type)
            .map_err(QueryResult::err)?;

        let mut updates_to_apply: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = Vec::new();
        let prepared_condition = q
            .condition
            .as_ref()
            .map(|c| self.prepare_condition_runtime(c))
            .transpose()
            .map_err(QueryResult::err)?;

        let btree = Btree::init(
            schema.btree_order,
            self.pager_accessor.clone(),
            schema.clone(),
        )
        .map_err(QueryResult::err)?;

        let mut scan_df = DataFrame::from_table(
            "UpdateScan".to_string(),
            schema.fields.clone(),
            btree,
            q.operation.clone(),
            q.seek_key.clone(),
        );

        if let Some(cond) = prepared_condition {
            scan_df = scan_df.filter_prepared(
                cond,
                schema.clone(),
                ConditionEvalContext {
                    pager_accessor: self.pager_accessor.clone(),
                    schemas: self.schema.tables.clone(),
                },
            );
        }

        let rows = scan_df.fetch().map_err(QueryResult::err)?;
        for row in rows {
            let mut updated_fields = Vec::with_capacity(schema.fields.len());
            for field_idx in 0..schema.fields.len() {
                updated_fields.push(
                    Serializer::get_field_on_row(&row, field_idx, &schema)
                        .map_err(QueryResult::err)?,
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
            btree
                .delete(original_key.clone())
                .map_err(QueryResult::err)?;
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
                seek_key,
                index_table_id,
                index_on_column,
            } => {
                if matches!(
                    operation,
                    SqlConditionOpCode::SelectIndexUnique | SqlConditionOpCode::SelectIndexRange
                ) && index_table_id.is_some()
                    && index_on_column.is_some()
                {
                    let base_schema = self.schema.tables[*table_id].clone();
                    let index_schema = self.schema.tables[index_table_id.unwrap()].clone();
                    let index_op = match operation {
                        SqlConditionOpCode::SelectIndexUnique => {
                            SqlConditionOpCode::SelectKeyUnique
                        }
                        SqlConditionOpCode::SelectIndexRange => SqlConditionOpCode::SelectFTS,
                        _ => SqlConditionOpCode::SelectFTS,
                    };

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

                    Ok(DataFrame::from_index_lookup(
                        table_name.clone(),
                        plan.get_header(&self.schema)?,
                        index_btree,
                        index_schema,
                        base_btree,
                        base_schema,
                        index_op,
                        seek_key.clone(),
                    ))
                } else {
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
                        seek_key.clone(),
                    ))
                }
            }

            PlanNode::Filter { source, condition } => {
                let source_df = self.exec_planned_tree(source)?;
                let prepared = self.prepare_condition_runtime(condition)?;
                let source_schema = source.get_schema(&self.schema)?;
                Ok(source_df.filter_prepared(
                    prepared,
                    source_schema,
                    ConditionEvalContext {
                        pager_accessor: self.pager_accessor.clone(),
                        schemas: self.schema.tables.clone(),
                    },
                ))
            }

            PlanNode::Project {
                source,
                fields,
                lookup_key_field_idx,
            } => {
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

                Ok(source_df.project(
                    fields.clone(),
                    mapping_indices,
                    *lookup_key_field_idx,
                ))
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
                let is_index_or_key = |op: &JoinOp| *op == JoinOp::Key || *op == JoinOp::Index;
                let strategy = if is_index_or_key(left_join_op) && is_index_or_key(right_join_op) {
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

    pub(crate) fn create_scan_source(
        &self,
        table_id: usize,
        operation: SqlConditionOpCode,
        seek_key: Option<Vec<u8>>,
    ) -> Result<BTreeScanSource, Status> {
        let schema = self.schema.tables[table_id].clone();
        let btree = Btree::init(
            schema.btree_order,
            self.pager_accessor.clone(),
            schema.clone(),
        )?;

        Ok(BTreeScanSource::new(btree, schema, operation, seek_key))
    }

    fn prepare_condition_runtime(
        &self,
        expr: &CompiledConditionExpr,
    ) -> Result<PreparedConditionExpr, Status> {
        match expr {
            CompiledConditionExpr::Logical { op, left, right } => {
                Ok(PreparedConditionExpr::Logical {
                    op: op.clone(),
                    left: Box::new(self.prepare_condition_runtime(left)?),
                    right: Box::new(self.prepare_condition_runtime(right)?),
                })
            }
            CompiledConditionExpr::Predicate(pred) => match pred {
                CompiledPredicateExpr::Compare {
                    column_idx,
                    op,
                    value,
                } => Ok(PreparedConditionExpr::Predicate(
                    PreparedPredicateExpr::Compare {
                        column_idx: *column_idx,
                        op: *op,
                        value: value.clone(),
                    },
                )),
                CompiledPredicateExpr::InSubquery {
                    column_idx,
                    strategy,
                } => {
                    let prepared_strategy = match strategy {
                        CompiledInStrategy::Materialize(plan) => {
                            let df = self.exec_planned_tree(plan)?;
                            PreparedInStrategy::Materialize(Box::new(df))
                        }
                        CompiledInStrategy::Lookup(plan) => {
                            let lookup_df = self.exec_planned_tree(plan)?;
                            PreparedInStrategy::Lookup {
                                lookup_df: Box::new(lookup_df),
                            }
                        }
                    };

                    Ok(PreparedConditionExpr::Predicate(
                        PreparedPredicateExpr::InSubquery {
                            column_idx: *column_idx,
                            strategy: prepared_strategy,
                        },
                    ))
                }
            },
        }
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

    pub fn reload_schema(&mut self) -> Result<QueryResult, QueryResult> {
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
        let mut pending_indices: Vec<(String, i32, String, crate::parser::ParsedCreateIndexQuery)> =
            vec![];
        let select_query = CompiledSelectQuery {
            plan: PlanNode::Project {
                source: Box::new(PlanNode::SeqScan {
                    table_id: 0,
                    table_name: MASTER_TABLE_NAME.to_string(),
                    operation: SqlConditionOpCode::SelectFTS,
                    seek_key: None,
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
                lookup_key_field_idx: None,
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
                    pending_indices.push((
                        idx.index_name.clone(),
                        rootpage,
                        free_list_encoded,
                        idx,
                    ));
                }
                _ => {
                    panic!(
                        "in the system table should only be create table or create index queries"
                    )
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
                        field_type: base_schema.fields[base_schema.key_position]
                            .field_type
                            .clone(),
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
