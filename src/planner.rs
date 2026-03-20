use crate::executor::{Field, QueryResult};
use crate::pager::{Key, Position, Row, TableName, Type};
use crate::parser::{
    JoinOp, JoinType, ParsedConditionExpr, ParsedLogicalOp, ParsedPredicateExpr, ParsedQuery,
    ParsedQueryTreeNode, ParsedSetOperator, ParsedSource, ParsedTransactionStatement,
    ParsedUpdateQuery, ParsedValueExpr,
};
use crate::schema::{Schema, TableSchema};
use crate::serializer::Serializer;
use crate::debug::Status;
use std::str::FromStr;

/// ## Responsibilities
/// - verifying queries (do they match the Query)
/// - planning the queries
pub struct Planner {}

#[repr(u8)]
#[derive(Debug, PartialEq, Clone)]
pub enum SqlConditionOpCode {
    SelectFTS = 60u8,         // Full table scan
    SelectIndexRange = 61u8,  // Unimplemented
    SelectIndexUnique = 62u8, // Unimplemented
    SelectKeyRange = 63u8,    // PK Range scan
    SelectKeyUnique = 64u8,   // PK Unique lookup
}

#[repr(u8)]
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum SqlStatementComparisonOperator {
    None = 0u8,
    Lesser = 1u8,
    Greater = 2u8,
    Equal = 3u8,
    GreaterOrEqual = 4u8,
    LesserOrEqual = 5u8,
}

#[derive(Debug)]
pub struct CompiledInsertQuery {
    pub table_id: usize,
    pub data: (Key, Row),
}

#[derive(Debug, Clone)]
pub enum CompiledLogicalOp {
    And,
    Or,
    Xor,
}

#[derive(Debug, Clone)]
pub enum CompiledPredicateExpr {
    Compare {
        column_idx: usize,
        op: SqlStatementComparisonOperator,
        value: Vec<u8>,
    },
    InSubquery {
        column_idx: usize,
        strategy: CompiledInStrategy,
    },
}

#[derive(Debug, Clone)]
pub enum CompiledConditionExpr {
    Logical {
        op: CompiledLogicalOp,
        left: Box<CompiledConditionExpr>,
        right: Box<CompiledConditionExpr>,
    },
    Predicate(CompiledPredicateExpr),
}

#[derive(Debug, Clone)]
pub enum CompiledInStrategy {
    Materialize(Box<PlanNode>),
    KeyLookup { table_id: usize },
    IndexLookup { index_table_id: usize },
}

/// A Node in the Query Execution Plan Tree
#[derive(Debug, Clone)]
pub enum PlanNode {
    SeqScan {
        table_id: usize,
        table_name: String,
        operation: SqlConditionOpCode,
        seek_key: Option<Vec<u8>>,
        index_table_id: Option<usize>,
        index_on_column: Option<usize>,
    },
    Filter {
        source: Box<PlanNode>,
        condition: CompiledConditionExpr,
    },
    Project {
        source: Box<PlanNode>,
        fields: Vec<Field>,
    },
    Join {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        join_type: JoinType,
        left_join_op: JoinOp,
        right_join_op: JoinOp,
        conditions: Vec<(Field, Field)>,
    },
    SetOperation {
        op: ParsedSetOperator,
        left: Box<PlanNode>,
        right: Box<PlanNode>,
    },
}

impl PlanNode {
    /// get the output schema of a node for parent resolution
    pub fn get_schema(&self, global_schema: &Schema) -> Result<TableSchema, Status> {
        match self {
            PlanNode::SeqScan {
                table_id,
                table_name,
                ..
            } => Ok(global_schema.tables[*table_id].clone()),
            PlanNode::Project { fields, source, .. } => {
                Ok(source.get_schema(global_schema)?.project(fields))
            }
            PlanNode::Join {
                left,
                right,
                conditions,
                ..
            } => {
                let right_schema = right.get_schema(global_schema)?;
                left.get_schema(global_schema)?.join(
                    &right_schema,
                    &conditions[0].0,
                    &conditions[0].1,
                )
            }
            PlanNode::SetOperation { left, .. } => {
                // ToDo Check This
                left.get_schema(global_schema)
            }
            PlanNode::Filter { source, .. } => source.get_schema(global_schema),
        }
    }

    pub fn get_header(&self, global_schema: &Schema) -> Result<Vec<Field>, Status> {
        let schema = self.get_schema(global_schema)?;
        Ok(schema.fields)
    }
}

#[derive(Debug)]
pub struct CompiledSelectQuery {
    pub plan: PlanNode,
}

#[derive(Debug)]
pub struct CompiledDeleteQuery {
    pub table_id: usize,
    pub operation: SqlConditionOpCode,
    pub seek_key: Option<Vec<u8>>,
    pub condition: Option<CompiledConditionExpr>,
}

#[derive(Debug)]
pub struct CompiledUpdateQuery {
    pub table_id: usize,
    pub operation: SqlConditionOpCode,
    pub seek_key: Option<Vec<u8>>,
    pub condition: Option<CompiledConditionExpr>,
    pub assignments: Vec<(usize, Vec<u8>)>,
}

#[derive(Debug)]
pub struct CompiledCreateTableQuery {
    pub table_name: String,
    pub schema: TableSchema,
}

#[derive(Debug)]
pub struct CompiledCreateIndexQuery {
    pub index_name: String,
    pub table_id: usize,
    pub base_table_name: String,
    pub column_name: String,
    pub schema: TableSchema,
}

#[derive(Debug)]
pub struct CompiledDropTableQuery {
    pub table_id: usize,
}

#[derive(Debug)]
pub struct CompiledDropIndexQuery {
    pub table_id: usize,
}

#[derive(Debug)]
pub enum CompiledQuery {
    CreateTable(CompiledCreateTableQuery),
    CreateIndex(CompiledCreateIndexQuery),
    DropTable(CompiledDropTableQuery),
    DropIndex(CompiledDropIndexQuery),
    Select(CompiledSelectQuery),
    Insert(CompiledInsertQuery),
    Delete(CompiledDeleteQuery),
    Update(CompiledUpdateQuery),
    Transaction(CompiledTransactionStatement),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompiledTransactionStatement {
    Begin,
    Commit,
    Rollback,
}

impl Planner {
    pub fn is_readonly_query(compiled_query: &CompiledQuery) -> bool {
        matches!(compiled_query, CompiledQuery::Select(_))
    }

    pub fn plan(schema: &Schema, query: ParsedQuery) -> Result<CompiledQuery, QueryResult> {
        match query {
            ParsedQuery::Insert(mut insert_query) => {
                let table_id = Self::find_table_id(schema, &insert_query.table_name)?;
                let table_schema = &schema.tables[table_id];

                if insert_query.fields.len() == 0 {
                    insert_query.fields = table_schema
                        .fields
                        .clone()
                        .into_iter()
                        .map(|f| f.name)
                        .collect();
                }

                if insert_query.fields.len() != insert_query.values.len() {
                    return Err(QueryResult::user_input_wrong(format!(
                        "Column count doesn't match value count: {} columns vs {} values",
                        insert_query.fields.len(),
                        insert_query.values.len()
                    )));
                }

                let mut ordered_data = Vec::new();

                for schema_field in &table_schema.fields {
                    let user_val_index = insert_query
                        .fields
                        .iter()
                        .position(|f| f == &schema_field.name);

                    match user_val_index {
                        Some(idx) => {
                            let value_str = &insert_query.values[idx];
                            let compiled_val = Self::compile_value(value_str, schema_field)?;
                            ordered_data.push(compiled_val);
                        }
                        None => {
                            return Err(QueryResult::user_input_wrong(format!(
                                "Missing value for field '{}'",
                                schema_field.name
                            )));
                        }
                    }
                }

                if ordered_data.is_empty() {
                    return Err(QueryResult::user_input_wrong(
                        "Cannot insert empty row".into(),
                    ));
                }

                let key = ordered_data[0].clone();
                let row: Vec<u8> = ordered_data[1..].iter().flat_map(|r| r.clone()).collect();

                Ok(CompiledQuery::Insert(CompiledInsertQuery {
                    table_id,
                    data: (key, row),
                }))
            }

            ParsedQuery::Select(tree_node) => {
                let plan = Self::plan_tree_node(schema, tree_node)?;
                Ok(CompiledQuery::Select(CompiledSelectQuery { plan }))
            }

            ParsedQuery::CreateTable(create_table_query) => {
                let mut fields = Vec::new();
                for (name, type_str) in create_table_query
                    .table_fields
                    .iter()
                    .zip(create_table_query.table_types.iter())
                {
                    let field_type = Type::from_str(type_str)
                        .map_err(QueryResult::user_input_wrong)?;
                    fields.push(Field {
                        name: name.clone(),
                        field_type,
                        table_name: create_table_query.table_name.clone(),
                    });
                }

                if fields.is_empty() {
                    return Err(QueryResult::user_input_wrong(
                        "Cannot create table with zero columns".into(),
                    ));
                }

                let schema = TableSchema {
                    root: Position::make_empty(),
                    next_position: Position::make_empty(),
                    has_key: true,
                    key_position: 0,
                    fields,
                    entry_count: 0,
                    table_type: 0,
                    name: create_table_query.table_name.clone(),
                    btree_order: 0,
                    free_list: vec![],
                };

                Ok(CompiledQuery::CreateTable(CompiledCreateTableQuery {
                    table_name: create_table_query.table_name,
                    schema,
                }))
            }

            ParsedQuery::CreateIndex(create_index_query) => {
                if create_index_query.columns.len() != 1 {
                    return Err(QueryResult::user_input_wrong(
                        "Only single-column indexes are supported".to_string(),
                    ));
                }

                let table_id = Self::find_table_id(schema, &create_index_query.table_name)?;
                let table_schema = &schema.tables[table_id];
                let column_name = create_index_query.columns[0].clone();

                let field = table_schema
                    .fields
                    .iter()
                    .find(|f| f.name == column_name)
                    .ok_or_else(|| {
                        QueryResult::user_input_wrong(format!(
                            "Column '{}.{}' not found",
                            create_index_query.table_name, column_name
                        ))
                    })?;

                if !matches!(
                    field.field_type,
                    Type::Integer | Type::String | Type::Varchar(_) | Type::Date
                ) {
                    return Err(QueryResult::user_input_wrong(format!(
                        "Type '{:?}' is not indexable",
                        field.field_type
                    )));
                }

                let pk_type = table_schema.fields[table_schema.key_position].field_type.clone();
                let index_table_name = create_index_query.index_name.clone();

                let index_schema = TableSchema {
                    root: Position::make_empty(),
                    next_position: Position::make_empty(),
                    has_key: true,
                    key_position: 0,
                    fields: vec![
                        Field {
                            name: "idx_value".to_string(),
                            field_type: field.field_type.clone(),
                            table_name: index_table_name.clone(),
                        },
                        Field {
                            name: "base_pk".to_string(),
                            field_type: pk_type,
                            table_name: index_table_name.clone(),
                        },
                    ],
                    entry_count: 0,
                    table_type: 0,
                    name: index_table_name,
                    btree_order: 0,
                    free_list: vec![],
                };

                Ok(CompiledQuery::CreateIndex(CompiledCreateIndexQuery {
                    index_name: create_index_query.index_name,
                    table_id,
                    base_table_name: create_index_query.table_name,
                    column_name,
                    schema: index_schema,
                }))
            }

            ParsedQuery::DropTable(drop_table_query) => {
                let table_id = Self::find_table_id(schema, &drop_table_query.table_name)?;
                Ok(CompiledQuery::DropTable(CompiledDropTableQuery {
                    table_id,
                }))
            }

            ParsedQuery::DropIndex(drop_index_query) => {
                let table_id = Self::find_table_id(schema, &drop_index_query.index_name)?;
                let is_index = schema
                    .index_definitions
                    .iter()
                    .any(|idx| idx.index_name == drop_index_query.index_name);
                if !is_index {
                    return Err(QueryResult::user_input_wrong(format!(
                        "Index '{}' not found",
                        drop_index_query.index_name
                    )));
                }
                Ok(CompiledQuery::DropIndex(CompiledDropIndexQuery {
                    table_id,
                }))
            }

            ParsedQuery::Delete(delete_query) => {
                let table_id = Self::find_table_id(schema, &delete_query.table_name)?;
                let table_schema = &schema.tables[table_id];

                let condition = match delete_query.conditions {
                    Some(expr) => Some(Self::compile_condition_expr(schema, table_schema, &expr)?),
                    None => None,
                };

                let (operation, seek_key) = Self::derive_scan_hint_for_table(
                    schema,
                    table_schema,
                    &condition,
                );

                Ok(CompiledQuery::Delete(CompiledDeleteQuery {
                    table_id,
                    operation,
                    seek_key,
                    condition,
                }))
            }

            ParsedQuery::Update(update_query) => {
                let table_id = Self::find_table_id(schema, &update_query.table_name)?;
                let table_schema = &schema.tables[table_id];

                let condition = match &update_query.conditions {
                    Some(expr) => Some(Self::compile_condition_expr(schema, table_schema, expr)?),
                    None => None,
                };

                let (operation, seek_key) = Self::derive_scan_hint_for_table(
                    schema,
                    table_schema,
                    &condition,
                );
                let assignments = Self::compile_update_assignments(&update_query, table_schema)?;

                Ok(CompiledQuery::Update(CompiledUpdateQuery {
                    table_id,
                    operation,
                    seek_key,
                    condition,
                    assignments,
                }))
            }

            ParsedQuery::Transaction(tx) => {
                let compiled = match tx {
                    ParsedTransactionStatement::Begin => CompiledTransactionStatement::Begin,
                    ParsedTransactionStatement::Commit => CompiledTransactionStatement::Commit,
                    ParsedTransactionStatement::Rollback => {
                        CompiledTransactionStatement::Rollback
                    }
                };
                Ok(CompiledQuery::Transaction(compiled))
            }
        }
    }

    fn compile_update_assignments(
        update_query: &ParsedUpdateQuery,
        table_schema: &TableSchema,
    ) -> Result<Vec<(usize, Vec<u8>)>, QueryResult> {
        let mut compiled = Vec::new();

        for (field_name, value_str) in &update_query.assignments {
            let mut matched_indices = Vec::new();
            for (idx, field) in table_schema.fields.iter().enumerate() {
                if let Some((tbl, fld)) = field_name.split_once('.') {
                    if field.table_name == tbl && field.name == fld {
                        matched_indices.push(idx);
                    }
                } else if field.name == *field_name {
                    matched_indices.push(idx);
                }
            }

            if matched_indices.is_empty() {
                return Err(QueryResult::user_input_wrong(format!(
                    "Column '{}' not found",
                    field_name
                )));
            }

            if matched_indices.len() > 1 {
                return Err(QueryResult::user_input_wrong(format!(
                    "Column '{}' is ambiguous",
                    field_name
                )));
            }

            let field_idx = matched_indices[0];

            if compiled.iter().any(|(idx, _)| *idx == field_idx) {
                return Err(QueryResult::user_input_wrong(format!(
                    "Column '{}' is assigned more than once",
                    field_name
                )));
            }

            let field_schema = &table_schema.fields[field_idx];
            let compiled_val = Self::compile_value(value_str, field_schema)?;
            compiled.push((field_idx, compiled_val));
        }

        if compiled.is_empty() {
            return Err(QueryResult::user_input_wrong(
                "Expected at least one assignment in SET clause".to_string(),
            ));
        }

        Ok(compiled)
    }

    fn plan_tree_node(schema: &Schema, node: ParsedQueryTreeNode) -> Result<PlanNode, QueryResult> {
        match node {
            ParsedQueryTreeNode::SingleQuery(select_query) => {
                let source_plan = Self::plan_source(schema, select_query.source)?;
                let source_schema = source_plan
                    .get_schema(schema)
                    .map_err(|_| QueryResult::user_input_wrong("".to_string()))?;
                let mut projected_fields = Vec::new();
                if select_query.result.len() == 1 && select_query.result[0] == "*" {
                    projected_fields = source_schema.fields.clone();
                } else {
                    for req_field in select_query.result {
                        let field = Self::resolve_field(&req_field, &source_schema)?;
                        projected_fields.push(field);
                    }
                }

                let filtered_plan = if let Some(cond) = select_query.conditions {
                    let compiled = Self::compile_condition_expr(schema, &source_schema, &cond)?;
                    let hinted_source = Self::apply_scan_hint_to_source(schema, source_plan, &compiled);
                    PlanNode::Filter {
                        source: Box::new(hinted_source),
                        condition: compiled,
                    }
                } else {
                    source_plan
                };

                Ok(PlanNode::Project {
                    source: Box::new(filtered_plan),
                    fields: projected_fields,
                })
            }
            ParsedQueryTreeNode::SetOperation(set_op) => {
                if set_op.operands.is_empty() {
                    return Err(QueryResult::msg("Set operation with no operands"));
                }

                let mut iter = set_op.operands.into_iter();
                let first = iter.next().unwrap();
                let mut current_plan = Self::plan_tree_node(schema, first)?;

                for next_operand in iter {
                    let next_plan = Self::plan_tree_node(schema, next_operand)?;

                    let left_schema = current_plan
                        .get_schema(schema)
                        .map_err(|_| QueryResult::user_input_wrong("".to_string()))?;
                    let right_schema = next_plan
                        .get_schema(schema)
                        .map_err(|_| QueryResult::user_input_wrong("".to_string()))?;

                    if left_schema.fields.len() != right_schema.fields.len() {
                        return Err(QueryResult::user_input_wrong(
                            "Set operation operands have different column counts".into(),
                        ));
                    }
                    //ToDo Check all Fields

                    current_plan = PlanNode::SetOperation {
                        op: match set_op.operation {
                            ParsedSetOperator::Union => ParsedSetOperator::Union,
                            ParsedSetOperator::Intersect => ParsedSetOperator::Intersect,
                            ParsedSetOperator::Except => ParsedSetOperator::Except,
                            ParsedSetOperator::Times => ParsedSetOperator::Times,
                            ParsedSetOperator::All => ParsedSetOperator::All,
                            ParsedSetOperator::Minus => ParsedSetOperator::Minus,
                        },
                        left: Box::new(current_plan),
                        right: Box::new(next_plan),
                    };
                }

                Ok(current_plan)
            }
        }
    }

    fn plan_source(schema: &Schema, source: ParsedSource) -> Result<PlanNode, QueryResult> {
        match source {
            ParsedSource::Table(table_name) => {
                let table_id = Self::find_table_id(schema, &table_name)?;
                // Default scan
                Ok(PlanNode::SeqScan {
                    table_id,
                    table_name: table_name.clone(),
                    operation: SqlConditionOpCode::SelectFTS,
                    seek_key: None,
                    index_table_id: None,
                    index_on_column: None,
                })
            }
            ParsedSource::SubQuery(sub_node) => Self::plan_tree_node(schema, *sub_node),
            ParsedSource::Join(join_box) => {
                let parsed_join = *join_box;
                let mut sources = parsed_join.sources.into_iter();
                let first_source = sources
                    .next()
                    .ok_or(QueryResult::msg("Join with no sources"))?;

                let mut current_plan = Self::plan_source(schema, first_source)?;
                let mut conditions_iter = parsed_join.conditions.into_iter();

                for next_source in sources {
                    let next_plan = Self::plan_source(schema, next_source)?;
                    let cond = conditions_iter
                        .next()
                        .ok_or(QueryResult::msg("Missing join condition"))?;

                    let left_schema = current_plan
                        .get_schema(schema)
                        .map_err(|_| QueryResult::user_input_wrong("".to_string()))?;
                    let right_schema = next_plan
                        .get_schema(schema)
                        .map_err(|_| QueryResult::user_input_wrong("".to_string()))?;

                    let join_conditions =
                        if cond.join_type == JoinType::Natural {
                            let mut natural_conds = Vec::new();
                            for l_field in &left_schema.fields {
                                if let Some(r_field) = right_schema
                                    .fields
                                    .iter()
                                    .find(|rf| rf.name == l_field.name)
                                {
                                    natural_conds.push((l_field.clone(), r_field.clone()));
                                    //ToDo Revisit this when we have multi-joins
                                    break
                                }
                            }
                            natural_conds
                        } else {
                            let resolve = |token: &str, l_sch: &TableSchema, r_sch: &TableSchema|
                                       -> Result<(char, Field), QueryResult>
                            {
                                let parts: Vec<&str> = token.split('.').collect();
                                if parts.len() == 2 {
                                    let field = Field {
                                        field_type: Type::Null,
                                        name: parts[1].to_string(),
                                        table_name: parts[0].to_string(),
                                    };
                                    if l_sch.get_column_and_field(&field).is_some() {
                                        return Ok(('L', field));
                                    } else if r_sch.get_column_and_field(&field).is_some() {
                                        return Ok(('R', field));
                                    } else {
                                        return Err(QueryResult::user_input_wrong(format!(
                                            "Column '{:?}' not found in join source", field
                                        )));
                                    }
                                } else if parts.len() == 1 {
                                    let name = parts[0];
                                    let left_matches: Vec<Field> = l_sch
                                        .fields
                                        .iter()
                                        .filter(|f| f.name == name)
                                        .cloned()
                                        .collect();
                                    let right_matches: Vec<Field> = r_sch
                                        .fields
                                        .iter()
                                        .filter(|f| f.name == name)
                                        .cloned()
                                        .collect();

                                    if left_matches.len() + right_matches.len() == 0 {
                                        return Err(QueryResult::user_input_wrong(format!(
                                            "Column '{}' not found in join source",
                                            token
                                        )));
                                    }
                                    if left_matches.len() + right_matches.len() > 1 {
                                        return Err(QueryResult::user_input_wrong(format!(
                                            "Column '{}' is ambiguous in join condition",
                                            token
                                        )));
                                    }

                                    if let Some(field) = left_matches.into_iter().next() {
                                        return Ok(('L', field));
                                    }
                                    if let Some(field) = right_matches.into_iter().next() {
                                        return Ok(('R', field));
                                    }
                                    unreachable!();
                                } else {
                                    return Err(QueryResult::user_input_wrong(format!(
                                        "Column '{:?}' is Invalid", token
                                    )));
                                }
                            };

                            let left_res = resolve(&cond.left, &left_schema, &right_schema)?;
                            let right_res = resolve(&cond.right, &left_schema, &right_schema)?;

                            let (left_field, right_field) = match (left_res.0, right_res.0) {
                                ('L', 'R') => (left_res.1, right_res.1),
                                ('R', 'L') => (right_res.1, left_res.1),
                                ('L', 'L') | ('R', 'R') => {
                                    return Err(QueryResult::user_input_wrong(
                                        "Join condition must reference one column from each side"
                                            .into(),
                                    ));
                                }
                                _ => unreachable!(),
                            };
                            vec![(left_field, right_field)]
                        };
                    let (mut left_op, mut right_op) = left_schema
                        .get_join_ops(&right_schema, &join_conditions[0].0, &join_conditions[0].1)
                        .map_err(|_| {
                            QueryResult::user_input_wrong("Cannot Get Join Operation".to_string())
                        })?;

                    if left_op == JoinOp::Scan
                        && Self::find_index_table_id(
                            schema,
                            &join_conditions[0].0.table_name,
                            &join_conditions[0].0.name,
                        )
                        .is_some()
                    {
                        left_op = JoinOp::Index;
                    }

                    if right_op == JoinOp::Scan
                        && Self::find_index_table_id(
                            schema,
                            &join_conditions[0].1.table_name,
                            &join_conditions[0].1.name,
                        )
                        .is_some()
                    {
                        right_op = JoinOp::Index;
                    }

                    current_plan = PlanNode::Join {
                        left: Box::new(current_plan),
                        right: Box::new(next_plan),
                        join_type: cond.join_type,
                        conditions: join_conditions,
                        left_join_op: left_op,
                        right_join_op: right_op,
                    };
                }
                Ok(current_plan)
            }
        }
    }

    pub fn find_table_id(schema: &Schema, table_name: &str) -> Result<usize, QueryResult> {
        schema
            .table_index
            .index
            .iter()
            .position(|t| t == &TableName::from(table_name))
            .ok_or_else(|| {
                QueryResult::user_input_wrong(format!("Table '{}' not found", table_name))
            })
    }

    fn resolve_field(request: &str, schema: &TableSchema) -> Result<Field, QueryResult> {
        let matches: Vec<_> = schema.fields.iter().filter(|f| f.name == request).collect();

        if matches.is_empty() {
            let parts: Vec<&str> = request.split('.').collect();
            if parts.len() == 2 {
                let found = schema
                    .fields
                    .iter()
                    .find(|f| f.name == parts[1] && f.table_name == parts[0]);
                return match found {
                    Some(pair) => Ok(pair.clone()),
                    None => {
                        let fallback: Vec<Field> = schema
                            .fields
                            .iter()
                            .filter(|f| f.name == parts[1])
                            .cloned()
                            .collect();
                        if fallback.len() == 1 {
                            Ok(fallback[0].clone())
                        } else {
                            Err(QueryResult::user_input_wrong(format!(
                                "Column '{}.{}' not found in source",
                                parts[0], parts[1]
                            )))
                        }
                    }
                };
            }
            Err(QueryResult::user_input_wrong(format!(
                "Column '{}' not found",
                request
            )))
        } else if matches.len() > 1 {
            Err(QueryResult::user_input_wrong(format!(
                "Column '{}' is ambiguous. Found in tables: {:?}",
                request,
                matches.iter().collect::<Vec<_>>()
            )))
        } else {
            //if field.name.split_once('.').is_none() {
            Ok(matches[0].clone())
        }
    }

    fn compile_condition_expr(
        global_schema: &Schema,
        schema: &TableSchema,
        expr: &ParsedConditionExpr,
    ) -> Result<CompiledConditionExpr, QueryResult> {
        match expr {
            ParsedConditionExpr::Logical { op, left, right } => {
                let compiled_op = match op {
                    ParsedLogicalOp::And => CompiledLogicalOp::And,
                    ParsedLogicalOp::Or => CompiledLogicalOp::Or,
                    ParsedLogicalOp::Xor => CompiledLogicalOp::Xor,
                };
                Ok(CompiledConditionExpr::Logical {
                    op: compiled_op,
                    left: Box::new(Self::compile_condition_expr(global_schema, schema, left)?),
                    right: Box::new(Self::compile_condition_expr(global_schema, schema, right)?),
                })
            }
            ParsedConditionExpr::Predicate(pred) => match pred {
                ParsedPredicateExpr::Compare {
                    left,
                    operator,
                    right,
                } => {
                    let col_idx = match Self::resolve_value_as_column_index(left, schema) {
                        Ok(idx) => idx,
                        Err(_) => {
                            return Ok(CompiledConditionExpr::Predicate(CompiledPredicateExpr::Compare {
                                column_idx: 0,
                                op: SqlStatementComparisonOperator::None,
                                value: Vec::new(),
                            }))
                        }
                    };
                    let field = &schema.fields[col_idx];
                    let right_token = match right {
                        ParsedValueExpr::Token(t) => t,
                    };
                    let value = Self::compile_value(&right_token.clone(), field)?;
                    let op = Self::compile_comparison_operator(operator)?;
                    Ok(CompiledConditionExpr::Predicate(CompiledPredicateExpr::Compare {
                        column_idx: col_idx,
                        op,
                        value,
                    }))
                }
                ParsedPredicateExpr::InSubquery { left, subquery } => {
                    let col_idx = Self::resolve_value_as_column_index(left, schema)?;
                    let strategy = Self::compile_in_strategy(global_schema, subquery)?;
                    Ok(CompiledConditionExpr::Predicate(CompiledPredicateExpr::InSubquery {
                        column_idx: col_idx,
                        strategy,
                    }))
                }
            },
        }
    }

    fn resolve_value_as_column_index(
        value_expr: &ParsedValueExpr,
        schema: &TableSchema,
    ) -> Result<usize, QueryResult> {
        let token = match value_expr {
            ParsedValueExpr::Token(t) => t,
        };
        let field = Self::resolve_field(token, schema)?;
        schema
            .fields
            .iter()
            .position(|f| Self::same_field(f, &field))
            .ok_or_else(|| QueryResult::user_input_wrong("Column not found in schema".to_string()))
    }

    fn compile_in_strategy(
        schema: &Schema,
        subquery: &Box<ParsedQueryTreeNode>,
    ) -> Result<CompiledInStrategy, QueryResult> {
        if let ParsedQueryTreeNode::SingleQuery(sq) = subquery.as_ref() {
            if sq.conditions.is_none() && sq.result.len() == 1 {
                if let ParsedSource::Table(table_name) = &sq.source {
                    let table_id = Self::find_table_id(schema, table_name)?;
                    let table_schema = &schema.tables[table_id];
                    let field = Self::resolve_field(&sq.result[0], table_schema)?;
                    let field_idx = table_schema
                        .fields
                        .iter()
                        .position(|f| Self::same_field(f, &field))
                        .ok_or_else(|| QueryResult::user_input_wrong("Invalid IN subquery field".to_string()))?;

                    if field_idx == table_schema.key_position {
                        return Ok(CompiledInStrategy::KeyLookup { table_id });
                    }

                    if let Some(index_table_id) = Self::find_index_table_id(schema, table_name, &field.name) {
                        return Ok(CompiledInStrategy::IndexLookup { index_table_id });
                    }
                }
            }
        }

        let plan = Self::plan_tree_node(schema, (*subquery.clone()).clone())?;
        Ok(CompiledInStrategy::Materialize(Box::new(plan)))
    }

    fn derive_scan_hint_for_table(
        _global_schema: &Schema,
        table_schema: &TableSchema,
        condition: &Option<CompiledConditionExpr>,
    ) -> (SqlConditionOpCode, Option<Vec<u8>>) {
        if let Some(CompiledConditionExpr::Predicate(CompiledPredicateExpr::Compare {
            column_idx,
            op,
            value,
        })) = condition
        {
            if *column_idx != table_schema.key_position {
                return (SqlConditionOpCode::SelectFTS, None);
            }
            return match op {
                SqlStatementComparisonOperator::Equal => {
                    (SqlConditionOpCode::SelectKeyUnique, Some(value.clone()))
                }
                SqlStatementComparisonOperator::Greater
                | SqlStatementComparisonOperator::GreaterOrEqual => {
                    (SqlConditionOpCode::SelectKeyRange, Some(value.clone()))
                }
                _ => (SqlConditionOpCode::SelectFTS, None),
            };
        }
        (SqlConditionOpCode::SelectFTS, None)
    }

    fn apply_scan_hint_to_source(global_schema: &Schema, plan: PlanNode, condition: &CompiledConditionExpr) -> PlanNode {
        match plan {
            PlanNode::SeqScan {
                table_id,
                table_name,
                operation: existing_operation,
                seek_key: existing_seek_key,
                index_table_id: existing_index_table_id,
                index_on_column: existing_index_on_column,
            } => {
                let mut operation = existing_operation;
                let mut seek_key = existing_seek_key;
                let mut index_table_id = existing_index_table_id;
                let mut index_on_column = existing_index_on_column;

                if let CompiledConditionExpr::Predicate(CompiledPredicateExpr::Compare {
                    column_idx,
                    op,
                    value,
                }) = condition
                {
                    let table_schema = &global_schema.tables[table_id];
                    if *column_idx == table_schema.key_position {
                        operation = match op {
                            SqlStatementComparisonOperator::Equal => SqlConditionOpCode::SelectKeyUnique,
                            SqlStatementComparisonOperator::Greater
                            | SqlStatementComparisonOperator::GreaterOrEqual => SqlConditionOpCode::SelectKeyRange,
                            _ => SqlConditionOpCode::SelectFTS,
                        };
                        if matches!(operation, SqlConditionOpCode::SelectKeyUnique | SqlConditionOpCode::SelectKeyRange) {
                            seek_key = Some(value.clone());
                        }
                    } else if let Some(field) = table_schema.fields.get(*column_idx) {
                        if let Some(index_id) = Self::find_index_table_id(global_schema, &table_name, &field.name) {
                            operation = match op {
                                SqlStatementComparisonOperator::Equal => SqlConditionOpCode::SelectIndexUnique,
                                _ => SqlConditionOpCode::SelectIndexRange,
                            };
                            index_table_id = Some(index_id);
                            index_on_column = Some(*column_idx);
                            seek_key = Some(value.clone());
                        }
                    }
                }

                PlanNode::SeqScan {
                    table_id,
                    table_name,
                    operation,
                    seek_key,
                    index_table_id,
                    index_on_column,
                }
            }
            other => other,
        }
    }

    fn pushdown_projections(
        _global_schema: &Schema,
        plan: PlanNode,
        _required_fields: Vec<Field>,
    ) -> Result<PlanNode, QueryResult> {
        Ok(plan)
    }

    fn prune_redundant_projections(
        _global_schema: &Schema,
        plan: PlanNode,
    ) -> Result<PlanNode, QueryResult> {
        Ok(plan)
    }

    fn extract_condition_fields(_condition: &CompiledConditionExpr, _schema: &TableSchema) -> Vec<Field> {
        vec![]
    }

    fn fields_for_schema(required_fields: &Vec<Field>, schema: &TableSchema) -> Vec<Field> {
        let mut selected = Vec::new();
        for field in &schema.fields {
            if required_fields.iter().any(|req| Self::same_field(req, field)) {
                selected.push(field.clone());
            }
        }
        selected
    }

    fn ordered_intersection(candidates: &Vec<Field>, required_fields: &Vec<Field>) -> Vec<Field> {
        let mut selected = Vec::new();
        for candidate in candidates {
            if required_fields
                .iter()
                .any(|required| Self::same_field(candidate, required))
            {
                selected.push(candidate.clone());
            }
        }
        selected
    }

    fn same_field(a: &Field, b: &Field) -> bool {
        a.name == b.name && a.table_name == b.table_name
    }

    fn push_unique_field(fields: &mut Vec<Field>, field: Field) {
        if !fields.iter().any(|existing| Self::same_field(existing, &field)) {
            fields.push(field);
        }
    }

    fn resolve_condition_side(
        column_ref: &str,
        left_schema: &TableSchema,
        right_schema: &TableSchema,
    ) -> Result<ConditionSide, QueryResult> {
        if let Some((table_name, field_name)) = column_ref.split_once('.') {
            let left_matches = left_schema
                .fields
                .iter()
                .filter(|f| f.table_name == table_name && f.name == field_name)
                .count();
            let right_matches = right_schema
                .fields
                .iter()
                .filter(|f| f.table_name == table_name && f.name == field_name)
                .count();

            return match left_matches + right_matches {
                0 => Ok(ConditionSide::Both),
                1 => {
                    if left_matches == 1 {
                        Ok(ConditionSide::Left)
                    } else {
                        Ok(ConditionSide::Right)
                    }
                }
                _ => Ok(ConditionSide::Both),
            };
        }

        let left_matches = left_schema
            .fields
            .iter()
            .filter(|f| f.name == column_ref)
            .count();
        let right_matches = right_schema
            .fields
            .iter()
            .filter(|f| f.name == column_ref)
            .count();

        match left_matches + right_matches {
            0 => Ok(ConditionSide::Both),
            1 => {
                if left_matches == 1 {
                    Ok(ConditionSide::Left)
                } else {
                    Ok(ConditionSide::Right)
                }
            }
            _ => Ok(ConditionSide::Both),
        }
    }

    fn compile_value(value: &String, field_schema: &Field) -> Result<Vec<u8>, QueryResult> {
        match field_schema.field_type {
            Type::Integer => {
                let _ = value.parse::<i32>().map_err(|_| {
                    QueryResult::user_input_wrong(format!("'{}' is not a valid integer", value))
                })?;
                Ok(Serializer::parse_int(value)
                    .map_err(QueryResult::err)?
                    .to_vec())
            }
            Type::String => Ok(Serializer::parse_string(value).to_vec()),
            Type::Varchar(max_len) => {
                if value.len() > max_len {
                    return Err(QueryResult::user_input_wrong(format!(
                        "'{}' exceeds VARCHAR({})",
                        value, max_len
                    )));
                }
                Ok(Serializer::parse_varchar(value, max_len))
            }
            Type::Date => Ok(Vec::from(
                Serializer::parse_date(value).map_err(QueryResult::err)?,
            )),
            Type::Boolean => Ok(vec![
                Serializer::parse_bool(value).map_err(QueryResult::err)?,
            ]),
            Type::Null => Ok(vec![]),
        }
    }

    fn compile_comparison_operator(
        token: &str,
    ) -> Result<SqlStatementComparisonOperator, QueryResult> {
        match token {
            "=" => Ok(SqlStatementComparisonOperator::Equal),
            "<" => Ok(SqlStatementComparisonOperator::Lesser),
            ">" => Ok(SqlStatementComparisonOperator::Greater),
            "<=" => Ok(SqlStatementComparisonOperator::LesserOrEqual),
            ">=" => Ok(SqlStatementComparisonOperator::GreaterOrEqual),
            _ => Err(QueryResult::user_input_wrong(format!(
                "Unknown comparison operator: {}",
                token
            ))),
        }
    }

    fn find_index_table_id(
        schema: &Schema,
        base_table: &str,
        column: &str,
    ) -> Option<usize> {
        let index_name = schema
            .index_definitions
            .iter()
            .find(|idx| idx.base_table == base_table && idx.column_name == column)
            .map(|idx| idx.index_name.clone())?;
        schema.table_index.index.iter().position(|t| t == &TableName::from(index_name.as_str()))
    }
}

enum ConditionSide {
    Left,
    Right,
    Both,
}

impl FromStr for Type {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lowered = s.trim().to_lowercase();
        if lowered.starts_with("varchar(") && lowered.ends_with(')') {
            let inner = &lowered[8..lowered.len() - 1];
            let length = inner
                .parse::<usize>()
                .map_err(|_| format!("Invalid type: {}", s))?;
            if length == 0 {
                return Err("Invalid type: VARCHAR must have length > 0".to_string());
            }
            return Ok(Type::Varchar(length));
        }

        match lowered.as_str() {
            "null" => Ok(Type::Null),
            "integer" => Ok(Type::Integer),
            "string" => Ok(Type::String),
            "date" => Ok(Type::Date),
            "boolean" => Ok(Type::Boolean),
            _ => Err(format!("Invalid type: {}", s)),
        }
    }
}
