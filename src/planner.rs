use crate::executor::{Field, QueryResult};
use crate::pager::{Key, Position, Row, TableName, Type};
use crate::parser::{
    JoinOp, JoinType, ParsedJoinCondition, ParsedQuery, ParsedQueryTreeNode, ParsedSetOperator,
    ParsedSource,
};
use crate::schema::{Schema, TableSchema};
use crate::serializer::Serializer;
use crate::status::Status;
use std::str::FromStr;

/// ## Responsibilities
/// - verifying queries (do they match the Query)
/// - planning the queries
/// - compiling the queries into bytecode
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

/// A Node in the Query Execution Plan Tree
#[derive(Debug)]
pub enum PlanNode {
    SeqScan {
        table_id: usize,
        table_name: String,
        operation: SqlConditionOpCode,
        conditions: Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
    },
    Filter {
        source: Box<PlanNode>,
        conditions: Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
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
    pub conditions: Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
}

#[derive(Debug)]
pub struct CompiledCreateTableQuery {
    pub table_name: String,
    pub schema: TableSchema,
}

#[derive(Debug)]
pub struct CompiledDropTableQuery {
    pub table_id: usize,
}

#[derive(Debug)]
pub enum CompiledQuery {
    CreateTable(CompiledCreateTableQuery),
    DropTable(CompiledDropTableQuery),
    Select(CompiledSelectQuery),
    Insert(CompiledInsertQuery),
    Delete(CompiledDeleteQuery),
}

impl Planner {
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
                        .map_err(|_| QueryResult::user_input_wrong("wrong type".to_string()))?;
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
                    btree_order: 0
                };

                Ok(CompiledQuery::CreateTable(CompiledCreateTableQuery {
                    table_name: create_table_query.table_name,
                    schema,
                }))
            }

            ParsedQuery::DropTable(drop_table_query) => {
                let table_id = Self::find_table_id(schema, &drop_table_query.table_name)?;
                Ok(CompiledQuery::DropTable(CompiledDropTableQuery {
                    table_id,
                }))
            }

            ParsedQuery::Delete(delete_query) => {
                let table_id = Self::find_table_id(schema, &delete_query.table_name)?;
                let table_schema = &schema.tables[table_id];

                let (operation, conditions) =
                    Self::compile_conditions(delete_query.conditions, &table_schema)?;

                Ok(CompiledQuery::Delete(CompiledDeleteQuery {
                    table_id,
                    operation,
                    conditions,
                }))
            }
        }
    }

    fn plan_tree_node(schema: &Schema, node: ParsedQueryTreeNode) -> Result<PlanNode, QueryResult> {
        match node {
            ParsedQueryTreeNode::SingleQuery(select_query) => {
                let source_plan = Self::plan_source(schema, select_query.source)?;
                let source_schema = source_plan
                    .get_schema(schema)
                    .map_err(|_| QueryResult::user_input_wrong("".to_string()))?;
                let (derived_op, compiled_conditions) =
                    Self::compile_conditions(select_query.conditions, &source_schema)?;

                // Optimization: Pushdown vs Filter Node
                // If the source is a SeqScan, we can inject the conditions directly (efficient scan).
                // Otherwise (Join, Subquery), we wrap the plan in a Filter node.
                let filtered_plan = if let PlanNode::SeqScan {
                    table_id,
                    table_name,
                    ..
                } = source_plan
                {
                    PlanNode::SeqScan {
                        table_id,
                        table_name,
                        operation: derived_op,
                        conditions: compiled_conditions,
                    }
                } else {
                    let has_conditions = compiled_conditions
                        .iter()
                        .any(|(op, _)| *op != SqlStatementComparisonOperator::None);
                    if has_conditions {
                        PlanNode::Filter {
                            source: Box::new(source_plan),
                            conditions: compiled_conditions,
                        }
                    } else {
                        source_plan
                    }
                };

                let mut projected_fields = Vec::new();
                if select_query.result.len() == 1 && select_query.result[0] == "*" {
                    projected_fields = source_schema.fields;
                } else {
                    for req_field in select_query.result {
                        let field = Self::resolve_field(&req_field, &source_schema)?;
                        projected_fields.push(field);
                    }
                }

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
                    conditions: vec![],
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
                                let mut field = Field {
                                    field_type: Type::Null,
                                    name: parts[0].to_string(),
                                    table_name: "".to_string(),
                                };
                                if parts.len() == 2 {
                                    field.name = parts[1].to_string();
                                    field.table_name = parts[0].to_string();
                                } else {
                                    return Err(QueryResult::user_input_wrong(format!(
                                        "Column '{:?}' is Invalid", token
                                    )));
                                }
                                if l_sch.get_column_and_field(&field).is_some() {
                                    return Ok(('L', field));
                                } else if r_sch.get_column_and_field(&field).is_some() {
                                    return Ok(('R', field));
                                } else {
                                    return Err(QueryResult::user_input_wrong(format!(
                                        "Column '{:?}' not found in join source", field
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
                    let (left_op, right_op) = left_schema
                        .get_join_ops(&right_schema, &join_conditions[0].0, &join_conditions[0].1)
                        .map_err(|_| {
                            QueryResult::user_input_wrong("Cannot Get Join Operation".to_string())
                        })?;

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
                    None => Err(QueryResult::user_input_wrong(format!(
                        "Column '{}.{}' not found in source",
                        parts[0], parts[1]
                    ))),
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

    fn compile_conditions(
        source: Vec<(String, String, String)>,
        schema: &TableSchema,
    ) -> Result<
        (
            SqlConditionOpCode,
            Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
        ),
        QueryResult,
    > {
        let mut op = SqlConditionOpCode::SelectFTS;
        let mut compiled_conditions = Vec::new();
        let mut is_primary_key = true;

        for field in &schema.fields {
            let user_condition = source.iter().find(|(col_name, _, _)| {
                if let Some((tbl, fld)) = col_name.split_once('.') {
                    field.table_name == tbl && field.name == fld
                } else {
                    field.name == *col_name
                }
            });

            match user_condition {
                Some((_, op_str, val_str)) => {
                    let comparison_op = Planner::compile_comparison_operator(op_str)?;
                    let compiled_val = Self::compile_value(val_str, field)?;

                    if is_primary_key {
                        if comparison_op == SqlStatementComparisonOperator::Equal {
                            op = SqlConditionOpCode::SelectKeyUnique;
                        } else {
                            op = SqlConditionOpCode::SelectKeyRange;
                        }
                    }
                    compiled_conditions.push((comparison_op, compiled_val));
                }
                None => {
                    compiled_conditions.push((SqlStatementComparisonOperator::None, Vec::new()));
                }
            }
            is_primary_key = false;
        }
        Ok((op, compiled_conditions))
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
            Type::Date => Ok(Vec::from(
                Serializer::parse_date(value).map_err(QueryResult::err)?,
            )),
            Type::Boolean => Ok(vec![
                Serializer::parse_bool(value).map_err(QueryResult::err)?,
            ]),
            Type::Null => Ok(vec![]),
            _ => Err(QueryResult::user_input_wrong(format!(
                "Unsupported type for compilation: {:?}",
                field_schema.field_type
            ))),
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
}

impl FromStr for Type {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "null" => Ok(Type::Null),
            "integer" => Ok(Type::Integer),
            "string" => Ok(Type::String),
            "date" => Ok(Type::Date),
            "boolean" => Ok(Type::Boolean),
            _ => Err(format!("Invalid type: {}", s)),
        }
    }
}
