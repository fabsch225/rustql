use crate::executor::{Field, QueryResult, Schema, TableSchema};
use crate::pager::{Key, Position, Row, TableName, Type};
use crate::parser::{
    JoinType, ParsedJoinCondition, ParsedQuery, ParsedQueryTreeNode, ParsedSetOperator,
    ParsedSource,
};
use crate::serializer::Serializer;
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
        fields: Vec<(String, Field)>,
    },
    Join {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        join_type: JoinType,
        conditions: Vec<(String, String)>,
    },
    SetOperation {
        op: ParsedSetOperator,
        left: Box<PlanNode>,
        right: Box<PlanNode>,
    },
}

impl PlanNode {
    /// get the output schema of a node for parent resolution
    pub fn get_schema(&self, global_schema: &Schema) -> Vec<(String, Field)> {
        match self {
            PlanNode::SeqScan { table_id, table_name, .. } => {
                let table = &global_schema.tables[*table_id];
                table.fields.iter().map(|f| (table_name.clone(), f.clone())).collect()
            }
            PlanNode::Project { fields, .. } => fields.clone(),
            PlanNode::Join { left, right, .. } => {
                let mut s = left.get_schema(global_schema);
                s.extend(right.get_schema(global_schema));
                s
            }
            PlanNode::SetOperation { left, .. } => {
                // ToDo Check This
                left.get_schema(global_schema)
            },
            PlanNode::Filter { source, .. } => {
                source.get_schema(global_schema)
            }
        }
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
                    insert_query.fields = table_schema.fields.clone().into_iter().map(|f|{f.name}).collect();;
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
                    return Err(QueryResult::user_input_wrong("Cannot insert empty row".into()));
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

                let temp_schema_mapping: Vec<(String, Field)> = table_schema
                    .fields
                    .iter()
                    .map(|f| (delete_query.table_name.clone(), f.clone()))
                    .collect();

                let (operation, conditions) = Self::compile_conditions(
                    delete_query.conditions,
                    &temp_schema_mapping,
                )?;

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
                let source_schema = source_plan.get_schema(schema);
                let (derived_op, compiled_conditions) = Self::compile_conditions(
                    select_query.conditions,
                    &source_schema,
                )?;

                // Optimization: Pushdown vs Filter Node
                // If the source is a SeqScan, we can inject the conditions directly (efficient scan).
                // Otherwise (Join, Subquery), we wrap the plan in a Filter node.
                let filtered_plan = if let PlanNode::SeqScan { table_id, table_name, .. } = source_plan {
                    PlanNode::SeqScan {
                        table_id,
                        table_name,
                        operation: derived_op,
                        conditions: compiled_conditions,
                    }
                } else {
                    let has_conditions = compiled_conditions.iter().any(|(op, _)| *op != SqlStatementComparisonOperator::None);
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
                    projected_fields = source_schema;
                } else {
                    for req_field in select_query.result {
                        let (tbl, field) = Self::resolve_field(&req_field, &source_schema)?;
                        projected_fields.push((tbl, field));
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

                    let left_schema = current_plan.get_schema(schema);
                    let right_schema = next_plan.get_schema(schema);

                    if left_schema.len() != right_schema.len() {
                        return Err(QueryResult::user_input_wrong(
                            "Set operation operands have different column counts".into()
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
                    conditions: vec![]
                })
            }
            ParsedSource::SubQuery(sub_node) => {
                Self::plan_tree_node(schema, *sub_node)
            }
            ParsedSource::Join(join_box) => {
                let parsed_join = *join_box;
                let mut sources = parsed_join.sources.into_iter();
                let first_source = sources.next().ok_or(QueryResult::msg("Join with no sources"))?;

                let mut current_plan = Self::plan_source(schema, first_source)?;
                let mut conditions_iter = parsed_join.conditions.into_iter();

                for next_source in sources {
                    let next_plan = Self::plan_source(schema, next_source)?;
                    let cond = conditions_iter.next().ok_or(QueryResult::msg("Missing join condition"))?;

                    let left_schema = current_plan.get_schema(schema);
                    let right_schema = next_plan.get_schema(schema);

                    let join_conditions = if cond.join_type == JoinType::Natural {
                        let mut natural_conds = Vec::new();
                        for (_, l_field) in &left_schema {
                            if right_schema.iter().any(|(_, r_field)| r_field.name == l_field.name) {
                                natural_conds.push((l_field.name.clone(), l_field.name.clone()));
                            }
                        }
                        natural_conds
                    } else {
                        let resolve = |token: &str, l_sch: &[(String, Field)], r_sch: &[(String, Field)]|
                                       -> Result<(char, String), QueryResult>
                            {
                                if token.contains('.') {
                                    let parts: Vec<&str> = token.split('.').collect();
                                    if parts.len() != 2 {
                                        return Err(QueryResult::user_input_wrong(format!("Invalid field reference: {}", token)));
                                    }
                                    let (tbl, col) = (parts[0], parts[1]);

                                    if l_sch.iter().any(|(t, f)| t == tbl && f.name == col) {
                                        return Ok(('L', col.to_string()));
                                    } else if r_sch.iter().any(|(t, f)| t == tbl && f.name == col) {
                                        return Ok(('R', col.to_string()));
                                    } else {
                                        return Err(QueryResult::user_input_wrong(format!(
                                            "Column '{}.{}' not found in join source", tbl, col
                                        )));
                                    }
                                } else {
                                    // Optimized lookup: find first match in Left, then Right
                                    let in_left = l_sch.iter().any(|(_, f)| f.name == token);
                                    let in_right = r_sch.iter().any(|(_, f)| f.name == token);

                                    if in_left && in_right {
                                        return Err(QueryResult::user_input_wrong(format!("Ambiguous column '{}' in join condition", token)));
                                    } else if in_left {
                                        return Ok(('L', token.to_string()));
                                    } else if in_right {
                                        return Ok(('R', token.to_string()));
                                    } else {
                                        return Err(QueryResult::user_input_wrong(format!("Column '{}' not found in join sides", token)));
                                    }
                                }
                            };

                        let left_res = resolve(&cond.left, &left_schema, &right_schema)?;
                        let right_res = resolve(&cond.right, &left_schema, &right_schema)?;

                        let (left_field, right_field) = match (left_res.0, right_res.0) {
                            ('L', 'R') => (left_res.1, right_res.1),
                            ('R', 'L') => (right_res.1, left_res.1),
                            ('L', 'L') | ('R', 'R') => {
                                return Err(QueryResult::user_input_wrong(
                                    "Join condition must reference one column from each side".into(),
                                ));
                            }
                            _ => unreachable!(),
                        };
                        vec![(left_field, right_field)]
                    };

                    current_plan = PlanNode::Join {
                        left: Box::new(current_plan),
                        right: Box::new(next_plan),
                        join_type: cond.join_type,
                        conditions: join_conditions,
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
            .ok_or_else(|| QueryResult::user_input_wrong(format!("Table '{}' not found", table_name)))
    }

    /// Resolves a field name like "id" or "table.id" against a schema list of (Table, Field).
    fn resolve_field(
        request: &str,
        available_fields: &[(String, Field)],
    ) -> Result<(String, Field), QueryResult> {
        let parts: Vec<&str> = request.split('.').collect();

        if parts.len() == 2 {
            // Specific request: table.column
            let t_req = parts[0];
            let c_req = parts[1];

            let found = available_fields.iter().find(|(t, f)| t == t_req && f.name == c_req);
            match found {
                Some(pair) => Ok(pair.clone()),
                None => Err(QueryResult::user_input_wrong(format!(
                    "Column '{}.{}' not found in source", t_req, c_req
                ))),
            }
        } else {
            // Ambiguous request: column
            let matches: Vec<_> = available_fields
                .iter()
                .filter(|(_, f)| f.name == request)
                .collect();

            if matches.is_empty() {
                Err(QueryResult::user_input_wrong(format!(
                    "Column '{}' not found", request
                )))
            } else if matches.len() > 1 {
                Err(QueryResult::user_input_wrong(format!(
                    "Column '{}' is ambiguous. Found in tables: {:?}",
                    request,
                    matches.iter().map(|(t, _)| t).collect::<Vec<_>>()
                )))
            } else {
                Ok(matches[0].clone())
            }
        }
    }

    fn compile_conditions(
        source: Vec<(String, String, String)>,
        schema: &[(String, Field)],
    ) -> Result<(SqlConditionOpCode, Vec<(SqlStatementComparisonOperator, Vec<u8>)>), QueryResult> {
        let mut op = SqlConditionOpCode::SelectFTS;
        let mut compiled_conditions = Vec::new();
        let mut is_primary_key = true; // Assumes index 0 is PK of the leading table

        for (table_alias, field) in schema {
            // Check if there is a condition matching this field (by alias.name or just name)
            // We need to be careful about ambiguity here, but for simplification we match first valid.
            let user_condition = source.iter().find(|(col_name, _, _)| {
                // col_name could be "id" or "users.id"
                if col_name.contains('.') {
                    col_name == &format!("{}.{}", table_alias, field.name)
                } else {
                    col_name == &field.name
                }
            });

            match user_condition {
                Some((_, op_str, val_str)) => {
                    let comparison_op = Planner::compile_comparison_operator(op_str)?;
                    let compiled_val = Self::compile_value(val_str, field)?;

                    // If we are on the first column, we infer Key operations.
                    // This is only strictly valid for SeqScan, but calculated here for convenience.
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
            Type::Boolean => Ok(vec![Serializer::parse_bool(value).map_err(QueryResult::err)?]),
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