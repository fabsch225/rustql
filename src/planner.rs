use crate::executor::{Field, QueryResult, Schema, TableSchema};
use crate::pager::{Key, Position, Row, Type, ROW_NAME_SIZE, TYPE_SIZE};
use crate::parser::ParsedQuery;
use crate::serializer::Serializer;
use crate::status::Status;
use crate::status::Status::ExceptionQueryMisformed;
use std::str::FromStr;

/// ## Responsibilities
/// - verifying queries (do they match the Query)
/// - planning the queries
/// - compiling the queries into bytecode

pub struct Planner {}

#[repr(u8)]
#[derive(Debug, PartialEq)]
pub enum SqlConditionOpCode {
    SelectFTS = 60u8,         //"Type 1" / full table scan will be performed
    SelectIndexRange = 61u8,  //we have not implemented indices :)
    SelectIndexUnique = 62u8, //we have not implemented indices :)
    SelectKeyRange = 63u8,    // "Type 3"
    SelectKeyUnique = 64u8, // "Type 2" / will only be used on a primary key with a unique constraint (generally, not all pks must be unique)
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
    pub table_id: u8,
    pub data: (Key, Row),
}

#[derive(Debug)]
pub struct CompiledSelectQuery {
    pub table_id: u8,
    pub operation: SqlConditionOpCode,
    pub result: Vec<Field>,
    pub conditions: Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
}

#[derive(Debug)]
pub struct CompiledDeleteQuery {
    pub table_id: u8,
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
    pub table_id: u8,
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
    /// make ParsedQuery -> CompiledQuery
    pub fn plan(schema: &Schema, query: ParsedQuery) -> Result<CompiledQuery, QueryResult> {
        match query {
            ParsedQuery::Insert(insert_query) => {
                if insert_query.fields.len() != insert_query.values.len() {
                    return Err(QueryResult::user_input_wrong(format!(
                        "the fields and values must be the same amount"
                    )));
                }
                //we dont have nullable values...
                //TODO add nullable Values!
                //TODO allow custom ordering of fields: INSERT INTO ... (Name, Id) VALUES (a, 1) <=> (Id, Name) VALUES  (1, a)
                if insert_query.fields.len() != schema.tables[0].fields.len() {
                    return Err(QueryResult::user_input_wrong(
                        "all fields must be set, there is are nullable values".to_string(),
                    ));
                }

                let mut data = Vec::new();
                for (field, value) in insert_query.fields.iter().zip(insert_query.values.iter()) {
                    let field_schema = schema.tables[0]
                        .fields
                        .iter()
                        .find(|f| &f.name == field)
                        .ok_or(QueryResult::user_input_wrong(format!(
                            "invalid field: {}",
                            field
                        )))?;

                    let pre_compiled_value = Self::compile_value(value, field_schema)?;
                    data.push(pre_compiled_value);
                }

                let key = data[0].clone();
                let row: Vec<u8> = data[1..].iter().flat_map(|r| r.clone()).collect();

                Ok(CompiledQuery::Insert(CompiledInsertQuery {
                    table_id: 0,
                    data: (key, row),
                }))
            }
            ParsedQuery::Select(select_query) => {
                let mut result = Vec::new();

                if select_query.result[0] == "*" {
                    result.append(&mut schema.tables[0].fields.clone());
                } else {
                    for field in select_query.result.iter() {
                        let field_schema = schema.tables[0]
                            .fields
                            .iter()
                            .find(|f| &f.name == field)
                            .ok_or(QueryResult::user_input_wrong(format!(
                                "at least one invalid field: {}",
                                field
                            )))?;
                        result.push(field_schema.clone());
                    }
                }

                let mut conditions = Vec::new();
                let operation = Self::compile_conditions(
                    select_query.conditions,
                    &mut conditions,
                    &schema.tables[0],
                )?;
                Ok(CompiledQuery::Select(CompiledSelectQuery {
                    table_id: 0,
                    operation,
                    result,
                    conditions,
                }))
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
                let col_count = fields.len();
                let schema = TableSchema {
                    root: Position::make_empty(),
                    next_position: Position::make_empty(), //(14 + fields.len() * (ROW_NAME_SIZE + TYPE_SIZE)) as Position, TODO
                    col_count,
                    whole_row_length: fields
                        .iter()
                        .map(|f| Serializer::get_size_of_type(&f.field_type).unwrap())
                        .sum(),
                    key_length: Serializer::get_size_of_type(&fields[0].field_type).unwrap(),
                    key_type: fields[0].field_type.clone(),
                    row_length: fields
                        .iter()
                        .map(|f| Serializer::get_size_of_type(&f.field_type).unwrap())
                        .sum::<usize>()
                        - Serializer::get_size_of_type(&fields[0].field_type).unwrap(),
                    fields,
                    entry_count: 0,
                    table_type: 0,
                };
                Ok(CompiledQuery::CreateTable(CompiledCreateTableQuery {
                    table_name: create_table_query.table_name,
                    schema,
                }))
            }
            ParsedQuery::DropTable(_) => Ok(CompiledQuery::DropTable(CompiledDropTableQuery {
                table_id: 0,
            })),
            ParsedQuery::Delete(delete_query) => {
                let mut conditions = Vec::new();
                let operation = Self::compile_conditions(
                    delete_query.conditions,
                    &mut conditions,
                    &schema.tables[0],
                )?;

                Ok(CompiledQuery::Delete(CompiledDeleteQuery {
                    table_id: 0,
                    operation,
                    conditions,
                }))
            }
        }
    }

    fn compile_conditions(
        source: Vec<(String, String, String)>,
        dest: &mut Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
        schema: &TableSchema,
    ) -> Result<SqlConditionOpCode, QueryResult> {
        let mut op = SqlConditionOpCode::SelectFTS;
        let mut at_id = true;

        for field in schema.fields.iter() {
            let cond = source.iter().find(|f| &f.0 == &field.name);
            if !cond.is_some() {
                dest.push((SqlStatementComparisonOperator::None, vec![]));
                at_id = false;
                continue;
            }
            let cond = cond.unwrap();
            let comparison_operator = Planner::compile_comparison_operator(&cond.1)?;

            if at_id {
                at_id = false;
                //TODO here check a unique constraint:
                /*if comparison_operator == SqlStatementComparisonOperator::Equal {
                    op = SqlConditionOpCode::SelectKeyUnique;
                } else {
                    op = SqlConditionOpCode::SelectKeyRange;
                }*/
                op = SqlConditionOpCode::SelectKeyRange;
            }

            let pre_compiled_value = Self::compile_value(&cond.2, field)?;

            dest.push((comparison_operator, pre_compiled_value));
        }
        Ok(op)
    }

    fn compile_value(value: &String, field_schema: &Field) -> Result<Vec<u8>, QueryResult> {
        let parsed_value = match field_schema.field_type {
            Type::Integer => {
                value.parse::<i32>().map_err(|_| {
                    QueryResult::user_input_wrong(format!("invalid integer: {}", value))
                })?;
                Serializer::parse_int(value)
                    .map_err(|s| QueryResult::err(s))?
                    .to_vec()
            }
            //TODO other error handling in Serializer
            Type::String => Serializer::parse_string(value).to_vec(),
            Type::Date => {
                Vec::from(Serializer::parse_date(value).map_err(|s| QueryResult::err(s))?)
            }
            Type::Boolean => vec![Serializer::parse_bool(value).map_err(|s| QueryResult::err(s))?],
            _ => {
                return Err(QueryResult::user_input_wrong(format!(
                    "invalid type: {:?}",
                    field_schema.field_type
                )))
            }
        };
        Ok(parsed_value)
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
                "Illegal Comparator: {}",
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
