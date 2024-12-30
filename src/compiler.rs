use std::str::FromStr;
use crate::executor::QueryResult;
use crate::pager::{Field, Key, Row, Schema, Type};
use crate::parser::ParsedQuery;
use crate::serializer::Serializer;
use crate::status::Status;
use crate::status::Status::ExceptionQueryMisformed;

/// ## Responsibilities
/// - verifying queries (do they match the Query)
/// - planning the queries
/// - compiling the queries into bytecode

pub struct Compiler {}

#[repr(u8)]
#[derive(Debug, PartialEq)]
pub enum SelectStatementOpCode {
    SelectFTS = 60u8,       //full table scan will be performed "Type 1"
    SelectFromIndex = 61u8, //we have not implemented indices :)
    SelectAtIndex = 62u8,   //we have not implemented indices :)
    SelectFromKey = 63u8,   // "Type 3"
    SelectAtKey = 64u8,     // "Type 2"
}

#[repr(u8)]
#[derive(Debug, PartialEq)]
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
    pub data: (Key, Row)
}

#[derive(Debug)]
pub struct CompiledSelectQuery {
    pub table_id: u8,
    pub operation: SelectStatementOpCode,
    pub result: Vec<Field>,
    pub conditions: Vec<(SqlStatementComparisonOperator, Vec<u8>)>
}

#[derive(Debug)]
pub struct CompiledCreateTableQuery {
    pub schema: Schema
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
}

impl Compiler {
    /// make ParsedQuery -> CompiledQuery
    pub fn compile(schema: &Schema, query: ParsedQuery) -> Result<CompiledQuery, QueryResult> {
        match query {
            ParsedQuery::Insert(insert_query) => {
                if insert_query.fields.len() != insert_query.values.len() {
                    return Err(QueryResult::user_input_wrong(format!("the fields and values must be the same amount")));
                }
                //we dont have nullable values...
                if insert_query.fields.len() != schema.fields.len() {
                    return Err(QueryResult::user_input_wrong(format!("all fields must be set, there is are nullable values")));
                }

                let mut data = Vec::new();
                for (field, value) in insert_query.fields.iter().zip(insert_query.values.iter()) {
                    let field_schema = schema.fields.iter().find(|f| &f.name == field)
                        .ok_or(QueryResult::user_input_wrong(format!("invalid field")))?;

                    let pre_compiled_value = Self::compile_value(value, field_schema)?;
                    data.push(pre_compiled_value);
                }

                let key = data[0].clone();
                let row: Vec<u8> = data[1..].iter().flat_map(|r| r.clone()).collect();

                Ok(CompiledQuery::Insert(CompiledInsertQuery {
                    table_id: 0,
                    data: (key, row)
                }))
            },
            ParsedQuery::Select(select_query) => {
                let mut result_fields = Vec::new();
                for field in select_query.result.iter() {
                    let field_schema = schema.fields.iter().find(|f| &f.name == field)
                        .ok_or(QueryResult::user_input_wrong(format!("at least one invalid field")))?;
                    result_fields.push(field_schema.clone());
                }

                let mut op = SelectStatementOpCode::SelectFTS;
                let mut conditions = Vec::new();
                for (field, operator, value) in select_query.conditions.iter() {
                    let field_schema = schema.fields.iter().find(|f| &f.name == field)
                        .ok_or(QueryResult::user_input_wrong(format!("at least one invalid field in the WHERE")))?;

                    let comparison_operator = Compiler::compile_comparison_operator(operator)?;

                    if &schema.fields[0].name == field {
                        if comparison_operator == SqlStatementComparisonOperator::Equal {
                            op = SelectStatementOpCode::SelectAtKey;
                        } else {
                            op = SelectStatementOpCode::SelectFromKey;
                        }
                    }

                    let pre_compiled_value = Self::compile_value(value, field_schema)?;

                    conditions.push((comparison_operator, pre_compiled_value));
                }

                Ok(CompiledQuery::Select(CompiledSelectQuery {
                    table_id: 0,
                    operation: op,
                    result: result_fields,
                    conditions,
                }))
            },
            ParsedQuery::CreateTable(create_table_query) => {
                let mut fields = Vec::new();
                for (name, type_str) in create_table_query.table_fields.iter().zip(create_table_query.table_types.iter()) {
                    let field_type = Type::from_str(type_str).map_err(|_| QueryResult::user_input_wrong(format!("wrong type")))?;
                    fields.push(Field { name: name.clone(), field_type });
                }

                let schema = Schema {
                    root: 0,
                    col_count: fields.len(),
                    col_length: fields.iter().map(|f| Serializer::get_size_of_type(&f.field_type).unwrap()).sum(),
                    key_length: Serializer::get_size_of_type(&fields[0].field_type).unwrap(),
                    key_type: fields[0].field_type.clone(),
                    row_length: fields.iter().map(|f| Serializer::get_size_of_type(&f.field_type).unwrap()).sum::<usize>() - Serializer::get_size_of_type(&fields[0].field_type).unwrap(),
                    fields,
                };

                Ok(CompiledQuery::CreateTable(CompiledCreateTableQuery { schema }))
            },
            ParsedQuery::DropTable(_) => {
                Ok(CompiledQuery::DropTable(CompiledDropTableQuery { table_id: 0 }))
            }
        }
    }

    fn compile_value(value: &String, field_schema: &Field) -> Result<Vec<u8>, QueryResult> {
        let parsed_value = match field_schema.field_type {
            Type::Integer => {
                value.parse::<i32>().map_err(|_| QueryResult::user_input_wrong(format!("invalid integer: {}", value)))?;
                Serializer::parse_int(value).to_vec()
            },
            //TODO other error handling in Serializer
            Type::String => Serializer::parse_string(value).to_vec(),
            Type::Date => Serializer::parse_date(value).to_vec(),
            Type::Boolean => vec![Serializer::parse_bool(value)],
            _ => return Err(QueryResult::user_input_wrong(format!("invalid type: {:?}", field_schema.field_type))),
        };
        Ok(parsed_value)
    }

    fn compile_comparison_operator(token: &str) -> Result<SqlStatementComparisonOperator, QueryResult> {
        match token {
            "=" => Ok(SqlStatementComparisonOperator::Equal),
            "<" => Ok(SqlStatementComparisonOperator::Lesser),
            ">" => Ok(SqlStatementComparisonOperator::Greater),
            "<=" => Ok(SqlStatementComparisonOperator::LesserOrEqual),
            ">=" => Ok(SqlStatementComparisonOperator::GreaterOrEqual),
            _ => Err(QueryResult::user_input_wrong(format!("Illegal Comparator: {}", token))),
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