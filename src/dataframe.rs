use std::fmt;
use std::fmt::{Display, Formatter};
use crate::btree::Btree;
use crate::cursor::BTreeCursor;
use crate::pager::{Position, Row, Type};
use crate::planner::{SqlConditionOpCode, SqlStatementComparisonOperator};
use crate::planner::SqlStatementComparisonOperator::{Equal, Greater, GreaterOrEqual};
use crate::schema::{Field, TableSchema};
use crate::serializer::Serializer;
use crate::status::Status;

#[derive(Debug, Clone)]
pub struct DataFrame
{
    pub identifier: String,
    pub header: Vec<Field>,
    pub(crate) row_source: Source
}

impl DataFrame
{
    pub fn from_memory(
        identifier: String,
        header: Vec<Field>,
        data: Vec<Vec<u8>>
    ) -> DataFrame {
        DataFrame {
            identifier,
            header,
            row_source: Source::Memory(MemorySource { data, idx: 0 })
        }
    }

    //TODO fix when there is a varchar--this is wrong, a string longer than 256 would be cut off
    pub fn msg(message: &str) -> DataFrame {
        DataFrame {
            identifier: "Message to the User".to_string(),
            header: vec![Field {
                field_type: Type::String,
                name: "Message".to_string(),
            }],
            row_source: Source::Memory(MemorySource { data: vec![Serializer::parse_string(message).to_vec()], idx: 0})
        }
    }

    pub fn get_data(mut self) -> Result<Vec<Row>, Status> {
        match self.row_source {
            Source::Memory(mut source) => {
                Ok(source.data)
            }
            Source::BTree(mut source) => {
                source.reset();
                let mut rows = vec![];
                while let Some(row) = source.next()? {
                    rows.push(row.clone());
                }
                Ok(rows)
            }
        }
    }

    pub fn join(
        &self,
        other: &DataFrame,
        conditions: &[(String, String)]
    ) -> Result<DataFrame, Status> {
        let left_header  = self.header.clone();
        let right_header = other.header.clone();

        let mut result_header = left_header.clone();
        result_header.extend(right_header.clone());

        let mut result_data: Vec<Vec<u8>> = Vec::new();

        let left_rows  = self.clone().get_data()?;
        let right_rows = other.clone().get_data()?;

        for l_row in left_rows {
            let l_fields = Serializer::split_row_into_fields(&l_row, &left_header)?;

            for r_row in right_rows.iter() {
                let r_fields = Serializer::split_row_into_fields(&r_row, &right_header)?;

                let mut match_found = true;

                for (l_col, r_col) in conditions {
                    let l_idx = left_header.iter()
                        .position(|f| f.name == *l_col)
                        //.ok_or(Status::Message(format!("Column '{}' not in left DF", l_col)))?;
                        .ok_or(Status::DataFrameJoinError)?;

                    let r_idx = right_header.iter()
                        .position(|f| f.name == *r_col)
                        //.ok_or(Status::Message(format!("Column '{}' not in right DF", r_col)))?;
                        .ok_or(Status::DataFrameJoinError)?;

                    if l_fields[l_idx] != r_fields[r_idx] {
                        match_found = false;
                        break;
                    }
                }

                if match_found {
                    let mut new_row = l_row.clone();
                    new_row.extend(r_row.clone());
                    result_data.push(new_row);
                }
            }
        }

        Ok(DataFrame::from_memory(
            self.identifier.clone(),
            result_header,
            result_data
        ))
    }
}

impl Display for DataFrame {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for field in &self.header {
            write!(f, "{}\t", field.name)?;
        }
        writeln!(f)?;

        for row in self.clone().get_data().expect("Error in the Data") {
            let mut position = 0;
            for field in &self.header {
                let field_type = &field.field_type;
                let field_len = Serializer::get_size_of_type(field_type).unwrap();
                let field_value = &row[position..position + field_len];
                let formatted_value =
                    Serializer::format_field(&field_value.to_vec(), field_type).unwrap();
                write!(f, "{}\t", formatted_value)?;
                position += field_len;
            }
            writeln!(f)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum Source {
    Memory(MemorySource),
    BTree(BTreeScanSource)
}

pub trait RowSource {
    fn next(&mut self) -> Result<Option<Vec<u8>>, Status>;
    fn reset(&mut self) -> Result<(), Status>;
}

#[derive(Debug, Clone)]
pub struct MemorySource {
    data: Vec<Row>,
    idx: usize,
}

impl MemorySource {
    pub(crate) fn new(data: Vec<Row>) -> MemorySource {
        MemorySource { data, idx: 0 }
    }
}

impl RowSource for MemorySource {
    fn next(&mut self) -> Result<Option<Vec<u8>>, Status> {
        if self.idx >= self.data.len() {
            return Ok(None);
        }
        let row = self.data[self.idx].clone();
        self.idx += 1;
        Ok(Some(row))
    }

    fn reset(&mut self) -> Result<(), Status> {
        self.idx = 0;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct BTreeScanSource {
    btree: Btree,
    schema: TableSchema,
    conditions: Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
    op_code: SqlConditionOpCode,
    cursor: BTreeCursor,
}

impl BTreeScanSource {
    pub(crate) fn new(
        btree: Btree,
        schema: TableSchema,
        op_code: SqlConditionOpCode,
        conditions: Vec<(SqlStatementComparisonOperator, Vec<u8>)>,
    ) -> Self {
        let cursor = BTreeCursor::new(btree.clone());
        Self {
            btree,
            schema,
            conditions,
            op_code,
            cursor,
        }
    }
}

impl RowSource for BTreeScanSource {
    fn next(&mut self) -> Result<Option<Vec<u8>>, Status> {
        loop {
            if !self.cursor.is_valid() {
                return Ok(None);
            }

            let (key, row_body) = self.cursor.current()?.ok_or(Status::InternalExceptionIntegrityCheckFailed)?;

            if Serializer::is_tomb(&key, &self.schema)? {
                self.cursor.advance()?;
                continue;
            }

            let full_row = Serializer::reconstruct_row(&key, &row_body, &self.schema)?;

            if Serializer::check_condition_on_bytes(&full_row, &self.conditions, &self.schema.fields) {
                self.cursor.advance()?;
                return Ok(Some(full_row));
            }

            self.cursor.advance()?;
        }
    }

    fn reset(&mut self) -> Result<(), Status> {
        match self.op_code {
            SqlConditionOpCode::SelectFTS => {
                self.cursor.move_to_start()?;
            }
            SqlConditionOpCode::SelectKeyRange => {
                //ToDO use the key_index in Schema.TableSchema
                let (op, ref val) = self.conditions[0];
                match op {
                    Greater | GreaterOrEqual | Equal => {
                        self.cursor.go_to(&val.clone())?;
                    }
                    _ => {
                        self.cursor.move_to_start()?;
                    }
                }
            }
            SqlConditionOpCode::SelectKeyUnique => {
                // For unique, search key again
                let (_, ref val) = self.conditions[0];
                self.cursor.go_to(&val.clone())?;
            }
            _ => {
                self.cursor.move_to_start()?;
            }
        }
        Ok(())
    }
}