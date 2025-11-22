use crate::pager::{Position, TableName, Type};
use crate::serializer::Serializer;
use crate::status::Status;

#[derive(Debug, Clone)]
pub struct Field {
    pub field_type: Type,
    pub identifier: String,
}

#[derive(Clone, Debug)]
pub struct Schema {
    pub table_index: TableIndex,
    pub tables: Vec<TableSchema>,
}

impl Schema {
    pub fn make_empty() -> Self {
        Schema {
            table_index: TableIndex { index: vec![] },
            tables: vec![],
        }
    }
}

#[derive(Clone, Debug)]
pub struct TableIndex {
    pub index: Vec<TableName>,
}

#[derive(Clone, Debug)]
pub struct TableSchema {
    pub next_position: Position,
    pub root: Position, //if 0 -> no tree
    pub has_key: bool,
    pub key_position: usize,
    pub fields: Vec<Field>,
    pub table_type: u8,
    pub entry_count: i32,
}

impl TableSchema {
    pub fn get_col_count(&self) -> Result<usize, Status> {
        Ok(self.fields.len() - 1)
    }

    pub fn get_key_and_row_length(&self) -> Result<usize, Status> {
        let mut len = 0usize;
        for field in &self.fields {
            len += Serializer::get_size_of_type(&field.field_type)?;
        }
        Ok(len)
    }

    pub fn get_key_length(&self) -> Result<usize, Status> {
        if self.fields.is_empty() {
            return Err(Status::InternalExceptionCompilerError);
        }
        if self.key_position >= self.fields.len() {
            return Err(Status::InternalExceptionCompilerError);
        }
        Serializer::get_size_of_type(&self.fields[self.key_position].field_type)
    }

    pub fn get_key_type(&self) -> Result<Type, Status> {
        if self.fields.is_empty() {
            return Err(Status::InternalExceptionCompilerError);
        }
        if self.key_position >= self.fields.len() {
            return Err(Status::InternalExceptionCompilerError);
        }
        Ok(self.fields[self.key_position].field_type.clone())
    }

    pub fn get_row_length(&self) -> Result<usize, Status> {
        if self.fields.is_empty() {
            return Err(Status::InternalExceptionCompilerError);
        }
        let mut len = 0usize;
        for (idx, field) in self.fields.iter().enumerate() {
            if idx == self.key_position {
                continue;
            }
            len += Serializer::get_size_of_type(&field.field_type)?;
        }
        Ok(len)
    }
}
