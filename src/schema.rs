use crate::pager::{Position, TableName, Type};
use crate::parser::JoinOp;
use crate::serializer::Serializer;
use crate::status::Status;

#[derive(Debug, Clone)]
pub struct Field {
    pub field_type: Type,
    pub name: String,
    pub table_name: String,
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
    pub name: String,
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

    pub fn join(
        &self,
        other: &TableSchema,
        left_key: &Field,
        right_key: &Field,
    ) -> Result<TableSchema, Status> {
        self.get_join_positions_and_validate(other, left_key, right_key)?;

        let mut merged_fields = Vec::new();

        for f in &self.fields {
            merged_fields.push(Field {
                field_type: f.field_type.clone(),
                name: f.name.clone(),
                table_name: f.table_name.clone()
            });
        }

        for f in &other.fields {
            merged_fields.push(Field {
                field_type: f.field_type.clone(),
                name: f.name.clone(),
                table_name: f.table_name.clone()
            });
        }

        let new_table = TableSchema {
            next_position: Position::make_empty(),
            root: Position::make_empty(),
            has_key: false,
            key_position: 0,
            fields: merged_fields,
            table_type: 0,
            entry_count: self.entry_count,
            name: format!("{}_JOIN_{}", self.name.clone(), other.name.clone()),
        };

        Ok(new_table)
    }

    pub fn get_join_positions_and_validate(
        &self,
        other: &TableSchema,
        left_key: &Field,
        right_key: &Field,
    ) -> Result<(usize, usize), Status> {
        let (left_pos, left_field) = self.get_column_and_field(left_key).ok_or(
            Status::Error
        )?;

        let (right_pos, right_field) = other.get_column_and_field(right_key).ok_or(
            Status::Error
        )?;

        if left_field.field_type != right_field.field_type {
            return Err(Status::Error);
        }

        Ok((left_pos, right_pos))
    }

    pub fn get_join_ops(
        &self,
        other: &TableSchema,
        left_key: &Field,
        right_key: &Field,
    ) -> Result<(JoinOp, JoinOp), Status> {
        println!("{:?} - {:?} - {:?} - {:?}", self, other, left_key, right_key);
        let (left_pos, right_pos) =
            self.get_join_positions_and_validate(other, left_key, right_key)?;
        let left_op = if self.has_key && self.key_position == left_pos {
            JoinOp::Key
        } else {
            JoinOp::Scan
        };

        let right_op = if other.has_key && other.key_position == right_pos {
            JoinOp::Key
        } else {
            JoinOp::Scan
        };

        Ok((left_op, right_op))
    }

    pub fn project(&self, fields: &Vec<Field>) -> Self {
        let mut projected_fields = Vec::new();

        for fld in fields {
            let req_name = fld.name.as_str();
            let mut matched = None;

            for f in &self.fields {
                let stored_name = match f.name.split_once('.') {
                    Some((_prefix, name)) => name.to_string(),
                    None => f.name.clone(),
                };

                if stored_name == req_name {
                    matched = Some(Field {
                        field_type: f.field_type.clone(),
                        name: f.name.clone(),
                        table_name: f.table_name.clone()
                    });
                    break;
                }
            }

            if let Some(field) = matched {
                projected_fields.push(field);
            }
        }

        TableSchema {
            next_position: self.next_position.clone(),
            root: self.root.clone(),
            has_key: false, //ToDo
            key_position: 0,
            fields: projected_fields,
            table_type: self.table_type,
            entry_count: self.entry_count,
            name: self.name.clone(),
        }
    }

    pub fn get_column_and_field(&self, key: &Field) -> Option<(usize, Field)> {
        let mut matches = self
            .fields
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                field.name == key.name && (key.table_name.is_empty() || field.table_name == key.table_name)
            });

        let first = matches.next()?;

        if matches.next().is_some() {
            return None;
        }

        Some((first.0, first.1.clone()))
    }

}
