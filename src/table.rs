use crate::pager::Type;
use crate::status::Status;

pub struct Table {
    pub id_size: usize,
}

pub struct Schema {

}

pub struct TableSchema {
    row_length: usize,
    row_types: Vec<Type>
}

impl TableSchema {
    pub fn get_id_type(&self) -> (Status, Option<&Type>) {
        (Status::InternalSuccess, self.row_types.get(0))
    }
}