use std::cmp::PartialEq;

#[derive(PartialEq)]
pub enum Status {
    InternalSuccess,
    InternalExceptionTypeMismatch,
    InternalExceptionIndexOutOfRange
}