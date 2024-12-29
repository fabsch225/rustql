use std::cmp::PartialEq;
use std::fmt::{Debug, Formatter};
use crate::btree::Btree;

#[derive(PartialEq)]
pub enum Status {
    //status codes to be sent to the end user
    Success,
    ExceptionSchemaUnclear,
    ExceptionFileNotFoundOrPermissionDenied,

    //internal status codes
    InternalSuccess,
    InternalExceptionTypeMismatch,
    InternalExceptionIndexOutOfRange,
    InternalExceptionFileNotFound,
    InternalExceptionReadFailed,
    InternalExceptionWriteFailed,
    InternalExceptionInvalidFieldType,
    InternalExceptionInvalidSchema,
    InternalExceptionInvalidFieldName,
    InternalExceptionInvalidFieldValue,
    InternalExceptionKeyNotFound,
    InternalExceptionInvalidRowLength,
    InternalExceptionInvalidColCount,
    InternalExceptionPagerMismatch,
}

impl Debug for Status {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Success => write!(f, "Success"),
            Status::ExceptionSchemaUnclear => write!(f, "ExceptionSchemaUnclear"),
            Status::ExceptionFileNotFoundOrPermissionDenied => write!(f, "ExceptionFileNotFoundOrPermissionDenied"),
            Status::InternalSuccess => write!(f, "InternalSuccess"),
            Status::InternalExceptionTypeMismatch => write!(f, "InternalExceptionTypeMismatch"),
            Status::InternalExceptionIndexOutOfRange => write!(f, "InternalExceptionIndexOutOfRange"),
            Status::InternalExceptionFileNotFound => write!(f, "InternalExceptionFileNotFound"),
            Status::InternalExceptionReadFailed => write!(f, "InternalExceptionReadFailed"),
            Status::InternalExceptionWriteFailed => write!(f, "InternalExceptionWriteFailed"),
            Status::InternalExceptionInvalidFieldType => write!(f, "InternalExceptionInvalidFieldType"),
            Status::InternalExceptionInvalidSchema => write!(f, "InternalExceptionInvalidSchema"),
            Status::InternalExceptionInvalidFieldName => write!(f, "InternalExceptionInvalidFieldName"),
            Status::InternalExceptionInvalidFieldValue => write!(f, "InternalExceptionInvalidFieldValue"),
            Status::InternalExceptionKeyNotFound => write!(f, "InternalExceptionKeyNotFound"),
            Status::InternalExceptionInvalidRowLength => write!(f, "InternalExceptionInvalidRowLength"),
            Status::InternalExceptionInvalidColCount => write!(f, "InternalExceptionInvalidColCount"),
            Status::InternalExceptionPagerMismatch => write!(f, "InternalExceptionPagerMismatch"),
        }.expect("Wierd Error");
        Ok(())
    }
}