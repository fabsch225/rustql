use std::cmp::PartialEq;
use std::fmt::{Debug, Formatter};
use crate::btree::Btree;

#[derive(PartialEq, Debug)]
pub enum Status {
    //status codes to be sent to the end user
    Error,
    Success,
    ExceptionSchemaUnclear,
    ExceptionFileNotFoundOrPermissionDenied,
    ExceptionQueryMisformed,

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
    InternalExceptionNoRoot,
}