use crate::btree::Btree;
use std::cmp::PartialEq;
use std::fmt::{Debug, Formatter};

#[derive(PartialEq, Debug)]
pub enum Status {
    //status codes to be sent to the end user
    Error,
    Success,
    ExceptionSchemaUnclear,
    ExceptionFileNotFoundOrPermissionDenied,
    ExceptionQueryMisformed,

    //internal status codes
    CacheMiss,
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
    InternalExceptionCacheDenied,
    InternalExceptionPageCorrupted,
    CannotParseDate,
    CannotParseInteger,
    CannotParseBoolean,
    CannotParseIllegalDate,
    InternalExceptionPagerWriteLock,
    InternalExceptionCompilerError,
    InternalExceptionIntegrityCheckFailed,
    InternalExceptionFileWriteError,
    InternalExceptionFileAlreadyExists,
    InternalExceptionFileOpenFailed,
    ExceptionTableAlreadyExists,
}
