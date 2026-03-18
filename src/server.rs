use crate::executor::QueryExecutor;
use crate::pager::PagerAccessor;
use crate::pager::{TransactionId, Type};
use crate::schema::Field;
use crate::serializer::Serializer;
use std::io::{self, ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

const MAGIC: &[u8; 4] = b"RSQL";
const PROTOCOL_VERSION: u8 = 2;
const DEFAULT_FETCH_N: usize = 256;

struct Request {
    sql: String,
    fetch_n: usize,
}

/// Wire protocol (v1):
/// request:
/// - [4] magic: "RSQL"
/// - [1] version: 2
/// - [4] big-endian SQL length
/// - [N] utf8 SQL bytes
/// - [4] big-endian fetch_n (0 => default)
///
/// response:
/// - [4] magic: "RSQL"
/// - [1] status: 0 success, 1 error
/// - [4] message length
/// - [M] utf8 message bytes
/// - [2] column count
/// - repeated columns:
///   - [2] column name length
///   - [name]
///   - [1] type tag
///   - [4] type argument (varchar length or 0)
/// - repeated chunks:
///   - [4] chunk row count
///   - repeated rows:
///     - [4] row length
///     - [row bytes]
///   - [1] done flag (0 => more chunks, 1 => done)
pub fn serve_tcp(
    bind_addr: &str,
    db_path: &str,
    btree_node_width: usize,
) -> io::Result<()> {
    let shared_pager = {
        let bootstrap = QueryExecutor::init(db_path, btree_node_width);
        bootstrap.pager_accessor.clone()
    };
    let shared_pager = Arc::new(shared_pager);

    let listener = TcpListener::bind(bind_addr)?;
    println!("RustQL TCP server listening on {bind_addr}");

    for stream in listener.incoming() {
        match stream {
            Ok(tcp_stream) => {
                let shared_pager = shared_pager.clone();

                thread::spawn(move || {
                    let executor = QueryExecutor::from_pager_accessor(
                        (*shared_pager).clone(),
                        btree_node_width,
                    );
                    if let Err(e) = handle_client(tcp_stream, executor) {
                        eprintln!("client connection ended: {e}");
                    }
                });
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }

    Ok(())
}

fn handle_client(
    mut stream: TcpStream,
    mut executor: QueryExecutor,
) -> io::Result<()> {
    /*
     * Connection-local transaction context.
     *
     * We keep one optional transaction id per TCP client connection.
     * - None: no explicit transaction is active for this client.
     * - Some(tx): this client has an explicit BEGIN ... COMMIT/ROLLBACK session.
     *
     * This allows independent clients to run concurrently while preserving
     * transaction ownership across multiple requests from the same socket.
     */
    let mut active_tx_id: Option<TransactionId> = None;

    loop {
        let request = match read_request(&mut stream) {
            Ok(request) => request,
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                rollback_open_transaction(&mut executor, active_tx_id);
                return Ok(());
            }
            Err(e) => return Err(e),
        };

        let query = request.sql;
        /*
         * Single request = single SQL statement.
         * We classify transaction control statements early because they use
         * different lifecycle rules than regular SQL.
         */
        let tx_control = parse_transaction_control(&query);

        if let Err(err_result) = executor.reload_schema() {
            let message = decode_message_from_dataframe(&err_result.data)
                .unwrap_or_else(|| "schema reload failed".to_string());
            return Err(io::Error::other(message));
        }

        /*
         * Execution model:
         *
         * 1) Explicit transaction lifecycle (BEGIN / COMMIT / ROLLBACK)
         *    is bound to this connection via `active_tx_id`.
         *
         * 2) If no explicit transaction is active and we receive a normal
         *    statement, we run it inside an implicit transaction:
         *      begin -> execute -> commit (or rollback on failure)
         *
         * 3) If an explicit transaction is active, normal statements execute
         *    inside that transaction and are not auto-committed.
         */
        let mut result = if active_tx_id.is_none() && tx_control == TransactionControl::Begin {
            if let Err(status) = executor.pager_accessor.set_current_transaction(None) {
                crate::executor::QueryResult::err(status)
            } else {
                let begin_result = executor.prepare(query);
                if begin_result.success {
                    active_tx_id = executor.pager_accessor.current_transaction_id();
                }
                let _ = executor.pager_accessor.set_current_transaction(None);
                begin_result
            }
        } else if active_tx_id.is_some()
            && (tx_control == TransactionControl::Commit || tx_control == TransactionControl::Rollback)
        {
            let end_result = executor.prepare_in_transaction_context(query, active_tx_id);
            if end_result.success {
                active_tx_id = None;
            }
            end_result
        } else if active_tx_id.is_some() && tx_control == TransactionControl::Begin {
            crate::executor::QueryResult::err(crate::debug::Status::ExceptionTransactionAlreadyActive)
        } else if active_tx_id.is_none()
            && (tx_control == TransactionControl::Commit || tx_control == TransactionControl::Rollback)
        {
            crate::executor::QueryResult::err(crate::debug::Status::ExceptionNoActiveTransaction)
        } else if active_tx_id.is_some() {
            executor.prepare_in_transaction_context(query, active_tx_id)
        } else {
            executor.prepare_in_implicit_transaction(query)
        };

        let success = result.success;
        let mut data = result.data;
        let message = if success {
            "OK".to_string()
        } else {
            decode_message_from_dataframe(&data).unwrap_or_else(|| "query failed".to_string())
        };
        let status = if success { 0u8 } else { 1u8 };

        let fetch_n = if request.fetch_n == 0 {
            DEFAULT_FETCH_N
        } else {
            request.fetch_n
        };
        write_response(&mut stream, status, &message, &mut data, fetch_n)?;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransactionControl {
    Begin,
    Commit,
    Rollback,
    Other,
}

fn parse_transaction_control(sql: &str) -> TransactionControl {
    let token = sql
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_uppercase();

    match token.as_str() {
        "BEGIN" => TransactionControl::Begin,
        "COMMIT" => TransactionControl::Commit,
        "ROLLBACK" => TransactionControl::Rollback,
        _ => TransactionControl::Other,
    }
}

fn rollback_open_transaction(executor: &mut QueryExecutor, tx_id: Option<TransactionId>) {
    /*
     * Safety net on disconnect:
     * if a client drops while holding an explicit transaction, we rollback
     * it so table locks and uncommitted state are not leaked.
     */
    let Some(id) = tx_id else {
        return;
    };

    let _ = executor.pager_accessor.rollback_transaction_by_id(id);
    let _ = executor.pager_accessor.set_current_transaction(None);
}

fn decode_message_from_dataframe(data: &crate::dataframe::DataFrame) -> Option<String> {
    let columns = data.header.clone();
    let rows = data.clone().fetch().ok()?;

    if columns.is_empty() || rows.is_empty() {
        return None;
    }

    let first_col = &columns[0];
    let first_row = &rows[0];
    let len = Serializer::get_size_of_type(&first_col.field_type).ok()?;
    if first_row.len() < len {
        return None;
    }

    let cell = first_row[0..len].to_vec();
    Serializer::format_field(&cell, &first_col.field_type).ok()
}

fn read_request(stream: &mut impl Read) -> io::Result<Request> {
    let mut magic = [0u8; 4];
    stream.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "invalid protocol magic",
        ));
    }

    let mut version = [0u8; 1];
    stream.read_exact(&mut version)?;
    if version[0] != PROTOCOL_VERSION {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "unsupported protocol version",
        ));
    }

    let sql_len = read_u32(stream)? as usize;
    let mut sql_bytes = vec![0u8; sql_len];
    stream.read_exact(&mut sql_bytes)?;

    let fetch_n = read_u32(stream)? as usize;

    let sql = String::from_utf8(sql_bytes)
        .map_err(|_| io::Error::new(ErrorKind::InvalidData, "SQL must be utf-8"))?;

    Ok(Request { sql, fetch_n })
}

fn write_response(
    stream: &mut impl Write,
    status: u8,
    message: &str,
    data: &mut crate::dataframe::DataFrame,
    fetch_n: usize,
) -> io::Result<()> {
    let columns = data.header.clone();

    stream.write_all(MAGIC)?;
    stream.write_all(&[status])?;

    write_u32(stream, message.len() as u32)?;
    stream.write_all(message.as_bytes())?;

    write_u16(stream, columns.len() as u16)?;
    for field in columns {
        let name = field.name.as_bytes();
        write_u16(stream, name.len() as u16)?;
        stream.write_all(name)?;

        let (tag, arg) = map_type(&field.field_type);
        stream.write_all(&[tag])?;
        write_u32(stream, arg)?;
    }

    loop {
        let rows = data
            .fetch_n(fetch_n)
            .map_err(|_| io::Error::other("failed to fetch dataframe rows"))?;
        let is_done = rows.is_empty();

        write_u32(stream, rows.len() as u32)?;
        for row in &rows {
            write_u32(stream, row.len() as u32)?;
            stream.write_all(row)?;
        }

        if is_done {
            stream.write_all(&[1])?;
            break;
        } else {
            stream.write_all(&[0])?;
        }
    }

    stream.flush()
}

fn map_type(ty: &Type) -> (u8, u32) {
    match ty {
        Type::Null => (0, 0),
        Type::Integer => (1, 0),
        Type::String => (2, 0),
        Type::Varchar(max) => (3, *max as u32),
        Type::Date => (4, 0),
        Type::Boolean => (5, 0),
    }
}

fn read_u32(stream: &mut impl Read) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf)?;
    Ok(u32::from_be_bytes(buf))
}

fn write_u16(stream: &mut impl Write, value: u16) -> io::Result<()> {
    stream.write_all(&value.to_be_bytes())
}

fn write_u32(stream: &mut impl Write, value: u32) -> io::Result<()> {
    stream.write_all(&value.to_be_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataframe::DataFrame;
    use crate::debug::Status;
    use crate::pager::Type;
    use crate::schema::Field;
    use crate::serializer::Serializer;
    use std::fs;
    use std::io::Cursor;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn parse_response_bytes(bytes: &[u8]) -> (u8, String, Vec<(String, u8, u32)>, Vec<usize>, Vec<u8>) {
        let mut cur = Cursor::new(bytes);

        let mut magic = [0u8; 4];
        cur.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, MAGIC);

        let mut status = [0u8; 1];
        cur.read_exact(&mut status).unwrap();

        let mut msg_len_buf = [0u8; 4];
        cur.read_exact(&mut msg_len_buf).unwrap();
        let msg_len = u32::from_be_bytes(msg_len_buf) as usize;
        let mut msg_bytes = vec![0u8; msg_len];
        cur.read_exact(&mut msg_bytes).unwrap();
        let message = String::from_utf8(msg_bytes).unwrap();

        let mut col_count_buf = [0u8; 2];
        cur.read_exact(&mut col_count_buf).unwrap();
        let col_count = u16::from_be_bytes(col_count_buf) as usize;

        let mut cols = Vec::new();
        for _ in 0..col_count {
            let mut name_len_buf = [0u8; 2];
            cur.read_exact(&mut name_len_buf).unwrap();
            let name_len = u16::from_be_bytes(name_len_buf) as usize;
            let mut name_bytes = vec![0u8; name_len];
            cur.read_exact(&mut name_bytes).unwrap();
            let name = String::from_utf8(name_bytes).unwrap();

            let mut tag_buf = [0u8; 1];
            cur.read_exact(&mut tag_buf).unwrap();
            let tag = tag_buf[0];

            let mut arg_buf = [0u8; 4];
            cur.read_exact(&mut arg_buf).unwrap();
            let arg = u32::from_be_bytes(arg_buf);

            cols.push((name, tag, arg));
        }

        let mut chunk_sizes = Vec::new();
        let mut done_flags = Vec::new();
        loop {
            let mut chunk_count_buf = [0u8; 4];
            cur.read_exact(&mut chunk_count_buf).unwrap();
            let chunk_count = u32::from_be_bytes(chunk_count_buf) as usize;
            chunk_sizes.push(chunk_count);

            for _ in 0..chunk_count {
                let mut row_len_buf = [0u8; 4];
                cur.read_exact(&mut row_len_buf).unwrap();
                let row_len = u32::from_be_bytes(row_len_buf) as usize;
                let mut row = vec![0u8; row_len];
                cur.read_exact(&mut row).unwrap();
            }

            let mut done_buf = [0u8; 1];
            cur.read_exact(&mut done_buf).unwrap();
            done_flags.push(done_buf[0]);
            if done_buf[0] == 1 {
                break;
            }
        }

        (status[0], message, cols, chunk_sizes, done_flags)
    }

    fn unique_db_path(prefix: &str) -> String {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("/tmp/{}.{}.{}.bin", prefix, std::process::id(), ts)
    }

    #[test]
    fn test_parse_tx_control_begin() {
        assert_eq!(parse_transaction_control("BEGIN TRANSACTION"), TransactionControl::Begin);
    }

    #[test]
    fn test_parse_tx_control_commit_lowercase() {
        assert_eq!(parse_transaction_control("commit"), TransactionControl::Commit);
    }

    #[test]
    fn test_parse_tx_control_rollback_mixed_case() {
        assert_eq!(parse_transaction_control("RoLlBaCk"), TransactionControl::Rollback);
    }

    #[test]
    fn test_parse_tx_control_other_select() {
        assert_eq!(parse_transaction_control("SELECT 1"), TransactionControl::Other);
    }

    #[test]
    fn test_parse_tx_control_empty() {
        assert_eq!(parse_transaction_control(""), TransactionControl::Other);
    }

    #[test]
    fn test_parse_tx_control_whitespace_only() {
        assert_eq!(parse_transaction_control("   \n\t  "), TransactionControl::Other);
    }

    #[test]
    fn test_parse_tx_control_begin_with_leading_spaces() {
        assert_eq!(parse_transaction_control("   BEGIN"), TransactionControl::Begin);
    }

    #[test]
    fn test_parse_tx_control_unknown_keyword() {
        assert_eq!(parse_transaction_control("UPSERT x"), TransactionControl::Other);
    }

    #[test]
    fn test_map_type_null() {
        assert_eq!(map_type(&Type::Null), (0, 0));
    }

    #[test]
    fn test_map_type_integer() {
        assert_eq!(map_type(&Type::Integer), (1, 0));
    }

    #[test]
    fn test_map_type_string() {
        assert_eq!(map_type(&Type::String), (2, 0));
    }

    #[test]
    fn test_map_type_varchar() {
        assert_eq!(map_type(&Type::Varchar(42)), (3, 42));
    }

    #[test]
    fn test_map_type_date() {
        assert_eq!(map_type(&Type::Date), (4, 0));
    }

    #[test]
    fn test_map_type_boolean() {
        assert_eq!(map_type(&Type::Boolean), (5, 0));
    }

    #[test]
    fn test_write_u16_big_endian() {
        let mut out = Vec::new();
        write_u16(&mut out, 0xABCD).unwrap();
        assert_eq!(out, vec![0xAB, 0xCD]);
    }

    #[test]
    fn test_write_u32_and_read_u32_roundtrip() {
        let mut out = Vec::new();
        write_u32(&mut out, 0xDEADBEEF).unwrap();
        let mut cur = Cursor::new(out);
        let read = read_u32(&mut cur).unwrap();
        assert_eq!(read, 0xDEADBEEF);
    }

    #[test]
    fn test_read_u32_eof_error() {
        let mut cur = Cursor::new(vec![0x00, 0x01, 0x02]);
        assert!(read_u32(&mut cur).is_err());
    }

    #[test]
    fn test_read_request_valid() {
        let sql = "SELECT * FROM x".as_bytes().to_vec();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(PROTOCOL_VERSION);
        bytes.extend_from_slice(&(sql.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&sql);
        bytes.extend_from_slice(&(7u32).to_be_bytes());

        let mut cur = Cursor::new(bytes);
        let req = read_request(&mut cur).unwrap();
        assert_eq!(req.sql, "SELECT * FROM x");
        assert_eq!(req.fetch_n, 7);
    }

    #[test]
    fn test_read_request_invalid_magic() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"NOPE");
        bytes.push(PROTOCOL_VERSION);
        bytes.extend_from_slice(&(0u32).to_be_bytes());
        bytes.extend_from_slice(&(0u32).to_be_bytes());
        let mut cur = Cursor::new(bytes);
        assert!(read_request(&mut cur).is_err());
    }

    #[test]
    fn test_read_request_invalid_version() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(99u8);
        bytes.extend_from_slice(&(0u32).to_be_bytes());
        bytes.extend_from_slice(&(0u32).to_be_bytes());
        let mut cur = Cursor::new(bytes);
        assert!(read_request(&mut cur).is_err());
    }

    #[test]
    fn test_read_request_invalid_utf8() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(PROTOCOL_VERSION);
        bytes.extend_from_slice(&(2u32).to_be_bytes());
        bytes.extend_from_slice(&[0xFF, 0xFF]);
        bytes.extend_from_slice(&(0u32).to_be_bytes());
        let mut cur = Cursor::new(bytes);
        assert!(read_request(&mut cur).is_err());
    }

    #[test]
    fn test_read_request_truncated_sql() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(PROTOCOL_VERSION);
        bytes.extend_from_slice(&(10u32).to_be_bytes());
        bytes.extend_from_slice(b"abc");
        bytes.extend_from_slice(&(0u32).to_be_bytes());
        let mut cur = Cursor::new(bytes);
        assert!(read_request(&mut cur).is_err());
    }

    #[test]
    fn test_decode_message_from_dataframe_msg() {
        let df = DataFrame::msg("hello");
        let decoded = decode_message_from_dataframe(&df).unwrap();
        assert_eq!(decoded, "hello");
    }

    #[test]
    fn test_decode_message_from_dataframe_empty_rows_and_cols() {
        let df = DataFrame::from_memory("x".to_string(), vec![], vec![]);
        assert!(decode_message_from_dataframe(&df).is_none());
    }

    #[test]
    fn test_decode_message_from_dataframe_row_too_short() {
        let header = vec![Field {
            field_type: Type::String,
            name: "Message".to_string(),
            table_name: "".to_string(),
        }];
        let df = DataFrame::from_memory("x".to_string(), header, vec![vec![1u8]]);
        assert!(decode_message_from_dataframe(&df).is_none());
    }

    #[test]
    fn test_write_response_basic_shape() {
        let mut df = DataFrame::msg("ok");
        let mut out = Vec::new();
        write_response(&mut out, 0, "OK", &mut df, 10).unwrap();

        let (status, message, cols, chunk_sizes, done_flags) = parse_response_bytes(&out);
        assert_eq!(status, 0);
        assert_eq!(message, "OK");
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].0, "Message");
        assert_eq!(chunk_sizes, vec![1, 0]);
        assert_eq!(done_flags, vec![0, 1]);
    }

    #[test]
    fn test_write_response_chunking_fetch_1() {
        let header = vec![Field {
            field_type: Type::Integer,
            name: "id".to_string(),
            table_name: "t".to_string(),
        }];
        let row1 = vec![0, 0, 0, 0, 1];
        let row2 = vec![0, 0, 0, 0, 2];
        let mut df = DataFrame::from_memory("t".to_string(), header, vec![row1, row2]);

        let mut out = Vec::new();
        write_response(&mut out, 0, "OK", &mut df, 1).unwrap();
        let (_, _, _, chunk_sizes, done_flags) = parse_response_bytes(&out);
        assert_eq!(chunk_sizes, vec![1, 1, 0]);
        assert_eq!(done_flags, vec![0, 0, 1]);
    }

    #[test]
    fn test_write_response_empty_dataframe_done_immediately() {
        let header = vec![Field {
            field_type: Type::Integer,
            name: "id".to_string(),
            table_name: "t".to_string(),
        }];
        let mut df = DataFrame::from_memory("t".to_string(), header, vec![]);

        let mut out = Vec::new();
        write_response(&mut out, 1, "ERR", &mut df, 5).unwrap();
        let (status, message, _cols, chunk_sizes, done_flags) = parse_response_bytes(&out);
        assert_eq!(status, 1);
        assert_eq!(message, "ERR");
        assert_eq!(chunk_sizes, vec![0]);
        assert_eq!(done_flags, vec![1]);
    }

    #[test]
    fn test_write_response_with_varchar_column_metadata() {
        let header = vec![Field {
            field_type: Type::Varchar(25),
            name: "name".to_string(),
            table_name: "t".to_string(),
        }];
        let row = Serializer::parse_varchar("abc", 25).to_vec();
        let mut df = DataFrame::from_memory("t".to_string(), header, vec![row]);

        let mut out = Vec::new();
        write_response(&mut out, 0, "OK", &mut df, 10).unwrap();
        let (_status, _message, cols, _chunk_sizes, _done_flags) = parse_response_bytes(&out);
        assert_eq!(cols[0].1, 3);
        assert_eq!(cols[0].2, 25);
    }

    #[test]
    fn test_rollback_open_transaction_no_tx_is_noop() {
        let db_path = unique_db_path("rustql_server_test_noop");
        let mut executor = QueryExecutor::init(&db_path, 3);
        rollback_open_transaction(&mut executor, None);
        assert!(executor.pager_accessor.current_transaction_id().is_none());
        let _ = fs::remove_file(db_path);
    }

    #[test]
    fn test_rollback_open_transaction_clears_tx() {
        let db_path = unique_db_path("rustql_server_test_rb");
        let mut executor = QueryExecutor::init(&db_path, 3);

        let tx_id = executor.pager_accessor.begin_transaction_with_id().unwrap();
        executor
            .pager_accessor
            .set_current_transaction(Some(tx_id))
            .unwrap();

        rollback_open_transaction(&mut executor, Some(tx_id));

        assert!(executor.pager_accessor.current_transaction_id().is_none());
        let should_fail = executor.pager_accessor.set_current_transaction(Some(tx_id));
        assert!(matches!(should_fail, Err(Status::ExceptionNoActiveTransaction)));

        let _ = fs::remove_file(db_path);
    }
}
