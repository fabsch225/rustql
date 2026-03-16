use crate::executor::QueryExecutor;
use crate::pager::Type;
use crate::schema::Field;
use crate::serializer::Serializer;
use std::io::{self, ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
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
    let engine = Arc::new(RwLock::new(QueryExecutor::init(db_path, btree_node_width)));

    let listener = TcpListener::bind(bind_addr)?;
    println!("RustQL TCP server listening on {bind_addr}");

    for stream in listener.incoming() {
        match stream {
            Ok(tcp_stream) => {
                let engine = engine.clone();

                thread::spawn(move || {
                    if let Err(e) = handle_client(tcp_stream, engine) {
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
    engine: Arc<RwLock<QueryExecutor>>,
) -> io::Result<()> {
    loop {
        let request = match read_request(&mut stream) {
            Ok(request) => request,
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        };

        let query = request.sql;
        let readonly = {
            let guard = engine
                .read()
                .map_err(|_| io::Error::other("engine read lock poisoned"))?;
            match guard.planner_feedback_is_readonly(&query) {
                Ok(v) => v,
                Err(result) => {
                    let success = result.success;
                    let mut data = result.data;
                    let message = if success {
                        "OK".to_string()
                    } else {
                        decode_message_from_dataframe(&data)
                            .unwrap_or_else(|| "query failed".to_string())
                    };
                    let fetch_n = if request.fetch_n == 0 {
                        DEFAULT_FETCH_N
                    } else {
                        request.fetch_n
                    };
                    write_response(
                        &mut stream,
                        if success { 0u8 } else { 1u8 },
                        &message,
                        &mut data,
                        fetch_n,
                    )?;
                    continue;
                }
            }
        };

        let (status, message, mut data) = if readonly {
            let guard = engine
                .read()
                .map_err(|_| io::Error::other("engine read lock poisoned"))?;

            let result = guard.execute_readonly(query);
            let success = result.success;
            let data = result.data;
            let message = if success {
                "OK".to_string()
            } else {
                decode_message_from_dataframe(&data).unwrap_or_else(|| "query failed".to_string())
            };
            (if success { 0u8 } else { 1u8 }, message, data)
        } else {
            let mut guard = engine
                .write()
                .map_err(|_| io::Error::other("engine write lock poisoned"))?;

            let result = guard.prepare(query);
            let success = result.success;
            let data = result.data;
            let message = if success {
                "OK".to_string()
            } else {
                decode_message_from_dataframe(&data).unwrap_or_else(|| "query failed".to_string())
            };
            (if success { 0u8 } else { 1u8 }, message, data)
        };

        let fetch_n = if request.fetch_n == 0 {
            DEFAULT_FETCH_N
        } else {
            request.fetch_n
        };
        write_response(&mut stream, status, &message, &mut data, fetch_n)?;
    }
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
