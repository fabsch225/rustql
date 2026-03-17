#[cfg(test)]
mod tests {
    use rustql::server::serve_tcp;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Mutex, OnceLock};
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    const MAGIC: &[u8; 4] = b"RSQL";
    const VERSION: u8 = 2;

    static NAME_COUNTER: AtomicUsize = AtomicUsize::new(0);
    static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    static SERVER_ADDR: OnceLock<String> = OnceLock::new();

    #[derive(Debug)]
    struct Response {
        status: u8,
        message: String,
        columns: usize,
        rows: Vec<Vec<u8>>,
        chunk_counts: Vec<u32>,
        done_flags: Vec<u8>,
    }

    struct Client {
        stream: TcpStream,
    }

    impl Client {
        fn connect() -> Self {
            let addr = server_addr();
            let stream = TcpStream::connect(&addr).expect("failed to connect to test server");
            Self { stream }
        }

        fn send(&mut self, sql: &str, fetch_n: u32) -> Response {
            let sql_bytes = sql.as_bytes();
            self.stream.write_all(MAGIC).unwrap();
            self.stream.write_all(&[VERSION]).unwrap();
            self.stream
                .write_all(&(sql_bytes.len() as u32).to_be_bytes())
                .unwrap();
            self.stream.write_all(sql_bytes).unwrap();
            self.stream.write_all(&fetch_n.to_be_bytes()).unwrap();
            self.stream.flush().unwrap();

            read_response(&mut self.stream)
        }
    }

    fn test_lock() -> &'static Mutex<()> {
        TEST_LOCK.get_or_init(|| Mutex::new(()))
    }

    fn unique_name(prefix: &str) -> String {
        let idx = NAME_COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{}_{}_{}", prefix, std::process::id(), idx)
    }

    fn server_addr() -> String {
        SERVER_ADDR
            .get_or_init(|| {
                let port = 16544u16;
                let addr = format!("127.0.0.1:{}", port);
                let db_path = format!(
                    "/tmp/rustql.server.integration.{}.db",
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                );

                let bind_addr = addr.clone();
                thread::spawn(move || {
                    let _ = serve_tcp(&bind_addr, &db_path, 7);
                });

                for _ in 0..80 {
                    if TcpStream::connect(&addr).is_ok() {
                        return addr;
                    }
                    thread::sleep(Duration::from_millis(50));
                }

                panic!("server did not start in time");
            })
            .clone()
    }

    fn read_response(stream: &mut TcpStream) -> Response {
        let mut magic = [0u8; 4];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, MAGIC);

        let mut status_buf = [0u8; 1];
        stream.read_exact(&mut status_buf).unwrap();
        let status = status_buf[0];

        let message = read_len_prefixed_string(stream);

        let mut col_count_buf = [0u8; 2];
        stream.read_exact(&mut col_count_buf).unwrap();
        let columns = u16::from_be_bytes(col_count_buf) as usize;

        for _ in 0..columns {
            let mut name_len_buf = [0u8; 2];
            stream.read_exact(&mut name_len_buf).unwrap();
            let name_len = u16::from_be_bytes(name_len_buf) as usize;
            let mut name = vec![0u8; name_len];
            stream.read_exact(&mut name).unwrap();

            let mut tag = [0u8; 1];
            stream.read_exact(&mut tag).unwrap();
            let mut arg = [0u8; 4];
            stream.read_exact(&mut arg).unwrap();
        }

        let mut rows = Vec::new();
        let mut chunk_counts = Vec::new();
        let mut done_flags = Vec::new();

        loop {
            let chunk_count = read_u32(stream);
            chunk_counts.push(chunk_count);
            for _ in 0..chunk_count {
                let row_len = read_u32(stream) as usize;
                let mut row = vec![0u8; row_len];
                stream.read_exact(&mut row).unwrap();
                rows.push(row);
            }
            let mut done = [0u8; 1];
            stream.read_exact(&mut done).unwrap();
            done_flags.push(done[0]);
            if done[0] == 1 {
                break;
            }
        }

        Response {
            status,
            message,
            columns,
            rows,
            chunk_counts,
            done_flags,
        }
    }

    fn read_u32(stream: &mut TcpStream) -> u32 {
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).unwrap();
        u32::from_be_bytes(buf)
    }

    fn read_len_prefixed_string(stream: &mut TcpStream) -> String {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).unwrap();
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut bytes = vec![0u8; len];
        stream.read_exact(&mut bytes).unwrap();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn integration_01_create_table_success() {
        let _g = test_lock().lock().unwrap();
        let t = unique_name("t_create_ok");
        let mut c = Client::connect();
        let r = c.send(&format!("CREATE TABLE {} (id Integer, name Varchar(25))", t), 256);
        assert_eq!(r.status, 0);
    }

    #[test]
    fn integration_02_create_table_duplicate_fails() {
        let _g = test_lock().lock().unwrap();
        let t = unique_name("t_create_dup");
        let mut c = Client::connect();
        assert_eq!(c.send(&format!("CREATE TABLE {} (id Integer)", t), 256).status, 0);
        assert_eq!(c.send(&format!("CREATE TABLE {} (id Integer)", t), 256).status, 1);
    }

    #[test]
    fn integration_03_insert_success() {
        let _g = test_lock().lock().unwrap();
        let t = unique_name("t_ins_ok");
        let mut c = Client::connect();
        c.send(&format!("CREATE TABLE {} (id Integer, name Varchar(25))", t), 256);
        let r = c.send(&format!("INSERT INTO {} (id, name) VALUES (1, 'a')", t), 256);
        assert_eq!(r.status, 0);
    }

    #[test]
    fn integration_04_select_returns_one_row() {
        let _g = test_lock().lock().unwrap();
        let t = unique_name("t_sel_one");
        let mut c = Client::connect();
        c.send(&format!("CREATE TABLE {} (id Integer, name Varchar(25))", t), 256);
        c.send(&format!("INSERT INTO {} (id, name) VALUES (1, 'a')", t), 256);
        let r = c.send(&format!("SELECT * FROM {}", t), 256);
        assert_eq!(r.status, 0);
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.columns, 2);
    }

    #[test]
    fn integration_05_select_where_filters() {
        let _g = test_lock().lock().unwrap();
        let t = unique_name("t_sel_where");
        let mut c = Client::connect();
        c.send(&format!("CREATE TABLE {} (id Integer, name Varchar(25))", t), 256);
        c.send(&format!("INSERT INTO {} VALUES (1, 'a')", t), 256);
        c.send(&format!("INSERT INTO {} VALUES (2, 'b')", t), 256);
        let r = c.send(&format!("SELECT * FROM {} WHERE id = 2", t), 256);
        assert_eq!(r.status, 0);
        assert_eq!(r.rows.len(), 1);
    }

    #[test]
    fn integration_06_update_success() {
        let _g = test_lock().lock().unwrap();
        let t = unique_name("t_update");
        let mut c = Client::connect();
        c.send(&format!("CREATE TABLE {} (id Integer, name Varchar(25))", t), 256);
        c.send(&format!("INSERT INTO {} VALUES (1, 'a')", t), 256);
        let r = c.send(&format!("UPDATE {} SET name = 'x' WHERE id = 1", t), 256);
        assert_eq!(r.status, 0);
    }

    #[test]
    fn integration_07_delete_success() {
        let _g = test_lock().lock().unwrap();
        let t = unique_name("t_delete");
        let mut c = Client::connect();
        c.send(&format!("CREATE TABLE {} (id Integer, name Varchar(25))", t), 256);
        c.send(&format!("INSERT INTO {} VALUES (1, 'a')", t), 256);
        let r = c.send(&format!("DELETE FROM {} WHERE id = 1", t), 256);
        assert_eq!(r.status, 0);
    }

    #[test]
    fn integration_08_invalid_sql_fails() {
        let _g = test_lock().lock().unwrap();
        let mut c = Client::connect();
        let r = c.send("NOT_A_VALID_SQL", 256);
        assert_eq!(r.status, 1);
    }

    #[test]
    fn integration_09_commit_without_begin_fails() {
        let _g = test_lock().lock().unwrap();
        let mut c = Client::connect();
        let r = c.send("COMMIT", 256);
        assert_eq!(r.status, 1);
        assert!(r.message.contains("ExceptionNoActiveTransaction"));
    }

    #[test]
    fn integration_10_rollback_without_begin_fails() {
        let _g = test_lock().lock().unwrap();
        let mut c = Client::connect();
        let r = c.send("ROLLBACK", 256);
        assert_eq!(r.status, 1);
        assert!(r.message.contains("ExceptionNoActiveTransaction"));
    }

    #[test]
    fn integration_11_begin_success() {
        let _g = test_lock().lock().unwrap();
        let mut c = Client::connect();
        let r = c.send("BEGIN TRANSACTION", 256);
        assert_eq!(r.status, 0);
        assert_eq!(c.send("ROLLBACK", 256).status, 0);
    }

    #[test]
    fn integration_12_begin_twice_same_connection_fails() {
        let _g = test_lock().lock().unwrap();
        let mut c = Client::connect();
        assert_eq!(c.send("BEGIN TRANSACTION", 256).status, 0);
        let r2 = c.send("BEGIN TRANSACTION", 256);
        assert_eq!(r2.status, 1);
        assert_eq!(c.send("ROLLBACK", 256).status, 0);
    }

    #[test]
    fn integration_13_begin_insert_commit_persists() {
        let _g = test_lock().lock().unwrap();
        let t = unique_name("t_tx_commit");
        let mut c = Client::connect();
        c.send(&format!("CREATE TABLE {} (id Integer, name Varchar(25))", t), 256);
        c.send("BEGIN TRANSACTION", 256);
        c.send(&format!("INSERT INTO {} VALUES (1, 'a')", t), 256);
        c.send("COMMIT", 256);
        let r = c.send(&format!("SELECT * FROM {}", t), 256);
        assert_eq!(r.status, 0);
        assert_eq!(r.rows.len(), 1);
    }

    #[test]
    fn integration_14_begin_insert_rollback_discards() {
        let _g = test_lock().lock().unwrap();
        let t = unique_name("t_tx_rb");
        let mut c = Client::connect();
        c.send(&format!("CREATE TABLE {} (id Integer, name Varchar(25))", t), 256);
        c.send("BEGIN TRANSACTION", 256);
        c.send(&format!("INSERT INTO {} VALUES (1, 'a')", t), 256);
        c.send("ROLLBACK", 256);
        let r = c.send(&format!("SELECT * FROM {}", t), 256);
        assert_eq!(r.status, 0);
        assert_eq!(r.rows.len(), 0);
    }

    #[test]
    fn integration_15_tx_context_is_connection_local() {
        let _g = test_lock().lock().unwrap();
        let mut c1 = Client::connect();
        let mut c2 = Client::connect();
        assert_eq!(c1.send("BEGIN TRANSACTION", 256).status, 0);
        let r2 = c2.send("COMMIT", 256);
        assert_eq!(r2.status, 1);
        assert_eq!(c1.send("ROLLBACK", 256).status, 0);
    }

    #[test]
    fn integration_16_lock_conflict_same_table() {
        let _g = test_lock().lock().unwrap();
        let t = unique_name("t_lock_conflict");
        let mut c1 = Client::connect();
        let mut c2 = Client::connect();
        c1.send(&format!("CREATE TABLE {} (id Integer, name Varchar(25))", t), 256);

        assert_eq!(c1.send("BEGIN TRANSACTION", 256).status, 0);
        assert_eq!(c1.send(&format!("INSERT INTO {} VALUES (1, 'a')", t), 256).status, 0);

        let r = c2.send(&format!("INSERT INTO {} VALUES (2, 'b')", t), 256);
        assert_eq!(r.status, 1);
        assert!(r.message.contains("ExceptionTableLocked"));

        assert_eq!(c1.send("ROLLBACK", 256).status, 0);
    }

    #[test]
    fn integration_17_lock_disjoint_tables_succeeds() {
        let _g = test_lock().lock().unwrap();
        let t1 = unique_name("t_lock_a");
        let t2 = unique_name("t_lock_b");
        let mut c1 = Client::connect();
        let mut c2 = Client::connect();

        c1.send(&format!("CREATE TABLE {} (id Integer, name Varchar(25))", t1), 256);
        c1.send(&format!("CREATE TABLE {} (id Integer, name Varchar(25))", t2), 256);

        assert_eq!(c1.send("BEGIN TRANSACTION", 256).status, 0);
        assert_eq!(c1.send(&format!("INSERT INTO {} VALUES (1, 'a')", t1), 256).status, 0);

        let r = c2.send(&format!("INSERT INTO {} VALUES (2, 'b')", t2), 256);
        assert_eq!(r.status, 0);

        assert_eq!(c1.send("ROLLBACK", 256).status, 0);
    }

    #[test]
    fn integration_18_lock_released_after_commit() {
        let _g = test_lock().lock().unwrap();
        let t = unique_name("t_lock_release");
        let mut c1 = Client::connect();
        let mut c2 = Client::connect();
        c1.send(&format!("CREATE TABLE {} (id Integer, name Varchar(25))", t), 256);

        c1.send("BEGIN TRANSACTION", 256);
        c1.send(&format!("INSERT INTO {} VALUES (1, 'a')", t), 256);
        c1.send("COMMIT", 256);

        let r = c2.send(&format!("INSERT INTO {} VALUES (2, 'b')", t), 256);
        assert_eq!(r.status, 0);
    }

    #[test]
    fn integration_19_create_index_success() {
        let _g = test_lock().lock().unwrap();
        let t = unique_name("t_idx");
        let idx = unique_name("idx_name");
        let mut c = Client::connect();
        c.send(&format!("CREATE TABLE {} (id Integer, name Varchar(25))", t), 256);
        let r = c.send(&format!("CREATE INDEX {} ON {} (name)", idx, t), 256);
        assert_eq!(r.status, 0);
    }

    #[test]
    fn integration_20_drop_table_success() {
        let _g = test_lock().lock().unwrap();
        let t = unique_name("t_drop");
        let mut c = Client::connect();
        c.send(&format!("CREATE TABLE {} (id Integer)", t), 256);
        let r = c.send(&format!("DROP TABLE {}", t), 256);
        assert_eq!(r.status, 0);
    }

    #[test]
    fn integration_21_drop_table_then_select_fails() {
        let _g = test_lock().lock().unwrap();
        let t = unique_name("t_drop_sel");
        let mut c = Client::connect();
        c.send(&format!("CREATE TABLE {} (id Integer)", t), 256);
        c.send(&format!("DROP TABLE {}", t), 256);
        let r = c.send(&format!("SELECT * FROM {}", t), 256);
        assert_eq!(r.status, 1);
    }

    #[test]
    fn integration_22_fetch_n_chunking_works() {
        let _g = test_lock().lock().unwrap();
        let t = unique_name("t_chunk");
        let mut c = Client::connect();
        c.send(&format!("CREATE TABLE {} (id Integer, name Varchar(25))", t), 256);
        c.send(&format!("INSERT INTO {} VALUES (1, 'a')", t), 256);
        c.send(&format!("INSERT INTO {} VALUES (2, 'b')", t), 256);
        c.send(&format!("INSERT INTO {} VALUES (3, 'c')", t), 256);

        let r = c.send(&format!("SELECT * FROM {}", t), 1);
        assert_eq!(r.status, 0);
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.chunk_counts, vec![1, 1, 1, 0]);
        assert_eq!(r.done_flags, vec![0, 0, 0, 1]);
    }

    #[test]
    fn integration_23_fetch_n_zero_returns_done_only() {
        let _g = test_lock().lock().unwrap();
        let t = unique_name("t_fetch0");
        let mut c = Client::connect();
        c.send(&format!("CREATE TABLE {} (id Integer)", t), 256);
        c.send(&format!("INSERT INTO {} VALUES (1)", t), 256);

        let r = c.send(&format!("SELECT * FROM {}", t), 0);
        assert_eq!(r.status, 0);
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.chunk_counts, vec![1, 0]);
        assert_eq!(r.done_flags, vec![0, 1]);
    }

    #[test]
    fn integration_24_bad_magic_disconnects() {
        let _g = test_lock().lock().unwrap();
        let addr = server_addr();
        let mut s = TcpStream::connect(addr).unwrap();
        s.write_all(b"NOPE").unwrap();
        s.write_all(&[VERSION]).unwrap();
        s.write_all(&0u32.to_be_bytes()).unwrap();
        s.write_all(&0u32.to_be_bytes()).unwrap();
        s.flush().unwrap();

        s.set_read_timeout(Some(Duration::from_millis(250))).unwrap();
        let mut one = [0u8; 1];
        let read = s.read(&mut one);
        assert!(read.is_err() || matches!(read, Ok(0)));
    }

    #[test]
    fn integration_25_unsupported_version_disconnects() {
        let _g = test_lock().lock().unwrap();
        let addr = server_addr();
        let mut s = TcpStream::connect(addr).unwrap();
        s.write_all(MAGIC).unwrap();
        s.write_all(&[99u8]).unwrap();
        s.write_all(&0u32.to_be_bytes()).unwrap();
        s.write_all(&0u32.to_be_bytes()).unwrap();
        s.flush().unwrap();

        s.set_read_timeout(Some(Duration::from_millis(250))).unwrap();
        let mut one = [0u8; 1];
        let read = s.read(&mut one);
        assert!(read.is_err() || matches!(read, Ok(0)));
    }
}
