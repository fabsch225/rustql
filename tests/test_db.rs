#[cfg(test)]
mod tests {
    use rustql::executor::QueryExecutor as RustqlQueryExecutor;
    use std::fs;
    use std::ops::{Deref, DerefMut};
    use std::sync::atomic::{AtomicUsize, Ordering};

    const BTREE_NODE_SIZE: usize = 7;
    static DB_COUNTER: AtomicUsize = AtomicUsize::new(0);

    struct QueryExecutor {
        inner: RustqlQueryExecutor,
        db_path: String,
    }

    impl QueryExecutor {
        fn init(_path: &str, btree_node_size: usize) -> Self {
            let idx = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = format!("./default.db.test_db.{}.{}.bin", std::process::id(), idx);
            let _ = fs::remove_file(&path);
            Self {
                inner: RustqlQueryExecutor::init(&path, btree_node_size),
                db_path: path,
            }
        }
    }

    impl Deref for QueryExecutor {
        type Target = RustqlQueryExecutor;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }

    impl DerefMut for QueryExecutor {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.inner
        }
    }

    impl Drop for QueryExecutor {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.db_path);
        }
    }

    #[test]
    fn test_create_table() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        let result = executor.prepare("CREATE TABLE test (id Integer, name String)".to_string());
        assert!(result.success);
    }

    #[test]
    fn test_insert_single_row() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Integer, name String)".to_string());
        let result =
            executor.prepare("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string());
        assert!(result.success);
    }

    #[test]
    fn test_insert_multiple_rows() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Integer, name String)".to_string());
        executor.prepare("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string());
        let result = executor.prepare("INSERT INTO test (id, name) VALUES (2, 'Bob')".to_string());
        assert!(result.success);
    }

    #[test]
    fn test_select_all() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Integer, name String)".to_string());
        executor.prepare("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string());
        executor.prepare("INSERT INTO test (id, name) VALUES (2, 'Bob')".to_string());
        let result = executor.prepare("SELECT * FROM test".to_string());
        assert!(result.success);
        assert_eq!(result.data.clone().fetch().unwrap().len(), 2);
        assert_eq!(
            result.data.clone().fetch().unwrap()[0][0..10],
            vec![0, 0, 0, 0, 1u8, b'A', b'l', b'i', b'c', b'e']
        );
        assert_eq!(
            result.data.fetch().unwrap()[1][0..8],
            vec![0, 0, 0, 0, 2u8, b'B', b'o', b'b']
        );
    }

    #[test]
    fn test_select_with_condition() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Integer, name String)".to_string());
        executor.prepare("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string());
        executor.prepare("INSERT INTO test (id, name) VALUES (2, 'Bob')".to_string());
        let result = executor.prepare("SELECT * FROM test WHERE id <= 1".to_string());
        assert!(result.success);
        println!("{}", result);
        assert_eq!(result.data.clone().fetch().unwrap().len(), 1);
        assert_eq!(
            result.data.fetch().unwrap()[0][0..10],
            vec![0, 0, 0, 0, 1, b'A', b'l', b'i', b'c', b'e']
        );
    }

    #[test]
    fn test_varchar_insert_and_select() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Integer, name VARCHAR(5))".to_string());

        let insert_result =
            executor.prepare("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string());
        assert!(insert_result.success);

        let result = executor.prepare("SELECT name FROM test WHERE id = 1".to_string());
        assert!(result.success);
        let rows = result.data.fetch().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![b'A', b'l', b'i', b'c', b'e', 0]);
    }

    #[test]
    fn test_varchar_rejects_too_long_values() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Integer, name VARCHAR(5))".to_string());

        let result =
            executor.prepare("INSERT INTO test (id, name) VALUES (1, 'TooLong')".to_string());

        assert!(!result.success);
    }

    #[test]
    fn test_delete_single_row() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Integer, name String)".to_string());
        executor.prepare("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string());
        executor.prepare("INSERT INTO test (id, name) VALUES (2, 'Bob')".to_string());
        let delete_result = executor.prepare("DELETE FROM test WHERE id = 1".to_string());
        assert!(delete_result.success);
        let result = executor.prepare("SELECT name FROM test".to_string());
        println!("{}", result);
        assert_eq!(result.data.clone().fetch().unwrap().len(), 1);
        assert_eq!(
            result.data.fetch().unwrap()[0][0..3],
            vec![b'B', b'o', b'b']
        );
    }

    #[test]
    fn test_update_non_key_field() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Integer, name String)".to_string());
        executor.prepare("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string());

        let result = executor.prepare("UPDATE test SET name = 'Alicia' WHERE id = 1".to_string());
        assert!(result.success);

        let selected = executor.prepare("SELECT * FROM test WHERE id = 1".to_string());
        assert!(selected.success);
        let rows = selected.data.fetch().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][5..11], [b'A', b'l', b'i', b'c', b'i', b'a']);
    }

    #[test]
    fn test_update_primary_key_by_delete_reinsert() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Integer, name String)".to_string());
        executor.prepare("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string());

        let result = executor.prepare("UPDATE test SET id = 10 WHERE id = 1".to_string());
        assert!(result.success);

        let old_key_result = executor.prepare("SELECT * FROM test WHERE id = 1".to_string());
        assert!(old_key_result.success);
        assert_eq!(old_key_result.data.fetch().unwrap().len(), 0);

        let new_key_result = executor.prepare("SELECT * FROM test WHERE id = 10".to_string());
        assert!(new_key_result.success);
        let rows = new_key_result.data.fetch().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0..5], [0, 0, 0, 0, 10]);
    }

    #[test]
    fn test_insert() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Integer, other Integer)".to_string());
        for i in 0..=10 {
            let res = executor.prepare(format!(
                "INSERT INTO test (id, other) VALUES ({}, 0)",
                10 - i
            ));
            assert!(res.success);
        }
        executor.debug(Some("test"));
        assert!(executor.check_integrity().is_ok());
    }

    #[test]
    fn test_delete_all_rows() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Integer, name String)".to_string());
        executor.prepare("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string());
        executor.prepare("INSERT INTO test (id, name) VALUES (2, 'Bob')".to_string());
        //executor.exec("INSERT INTO test (id, name) VALUES (4, 'Charlie')".to_string());
        executor.prepare("DELETE FROM test WHERE id <= 2".to_string());
        let result = executor.prepare("SELECT * FROM test".to_string());
        println!("{}", result);
        assert!(result.success);
        assert_eq!(result.data.fetch().unwrap().len(), 0);
    }

    #[test]
    fn test_insert_and_select_large_dataset() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Integer, name String)".to_string());
        for i in 1..=100 {
            executor.prepare(format!(
                "INSERT INTO test (id, name) VALUES ({}, 'User{}')",
                i, i
            ));
        }
        let result = executor.prepare("SELECT * FROM test".to_string());
        assert!(result.success);
        assert_eq!(result.data.clone().fetch().unwrap().len(), 100);
        for (i, row) in result.data.fetch().unwrap().iter().enumerate() {
            let expected_name = format!("User{}", i + 1).as_bytes().to_vec();
            assert_eq!(row[0..5], [0u8, 0, 0, 0, (i + 1) as u8]);
            assert_eq!(row[5..10], expected_name[0..5]);
        }
    }

    #[test]
    fn test_delete_and_reinsert_with_loops() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Integer, other Integer)".to_string());
        for len in (100..350).step_by(10) {
            executor.prepare("DELETE FROM test".to_string());
            for i in 1..=len {
                executor.prepare(format!(
                    "INSERT INTO test (id, other) VALUES ({}, {})",
                    i, 0
                ));
            }
            let result = executor.prepare(format!("SELECT * FROM test WHERE id <= {}", len));
            assert!(result.success);
            assert_eq!(result.data.fetch().unwrap().len(), len);
            //println!("{}", result);
            let result = executor.prepare(format!("SELECT * FROM test WHERE id <= {}", len / 2));
            assert!(result.success);
            assert_eq!(result.data.fetch().unwrap().len(), len / 2);
            //println!("{}", result);
            //println!("---");
            //executor.debug(Some("test"));
            let result = executor.prepare(format!("DELETE FROM test WHERE id <= {}", len / 2));
            assert!(result.success);
            //executor.debug(Some("test"));
            let result = executor.prepare("SELECT * FROM test".to_string());
            assert!(result.success);
            //println!("{}", result);
            //println!("---");
            assert_eq!(result.data.clone().fetch().unwrap().len(), len / 2);
            for (_i, _row) in result.data.fetch().unwrap().iter().enumerate() {
                //assert_eq!(Serializer::bytes_to_int(row[0..5].try_into().unwrap()), expected_id as i32);
            }

            for i in 1..=len / 2 {
                let result = executor.prepare(format!(
                    "INSERT INTO test (id, other) VALUES ({}, '{}')",
                    i,
                    i * 2
                ));

                //println!("{}", result);
                assert!(result.success)
            }

            let result = executor.prepare("SELECT * FROM test".to_string());
            assert_eq!(result.data.fetch().unwrap().len(), len);
            if !executor.check_integrity().is_ok() {
                executor.debug(Some("test"));
            }
            assert!(executor.check_integrity().is_ok())
            //
        }
    }

    #[test]
    fn test_specific_reinsert() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Integer, other Integer)".to_string());
        for i in 1..=10 {
            let res = executor.prepare(format!(
                "INSERT INTO test (id, other) VALUES ({}, {})",
                i, 0
            ));
            assert!(res.success);
        }
        for i in 1..=5 {
            executor.prepare(format!("DELETE FROM test WHERE id = {}", i));
        }
        println!("after deletion");
        for i in 1..=5 {
            executor.prepare(format!(
                "INSERT INTO test (id, other) VALUES ({}, {})",
                i, 0
            ));
            //executor.debug(Some("test"));
        }
        assert!(executor.check_integrity().is_ok());
    }

    #[test]
    fn test_string_pk_reinsert() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Varchar(20), other Integer)".to_string());

        for i in 1..=10 {
            let res = executor.prepare(format!(
                "INSERT INTO test (id, other) VALUES ('key{}', {})",
                i, 0
            ));
            assert!(res.success);
        }

        for i in 1..=5 {
            executor.prepare(format!("DELETE FROM test WHERE id = 'key{}'", i));
        }

        for i in 1..=5 {
            executor.prepare(format!(
                "INSERT INTO test (id, other) VALUES ('key{}', {})",
                i, 0
            ));
        }

        assert!(executor.check_integrity().is_ok());
    }

    #[test]
    fn test_string_pk_sparse_reinsert() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Varchar(20), other Integer)".to_string());

        let keys = ["a", "c", "e", "g", "i", "k", "m", "o", "q", "s"];

        for (i, k) in keys.iter().enumerate() {
            let res = executor.prepare(format!(
                "INSERT INTO test (id, other) VALUES ('{}', {})",
                k, i
            ));
            assert!(res.success);
        }

        for k in ["c", "g", "k", "o", "s"] {
            executor.prepare(format!("DELETE FROM test WHERE id = '{}'", k));
        }

        for k in ["c", "g", "k", "o", "s"] {
            executor.prepare(format!(
                "INSERT INTO test (id, other) VALUES ('{}', {})",
                k, 999
            ));
        }

        assert!(executor.check_integrity().is_ok());
    }

    #[test]
    fn test_string_pk_prefix_keys() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Varchar(20), other Integer)".to_string());

        for i in 1..=10 {
            let res = executor.prepare(format!(
                "INSERT INTO test (id, other) VALUES ('user_{:02}', {})",
                i, i
            ));
            assert!(res.success);
        }

        for i in 3..=7 {
            executor.prepare(format!("DELETE FROM test WHERE id = 'user_{:02}'", i));
        }

        for i in 3..=7 {
            executor.prepare(format!(
                "INSERT INTO test (id, other) VALUES ('user_{:02}', {})",
                i, 42
            ));
        }

        assert!(executor.check_integrity().is_ok());
    }

    #[test]
    fn test_varchar_2000_pk_reinsert() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Varchar(2000), other Integer)".to_string());

        // Create long keys (~1500 chars each)
        for i in 1..=10 {
            let long_key = "x".repeat(1500) + &i.to_string();

            let res = executor.prepare(format!(
                "INSERT INTO test (id, other) VALUES ('{}', {})",
                long_key, 0
            ));
            assert!(res.success);
        }

        for i in 1..=5 {
            let long_key = "x".repeat(1500) + &i.to_string();
            executor.prepare(format!("DELETE FROM test WHERE id = '{}'", long_key));
        }

        for i in 1..=5 {
            let long_key = "x".repeat(1500) + &i.to_string();
            executor.prepare(format!(
                "INSERT INTO test (id, other) VALUES ('{}', {})",
                long_key, 0
            ));
        }

        assert!(executor.check_integrity().is_ok());
    }

    #[test]
    fn test_modulo() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Integer, other Integer)".to_string());

        let test_sizes = [50, 100, 150]; //, 300, 800];
        let modulos = [2, 3, 4, 5, 6];

        for &size in &test_sizes {
            for &modulo in &modulos {
                executor.prepare("DELETE FROM test".to_string());

                for i in 1..=size {
                    executor.prepare(format!(
                        "INSERT INTO test (id, other) VALUES ({}, {})",
                        i, 0
                    ));
                }
                let result = executor.prepare("SELECT * FROM test".to_string());
                println!("{}", result);
                assert_eq!(result.data.fetch().unwrap().len(), size);

                let mut count_deleted = 0;
                for i in 1..=size {
                    if i % modulo == 0 {
                        //println!("Deleting {}", i);
                        let result = executor.prepare(format!("DELETE FROM test WHERE id = {}", i));
                        if result.success {
                            count_deleted += 1;
                        } else {
                            println!("Failed to delete {}: {}", i, result);
                        }
                        //executor.debug(Some("test"));
                        assert!(result.success);
                    }
                }
                let result = executor.prepare("SELECT * FROM test".to_string());
                println!(
                    "After deleting multiples of {}: {} entries left",
                    modulo,
                    result.data.clone().fetch().unwrap().len()
                );
                assert_eq!(result.data.fetch().unwrap().len(), size - count_deleted);
                assert!(executor.check_integrity().is_ok())
            }
        }
    }
    #[test]
    fn test_delete_and_insert_complex() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Integer, name String)".to_string());

        // Insert initial rows
        for i in 1..=10 {
            executor.prepare(format!(
                "INSERT INTO test (id, name) VALUES ({}, 'User{}')",
                i, i
            ));
        }

        executor.debug_readonly(Some("test"));

        // Delete some rows
        for i in 1..=5 {
            executor.prepare(format!("DELETE FROM test WHERE id = {}", i));
        }

        executor.debug_readonly(Some("test"));

        // Insert new rows
        for i in 11..=15 {
            executor.prepare(format!(
                "INSERT INTO test (id, name) VALUES ({}, 'NewUser{}')",
                i, i
            ));
        }
        executor.debug_readonly(Some("test"));
        // Verify the remaining rows
        let result = executor.prepare("SELECT * FROM test".to_string());
        println!("{}", result);
        assert!(result.success);
        assert_eq!(result.data.clone().fetch().unwrap().len(), 10);

        // Check the integrity of the data
        for (i, row) in result.data.fetch().unwrap().iter().enumerate() {
            let expected_id = if i < 5 { i + 6 } else { i + 6 };
            let expected_name = if i < 5 {
                format!("User{}", expected_id)
            } else {
                format!("NewUser{}", expected_id)
            }
            .as_bytes()
            .to_vec();
            assert_eq!(row[0..5], [0u8, 0, 0, 0, expected_id as u8]);
            assert_eq!(row[5..10], expected_name[0..5]);
        }

        assert!(executor.check_integrity().is_ok());
    }

    #[test]
    fn test_delete_and_insert_with_conditions() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Integer, name String)".to_string());

        // Insert initial rows
        for i in 1..=20 {
            executor.prepare(format!(
                "INSERT INTO test (id, name) VALUES ({}, 'User{}')",
                i, i
            ));
        }

        // Delete rows with specific conditions
        executor.prepare("DELETE FROM test WHERE id % 2 = 0".to_string());

        // Insert new rows with conditions
        for i in 21..=30 {
            if i % 2 != 0 {
                executor.prepare(format!(
                    "INSERT INTO test (id, name) VALUES ({}, 'OddUser{}')",
                    i, i
                ));
            }
        }

        // Verify the remaining rows
        let result = executor.prepare("SELECT * FROM test".to_string());
        println!("{}", result);
        assert!(result.success);
        assert_eq!(result.data.fetch().unwrap().len(), 25);
        assert!(executor.check_integrity().is_ok());
    }

    #[test]
    fn test_modulo_with_reinserts() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        let result = executor
            .prepare("CREATE TABLE test (id Integer, other Integer, name String)".to_string());
        assert!(result.success);
        let test_sizes = [50, 100, 150]; //, 300, 800];
        let modulos = [2, 3, 4, 5, 6];

        for &size in &test_sizes {
            for &modulo in &modulos {
                println!("mod {}", modulo);
                executor.prepare("DELETE FROM test".to_string());
                let result = executor.prepare("SELECT * FROM test".to_string());
                assert_eq!(result.data.fetch().unwrap().len(), 0);
                //executor.exec("CREATE TABLE test (id Integer, other Integer)".to_string());

                for i in 1..=size {
                    let result = executor.prepare(format!(
                        "INSERT INTO test (id, other, name) VALUES ({}, {}, '{}')",
                        i,
                        modulo,
                        format!("Hallo Welt {}", i)
                    ));
                    //println!("{}", result);
                    assert!(result.success)
                }
                let result = executor.prepare("SELECT * FROM test".to_string());
                assert!(result.success);
                //println!("{}", result);
                assert_eq!(result.data.fetch().unwrap().len(), size);

                let mut count_deleted = 0;
                for i in 1..=size {
                    if i % modulo == 0 {
                        let result = executor.prepare(format!("DELETE FROM test WHERE id = {}", i));
                        //println!("{}", result);
                        assert!(result.success);
                        count_deleted += 1;
                    }
                }
                let result = executor.prepare("SELECT * FROM test".to_string());
                //println!("{}", result);
                assert_eq!(result.data.fetch().unwrap().len(), size - count_deleted);
                assert!(executor.check_integrity().is_ok());
                for i in 1..=size {
                    if i % modulo == 0 {
                        let result = executor.prepare(format!(
                            "INSERT INTO test (id, other, name) VALUES ({}, {}, '{}')",
                            i,
                            modulo,
                            format!("Hallo Welt {}", i)
                        ));
                        assert!(result.success);
                    }
                }

                let result = executor.prepare("SELECT * FROM test".to_string());
                assert_eq!(result.data.fetch().unwrap().len(), size);
                assert!(executor.check_integrity().is_ok());
            }
        }
        executor.debug(Some("test"));
    }

    #[test]
    fn test_very_large_inserts() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Integer, other Integer)".to_string());
        for i in 1..=100000 {
            let result = executor.prepare(format!(
                "INSERT INTO test (id, other) VALUES ({}, {})",
                i,
                i * 3
            ));
            if !result.success {
                println!("Failed to insert {}: {}", i, result);
            }
            assert!(result.success);
        }
        let result = executor.prepare("SELECT * FROM test WHERE other < 90000".to_string());
        assert!(result.success);
        assert_eq!(result.data.fetch().unwrap().len(), 29999);
    }

    #[test]
    fn test_many_varchars() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor
            .prepare("CREATE TABLE test (id Integer, name VARCHAR(420), age Integer)".to_string());
        for i in 1..=200 {
            let long_name = "User ".to_string() + &i.to_string() + &"x".repeat(300);
            let result = executor.prepare(format!(
                "INSERT INTO test (id, name, age) VALUES ({}, '{}', {})",
                i,
                long_name,
                i * 3
            ));
            if !result.success {
                println!("Failed to insert {}: {}", i, result);
            }
            assert!(result.success);
        }
        let result = executor.prepare("SELECT * FROM test WHERE id <= 20".to_string());
        assert!(result.success);
        assert_eq!(result.data.fetch().unwrap().len(), 20);
    }

    #[test]
    fn test_many_strings() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Integer, name String, age Integer)".to_string());
        for i in 1..=200 {
            let long_name = "User ".to_string() + &i.to_string() + &"x".repeat(100);
            let result = executor.prepare(format!(
                "INSERT INTO test (id, name, age) VALUES ({}, '{}', {})",
                i,
                long_name,
                i * 3
            ));
            if !result.success {
                println!("Failed to insert {}: {}", i, result);
            }
            assert!(result.success);
        }
        let result = executor.prepare("SELECT * FROM test WHERE id <= 20".to_string());
        assert!(result.success);
        assert_eq!(result.data.fetch().unwrap().len(), 20);
    }

    #[test]
    fn test_large_nodes() {
        let mut executor = QueryExecutor::init("./default.db.bin", 50);
        executor
            .prepare("CREATE TABLE test (id Integer, name VARCHAR(420), age Integer)".to_string());
        for i in 1..=100 {
            let long_name = "User ".to_string() + &i.to_string() + &"x".repeat(300);
            let result = executor.prepare(format!(
                "INSERT INTO test (id, name, age) VALUES ({}, '{}', {})",
                i,
                long_name,
                i * 3
            ));
            if !result.success {
                println!("Failed to insert {}: {}", i, result);
            }
            assert!(result.success);
        }
        let result = executor.prepare("SELECT * FROM test WHERE id <= 20".to_string());
        assert!(result.success);
        assert_eq!(result.data.fetch().unwrap().len(), 20);
    }

    #[test]
    fn test_large_nodes_multiple_splits_and_integrity() {
        let mut executor = QueryExecutor::init("./default.db.bin", 50);
        executor
            .prepare("CREATE TABLE test (id Integer, name VARCHAR(420), age Integer)".to_string());

        for i in 1..=220 {
            let long_name = "User ".to_string() + &i.to_string() + &"y".repeat(320);
            let result = executor.prepare(format!(
                "INSERT INTO test (id, name, age) VALUES ({}, '{}', {})",
                i,
                long_name,
                i * 2
            ));
            if !result.success {
                println!("Failed to insert {}: {}", i, result);
            }
            assert!(result.success);
        }

        let all_rows = executor.prepare("SELECT * FROM test".to_string());
        assert!(all_rows.success);
        assert_eq!(all_rows.data.fetch().unwrap().len(), 220);

        let subset = executor.prepare("SELECT * FROM test WHERE id <= 37".to_string());
        assert!(subset.success);
        assert_eq!(subset.data.fetch().unwrap().len(), 37);

        assert!(executor.check_integrity().is_ok());
    }

    #[test]
    fn test_large_nodes_delete_and_reinsert() {
        let mut executor = QueryExecutor::init("./default.db.bin", 50);
        executor
            .prepare("CREATE TABLE test (id Integer, name VARCHAR(420), age Integer)".to_string());

        for i in 1..=160 {
            let long_name = "User ".to_string() + &i.to_string() + &"z".repeat(280);
            let result = executor.prepare(format!(
                "INSERT INTO test (id, name, age) VALUES ({}, '{}', {})",
                i,
                long_name,
                i * 3
            ));
            if !result.success {
                println!("Failed to insert {}: {}", i, result);
            }
            assert!(result.success);
        }

        let delete_result = executor.prepare("DELETE FROM test WHERE id <= 60".to_string());
        assert!(delete_result.success);

        for i in 1..=60 {
            let long_name = "ReUser ".to_string() + &i.to_string() + &"w".repeat(260);
            let result = executor.prepare(format!(
                "INSERT INTO test (id, name, age) VALUES ({}, '{}', {})",
                i,
                long_name,
                i * 5
            ));
            if !result.success {
                println!("Failed to reinsert {}: {}", i, result);
            }
            assert!(result.success);
        }

        let all_rows = executor.prepare("SELECT * FROM test".to_string());
        assert!(all_rows.success);
        assert_eq!(all_rows.data.fetch().unwrap().len(), 160);

        assert!(executor.check_integrity().is_ok());
    }
}
