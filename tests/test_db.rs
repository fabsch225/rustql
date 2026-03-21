#[cfg(test)]
mod tests {
    use rustql::btree::Btree;
    use rustql::executor::QueryExecutor as RustqlQueryExecutor;
    use rustql::pager::Position;
    use rustql::pager_proxy::PagerProxy;
    use rustql::planner::{CompiledQuery, PlanNode, SqlConditionOpCode};
    use rustql::serializer::Serializer;
    use std::collections::HashSet;
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
    fn test_transaction_commit_persists_changes() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        assert!(
            executor
                .prepare("CREATE TABLE test (id Integer, name String)".to_string())
                .success
        );

        assert!(executor.prepare("BEGIN TRANSACTION".to_string()).success);
        assert!(
            executor
                .prepare("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string())
                .success
        );

        let in_tx = executor.prepare("SELECT * FROM test".to_string());
        assert!(in_tx.success);
        assert_eq!(in_tx.data.fetch().unwrap().len(), 1);

        assert!(executor.prepare("COMMIT".to_string()).success);

        let after_commit = executor.prepare("SELECT * FROM test".to_string());
        assert!(after_commit.success);
        assert_eq!(after_commit.data.fetch().unwrap().len(), 1);
    }

    #[test]
    fn test_transaction_rollback_discards_changes() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        assert!(
            executor
                .prepare("CREATE TABLE test (id Integer, name String)".to_string())
                .success
        );

        assert!(executor.prepare("BEGIN TRANSACTION".to_string()).success);
        assert!(
            executor
                .prepare("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string())
                .success
        );

        let in_tx = executor.prepare("SELECT * FROM test".to_string());
        assert!(in_tx.success);
        assert_eq!(in_tx.data.fetch().unwrap().len(), 1);

        assert!(executor.prepare("ROLLBACK".to_string()).success);

        let after_rollback = executor.prepare("SELECT * FROM test".to_string());
        assert!(after_rollback.success);
        assert_eq!(after_rollback.data.fetch().unwrap().len(), 0);
    }

    #[test]
    fn test_begin_transaction_while_active_fails() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        assert!(executor.prepare("BEGIN TRANSACTION".to_string()).success);
        let second = executor.prepare("BEGIN TRANSACTION".to_string());
        assert!(!second.success);
        assert!(executor.prepare("ROLLBACK".to_string()).success);
    }

    #[test]
    fn test_create_table_duplicate_name_with_trailing_zero_fails() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        let table_name = "table_ends_with_zero0";

        let first = executor.prepare(format!("CREATE TABLE {} (id Integer)", table_name));
        assert!(first.success);

        let second = executor.prepare(format!("CREATE TABLE {} (id Integer)", table_name));
        assert!(!second.success);
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

    #[test]
    fn test_manual_index_create_and_drop_lifecycle() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);

        let created = executor.prepare("CREATE TABLE people (id Integer, name String)".to_string());
        assert!(created.success);

        executor.prepare("INSERT INTO people (id, name) VALUES (1, 'Alice')".to_string());
        executor.prepare("INSERT INTO people (id, name) VALUES (2, 'Bob')".to_string());

        let before_idx = executor.prepare("SELECT * FROM idx_people_name".to_string());
        assert!(!before_idx.success);

        let create_idx =
            executor.prepare("CREATE INDEX idx_people_name ON people (name)".to_string());
        assert!(create_idx.success);

        let manual_index_table = executor
            .prepare("CREATE TABLE _people_name (idx_value String, base_pk Integer)".to_string());
        assert!(!manual_index_table.success);

        let index_read =
            executor.prepare("SELECT * FROM idx_people_name WHERE idx_value = 'Alice'".to_string());
        assert!(index_read.success);
        assert_eq!(index_read.data.fetch().unwrap().len(), 1);

        let drop_idx = executor.prepare("DROP INDEX idx_people_name".to_string());
        assert!(drop_idx.success);

        let after_idx = executor.prepare("SELECT * FROM idx_people_name".to_string());
        assert!(!after_idx.success);

        let compiled = executor
            .compile_query("SELECT id FROM people WHERE name = 'Alice'")
            .unwrap();
        match compiled {
            CompiledQuery::Select(select) => {
                fn find_scan(plan: &PlanNode) -> Option<&PlanNode> {
                    match plan {
                        PlanNode::SeqScan { .. } => Some(plan),
                        PlanNode::Project { source, .. } => find_scan(source),
                        PlanNode::Filter { source, .. } => find_scan(source),
                        _ => None,
                    }
                }

                match find_scan(&select.plan) {
                    Some(PlanNode::SeqScan { operation, .. }) => {
                        assert_eq!(*operation, SqlConditionOpCode::SelectFTS);
                    }
                    _ => panic!("expected SeqScan in plan"),
                }
            }
            _ => panic!("expected compiled SELECT"),
        }
    }

    #[test]
    fn test_drop_table_removes_table() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        assert!(
            executor
                .prepare("CREATE TABLE to_drop (id Integer, name String)".to_string())
                .success
        );
        assert!(
            executor
                .prepare("INSERT INTO to_drop (id, name) VALUES (1, 'x')".to_string())
                .success
        );

        let dropped = executor.prepare("DROP TABLE to_drop".to_string());
        assert!(dropped.success);

        let after = executor.prepare("SELECT * FROM to_drop".to_string());
        assert!(!after.success);
    }

    #[test]
    fn test_drop_table_marks_related_pages_deleted() {
        fn collect_pages(node: &rustql::btree::BTreeNode, pages: &mut HashSet<usize>) {
            if !pages.insert(node.position.page()) {
                return;
            }
            for child in PagerProxy::get_children(node).unwrap() {
                collect_pages(&child, pages);
            }
        }

        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        assert!(
            executor
                .prepare("CREATE TABLE to_drop_pages (id Integer, name String)".to_string())
                .success
        );

        for i in 1..=200 {
            let q = format!(
                "INSERT INTO to_drop_pages (id, name) VALUES ({}, 'user{}')",
                i, i
            );
            assert!(executor.prepare(q).success);
        }

        let table_id = rustql::planner::Planner::find_table_id(&executor.schema, "to_drop_pages")
            .expect("table id");
        let table_schema = executor.schema.tables[table_id].clone();
        let btree = Btree::init(
            table_schema.btree_order,
            executor.pager_accessor.clone(),
            table_schema.clone(),
        )
        .expect("btree init");

        let mut pages = HashSet::new();
        let root = btree.root.expect("root must exist");
        collect_pages(&root, &mut pages);
        assert!(!pages.is_empty());

        let dropped = executor.prepare("DROP TABLE to_drop_pages".to_string());
        assert!(dropped.success);

        for page in pages {
            let page_container = executor
                .pager_accessor
                .access_pager_write(|p| p.access_page_read(&Position::new(page, 0)))
                .expect("page read");
            assert!(Serializer::is_deleted(&page_container).expect("deleted flag"));
        }
    }

    #[test]
    fn test_manual_index_created_after_data_is_used_for_select() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        assert!(
            executor
                .prepare(
                    "CREATE TABLE products (id Integer, price Integer, name String)".to_string()
                )
                .success
        );

        for i in 1..=40 {
            let q = format!(
                "INSERT INTO products (id, price, name) VALUES ({}, {}, 'p{}')",
                i,
                100 + i,
                i
            );
            assert!(executor.prepare(q).success);
        }

        assert!(
            executor
                .prepare("CREATE INDEX idx_products_price ON products (price)".to_string())
                .success
        );

        let result = executor.prepare("SELECT id FROM products WHERE price = 117".to_string());
        assert!(result.success);
        let rows = result.data.fetch().unwrap();
        assert_eq!(rows.len(), 1);

        let compiled = executor
            .compile_query("SELECT id FROM products WHERE price = 117")
            .unwrap();
        match compiled {
            CompiledQuery::Select(select) => {
                fn find_scan(plan: &PlanNode) -> Option<&PlanNode> {
                    match plan {
                        PlanNode::SeqScan { .. } => Some(plan),
                        PlanNode::Project { source, .. } => find_scan(source),
                        PlanNode::Filter { source, .. } => find_scan(source),
                        _ => None,
                    }
                }

                match find_scan(&select.plan) {
                    Some(PlanNode::SeqScan {
                        operation,
                        index_table_id,
                        index_on_column,
                        ..
                    }) => {
                        assert_eq!(*operation, SqlConditionOpCode::SelectIndexUnique);
                        assert!(index_table_id.is_some());
                        assert_eq!(*index_on_column, Some(1));
                    }
                    _ => panic!("expected SeqScan in plan"),
                }
            }
            _ => panic!("expected compiled SELECT"),
        }
    }

    #[test]
    fn test_select_uses_index_when_available() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE people (id Integer, name String, age Integer)".to_string());
        assert!(
            executor
                .prepare("CREATE INDEX idx_people_name ON people (name)".to_string())
                .success
        );

        for i in 1..=30 {
            let q = format!(
                "INSERT INTO people (id, name, age) VALUES ({}, 'user{}', {})",
                i,
                i,
                i + 20
            );
            assert!(executor.prepare(q).success);
        }

        let result = executor.prepare("SELECT id FROM people WHERE name = 'user17'".to_string());
        assert!(result.success);
        assert_eq!(result.data.fetch().unwrap().len(), 1);

        let compiled = executor
            .compile_query("SELECT id FROM people WHERE name = 'user17'")
            .unwrap();

        match compiled {
            CompiledQuery::Select(select) => {
                fn find_scan(plan: &PlanNode) -> Option<&PlanNode> {
                    match plan {
                        PlanNode::SeqScan { .. } => Some(plan),
                        PlanNode::Project { source, .. } => find_scan(source),
                        PlanNode::Filter { source, .. } => find_scan(source),
                        _ => None,
                    }
                }

                match find_scan(&select.plan) {
                    Some(PlanNode::SeqScan {
                        operation,
                        index_table_id,
                        index_on_column,
                        ..
                    }) => {
                        assert_eq!(*operation, SqlConditionOpCode::SelectIndexUnique);
                        assert!(index_table_id.is_some());
                        assert_eq!(*index_on_column, Some(1));
                    }
                    _ => panic!("expected SeqScan in plan"),
                }
            }
            _ => panic!("expected compiled SELECT"),
        }
    }

    #[test]
    fn test_date_pk_reinsert() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.prepare("CREATE TABLE test (id Date, other Integer)".to_string());

        for d in 1..=10 {
            let res = executor.prepare(format!(
                "INSERT INTO test (id, other) VALUES ('2026-03-{:02}', {})",
                d, d
            ));
            assert!(res.success);
        }

        for d in 1..=5 {
            let res = executor.prepare(format!("DELETE FROM test WHERE id = '2026-03-{:02}'", d));
            assert!(res.success);
        }

        for d in 1..=5 {
            let res = executor.prepare(format!(
                "INSERT INTO test (id, other) VALUES ('2026-03-{:02}', {})",
                d,
                d * 10
            ));
            assert!(res.success);
        }

        let result = executor.prepare("SELECT * FROM test".to_string());
        assert!(result.success);
        assert_eq!(result.data.fetch().unwrap().len(), 10);
        assert!(executor.check_integrity().is_ok());
    }

    #[test]
    fn test_date_index_usage_in_select_plan_and_execution() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor
            .prepare("CREATE TABLE events (id Integer, event_date Date, name String)".to_string());
        assert!(
            executor
                .prepare("CREATE INDEX idx_events_event_date ON events (event_date)".to_string())
                .success
        );

        assert!(
            executor
                .prepare(
                    "INSERT INTO events (id, event_date, name) VALUES (1, '2026-03-15', 'A')"
                        .to_string()
                )
                .success
        );
        assert!(
            executor
                .prepare(
                    "INSERT INTO events (id, event_date, name) VALUES (2, '2026-03-16', 'B')"
                        .to_string()
                )
                .success
        );
        assert!(
            executor
                .prepare(
                    "INSERT INTO events (id, event_date, name) VALUES (3, '2026-03-17', 'C')"
                        .to_string()
                )
                .success
        );

        let result =
            executor.prepare("SELECT id FROM events WHERE event_date = '2026-03-17'".to_string());
        assert!(result.success);
        assert_eq!(result.data.fetch().unwrap().len(), 1);

        let compiled = executor
            .compile_query("SELECT id FROM events WHERE event_date = '2026-03-17'")
            .unwrap();

        match compiled {
            CompiledQuery::Select(select) => {
                fn find_scan(plan: &PlanNode) -> Option<&PlanNode> {
                    match plan {
                        PlanNode::SeqScan { .. } => Some(plan),
                        PlanNode::Project { source, .. } => find_scan(source),
                        PlanNode::Filter { source, .. } => find_scan(source),
                        _ => None,
                    }
                }

                match find_scan(&select.plan) {
                    Some(PlanNode::SeqScan {
                        operation,
                        index_table_id,
                        index_on_column,
                        ..
                    }) => {
                        assert_eq!(*operation, SqlConditionOpCode::SelectIndexUnique);
                        assert!(index_table_id.is_some());
                        assert_eq!(*index_on_column, Some(1));
                    }
                    _ => panic!("expected SeqScan in plan"),
                }
            }
            _ => panic!("expected compiled SELECT"),
        }
    }

    #[test]
    fn test_date_index_range_usage() {
        let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor
            .prepare("CREATE TABLE events (id Integer, event_date Date, name String)".to_string());
        assert!(
            executor
                .prepare("CREATE INDEX idx_events_event_date ON events (event_date)".to_string())
                .success
        );

        for d in 10..=20 {
            let q = format!(
                "INSERT INTO events (id, event_date, name) VALUES ({}, '2026-03-{}', 'e{}')",
                d, d, d
            );
            assert!(executor.prepare(q).success);
        }

        let result =
            executor.prepare("SELECT id FROM events WHERE event_date >= '2026-03-15'".to_string());
        assert!(result.success);
        assert_eq!(result.data.fetch().unwrap().len(), 6);

        let compiled = executor
            .compile_query("SELECT id FROM events WHERE event_date >= '2026-03-15'")
            .unwrap();

        match compiled {
            CompiledQuery::Select(select) => {
                fn find_scan(plan: &PlanNode) -> Option<&PlanNode> {
                    match plan {
                        PlanNode::SeqScan { .. } => Some(plan),
                        PlanNode::Project { source, .. } => find_scan(source),
                        PlanNode::Filter { source, .. } => find_scan(source),
                        _ => None,
                    }
                }

                match find_scan(&select.plan) {
                    Some(PlanNode::SeqScan {
                        operation,
                        index_table_id,
                        index_on_column,
                        ..
                    }) => {
                        assert_eq!(*operation, SqlConditionOpCode::SelectIndexRange);
                        assert!(index_table_id.is_some());
                        assert_eq!(*index_on_column, Some(1));
                    }
                    _ => panic!("expected SeqScan in plan"),
                }
            }
            _ => panic!("expected compiled SELECT"),
        }
    }
}
