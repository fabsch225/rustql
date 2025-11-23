#[cfg(test)]
mod tests {
    use rustql::executor::Executor;
    use rustql::serializer::Serializer;

    const BTREE_NODE_SIZE: usize = 3;

    #[test]
    fn test_create_table() {
        let mut executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        let result = executor.exec("CREATE TABLE test (id Integer, name String)".to_string());
        assert!(result.success);
    }

    #[test]
    fn test_insert_single_row() {
        let mut executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, name String)".to_string());
        let result = executor.exec("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string());
        assert!(result.success);
    }

    #[test]
    fn test_insert_multiple_rows() {
        let mut executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, name String)".to_string());
        executor.exec("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string());
        let result = executor.exec("INSERT INTO test (id, name) VALUES (2, 'Bob')".to_string());
        assert!(result.success);
    }

    #[test]
    fn test_select_all() {
        let mut executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, name String)".to_string());
        executor.exec("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string());
        executor.exec("INSERT INTO test (id, name) VALUES (2, 'Bob')".to_string());
        let result = executor.exec("SELECT * FROM test".to_string());
        assert!(result.success);
        assert_eq!(result.result.clone().fetch_data().unwrap().len(), 2);
        assert_eq!(
            result.result.clone().fetch_data().unwrap()[0][0..10],
            vec![0, 0, 0, 0, 1u8, b'A', b'l', b'i', b'c', b'e']
        );
        assert_eq!(
            result.result.fetch_data().unwrap()[1][0..8],
            vec![0, 0, 0, 0, 2u8, b'B', b'o', b'b']
        );
    }

    #[test]
    fn test_select_with_condition() {
        let mut executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, name String)".to_string());
        executor.exec("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string());
        executor.exec("INSERT INTO test (id, name) VALUES (2, 'Bob')".to_string());
        let result = executor.exec("SELECT * FROM test WHERE id <= 1".to_string());
        assert!(result.success);
        println!("{}", result);
        assert_eq!(result.result.clone().fetch_data().unwrap().len(), 1);
        assert_eq!(
            result.result.fetch_data().unwrap()[0][0..10],
            vec![0, 0, 0, 0, 1, b'A', b'l', b'i', b'c', b'e']
        );
    }

    #[test]
    fn test_delete_single_row() {
        let mut executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, name String)".to_string());
        executor.exec("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string());
        executor.exec("INSERT INTO test (id, name) VALUES (2, 'Bob')".to_string());
        let delete_result = executor.exec("DELETE FROM test WHERE id = 1".to_string());
        assert!(delete_result.success);
        let result = executor.exec("SELECT name FROM test".to_string());
        println!("{}", result);
        assert_eq!(result.result.clone().fetch_data().unwrap().len(), 1);
        assert_eq!(
            result.result.fetch_data().unwrap()[0][0..3],
            vec![b'B', b'o', b'b']
        );
    }

    #[test]
    fn test_insert() {
        let mut executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, other Integer)".to_string());
        for i in 0..=10 {
            let res = executor.exec(format!(
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
        let mut executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, name String)".to_string());
        executor.exec("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string());
        executor.exec("INSERT INTO test (id, name) VALUES (2, 'Bob')".to_string());
        //executor.exec("INSERT INTO test (id, name) VALUES (4, 'Charlie')".to_string());
        executor.exec("DELETE FROM test WHERE id <= 2".to_string());
        let result = executor.exec("SELECT * FROM test".to_string());
        println!("{}", result);
        assert!(result.success);
        assert_eq!(result.result.fetch_data().unwrap().len(), 0);
    }

    #[test]
    fn test_insert_and_select_large_dataset() {
        let mut executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, name String)".to_string());
        for i in 1..=100 {
            executor.exec(format!(
                "INSERT INTO test (id, name) VALUES ({}, 'User{}')",
                i, i
            ));
        }
        let result = executor.exec("SELECT * FROM test".to_string());
        assert!(result.success);
        assert_eq!(result.result.clone().fetch_data().unwrap().len(), 100);
        for (i, row) in result.result.fetch_data().unwrap().iter().enumerate() {
            let expected_name = format!("User{}", i + 1).as_bytes().to_vec();
            assert_eq!(row[0..5], [0u8, 0, 0, 0, (i + 1) as u8]);
            assert_eq!(row[5..10], expected_name[0..5]);
        }
    }

    #[test]
    fn test_delete_and_reinsert_with_loops() {
        let mut executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, other Integer)".to_string());
        for len in (100..350).step_by(10) {
            executor.exec("DELETE FROM test".to_string());
            for i in 1..=len {
                executor.exec(format!(
                    "INSERT INTO test (id, other) VALUES ({}, {})",
                    i, 0
                ));
            }
            let result = executor.exec(format!("SELECT * FROM test WHERE id <= {}", len));
            assert!(result.success);
            assert_eq!(result.result.fetch_data().unwrap().len(), len);
            //println!("{}", result);
            let result = executor.exec(format!("SELECT * FROM test WHERE id <= {}", len / 2));
            assert!(result.success);
            assert_eq!(result.result.fetch_data().unwrap().len(), len / 2);
            //println!("{}", result);
            //println!("---");
            //executor.debug(Some("test"));
            let result = executor.exec(format!("DELETE FROM test WHERE id <= {}", len / 2));
            assert!(result.success);
            //executor.debug(Some("test"));
            let result = executor.exec("SELECT * FROM test".to_string());
            assert!(result.success);
            //println!("{}", result);
            //println!("---");
            assert_eq!(result.result.clone().fetch_data().unwrap().len(), len / 2);
            for (i, row) in result.result.fetch_data().unwrap().iter().enumerate() {
                let expected_id = i + len / 2 + 2;
                //assert_eq!(Serializer::bytes_to_int(row[0..5].try_into().unwrap()), expected_id as i32);
            }

            for i in 1..=len / 2 {
                let result = executor.exec(format!(
                    "INSERT INTO test (id, other) VALUES ({}, '{}')",
                    i,
                    i * 2
                ));

                //println!("{}", result);
                assert!(result.success)
            }

            let result = executor.exec("SELECT * FROM test".to_string());
            assert_eq!(result.result.fetch_data().unwrap().len(), len);
            if !executor.check_integrity().is_ok() {
                executor.debug(Some("test"));
            }
            assert!(executor.check_integrity().is_ok())
            //
        }
    }

    #[test]
    fn test_specific_reinsert() {
        let mut executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, other Integer)".to_string());
        for i in 1..=10 {
            let res = executor.exec(format!(
                "INSERT INTO test (id, other) VALUES ({}, {})",
                i, 0
            ));
            assert!(res.success);
        }
        for i in 1..=5 {
            executor.exec(format!("DELETE FROM test WHERE id = {}", i));
        }
        println!("after deletion");
        for i in 1..=5 {
            executor.exec(format!(
                "INSERT INTO test (id, other) VALUES ({}, {})",
                i, 0
            ));
            //executor.debug(Some("test"));
        }
        assert!(executor.check_integrity().is_ok());
    }

    #[test]
    fn test_modulo() {
        let mut executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, other Integer)".to_string());

        let test_sizes = [50, 100, 150, 300, 800];
        let modulos = [2, 3, 4, 5, 6];

        for &size in &test_sizes {
            for &modulo in &modulos {
                executor.exec("DELETE FROM test".to_string());

                for i in 1..=size {
                    executor.exec(format!(
                        "INSERT INTO test (id, other) VALUES ({}, {})",
                        i, 0
                    ));
                }
                let result = executor.exec("SELECT * FROM test".to_string());
                println!("{}", result);
                assert_eq!(result.result.fetch_data().unwrap().len(), size);

                let mut count_deleted = 0;
                for i in 1..=size {
                    if i % modulo == 0 {
                        //println!("Deleting {}", i);
                        let result = executor.exec(format!("DELETE FROM test WHERE id = {}", i));
                        if result.success {
                            count_deleted += 1;
                        } else {
                            println!("Failed to delete {}: {}", i, result);
                        }
                        //executor.debug(Some("test"));
                        assert!(result.success);
                    }
                }
                let result = executor.exec("SELECT * FROM test".to_string());
                println!(
                    "After deleting multiples of {}: {} entries left",
                    modulo,
                    result.result.clone().fetch_data().unwrap().len()
                );
                assert_eq!(
                    result.result.fetch_data().unwrap().len(),
                    size - count_deleted
                );
                assert!(executor.check_integrity().is_ok())
            }
        }
    }
    #[test]
    fn test_delete_and_insert_complex() {
        let mut executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, name String)".to_string());

        // Insert initial rows
        for i in 1..=10 {
            executor.exec(format!(
                "INSERT INTO test (id, name) VALUES ({}, 'User{}')",
                i, i
            ));
        }

        executor.debug_lite(Some("test"));

        // Delete some rows
        for i in 1..=5 {
            executor.exec(format!("DELETE FROM test WHERE id = {}", i));
        }

        executor.debug_lite(Some("test"));

        // Insert new rows
        for i in 11..=15 {
            executor.exec(format!(
                "INSERT INTO test (id, name) VALUES ({}, 'NewUser{}')",
                i, i
            ));
        }
        executor.debug_lite(Some("test"));
        // Verify the remaining rows
        let result = executor.exec("SELECT * FROM test".to_string());
        println!("{}", result);
        assert!(result.success);
        assert_eq!(result.result.clone().fetch_data().unwrap().len(), 10);

        // Check the integrity of the data
        for (i, row) in result.result.fetch_data().unwrap().iter().enumerate() {
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
        let mut executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, name String)".to_string());

        // Insert initial rows
        for i in 1..=20 {
            executor.exec(format!(
                "INSERT INTO test (id, name) VALUES ({}, 'User{}')",
                i, i
            ));
        }

        // Delete rows with specific conditions
        executor.exec("DELETE FROM test WHERE id % 2 = 0".to_string());

        // Insert new rows with conditions
        for i in 21..=30 {
            if i % 2 != 0 {
                executor.exec(format!(
                    "INSERT INTO test (id, name) VALUES ({}, 'OddUser{}')",
                    i, i
                ));
            }
        }

        // Verify the remaining rows
        let result = executor.exec("SELECT * FROM test".to_string());
        println!("{}", result);
        assert!(result.success);
        assert_eq!(result.result.fetch_data().unwrap().len(), 25);
        assert!(executor.check_integrity().is_ok());
    }

    #[test]
    fn test_modulo_with_reinserts() {
        let mut executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, other Integer)".to_string());

        let test_sizes = [50, 100, 150, 300, 800];
        let modulos = [2, 3, 4, 5, 6];

        for &size in &test_sizes {
            for &modulo in &modulos {
                println!("mod {}", modulo);
                executor.exec("DELETE FROM test".to_string());
                let result = executor.exec("SELECT * FROM test".to_string());
                assert_eq!(result.result.fetch_data().unwrap().len(), 0);
                //executor.exec("CREATE TABLE test (id Integer, other Integer)".to_string());

                for i in 1..=size {
                    executor.exec(format!(
                        "INSERT INTO test (id, other) VALUES ({}, {})",
                        i, modulo
                    ));
                }
                let result = executor.exec("SELECT * FROM test".to_string());
                assert_eq!(result.result.fetch_data().unwrap().len(), size);

                let mut count_deleted = 0;
                for i in 1..=size {
                    if i % modulo == 0 {
                        let result = executor.exec(format!("DELETE FROM test WHERE id = {}", i));
                        //println!("{}", result);
                        assert!(result.success);
                        count_deleted += 1;
                    }
                }
                let result = executor.exec("SELECT * FROM test".to_string());
                //println!("{}", result);
                assert_eq!(
                    result.result.fetch_data().unwrap().len(),
                    size - count_deleted
                );
                assert!(executor.check_integrity().is_ok());
                for i in 1..=size {
                    if i % modulo == 0 {
                        let result = executor.exec(format!(
                            "INSERT INTO test (id, other) VALUES ({}, {})",
                            i,
                            i * 10
                        ));
                        assert!(result.success);
                    }
                }

                let result = executor.exec("SELECT * FROM test".to_string());
                assert_eq!(result.result.fetch_data().unwrap().len(), size);
                assert!(executor.check_integrity().is_ok());
            }
        }
        executor.debug(Some("test"));
    }
}
