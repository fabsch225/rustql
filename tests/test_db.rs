#[cfg(test)]
mod tests {
    use rustql::executor::Executor;
    use rustql::serializer::Serializer;

    const BTREE_NODE_SIZE: usize = 3;

    #[test]
    fn test_create_table() {
        let executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        let result = executor.exec("CREATE TABLE test (id Integer, name String)".to_string());
        assert!(result.success);
    }

    #[test]
    fn test_insert_single_row() {
        let executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, name String)".to_string());
        let result = executor.exec("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string());
        assert!(result.success);
    }

    #[test]
    fn test_insert_multiple_rows() {
        let executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, name String)".to_string());
        executor.exec("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string());
        let result = executor.exec("INSERT INTO test (id, name) VALUES (2, 'Bob')".to_string());
        assert!(result.success);
    }

    #[test]
    fn test_select_all() {
        let executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, name String)".to_string());
        executor.exec("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string());
        executor.exec("INSERT INTO test (id, name) VALUES (2, 'Bob')".to_string());
        let result = executor.exec("SELECT * FROM test".to_string());
        assert!(result.success);
        assert_eq!(result.result.data.len(), 2);
        assert_eq!(result.result.data[0][0..10], vec![0, 0, 0, 1u8, 0, b'A', b'l', b'i', b'c', b'e']);
        assert_eq!(result.result.data[1][0..8], vec![0, 0, 0, 2u8, 0, b'B', b'o', b'b']);
    }

    #[test]
    fn test_select_with_condition() {
        let executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, name String)".to_string());
        executor.exec("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string());
        executor.exec("INSERT INTO test (id, name) VALUES (2, 'Bob')".to_string());
        let result = executor.exec("SELECT * FROM test WHERE id <= 1".to_string());
        assert!(result.success);
        assert_eq!(result.result.data.len(), 1);
        assert_eq!(result.result.data[0][0..10], vec![0, 0, 0, 1, 0, b'A', b'l', b'i', b'c', b'e']);
    }

    #[test]
    fn test_delete_single_row() {
        let executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, name String)".to_string());
        executor.exec("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string());
        executor.exec("INSERT INTO test (id, name) VALUES (2, 'Bob')".to_string());
        let delete_result = executor.exec("DELETE FROM test WHERE id = 1".to_string());
        assert!(delete_result.success);
        let result = executor.exec("SELECT name FROM test".to_string());
        assert_eq!(result.result.data.len(), 1);
        assert_eq!(result.result.data[0][0..3], vec![b'B', b'o', b'b']);
    }

    #[test]
    fn test_delete_all_rows() {
        let executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, name String)".to_string());
        executor.exec("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string());
        executor.exec("INSERT INTO test (id, name) VALUES (2, 'Bob')".to_string());
        //executor.exec("INSERT INTO test (id, name) VALUES (4, 'Charlie')".to_string());
        executor.exec("DELETE FROM test WHERE id <= 2".to_string());
        let result = executor.exec("SELECT * FROM test".to_string());
        println!("{}", result);
        assert!(result.success);
        assert_eq!(result.result.data.len(), 0);
    }

    #[test]
    fn test_insert_and_select_large_dataset() {
        let executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, name String)".to_string());
        for i in 1..=100 {
            executor.exec(format!("INSERT INTO test (id, name) VALUES ({}, 'User{}')", i, i));
        }
        let result = executor.exec("SELECT * FROM test".to_string());
        assert!(result.success);
        assert_eq!(result.result.data.len(), 100);
        for (i, row) in result.result.data.iter().enumerate() {
            let expected_name = format!("User{}", i + 1).as_bytes().to_vec();
            assert_eq!(row[0..5], [0u8, 0, 0, (i + 1) as u8, 0]);
            assert_eq!(row[5..10], expected_name[0..5]);
        }
    }

    #[test]
    fn test_delete_and_reinsert_with_loops() {
        let executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        let len = 160;
        //for len in (0..250).step_by(10) {
            executor.exec("CREATE TABLE test (id Integer, other Integer)".to_string());
            for i in 1..=len {
                executor.exec(format!("INSERT INTO test (id, other) VALUES ({}, {})", i, 0));
            }
            let result = executor.exec(format!("SELECT * FROM test WHERE id <= {}", len));
            assert!(result.success);
            assert_eq!(result.result.data.len(), len);
            println!("{}", result);
            let result = executor.exec(format!("SELECT * FROM test WHERE id <= {}", len / 2));
            assert!(result.success);
            assert_eq!(result.result.data.len(), len / 2);
            //println!("{}", result);
            //println!("---");
            let result = executor.exec(format!("DELETE FROM test WHERE id <= {}", len / 2));
            assert!(result.success);
            let result = executor.exec("SELECT * FROM test".to_string());
            assert!(result.success);
            //println!("{}", result);
            //println!("---");
            assert_eq!(result.result.data.len(), len / 2);
            for (i, row) in result.result.data.iter().enumerate() {
                let expected_id = i + len / 2 + 2;
                //assert_eq!(Serializer::bytes_to_int(row[0..5].try_into().unwrap()), expected_id as i32);
            }

            for i in 1..=len / 2 {
                let result = executor.exec(format!("INSERT INTO test (id, other) VALUES ({}, '{}')", i, i * 2));
                //println!("{}", result);
                assert!(result.success)
            }

            let result = executor.exec("SELECT * FROM test".to_string());
            //println!("{}", result);
            assert_eq!(result.result.data.len(), len);
       //
       // }
    }

    #[test]
    fn test_mod_3() {
        let executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
        executor.exec("CREATE TABLE test (id Integer, other Integer)".to_string());
        for i in 1..=120 {
            executor.exec(format!("INSERT INTO test (id, other) VALUES ({}, {})", i, 0));
        }
        let result = executor.exec("SELECT * FROM test".to_string());
        //println!("{}", result);
        assert_eq!(result.result.data.len(), 120);

        for i in 1..=40 {
            let result = executor.exec(format!("DELETE FROM test WHERE id = {}", i * 3));
            if !result.success {
                println!("{}", result);
            }
            assert!(result.success);
        }
        let result = executor.exec("SELECT * FROM test".to_string());
        println!("{}", result);
        assert_eq!(result.result.data.len(), 80);
    }
}
