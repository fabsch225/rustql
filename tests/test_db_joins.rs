mod tests {
    use rustql::executor::{QueryExecutor, QueryResult};
    use rustql::parser::JoinOp;
    use rustql::planner::{CompiledQuery, PlanNode};
    use std::collections::HashSet;
    use std::fs;
    use std::ops::{Deref, DerefMut};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    const BTREE_NODE_SIZE: usize = 10;
    static DB_COUNTER: AtomicUsize = AtomicUsize::new(0);

    struct TestExecutor {
        inner: QueryExecutor,
        db_path: String,
    }

    impl Deref for TestExecutor {
        type Target = QueryExecutor;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }

    impl DerefMut for TestExecutor {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.inner
        }
    }

    impl Drop for TestExecutor {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.db_path);
        }
    }

    fn setup_executor() -> TestExecutor {
        setup_executor_with_order(BTREE_NODE_SIZE)
    }

    fn setup_executor_with_order(btree_node_size: usize) -> TestExecutor {
        let idx = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = format!("./default.db.joins.{}.{}.bin", std::process::id(), idx);
        let _ = fs::remove_file(&path);
        TestExecutor {
            inner: QueryExecutor::init(&path, btree_node_size),
            db_path: path,
        }
    }

    fn assert_success(result: QueryResult) {
        assert!(result.success, "Query failed: {:?}", result);
    }

    fn assert_row_count(result: QueryResult, expected: usize) {
        if !result.success {
            println!("{}", result);
        }
        assert!(result.success);
        assert_eq!(
            result.data.fetch().unwrap().len(),
            expected,
            "Row count mismatch"
        );
    }

    fn assert_join_plan_ops(
        exec: &TestExecutor,
        query: &str,
        expected_left: JoinOp,
        expected_right: JoinOp,
    ) {
        let compiled = exec.compile_query(query).unwrap();

        fn find_join(plan: &PlanNode) -> Option<(&JoinOp, &JoinOp)> {
            match plan {
                PlanNode::Join {
                    left_join_op,
                    right_join_op,
                    ..
                } => Some((left_join_op, right_join_op)),
                PlanNode::Project { source, .. } => find_join(source),
                PlanNode::Filter { source, .. } => find_join(source),
                PlanNode::SetOperation { left, right, .. } => {
                    find_join(left).or_else(|| find_join(right))
                }
                _ => None,
            }
        }

        match compiled {
            CompiledQuery::Select(select) => {
                let (left_op, right_op) = find_join(&select.plan).expect("expected join in plan");
                assert_eq!(*left_op, expected_left);
                assert_eq!(*right_op, expected_right);
            }
            _ => panic!("expected compiled select"),
        }
    }

    #[test]
    fn test_basic_inner_join_integers() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE users (id Integer, name String)".into());
        exec.prepare("CREATE TABLE orders (id Integer, user_id Integer, item String)".into());

        exec.prepare("INSERT INTO users (id, name) VALUES (1, 'Alice')".into());
        exec.prepare("INSERT INTO users (id, name) VALUES (2, 'Bob')".into());

        exec.prepare("INSERT INTO orders (id, user_id, item) VALUES (100, 1, 'Book')".into());
        exec.prepare("INSERT INTO orders (id, user_id, item) VALUES (101, 1, 'Pen')".into());
        exec.prepare("INSERT INTO orders (id, user_id, item) VALUES (102, 2, 'Laptop')".into());
        exec.prepare("INSERT INTO orders (id, user_id, item) VALUES (103, 3, 'Phone')".into()); // Orphan order

        let query = "SELECT users.name, orders.item FROM users INNER JOIN orders ON users.id = orders.user_id";
        let result = exec.prepare(query.into());

        assert_row_count(result, 3);
    }

    #[test]
    fn test_inner_join_strings() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE countries (code String, name String)".into());
        exec.prepare("CREATE TABLE cities (name String, country_code String)".into());

        exec.prepare("INSERT INTO countries (code, name) VALUES ('US', 'USA')".into());
        exec.prepare("INSERT INTO countries (code, name) VALUES ('DE', 'Germany')".into());

        exec.prepare("INSERT INTO cities (name, country_code) VALUES ('Berlin', 'DE')".into());
        exec.prepare("INSERT INTO cities (name, country_code) VALUES ('New York', 'US')".into());
        exec.prepare("INSERT INTO cities (name, country_code) VALUES ('Munich', 'DE')".into());
        exec.prepare("INSERT INTO cities (name, country_code) VALUES ('Paris', 'FR')".into()); // No match

        let query = "SELECT cities.name, countries.name FROM cities INNER JOIN countries ON cities.country_code = countries.code";
        let result = exec.prepare(query.into());

        // Should contain Berlin-Germany, New York-USA, Munich-Germany
        assert_row_count(result, 3);
    }

    #[test]
    fn test_join_three_tables() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE students (id Integer, name String)".into());
        exec.prepare("CREATE TABLE enrollments (student_id Integer, course_id Integer)".into());
        exec.prepare("CREATE TABLE courses (id Integer, title String)".into());

        exec.prepare("INSERT INTO students (id, name) VALUES (1, 'Alice')".into());
        exec.prepare("INSERT INTO courses (id, title) VALUES (10, 'Math')".into());
        exec.prepare("INSERT INTO enrollments (student_id, course_id) VALUES (1, 10)".into());

        // Join Students -> Enrollments -> Courses
        let query = "SELECT students.name, courses.title \
                     FROM students \
                     INNER JOIN enrollments ON students.id = enrollments.student_id \
                     INNER JOIN courses ON enrollments.course_id = courses.id";

        let result = exec.prepare(query.into());
        assert_row_count(result, 1);
    }

    #[test]
    fn test_natural_join_success() {
        let mut exec = setup_executor();

        assert_success(exec.prepare("CREATE TABLE L (id Integer, val_l String)".into()));
        assert_success(exec.prepare("CREATE TABLE R (id Integer, val_r String)".into()));

        assert_success(exec.prepare("INSERT INTO L (id, val_l) VALUES (1, 'One')".into()));
        assert_success(exec.prepare("INSERT INTO L (id, val_l) VALUES (2, 'Two')".into()));
        assert_success(exec.prepare("INSERT INTO R (id, val_r) VALUES (1, 'Apple')".into()));
        assert_success(exec.prepare("INSERT INTO R (id, val_r) VALUES (3, 'Orange')".into()));

        let query = "SELECT * FROM L NATURAL JOIN R";
        let result = exec.prepare(query.into());

        assert_row_count(result, 1);
    }

    #[test]
    fn test_natural_join_no_match() {
        let mut exec = setup_executor();

        assert_success(exec.prepare("CREATE TABLE X (key Integer, data String)".into()));
        assert_success(exec.prepare("CREATE TABLE Y (key Integer, value String)".into()));

        assert_success(exec.prepare("INSERT INTO X (key, data) VALUES (10, 'A')".into()));
        assert_success(exec.prepare("INSERT INTO Y (key, value) VALUES (20, 'B')".into()));

        let query = "SELECT * FROM X NATURAL JOIN Y";
        let result = exec.prepare(query.into());

        assert_row_count(result, 0);
    }

    #[test]
    fn test_join_with_filter() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE t1 (id Integer, val Integer)".into());
        exec.prepare("CREATE TABLE t2 (id Integer, val Integer)".into());

        exec.prepare("INSERT INTO t1 (id, val) VALUES (1, 100)".into());
        exec.prepare("INSERT INTO t1 (id, val) VALUES (2, 200)".into());
        exec.prepare("INSERT INTO t2 (id, val) VALUES (1, 100)".into());
        exec.prepare("INSERT INTO t2 (id, val) VALUES (2, 200)".into());

        // Join matches both, but WHERE filters one out
        let query = "SELECT t1.id FROM t1 INNER JOIN t2 ON t1.id = t2.id WHERE t1.val > 150";
        let result = exec.prepare(query.into());

        assert_row_count(result, 1); // Should only match id 2
    }

    #[test]
    fn test_empty_join_result() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE A (id Integer)".into());
        exec.prepare("CREATE TABLE B (id Integer)".into());

        exec.prepare("INSERT INTO A (id) VALUES (1)".into());
        exec.prepare("INSERT INTO B (id) VALUES (2)".into());

        let result = exec.prepare("SELECT * FROM A INNER JOIN B ON A.id = B.id".into());
        assert_row_count(result, 0);
    }

    #[test]
    fn test_subquery_in_from_clause() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE data (val Integer)".into());
        exec.prepare("INSERT INTO data (val) VALUES (10)".into());
        exec.prepare("INSERT INTO data (val) VALUES (20)".into());
        exec.prepare("INSERT INTO data (val) VALUES (30)".into());

        let query = "SELECT * FROM (SELECT * FROM data)";
        let result = exec.prepare(query.into());
        assert_row_count(result, 3);
    }

    #[test]
    fn test_subquery_with_filter() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE numbers (n Integer)".into());
        exec.prepare("INSERT INTO numbers (n) VALUES (1)".into());
        exec.prepare("INSERT INTO numbers (n) VALUES (2)".into());
        exec.prepare("INSERT INTO numbers (n) VALUES (3)".into());
        assert!(
            exec.prepare("INSERT INTO numbers (n) VALUES (4)".into())
                .success
        );

        let query = "SELECT * FROM (SELECT * FROM numbers WHERE n > 2)";
        let result = exec.prepare(query.into());
        println!("{}", result);
        assert_row_count(result, 2);
    }

    #[test]
    fn test_join_on_subquery() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE main (id Integer)".into());
        exec.prepare("CREATE TABLE extra (id Integer, info String)".into());

        exec.prepare("INSERT INTO main (id) VALUES (1)".into());
        exec.prepare("INSERT INTO main (id) VALUES (2)".into());
        exec.prepare("INSERT INTO extra (id, info) VALUES (1, 'keep')".into());
        exec.prepare("INSERT INTO extra (id, info) VALUES (2, 'drop')".into());

        // Join main table with a filtered subquery of extra
        let query = "SELECT main.id FROM main \
                     INNER JOIN (SELECT * FROM extra WHERE info = 'keep') \
                     ON main.id = extra.id";

        let result = exec.prepare(query.into());
        assert_row_count(result, 1); // Only id 1 matches 'keep'
    }

    #[test]
    fn test_nested_subquery_deep() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE t (v Integer)".into());
        exec.prepare("INSERT INTO t (v) VALUES (1)".into());

        // SELECT * FROM (SELECT * FROM (SELECT * FROM t))
        let query = "SELECT * FROM (SELECT * FROM (SELECT * FROM t))";
        let result = exec.prepare(query.into());
        assert_row_count(result, 1);
    }

    #[test]
    fn test_union_all() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE A (val Integer)".into());
        exec.prepare("CREATE TABLE B (val Integer)".into());

        exec.prepare("INSERT INTO A (val) VALUES (1)".into());
        exec.prepare("INSERT INTO B (val) VALUES (2)".into());
        exec.prepare("INSERT INTO B (val) VALUES (1)".into());

        let query = "SELECT val FROM A ALL SELECT val FROM B";
        let result = exec.prepare(query.into());

        assert_row_count(result, 3);
    }

    #[test]
    fn test_intersect() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE A (val Integer)".into());
        exec.prepare("CREATE TABLE B (val Integer)".into());

        exec.prepare("INSERT INTO A (val) VALUES (1)".into());
        exec.prepare("INSERT INTO A (val) VALUES (2)".into());
        exec.prepare("INSERT INTO B (val) VALUES (2)".into());
        exec.prepare("INSERT INTO B (val) VALUES (3)".into());

        // Intersection should be {2}
        let query = "SELECT val FROM A INTERSECT SELECT val FROM B";
        let result = exec.prepare(query.into());

        assert_row_count(result, 1);
    }

    #[test]
    fn test_except() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE A (val Integer)".into());
        exec.prepare("CREATE TABLE B (val Integer)".into());

        exec.prepare("INSERT INTO A (val) VALUES (1)".into());
        exec.prepare("INSERT INTO A (val) VALUES (2)".into());
        exec.prepare("INSERT INTO B (val) VALUES (2)".into());
        exec.prepare("INSERT INTO B (val) VALUES (3)".into());

        // A (1, 2) EXCEPT B (2, 3) -> {1}
        let query = "SELECT val FROM A EXCEPT SELECT val FROM B";
        let result = exec.prepare(query.into());

        assert_row_count(result, 1);
    }

    #[test]
    fn test_set_operation_schema_mismatch() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE A (val Integer)".into());
        exec.prepare("CREATE TABLE B (val Integer, x Integer)".into());

        let query = "SELECT * FROM A UNION SELECT * FROM B";
        let result = exec.prepare(query.into());

        // Should fail due to column count mismatch
        assert!(!result.success);
    }

    #[test]
    fn test_union_of_subqueries() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE T (v Integer)".into());
        exec.prepare("INSERT INTO T (v) VALUES (1)".into());
        exec.prepare("INSERT INTO T (v) VALUES (2)".into());
        exec.prepare("INSERT INTO T (v) VALUES (3)".into());
        exec.prepare("INSERT INTO T (v) VALUES (4)".into());

        // (Select < 3) UNION (Select > 3) -> 1, 2, 4
        let query = "(SELECT * FROM T WHERE v < 3) UNION (SELECT * FROM T WHERE v > 3)";
        let result = exec.prepare(query.into());

        assert_row_count(result, 3);
    }

    #[test]
    fn test_join_plan_marks_index_ops_when_available() {
        let mut exec = setup_executor();

        assert_success(exec.prepare("CREATE TABLE users (id Integer, name String)".into()));
        assert_success(exec.prepare("CREATE TABLE orders (id Integer, user_name String)".into()));
        assert_success(exec.prepare("CREATE INDEX idx_users_name ON users (name)".into()));
        assert_success(
            exec.prepare("CREATE INDEX idx_orders_user_name ON orders (user_name)".into()),
        );

        let compiled = exec
            .compile_query(
                "SELECT users.id FROM users INNER JOIN orders ON users.name = orders.user_name",
            )
            .unwrap();

        fn find_join(plan: &PlanNode) -> Option<(&JoinOp, &JoinOp)> {
            match plan {
                PlanNode::Join {
                    left_join_op,
                    right_join_op,
                    ..
                } => Some((left_join_op, right_join_op)),
                PlanNode::Project { source, .. } => find_join(source),
                PlanNode::Filter { source, .. } => find_join(source),
                PlanNode::SetOperation { left, right, .. } => {
                    find_join(left).or_else(|| find_join(right))
                }
                _ => None,
            }
        }

        match compiled {
            CompiledQuery::Select(select) => {
                let (left_op, right_op) = find_join(&select.plan).expect("expected join in plan");
                assert_eq!(*left_op, JoinOp::Index);
                assert_eq!(*right_op, JoinOp::Index);
            }
            _ => panic!("expected compiled select"),
        }
    }

    #[test]
    fn test_join_execution_uses_manual_indices() {
        let mut exec = setup_executor();

        assert_success(exec.prepare("CREATE TABLE users (id Integer, name String)".into()));
        assert_success(exec.prepare("CREATE TABLE orders (id Integer, user_name String)".into()));

        assert_success(exec.prepare("INSERT INTO users VALUES (1, 'alice')".into()));
        assert_success(exec.prepare("INSERT INTO users VALUES (2, 'bob')".into()));
        assert_success(exec.prepare("INSERT INTO users VALUES (3, 'carol')".into()));

        assert_success(exec.prepare("INSERT INTO orders VALUES (10, 'alice')".into()));
        assert_success(exec.prepare("INSERT INTO orders VALUES (11, 'alice')".into()));
        assert_success(exec.prepare("INSERT INTO orders VALUES (12, 'carol')".into()));

        assert_success(exec.prepare("CREATE INDEX idx_users_name ON users (name)".into()));
        assert_success(
            exec.prepare("CREATE INDEX idx_orders_user_name ON orders (user_name)".into()),
        );

        let query = "SELECT users.id FROM users INNER JOIN orders ON users.name = orders.user_name";
        let result = exec.prepare(query.into());
        //assert_row_count(result, 3);

        let compiled = exec.compile_query(query).unwrap();
        match compiled {
            CompiledQuery::Select(select) => {
                fn find_join(plan: &PlanNode) -> Option<(&JoinOp, &JoinOp)> {
                    match plan {
                        PlanNode::Join {
                            left_join_op,
                            right_join_op,
                            ..
                        } => Some((left_join_op, right_join_op)),
                        PlanNode::Project { source, .. } => find_join(source),
                        PlanNode::Filter { source, .. } => find_join(source),
                        PlanNode::SetOperation { left, right, .. } => {
                            find_join(left).or_else(|| find_join(right))
                        }
                        _ => None,
                    }
                }

                let (left_op, right_op) = find_join(&select.plan).expect("expected join in plan");
                assert_eq!(*left_op, JoinOp::Index);
                assert_eq!(*right_op, JoinOp::Index);
            }
            _ => panic!("expected compiled select"),
        }
    }

    #[test]
    fn test_indexed_join_with_150_inserts_and_deletes_during_process() {
        let mut exec = setup_executor();

        assert_success(exec.prepare("CREATE TABLE a (id Integer, k Integer)".into()));
        assert_success(exec.prepare("CREATE TABLE b (id Integer, k Integer)".into()));
        assert_success(exec.prepare("CREATE INDEX idx_a_k ON a (k)".into()));
        assert_success(exec.prepare("CREATE INDEX idx_b_k ON b (k)".into()));

        let query = "SELECT a.id FROM a INNER JOIN b ON a.k = b.k";
        assert_join_plan_ops(&exec, query, JoinOp::Index, JoinOp::Index);

        let mut a_active = HashSet::new();
        let mut b_active = HashSet::new();

        for i in 1..=150 {
            assert_success(exec.prepare(format!("INSERT INTO a VALUES ({}, {})", i, i)));
            assert_success(exec.prepare(format!("INSERT INTO b VALUES ({}, {})", i, i)));
            a_active.insert(i);
            b_active.insert(i);

            if i % 4 == 0 {
                let del_k = i - 2;
                if b_active.remove(&del_k) {
                    assert_success(exec.prepare(format!("DELETE FROM b WHERE id = {}", del_k)));
                }
            }

            if i % 5 == 0 {
                let del_k = i - 3;
                if a_active.remove(&del_k) {
                    assert_success(exec.prepare(format!("DELETE FROM a WHERE id = {}", del_k)));
                }
            }

            if i % 25 == 0 {
                let expected = a_active.intersection(&b_active).count();
                let result = exec.prepare(query.into());
                assert_row_count(result, expected);
            }
        }

        let expected = a_active.intersection(&b_active).count();
        let result = exec.prepare(query.into());
        assert_row_count(result, expected);
    }

    #[test]
    fn test_drop_index_reverts_join_plan_to_default_scan() {
        let mut exec = setup_executor();

        assert_success(exec.prepare("CREATE TABLE users (id Integer, name String)".into()));
        assert_success(exec.prepare("CREATE TABLE orders2 (id Integer, user String)".into()));
        assert_success(exec.prepare("CREATE INDEX idx_users_name ON users (name)".into()));
        assert_success(exec.prepare("CREATE INDEX idx_orders2_user ON orders2 (user)".into()));

        let query = "SELECT users.id FROM users INNER JOIN orders2 ON users.name = orders2.user";
        assert_join_plan_ops(&exec, query, JoinOp::Index, JoinOp::Index);

        assert_success(exec.prepare("DROP INDEX idx_users_name".into()));
        assert_success(exec.prepare("DROP INDEX idx_orders2_user".into()));

        assert_join_plan_ops(&exec, query, JoinOp::Scan, JoinOp::Scan);
    }

    #[test]
    fn test_large_date_indexed_join_plan_and_results() {
        let mut exec = setup_executor();

        assert_success(exec.prepare("CREATE TABLE events (id Integer, d Date)".into()));
        assert_success(exec.prepare("CREATE TABLE calendar (id Integer, d Date)".into()));
        assert_success(exec.prepare("CREATE INDEX idx_events_d ON events (d)".into()));
        assert_success(exec.prepare("CREATE INDEX idx_calendar_d ON calendar (d)".into()));

        for i in 0..150 {
            let month = (i / 28) + 1;
            let day = (i % 28) + 1;
            let date = format!("2026-{:02}-{:02}", month, day);

            assert_success(exec.prepare(format!(
                "INSERT INTO events VALUES ({}, '{}')",
                i + 1,
                date
            )));
            assert_success(exec.prepare(format!(
                "INSERT INTO calendar VALUES ({}, '{}')",
                i + 1000,
                date
            )));
        }

        let query = "SELECT events.id FROM events INNER JOIN calendar ON events.d = calendar.d";
        assert_join_plan_ops(&exec, query, JoinOp::Index, JoinOp::Index);

        let result = exec.prepare(query.into());
        assert_row_count(result, 150);
    }

    #[test]
    fn test_large_string_and_varchar_indexed_joins() {
        let mut exec = setup_executor_with_order(3);

        // String join
        assert_success(exec.prepare("CREATE TABLE s_left (id Integer, k String)".into()));
        assert_success(exec.prepare("CREATE TABLE s_right (id Integer, k2 String)".into()));
        assert_success(exec.prepare("CREATE INDEX idx_s_left_k ON s_left (k)".into()));
        assert_success(exec.prepare("CREATE INDEX idx_s_right_k2 ON s_right (k2)".into()));

        for i in 1..=40 {
            let val = format!("S{:03}_{}", i, "x".repeat(80));
            assert_success(exec.prepare(format!("INSERT INTO s_left VALUES ({}, '{}')", i, val)));
            assert_success(exec.prepare(format!(
                "INSERT INTO s_right VALUES ({}, '{}')",
                i + 1000,
                val
            )));
        }

        let string_join =
            "SELECT s_left.id FROM s_left INNER JOIN s_right ON s_left.k = s_right.k2";
        assert_join_plan_ops(&exec, string_join, JoinOp::Index, JoinOp::Index);
        assert_row_count(exec.prepare(string_join.into()), 40);

        // Varchar join
        assert_success(exec.prepare("CREATE TABLE v_left (id Integer, k VARCHAR(128))".into()));
        assert_success(exec.prepare("CREATE TABLE v_right (id Integer, k2 VARCHAR(128))".into()));
        assert_success(exec.prepare("CREATE INDEX idx_v_left_k ON v_left (k)".into()));
        assert_success(exec.prepare("CREATE INDEX idx_v_right_k2 ON v_right (k2)".into()));

        for i in 1..=40 {
            let val = format!("V{:03}_{}", i, "y".repeat(90));
            assert_success(exec.prepare(format!("INSERT INTO v_left VALUES ({}, '{}')", i, val)));
            assert_success(exec.prepare(format!(
                "INSERT INTO v_right VALUES ({}, '{}')",
                i + 2000,
                val
            )));
        }

        let varchar_join =
            "SELECT v_left.id FROM v_left INNER JOIN v_right ON v_left.k = v_right.k2";
        assert_join_plan_ops(&exec, varchar_join, JoinOp::Index, JoinOp::Index);
        assert_row_count(exec.prepare(varchar_join.into()), 40);
    }

    #[test]
    fn test_complex_natural_join_and_filter() {
        let mut exec = setup_executor();

        assert!(exec.prepare("CREATE TABLE A (id Integer)".into()).success);
        assert!(
            exec.prepare("CREATE TABLE B (id Integer, score Integer)".into())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE C (id Integer, pass Boolean)".into())
                .success
        );

        assert!(exec.prepare("INSERT INTO A VALUES (1)".into()).success);
        assert!(exec.prepare("INSERT INTO A VALUES (2)".into()).success);

        assert!(exec.prepare("INSERT INTO B VALUES (1, 90)".into()).success);
        assert!(exec.prepare("INSERT INTO B VALUES (2, 40)".into()).success);

        assert!(
            exec.prepare("INSERT INTO C VALUES (1, true)".into())
                .success
        );
        assert!(
            exec.prepare("INSERT INTO C VALUES (2, false)".into())
                .success
        );

        let query = "SELECT A.id FROM A NATURAL JOIN B NATURAL JOIN C WHERE B.score > 50";

        let result = exec.prepare(query.into());
        println!("{}", result);
        assert_row_count(result, 1);
    }

    #[test]
    fn test_join_subquery_with_union() {
        let mut exec = setup_executor();

        assert_success(exec.prepare("CREATE TABLE Employees (id Integer, name String)".into()));
        assert_success(exec.prepare("CREATE TABLE Sales (emp_id Integer, region String)".into()));
        assert_success(
            exec.prepare("CREATE TABLE Marketing (emp_id Integer, region String)".into()),
        );

        assert_success(exec.prepare("INSERT INTO Employees VALUES (1, 'Alice')".into()));
        assert_success(exec.prepare("INSERT INTO Employees VALUES (2, 'Bob')".into()));
        assert_success(exec.prepare("INSERT INTO Employees VALUES (3, 'Charlie')".into()));

        assert_success(exec.prepare("INSERT INTO Sales VALUES (1, 'East')".into()));
        assert_success(exec.prepare("INSERT INTO Sales VALUES (2, 'West')".into()));

        assert_success(exec.prepare("INSERT INTO Marketing VALUES (1, 'North')".into()));
        assert_success(exec.prepare("INSERT INTO Marketing VALUES (3, 'South')".into()));

        let union_query = "SELECT emp_id FROM Sales UNION SELECT emp_id FROM Marketing";

        let full_query = format!(
            "SELECT name, emp_id FROM Employees JOIN ({}) ON id = emp_id",
            union_query
        );

        let result = exec.prepare(full_query.into());

        assert_row_count(result, 3);
    }

    #[test]
    fn test_intersect_of_joined_results() {
        let mut exec = setup_executor();

        assert_success(exec.prepare("CREATE TABLE Users (id Integer, name String)".into()));
        assert_success(exec.prepare("CREATE TABLE Groups (id Integer, name String)".into()));
        assert_success(
            exec.prepare("CREATE TABLE GroupA (user_id Integer, group_id Integer)".into()),
        );
        assert_success(
            exec.prepare("CREATE TABLE GroupB (user_id Integer, group_id Integer)".into()),
        );

        assert_success(exec.prepare("INSERT INTO Users VALUES (1, 'Alice')".into()));
        assert_success(exec.prepare("INSERT INTO Users VALUES (2, 'Bob')".into()));
        assert_success(exec.prepare("INSERT INTO Users VALUES (3, 'Charlie')".into()));
        assert_success(exec.prepare("INSERT INTO Groups VALUES (10, 'Admin')".into()));

        assert_success(exec.prepare("INSERT INTO GroupA VALUES (1, 10)".into()));
        assert_success(exec.prepare("INSERT INTO GroupA VALUES (2, 10)".into()));
        assert_success(exec.prepare("INSERT INTO GroupB VALUES (1, 10)".into()));
        assert_success(exec.prepare("INSERT INTO GroupB VALUES (3, 10)".into()));
        let query_a = "SELECT Users.name FROM Users JOIN GroupA ON Users.id = GroupA.user_id JOIN Groups ON GroupA.group_id = Groups.id WHERE Groups.name = 'Admin'";

        let query_b = "SELECT Users.name FROM Users JOIN GroupB ON Users.id = GroupB.user_id JOIN Groups ON GroupB.group_id = Groups.id WHERE Groups.name = 'Admin'";

        let full_query = format!("({}) INTERSECT ({})", query_a, query_b);

        let result = exec.prepare(full_query.into());
        assert_row_count(result, 1);
    }

    #[test]
    fn test_complex_nested_filter_on_join() {
        let mut exec = setup_executor();

        assert_success(exec.prepare("CREATE TABLE T1 (id Integer, value Integer)".into()));
        assert_success(exec.prepare("CREATE TABLE T2 (t1_id Integer, score Integer)".into()));
        assert_success(exec.prepare("CREATE TABLE T3 (id Integer, score Integer)".into()));

        assert_success(exec.prepare("INSERT INTO T1 VALUES (1, 100)".into()));
        assert_success(exec.prepare("INSERT INTO T1 VALUES (2, 200)".into()));
        assert_success(exec.prepare("INSERT INTO T2 VALUES (1, 90)".into()));
        assert_success(exec.prepare("INSERT INTO T2 VALUES (2, 95)".into()));
        assert_success(exec.prepare("INSERT INTO T3 VALUES (1, 110)".into()));
        assert_success(exec.prepare("INSERT INTO T3 VALUES (2, 915)".into()));

        let inner_query = "SELECT T1.id FROM T1 JOIN T2 ON T1.id = T2.t1_id WHERE T2.score > 90";

        let full_query = format!(
            "SELECT T1.id FROM ({}) JOIN T3 ON T3.id = T1.id WHERE T3.value > 150",
            inner_query
        );

        let result = exec.prepare(full_query.into());
        println!("{}", result);
        // ID=1 (100, 90) -> Fails inner filter (score > 90)
        // ID=2 (200, 95) -> Passes inner filter (score > 90). Passes outer filter (value > 150).
        assert_row_count(result, 1);
    }

    #[test]
    fn test_project_subset() {
        let mut exec = setup_executor();

        assert_success(exec.prepare("CREATE TABLE t (a Integer, b Integer, c Integer)".into()));
        assert_success(exec.prepare("INSERT INTO t VALUES (1, 2, 3)".into()));

        let result = exec.prepare("SELECT b, a FROM t".into());
        assert_success(result);
    }

    #[test]
    fn test_delete_single_row() {
        let mut exec = setup_executor();

        assert_success(exec.prepare("CREATE TABLE test (id Integer, name String)".to_string()));
        assert_success(exec.prepare("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string()));
        assert_success(exec.prepare("INSERT INTO test (id, name) VALUES (2, 'Bob')".to_string()));

        let delete_result = exec.prepare("DELETE FROM test WHERE id = 1".to_string());
        assert_success(delete_result);

        let result = exec.prepare("SELECT name FROM test".to_string());
        assert_row_count(result, 1);
    }

    #[test]
    fn test_large_union_all() {
        let mut exec = setup_executor();
        exec.prepare("CREATE TABLE A (val Integer)".into());
        exec.prepare("CREATE TABLE B (val Integer)".into());

        // A: 1..1000
        // B: 500..1500 (overlap of 500..1000)
        for i in 1..=1000 {
            exec.prepare(format!("INSERT INTO A (val) VALUES ({})", i));
        }
        for i in 500..=1500 {
            exec.prepare(format!("INSERT INTO B (val) VALUES ({})", i));
        }

        let query = "SELECT val FROM A UNION ALL SELECT val FROM B";
        let result = exec.prepare(query.into());

        // 1000 rows from A + 1001 rows from B = 2001
        assert_row_count(result, 2001);
    }

    #[test]
    fn test_large_union_distinct() {
        let mut exec = setup_executor();
        exec.prepare("CREATE TABLE A (val Integer)".into());
        exec.prepare("CREATE TABLE B (val Integer)".into());

        for i in 1..=5000 {
            exec.prepare(format!("INSERT INTO A (val) VALUES ({})", i));
        }
        for i in 4000..=9000 {
            exec.prepare(format!("INSERT INTO B (val) VALUES ({})", i));
        }

        let query = "SELECT val FROM A UNION SELECT val FROM B";
        let result = exec.prepare(query.into());

        // Distinct set = 1..9000 → 9000 values
        assert_row_count(result, 9000);
    }

    #[test]
    fn test_large_intersect() {
        let mut exec = setup_executor();
        exec.prepare("CREATE TABLE A (val Integer)".into());
        exec.prepare("CREATE TABLE B (val Integer)".into());

        // A: 1..10k
        // B: 5k..15k
        for i in 1..=10000 {
            exec.prepare(format!("INSERT INTO A (val) VALUES ({})", i));
        }
        for i in 5000..=15000 {
            exec.prepare(format!("INSERT INTO B (val) VALUES ({})", i));
        }

        let query = "SELECT val FROM A INTERSECT SELECT val FROM B";
        let result = exec.prepare(query.into());

        // Intersection = 5000..10000 (inclusive) → 5001 rows
        assert_row_count(result, 5001);
    }

    #[test]
    fn test_large_except() {
        let mut exec = setup_executor();
        exec.prepare("CREATE TABLE A (val Integer)".into());
        exec.prepare("CREATE TABLE B (val Integer)".into());

        // A: 1..8000
        // B: 4000..12000
        for i in 1..=8000 {
            exec.prepare(format!("INSERT INTO A (val) VALUES ({})", i));
        }
        for i in 4000..=12000 {
            exec.prepare(format!("INSERT INTO B (val) VALUES ({})", i));
        }

        let query = "SELECT val FROM A EXCEPT SELECT val FROM B";
        let result = exec.prepare(query.into());

        // A minus B = 1..3999 → 3999 rows
        assert_row_count(result, 3999);
    }

    #[test]
    fn test_large_inner_join() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE A (id Integer, v Integer)".into());
        exec.prepare("CREATE TABLE B (id Integer, x Integer)".into());

        // A: ids 1..10k
        // B: ids 5000..15k  => overlap: 5000..10000 → 5001 rows
        for i in 1..=10000 {
            exec.prepare(format!("INSERT INTO A VALUES ({}, {})", i, i * 2));
        }
        for i in 5000..=15000 {
            exec.prepare(format!("INSERT INTO B VALUES ({}, {})", i, i * 3));
        }

        let query = "SELECT A.id FROM A JOIN B ON A.id = B.id";
        let result = exec.prepare(query.into());

        assert_row_count(result, 5001);
    }

    #[test]
    fn test_large_left_join_sparse_matches() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE A (id Integer)".into());
        exec.prepare("CREATE TABLE B (id Integer)".into());

        for i in 1..=10000 {
            exec.prepare(format!("INSERT INTO A VALUES ({})", i));
        }

        for i in (100..=10000).step_by(100) {
            exec.prepare(format!("INSERT INTO B VALUES ({})", i));
        }

        let query = "SELECT A.id FROM A INNER JOIN B ON A.id = B.id";
        let result = exec.prepare(query.into());
        assert_row_count(result, 100);
    }

    #[test]
    fn test_large_natural_join_three_tables() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE A (id Integer)".into());
        exec.prepare("CREATE TABLE B (id Integer)".into());
        exec.prepare("CREATE TABLE C (id Integer)".into());

        // IDs 1..5000 in each table
        for i in 1..=5000 {
            exec.prepare(format!("INSERT INTO A VALUES ({})", i));
            exec.prepare(format!("INSERT INTO B VALUES ({})", i));
            exec.prepare(format!("INSERT INTO C VALUES ({})", i));
        }

        let query = "SELECT A.id FROM A NATURAL JOIN B NATURAL JOIN C";
        let result = exec.prepare(query.into());

        assert_row_count(result, 5000);
    }

    #[test]
    fn test_large_mixed_operations() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE A (id Integer)".into());
        exec.prepare("CREATE TABLE B (id Integer)".into());
        exec.prepare("CREATE TABLE C (id Integer)".into());
        let start = Instant::now();
        // Insert 1..10000 into A
        for i in 1..=10000 {
            exec.prepare(format!("INSERT INTO A VALUES ({})", i));
        }
        // Insert 5000..20000 into B
        for i in 5000..=20000 {
            exec.prepare(format!("INSERT INTO B VALUES ({})", i));
        }
        // Insert 8000..12000 into C
        for i in 8000..=12000 {
            exec.prepare(format!("INSERT INTO C VALUES ({})", i));
        }
        let duration = start.elapsed();
        println!("Time elapsed: {:?}", duration);
        let query = r#"
        SELECT id FROM (
            SELECT id FROM A
            INTERSECT
            SELECT id FROM B
        ) INTERSECT SELECT id FROM C
    "#;
        let start = Instant::now();
        let result = exec.prepare(query.into());
        assert_row_count(result, 2001);
        let duration = start.elapsed();
        println!("Time elapsed: {:?}", duration);
        // Intersection of:
        //   A: 1..10000
        //   B: 5000..20000 → 5000..10000
        //   C: 8000..12000 → final = 8000..10000 (2001 rows)
    }

    #[test]
    fn test_pk_join_integer_exact_overlap_small() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE A (id Integer, v Integer)".into());
        exec.prepare("CREATE TABLE B (id Integer, x Integer)".into());

        for i in 1..=5 {
            exec.prepare(format!("INSERT INTO A VALUES ({}, {})", i, i * 10));
        }
        for i in 3..=7 {
            exec.prepare(format!("INSERT INTO B VALUES ({}, {})", i, i * 100));
        }

        let result = exec.prepare("SELECT A.id FROM A JOIN B ON A.id = B.id".into());
        assert_row_count(result, 3);
    }

    #[test]
    fn test_pk_join_integer_no_overlap() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE A (id Integer, v Integer)".into());
        exec.prepare("CREATE TABLE B (id Integer, x Integer)".into());

        for i in 1..=5 {
            exec.prepare(format!("INSERT INTO A VALUES ({}, {})", i, i));
        }
        for i in 10..=15 {
            exec.prepare(format!("INSERT INTO B VALUES ({}, {})", i, i));
        }

        let result = exec.prepare("SELECT A.id FROM A JOIN B ON A.id = B.id".into());
        assert_row_count(result, 0);
    }

    #[test]
    fn test_pk_join_string_keys() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE A (id String, v Integer)".into());
        exec.prepare("CREATE TABLE B (id String, x Integer)".into());

        exec.prepare("INSERT INTO A VALUES ('k1', 1)".into());
        exec.prepare("INSERT INTO A VALUES ('k2', 2)".into());
        exec.prepare("INSERT INTO A VALUES ('k3', 3)".into());

        exec.prepare("INSERT INTO B VALUES ('k2', 20)".into());
        exec.prepare("INSERT INTO B VALUES ('k3', 30)".into());
        exec.prepare("INSERT INTO B VALUES ('k4', 40)".into());

        let result = exec.prepare("SELECT A.id FROM A JOIN B ON A.id = B.id".into());
        assert_row_count(result, 2);
    }

    #[test]
    fn test_pk_join_integer_sparse_keys() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE A (id Integer, v Integer)".into());
        exec.prepare("CREATE TABLE B (id Integer, x Integer)".into());

        exec.prepare("INSERT INTO A VALUES (10, 1)".into());
        exec.prepare("INSERT INTO A VALUES (100, 2)".into());
        exec.prepare("INSERT INTO A VALUES (1000, 3)".into());

        exec.prepare("INSERT INTO B VALUES (1, 11)".into());
        exec.prepare("INSERT INTO B VALUES (10, 22)".into());
        exec.prepare("INSERT INTO B VALUES (1000, 33)".into());

        let result = exec.prepare("SELECT A.id FROM A JOIN B ON A.id = B.id".into());
        assert_row_count(result, 2);
    }

    #[test]
    fn test_pk_join_with_where_filter_small() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE A (id Integer, v Integer)".into());
        exec.prepare("CREATE TABLE B (id Integer, x Integer)".into());

        exec.prepare("INSERT INTO A VALUES (1, 10)".into());
        exec.prepare("INSERT INTO A VALUES (2, 20)".into());
        exec.prepare("INSERT INTO A VALUES (3, 30)".into());

        exec.prepare("INSERT INTO B VALUES (1, 100)".into());
        exec.prepare("INSERT INTO B VALUES (2, 200)".into());
        exec.prepare("INSERT INTO B VALUES (3, 300)".into());

        let result = exec.prepare("SELECT A.id FROM A JOIN B ON A.id = B.id WHERE A.v > 15".into());
        assert_row_count(result, 2);
    }

    #[test]
    fn test_non_pk_join_integer_column() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE A (id Integer, v Integer)".into());
        exec.prepare("CREATE TABLE B (id Integer, v Integer)".into());

        exec.prepare("INSERT INTO A VALUES (1, 10)".into());
        exec.prepare("INSERT INTO A VALUES (2, 20)".into());
        exec.prepare("INSERT INTO A VALUES (3, 20)".into());

        exec.prepare("INSERT INTO B VALUES (11, 20)".into());
        exec.prepare("INSERT INTO B VALUES (12, 30)".into());

        let result = exec.prepare("SELECT A.id FROM A JOIN B ON A.v = B.v".into());
        assert_row_count(result, 2);
    }

    #[test]
    fn test_non_pk_join_duplicate_multiplicity() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE A (id Integer, v Integer)".into());
        exec.prepare("CREATE TABLE B (id Integer, v Integer)".into());

        exec.prepare("INSERT INTO A VALUES (1, 5)".into());
        exec.prepare("INSERT INTO A VALUES (2, 5)".into());

        exec.prepare("INSERT INTO B VALUES (10, 5)".into());
        exec.prepare("INSERT INTO B VALUES (11, 5)".into());
        exec.prepare("INSERT INTO B VALUES (12, 5)".into());

        let result = exec.prepare("SELECT A.id FROM A JOIN B ON A.v = B.v".into());
        assert_row_count(result, 6);
    }

    #[test]
    fn test_mixed_chain_join_pk_then_nonpk() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE A (id Integer, v Integer)".into());
        exec.prepare("CREATE TABLE B (id Integer, v Integer)".into());
        exec.prepare("CREATE TABLE C (cid Integer, bv Integer)".into());

        exec.prepare("INSERT INTO A VALUES (1, 100)".into());
        exec.prepare("INSERT INTO A VALUES (2, 200)".into());

        exec.prepare("INSERT INTO B VALUES (1, 900)".into());
        exec.prepare("INSERT INTO B VALUES (2, 901)".into());

        exec.prepare("INSERT INTO C VALUES (10, 900)".into());

        let query = "SELECT A.id FROM A JOIN B ON A.id = B.id JOIN C ON B.v = C.bv";
        let result = exec.prepare(query.into());
        assert_row_count(result, 1);
    }

    #[test]
    fn test_join_subquery_on_non_pk_column() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE A (id Integer, v Integer)".into());
        exec.prepare("CREATE TABLE B (id Integer, v Integer)".into());

        exec.prepare("INSERT INTO A VALUES (1, 10)".into());
        exec.prepare("INSERT INTO A VALUES (2, 20)".into());
        exec.prepare("INSERT INTO A VALUES (3, 30)".into());

        exec.prepare("INSERT INTO B VALUES (11, 5)".into());
        exec.prepare("INSERT INTO B VALUES (12, 20)".into());
        exec.prepare("INSERT INTO B VALUES (13, 30)".into());

        let query = "SELECT A.id FROM A INNER JOIN (SELECT * FROM B WHERE v > 10) ON A.v = B.v";
        let result = exec.prepare(query.into());
        assert_row_count(result, 2);
    }

    #[test]
    fn test_natural_join_three_tables_small_pk_chain() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE A (id Integer, x Integer)".into());
        exec.prepare("CREATE TABLE B (id Integer, y Integer)".into());
        exec.prepare("CREATE TABLE C (id Integer, z Integer)".into());

        exec.prepare("INSERT INTO A VALUES (1, 10)".into());
        exec.prepare("INSERT INTO A VALUES (2, 20)".into());
        exec.prepare("INSERT INTO A VALUES (3, 30)".into());

        exec.prepare("INSERT INTO B VALUES (2, 200)".into());
        exec.prepare("INSERT INTO B VALUES (3, 300)".into());
        exec.prepare("INSERT INTO B VALUES (4, 400)".into());

        exec.prepare("INSERT INTO C VALUES (3, 3000)".into());
        exec.prepare("INSERT INTO C VALUES (4, 4000)".into());

        let result = exec.prepare("SELECT A.id FROM A NATURAL JOIN B NATURAL JOIN C".into());
        assert_row_count(result, 1);
    }

    #[test]
    fn test_nested_setops_and_joins_50k() {
        let mut exec = setup_executor();
        let start = Instant::now();
        exec.prepare("CREATE TABLE A (id Integer, v Integer)".into());
        exec.prepare("CREATE TABLE B (id Integer, v Integer)".into());
        exec.prepare("CREATE TABLE C (id Integer, v Integer)".into());
        exec.prepare("CREATE TABLE D (id Integer, v Integer)".into());

        // A: 1..50000
        for i in 1..=10000 {
            exec.prepare(format!("INSERT INTO A VALUES ({}, {})", i, i * 2));
        }
        // B: 25000..75000
        for i in 2500..=17500 {
            exec.prepare(format!("INSERT INTO B VALUES ({}, {})", i, i * 3));
        }
        // C: 40000..90000
        for i in 4000..=19000 {
            exec.prepare(format!("INSERT INTO C VALUES ({}, {})", i, i * 4));
        }
        // D: 10000..60000
        for i in 1000..=16000 {
            exec.prepare(format!("INSERT INTO D VALUES ({}, {})", i, i * 5));
        }
        let duration = start.elapsed();
        println!("Time elapsed: {:?}", duration);
        // Expected:
        //   A ∩ B = 25000..50000
        //   C ∩ D = 40000..60000 → 40000..50000 when intersected with A∩B
        //   final = 40000..50000 = 10001 rows

        let query = r#"
        SELECT A.id FROM (
            SELECT A.id FROM A INNER JOIN D ON D.id = A.id
            UNION
            SELECT B.id FROM B
        ) INTERSECT SELECT C.id FROM C
    "#;
        let start = Instant::now();
        let result = exec.prepare(query.into());

        assert_row_count(result, 13501);
        let duration = start.elapsed();
        println!("Time elapsed: {:?}", duration);
    }

    #[test]
    fn test_set_minus() {
        let mut exec = setup_executor();

        exec.prepare("CREATE TABLE A (id Integer)".into());
        exec.prepare("CREATE TABLE B (id Integer)".into());

        for i in 1..=100 {
            exec.prepare(format!("INSERT INTO A VALUES ({})", i));
        }
        for i in 50..=150 {
            exec.prepare(format!("INSERT INTO B VALUES ({})", i));
        }

        let query = "SELECT id FROM A EXCEPT SELECT id FROM B";
        let result = exec.prepare(query.into());

        assert_row_count(result, 49);

        let query = "SELECT id FROM A MINUS SELECT id FROM B";
        let result = exec.prepare(query.into());

        assert_row_count(result, 49);
    }
}
