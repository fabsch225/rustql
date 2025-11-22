mod tests {
    use rustql::executor::{Executor, QueryResult};
    use rustql::serializer::Serializer;
    use std::fs;

    const BTREE_NODE_SIZE: usize = 3;

    fn setup_executor() -> Executor {
        Executor::init("default.db.bin", BTREE_NODE_SIZE)
    }

    fn assert_success(result: QueryResult) {
        assert!(result.success, "Query failed: {:?}", result);
    }

    fn assert_row_count(result: QueryResult, expected: usize) {
        if !result.success {
            println!("{}", result);
        }
        assert!(result.success);
        assert_eq!(result.result.get_data().unwrap().len(), expected, "Row count mismatch");
    }

    #[test]
    fn test_basic_inner_join_integers() {
        let mut exec = setup_executor();

        exec.exec("CREATE TABLE users (id Integer, name String)".into());
        exec.exec("CREATE TABLE orders (id Integer, user_id Integer, item String)".into());

        exec.exec("INSERT INTO users (id, name) VALUES (1, 'Alice')".into());
        exec.exec("INSERT INTO users (id, name) VALUES (2, 'Bob')".into());

        exec.exec("INSERT INTO orders (id, user_id, item) VALUES (100, 1, 'Book')".into());
        exec.exec("INSERT INTO orders (id, user_id, item) VALUES (101, 1, 'Pen')".into());
        exec.exec("INSERT INTO orders (id, user_id, item) VALUES (102, 2, 'Laptop')".into());
        exec.exec("INSERT INTO orders (id, user_id, item) VALUES (103, 3, 'Phone')".into()); // Orphan order
        
        let query = "SELECT users.name, orders.item FROM users INNER JOIN orders ON users.id = orders.user_id";
        let result = exec.exec(query.into());

        assert_row_count(result, 3);

    }

    #[test]
    fn test_inner_join_strings() {
        let mut exec = setup_executor();

        exec.exec("CREATE TABLE countries (code String, name String)".into());
        exec.exec("CREATE TABLE cities (name String, country_code String)".into());

        exec.exec("INSERT INTO countries (code, name) VALUES ('US', 'USA')".into());
        exec.exec("INSERT INTO countries (code, name) VALUES ('DE', 'Germany')".into());

        exec.exec("INSERT INTO cities (name, country_code) VALUES ('Berlin', 'DE')".into());
        exec.exec("INSERT INTO cities (name, country_code) VALUES ('New York', 'US')".into());
        exec.exec("INSERT INTO cities (name, country_code) VALUES ('Munich', 'DE')".into());
        exec.exec("INSERT INTO cities (name, country_code) VALUES ('Paris', 'FR')".into()); // No match

        let query = "SELECT cities.name, countries.name FROM cities INNER JOIN countries ON cities.country_code = countries.code";
        let result = exec.exec(query.into());

        // Should contain Berlin-Germany, New York-USA, Munich-Germany
        assert_row_count(result, 3);

    }

    #[test]
    fn test_join_three_tables() {
        let mut exec = setup_executor();

        exec.exec("CREATE TABLE students (id Integer, name String)".into());
        exec.exec("CREATE TABLE enrollments (student_id Integer, course_id Integer)".into());
        exec.exec("CREATE TABLE courses (id Integer, title String)".into());

        exec.exec("INSERT INTO students (id, name) VALUES (1, 'Alice')".into());
        exec.exec("INSERT INTO courses (id, title) VALUES (10, 'Math')".into());
        exec.exec("INSERT INTO enrollments (student_id, course_id) VALUES (1, 10)".into());

        // Join Students -> Enrollments -> Courses
        let query = "SELECT students.name, courses.title \
                     FROM students \
                     INNER JOIN enrollments ON students.id = enrollments.student_id \
                     INNER JOIN courses ON enrollments.course_id = courses.id";

        let result = exec.exec(query.into());
        assert_row_count(result, 1);

    }

    #[test]
    fn test_natural_join_success() {
        let mut exec = setup_executor();

        assert_success(exec.exec("CREATE TABLE L (id Integer, val_l String)".into()));
        assert_success(exec.exec("CREATE TABLE R (id Integer, val_r String)".into()));

        assert_success(exec.exec("INSERT INTO L (id, val_l) VALUES (1, 'One')".into()));
        assert_success(exec.exec("INSERT INTO L (id, val_l) VALUES (2, 'Two')".into()));
        assert_success(exec.exec("INSERT INTO R (id, val_r) VALUES (1, 'Apple')".into()));
        assert_success(exec.exec("INSERT INTO R (id, val_r) VALUES (3, 'Orange')".into()));

        let query = "SELECT * FROM L NATURAL JOIN R";
        let result = exec.exec(query.into());

        assert_row_count(result, 1);
    }

    #[test]
    fn test_natural_join_no_match() {
        let mut exec = setup_executor();

        assert_success(exec.exec("CREATE TABLE X (key Integer, data String)".into()));
        assert_success(exec.exec("CREATE TABLE Y (key Integer, value String)".into()));

        assert_success(exec.exec("INSERT INTO X (key, data) VALUES (10, 'A')".into()));
        assert_success(exec.exec("INSERT INTO Y (key, value) VALUES (20, 'B')".into()));

        let query = "SELECT * FROM X NATURAL JOIN Y";
        let result = exec.exec(query.into());

        assert_row_count(result, 0);
    }


    #[test]
    fn test_join_with_filter() {
        let mut exec = setup_executor();

        exec.exec("CREATE TABLE t1 (id Integer, val Integer)".into());
        exec.exec("CREATE TABLE t2 (id Integer, val Integer)".into());

        exec.exec("INSERT INTO t1 (id, val) VALUES (1, 100)".into());
        exec.exec("INSERT INTO t1 (id, val) VALUES (2, 200)".into());
        exec.exec("INSERT INTO t2 (id, val) VALUES (1, 100)".into());
        exec.exec("INSERT INTO t2 (id, val) VALUES (2, 200)".into());

        // Join matches both, but WHERE filters one out
        let query = "SELECT t1.id FROM t1 INNER JOIN t2 ON t1.id = t2.id WHERE t1.val > 150";
        let result = exec.exec(query.into());

        assert_row_count(result, 1); // Should only match id 2

    }

    #[test]
    fn test_empty_join_result() {
        let mut exec = setup_executor();

        exec.exec("CREATE TABLE A (id Integer)".into());
        exec.exec("CREATE TABLE B (id Integer)".into());

        exec.exec("INSERT INTO A (id) VALUES (1)".into());
        exec.exec("INSERT INTO B (id) VALUES (2)".into());

        let result = exec.exec("SELECT * FROM A INNER JOIN B ON A.id = B.id".into());
        assert_row_count(result, 0);

    }

    #[test]
    fn test_subquery_in_from_clause() {
        let mut exec = setup_executor();

        exec.exec("CREATE TABLE data (val Integer)".into());
        exec.exec("INSERT INTO data (val) VALUES (10)".into());
        exec.exec("INSERT INTO data (val) VALUES (20)".into());
        exec.exec("INSERT INTO data (val) VALUES (30)".into());

        let query = "SELECT * FROM (SELECT * FROM data)";
        let result = exec.exec(query.into());
        assert_row_count(result, 3);

    }

    #[test]
    fn test_subquery_with_filter() {
        let mut exec = setup_executor();

        exec.exec("CREATE TABLE numbers (n Integer)".into());
        exec.exec("INSERT INTO numbers (n) VALUES (1)".into());
        exec.exec("INSERT INTO numbers (n) VALUES (2)".into());
        exec.exec("INSERT INTO numbers (n) VALUES (3)".into());
        assert!(exec.exec("INSERT INTO numbers (n) VALUES (4)".into()).success);

        let query = "SELECT * FROM (SELECT * FROM numbers WHERE n > 2)";
        let result = exec.exec(query.into());
        println!("{}", result);
        assert_row_count(result, 2);
    }

    #[test]
    fn test_join_on_subquery() {
        let mut exec = setup_executor();

        exec.exec("CREATE TABLE main (id Integer)".into());
        exec.exec("CREATE TABLE extra (id Integer, info String)".into());

        exec.exec("INSERT INTO main (id) VALUES (1)".into());
        exec.exec("INSERT INTO main (id) VALUES (2)".into());
        exec.exec("INSERT INTO extra (id, info) VALUES (1, 'keep')".into());
        exec.exec("INSERT INTO extra (id, info) VALUES (2, 'drop')".into());

        // Join main table with a filtered subquery of extra
        let query = "SELECT main.id FROM main \
                     INNER JOIN (SELECT * FROM extra WHERE info = 'keep') \
                     ON main.id = extra.id";

        let result = exec.exec(query.into());
        assert_row_count(result, 1); // Only id 1 matches 'keep'

    }

    #[test]
    fn test_nested_subquery_deep() {
        let mut exec = setup_executor();

        exec.exec("CREATE TABLE t (v Integer)".into());
        exec.exec("INSERT INTO t (v) VALUES (1)".into());

        // SELECT * FROM (SELECT * FROM (SELECT * FROM t))
        let query = "SELECT * FROM (SELECT * FROM (SELECT * FROM t))";
        let result = exec.exec(query.into());
        assert_row_count(result, 1);

    }

    #[test]
    fn test_union_all() {
        let mut exec = setup_executor();

        exec.exec("CREATE TABLE A (val Integer)".into());
        exec.exec("CREATE TABLE B (val Integer)".into());

        exec.exec("INSERT INTO A (val) VALUES (1)".into());
        exec.exec("INSERT INTO B (val) VALUES (2)".into());
        exec.exec("INSERT INTO B (val) VALUES (1)".into());

        // Executor implements simple append (UNION ALL behavior)
        let query = "SELECT val FROM A UNION SELECT val FROM B";
        let result = exec.exec(query.into());

        // 1 (from A) + 2 (from B) + 1 (from B) = 3 rows
        assert_row_count(result, 3);

    }

    #[test]
    fn test_intersect() {
        let mut exec = setup_executor();

        exec.exec("CREATE TABLE A (val Integer)".into());
        exec.exec("CREATE TABLE B (val Integer)".into());

        exec.exec("INSERT INTO A (val) VALUES (1)".into());
        exec.exec("INSERT INTO A (val) VALUES (2)".into());
        exec.exec("INSERT INTO B (val) VALUES (2)".into());
        exec.exec("INSERT INTO B (val) VALUES (3)".into());

        // Intersection should be {2}
        let query = "SELECT val FROM A INTERSECT SELECT val FROM B";
        let result = exec.exec(query.into());

        assert_row_count(result, 1);

    }

    #[test]
    fn test_except() {
        let mut exec = setup_executor();

        exec.exec("CREATE TABLE A (val Integer)".into());
        exec.exec("CREATE TABLE B (val Integer)".into());

        exec.exec("INSERT INTO A (val) VALUES (1)".into());
        exec.exec("INSERT INTO A (val) VALUES (2)".into());
        exec.exec("INSERT INTO B (val) VALUES (2)".into());
        exec.exec("INSERT INTO B (val) VALUES (3)".into());

        // A (1, 2) EXCEPT B (2, 3) -> {1}
        let query = "SELECT val FROM A EXCEPT SELECT val FROM B";
        let result = exec.exec(query.into());

        assert_row_count(result, 1);

    }

    #[test]
    fn test_set_operation_schema_mismatch() {
        let mut exec = setup_executor();

        exec.exec("CREATE TABLE A (val Integer)".into());
        exec.exec("CREATE TABLE B (val Integer, x Integer)".into());

        let query = "SELECT * FROM A UNION SELECT * FROM B";
        let result = exec.exec(query.into());

        // Should fail due to column count mismatch
        assert!(!result.success);

    }

    #[test]
    fn test_union_of_subqueries() {
        let mut exec = setup_executor();

        exec.exec("CREATE TABLE T (v Integer)".into());
        exec.exec("INSERT INTO T (v) VALUES (1)".into());
        exec.exec("INSERT INTO T (v) VALUES (2)".into());
        exec.exec("INSERT INTO T (v) VALUES (3)".into());
        exec.exec("INSERT INTO T (v) VALUES (4)".into());

        // (Select < 3) UNION (Select > 3) -> 1, 2, 4
        let query = "(SELECT * FROM T WHERE v < 3) UNION (SELECT * FROM T WHERE v > 3)";
        let result = exec.exec(query.into());

        assert_row_count(result, 3);

    }

    #[test]
    fn test_complex_natural_join_and_filter() {
        let mut exec = setup_executor();

        assert!(exec.exec("CREATE TABLE A (id Integer)".into()).success);
        assert!(exec.exec("CREATE TABLE B (id Integer, score Integer)".into()).success);
        assert!(exec.exec("CREATE TABLE C (id Integer, pass Boolean)".into()).success);

        assert!(exec.exec("INSERT INTO A VALUES (1)".into()).success);
        assert!(exec.exec("INSERT INTO A VALUES (2)".into()).success);

        assert!(exec.exec("INSERT INTO B VALUES (1, 90)".into()).success);
        assert!(exec.exec("INSERT INTO B VALUES (2, 40)".into()).success);

        assert!(exec.exec("INSERT INTO C VALUES (1, true)".into()).success);
        assert!(exec.exec("INSERT INTO C VALUES (2, false)".into()).success);

        let query = "SELECT A.id FROM A NATURAL JOIN B NATURAL JOIN C WHERE B.score > 50";

        let result = exec.exec(query.into());
        println!("{}", result);
        assert_row_count(result, 1);

    }

    #[test]
    fn test_join_subquery_with_union() {
        let mut exec = setup_executor();

        assert_success(exec.exec("CREATE TABLE Employees (id Integer, name String)".into()));
        assert_success(exec.exec("CREATE TABLE Sales (emp_id Integer, region String)".into()));
        assert_success(exec.exec("CREATE TABLE Marketing (emp_id Integer, region String)".into()));

        assert_success(exec.exec("INSERT INTO Employees VALUES (1, 'Alice')".into()));
        assert_success(exec.exec("INSERT INTO Employees VALUES (2, 'Bob')".into()));
        assert_success(exec.exec("INSERT INTO Employees VALUES (3, 'Charlie')".into()));

        assert_success(exec.exec("INSERT INTO Sales VALUES (1, 'East')".into()));
        assert_success(exec.exec("INSERT INTO Sales VALUES (2, 'West')".into()));

        assert_success(exec.exec("INSERT INTO Marketing VALUES (1, 'North')".into()));
        assert_success(exec.exec("INSERT INTO Marketing VALUES (3, 'South')".into()));

        let union_query = "(SELECT emp_id FROM Sales) UNION (SELECT emp_id FROM Marketing)";

        let full_query = format!(
            "SELECT name, ump_id FROM Employees JOIN ({}) ON id = emp_id",
            union_query
        );

        let result = exec.exec(full_query.into());

        assert_row_count(result, 4);
    }

    #[test]
    fn test_intersect_of_joined_results() {
        let mut exec = setup_executor();

        assert_success(exec.exec("CREATE TABLE Users (id Integer, name String)".into()));
        assert_success(exec.exec("CREATE TABLE Groups (id Integer, name String)".into()));
        assert_success(exec.exec("CREATE TABLE GroupA (user_id Integer, group_id Integer)".into()));
        assert_success(exec.exec("CREATE TABLE GroupB (user_id Integer, group_id Integer)".into()));

        assert_success(exec.exec("INSERT INTO Users VALUES (1, 'Alice')".into()));
        assert_success(exec.exec("INSERT INTO Users VALUES (2, 'Bob')".into()));
        assert_success(exec.exec("INSERT INTO Users VALUES (3, 'Charlie')".into()));
        assert_success(exec.exec("INSERT INTO Groups VALUES (10, 'Admin')".into()));

        assert_success(exec.exec("INSERT INTO GroupA VALUES (1, 10)".into()));
        assert_success(exec.exec("INSERT INTO GroupA VALUES (2, 10)".into()));
        assert_success(exec.exec("INSERT INTO GroupB VALUES (1, 10)".into()));
        assert_success(exec.exec("INSERT INTO GroupB VALUES (3, 10)".into()));
        let query_a = "SELECT Users.name FROM Users JOIN GroupA ON Users.id = GroupA.user_id JOIN Groups ON GroupA.group_id = Groups.id WHERE Groups.name = 'Admin'";

        let query_b = "SELECT Users.name FROM Users JOIN GroupB ON Users.id = GroupB.user_id JOIN Groups ON GroupB.group_id = Groups.id WHERE Groups.name = 'Admin'";

        let full_query = format!("({}) INTERSECT ({})", query_a, query_b);

        let result = exec.exec(full_query.into());
        assert_row_count(result, 1);
    }

    #[test]
    fn test_complex_nested_filter_on_join() {
        let mut exec = setup_executor();

        assert_success(exec.exec("CREATE TABLE T1 (id Integer, value Integer)".into()));
        assert_success(exec.exec("CREATE TABLE T2 (t1_id Integer, score Integer)".into()));
        assert_success(exec.exec("CREATE TABLE T3 (id Integer, score Integer)".into()));

        assert_success(exec.exec("INSERT INTO T1 VALUES (1, 100)".into()));
        assert_success(exec.exec("INSERT INTO T1 VALUES (2, 200)".into()));
        assert_success(exec.exec("INSERT INTO T2 VALUES (1, 90)".into()));
        assert_success(exec.exec("INSERT INTO T2 VALUES (2, 95)".into()));
        assert_success(exec.exec("INSERT INTO T3 VALUES (1, 110)".into()));
        assert_success(exec.exec("INSERT INTO T3 VALUES (2, 915)".into()));

        let inner_query = "SELECT T1.id FROM T1 JOIN T2 ON T1.id = T2.t1_id WHERE T2.score > 90";

        let full_query = format!(
            "SELECT T1.id FROM ({}) JOIN T3 ON T3.id = T1.id WHERE T3.value > 150",
            inner_query
        );

        let result = exec.exec(full_query.into());
        // ID=1 (100, 90) -> Fails inner filter (score > 90)
        // ID=2 (200, 95) -> Passes inner filter (score > 90). Passes outer filter (value > 150).
        assert_row_count(result, 1);

    }

    #[test]
    fn test_project_subset() {
        let mut exec = setup_executor();

        assert_success(exec.exec("CREATE TABLE t (a Integer, b Integer, c Integer)".into()));
        assert_success(exec.exec("INSERT INTO t VALUES (1, 2, 3)".into()));

        let result = exec.exec("SELECT b, a FROM t".into());
        assert_success(result);
    }

    #[test]
    fn test_delete_single_row() {
        let mut exec = setup_executor();

        assert_success(exec.exec("CREATE TABLE test (id Integer, name String)".to_string()));
        assert_success(exec.exec("INSERT INTO test (id, name) VALUES (1, 'Alice')".to_string()));
        assert_success(exec.exec("INSERT INTO test (id, name) VALUES (2, 'Bob')".to_string()));

        let delete_result = exec.exec("DELETE FROM test WHERE id = 1".to_string());
        assert_success(delete_result);

        let result = exec.exec("SELECT name FROM test".to_string());
        assert_row_count(result, 1);
    }
}