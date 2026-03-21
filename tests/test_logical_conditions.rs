#[cfg(test)]
mod tests {
    use rustql::executor::QueryExecutor as RustqlQueryExecutor;
    use rustql::planner::{
        CompiledConditionExpr, CompiledInStrategy, CompiledPredicateExpr, CompiledQuery, PlanNode,
    };
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
        fn init() -> Self {
            let idx = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = format!("./default.db.test_logic.{}.{}.bin", std::process::id(), idx);
            let _ = fs::remove_file(&path);
            Self {
                inner: RustqlQueryExecutor::init(&path, BTREE_NODE_SIZE),
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

    fn extract_filter_condition(plan: &PlanNode) -> Option<&CompiledConditionExpr> {
        match plan {
            PlanNode::Filter { condition, .. } => Some(condition),
            PlanNode::Project { source, .. } => extract_filter_condition(source),
            _ => None,
        }
    }

    #[test]
    fn test_and_or_xor_logic_select() {
        let mut exec = QueryExecutor::init();
        assert!(
            exec.prepare("CREATE TABLE t (id Integer, v Integer)".to_string())
                .success
        );
        assert!(
            exec.prepare("INSERT INTO t (id, v) VALUES (1, 10)".to_string())
                .success
        );
        assert!(
            exec.prepare("INSERT INTO t (id, v) VALUES (2, 20)".to_string())
                .success
        );
        assert!(
            exec.prepare("INSERT INTO t (id, v) VALUES (3, 30)".to_string())
                .success
        );

        let r = exec
            .prepare("SELECT id FROM t WHERE id = 1 OR id = 2 AND v = 20".to_string())
            .data
            .fetch()
            .unwrap();
        assert_eq!(r.len(), 2);

        let x = exec
            .prepare("SELECT id FROM t WHERE id = 1 XOR id = 2".to_string())
            .data
            .fetch()
            .unwrap();
        assert_eq!(x.len(), 2);
    }

    #[test]
    fn test_in_subquery_key_lookup_strategy() {
        let mut exec = QueryExecutor::init();
        assert!(
            exec.prepare("CREATE TABLE a (id Integer, v Integer)".to_string())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE b (id Integer, v Integer)".to_string())
                .success
        );

        let compiled = exec
            .compile_query("SELECT id FROM a WHERE id IN (SELECT id FROM b)")
            .unwrap();

        if let CompiledQuery::Select(q) = compiled {
            let cond = extract_filter_condition(&q.plan).expect("expected filter condition");
            match cond {
                CompiledConditionExpr::Predicate(CompiledPredicateExpr::InSubquery {
                    strategy: CompiledInStrategy::KeyLookup { .. },
                    ..
                }) => {}
                _ => panic!("expected key-lookup strategy for IN subquery"),
            }
        } else {
            panic!("expected select query");
        }
    }

    #[test]
    fn test_in_subquery_index_lookup_strategy() {
        let mut exec = QueryExecutor::init();
        assert!(
            exec.prepare("CREATE TABLE a (id Integer, v Integer)".to_string())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE b (id Integer, v Integer)".to_string())
                .success
        );
        assert!(
            exec.prepare("CREATE INDEX idx_b_v ON b (v)".to_string())
                .success
        );

        let compiled = exec
            .compile_query("SELECT id FROM a WHERE v IN (SELECT v FROM b)")
            .unwrap();

        if let CompiledQuery::Select(q) = compiled {
            let cond = extract_filter_condition(&q.plan).expect("expected filter condition");
            match cond {
                CompiledConditionExpr::Predicate(CompiledPredicateExpr::InSubquery {
                    strategy: CompiledInStrategy::IndexLookup { .. },
                    ..
                }) => {}
                _ => panic!("expected index-lookup strategy for IN subquery"),
            }
        } else {
            panic!("expected select query");
        }
    }

    #[test]
    fn test_in_subquery_materialized_strategy() {
        let mut exec = QueryExecutor::init();
        assert!(
            exec.prepare("CREATE TABLE a (id Integer, v Integer)".to_string())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE b (id Integer, v Integer)".to_string())
                .success
        );

        let compiled = exec
            .compile_query("SELECT id FROM a WHERE v IN (SELECT v FROM b WHERE id = 1)")
            .unwrap();

        if let CompiledQuery::Select(q) = compiled {
            let cond = extract_filter_condition(&q.plan).expect("expected filter condition");
            match cond {
                CompiledConditionExpr::Predicate(CompiledPredicateExpr::InSubquery {
                    strategy: CompiledInStrategy::Materialize(_),
                    ..
                }) => {}
                _ => panic!("expected materialized strategy for filtered IN subquery"),
            }
        } else {
            panic!("expected select query");
        }
    }

    #[test]
    fn test_update_and_delete_with_or_conditions() {
        let mut exec = QueryExecutor::init();
        assert!(
            exec.prepare("CREATE TABLE t (id Integer, v Integer)".to_string())
                .success
        );
        for i in 1..=4 {
            assert!(
                exec.prepare(format!("INSERT INTO t (id, v) VALUES ({}, {})", i, i * 10))
                    .success
            );
        }

        assert!(
            exec.prepare("UPDATE t SET v = 99 WHERE id = 1 OR id = 3".to_string())
                .success
        );

        let updated = exec
            .prepare("SELECT id FROM t WHERE v = 99".to_string())
            .data
            .fetch()
            .unwrap();
        assert_eq!(updated.len(), 2);

        assert!(
            exec.prepare("DELETE FROM t WHERE id = 2 OR id = 4".to_string())
                .success
        );
        let remaining = exec
            .prepare("SELECT * FROM t".to_string())
            .data
            .fetch()
            .unwrap();
        assert_eq!(remaining.len(), 2);
    }

    #[test]
    fn test_stress_join_and_or_nested_in_subquery() {
        let mut exec = QueryExecutor::init();
        let n = 700;

        assert!(
            exec.prepare("CREATE TABLE a (id Integer, v Integer)".into())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE b (id Integer, w Integer)".into())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE c (id Integer, z Integer)".into())
                .success
        );

        for i in 1..=n {
            assert!(
                exec.prepare(format!("INSERT INTO a VALUES ({}, {})", i, i % 101))
                    .success
            );
            assert!(
                exec.prepare(format!("INSERT INTO b VALUES ({}, {})", i, (i * 3) % 97))
                    .success
            );
            assert!(
                exec.prepare(format!("INSERT INTO c VALUES ({}, {})", i, (i * 5) % 103))
                    .success
            );
        }

        let query = "SELECT a.id FROM a JOIN b ON a.id = b.id JOIN c ON a.id = c.id WHERE (a.v > 70 AND b.w < 30) OR a.id IN (SELECT id FROM c WHERE z > 80)";
        let rows = exec.prepare(query.into()).data.fetch().unwrap();

        let mut expected = 0usize;
        for i in 1..=n {
            let av = i % 101;
            let bw = (i * 3) % 97;
            let z = (i * 5) % 103;
            if (av > 70 && bw < 30) || z > 80 {
                expected += 1;
            }
        }
        assert_eq!(rows.len(), expected);
    }

    #[test]
    fn test_stress_join_with_xor_and_nested_from_subquery() {
        let mut exec = QueryExecutor::init();
        let n = 600;

        assert!(
            exec.prepare("CREATE TABLE a (id Integer, v Integer)".into())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE b (id Integer, w Integer)".into())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE d (id Integer, q Integer)".into())
                .success
        );

        for i in 1..=n {
            assert!(
                exec.prepare(format!("INSERT INTO a VALUES ({}, {})", i, i % 100))
                    .success
            );
            assert!(
                exec.prepare(format!("INSERT INTO b VALUES ({}, {})", i, (i * 2) % 100))
                    .success
            );
            assert!(
                exec.prepare(format!("INSERT INTO d VALUES ({}, {})", i, (i * 7) % 100))
                    .success
            );
        }

        let query = "SELECT a.id FROM (SELECT a.id, a.v FROM a JOIN b ON a.id = b.id) JOIN d ON a.id = d.id WHERE (a.v > 70) XOR (d.q > 70)";
        let rows = exec.prepare(query.into()).data.fetch().unwrap();

        let mut expected = 0usize;
        for i in 1..=n {
            let av = i % 100 > 70;
            let dq = (i * 7) % 100 > 70;
            if av ^ dq {
                expected += 1;
            }
        }
        assert_eq!(rows.len(), expected);
    }

    #[test]
    fn test_stress_nested_setop_in_subquery_with_join() {
        let mut exec = QueryExecutor::init();
        let n = 500;

        assert!(
            exec.prepare("CREATE TABLE a (id Integer, v Integer)".into())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE b (id Integer, v Integer)".into())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE c (id Integer, v Integer)".into())
                .success
        );

        for i in 1..=n {
            assert!(
                exec.prepare(format!("INSERT INTO a VALUES ({}, {})", i, i % 100))
                    .success
            );
            assert!(
                exec.prepare(format!("INSERT INTO b VALUES ({}, {})", i + 100, i % 100))
                    .success
            );
            assert!(
                exec.prepare(format!("INSERT INTO c VALUES ({}, {})", i, i % 100))
                    .success
            );
        }

        let query = "SELECT c.id FROM c WHERE c.id IN (SELECT id FROM (SELECT a.id FROM a UNION SELECT c.id FROM c) WHERE id > 250)";
        let rows = exec.prepare(query.into()).data.fetch().unwrap();
        assert_eq!(rows.len(), 250);
    }

    #[test]
    fn test_stress_update_with_complex_logic_and_nested_in() {
        let mut exec = QueryExecutor::init();
        let n = 900;

        assert!(
            exec.prepare("CREATE TABLE t (id Integer, v Integer)".into())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE f (id Integer, mark Integer)".into())
                .success
        );

        for i in 1..=n {
            assert!(
                exec.prepare(format!("INSERT INTO t VALUES ({}, {})", i, i % 100))
                    .success
            );
            assert!(
                exec.prepare(format!("INSERT INTO f VALUES ({}, {})", i, (i * 11) % 100))
                    .success
            );
        }

        let update = "UPDATE t SET v = 999 WHERE (id <= 300 OR id >= 850) AND id IN (SELECT id FROM f WHERE mark > 50)";
        assert!(exec.prepare(update.into()).success);

        let rows = exec
            .prepare("SELECT id FROM t WHERE v = 999".into())
            .data
            .fetch()
            .unwrap();

        let mut expected = 0usize;
        for i in 1..=n {
            let cond = (i <= 300 || i >= 850) && ((i * 11) % 100 > 50);
            if cond {
                expected += 1;
            }
        }
        assert_eq!(rows.len(), expected);
    }

    #[test]
    fn test_stress_delete_with_complex_logic_and_joinable_subquery() {
        let mut exec = QueryExecutor::init();
        let n = 850;

        assert!(
            exec.prepare("CREATE TABLE t (id Integer, v Integer)".into())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE x (id Integer, flag Integer)".into())
                .success
        );

        for i in 1..=n {
            assert!(
                exec.prepare(format!("INSERT INTO t VALUES ({}, {})", i, i % 120))
                    .success
            );
            assert!(
                exec.prepare(format!("INSERT INTO x VALUES ({}, {})", i, (i * 13) % 120))
                    .success
            );
        }

        let del = "DELETE FROM t WHERE (v < 10 XOR v > 110) AND id IN (SELECT id FROM x WHERE flag > 90 OR flag < 5)";
        assert!(exec.prepare(del.into()).success);

        let remaining = exec.prepare("SELECT * FROM t".into()).data.fetch().unwrap();

        let mut deleted = 0usize;
        for i in 1..=n {
            let v = i % 120;
            let flag = (i * 13) % 120;
            let cond = (v < 10) ^ (v > 110);
            let cond2 = flag > 90 || flag < 5;
            if cond && cond2 {
                deleted += 1;
            }
        }
        assert_eq!(remaining.len(), (n as usize) - deleted);
    }

    #[test]
    fn test_stress_in_key_lookup_with_large_dataset() {
        let mut exec = QueryExecutor::init();
        let n = 2000;

        assert!(
            exec.prepare("CREATE TABLE a (id Integer, v Integer)".into())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE b (id Integer, v Integer)".into())
                .success
        );

        for i in 1..=n {
            assert!(
                exec.prepare(format!("INSERT INTO a VALUES ({}, {})", i, (i * 2) % 1000))
                    .success
            );
            if i % 3 == 0 {
                assert!(
                    exec.prepare(format!("INSERT INTO b VALUES ({}, {})", i, (i * 7) % 1000))
                        .success
                );
            }
        }

        let rows = exec
            .prepare("SELECT id FROM a WHERE id IN (SELECT id FROM b)".into())
            .data
            .fetch()
            .unwrap();
        assert_eq!(rows.len(), n as usize / 3);
    }

    #[test]
    fn test_stress_in_index_lookup_with_large_dataset() {
        let mut exec = QueryExecutor::init();
        let n = 1500;

        assert!(
            exec.prepare("CREATE TABLE a (id Integer, v Integer)".into())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE b (id Integer, v Integer)".into())
                .success
        );
        assert!(exec.prepare("CREATE INDEX idx_b_v ON b (v)".into()).success);

        for i in 1..=n {
            assert!(
                exec.prepare(format!("INSERT INTO a VALUES ({}, {})", i, i % 200))
                    .success
            );
            assert!(
                exec.prepare(format!("INSERT INTO b VALUES ({}, {})", i, (i + 50) % 200))
                    .success
            );
        }

        let rows = exec
            .prepare("SELECT id FROM a WHERE v IN (SELECT v FROM b)".into())
            .data
            .fetch()
            .unwrap();
        // b covers all residues modulo 200, so every row in a matches.
        assert_eq!(rows.len(), n as usize);
    }

    #[test]
    fn test_stress_deep_nested_join_subquery_with_logical_filters() {
        let mut exec = QueryExecutor::init();
        let n = 650;

        assert!(
            exec.prepare("CREATE TABLE p (id Integer, a Integer)".into())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE q (id Integer, b Integer)".into())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE r (id Integer, c Integer)".into())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE s (id Integer, d Integer)".into())
                .success
        );

        for i in 1..=n {
            assert!(
                exec.prepare(format!("INSERT INTO p VALUES ({}, {})", i, i % 90))
                    .success
            );
            assert!(
                exec.prepare(format!("INSERT INTO q VALUES ({}, {})", i, (i * 2) % 90))
                    .success
            );
            assert!(
                exec.prepare(format!("INSERT INTO r VALUES ({}, {})", i, (i * 3) % 90))
                    .success
            );
            assert!(
                exec.prepare(format!("INSERT INTO s VALUES ({}, {})", i, (i * 5) % 90))
                    .success
            );
        }

        let query = "SELECT p.id FROM (SELECT p.id, p.a FROM p JOIN q ON p.id = q.id WHERE q.b > 20 OR p.a < 10) JOIN r ON p.id = r.id JOIN s ON p.id = s.id WHERE (r.c > 30 AND s.d < 40) OR (p.a > 70 XOR s.d > 70)";
        let rows = exec.prepare(query.into()).data.fetch().unwrap();

        let mut expected = 0usize;
        for i in 1..=n {
            let a = i % 90;
            let b = (i * 2) % 90;
            let c = (i * 3) % 90;
            let d = (i * 5) % 90;
            let inner = b > 20 || a < 10;
            let outer = (c > 30 && d < 40) || ((a > 70) ^ (d > 70));
            if inner && outer {
                expected += 1;
            }
        }
        assert_eq!(rows.len(), expected);
    }

    #[test]
    fn test_stress_join_with_two_nested_in_subqueries_and_xor() {
        let mut exec = QueryExecutor::init();
        let n = 1000;

        assert!(
            exec.prepare("CREATE TABLE u (id Integer, x Integer)".into())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE v (id Integer, y Integer)".into())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE m (id Integer, t Integer)".into())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE n (id Integer, t Integer)".into())
                .success
        );

        for i in 1..=n {
            assert!(
                exec.prepare(format!("INSERT INTO u VALUES ({}, {})", i, i % 75))
                    .success
            );
            assert!(
                exec.prepare(format!("INSERT INTO v VALUES ({}, {})", i, (i * 2) % 75))
                    .success
            );
            assert!(
                exec.prepare(format!("INSERT INTO m VALUES ({}, {})", i, (i * 3) % 75))
                    .success
            );
            assert!(
                exec.prepare(format!("INSERT INTO n VALUES ({}, {})", i, (i * 5) % 75))
                    .success
            );
        }

        let query = "SELECT u.id FROM u JOIN v ON u.id = v.id WHERE (u.id IN (SELECT id FROM m WHERE t > 40)) XOR (u.id IN (SELECT id FROM n WHERE t < 10))";
        let rows = exec.prepare(query.into()).data.fetch().unwrap();

        let mut expected = 0usize;
        for i in 1..=n {
            let m = (i * 3) % 75 > 40;
            let nflag = (i * 5) % 75 < 10;
            if m ^ nflag {
                expected += 1;
            }
        }
        assert_eq!(rows.len(), expected);
    }

    #[test]
    fn test_stress_multi_join_or_and_with_nested_setop_subquery() {
        let mut exec = QueryExecutor::init();
        let n = 750;

        assert!(
            exec.prepare("CREATE TABLE a (id Integer, v Integer)".into())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE b (id Integer, v Integer)".into())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE c (id Integer, v Integer)".into())
                .success
        );
        assert!(
            exec.prepare("CREATE TABLE d (id Integer, v Integer)".into())
                .success
        );

        for i in 1..=n {
            assert!(
                exec.prepare(format!("INSERT INTO a VALUES ({}, {})", i, i % 110))
                    .success
            );
            assert!(
                exec.prepare(format!("INSERT INTO b VALUES ({}, {})", i, (i * 2) % 110))
                    .success
            );
            assert!(
                exec.prepare(format!("INSERT INTO c VALUES ({}, {})", i, (i * 3) % 110))
                    .success
            );
            assert!(
                exec.prepare(format!("INSERT INTO d VALUES ({}, {})", i, (i * 4) % 110))
                    .success
            );
        }

        let query = "SELECT a.id FROM a JOIN b ON a.id = b.id JOIN c ON a.id = c.id WHERE (a.v > 90 AND b.v > 80) OR a.id IN (SELECT id FROM (SELECT id FROM c UNION SELECT id FROM d) WHERE id > 500)";
        let rows = exec.prepare(query.into()).data.fetch().unwrap();

        let mut expected = 0usize;
        for i in 1..=n {
            let av = i % 110;
            let bv = (i * 2) % 110;
            let in_set = i > 500;
            if (av > 90 && bv > 80) || in_set {
                expected += 1;
            }
        }
        assert_eq!(rows.len(), expected);
    }
}
