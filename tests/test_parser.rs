#[cfg(test)]
mod tests {
    use rustql::parser::{JoinType, ParsedQuery, ParsedQueryTreeNode, ParsedSource, Parser};

    #[test]
    fn test_create_table_valid() {
        let query = "CREATE TABLE users (id Integer, name String, age Integer)";
        let mut parser = Parser::new(query.to_string());
        let result = parser.parse_query();
        assert!(result.is_ok());

        if let ParsedQuery::CreateTable(create_query) = result.unwrap() {
            assert_eq!(create_query.table_name, "users");
            assert_eq!(create_query.table_fields.len(), 3);
            assert_eq!(create_query.table_fields[0], "id");
            assert_eq!(create_query.table_types[0], "Integer");
        } else {
            panic!("Expected CreateTable query");
        }
    }

    #[test]
    fn test_create_table_missing_field_type() {
        let query = "CREATE TABLE users (id Integer, name";
        let mut parser = Parser::new(query.to_string());
        let result = parser.parse_query();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Expected field type".to_string());
    }

    #[test]
    fn test_create_table_missing_field_type_2() {
        let query = "CREATE TABLE users (id Integer, name)";
        let mut parser = Parser::new(query.to_string());
        let result = parser.parse_query();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Invalid type: )".to_string());
    }

    #[test]
    fn test_drop_table_valid() {
        let query = "DROP TABLE x";
        let mut parser = Parser::new(query.to_string());
        let result = parser.parse_query();
        assert!(result.is_ok());

        if let ParsedQuery::DropTable(drop_query) = result.unwrap() {
            assert_eq!(drop_query.table_name, "x");
        } else {
            panic!("Expected DropTable query");
        }
    }

    #[test]
    fn test_select_with_conditions() {
        let query = "SELECT id, name FROM users WHERE id = 10 AND name = 'John'";
        let mut parser = Parser::new(query.to_string());
        let result = parser.parse_query();
        assert!(result.is_ok());

        if let ParsedQuery::Select(select_query) = result.unwrap() {
            match select_query {
                ParsedQueryTreeNode::SingleQuery(select_query) => {
                    assert_eq!(select_query.result.len(), 2);
                    assert_eq!(select_query.result[0], "id");
                    assert_eq!(select_query.result[1], "name");
                    assert_eq!(select_query.conditions.len(), 2);

                    let first_condition = &select_query.conditions[0];
                    assert_eq!(first_condition.0, "id");
                    assert_eq!(first_condition.1, "=");
                    assert_eq!(first_condition.2, "10");

                    let second_condition = &select_query.conditions[1];
                    assert_eq!(second_condition.0, "name");
                    assert_eq!(second_condition.1, "=");
                    assert_eq!(second_condition.2, "John");
                }
                _ => panic!("Expected Select query"),
            }
        } else {
            panic!("Expected Select query");
        }
    }

    #[test]
    fn test_select_without_conditions() {
        let query = "SELECT id, name FROM users";
        let mut parser = Parser::new(query.to_string());
        let result = parser.parse_query();
        assert!(result.is_ok());

        if let ParsedQuery::Select(select_query) = result.unwrap() {
            match select_query {
                ParsedQueryTreeNode::SingleQuery(select_query) => {
                    assert_eq!(select_query.result.len(), 2);
                    assert_eq!(select_query.result[0], "id");
                    assert_eq!(select_query.result[1], "name");
                    assert!(select_query.conditions.is_empty());
                }
                _ => panic!("Expected Select query"),
            }
        } else {
            panic!("Expected Select query");
        }
    }

    #[test]
    fn test_select_with_invalid_condition() {
        let query = "SELECT id, name FROM users WHERE id =";
        let mut parser = Parser::new(query.to_string());
        let result = parser.parse_query();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Expected value in condition".to_string()
        );
    }

    #[test]
    fn test_select_missing_from() {
        let query = "SELECT id, name users";
        let mut parser = Parser::new(query.to_string());
        let result = parser.parse_query();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Expected 'FROM', but found 'users'".to_string()
        );
    }

    #[test]
    fn test_nested_join_inside_subqueries() {
        let query = "SELECT * FROM (SELECT * FROM t INNER JOIN (SELECT * FROM x LEFT JOIN y ON x.e = y.e) ON t.e = a.e)";
        let mut parser = Parser::new(query.to_string());
        let parsed = parser.parse_query().expect("Parse failed");

        match parsed {
            ParsedQuery::Select(ParsedQueryTreeNode::SingleQuery(select)) => {
                // outer SELECT *
                assert_eq!(select.result, vec!["*"]);

                match &select.source {
                    ParsedSource::SubQuery(subq1) => {
                        match subq1.as_ref() {
                            ParsedQueryTreeNode::SingleQuery(inner1) => {
                                assert_eq!(inner1.result, vec!["*"]);

                                match &inner1.source {
                                    ParsedSource::Join(join1) => {
                                        // join1: t JOIN <subquery> ON t.e = a.e
                                        assert_eq!(join1.sources.len(), 2);
                                        assert_eq!(join1.conditions.len(), 1);
                                        assert_eq!(join1.conditions[0].left, "t.e");
                                        assert_eq!(join1.conditions[0].right, "a.e");
                                        match &join1.conditions[0].join_type {
                                            JoinType::Inner => {}
                                            _ => panic!("Expected Inner Join"),
                                        }

                                        // left source = Table("t")
                                        match &join1.sources[0] {
                                            ParsedSource::Table(tname) => assert_eq!(tname, "t"),
                                            _ => panic!("Expected Table(\"t\")"),
                                        }

                                        // right source = SubQuery(...)
                                        match &join1.sources[1] {
                                            ParsedSource::SubQuery(subq2) => {
                                                match subq2.as_ref() {
                                                    ParsedQueryTreeNode::SingleQuery(inner2) => {
                                                        assert_eq!(inner2.result, vec!["*"]);

                                                        match &inner2.source {
                                                            ParsedSource::Join(join2) => {
                                                                // join2: x JOIN y ON x.e = y.e
                                                                assert_eq!(join2.sources.len(), 2);
                                                                assert_eq!(
                                                                    join2.conditions.len(),
                                                                    1
                                                                );

                                                                assert_eq!(
                                                                    join2.conditions[0].left,
                                                                    "x.e"
                                                                );
                                                                assert_eq!(
                                                                    join2.conditions[0].right,
                                                                    "y.e"
                                                                );
                                                                match &join2.conditions[0].join_type
                                                                {
                                                                    JoinType::Left => {}
                                                                    _ => {
                                                                        panic!("Expected Left Join")
                                                                    }
                                                                }

                                                                match &join2.sources[0] {
                                                                    ParsedSource::Table(t) => {
                                                                        assert_eq!(t, "x")
                                                                    }
                                                                    _ => panic!(
                                                                        "Expected Table(\"x\")"
                                                                    ),
                                                                }
                                                                match &join2.sources[1] {
                                                                    ParsedSource::Table(t) => {
                                                                        assert_eq!(t, "y")
                                                                    }
                                                                    _ => panic!(
                                                                        "Expected Table(\"y\")"
                                                                    ),
                                                                }
                                                            }
                                                            _ => panic!(
                                                                "Expected join inside subquery"
                                                            ),
                                                        }
                                                    }
                                                    _ => panic!("Expected SingleQuery in subquery"),
                                                }
                                            }
                                            _ => panic!("Expected SubQuery on RHS of join1"),
                                        }
                                    }
                                    _ => panic!("Expected join as source of inner1"),
                                }
                            }
                            _ => panic!("Expected SingleQuery for first subquery"),
                        }
                    }
                    _ => panic!("Expected outer FROM to be SubQuery"),
                }
            }
            _ => panic!("Expected SELECT query"),
        }
    }

    #[test]
    fn test_subquery_and_top_level_join() {
        let query =
            "SELECT * FROM (SELECT * FROM x INNER JOIN y ON x.a = y.b) INNER JOIN z ON z.a = y.b";
        let mut parser = Parser::new(query.to_string());
        let parsed = parser.parse_query().expect("Parse failed");

        match parsed {
            ParsedQuery::Select(ParsedQueryTreeNode::SingleQuery(select)) => {
                assert_eq!(select.result, vec!["*"]);

                match &select.source {
                    ParsedSource::Join(top_join) => {
                        assert_eq!(top_join.sources.len(), 2);
                        assert_eq!(top_join.conditions.len(), 1);

                        assert_eq!(top_join.conditions[0].left, "z.a");
                        assert_eq!(top_join.conditions[0].right, "y.b");
                        match &top_join.conditions[0].join_type {
                            JoinType::Inner => {}
                            _ => panic!("Expected Inner Join"),
                        }

                        match &top_join.sources[0] {
                            ParsedSource::SubQuery(subq) => match subq.as_ref() {
                                ParsedQueryTreeNode::SingleQuery(inner) => {
                                    assert_eq!(inner.result, vec!["*"]);

                                    match &inner.source {
                                        ParsedSource::Join(j) => {
                                            assert_eq!(j.sources.len(), 2);
                                            assert_eq!(j.conditions.len(), 1);

                                            assert_eq!(j.conditions[0].left, "x.a");
                                            assert_eq!(j.conditions[0].right, "y.b");
                                            match &j.conditions[0].join_type {
                                                JoinType::Inner => {}
                                                _ => panic!("Expected Inner Join"),
                                            }

                                            match &j.sources[0] {
                                                ParsedSource::Table(t) => assert_eq!(t, "x"),
                                                _ => panic!("Expected Table(\"x\")"),
                                            }
                                            match &j.sources[1] {
                                                ParsedSource::Table(t) => assert_eq!(t, "y"),
                                                _ => panic!("Expected Table(\"y\")"),
                                            }
                                        }
                                        _ => panic!("Expected join inside subquery"),
                                    }
                                }
                                _ => panic!("Expected SingleQuery in subquery"),
                            },
                            _ => panic!("Expected subquery as first join source"),
                        }

                        match &top_join.sources[1] {
                            ParsedSource::Table(t) => assert_eq!(t, "z"),
                            _ => panic!("Expected Table(\"z\")"),
                        }
                    }
                    _ => panic!("Expected top-level join"),
                }
            }
            _ => panic!("Expected SELECT query"),
        }
    }

    #[test]
    fn test_unknown_query_type() {
        let query = "UNKNOWN QUERY";
        let mut parser = Parser::new(query.to_string());
        let result = parser.parse_query();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Unknown statement type: UNKNOWN".to_string()
        );
    }

    #[test]
    fn test_parse_insert_basic() {
        let query = "INSERT INTO users (id, name, age) VALUES (1, 'John Doe', 30)".to_string();
        let mut parser = Parser::new(query);
        let result = parser.parse_query();

        assert!(result.is_ok());
        if let ParsedQuery::Insert(insert_query) = result.unwrap() {
            assert_eq!(insert_query.table_name, "users");
            assert_eq!(insert_query.fields, vec!["id", "name", "age"]);
            assert_eq!(insert_query.values, vec!["1", "John Doe", "30"]);
        } else {
            panic!("Expected InsertQuery");
        }
    }

    #[test]
    fn test_parse_insert_multiple_values() {
        let query =
            "INSERT INTO products (id, name, price) VALUES (101, 'Laptop', 999.99)".to_string();
        let mut parser = Parser::new(query);
        let result = parser.parse_query();

        assert!(result.is_ok());
        if let ParsedQuery::Insert(insert_query) = result.unwrap() {
            assert_eq!(insert_query.table_name, "products");
            assert_eq!(insert_query.fields, vec!["id", "name", "price"]);
            assert_eq!(insert_query.values, vec!["101", "Laptop", "999.99"]);
        } else {
            panic!("Expected InsertQuery");
        }
    }

    #[test]
    fn test_parse_insert_mismatched_fields_and_values() {
        let query = "INSERT INTO users (id, name) VALUES (1, 'John Doe', 30)".to_string();
        let mut parser = Parser::new(query);
        let result = parser.parse_query();

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Mismatched fields and values count: 2 fields, 3 values"
        );
    }

    #[test]
    fn test_parse_insert_missing_values() {
        let query = "INSERT INTO users (id, name, age)".to_string();
        let mut parser = Parser::new(query);
        let result = parser.parse_query();

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Expected 'VALUES', but reached end of input"
        );
    }

    #[test]
    fn test_parse_insert_extra_comma_in_fields() {
        let query = "INSERT INTO users (id, name, age,) VALUES (1, 'John Doe', 30)".to_string();
        let mut parser = Parser::new(query);
        let result = parser.parse_query();

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Expected field name");
    }

    #[test]
    fn test_parse_insert_extra_comma_in_values() {
        let query = "INSERT INTO users (id, name, age) VALUES (1, 'John Doe', 30,)".to_string();
        let mut parser = Parser::new(query);
        let result = parser.parse_query();

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Expected value");
    }

    #[test]
    fn test_parse_insert_empty_fields_and_values() {
        let query = "INSERT INTO users () VALUES ()".to_string();
        let mut parser = Parser::new(query);
        let result = parser.parse_query();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Expected field name");
    }

    #[test]
    fn test_parse_insert_unclosed_fields() {
        let query = "INSERT INTO users (id, name, age VALUES (1, 'John Doe', 30)".to_string();
        let mut parser = Parser::new(query);
        let result = parser.parse_query();

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Expected ',' or ')' after field name");
    }

    #[test]
    fn test_parse_insert_unclosed_values() {
        let query = "INSERT INTO users (id, name, age) VALUES (1, 'John Doe', 30".to_string();
        let mut parser = Parser::new(query);
        let result = parser.parse_query();

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Expected ',' or ')' after value");
    }

    #[test]
    fn test_parse_insert_whitespace_handling() {
        let query =
            "  INSERT   INTO   users   ( id , name , age )   VALUES (  1 ,  'John Doe' ,  30  )  "
                .to_string();
        let mut parser = Parser::new(query);
        let result = parser.parse_query();

        assert!(result.is_ok());
        if let ParsedQuery::Insert(insert_query) = result.unwrap() {
            assert_eq!(insert_query.table_name, "users");
            assert_eq!(insert_query.fields, vec!["id", "name", "age"]);
            assert_eq!(insert_query.values, vec!["1", "John Doe", "30"]);
        } else {
            panic!("Expected InsertQuery");
        }
    }

    #[test]
    fn test_parse_insert_no_table_name() {
        let query = "INSERT INTO (id, name, age) VALUES (1, 'John Doe', 30)".to_string();
        let mut parser = Parser::new(query);
        let result = parser.parse_query();

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Expected table name");
    }
}
