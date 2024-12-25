#[cfg(test)]
mod tests {
    use rustql::pager::{Field, Key, Pager, Serializer, TableSchema, Type};

    fn get_schema() -> TableSchema {
        TableSchema {
            col_count: 2,
            row_length: 260,
            key_length: 4,
            key_type: Type::Integer,
            data_length: 256,
            fields: vec![
                Field {
                    name: "Id".to_string(),
                    field_type: Type::Integer,
                },
                Field {
                    name: "Name".to_string(),
                    field_type: Type::String,
                }
            ],
        }
    }

    #[test]
    fn verify_test_validity() {
        assert_eq!(Serializer::verify_schema(&get_schema()).is_ok(), true);
    }

    #[test]
    fn test_insert_and_read() {
        let mut p = Pager::init_from_schema("./default.db.bin", get_schema()).unwrap();

        let key : Key = vec![0, 0, 0, 1];
        let str = Serializer::ascii_to_bytes("Fabian").to_vec();

        let r = p.access_pager_write(|s|Pager::create_page_at_position(
            s,
            0,
            vec![key],
            vec![],
            vec![str],
            &get_schema(),
            p.clone()
        ));

        assert!(r.is_ok());
    }
}