#[cfg(test)]
mod tests {
    use rustql::pager::{Field, Key, PagerCore, TableSchema, Type};
    use rustql::serializer::Serializer;

    fn get_schema() -> TableSchema {
        TableSchema {
            root: 0,
            col_count: 2,
            whole_row_length: 260,
            key_length: 4,
            key_type: Type::Integer,
            row_length: 256,
            fields: vec![
                Field {
                    name: "Id".to_string(),
                    field_type: Type::Integer,
                },
                Field {
                    name: "Name".to_string(),
                    field_type: Type::String,
                },
            ],
        }
    }

    #[test]
    fn verify_test_validity() {
        assert_eq!(Serializer::verify_schema(&get_schema()).is_ok(), true);
    }

    #[test]
    fn test_insert_and_read() {
        let mut p = PagerCore::init_from_schema("./default.db.bin", get_schema()).unwrap();

        let key: Key = vec![0, 0, 0, 1];
        let str = Serializer::ascii_to_bytes("Fabian").to_vec();

        let w = p.access_pager_write(|s| {
            PagerCore::create_page(
                s,
                vec![key.clone()],
                vec![],
                vec![str.clone()],
                &get_schema(),
                p.clone(),
            )
        });

        assert!(w.is_ok());
        let w = w.unwrap();

        let r = p.access_page_read(&w.clone(), |d, _| {
            Serializer::read_data_as_vec(d, &get_schema())
        });

        assert!(r.is_ok());

        let value = r.unwrap().get(0).unwrap().to_vec();

        assert_eq!(value, str.clone());

        let r2 = p.access_page_read(&w.clone(), |d, _| {
            Serializer::read_data_by_key(d, key.clone(), &get_schema())
        });

        assert!(r2.is_ok());
        assert_eq!(r2.unwrap(), str.clone());

        let r3 = p.access_page_read(&w.clone(), |d, _| {
            Serializer::read_data_by_index(d, 0, &get_schema())
        });

        assert!(r3.is_ok());
        assert_eq!(r3.unwrap(), str.clone());
    }
}
