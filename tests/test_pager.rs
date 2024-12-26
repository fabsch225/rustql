#[cfg(test)]
mod tests {
    use std::ffi::c_long;
    use std::thread::scope;
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

        let w = p.access_pager_write(|s|Pager::create_page_at_position(
            s,
            1,
            vec![key.clone()],
            vec![],
            vec![str.clone()],
            &get_schema(),
            p.clone()
        ));

        assert!(w.is_ok());
        let w = w.unwrap();

        let r = p.access_page_read(&w.clone(), |d, _|Serializer::read_data_by_vec(d, &get_schema()));

        assert!(r.is_ok());

        let value = r.unwrap().get(0).unwrap().to_vec();

        assert_eq!(value, str.clone());

        let r2 = p.access_page_read(&w.clone(), |d, _|Serializer::read_data_by_key(d, key.clone(), &get_schema()));

        assert!(r2.is_ok());
        assert_eq!(r2.unwrap(), str.clone());

        let r3 = p.access_page_read(&w.clone(), |d, _|Serializer::read_data_by_index(d, 0, &get_schema()));

        assert!(r3.is_ok());
        assert_eq!(r3.unwrap(), str.clone());
    }
}