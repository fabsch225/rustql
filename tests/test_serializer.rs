#[cfg(test)]
mod tests {
    use rustql::pager::*;
    use rustql::serializer::Serializer;
    use rustql::status::Status;

    fn get_schema() -> Schema {
        Schema {
            col_count: 2,
            col_length: 260,
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

    fn create_mock_page_data(num_keys: usize) -> PageData {
        let schema = get_schema();
        let keys: Vec<Key> = (0..num_keys)
            .map(|i| vec![i as u8; schema.key_length])
            .collect();
        let children: Vec<Position> = vec![2; num_keys + 1];
        let rows: Vec<Row> = (0..num_keys)
            .map(|i| {
                let mut row = vec![0u8; schema.row_length];
                row[0..9].copy_from_slice(b"Mock Name");
                row
            })
            .collect();

        Serializer::init_page_data_with_children(keys, children, rows)
    }

    #[test]
    fn test_schema_to_bytes() {
        let schema = Schema {
            col_count: 3,
            col_length: 20,
            key_length: 4,
            key_type: Type::Integer,
            row_length: 16,
            fields: vec![
                Field { name: "id".to_string(), field_type: Type::Integer },
                Field { name: "name".to_string(), field_type: Type::String },
                Field { name: "active".to_string(), field_type: Type::Boolean },
            ],
        };

        let expected_bytes = vec![
            1, // Type::Integer
            105, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // "id"
            2, // Type::String
            110, 97, 109, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // "name"
            4, // Type::Boolean
            97, 99, 116, 105, 118, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // "active"
        ];

        let result_bytes = Serializer::schema_to_bytes(&schema);
        assert_eq!(result_bytes, expected_bytes);
    }

    #[test]
    fn test_schema_to_bytes_empty_schema() {
        let schema = Schema {
            col_count: 0,
            col_length: 0,
            key_length: 0,
            key_type: Type::Null,
            row_length: 0,
            fields: vec![],
        };

        let expected_bytes: Vec<u8> = vec![];

        let result_bytes = Serializer::schema_to_bytes(&schema);
        assert_eq!(result_bytes, expected_bytes);
    }

    #[test]
    fn test_schema_to_bytes_single_field() {
        let schema = Schema {
            col_count: 1,
            col_length: 4,
            key_length: 4,
            key_type: Type::Integer,
            row_length: 0,
            fields: vec![
                Field { name: "id".to_string(), field_type: Type::Integer },
            ],
        };

        let expected_bytes = vec![
            1, // Type::Integer
            105, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // "id"
        ];

        let result_bytes = Serializer::schema_to_bytes(&schema);
        assert_eq!(result_bytes, expected_bytes);
    }

    #[test]
    fn test_init_page_data() {
        let schema = get_schema();
        let page = create_mock_page_data(2);

        assert_eq!(page[0], 2);
        assert_eq!(page[2..6], vec![0u8, 0u8, 0u8, 0u8]);
        assert_eq!(page[6..10], vec![1u8, 1u8, 1u8, 1u8]);
    }

    #[test]
    fn test_write_keys_vec_resize_increase() {
        let schema = get_schema();
        let mut page = create_mock_page_data(2);
        let new_keys: Vec<Key> = vec![vec![3u8; 4], vec![4u8; 4], vec![5u8; 4]];

        let status = Serializer::write_keys_vec_resize(&new_keys, &mut page, &schema);
        assert_eq!(status, Status::InternalSuccess);
        assert_eq!(page[0], 3);
        assert_eq!(page[2..6], vec![3u8; 4]);
        assert_eq!(page[6..10], vec![4u8; 4]);
        assert_eq!(page[10..14], vec![5u8; 4]);
    }

    #[test]
    fn test_write_keys_vec_resize_decrease() {
        let schema = get_schema();
        let mut page = create_mock_page_data(3);
        let new_keys: Vec<Key> = vec![vec![6u8; 4]];

        let status = Serializer::write_keys_vec_resize(&new_keys, &mut page, &schema);
        assert_eq!(status, Status::InternalSuccess);
        assert_eq!(page[0], 1);
        assert_eq!(page[2..6], vec![6u8; 4]);
    }

    #[test]
    fn test_write_keys_vec_resize_no_change() {
        let schema = get_schema();
        let mut page = create_mock_page_data(2);
        let new_keys: Vec<Key> = vec![vec![7u8; 4], vec![8u8; 4]];

        let status = Serializer::write_keys_vec_resize(&new_keys, &mut page, &schema);
        assert_eq!(status, Status::InternalSuccess);
        assert_eq!(page[0], 2);
        assert_eq!(page[2..6], vec![7u8; 4]);
        assert_eq!(page[6..10], vec![8u8; 4]);
    }

    #[test]
    fn test_is_leaf() {
        let page = create_mock_page_data(2);
        let is_leaf = Serializer::is_leaf(&page, &get_schema()).unwrap();

        assert!(!is_leaf);
    }

    #[test]
    fn test_expand_keys_by() {
        let mut page = create_mock_page_data(2);
        let schema = get_schema();

        Serializer::expand_keys_by(2, &mut page, &schema).unwrap();

        assert_eq!(page[0], 4);
    }

    #[test]
    fn test_read_key() {
        let page = create_mock_page_data(2);
        let schema = get_schema();

        let key = Serializer::read_key(1, &page, &schema).unwrap();
        assert_eq!(key, vec![1u8; 4]);
    }

    #[test]
    fn test_write_key() {
        let mut page = create_mock_page_data(2);
        let schema = get_schema();

        Serializer::write_key(0, &vec![9u8; 4], &mut page, &schema);
        let key = Serializer::read_key(0, &page, &schema).unwrap();

        assert_eq!(key, vec![9u8; 4]);
    }

    #[test]
    fn test_read_child() {
        let page = create_mock_page_data(2);
        let schema = get_schema();

        let child = Serializer::read_child(0, &page, &schema).unwrap();
        assert_eq!(child, 2);
    }

    #[test]
    fn test_write_child() {
        let mut page = create_mock_page_data(2);
        let schema = get_schema();

        Serializer::write_child(1, 42, &mut page, &schema);
        let child = Serializer::read_child(1, &page, &schema).unwrap();

        assert_eq!(child, 42);
    }

    #[test]
    fn test_read_data_by_index() {
        let page = create_mock_page_data(2);
        let schema = get_schema();

        let row = Serializer::read_data_by_index(&page, 1, &schema).unwrap();
        assert_eq!(row[0..9].to_vec(), b"Mock Name".to_vec());
    }

    #[test]
    fn test_write_data_by_index() {
        let mut page = create_mock_page_data(2);
        let schema = get_schema();

        let mut new_row = vec![0u8; schema.row_length];
        new_row[0..12].copy_from_slice(b"Updated Name");

        Serializer::write_data_by_index(&mut page, 1, new_row.clone(), &schema).unwrap();
        let row = Serializer::read_data_by_index(&page, 1, &schema).unwrap();

        assert_eq!(row, new_row);
    }

    #[test]
    fn test_expand_keys_with_vec() {
        let mut page = create_mock_page_data(2);
        let schema = get_schema();

        let new_keys = vec![vec![5u8; 4], vec![6u8; 4]];
        Serializer::expand_keys_with_vec(&new_keys, &mut page, &schema);

        let expanded_keys = Serializer::read_keys_as_vec(&page, &schema).unwrap();
        assert_eq!(expanded_keys[2], new_keys[0]);
        assert_eq!(expanded_keys[3], new_keys[1]);
    }

    #[test]
    fn test_read_keys_as_vec() {
        let page = create_mock_page_data(2);
        let schema = get_schema();

        let keys = Serializer::read_keys_as_vec(&page, &schema).unwrap();
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn test_read_children_as_vec() {
        let page = create_mock_page_data(2);
        let schema = get_schema();

        let children = Serializer::read_children_as_vec(&page, &schema).unwrap();
        assert_eq!(children.len(), 3);
    }

    #[test]
    fn test_compare_integers_equal() {
        let a: Vec<u8> = vec![1, 0, 0, 0, 42];
        let b: Vec<u8> = vec![1, 0, 0, 0, 42];
        assert_eq!(Serializer::compare(&a, &b), Ok(std::cmp::Ordering::Equal));
    }

    #[test]
    fn test_compare_integers_not_equal() {
        let a: Vec<u8> = vec![1, 0, 0, 0, 42];
        let b: Vec<u8> = vec![1, 0, 0, 0, 100];
        assert_eq!(Serializer::compare(&a, &b), Ok(std::cmp::Ordering::Less));
    }

    #[test]
    fn test_compare_strings_equal() {
        let mut a: Vec<u8> = vec![2; 257];
        a[TYPE_SIZE..TYPE_SIZE + 5].copy_from_slice(b"hello");
        let mut b: Vec<u8> = vec![2; 257];
        b[TYPE_SIZE..TYPE_SIZE + 5].copy_from_slice(b"hello");

        assert_eq!(Serializer::compare(&a, &b), Ok(std::cmp::Ordering::Equal));
    }

    #[test]
    fn test_compare_strings_not_equal() {
        let mut a: Vec<u8> = vec![2; 257];
        a[TYPE_SIZE..TYPE_SIZE + 5].copy_from_slice(b"hello");
        let mut b: Vec<u8> = vec![2; 257];
        b[TYPE_SIZE..TYPE_SIZE + 5].copy_from_slice(b"world");

        assert_eq!(Serializer::compare(&a, &b), Ok(std::cmp::Ordering::Less));
    }

    #[test]
    fn test_compare_dates_equal() {
        let a: Vec<u8> = vec![3, 0x07, 0xE5, 0x91];
        let b: Vec<u8> = vec![3, 0x07, 0xE5, 0x91];

        assert_eq!(Serializer::compare(&a, &b), Ok(std::cmp::Ordering::Equal));
    }

    #[test]
    fn test_compare_dates_not_equal() {
        let a: Vec<u8> = vec![3, 0x07, 0xE5, 0x91];
        let b: Vec<u8> = vec![3, 0x07, 0xE4, 0x91];

        assert_eq!(Serializer::compare(&a, &b), Ok(std::cmp::Ordering::Greater));
    }

    #[test]
    fn test_compare_booleans_equal() {
        let a: Vec<u8> = vec![4, 1];
        let b: Vec<u8> = vec![4, 1];

        assert_eq!(Serializer::compare(&a, &b), Ok(std::cmp::Ordering::Equal));
    }

    #[test]
    fn test_compare_booleans_not_equal() {
        let a: Vec<u8> = vec![4, 1];
        let b: Vec<u8> = vec![4, 0];

        assert_eq!(Serializer::compare(&a, &b), Ok(std::cmp::Ordering::Greater));
    }

    #[test]
    fn test_type_mismatch_error() {
        let a: Vec<u8> = vec![1, 0, 0, 0, 42];
        let b: Vec<u8> = vec![2, 0, 0, 0, 42];

        assert_eq!(
            Serializer::compare(&a, &b),
            Err(Status::InternalExceptionTypeMismatch)
        );
    }

    #[test]
    fn test_invalid_schema_error() {
        let a: Vec<u8> = vec![255, 0, 0, 0, 42];
        let b: Vec<u8> = vec![1, 0, 0, 0, 42];

        assert_eq!(
            Serializer::compare(&a, &b),
            Err(Status::InternalExceptionInvalidSchema)
        );
    }

    #[test]
    fn test_bytes_to_ascii() {
        let mut input: [u8; STRING_SIZE] = [0; STRING_SIZE];
        input[..5].copy_from_slice(b"hello");
        let expected = "hello".to_string();
        assert_eq!(Serializer::bytes_to_ascii(&input), expected);
    }

    #[test]
    fn test_ascii_to_bytes() {
        let mut expected: [u8; STRING_SIZE] = [0; STRING_SIZE];
        expected[..5].copy_from_slice(b"hello");
        let input = "hello".to_string();
        assert_eq!(Serializer::ascii_to_bytes(&input), expected);
    }

    #[test]
    fn test_bytes_to_position() {
        let input: [u8; 4] = [0, 0, 0, 1];
        let expected: i32 = 1;
        assert_eq!(Serializer::bytes_to_position(&input), expected);

        let input: [u8; 4] = [1, 0, 0, 0];
        let expected: i32 = 1 << 24;
        assert_eq!(Serializer::bytes_to_position(&input), expected);
    }

    #[test]
    fn test_position_to_bytes() {
        let expected: [u8; 4] = [0, 0, 0, 1];
        let input: Position = 1;
        assert_eq!(Serializer::position_to_bytes(input), expected);

        let expected: [u8; 4] = [1, 0, 0, 0];
        let input: Position = 1 << 24;
        assert_eq!(Serializer::position_to_bytes(input), expected);
    }

    #[test]
    fn test_bytes_to_int() {
        let input: [u8; 4] = [0, 0, 0, 42];
        let expected: i32 = 42;
        assert_eq!(Serializer::bytes_to_int(&input), expected);

        let input: [u8; 4] = [0, 1, 0, 0];
        let expected: i32 = 1 << 16;
        assert_eq!(Serializer::bytes_to_int(&input), expected);
    }

    #[test]
    fn test_int_to_bytes() {
        let expected: [u8; 4] = [0, 0, 0, 42];
        let input: i32 = 42;
        assert_eq!(Serializer::int_to_bytes(input), expected);

        let expected: [u8; 4] = [0, 1, 0, 0];
        let input: i32 = 1 << 16;
        assert_eq!(Serializer::int_to_bytes(input), expected);
    }

    #[test]
    fn test_bytes_to_int_variable_length() {
        let input: [u8; 3] = [0, 1, 0];
        let expected: i32 = 1 << 8;
        assert_eq!(Serializer::bytes_to_int_variable_length(&input), expected);
    }

    #[test]
    fn test_bytes_to_date() {
        let input: [u8; 3] = [0x07, 0xE5, 0x91];
        let expected = (2021, 9, 1);
        assert_eq!(Serializer::bytes_to_date(&input), expected);

        let input: [u8; 3] = [0x07, 0xE4, 0x21];
        let expected = (2020, 2, 1);
        assert_eq!(Serializer::bytes_to_date(&input), expected);
    }

    #[test]
    fn test_date_to_bytes() {
        let expected: [u8; 3] = [0x07, 0xE5, 0x91];
        let input = (2021, 9, 1);
        assert_eq!(
            Serializer::date_to_bytes(input.0, input.1, input.2),
            expected
        );

        let expected: [u8; 3] = [0x07, 0xE4, 0x21];
        let input = (2020, 2, 1);
        assert_eq!(
            Serializer::date_to_bytes(input.0, input.1, input.2),
            expected
        );
    }

    #[test]
    fn test_byte_to_bool() {
        assert_eq!(Serializer::byte_to_bool(0), false);
        assert_eq!(Serializer::byte_to_bool(1), true);
        assert_eq!(Serializer::byte_to_bool(2), false);
        assert_eq!(Serializer::byte_to_bool(3), true);
    }

    #[test]
    fn test_byte_to_bool_at_position() {
        assert_eq!(Serializer::byte_to_bool_at_position(0b00000001, 0), true);
        assert_eq!(Serializer::byte_to_bool_at_position(0b00000010, 1), true);
        assert_eq!(Serializer::byte_to_bool_at_position(0b00000100, 2), true);
        assert_eq!(Serializer::byte_to_bool_at_position(0b00001000, 3), true);
        assert_eq!(Serializer::byte_to_bool_at_position(0b11111111, 7), true);
        assert_eq!(Serializer::byte_to_bool_at_position(0b00000000, 0), false);
    }

    #[test]
    fn test_bytes_to_bits() {
        let input: [u8; 2] = [0b10101010, 0b01010101];
        let expected = vec![
            true, false, true, false, true, false, true, false, false, true, false, true, false,
            true, false, true,
        ];
        assert_eq!(Serializer::bytes_to_bits(&input), expected);
    }

    #[test]
    fn test_parse_type() {
        assert_eq!(Serializer::byte_to_type(0), Some(Type::Null));
        assert_eq!(Serializer::byte_to_type(1), Some(Type::Integer));
        assert_eq!(Serializer::byte_to_type(2), Some(Type::String));
        assert_eq!(Serializer::byte_to_type(3), Some(Type::Date));
        assert_eq!(Serializer::byte_to_type(4), Some(Type::Boolean));
        assert_eq!(Serializer::byte_to_type(255), None);
    }
}
