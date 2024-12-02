#[cfg(test)]
mod tests {
    use rustql::pager::*;
    use rustql::status::Status;

    #[test]
    fn test_compare_integers_equal() {
        let a: Vec<u8> = vec![1, 0, 0, 0, 42]; // Type: Integer (1), Value: 42
        let b: Vec<u8> = vec![1, 0, 0, 0, 42]; // Type: Integer (1), Value: 42
        assert_eq!(Serializer::compare(&a, &b), Ok(std::cmp::Ordering::Equal));
    }
    #[test]
    fn test_compare_integers_not_equal() {
        let a: Vec<u8> = vec![1, 0, 0, 0, 42]; // Type: Integer (1), Value: 42
        let b: Vec<u8> = vec![1, 0, 0, 0, 100]; // Type: Integer (1), Value: 100
        assert_eq!(Serializer::compare(&a, &b), Ok(std::cmp::Ordering::Less));
    }

    #[test]
    fn test_compare_strings_equal() {
        let mut a: Vec<u8> = vec![2; 257]; // Type: String (2)
        a[TYPE_SIZE..TYPE_SIZE + 5].copy_from_slice(b"hello");
        let mut b: Vec<u8> = vec![2; 257]; // Type: String (2)
        b[TYPE_SIZE..TYPE_SIZE + 5].copy_from_slice(b"hello");

        assert_eq!(Serializer::compare(&a, &b), Ok(std::cmp::Ordering::Equal));
    }

    #[test]
    fn test_compare_strings_not_equal() {
        let mut a: Vec<u8> = vec![2; 257]; // Type: String (2)
        a[TYPE_SIZE..TYPE_SIZE + 5].copy_from_slice(b"hello");
        let mut b: Vec<u8> = vec![2; 257]; // Type: String (2)
        b[TYPE_SIZE..TYPE_SIZE + 5].copy_from_slice(b"world");

        assert_eq!(Serializer::compare(&a, &b), Ok(std::cmp::Ordering::Less));
    }

    #[test]
    fn test_compare_dates_equal() {
        let a: Vec<u8> = vec![3, 0x07, 0xE5, 0x91]; // Type: Date (3), Date: 2021-09-01
        let b: Vec<u8> = vec![3, 0x07, 0xE5, 0x91]; // Type: Date (3), Date: 2021-09-01

        assert_eq!(Serializer::compare(&a, &b), Ok(std::cmp::Ordering::Equal));
    }

    #[test]
    fn test_compare_dates_not_equal() {
        let a: Vec<u8> = vec![3, 0x07, 0xE5, 0x91]; // Type: Date (3), Date: 2021-09-01
        let b: Vec<u8> = vec![3, 0x07, 0xE4, 0x91]; // Type: Date (3), Date: 2020-09-01

        assert_eq!(Serializer::compare(&a, &b), Ok(std::cmp::Ordering::Greater));
    }

    #[test]
    fn test_compare_booleans_equal() {
        let a: Vec<u8> = vec![4, 1]; // Type: Boolean (4), Value: true
        let b: Vec<u8> = vec![4, 1]; // Type: Boolean (4), Value: true

        assert_eq!(Serializer::compare(&a, &b), Ok(std::cmp::Ordering::Equal));
    }

    #[test]
    fn test_compare_booleans_not_equal() {
        let a: Vec<u8> = vec![4, 1]; // Type: Boolean (4), Value: true
        let b: Vec<u8> = vec![4, 0]; // Type: Boolean (4), Value: false

        assert_eq!(Serializer::compare(&a, &b), Ok(std::cmp::Ordering::Greater));
    }

    #[test]
    fn test_type_mismatch_error() {
        let a: Vec<u8> = vec![1, 0, 0, 0, 42]; // Type: Integer (1)
        let b: Vec<u8> = vec![2, 0, 0, 0, 42]; // Type: String (2)

        assert_eq!(
            Serializer::compare(&a, &b),
            Err(Status::InternalExceptionTypeMismatch)
        );
    }

    #[test]
    fn test_invalid_schema_error() {
        let a: Vec<u8> = vec![255, 0, 0, 0, 42]; // Invalid Type
        let b: Vec<u8> = vec![1, 0, 0, 0, 42];   // Type: Integer (1)

        assert_eq!(
            Serializer::compare(&a, &b),
            Err(Status::InternalExceptionInvalidSchema)
        );
    }

    #[test]
    fn test_bytes_to_ascii() {
        // Ensure the size matches STRING_SIZE
        let mut input: [u8; STRING_SIZE] = [0; STRING_SIZE];
        input[..5].copy_from_slice(b"hello"); // Copy "hello" into the beginning
        let expected = "hello".to_string();
        assert_eq!(Serializer::bytes_to_ascii(&input), expected);
    }

    #[test]
    fn test_bytes_to_position() {
        let input: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]; // Represents the integer 1
        let expected: i128 = 1;
        assert_eq!(Serializer::bytes_to_position(&input), expected);

        let input: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0]; // Represents 2^24
        let expected: i128 = 1 << 24;
        assert_eq!(Serializer::bytes_to_position(&input), expected);
    }

    #[test]
    fn test_bytes_to_int() {
        let input: [u8; 4] = [0, 0, 0, 42]; // Represents the integer 42
        let expected: i32 = 42;
        assert_eq!(Serializer::bytes_to_int(&input), expected);

        let input: [u8; 4] = [0, 1, 0, 0]; // Represents 2^16
        let expected: i32 = 1 << 16;
        assert_eq!(Serializer::bytes_to_int(&input), expected);
    }

    #[test]
    fn test_bytes_to_int_variable_length() {
        let input: [u8; 3] = [0, 1, 0]; // Represents 2^8
        let expected: i32 = 1 << 8;
        assert_eq!(Serializer::bytes_to_int_variable_length(&input), expected);
    }

    #[test]
    fn test_bytes_to_date() {
        let input: [u8; 3] = [0x07, 0xE5, 0x91]; // Encodes 2021-09-01
        let expected = (2021, 9, 1);
        assert_eq!(Serializer::bytes_to_date(&input), expected);

        let input: [u8; 3] = [0x07, 0xE4, 0x21]; // Encodes 2020-02-01
        let expected = (2020, 2, 1);
        assert_eq!(Serializer::bytes_to_date(&input), expected);
    }

    #[test]
    fn test_byte_to_bool() {
        assert_eq!(Serializer::byte_to_bool(0), false);
        assert_eq!(Serializer::byte_to_bool(1), true);
        assert_eq!(Serializer::byte_to_bool(2), false); // Only LSB matters
        assert_eq!(Serializer::byte_to_bool(3), true);  // Only LSB matters
    }

    #[test]
    fn test_byte_to_bool_at_position() {
        assert_eq!(Serializer::byte_to_bool_at_position(0b00000001, 0), true); // LSB
        assert_eq!(Serializer::byte_to_bool_at_position(0b00000010, 1), true);
        assert_eq!(Serializer::byte_to_bool_at_position(0b00000100, 2), true);
        assert_eq!(Serializer::byte_to_bool_at_position(0b00001000, 3), true);
        assert_eq!(Serializer::byte_to_bool_at_position(0b11111111, 7), true); // MSB
        assert_eq!(Serializer::byte_to_bool_at_position(0b00000000, 0), false);
    }

    #[test]
    fn test_bytes_to_bits() {
        let input: [u8; 2] = [0b10101010, 0b01010101]; // Alternating bits
        let expected = vec![
            true, false, true, false, true, false, true, false, // First byte
            false, true, false, true, false, true, false, true, // Second byte
        ];
        assert_eq!(Serializer::bytes_to_bits(&input), expected);
    }

    #[test]
    fn test_parse_type() {
        assert_eq!(Serializer::parse_type(0), Some(Type::Null));
        assert_eq!(Serializer::parse_type(1), Some(Type::Integer));
        assert_eq!(Serializer::parse_type(2), Some(Type::String));
        assert_eq!(Serializer::parse_type(3), Some(Type::Date));
        assert_eq!(Serializer::parse_type(4), Some(Type::Boolean));
        assert_eq!(Serializer::parse_type(255), None); // Invalid type
    }
}
