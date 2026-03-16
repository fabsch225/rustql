use rustql::dataframe::DataFrame;
use rustql::pager::Type;
use rustql::schema::Field;
use rustql::serializer::Serializer;

fn int_row(v: i32) -> Vec<u8> {
    Serializer::int_to_bytes(v).to_vec()
}

fn decode_int_row(row: &[u8]) -> i32 {
    let arr: [u8; 5] = row.try_into().expect("row must be INTEGER-sized");
    Serializer::bytes_to_int(arr)
}

fn single_int_header() -> Vec<Field> {
    vec![Field {
        field_type: Type::Integer,
        name: "id".to_string(),
        table_name: "t".to_string(),
    }]
}

#[test]
fn test_fetch_n_batches_and_cursor_progresses() {
    let mut df = DataFrame::from_memory(
        "mem".to_string(),
        single_int_header(),
        vec![int_row(1), int_row(2), int_row(3), int_row(4), int_row(5)],
    );

    let b1 = df.fetch_n(2).expect("fetch_n should work");
    assert_eq!(b1.len(), 2);
    assert_eq!(decode_int_row(&b1[0]), 1);
    assert_eq!(decode_int_row(&b1[1]), 2);

    let b2 = df.fetch_n(2).expect("fetch_n should work");
    assert_eq!(b2.len(), 2);
    assert_eq!(decode_int_row(&b2[0]), 3);
    assert_eq!(decode_int_row(&b2[1]), 4);

    let b3 = df.fetch_n(2).expect("fetch_n should work");
    assert_eq!(b3.len(), 1);
    assert_eq!(decode_int_row(&b3[0]), 5);

    let b4 = df.fetch_n(2).expect("fetch_n should work");
    assert!(b4.is_empty());
}

#[test]
fn test_fetch_n_zero_does_not_advance_cursor() {
    let mut df = DataFrame::from_memory(
        "mem".to_string(),
        single_int_header(),
        vec![int_row(10), int_row(20), int_row(30)],
    );

    let empty = df.fetch_n(0).expect("fetch_n(0) should work");
    assert!(empty.is_empty());

    let next = df.fetch_n(2).expect("fetch_n should work");
    assert_eq!(next.len(), 2);
    assert_eq!(decode_int_row(&next[0]), 10);
    assert_eq!(decode_int_row(&next[1]), 20);
}

#[test]
fn test_fetch_after_partial_fetch_n_resets_and_returns_all() {
    let mut df = DataFrame::from_memory(
        "mem".to_string(),
        single_int_header(),
        vec![int_row(7), int_row(8), int_row(9)],
    );

    let _ = df.fetch_n(1).expect("fetch_n should work");

    let rows = df.fetch().expect("fetch should work");
    let values: Vec<i32> = rows.iter().map(|r| decode_int_row(r)).collect();
    assert_eq!(values, vec![7, 8, 9]);
}
