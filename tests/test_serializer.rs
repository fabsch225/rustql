//mind that the density of tests does not correspond to their significance

#[cfg(test)]
mod tests {
    use rustql::pager::*;
    use rustql::schema::{Field, TableSchema};
    use rustql::serializer::Serializer;

    fn get_schema() -> TableSchema {
        TableSchema {
            next_position: Position::make_empty(),
            root: Position::make_empty(),
            has_key: true,
            key_position: 0,
            fields: vec![
                Field {
                    identifier: "Id".to_string(),
                    field_type: Type::Integer,
                },
                Field {
                    identifier: "Num".to_string(),
                    field_type: Type::Integer,
                },
            ],
            table_type: 0,
            entry_count: 0,
        }
    }

    fn create_mock_page_data(num_keys: usize) -> PageData {
        let schema = get_schema();
        let keys: Vec<Key> = (0..num_keys)
            .map(|i| vec![i as u8; schema.get_key_length().unwrap()])
            .collect();
        let children: Vec<Position> = vec![Position::new(2, 2); num_keys + 1];
        let rows: Vec<Row> = (0..num_keys)
            .map(|i| {
                let mut row = vec![(i * 2) as u8; schema.get_row_length().unwrap()];
                row
            })
            .collect();

        println!("A writing keys {:?}", keys);
        println!("A writing rows {:?}", rows);
        println!("A writing children {:?}", children);

        Serializer::init_page_data_with_children(keys, children, rows, &schema).unwrap()
    }

    fn create_mock_page_data_multiple_nodes(
        num_nodes: usize,
        num_keys_lambda: fn(usize) -> usize,
        key_lambda: fn(usize, usize) -> usize,
    ) -> PageData {
        let schema = get_schema();
        let mut page = create_mock_page_data(num_keys_lambda(0));
        for j in 1..num_nodes {
            let keys: Vec<Key> = (0..num_keys_lambda(j))
                .map(|i| vec![key_lambda(i, j) as u8; schema.get_key_length().unwrap()])
                .collect();
            let children: Vec<Position> = vec![Position::new(2, 2); num_keys_lambda(j) + 1];
            let rows: Vec<Row> = (0..num_keys_lambda(j))
                .map(|i| {
                    let mut row = vec![(i * 2) as u8; schema.get_row_length().unwrap()];
                    row
                })
                .collect();
            let position = Position::new(0, j);
            println!("writing keys {:?}", keys);
            println!("writing rows {:?}", rows);
            println!("writing children {:?}", children);
            Serializer::write_keys_vec_resize_with_rows(
                &keys, &rows, &mut page, &position, &schema,
            )
            .unwrap();
            Serializer::write_children_vec(&children, &mut page, &position, &schema).unwrap();
        }
        page
    }

    #[test]
    fn test_copy_node_same_page() {
        let schema = get_schema();
        let mut page = create_mock_page_data(3);
        let pos_a = Position::new(0, 0);
        let pos_b = Position::new(0, 1);
        let src = page.clone();
        Serializer::copy_node(&schema, &pos_b, &pos_a, &mut page, &src).unwrap();

        let keys_a = Serializer::read_keys_as_vec(&page, &pos_a, &schema).unwrap();
        let keys_b = Serializer::read_keys_as_vec(&page, &pos_b, &schema).unwrap();
        assert_eq!(keys_a, keys_b);
    }

    #[test]
    fn test_copy_node_different_pages() {
        let schema = get_schema();
        let mut page_a = create_mock_page_data_multiple_nodes(2, |i| 3, |i, j| j * 4);
        let mut page_b = create_mock_page_data_multiple_nodes(3, |i| 3, |i, j| i);
        let pos_a = Position::new(0, 0);
        let pos_b = Position::new(1, 4);

        Serializer::copy_node(&schema, &pos_b, &pos_a, &mut page_b, &mut page_a).unwrap();

        let keys_a = Serializer::read_keys_as_vec(&page_a, &pos_a, &schema).unwrap();
        let keys_b = Serializer::read_keys_as_vec(&page_b, &pos_b, &schema).unwrap();
        assert_eq!(keys_a, keys_b);
    }

    #[test]
    fn test_copy_node_with_data() {
        let schema = get_schema();
        let mut page_a = create_mock_page_data(2);
        let mut page_b = create_mock_page_data(3);
        let pos_a = Position::new(0, 0);
        let pos_b = Position::new(1, 0);

        Serializer::copy_node(&schema, &pos_b, &pos_a, &mut page_b, &mut page_a).unwrap();

        let data_a = Serializer::read_data_as_vec(&page_a, &pos_a, &schema).unwrap();
        let data_b = Serializer::read_data_as_vec(&page_b, &pos_b, &schema).unwrap();
        assert_eq!(data_a, data_b);
    }

    #[test]
    fn test_copy_node_v2() {
        let schema = get_schema();
        let mut page_a = create_mock_page_data_multiple_nodes(6, |i| 3, |i, j| j * j * j);
        let mut page_b = create_mock_page_data_multiple_nodes(5, |i| 3, |i, j| i);
        let pos_a = Position::new(0, 2);
        let pos_b = Position::new(1, 4);

        Serializer::copy_node(&schema, &pos_b, &pos_a, &mut page_b, &mut page_a).unwrap();

        let children_a = Serializer::read_children_as_vec(&page_a, &pos_a, &schema).unwrap();
        let children_b = Serializer::read_children_as_vec(&page_b, &pos_b, &schema).unwrap();
        assert_eq!(children_a, children_b);
    }

    #[test]
    fn test_switch_nodes_same_page() {
        let schema = get_schema();
        let mut page = create_mock_page_data_multiple_nodes(4, |i| 3, |i, j| j * 4);
        let pos_a = Position::new(0, 1);
        let pos_b = Position::new(0, 2);

        let keys = Serializer::read_keys_as_vec(&page, &Position::new(0, 1), &schema).unwrap();
        assert_eq!(keys[1], vec![4u8; schema.get_key_length().unwrap()]);
        let keys = Serializer::read_keys_as_vec(&page, &Position::new(0, 2), &schema).unwrap();
        assert_eq!(keys[2], vec![8u8; schema.get_key_length().unwrap()]);

        Serializer::switch_nodes(&schema, &pos_a, &pos_b, &mut page, None).unwrap();

        let keys = Serializer::read_keys_as_vec(&page, &Position::new(0, 0), &schema).unwrap();
        assert_eq!(keys[1], vec![1u8; schema.get_key_length().unwrap()]);
        assert_eq!(keys[2], vec![2u8; schema.get_key_length().unwrap()]);

        let keys = Serializer::read_keys_as_vec(&page, &Position::new(0, 1), &schema).unwrap();
        assert_eq!(keys[1], vec![8u8; schema.get_key_length().unwrap()]);
        assert_eq!(keys[2], vec![8u8; schema.get_key_length().unwrap()]);

        let keys = Serializer::read_keys_as_vec(&page, &Position::new(0, 2), &schema).unwrap();
        assert_eq!(keys[1], vec![4u8; schema.get_key_length().unwrap()]);
        assert_eq!(keys[2], vec![4u8; schema.get_key_length().unwrap()]);
    }

    #[test]
    fn test_switch_nodes_different_pages() {
        let schema = get_schema();
        let mut page_a = create_mock_page_data(1);
        let mut page_b = create_mock_page_data_multiple_nodes(2, |i| 3, |i, j| j * 4);
        let pos_a = Position::new(0, 0);
        let pos_b = Position::new(1, 1);
        Serializer::switch_nodes(&schema, &pos_a, &pos_b, &mut page_a, Some(&mut page_b)).unwrap();
        let keys_a =
            Serializer::read_keys_as_vec(&page_a, &Position::make_empty(), &schema).unwrap();
        let keys_b =
            Serializer::read_keys_as_vec(&page_b, &Position::make_empty(), &schema).unwrap();
        let keys_b_altered = Serializer::read_keys_as_vec(&page_b, &pos_b, &schema).unwrap();
        assert_eq!(keys_a[0], vec![4u8; schema.get_key_length().unwrap()]);
        assert_eq!(keys_b[0], vec![0u8; schema.get_key_length().unwrap()]);
        assert_eq!(keys_b_altered[0], vec![0u8; schema.get_key_length().unwrap()]);
    }
    #[test]
    fn test_switch_nodes_large_page() {
        let schema = get_schema();
        let mut page = create_mock_page_data_multiple_nodes(5, |i| 5, |i, j| j * 3 + i * 2);
        let pos_a = Position::new(0, 0);
        let pos_b = Position::new(0, 4);

        let keys = Serializer::read_keys_as_vec(&page, &Position::new(0, 0), &schema).unwrap();
        let data = Serializer::read_data_as_vec(&page, &Position::make_empty(), &schema).unwrap();
        println!("keys {:?}", keys);
        println!("data {:?}", data);

        let keys = Serializer::read_keys_as_vec(&page, &Position::make_empty(), &schema).unwrap();
        assert_eq!(keys[0], vec![0u8; schema.get_key_length().unwrap()]);
        assert_eq!(keys[4], vec![4u8; schema.get_key_length().unwrap()]);

        println!("page is {:?}", page);
        Serializer::switch_nodes(&schema, &pos_a, &pos_b, &mut page, None).unwrap();
        println!("page is {:?}", page);

        let keys = Serializer::read_keys_as_vec(&page, &Position::make_empty(), &schema).unwrap();
        assert_eq!(keys[0], vec![12u8; schema.get_key_length().unwrap()]);
        assert_eq!(keys[4], vec![20u8; schema.get_key_length().unwrap()]);
    }

    #[test]
    fn test_init_page_data_w_children() {
        let schema = get_schema();
        let keys = vec![vec![1u8; schema.get_key_length().unwrap()], vec![2u8; schema.get_key_length().unwrap()]];
        let rows = vec![vec![9u8; schema.get_row_length().unwrap()], vec![8u8; schema.get_row_length().unwrap()]];
        let children = vec![Position::new(2, 2); 3];
        let page = Serializer::init_page_data_with_children(keys, children, rows, &schema).unwrap();
        assert_eq!(page[0], 2); // Number of keys
        let keys = Serializer::read_keys_as_vec(&page, &Position::make_empty(), &schema).unwrap();
        assert_eq!(keys[0], vec![1u8; schema.get_key_length().unwrap()]);
        assert_eq!(keys[1], vec![2u8; schema.get_key_length().unwrap()]);

        let children =
            Serializer::read_children_as_vec(&page, &Position::make_empty(), &schema).unwrap();
        assert_eq!(children.len(), 3);
        assert_eq!(children[2].page(), 2);
        assert_eq!(children[1].cell(), 2);

        let rows = Serializer::read_data_as_vec(&page, &Position::make_empty(), &schema).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![9u8; schema.get_row_length().unwrap()]);
        assert_eq!(rows[1], vec![8u8; schema.get_row_length().unwrap()]);
    }

    #[test]
    fn test_init_page_data() {
        let schema = get_schema();
        let keys = vec![vec![1u8; schema.get_key_length().unwrap()], vec![2u8; schema.get_key_length().unwrap()]];
        let rows = vec![vec![9u8; schema.get_row_length().unwrap()], vec![8u8; schema.get_row_length().unwrap()]];
        let children = vec![Position::new(2, 2); 3];
        let page = Serializer::init_page_data_with_children(keys, children, rows, &schema).unwrap();
        assert_eq!(page[0], 2); // Number of keys
        let keys = Serializer::read_keys_as_vec(&page, &Position::make_empty(), &schema).unwrap();
        assert_eq!(keys[0], vec![1u8; schema.get_key_length().unwrap()]);
        assert_eq!(keys[1], vec![2u8; schema.get_key_length().unwrap()]);

        let rows = Serializer::read_data_as_vec(&page, &Position::make_empty(), &schema).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![9u8; schema.get_row_length().unwrap()]);
        assert_eq!(rows[1], vec![8u8; schema.get_row_length().unwrap()]);

        assert_eq!(page.len(), PAGE_SIZE);
    }

    #[test]
    fn test_write_keys_vec_resize_increase() {
        let schema = get_schema();
        let keys = vec![vec![1u8; schema.get_key_length().unwrap()], vec![2u8; schema.get_key_length().unwrap()]];
        let rows = vec![vec![9u8; schema.get_row_length().unwrap()], vec![8u8; schema.get_row_length().unwrap()]];
        let children = vec![Position::new(2, 2); 3];
        let mut page =
            Serializer::init_page_data_with_children(keys, children, rows, &schema).unwrap();

        let keys = Serializer::read_keys_as_vec(&page, &Position::make_empty(), &schema).unwrap();
        assert_eq!(keys[0], vec![1u8; schema.get_key_length().unwrap()]);
        assert_eq!(keys[1], vec![2u8; schema.get_key_length().unwrap()]);
        let rows = Serializer::read_data_as_vec(&page, &Position::make_empty(), &schema).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![9u8; schema.get_row_length().unwrap()]);
        assert_eq!(rows[1], vec![8u8; schema.get_row_length().unwrap()]);
        let new_keys: Vec<Key> = vec![vec![3u8; 5], vec![4u8; 5], vec![5u8; 5]];

        Serializer::write_keys_vec_resize(&new_keys, &mut page, &Position::make_empty(), &schema)
            .unwrap();

        let keys = Serializer::read_keys_as_vec(&page, &Position::make_empty(), &schema).unwrap();
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], vec![3u8; schema.get_key_length().unwrap()]);
        assert_eq!(keys[1], vec![4u8; schema.get_key_length().unwrap()]);
        assert_eq!(keys[2], vec![5u8; schema.get_key_length().unwrap()]);

        //rows and children should be unchanged
        let rows = Serializer::read_data_as_vec(&page, &Position::make_empty(), &schema).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec![9u8; schema.get_row_length().unwrap()]);
        assert_eq!(rows[1], vec![8u8; schema.get_row_length().unwrap()]);
        assert_eq!(rows[2], vec![0u8; schema.get_row_length().unwrap()]); //padded with 0s

        let children =
            Serializer::read_children_as_vec(&page, &Position::make_empty(), &schema).unwrap();
        assert_eq!(children.len(), 3);
        assert_eq!(children[0].page(), 2);
        assert_eq!(children[1].cell(), 2);
        assert_eq!(children[2].cell(), 2);

        assert_eq!(page.len(), PAGE_SIZE);
    }

    #[test]
    fn test_write_keys_vec_resize_decrease() {
        let schema = get_schema();
        let mut page = create_mock_page_data(3);
        let position = Position::make_empty();
        let new_keys: Vec<Key> = vec![vec![6u8; 5]];

        let old_row = Serializer::read_data_by_index(0, &page, &position, &schema).unwrap();

        Serializer::write_keys_vec_resize(&new_keys, &mut page, &position, &schema).unwrap();

        let keys = Serializer::read_keys_as_vec(&page, &position, &schema).unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], vec![6u8; 5]);

        let rows = Serializer::read_data_as_vec(&page, &position, &schema).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], old_row);

        let children = Serializer::read_children_as_vec(&page, &position, &schema).unwrap();
        assert_eq!(children.len(), 2);
        assert_eq!(children[0].page(), 2);
        assert_eq!(children[1].cell(), 2);

        assert_eq!(page.len(), PAGE_SIZE);
    }

    #[test]
    fn test_write_keys_vec_resize_no_change() {
        let schema = get_schema();
        let mut page = create_mock_page_data(2);
        let position = Position::make_empty();
        let new_keys: Vec<Key> = vec![vec![4u8; 5], vec![5u8; 5]];

        Serializer::write_keys_vec_resize(&new_keys, &mut page, &position, &schema).unwrap();
        assert_eq!(page[0], 2); // Number of keys unchanged

        let keys = Serializer::read_keys_as_vec(&page, &position, &schema).unwrap();
        assert_eq!(keys[0], vec![4u8; 5]);
        assert_eq!(keys[1], vec![5u8; 5]);

        assert_eq!(page.len(), PAGE_SIZE);
    }

    #[test]
    fn test_is_leaf() {
        let mut page = create_mock_page_data(1);
        //assert!(Serializer::is_leaf(&page).unwrap());

        //Serializer::set_is_leaf(&mut page, false).unwrap();
        //assert!(!Serializer::is_leaf(&page).unwrap());
    }

    #[test]
    fn test_expand_keys_by() {
        let schema = get_schema();
        let mut page = create_mock_page_data(2);
        let position = Position::make_empty();
        let initial_count = page[0];

        Serializer::expand_keys_by(2, &mut page, &position, &schema).unwrap();

        assert_eq!(page[0], initial_count + 2);
        assert_eq!(page.len(), PAGE_SIZE);
    }

    #[test]
    fn test_read_key() {
        let schema = get_schema();
        let page = create_mock_page_data(2);
        let position = Position::make_empty();
        let key = Serializer::read_key(0, &page, &position, &schema).unwrap();

        assert_eq!(key.len(), schema.get_key_length().unwrap());
        assert_eq!(key, vec![0u8; schema.get_key_length().unwrap()]);
        assert_eq!(page.len(), PAGE_SIZE);
    }

    #[test]
    fn test_write_key() {
        let schema = get_schema();
        let mut page = create_mock_page_data(2);
        let position = Position::make_empty();
        let new_key = vec![42u8; schema.get_key_length().unwrap()];

        Serializer::write_key(0, &mut page, &position, &new_key, &schema).unwrap();

        let read_key = Serializer::read_key(0, &page, &position, &schema).unwrap();
        assert_eq!(read_key, new_key);
        assert_eq!(page.len(), PAGE_SIZE);
    }

    #[test]
    fn test_read_child() {
        let schema = get_schema();
        let page = create_mock_page_data(2);
        let position = Position::make_empty();
        let child = Serializer::read_child(0, &page, &position, &schema).unwrap();

        assert_eq!(child.cell(), 2); // From create_mock_page_data
        assert_eq!(page.len(), PAGE_SIZE);
    }

    #[test]
    fn test_write_child() {
        let schema = get_schema();
        let mut page = create_mock_page_data(2);
        let position = Position::make_empty();
        let new_child = Position::make_empty();

        Serializer::write_child(0, &mut page, &position, new_child.clone(), &schema).unwrap();

        let read_child = Serializer::read_child(0, &page, &position, &schema).unwrap();
        assert_eq!(read_child, new_child);
        assert_eq!(page.len(), PAGE_SIZE);
    }

    #[test]
    fn test_read_data_by_index() {
        let schema = get_schema();
        let page = create_mock_page_data(2);
        let position = Position::make_empty();
        let row = Serializer::read_data_by_index(0, &page, &position, &schema).unwrap();

        //TODO something
        assert_eq!(page.len(), PAGE_SIZE);
    }

    #[test]
    fn test_write_data_by_index() {
        let schema = get_schema();
        let mut page = create_mock_page_data(2);
        let position = Position::make_empty();
        let new_row = vec![9u8; schema.get_row_length().unwrap()];

        Serializer::write_data_by_index(1, &mut page, &position, new_row.clone(), &schema).unwrap();

        let read_row = Serializer::read_data_by_index(1, &page, &position, &schema).unwrap();
        assert_eq!(read_row, new_row);
        assert_eq!(page.len(), PAGE_SIZE);
    }

    #[test]
    fn test_expand_keys_with_vec() {
        let mut page = create_mock_page_data(2);
        println!("{:?}", page);
        let schema = get_schema();
        let position = Position::make_empty();
        let new_keys = vec![vec![5; 5], vec![6; 5]];
        Serializer::expand_keys_with_vec(&new_keys, &mut page, &position, &schema).unwrap();
        println!("{:?}", page);
        let expanded_keys = Serializer::read_keys_as_vec(&page, &position, &schema).unwrap();
        assert_eq!(expanded_keys[2], new_keys[0]);
        assert_eq!(expanded_keys[3], new_keys[1]);
        assert_eq!(page.len(), PAGE_SIZE);
    }

    #[test]
    fn test_read_keys_as_vec() {
        let page = create_mock_page_data(2);
        let position = Position::make_empty();
        let schema = get_schema();

        let keys = Serializer::read_keys_as_vec(&page, &position, &schema).unwrap();
        assert_eq!(keys.len(), 2);
        assert_eq!(page.len(), PAGE_SIZE);
    }

    #[test]
    fn test_read_children_as_vec() {
        let page = create_mock_page_data(2);
        let position = Position::make_empty();
        let schema = get_schema();

        let children = Serializer::read_children_as_vec(&page, &position, &schema).unwrap();
        assert_eq!(children.len(), 3);
        assert_eq!(page.len(), PAGE_SIZE);
    }

    #[test]
    fn test_shift_page_right_block_small() {
        //let mut page = vec![1, 2, 3, 4, 5];
        let mut page = [0; PAGE_SIZE];
        page[0..5].copy_from_slice(&[1, 2, 3, 4, 5]);
        Serializer::shift_page_block(&mut page, 1, 4, 1).unwrap();
        assert_eq!(page[0..5], vec![1, 0, 2, 3, 5]);
    }

    #[test]
    fn test_shift_page_left_block_small() {
        //let mut page = vec![1, 2, 3, 4, 5];
        let mut page = [0; PAGE_SIZE];
        page[0..5].copy_from_slice(&[1, 2, 3, 4, 5]);
        Serializer::shift_page_block(&mut page, 2, 3, -1).unwrap();
        assert_eq!(page[0..5], vec![1, 3, 0, 4, 5]);
    }

    #[test]
    fn test_shift_page_right_block_small_bigger() {
        //let mut page = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let mut page = [0; PAGE_SIZE];
        page[0..10].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        Serializer::shift_page_block(&mut page, 1, 7, 3).unwrap();
        assert_eq!(page[0..10], vec![1, 0, 0, 0, 2, 3, 4, 8, 9, 10]);
    }

    #[test]
    fn test_shift_page_left_block_small_bigger() {
        //let mut page = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let mut page = [0; PAGE_SIZE];
        page[0..10].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        Serializer::shift_page_block(&mut page, 4, 7, -3).unwrap();
        assert_eq!(page[0..10], vec![1, 5, 6, 7, 0, 0, 0, 8, 9, 10]);
    }

    #[test]
    fn test_shift_page_right_small() {
        //let mut page = vec![1, 2, 3, 4, 5];
        let mut page = [0; PAGE_SIZE];
        page[0..5].copy_from_slice(&[1, 2, 3, 4, 5]);
        Serializer::shift_page(&mut page, 1, 2).unwrap();
        assert_eq!(page[0..5], vec![1, 0, 0, 2, 3]);
    }

    #[test]
    fn test_shift_page_left_small() {
        //let mut page = vec![1, 2, 3, 4, 5];
        let mut page = [0; PAGE_SIZE];
        page[0..5].copy_from_slice(&[1, 2, 3, 4, 5]);
        Serializer::shift_page(&mut page, 2, -2).unwrap();
        assert_eq!(page[0..5], vec![3, 4, 5, 0, 0]);
    }

    #[test]
    fn test_shift_page_right_large() {
        //let mut page = vec![1, 0, 0, 2, 3, 0, 0, 4, 5, 6];
        let mut page = [0; PAGE_SIZE];
        page[0..10].copy_from_slice(&[1, 0, 0, 2, 3, 0, 0, 4, 5, 6]);
        Serializer::shift_page(&mut page, 3, 3).unwrap();
        assert_eq!(page[0..10], vec![1, 0, 0, 0, 0, 0, 2, 3, 0, 0]);
    }

    #[test]
    fn test_shift_page_right_large_big_offset() {
        //let mut page = vec![1, 0, 0, 2, 3, 0, 0, 4, 5, 6];
        let mut page = [0; PAGE_SIZE];
        page[0..10].copy_from_slice(&[1, 0, 0, 2, 3, 0, 0, 4, 5, 6]);
        Serializer::shift_page(&mut page, 0, 8).unwrap();
        assert_eq!(page[0..10], vec![0, 0, 0, 0, 0, 0, 0, 0, 1, 0]);
    }

    #[test]
    fn test_shift_page_left_large() {
        //let mut page = vec![1, 2, 3, 0, 0, 4, 5, 6, 0, 0];
        let mut page = [0; PAGE_SIZE];
        page[0..10].copy_from_slice(&[1, 2, 3, 0, 0, 4, 5, 6, 0, 0]);
        Serializer::shift_page(&mut page, 5, -3).unwrap();
        assert_eq!(page[0..10], vec![1, 2, 4, 5, 6, 0, 0, 0, 0, 0]); //[0, 0, 4, 0, 0, 0, 5, 6, 0, 0]
    }

    #[test]
    fn test_shift_page_no_op() {
        //let mut page = vec![1, 2, 3, 4, 5];
        let mut page = [0; PAGE_SIZE];
        page[0..5].copy_from_slice(&[1, 2, 3, 4, 5]);
        Serializer::shift_page(&mut page, 2, 0).unwrap();
        assert_eq!(page[0..5], vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_bytes_to_ascii() {
        let mut input: [u8; STRING_SIZE] = [0; STRING_SIZE];
        input[..5].copy_from_slice(b"hello");
        let expected = "hello".to_string();
        assert_eq!(Serializer::bytes_to_ascii(input), expected);
    }

    #[test]
    fn test_ascii_to_bytes() {
        let mut expected: [u8; STRING_SIZE] = [0; STRING_SIZE];
        expected[..5].copy_from_slice(b"hello");
        let input = "hello".to_string();
        assert_eq!(Serializer::ascii_to_bytes(&*input), expected);
    }

    #[test]
    fn test_bytes_to_position() {
        let input: [u8; 4] = [0, 0, 0, 1];
        let expected = Position::new(0, 1);
        assert_eq!(Serializer::bytes_to_position(&input), expected);

        let input: [u8; 4] = [1, 0, 0, 0];
        let expected = Position::new(1 << 8, 0);
        assert_eq!(Serializer::bytes_to_position(&input), expected);
    }

    #[test]
    fn test_position_to_bytes() {
        let expected: [u8; 4] = [0, 0, 0, 1];
        let input = Position::new(0, 1);
        assert_eq!(Serializer::position_to_bytes(input), expected);

        let expected: [u8; 4] = [1, 0, 0, 0];
        let input = Position::new(1 << 8, 0);
        assert_eq!(Serializer::position_to_bytes(input), expected);
    }

    #[test]
    fn test_bytes_to_int() {
        let input: [u8; 5] = [0, 0, 0, 0, 42];
        let expected: i32 = 42;
        assert_eq!(Serializer::bytes_to_int(input), expected);

        let input: [u8; 5] = [0, 0, 1, 0, 0];
        let expected: i32 = 1 << 16;
        assert_eq!(Serializer::bytes_to_int(input), expected);
    }

    #[test]
    fn test_int_to_bytes() {
        let expected: [u8; 5] = [0, 0, 0, 0, 42];
        let input: i32 = 42;
        assert_eq!(Serializer::int_to_bytes(input), expected);

        let expected: [u8; 5] = [0, 0, 1, 0, 0];
        let input: i32 = 1 << 16;
        assert_eq!(Serializer::int_to_bytes(input), expected);
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
