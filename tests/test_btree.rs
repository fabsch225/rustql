#[cfg(test)]
mod tests {
    use rustql::btree::{BTreeNode, Btree};
    use rustql::pager::{Field, Key, PagerAccessor, PagerCore, Position, Row, TableSchema, Type};

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

    fn create_and_insert_mock_btree_node(
        num_keys: usize,
        pager_interface: PagerAccessor,
    ) -> BTreeNode {
        let schema = get_schema();
        let keys: Vec<Key> = (0..num_keys)
            .map(|i| vec![i as u8; schema.key_length])
            .collect();
        let children: Vec<Position> = vec![0; num_keys + 1];
        let rows: Vec<Row> = (0..num_keys)
            .map(|i| {
                let mut row = vec![0u8; schema.row_length];
                row[0..9].copy_from_slice(b"Mock Name");
                row
            })
            .collect();

        let node = pager_interface
            .access_pager_write(|p| {
                p.create_page(
                    keys.clone(),
                    children.clone(),
                    rows.clone(),
                    &schema,
                    pager_interface.clone(),
                )
            })
            .unwrap();

        node
    }

    #[test]
    fn test_scan() {
        let pager_interface =
            PagerCore::init_from_schema("./default.db.bin", get_schema()).unwrap();
        let mut btree = Btree::new(2, pager_interface.clone());
        let node = create_and_insert_mock_btree_node(5, pager_interface.clone());
        btree.root = Some(node);

        let (keys, rows) = btree.scan();
        assert_eq!(keys.len(), 5);
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn test_insert() {
        let pager_interface =
            PagerCore::init_from_schema("./default.db.bin", get_schema()).unwrap();
        let mut btree = Btree::new(2, pager_interface.clone());

        let key: Key = vec![1, 0, 0, 0];
        let mut row: Row = vec![0u8; 256];
        row[0..9].copy_from_slice(b"Mock Name");

        btree.insert(key.clone(), row.clone());

        let (keys, rows) = btree.scan();
        assert_eq!(keys.len(), 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(keys[0], key);
        assert_eq!(rows[0], row);
    }

    #[test]
    fn test_delete() {
        let pager_interface =
            PagerCore::init_from_schema("./default.db.bin", get_schema()).unwrap();
        let mut btree = Btree::new(2, pager_interface.clone()).unwrap();

        for i in 0..4 {
            let key: Key = vec![i, 0, 0, 0];
            let mut row: Row = vec![0u8; 256];
            row[0..9].copy_from_slice(b"Mock Name");

            btree.insert(key.clone(), row.clone());
        }
        btree.delete(vec![2, 0, 0, 0]);

        let (keys, rows) = btree.scan();
        assert_eq!(keys.len(), 3);
        assert_eq!(rows.len(), 3);
        assert_eq!(keys.binary_search(&vec![0, 0, 0, 0]).unwrap(), 0 as usize);
        assert_eq!(keys.binary_search(&vec![1, 0, 0, 0]).unwrap(), 1 as usize);
        assert_eq!(keys.binary_search(&vec![3, 0, 0, 0]).unwrap(), 2 as usize);
    }

    #[test]
    fn test_find_range() {
        let pager_interface =
            PagerCore::init_from_schema("./default.db.bin", get_schema()).unwrap();
        let mut btree = Btree::new(2, pager_interface.clone()).expect("");

        for i in 0..10 {
            let key: Key = vec![i, 0, 0, 0];
            let mut row: Row = vec![0u8; 256];
            row[0..9].copy_from_slice(b"Mock Name");

            btree.insert(key.clone(), row.clone());
        }

        let (keys, rows) = btree
            .find_range(vec![1, 0, 0, 0], vec![3, 0, 0, 0], false, false)
            .unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(rows.len(), 1);

        let (keys, rows) = btree
            .find_range(vec![1, 0, 0, 0], vec![3, 0, 0, 0], true, false)
            .unwrap();
        assert_eq!(keys.len(), 2);
        assert_eq!(rows.len(), 2);

        let (keys, rows) = btree
            .find_range(vec![1, 0, 0, 0], vec![4, 0, 0, 0], false, true)
            .unwrap();
        assert_eq!(keys.len(), 3);
        assert_eq!(rows.len(), 3);

        let (keys, rows) = btree
            .find_range(vec![1, 0, 0, 0], vec![20, 0, 0, 0], false, true)
            .unwrap();
        assert_eq!(keys.len(), 8);
        assert_eq!(rows.len(), 8);

        let (keys, rows) = btree
            .find_range(vec![7, 0, 0, 0], vec![20, 0, 0, 0], true, true)
            .unwrap();
        assert_eq!(keys.len(), 3);
        assert_eq!(rows.len(), 3);

        let (keys, rows) = btree
            .find_range(vec![1, 0, 0, 0], vec![3, 0, 0, 0], true, true)
            .unwrap();
        assert_eq!(keys.len(), 3);
        assert_eq!(rows.len(), 3);
    }
}
