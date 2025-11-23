use rustql::pager::{Key, PageData, Position, Row};
use rustql::serializer::Serializer;

#[cfg(test)]
mod tests {
    use rustql::btree::BTreeNode;
    use rustql::pager::{Key, PageData, PagerAccessor, PagerCore, Position, Row, Type};
    use rustql::pager_proxy::PagerProxy;
    use rustql::schema::{Field, TableSchema};
    use rustql::serializer::Serializer;

    fn get_schema() -> TableSchema {
        TableSchema {
            next_position: Position::new(0, 0),
            root: Position::new(0, 0),
            has_key: true,
            key_position: 0,
            fields: vec![
                Field {
                    name: "Id".to_string(),
                    field_type: Type::Integer,
                    table_name: "".to_string(),
                },
                Field {
                    name: "Name".to_string(),
                    field_type: Type::String,
                    table_name: "".to_string(),
                },
            ],
            table_type: 0,
            entry_count: 0,
            name: "".to_string()
        }
    }

    fn create_and_insert_mock_btree_node(
        num_keys: usize,
        pager_interface: PagerAccessor,
    ) -> BTreeNode {
        let schema = get_schema();
        let keys: Vec<Key> = (0..num_keys)
            .map(|i| vec![i as u8; schema.get_key_length().unwrap()])
            .collect();
        let children: Vec<Position> = vec![Position::make_empty(); num_keys + 1];
        let rows: Vec<Row> = (0..num_keys)
            .map(|i| {
                let mut row = vec![0u8; schema.get_row_length().unwrap()];
                row[0..9].copy_from_slice(b"Mock Name");
                row
            })
            .collect();

        PagerProxy::create_node(schema, pager_interface, None, keys, children, rows).unwrap()
    }

    #[test]
    fn test_get_keys_and_rows() {
        let pager_interface = PagerCore::init_from_file("./default.db.bin").unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());
        let (keys, data) = PagerProxy::get_keys(&node).unwrap();
        assert_eq!(keys.len(), 2);
        assert_eq!(data.len(), 2);
    }

    #[test]
    fn test_set_keys_and_rows() {
        let pager_interface = PagerCore::init_from_file("./default.db.bin").unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());
        let new_keys: Vec<Key> = vec![vec![3u8; 5], vec![4u8; 5]];
        let new_data: Vec<Row> = vec![vec![0u8; 256], vec![1u8; 256]];
        PagerProxy::set_keys(&node, new_keys.clone(), new_data.clone()).unwrap();
        let (keys, data) = PagerProxy::get_keys(&node).unwrap();
        assert_eq!(keys, new_keys);
        assert_eq!(data, new_data);
    }

    #[test]
    fn test_get_key_and_row() {
        let pager_interface = PagerCore::init_from_file("./default.db.bin").unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());

        let (key, data) = PagerProxy::get_key(1, &node).unwrap();
        assert_eq!(key, vec![1u8; 5]);
        assert_eq!(data[0..9], b"Mock Name"[..]);
    }

    #[test]
    fn test_set_key_and_row() {
        let pager_interface = PagerCore::init_from_file("./default.db.bin").unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());
        let new_key: Key = vec![5u8; 5];
        let new_data: Row = vec![2u8; 256];
        PagerProxy::set_key(1, &node, new_key.clone(), new_data.clone()).unwrap();
        let (key, data) = PagerProxy::get_key(1, &node).unwrap();
        assert_eq!(key, new_key);
        assert_eq!(data, new_data);
    }

    #[test]
    fn test_get_key() {
        let pager_interface = PagerCore::init_from_file("./default.db.bin").unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());
        let key = PagerProxy::get_key(1, &node).unwrap();
        assert_eq!(key.0, vec![1u8; 5]);
    }

    #[test]
    fn test_get_keys() {
        let pager_interface = PagerCore::init_from_file("./default.db.bin").unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());
        let keys = PagerProxy::get_keys(&node).unwrap();
        assert_eq!(keys.0.len(), 2);
    }

    #[test]
    fn test_set_keys() {
        let pager_interface = PagerCore::init_from_file("./default.db.bin").unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());
        let new_keys = vec![vec![9u8; 5], vec![10u8; 5]];
        let new_rows: Vec<Row> = (0..2)
            .map(|i| {
                let mut row = vec![0u8; get_schema().get_row_length().unwrap()];
                row[0..9].copy_from_slice(b"Mock Name");
                row
            })
            .collect();
        PagerProxy::set_keys(&node, new_keys.clone(), new_rows).unwrap();
        let keys = PagerProxy::get_keys(&node).unwrap();
        assert_eq!(keys.0, new_keys);
    }

    #[test]
    fn test_get_children() {
        let pager_interface = PagerCore::init_from_file("./default.db.bin").unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());
        let children = PagerProxy::get_children(&node).unwrap();
        assert_eq!(children.len(), 0);
    }

    #[test]
    fn test_set_children() {
        let pager_interface = PagerCore::init_from_file("./default.db.bin").unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());
        let child_nodes = vec![
            create_and_insert_mock_btree_node(1, pager_interface.clone()),
            create_and_insert_mock_btree_node(1, pager_interface.clone()),
        ];
        PagerProxy::set_children(&node, child_nodes.clone()).unwrap();
        let children = PagerProxy::get_children(&node).unwrap();
        assert_eq!(children.len(), child_nodes.len());
    }

    #[test]
    fn test_is_leaf() {
        let pager_interface = PagerCore::init_from_file("./default.db.bin").unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());
        pager_interface.access_page_read(&node, |data| Ok(()));
        let is_leaf = PagerProxy::is_leaf(&node).unwrap();
        assert!(is_leaf);
    }

    #[test]
    fn test_get_keys_count() {
        let pager_interface = PagerCore::init_from_file("./default.db.bin").unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());
        let count = PagerProxy::get_keys_count(&node).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_get_children_count() {
        let pager_interface = PagerCore::init_from_file("./default.db.bin").unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());
        let count = PagerProxy::get_children_count(&node).unwrap();
        assert_eq!(count, 0);
        let child_nodes = vec![
            create_and_insert_mock_btree_node(1, pager_interface.clone()),
            create_and_insert_mock_btree_node(1, pager_interface.clone()),
        ];
        PagerProxy::set_children(&node, child_nodes.clone()).unwrap();
        let count = PagerProxy::get_children_count(&node).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_get_child() {
        let pager_interface = PagerCore::init_from_file("./default.db.bin").unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());
        let child_nodes = vec![
            create_and_insert_mock_btree_node(1, pager_interface.clone()),
            create_and_insert_mock_btree_node(1, pager_interface.clone()),
        ];
        PagerProxy::set_children(&node, child_nodes.clone()).unwrap();
        let child = PagerProxy::get_child(0, &node).unwrap();
        //assert_eq!(child.page_position, 261);
    }
}
