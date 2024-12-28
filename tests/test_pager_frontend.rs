use rustql::pager::{Key, PageData, Position, Row};
use rustql::serializer::Serializer;

#[cfg(test)]
mod tests {
    use rustql::btree::BTreeNode;
    use rustql::pager::{Field, Key, PageData, PagerAccessor, PagerCore, Position, Row, TableSchema, Type};
    use rustql::pager_frontend::PagerFrontend;
    use rustql::serializer::Serializer;

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

    fn create_and_insert_mock_btree_node(num_keys: usize, pager_interface: PagerAccessor) -> BTreeNode {
        let schema = get_schema();
        let keys: Vec<Key> = (0..num_keys)
            .map(|i| vec![i as u8; schema.key_length])
            .collect();
        let children: Vec<Position> = vec![0; num_keys + 1];
        let rows: Vec<Row> = (0..num_keys)
            .map(|i| {
                let mut row = vec![0u8; schema.data_length];
                row[0..9].copy_from_slice(b"Mock Name");
                row
            })
            .collect();

        //let page_data = Serializer::init_page_data_with_children(keys.clone(), children.clone(), rows.clone());
        let schema = get_schema();

        let node = pager_interface.access_pager_write(|p| p.create_page(
            keys.clone(),
            children.clone(),
            rows.clone(),
            &schema,
            pager_interface.clone()
        )).unwrap();

        node
    }

    #[test]
    fn test_get_key() {
        let pager_interface = PagerCore::init_from_schema("./default.db.bin", get_schema()).unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());
        let key = PagerFrontend::get_key(1, &node).unwrap();
        assert_eq!(key, vec![1u8; 4]);
    }

    #[test]
    fn test_get_keys() {
        let pager_interface = PagerCore::init_from_schema("./default.db.bin", get_schema()).unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());
        let keys = PagerFrontend::get_keys(&node).unwrap();
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn test_set_keys() {
        let pager_interface = PagerCore::init_from_schema("./default.db.bin", get_schema()).unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());
        let new_keys = vec![vec![9u8; 4], vec![10u8; 4]];
        PagerFrontend::set_keys(&node, new_keys.clone()).unwrap();
        let keys = PagerFrontend::get_keys(&node).unwrap();
        assert_eq!(keys, new_keys);
    }

    #[test]
    fn test_get_children() {
        let pager_interface = PagerCore::init_from_schema("./default.db.bin", get_schema()).unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());
        let children = PagerFrontend::get_children(&node).unwrap();
        assert_eq!(children.len(), 0);
    }

    #[test]
    fn test_set_children() {
        let pager_interface = PagerCore::init_from_schema("./default.db.bin", get_schema()).unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());
        let child_nodes = vec![create_and_insert_mock_btree_node(1, pager_interface.clone()), create_and_insert_mock_btree_node(1, pager_interface.clone())];
        PagerFrontend::set_children(&node, child_nodes.clone()).unwrap();
        println!("{:?}", child_nodes);
        let children = PagerFrontend::get_children(&node).unwrap();
        println!("{:?}", children);
        assert_eq!(children.len(), child_nodes.len());
    }

    #[test]
    fn test_is_leaf() {
        let pager_interface = PagerCore::init_from_schema("./default.db.bin", get_schema()).unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());
        let is_leaf = PagerFrontend::is_leaf(node.page_position, node.pager_interface.clone()).unwrap();
        assert!(is_leaf);
    }

    #[test]
    fn test_get_keys_count() {
        let pager_interface = PagerCore::init_from_schema("./default.db.bin", get_schema()).unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());
        let count = PagerFrontend::get_keys_count(&node).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_get_children_count() {
        let pager_interface = PagerCore::init_from_schema("./default.db.bin", get_schema()).unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());
        let count = PagerFrontend::get_children_count(&node).unwrap();
        assert_eq!(count, 0);
        let child_nodes = vec![create_and_insert_mock_btree_node(1, pager_interface.clone()), create_and_insert_mock_btree_node(1, pager_interface.clone())];
        PagerFrontend::set_children(&node, child_nodes.clone()).unwrap();
        let count = PagerFrontend::get_children_count(&node).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_get_child() {
        let pager_interface = PagerCore::init_from_schema("./default.db.bin", get_schema()).unwrap();
        let node = create_and_insert_mock_btree_node(2, pager_interface.clone());
        let child = PagerFrontend::get_child(0, &node).unwrap();
        assert_eq!(child.page_position, 0);
    }
}