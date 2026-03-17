#[cfg(test)]
mod tests {
    use rustql::btree::BTreeNode;
    use rustql::pager::{Key, PagerAccessor, PagerCore, Position, Row, Type, PAGE_SIZE};
    use rustql::pager_proxy::{PageManager, PagerProxy};
    use rustql::schema::{Field, TableSchema};
    use rustql::serializer::Serializer;
    use std::collections::HashSet;

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
            name: "".to_string(),
            btree_order: 0,
            free_list: vec![],
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
            .map(|_| {
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
            .map(|_| {
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
        let _ = pager_interface.access_page_read(&node, |_data| Ok(()));
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
        let _child = PagerProxy::get_child(0, &node).unwrap();
        //assert_eq!(child.page_position, 261);
    }

    #[test]
    fn test_string_tail_is_offloaded_and_roundtrips() {
        let pager_interface = PagerCore::init_from_file("./default.db.bin").unwrap();
        let node = create_and_insert_mock_btree_node(1, pager_interface.clone());

        let long_name = "abcdefghijklmnopqrstuvwx";
        let row: Row = Serializer::parse_string(long_name).to_vec();

        PagerProxy::set_data(&node, vec![row.clone()]).unwrap();
        let rows = PagerProxy::get_data(&node).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], row);

        let raw_row = pager_interface
            .access_page_read(&node, |pc| {
                Serializer::read_data_by_index(0, &pc.data, &node.position, &node.table_schema)
            })
            .unwrap();

        assert!(Serializer::is_external(&raw_row, &Type::String).unwrap());
        assert_eq!(&raw_row[0..12], b"abcdefghijkl");
        assert_eq!(raw_row[12], 0);

        let ptr = Serializer::bytes_to_position(
            <&[u8; 4]>::try_from(&raw_row[13..17]).expect("invalid pointer bytes"),
        );
        assert!(!ptr.is_empty());

        let data_page = pager_interface
            .access_pager_write(|p| p.access_page_read(&ptr))
            .unwrap();
        assert!(Serializer::is_data_page(&data_page).unwrap());
    }

    #[test]
    fn test_payload_spills_to_overflow_pages() {
        let pager_interface = PagerCore::init_from_file("./default.db.bin").unwrap();
        let payload = vec![42u8; PAGE_SIZE + 128];

        let start = PageManager::write_payload_to_data_pages(pager_interface.clone(), &payload, 0)
            .unwrap();
        assert!(!start.is_empty());

        let restored = PageManager::read_payload_from_pages(pager_interface.clone(), start.clone())
            .unwrap();
        assert_eq!(restored, payload);

        let first_page = pager_interface
            .access_pager_write(|p| p.access_page_read(&start))
            .unwrap();
        assert!(Serializer::is_data_page(&first_page).unwrap());

        let next_page = u16::from_be_bytes([first_page.data[0], first_page.data[1]]) as usize;
        assert!(next_page > 0);

        let overflow_pos = Position::new(next_page, 0);
        let overflow_page = pager_interface
            .access_pager_write(|p| p.access_page_read(&overflow_pos))
            .unwrap();
        assert!(Serializer::is_overflow_page(&overflow_page).unwrap());
    }

    #[test]
    fn test_deprecated_data_pages_are_reused() {
        let pager_interface = PagerCore::init_from_file("./default.db.bin").unwrap();
        let node = create_and_insert_mock_btree_node(1, pager_interface.clone());

        let first_long = Serializer::parse_string(&("A".repeat(48))).to_vec();
        PagerProxy::set_data(&node, vec![first_long]).unwrap();

        let raw_first = pager_interface
            .access_page_read(&node, |pc| {
                Serializer::read_data_by_index(0, &pc.data, &node.position, &node.table_schema)
            })
            .unwrap();
        let first_ptr = Serializer::bytes_to_position(
            <&[u8; 4]>::try_from(&raw_first[13..17]).expect("invalid pointer bytes"),
        );
        assert!(!first_ptr.is_empty());

        let short = Serializer::parse_string("short").to_vec();
        PagerProxy::set_data(&node, vec![short]).unwrap();

        let second_long = Serializer::parse_string(&("B".repeat(64))).to_vec();
        PagerProxy::set_data(&node, vec![second_long]).unwrap();

        let raw_second = pager_interface
            .access_page_read(&node, |pc| {
                Serializer::read_data_by_index(0, &pc.data, &node.position, &node.table_schema)
            })
            .unwrap();
        let second_ptr = Serializer::bytes_to_position(
            <&[u8; 4]>::try_from(&raw_second[13..17]).expect("invalid pointer bytes"),
        );
        assert_eq!(first_ptr.page(), second_ptr.page());
    }

    #[test]
    fn test_mark_unreferenced_payload_pages_as_deleted_marks_data_and_overflow() {
        let pager_interface = PagerCore::init_from_file("./default.db.bin").unwrap();
        let node = create_and_insert_mock_btree_node(1, pager_interface.clone());

        // referenced chain (kept)
        let referenced_long = Serializer::parse_string(&("R".repeat(64))).to_vec();
        PagerProxy::set_data(&node, vec![referenced_long]).unwrap();
        let raw_referenced = pager_interface
            .access_page_read(&node, |pc| {
                Serializer::read_data_by_index(0, &pc.data, &node.position, &node.table_schema)
            })
            .unwrap();
        let referenced_head = Serializer::bytes_to_position(
            <&[u8; 4]>::try_from(&raw_referenced[13..17]).expect("invalid pointer bytes"),
        );

        // orphan chain (to be deleted+deprecated), create with overflow page as well
        let orphan_payload = vec![0xAB; PAGE_SIZE + 64];
        let orphan_head =
            PageManager::write_payload_to_data_pages(pager_interface.clone(), &orphan_payload, 0)
                .unwrap();
        let orphan_first = pager_interface
            .access_pager_write(|p| p.access_page_read(&orphan_head))
            .unwrap();
        let orphan_next_page = u16::from_be_bytes([orphan_first.data[0], orphan_first.data[1]]) as usize;
        assert!(orphan_next_page > 0);

        let mut referenced_heads = HashSet::new();
        referenced_heads.insert(referenced_head.page());

        let marked = PageManager::mark_unreferenced_payload_pages_as_deleted(
            pager_interface.clone(),
            &referenced_heads,
        )
        .unwrap();
        assert!(marked >= 2);

        // orphan chain is now deprecated+deleted
        assert!(
            PageManager::read_payload_from_pages(pager_interface.clone(), orphan_head.clone())
                .is_err()
        );

        let orphan_data_page = pager_interface
            .access_pager_write(|p| p.access_page_read(&Position::new(orphan_head.page(), 0)))
            .unwrap();
        assert!(Serializer::is_deleted(&orphan_data_page).unwrap());

        let orphan_overflow_page = pager_interface
            .access_pager_write(|p| p.access_page_read(&Position::new(orphan_next_page, 0)))
            .unwrap();
        assert!(Serializer::is_deleted(&orphan_overflow_page).unwrap());

        // referenced chain remains readable
        assert!(PageManager::read_payload_from_pages(pager_interface.clone(), referenced_head).is_ok());
    }
}
