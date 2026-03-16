use crate::btree::BTreeNode;
use crate::pager::{Key, NODE_METADATA_SIZE, PAGE_SIZE, POSITION_SIZE, PagerAccessor, Position, Row};
use crate::schema::TableSchema;
use crate::serializer::Serializer;
use crate::status::Status;

pub struct PagerProxy {}

impl PagerProxy {
    fn node_size_for_num_keys(schema: &TableSchema, num_keys: usize) -> Result<usize, Status> {
        let key_length = schema.get_key_length()?;
        let row_length = schema.get_row_length()?;
        Ok(NODE_METADATA_SIZE
            + num_keys * (key_length + row_length)
            + (num_keys + 1) * POSITION_SIZE)
    }

    fn count_nodes_on_page(schema: &TableSchema, page_data: &[u8; PAGE_SIZE]) -> Result<usize, Status> {
        let mut count = 0usize;
        let mut offset = 0usize;

        while offset + NODE_METADATA_SIZE <= PAGE_SIZE {
            let num_keys = page_data[offset] as usize;
            let flag = page_data[offset + 1];

            // Unused tail of page
            if num_keys == 0 && flag == 0 {
                break;
            }

            let node_size = Self::node_size_for_num_keys(schema, num_keys)?;
            if offset + node_size > PAGE_SIZE {
                return Err(Status::InternalExceptionIndexOutOfRange);
            }

            count += 1;
            offset += node_size;
        }

        Ok(count)
    }

    pub fn clear_table_root(
        table_schema: &TableSchema,
        pager_interface: PagerAccessor,
    ) -> Result<(), Status> {
        let src_page =
            Serializer::init_page_data_with_children(vec![], vec![], vec![], &table_schema)?;
        let root = BTreeNode {
            position: table_schema.root.clone(),
            pager_accessor: pager_interface.clone(),
            table_schema: table_schema.clone(),
        };
        pager_interface.access_page_write(&root, |pc| {
            Serializer::copy_node(
                &table_schema,
                &root.position,
                &Position::make_empty(),
                &mut pc.data,
                &src_page,
            )?;
            Ok(())
        })
    }

    pub fn set_table_root(
        schema: &TableSchema,
        pager_interface: PagerAccessor,
        node: &BTreeNode,
    ) -> Result<(), Status> {
        let root = BTreeNode {
            position: schema.root.clone(),
            pager_accessor: pager_interface.clone(),
            table_schema: schema.clone(),
        };
        let src_page =
            pager_interface.access_pager_write(|p| p.access_page_read(&node.position))?;
        pager_interface.access_page_write(&root, |pc| {
            Serializer::copy_node(
                &schema,
                &root.position,
                &node.position,
                &mut pc.data,
                &src_page.data,
            )?;
            Ok(())
        })
    }
    pub fn create_empty_node_on_new_page(
        schema: &TableSchema,
        pager_interface: PagerAccessor,
    ) -> Result<BTreeNode, Status> {
        let page = pager_interface.access_pager_write(|p| p.create_page())?;
        let cell = 0;
        let position = Position::new(page, cell);
        Self::create_empty_node_at_position(schema, pager_interface, position)
    }

    fn create_empty_node_at_position(
        schema: &TableSchema,
        pager_interface: PagerAccessor,
        position: Position,
    ) -> Result<BTreeNode, Status> {
        let node = BTreeNode {
            position,
            pager_accessor: pager_interface.clone(),
            table_schema: schema.clone(),
        };

        //create the inital node-flag (set is_leaf to true)
        pager_interface.access_page_write(&node, |d| {
            let node_offset = Serializer::find_position_offset(&d.data, &node.position, schema)?;
            d.data[node_offset + 1] = Serializer::create_node_flag(true);
            Ok(())
        })?;
        Ok(node)
    }

    /// - switches nodes within pages
    /// - in both, parameters, the children are updated
    /// - still, if not carefully used, this could still create cyclic references
    pub fn switch_nodes(
        schema: &TableSchema,
        pager_interface: PagerAccessor,
        node1: &BTreeNode,
        node2: &BTreeNode,
    ) -> Result<(), Status> {
        let switch_on_same_page = node1.position.page() == node2.position.page();
        if switch_on_same_page {
            pager_interface.access_page_write(node1, |p| {
                Serializer::switch_nodes(
                    schema,
                    &node1.position,
                    &node2.position,
                    &mut p.data,
                    None,
                )?;
                Ok(())
            })?;
        } else {
            pager_interface.access_pager_write(|p| {
                let mut page1 = p.access_page_read(&node1.position)?;
                let mut page2 = p.access_page_write(&node2.position)?;
                Serializer::switch_nodes(
                    schema,
                    &node1.position,
                    &node2.position,
                    &mut page1.data,
                    Some(&mut page2.data),
                )?;
                let mut page1_write = p.access_page_write(&node1.position)?;
                page1_write.data = page1.data;
                Ok(())
            })?;
        }
        //search and replace children (only in these 2 nodes)
        let node_1_position = node1.position.clone();
        let node_2_position = node2.position.clone();
        //we assume neither of the nodes had itself as a child on input
        //mind that now the nodes are switched:
        let mut node1_children = PagerProxy::get_children(node1)?;
        let mut change_to_node1_children = false;
        for i in 0..node1_children.len() {
            if node1_children[i].position == node_1_position {
                node1_children[i].position = node_2_position.clone();
                change_to_node1_children = true;
                break; //might as well
            }
        }
        if change_to_node1_children {
            PagerProxy::set_children(node1, node1_children.clone())?;
        }
        let mut node2_children = PagerProxy::get_children(node2)?;
        let mut change_to_node2_children = false;
        for i in 0..node2_children.len() {
            if node2_children[i].position == node_2_position {
                node2_children[i].position = node_1_position.clone();
                change_to_node2_children = true;
                break; //might as well
            }
        }
        if change_to_node2_children {
            PagerProxy::set_children(node2, node2_children)?;
        }

        Ok(())
    }
    pub fn create_node(
        schema: TableSchema,
        pager_interface: PagerAccessor,
        _page_hint: Option<&BTreeNode>,
        keys: Vec<Key>,
        children: Vec<Position>,
        data: Vec<Row>,
    ) -> Result<BTreeNode, Status> {
        if !children.is_empty() {
            let new_node = Self::create_empty_node_on_new_page(&schema, pager_interface.clone())?;
            Self::set_keys_and_children_as_positions(&new_node, keys, children)?;
            Self::set_data(&new_node, data)?;
            return Ok(new_node);
        }

        let page_capacity = schema.max_nodes_per_page()?;

        let mut chosen_position: Option<Position> = None;
        for (page, advertised_free_slots) in &schema.free_list {
            if *advertised_free_slots == 0 {
                continue;
            }
            let used_slots = pager_interface
                .access_pager_write(|p| p.access_page_read(&Position::new(*page, 0)))
                .and_then(|pc| Self::count_nodes_on_page(&schema, &pc.data))?;

            if used_slots < page_capacity {
                chosen_position = Some(Position::new(*page, used_slots));
                break;
            }
        }

        let new_node = if let Some(position) = chosen_position {
            println!("Reusing page {} for new node (used slots: {}, capacity: {})", position.page(), position.cell(), page_capacity);
            Self::create_empty_node_at_position(&schema, pager_interface.clone(), position)?
        } else {
            println!("No suitable page found in free list, creating new page for new node");
            Self::create_empty_node_on_new_page(&schema, pager_interface.clone())?
        };

        Self::set_keys_and_children_as_positions(&new_node, keys, children)?;    
        Self::set_data(&new_node, data)?;
       
        Ok(new_node)
    }

    pub fn create_node_without_children(
        schema: TableSchema,
        pager_interface: PagerAccessor,
        parent: Option<&BTreeNode>,
        key: Key,
        data: Row,
    ) -> Result<BTreeNode, Status> {
        Self::create_node(
            schema,
            pager_interface,
            parent,
            vec![key],
            vec![],
            vec![data],
        )
    }

    pub fn is_leaf(btree_node: &BTreeNode) -> Result<bool, Status> {
        let position = &btree_node.position;
        let interface = btree_node.pager_accessor.clone();
        let table_schema = &btree_node.table_schema;
        let page_container = interface.access_pager_write(|p| p.access_page_read(&position))?;
        Serializer::is_leaf(&page_container.data, position, table_schema)
    }

    pub fn get_keys_count(node: &BTreeNode) -> Result<usize, Status> {
        //TODO this is very suboptimal
        node.pager_accessor
            .access_page_read(&node, |page_container| {
                Serializer::read_keys_as_vec(
                    &page_container.data,
                    &node.position,
                    &node.table_schema,
                )
                .map(|v| v.len())
            })
    }

    pub fn get_children_count(node: &BTreeNode) -> Result<usize, Status> {
        node.pager_accessor
            .access_page_read(&node, |page_container| {
                Serializer::read_children_as_vec(
                    &page_container.data,
                    &node.position,
                    &node.table_schema,
                )
                .map(|v| v.len())
            })
    }

    //this seems useless XD
    pub fn get_node(
        pager_accessor: PagerAccessor,
        table_schema: TableSchema,
        position: Position,
    ) -> Result<BTreeNode, Status> {
        Ok(BTreeNode {
            position,
            pager_accessor,
            table_schema,
        })
    }

    pub fn get_child(index: usize, parent: &BTreeNode) -> Result<BTreeNode, Status> {
        let page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;
        let mut position = match Serializer::read_child(
            index,
            &page.data,
            &parent.position,
            &parent.table_schema,
        ) {
            Ok(p) => p,
            Err(_) => Position::make_empty(),
        };

        if position.is_empty() {
            let children =
                Serializer::read_children_as_vec(&page.data, &parent.position, &parent.table_schema)?;
            if children.is_empty() {
                return Err(Status::InternalExceptionIndexOutOfRange);
            }
            position = if index < children.len() {
                children[index].clone()
            } else {
                children.last().cloned().unwrap()
            };
        }

        Ok(BTreeNode {
            position,
            pager_accessor: parent.pager_accessor.clone(),
            table_schema: parent.table_schema.clone(),
        })
    }

    pub fn set_child(index: usize, parent: &BTreeNode, child: BTreeNode) -> Result<(), Status> {
        let mut page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;

        let mut children_positions =
            Serializer::read_children_as_vec(&page.data, &parent.position, &parent.table_schema)?;
        if index >= children_positions.len() {
            panic!("this should not happen");
            return Err(Status::InternalExceptionIndexOutOfRange);
        }
        children_positions[index] = child.position;

        Serializer::write_children_vec(
            &children_positions,
            &mut page.data,
            &parent.position,
            &parent.table_schema,
        )?;

        parent.pager_accessor.access_page_write(parent, |d| {
            d.data = page.data;
            Ok(())
        })
    }

    pub fn get_children(parent: &BTreeNode) -> Result<Vec<BTreeNode>, Status> {
        let page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;
        let positions =
            Serializer::read_children_as_vec(&page.data, &parent.position, &parent.table_schema)?;

        let mut result = vec![];

        for position in positions {
            result.push(BTreeNode {
                position: position,
                pager_accessor: parent.pager_accessor.clone(),
                table_schema: parent.table_schema.clone(),
            })
        }

        Ok(result)
    }

    pub fn set_children(parent: &BTreeNode, children: Vec<BTreeNode>) -> Result<(), Status> {
        Self::set_children_as_positions(
            parent,
            children.iter().map(|c| c.position.clone()).collect(),
        )
    }

    pub fn set_children_as_positions(
        parent: &BTreeNode,
        children: Vec<Position>,
    ) -> Result<(), Status> {
        let mut page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;

        Serializer::write_children_vec(
            &children,
            &mut page.data,
            &parent.position,
            &parent.table_schema,
        )?;

        parent.pager_accessor.access_page_write(parent, |mut d| {
            d.data = page.data;
            Ok(())
        })
    }

    pub fn get_keys(parent: &BTreeNode) -> Result<(Vec<Key>, Vec<Row>), Status> {
        let page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;
        let keys =
            Serializer::read_keys_as_vec(&page.data, &parent.position, &parent.table_schema)?;
        let data =
            Serializer::read_data_as_vec(&page.data, &parent.position, &parent.table_schema)?;
        Ok((keys, data))
    }

    pub fn set_keys(parent: &BTreeNode, keys: Vec<Key>, data: Vec<Row>) -> Result<(), Status> {
        let mut page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;
        let was_leaf = Serializer::is_leaf(&page.data, &parent.position, &parent.table_schema)?;
        Serializer::write_keys_vec_resize_with_rows(
            &keys,
            &data,
            &mut page.data,
            &parent.position,
            &parent.table_schema,
        )?;

        if was_leaf {
            Serializer::write_children_vec(
                &vec![],
                &mut page.data,
                &parent.position,
                &parent.table_schema,
            )?;
            Serializer::set_is_leaf(
                &mut page.data,
                &parent.position,
                &parent.table_schema,
                true,
            )?;
        }

        parent.pager_accessor.access_page_write(parent, |d| {
            d.free_space = PAGE_SIZE
                - (parent.table_schema.get_key_and_row_length()? + NODE_METADATA_SIZE) * keys.len();
            d.data = page.data;
            Ok(())
        })?;

        Ok(())
    }

    pub fn get_key(index: usize, parent: &BTreeNode) -> Result<(Key, Row), Status> {
        let page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;
        let key = Serializer::read_key(index, &page.data, &parent.position, &parent.table_schema)?;
        let data = Serializer::read_data_by_index(
            index,
            &page.data,
            &parent.position,
            &parent.table_schema,
        )?;
        Ok((key, data))
    }

    pub fn set_key(index: usize, parent: &BTreeNode, key: Key, data: Row) -> Result<(), Status> {
        let mut page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;
        Serializer::write_key(
            index,
            &mut page.data,
            &parent.position,
            &key,
            &parent.table_schema,
        )
        .unwrap();
        Serializer::write_data_by_index(
            index,
            &mut page.data,
            &parent.position,
            data,
            &parent.table_schema,
        )
        .unwrap();

        parent.pager_accessor.access_page_write(parent, |d| {
            d.data = page.data;
            Ok(())
        })
    }

    pub fn get_keys_and_children(parent: &BTreeNode) -> Result<(Vec<Key>, Vec<BTreeNode>), Status> {
        let page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;
        let keys =
            Serializer::read_keys_as_vec(&page.data, &parent.position, &parent.table_schema)?;

        let positions =
            Serializer::read_children_as_vec(&page.data, &parent.position, &parent.table_schema)?;

        let mut children = vec![];

        for position in positions {
            children.push(BTreeNode {
                position,
                pager_accessor: parent.pager_accessor.clone(),
                table_schema: parent.table_schema.clone(),
            })
        }

        Ok((keys, children))
    }

    pub fn set_keys_and_children(
        parent: &BTreeNode,
        keys: Vec<Key>,
        children: Vec<BTreeNode>,
    ) -> Result<(), Status> {
        Self::set_children_as_positions(
            parent,
            children.iter().map(|c| c.position.clone()).collect(),
        )
    }

    pub fn set_keys_and_children_as_positions(
        parent: &BTreeNode,
        keys: Vec<Key>,
        children: Vec<Position>,
    ) -> Result<(), Status> {
        let mut page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;
        Serializer::write_keys_vec(
            &keys,
            &mut page.data,
            &parent.position,
            &parent.table_schema,
        )?;
        Serializer::write_children_vec(
            &children,
            &mut page.data,
            &parent.position,
            &parent.table_schema,
        )?;
        parent.pager_accessor.access_page_write(parent, |d| {
            d.free_space = PAGE_SIZE
                - (parent.table_schema.get_key_and_row_length()? + NODE_METADATA_SIZE) * keys.len();
            d.data = page.data;
            Ok(())
        })?;
        Ok(())
    }

    pub fn get_data(node: &BTreeNode) -> Result<Vec<Row>, Status> {
        let page = node
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&node.position))?;
        Serializer::read_data_as_vec(&page.data, &node.position, &node.table_schema)
    }

    pub fn set_data(node: &BTreeNode, data: Vec<Row>) -> Result<(), Status> {
        let mut page = node
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&node.position))?;

        Serializer::write_data_by_vec(&mut page.data, &node.position, &data, &node.table_schema)?;
        node.pager_accessor.access_page_write(node, |d| {
            d.data = page.data;
            Ok(())
        })
    }
}
