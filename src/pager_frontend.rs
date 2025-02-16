use crate::btree::BTreeNode;
use crate::executor::TableSchema;
use crate::pager::{Key, PagerAccessor, Position, Row};
use crate::serializer::Serializer;
use crate::status::Status;
use crate::status::Status::InternalSuccess;

pub struct PagerFrontend {}

impl PagerFrontend {
    pub fn set_table_root(
        schema: &TableSchema,
        pager_interface: PagerAccessor,
        root_node: &BTreeNode,
    ) -> Result<(), Status> {
        let mut page =
            pager_interface.access_pager_write(|p| p.access_page_read(&root_node.position))?;

        todo!();
        pager_interface.access_page_write(root_node, |d| {
            d.data = page.data;
            Ok(())
        })
    }
    pub fn create_new_table_root(
        schema: &TableSchema,
        pager_interface: PagerAccessor,
    ) -> Result<BTreeNode, Status> {
        let page = pager_interface.access_pager_write(|p| p.create_page())?;
        let cell = 0u16;
        let position = Position { page, cell };
        let node = BTreeNode {
            position,
            pager_accessor: pager_interface.clone(),
            table_schema: schema.clone(),
        };
        Ok(node)
    }
    pub fn switch_nodes(
        schema: &TableSchema,
        pager_interface: PagerAccessor,
        node1: &BTreeNode,
        node2: &BTreeNode,
    ) -> Result<(), Status> {
        todo!();
    }
    pub fn create_node(
        schema: TableSchema,
        pager_interface: PagerAccessor,
        keys: Vec<Key>,
        children: Vec<Position>,
        data: Vec<Row>,
    ) -> Result<BTreeNode, Status> {
        let page_index = pager_interface
            .access_pager_write(|p| p.create_page())
            .expect("error creating page");
        //we should create the node in the same page as its parent, if it fits
        //if not, create a new page!
        todo!();
    }

    pub fn create_node_without_children(
        schema: TableSchema,
        pager_interface: PagerAccessor,
        key: Key,
        data: Row,
    ) -> Result<BTreeNode, Status> {
        Self::create_node(schema, pager_interface, vec![key], vec![], vec![data])
    }

    pub fn is_leaf(position: Position, interface: PagerAccessor) -> Result<bool, Status> {
        let page = interface.access_pager_write(|p| p.access_page_read(&position))?;
        Serializer::is_leaf(&page.data)
    }

    pub fn get_keys_count(node: &BTreeNode) -> Result<usize, Status> {
        //TODO this is very suboptimal
        node.pager_accessor.access_page_read(&node, |d| {
            Serializer::read_keys_as_vec(d, &node.table_schema).map(|v| v.len())
        })
    }

    pub fn get_children_count(node: &BTreeNode) -> Result<usize, Status> {
        node.pager_accessor.access_page_read(&node, |d| {
            Serializer::read_children_as_vec(d, &node.table_schema).map(|v| v.len())
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
        let position = Serializer::read_child(index, &page.data, &parent.table_schema)?;

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
            Serializer::read_children_as_vec(&page.data, &parent.table_schema)?;
        if index >= children_positions.len() {
            panic!("why");
            return Err(Status::InternalExceptionIndexOutOfRange);
        }
        children_positions[index] = child.position;

        Serializer::write_children_vec(&children_positions, &mut page.data, &parent.table_schema)?;

        parent.pager_accessor.access_page_write(parent, |d| {
            d.data = page.data;
            Ok(())
        })
    }

    pub fn get_children(parent: &BTreeNode) -> Result<Vec<BTreeNode>, Status> {
        let page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;
        let positions = Serializer::read_children_as_vec(&page.data, &parent.table_schema)?;

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
        let mut page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;

        Serializer::write_children_vec(
            &children.iter().map(|c| c.position.clone()).collect(),
            &mut page.data,
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
        let keys = Serializer::read_keys_as_vec(&page.data, &parent.table_schema)?;
        let data = Serializer::read_data_by_vec(&page.data, &parent.table_schema)?;
        Ok((keys, data))
    }

    pub fn set_keys(parent: &BTreeNode, keys: Vec<Key>, data: Vec<Row>) -> Result<(), Status> {
        let mut page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;

        Serializer::write_keys_vec_resize_with_rows(
            &keys,
            &data,
            &mut page.data,
            &parent.table_schema,
        )?;

        println!("wrote page: {:?}", page);

        parent.pager_accessor.access_page_write(parent, |d| {
            d.data = page.data;
            Ok(())
        })
    }

    pub fn get_key(index: usize, parent: &BTreeNode) -> Result<(Key, Row), Status> {
        let page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;
        let key = Serializer::read_key(index, &page.data, &parent.table_schema)?;
        let data = Serializer::read_data_by_index(&page.data, index, &parent.table_schema)?;
        Ok((key, data))
    }

    pub fn set_key(index: usize, parent: &BTreeNode, key: Key, data: Row) -> Result<(), Status> {
        let mut page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;
        Serializer::write_key(index, &key, &mut page.data, &parent.table_schema)?;

        Serializer::write_data_by_index(&mut page.data, index, data, &parent.table_schema)?;

        parent.pager_accessor.access_page_write(parent, |d| {
            d.data = page.data;
            Ok(())
        })
    }

    pub fn get_keys_and_children(parent: &BTreeNode) -> Result<(Vec<Key>, Vec<BTreeNode>), Status> {
        let page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;
        let keys = Serializer::read_keys_as_vec(&page.data, &parent.table_schema)?;

        let positions = Serializer::read_children_as_vec(&page.data, &parent.table_schema)?;

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
        let mut page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;

        Serializer::write_keys_vec(&keys, &mut page.data, &parent.table_schema)?;
        Serializer::write_children_vec(
            &children.iter().map(|c| c.position.clone()).collect(),
            &mut page.data,
            &parent.table_schema,
        )?;
        parent.pager_accessor.access_page_write(parent, |d| {
            d.data = page.data;
            Ok(())
        })
    }

    pub fn get_data(node: &BTreeNode) -> Result<Vec<Row>, Status> {
        let page = node
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&node.position))?;
        Serializer::read_data_by_vec(&page.data, &node.table_schema)
    }

    pub fn set_data(node: &BTreeNode, data: Vec<Row>) -> Result<(), Status> {
        let mut page = node
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&node.position))?;

        Serializer::write_data_by_vec(&mut page.data, &data, &node.table_schema)?;
        node.pager_accessor.access_page_write(node, |d| {
            d.data = page.data;
            Ok(())
        })
    }
}
