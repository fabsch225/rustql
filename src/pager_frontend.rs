use crate::btree::BTreeNode;
use crate::pager::{Key, PagerAccessor, Position, Row, TableSchema};
use crate::serializer::Serializer;
use crate::status::Status;
use crate::status::Status::InternalSuccess;

pub struct PagerFrontend {}

impl PagerFrontend {
    pub fn create_node(schema: TableSchema, pager_interface: PagerAccessor, keys: Vec<Key>, children: Vec<Position>, data: Vec<Row>) -> Result<BTreeNode, Status> {
        let node = pager_interface.access_pager_write(|p| p.create_page(
            keys,
            children,
            data,
            &schema,
            pager_interface.clone()
        ))?;

        Ok(BTreeNode {
            page_position: node.page_position,
            is_leaf: node.is_leaf,
            pager_interface: pager_interface.clone(),
            schema: schema.clone(),
        })
    }
    pub fn create_singular_node(schema: TableSchema, pager_interface: PagerAccessor, key: Key, data: Row) -> Result<BTreeNode, Status> {
        Self::create_node(schema, pager_interface, vec![key], vec![], vec![data])
    }
    pub fn is_leaf(position: Position, interface: PagerAccessor) -> Result<bool, Status> {
        let page = interface
            .access_pager_write(|p| p.access_page_read(position))?;
        Serializer::is_leaf(&page.data)
    }
    pub fn get_keys_count(node: &BTreeNode) -> Result<usize, Status> {
        //TODO this is very suboptimal
        node.pager_interface.access_page_read(&node, |d, t|Serializer::read_keys_as_vec(d, t).map(|v|v.len()))
    }
    pub fn get_children_count(node: &BTreeNode) -> Result<usize, Status> {
        node.pager_interface.access_page_read(&node, |d, t|Serializer::read_children_as_vec(d, t).map(|v|v.len()))
    }
    pub fn get_child(index: usize, parent: &BTreeNode) -> Result<BTreeNode, Status> {
        //TODO Error handling
        let parent_position = parent.page_position;
        let page = parent
            .pager_interface
            .access_pager_write(|p| p.access_page_read(parent_position))?;
        let position = Serializer::read_child(index, &page.data, &parent.schema)?;

        Ok(BTreeNode {
            page_position: position,
            is_leaf: Self::is_leaf(position, parent.pager_interface.clone())?,
            pager_interface: parent.pager_interface.clone(),
            schema: parent.schema.clone(),
        })
    }

    pub fn get_children(parent: &BTreeNode) -> Result<Vec<BTreeNode>, Status> {
        let parent_position = parent.page_position;
        let page = parent
            .pager_interface
            .access_pager_write(|p| p.access_page_read(parent_position))?;
        let positions = Serializer::read_children_as_vec(&page.data, &parent.schema)?;

        let mut result = vec![];

        for position in positions {
            result.push(BTreeNode{
                page_position: position,
                is_leaf: Self::is_leaf(position, parent.pager_interface.clone())?,
                pager_interface: parent.pager_interface.clone(),
                schema: parent.schema.clone(),
            })
        }

        Ok(result)
    }

    pub fn set_children(parent: &BTreeNode, children: Vec<BTreeNode>) -> Result<(), Status> {
        let parent_position = parent.page_position;
        let mut page = parent
            .pager_interface
            .access_pager_write(|p| p.access_page_read(parent_position))?;

        let result = Serializer::write_children_vec(&children.iter().map(|c|c.page_position).collect(), &mut page.data, &parent.schema);
        if result != InternalSuccess {
            return Err(result);
        }

        /*let result = parent.pager_interface.access_pager_write(|p| p.access_page_write(parent_position));
        if result.is_ok() {
            result?.data = page.data;
            Ok(())
        } else {
            Err(result.unwrap_err())
        }*/
        //more elegant:
        let result = parent.pager_interface.access_page_write(parent, |mut d, s|{d.data = page.data; InternalSuccess});
        if result != InternalSuccess {
            Err(result)
        } else {
            Ok(())
        }
    }

    pub fn get_key(index: usize, parent: &BTreeNode) -> Result<Key, Status> {
        let parent_position = parent.page_position;
        let page = parent
            .pager_interface
            .access_pager_write(|p| p.access_page_read(parent_position))?;
        Serializer::read_key(index, &page.data, &parent.schema)
    }

    pub fn get_keys(parent: &BTreeNode) -> Result<Vec<Key>, Status> {
        let parent_position = parent.page_position;
        let page = parent
            .pager_interface
            .access_pager_write(|p| p.access_page_read(parent_position))?;
        Serializer::read_keys_as_vec(&page.data, &parent.schema)
    }

    //TODO this will not extend the keys.
    pub fn set_keys(parent: &BTreeNode, keys: Vec<Key>) -> Result<(), Status> {
        let parent_position = parent.page_position;
        let mut page = parent
            .pager_interface
            .access_pager_write(|p| p.access_page_read(parent_position))?;

        let result = Serializer::write_keys_vec(&keys, &mut page.data, &parent.schema);
        if result != InternalSuccess {
            return Err(result);
        }

        let result = parent.pager_interface.access_page_write(parent, |d, s| { d.data = page.data; InternalSuccess });
        if result != InternalSuccess {
            Err(result)
        } else {
            Ok(())
        }
    }
}