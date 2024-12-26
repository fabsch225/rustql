use crate::btree::BTreeNode;
use crate::pager::{Key, PagerAccessor, Position};
use crate::serializer::Serializer;
use crate::status::Status;

pub struct PagerFrontend {}

impl PagerFrontend {
    fn is_leaf(position: Position, interface: PagerAccessor) -> Result<bool, Status> {
        let page = interface
            .access_pager_write(|p| p.access_page_read(position))?;
        Serializer::is_leaf(&page.data)
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


    pub fn set_children(parent: &BTreeNode) -> Result<Vec<BTreeNode>, Status> {
        todo!();
    }

    pub fn get_key(index: usize, parent: &BTreeNode) -> Result<Vec<Key>, Status> {
        todo!();
    }

    pub fn get_keys(parent: &BTreeNode) -> Result<Vec<Key>, Status> {
        todo!();
    }

    pub fn set_keys(parent: &BTreeNode) -> Result<Vec<Key>, Status> {
        todo!();
    }
}