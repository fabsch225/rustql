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
        )).expect("error creating page");

        Ok(node)
    }

    pub fn create_singular_node(schema: TableSchema, pager_interface: PagerAccessor, key: Key, data: Row) -> Result<BTreeNode, Status> {
        Self::create_node(schema, pager_interface, vec![key], vec![], vec![data])
    }

    pub fn is_leaf(position: Position, interface: PagerAccessor) -> Result<bool, Status> {
        let page = interface
            .access_pager_write(|p| p.access_page_read(position))?;
        Serializer::is_leaf(&page.data, &interface.schema)
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

    pub fn set_child(index: usize, parent: &BTreeNode, child: BTreeNode) -> Result<(), Status> {
        let parent_position = parent.page_position;
        let mut page = parent
            .pager_interface
            .access_pager_write(|p| p.access_page_read(parent_position))?;

        let mut children_positions = Serializer::read_children_as_vec(&page.data, &parent.schema)?;
        if index >= children_positions.len() {
            return Err(Status::InternalExceptionIndexOutOfRange);
        }
        children_positions[index] = child.page_position;

        let result = Serializer::write_children_vec(&children_positions, &mut page.data, &parent.schema);
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

    pub fn get_keys_and_children(parent: &BTreeNode) -> Result<(Vec<Key>, Vec<BTreeNode>), Status> {
        let parent_position = parent.page_position;
        let page = parent
            .pager_interface
            .access_pager_write(|p| p.access_page_read(parent_position))?;
        let keys = Serializer::read_keys_as_vec(&page.data, &parent.schema)?;

        let positions = Serializer::read_children_as_vec(&page.data, &parent.schema)?;

        let mut children = vec![];

        for position in positions {
            children.push(BTreeNode{
                page_position: position,
                is_leaf: Self::is_leaf(position, parent.pager_interface.clone())?,
                pager_interface: parent.pager_interface.clone(),
                schema: parent.schema.clone(),
            })
        }

        Ok((keys, children))
    }

    pub fn set_keys_and_children(parent: &BTreeNode, keys: Vec<Key>, children: Vec<BTreeNode>) -> Result<(), Status> {
        let page_position = parent.page_position;
        let mut page = parent
            .pager_interface
            .access_pager_write(|p| p.access_page_read(page_position))?;

        let result = Serializer::write_keys_vec(&keys, &mut page.data, &parent.schema);
        if result != InternalSuccess {
            return Err(result);
        }

        let result = Serializer::write_children_vec(&children.iter().map(|c|c.page_position).collect(), &mut page.data, &parent.schema);
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

    pub fn merge(x: &BTreeNode, i: usize, t: usize) -> Result<(), Status> {
        let mut child = Self::get_child(i, x)?;
        let sibling = Self::get_child(i + 1, x)?;

        let key = Self::get_key(i, x)?;
        let mut child_keys = Self::get_keys(&child)?;
        child_keys.push(key);
        let sibling_keys = Self::get_keys(&sibling)?;
        child_keys.extend(sibling_keys);
        Self::set_keys(&child, child_keys)?;

        if !child.is_leaf {
            let mut child_children = Self::get_children(&child)?;
            let sibling_children = Self::get_children(&sibling)?;
            child_children.extend(sibling_children);
            Self::set_children(&child, child_children)?;
        }

        let mut x_keys = Self::get_keys(x)?;
        x_keys.remove(i);
        Self::set_keys(x, x_keys)?;

        let mut x_children = Self::get_children(x)?;
        x_children.remove(i + 1);
        Self::set_children(x, x_children)?;

        Self::set_child(i, x, child)?;

        Ok(())
    }

    /// Borrows a key from the previous sibling and moves it to the child at index `i` of the node `x`.
    pub fn borrow_from_prev(x: &BTreeNode, i: usize) -> Result<(), Status> {
        let mut child = Self::get_child(i, x)?;
        let sibling = Self::get_child(i - 1, x)?;

        let key = Self::get_key(i - 1, x)?;
        let mut child_keys = Self::get_keys(&child)?;
        child_keys.insert(0, key);
        if !sibling.is_leaf {
            let mut child_children = Self::get_children(&child)?;
            let sibling_children = Self::get_children(&sibling)?;
            child_children.insert(0, sibling_children[sibling_children.len() - 1].clone());
            Self::set_children(&child, child_children)?;
        }
        Self::set_keys(&child, child_keys)?;

        let mut sibling_keys = Self::get_keys(&sibling)?;
        let sibling_key = sibling_keys.pop().unwrap();
        Self::set_keys(&sibling, sibling_keys)?;
        let mut x_keys = Self::get_keys(x)?;
        x_keys[i - 1] = sibling_key;
        Self::set_keys(x, x_keys)?;

        Ok(())
    }

    pub fn borrow_from_next(x: &BTreeNode, i: usize) -> Result<(), Status> {
        let mut child = Self::get_child(i, x)?;
        let sibling = Self::get_child(i + 1, x)?;

        // Move the key from x to the child
        let key = Self::get_key(i, x)?;
        let mut child_keys = Self::get_keys(&child)?;
        child_keys.push(key);
        if !sibling.is_leaf {
            let mut child_children = Self::get_children(&child)?;
            let sibling_children = Self::get_children(&sibling)?;
            child_children.push(sibling_children[0].clone());
            Self::set_children(&child, child_children)?;
        }
        Self::set_keys(&child, child_keys)?;

        // Move the key from the sibling to x
        let mut sibling_keys = Self::get_keys(&sibling)?;
        let sibling_key = sibling_keys.remove(0);
        Self::set_keys(&sibling, sibling_keys)?;
        let mut x_keys = Self::get_keys(x)?;
        x_keys[i] = sibling_key;
        Self::set_keys(x, x_keys)?;

        Ok(())
    }
}