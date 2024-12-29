use std::fmt::{Debug, Formatter};
use crate::pager::{Key, PageContainer, PagerCore, PagerAccessor, Position, Row, TableSchema};
use crate::serializer::Serializer;
use crate::pager_frontend::PagerFrontend;
use crate::status::Status;
//TODO implement error handling with unwraps or else
//TODO implement interaction with Node only with getters and setters
//TODO move the data along with the keys

//CREATE TABLE
//WHERE

//BTree and BTreeNode will end up as facades for the pager

//Thought: If we balance children, we will move a Childs Position to another Node,
//should the Page -> The Actual Child also be moved!?
// => Yes, and there are several approaches to this: Indirection Layer, Periodical Compaction
//wont do this for now

#[derive(Clone, Debug)]
pub struct BTreeNode {
    pub page_position: Position,          // Unique ID for the node (corresponds to a page in the pager)
    pub is_leaf: bool,          // Indicates if the node is a leaf
    //#### dont cache this stuff **twice**. use f.ex. pager_interface -> readKeysFromCache() -> ((modify the vector as needed)) -> writeKeysToCache(vec) [or is it not a cache but a buffer!?]
    //pub keys: Vec<Key>,         // Cached keys (loaded from pager)
    //pub children: Vec<Position>,    // Child page IDs (loaded from pager)
    pub pager_interface: PagerAccessor,           // Reference to the pager for disk-backed storage

    pub schema: TableSchema,
    //maybe dont store so much in memory! !? :)
    //dont store everything double in memory. TODO: concept clean up system in the btree (wont rust to that by itself!?), for example, when getting a new node, check if we can delete the old one!
    //#### doing the same: implement getters and setters on the page cache
    //pub data: Vec<Row>

    //Thought why dont i store the pages in seperated Vecs!?!?!? --> lets not do this, then memory is more efficient...
}

impl BTreeNode {
    fn is_leaf(&self) -> bool {
       PagerFrontend::is_leaf(self.page_position, self.pager_interface.clone()).unwrap()
    }
    fn get_keys_count(&self) -> Result<usize, Status> {
        PagerFrontend::get_keys_count(&self)
    }
    fn get_children_count(&self) -> Result<usize, Status> {
        PagerFrontend::get_children_count(&self)
    }
    fn get_keys_from(&self, index: usize) -> Result<Vec<Key>, Status> {
        PagerFrontend::get_keys(&self).map(|v|v[index..].to_vec())
        //self.keys[index..].to_vec()
    }
    fn get_key(&self, index: usize) -> Result<Key, Status> {
        PagerFrontend::get_key(index, &self)
    }
    fn set_key(&self, index: usize, key: Key) -> Result<(), Status> {
        let mut keys = self.get_keys()?.clone();
        keys[index] = key;
        PagerFrontend::set_keys(self, keys)
    }

    fn set_keys(&self, keys: Vec<Key>) -> Result<(), Status> {
        PagerFrontend::set_keys(self, keys)
    }

    fn get_keys(&self) -> Result<Vec<Key>, Status> {
        PagerFrontend::get_keys(self)
    }

    fn remove_key(&self, index: usize) -> Result<Key, Status> {
        let mut keys = self.get_keys()?.clone();
        let removed_key = keys.remove(index);
        PagerFrontend::set_keys(self, keys)?;
        Ok(removed_key)
    }

    fn exchange_key(&self, prev_key: Key, key: Key) -> Result<(), Status> {
        let mut keys = self.get_keys()?.clone();
        if let Some(index) = keys.iter().position(|x| *x == prev_key) {
            keys[index] = key;
            PagerFrontend::set_keys(self, keys)
        } else {
            Err(Status::InternalExceptionKeyNotFound)
        }
    }

    fn push_key(&self, key: Key) -> Result<(), Status> {
        let mut keys = self.get_keys()?.clone();
        keys.push(key);
        PagerFrontend::set_keys(self, keys)
    }

    fn extend_keys(&self, keys: Vec<Key>) -> Result<(), Status> {
        let mut current_keys = self.get_keys()?.clone();
        current_keys.extend(keys);
        PagerFrontend::set_keys(self, current_keys)
    }

    fn truncate_keys(&self, index: usize) -> Result<(), Status> {
        let mut keys = self.get_keys()?.clone();
        keys.truncate(index);
        PagerFrontend::set_keys(self, keys)
    }

    fn set_children(&self, children: Vec<BTreeNode>) -> Result<(), Status> {
        PagerFrontend::set_children(self, children)
    }

    pub fn get_children(&self) -> Result<Vec<BTreeNode>, Status> {
        PagerFrontend::get_children(self)
    }

    fn get_children_from(&self, index: usize) -> Result<Vec<BTreeNode>, Status> {
        let children = PagerFrontend::get_children(self)?;
        Ok(children[index..].to_vec())
    }

    fn get_child(&self, index: usize) -> Result<BTreeNode, Status> {
        PagerFrontend::get_child(index, self)
    }

    fn set_child(&self, index: usize, child: BTreeNode) -> Result<(), Status> {
        let mut children = PagerFrontend::get_children(self)?;
        children[index] = child;
        PagerFrontend::set_children(self, children)
    }

    fn remove_child(&self, index: usize) -> Result<BTreeNode, Status> {
        let mut children = PagerFrontend::get_children(self)?;
        let removed_child = children.remove(index);
        PagerFrontend::set_children(self, children)?;
        Ok(removed_child)
    }

    fn push_child(&self, child: BTreeNode) -> Result<(), Status> {
        let mut children = PagerFrontend::get_children(self)?;
        children.push(child);
        PagerFrontend::set_children(self, children)
    }

    #[deprecated]
    fn truncate_children(&self, index: usize) -> Result<(), Status> {
        let mut children = PagerFrontend::get_children(self)?;
        children.truncate(index);
        PagerFrontend::set_children(self, children)
    }

    fn extend_children(&self, children: Vec<BTreeNode>) -> Result<(), Status> {
        let mut current_children = PagerFrontend::get_children(self)?;
        current_children.extend(children);
        PagerFrontend::set_children(self, current_children)
    }

    fn extend_over_children(&self, index_from: usize, index_to: usize) -> Result<(), Status> {
        let mut children = PagerFrontend::get_children(self)?;
        let new_children = children[index_to].get_children()?.clone();
        children[index_from].extend_children(new_children)?;
        PagerFrontend::set_children(self, children)
    }

    fn extend_over_keys(&self, index_from: usize, index_to: usize) -> Result<(), Status> {
        let mut children = PagerFrontend::get_children(self)?;
        let new_keys = children[index_to].get_keys()?.clone();
        children[index_from].extend_keys(new_keys)?;
        PagerFrontend::set_children(self, children)
    }

    fn insert_key(&self, index: usize, key: Key) -> Result<(), Status> {
        let mut keys = self.get_keys()?.clone();
        keys.insert(index, key);
        PagerFrontend::set_keys(self, keys)
    }

    fn insert_child(&self, index: usize, child: BTreeNode) -> Result<(), Status> {
        let mut children = PagerFrontend::get_children(self)?;
        children.insert(index, child);
        PagerFrontend::set_children(self, children)
    }

    fn child_insert_key(&self, index: usize, sub_index: usize, key: Key) -> Result<(), Status> {
        let mut children = PagerFrontend::get_children(self)?;
        children[index].insert_key(sub_index, key)?;
        PagerFrontend::set_children(self, children)
    }

    fn child_push_key(&self, index: usize, key: Key) -> Result<(), Status> {
        let mut children = PagerFrontend::get_children(self)?;
        children[index].push_key(key)?;
        PagerFrontend::set_children(self, children)
    }

    fn child_pop_key(&self, index: usize) -> Result<Option<Key>, Status> {
        let mut children = PagerFrontend::get_children(self)?;
        let key = children[index].remove_key(children[index].get_keys_count()? - 1)?;
        PagerFrontend::set_children(self, children)?;
        Ok(Some(key))
    }

    fn child_pop_first_key(&self, index: usize) -> Result<Option<Key>, Status> {
        let mut children = PagerFrontend::get_children(self)?;
        let key = children[index].remove_key(0)?;
        PagerFrontend::set_children(self, children)?;
        Ok(Some(key))
    }

    fn children_move_key_left(&self, to_index: usize, from_index: usize) -> Result<(), Status> {
        let mut children = PagerFrontend::get_children(self)?;
        let key = children[from_index].remove_key(children[from_index].get_keys_count()? - 1)?;
        children[to_index].insert_key(0, key)?;
        PagerFrontend::set_children(self, children)
    }

    fn children_move_child_left(&self, to_index: usize, from_index: usize) -> Result<(), Status> {
        let mut children = PagerFrontend::get_children(self)?;
        let child = children[from_index].remove_child(children[from_index].get_children_count()? - 1)?;
        children[to_index].insert_child(0, child)?;
        PagerFrontend::set_children(self, children)
    }

    fn children_move_key_right(&self, to_index: usize, from_index: usize) -> Result<(), Status> {
        let mut children = PagerFrontend::get_children(self)?;
        let key = children[from_index].remove_key(0)?;
        children[to_index].push_key(key)?;
        PagerFrontend::set_children(self, children)
    }

    fn children_move_child_right(&self, to_index: usize, from_index: usize) -> Result<(), Status> {
        let mut children = PagerFrontend::get_children(self)?;
        let child = children[from_index].remove_child(0)?;
        children[to_index].push_child(child)?;
        PagerFrontend::set_children(self, children)
    }
}

pub struct Btree {
    pub root: Option<BTreeNode>,
    pub t: usize, // Minimum degree
    pub table_schema: TableSchema,
    pub pager_accessor: PagerAccessor
}

impl Btree {
    pub fn new(t: usize, table_schema: TableSchema, pager_accessor: PagerAccessor) -> Self {
        Btree {
            root: None,
            t,
            table_schema,
            pager_accessor
        }
    }

    pub fn insert(&mut self, k: Key, v: Row) {
        if let Some(ref mut root) = self.root {
            if root.get_keys_count().unwrap() == (2 * self.t) - 1 {
                //dummy key for now
                let mut new_root = PagerFrontend::create_singular_node(self.table_schema.clone(), self.pager_accessor.clone(), k.clone(), v).unwrap();
                new_root.push_child(root.clone()).unwrap();
                Btree::split_child(&new_root, 0, self.t, true);

                Btree::insert_non_full(&new_root, k, self.t);
                self.root = Some(new_root);

            } else {
                Btree::insert_non_full(root, k, self.t);
            }
        } else {
            self.root = Some(PagerFrontend::create_singular_node(self.table_schema.clone(), self.pager_accessor.clone(), k.clone(), v).unwrap());
        }
    }

    fn insert_non_full(x: &BTreeNode, k: Key, t: usize) {
        let mut i = x.get_keys_count().unwrap() as isize - 1;
        if x.is_leaf() {
            x.push_key(k.clone()); // Add a dummy value
            //while i >= 0 && k < *x.get_key(i as usize).unwrap() {
            while i >= 0 && Serializer::compare(&k, &x.get_key(i as usize).unwrap()).unwrap() == std::cmp::Ordering::Less {
                x.set_key((i + 1) as usize, x.get_key(i as usize).unwrap().clone());
                i -= 1;
            }
            x.set_key((i + 1) as usize, k);
        } else {
            //while i >= 0 && k < *x.get_key(i as usize).unwrap() {
            while i >= 0 && Serializer::compare(&k, &x.get_key(i as usize).unwrap()).unwrap() == std::cmp::Ordering::Less {
                i -= 1;
            }
            let mut i = (i + 1) as usize;
            if x.get_child(i).unwrap().get_keys_count().unwrap() == (2 * t) - 1 {
                Btree::split_child(x, i, t, false);
                //if k > *x.get_key(i).unwrap() {
                if Serializer::compare(&k, &x.get_key(i).unwrap()).unwrap() == std::cmp::Ordering::Greater {
                    i += 1;
                }
            }
            Btree::insert_non_full(&x.get_child(i).unwrap(), k, t);
        }
    }

    fn split_child(x: &BTreeNode, i: usize, t: usize, is_root: bool) {
        let mut y = x.get_child(i).unwrap().clone();
        let keys = y.get_keys_from(t).unwrap();
        //TODO add data
        //let v = y.get(t).unwrap();
        let mut z = PagerFrontend::create_node(y.schema.clone(), y.pager_interface.clone(), keys, vec![], vec![]).unwrap();

        //TODO suboptimal
        if is_root {
            x.set_key(0, y.get_key(t - 1).unwrap().clone());
        } else {
            x.insert_key(i, y.get_key(t - 1).unwrap().clone());
        }

        if !y.is_leaf {
            z.set_children(y.get_children_from(t).unwrap()).unwrap();
            //truncate_keys will do that aswell
            //y.truncate_children(t).unwrap();
        }

        y.truncate_keys(t - 1).unwrap();

        x.insert_child(i + 1, z).unwrap();
    }

    //force borrow checker here...
    pub fn delete(&mut self, k: Key) {
        if let Some(ref root) = self.root {
            Self::delete_from(root, k, self.t);
        } else {
            panic!();
        }
    }

    fn delete_from(x: &BTreeNode, k: Key, t: usize) {
        let mut i = 0;
        //while i < x.get_keys_count() && k > *x.get_key(i).unwrap() {
        while i < x.get_keys_count().unwrap() && Serializer::compare(&k, &x.get_key(i).unwrap()).unwrap() == std::cmp::Ordering::Greater {
            i += 1;
        }

        if x.is_leaf() {
            // Case 1: Node is a leaf
            if i < x.get_keys_count().unwrap() && k == *x.get_key(i).unwrap() {
                x.remove_key(i);
            }
        } else {
            // Case 2: Key is in an internal node
            if i < x.get_keys_count().unwrap() && k == *x.get_key(i).unwrap() {
                Btree::delete_internal_node(x, k, i, t);
            } else {
                if x.get_child(i).unwrap().get_keys_count().unwrap() < t {
                    Btree::fill(x, i, t);
                }
                Btree::delete_from(&x.get_child(i).unwrap(), k, t);
            }
        }
    }

    fn delete_internal_node(x: &BTreeNode, k: Key, i: usize, t: usize) {
        if x.get_child(i).unwrap().get_keys_count().unwrap() >= t {
            let pred = Btree::get_predecessor(&x.get_child(i).unwrap());
            x.set_key(i, pred.clone());
            Btree::delete_from(&mut x.get_child(i).unwrap(), pred, t);
        } else if x.get_child(i + 1).unwrap().get_keys_count().unwrap() >= t {
            let succ = Btree::get_successor(&x.get_child(i + 1).unwrap());
            x.set_key(i, succ.clone());
            Btree::delete_from(&mut x.get_child(i + 1).unwrap(), succ, t);
        } else {
            Btree::merge(x, i, t);
            Btree::delete_from(&mut x.get_child(i).unwrap(), k, t);
        }
    }

    //TODO optimize this!!!!
    fn get_predecessor(x: &BTreeNode) -> Key {
        let mut cur = x.clone();
        while !cur.is_leaf() {
            cur = cur.get_child(cur.get_children_count().unwrap() - 1).unwrap().clone();
        }
        cur.get_key(cur.get_keys_count().unwrap() - 1).unwrap().clone()
    }

    //TODO optimize this!!!!
    fn get_successor(x: &BTreeNode) -> Key {
        let mut cur = x.clone();
        while !cur.is_leaf() {
            cur = cur.get_child(0).unwrap().clone();
        }
        cur.get_key(0).unwrap().clone()
    }

    #[deprecated]
    fn merge(x: &BTreeNode, i: usize, t: usize) {
        let child = x.get_child(i).unwrap().clone();

        child.push_key(x.get_key(i).unwrap().clone());
        child.extend_over_keys(i, i + 1);

        if !child.is_leaf() {
            x.extend_over_children(i, i + 1);
        }

        x.remove_key(i);
        x.remove_child(i + 1);
        x.set_child(i, child);
    }

    fn fill(x: &BTreeNode, i: usize, t: usize) {
        if i != 0 && x.get_child(i - 1).unwrap().get_keys_count().unwrap() >= t {
            Btree::borrow_from_prev(x, i);
        } else if i != x.get_children_count().unwrap() - 1 && x.get_child(i + 1).unwrap().get_keys_count().unwrap() >= t {
            Btree::borrow_from_next(x, i);
        } else {
            if i != x.get_children_count().unwrap() - 1 {
                Btree::merge(x, i, t);
            } else {
                Btree::merge(x, i - 1, t);
            }
        }
    }

    #[deprecated]
    fn borrow_from_prev(x: &BTreeNode, i: usize) {
        x.children_move_key_left(i, i - 1);

        let parent_key = x.get_key(i - 1).unwrap().clone();
        x.child_insert_key(i, 0, parent_key);

        if !x.get_child(i - 1).unwrap().is_leaf() {
            x.children_move_child_left(i, i - 1);
        }

        let sibling_key = x.child_pop_key(i - 1).unwrap().unwrap();
        x.set_key(i - 1, sibling_key);
    }

    #[deprecated]
    fn borrow_from_next(x: &BTreeNode, i: usize) {
        x.children_move_key_right(i, i + 1);
        let parent_key = x.get_key(i).unwrap().clone();
        x.child_push_key(i, parent_key);

        let sibling_key = x.child_pop_first_key(i+1).unwrap().unwrap();
        x.set_key(i, sibling_key);

        if !x.get_child(i + 1).unwrap().is_leaf() {
            x.children_move_child_right(i, i + 1);
        }
    }

    pub fn scan(&self) -> (Vec<Key>, Vec<Row>) {
        let mut keys = Vec::new();
        let mut rows = Vec::new();
        if let Some(ref root) = self.root {
            self.in_order_traversal(root, &mut keys, &mut rows);
        }
        (keys, rows)
    }

    fn in_order_traversal(&self, node: &BTreeNode, keys: &mut Vec<Key>, rows: &mut Vec<Row>) {
        let key_count = node.get_keys_count().unwrap();
        for i in 0..key_count {
            if !node.is_leaf() {
                let child = node.get_child(i).unwrap();
                self.in_order_traversal(&child, keys, rows);
            }
            keys.push(node.get_key(i).unwrap());
            //rows.push(PagerFrontend::get_data(node.page_position, i, &self.pager_accessor).unwrap());
        }
        if !node.is_leaf() {
            let child = node.get_child(key_count).unwrap();
            self.in_order_traversal(&child, keys, rows);
        }
    }
}

impl Debug for Btree {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(ref root) = self.root {
            let mut queue = std::collections::VecDeque::new();
            queue.push_back(root.clone());

            writeln!(f, "Btree Level-Order Traversal:")?;
            while !queue.is_empty() {
                let level_size = queue.len();
                let mut level_keys = Vec::new();

                for _ in 0..level_size {
                    if let Some(node) = queue.pop_front() {
                        let keys = node.get_keys().unwrap();
                        level_keys.push(keys);

                        if !node.is_leaf() {
                            let children = node.get_children().unwrap();
                            for child in children {
                                queue.push_back(child);
                            }
                        }
                    }
                }

                writeln!(f, "{:?}", level_keys)?;
            }
        } else {
            writeln!(f, "Tree is empty")?;
        }
        Ok(())
    }
}

