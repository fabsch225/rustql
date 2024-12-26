use std::fmt::{Debug, Formatter};
use crate::pager::{Key, PageContainer, PagerCore, PagerAccessor, Position, Row, TableSchema};
use crate::serializer::Serializer;
use crate::pager_frontend::PagerFrontend;
use crate::status::Status;
//TODO implement error handling with unwraps or else

//TODO implement interaction with Node only with getters and setters

//CREATE TABLE
//WHERE

//BTree and BTreeNode will end up as facades for the pager

//Thought: If we balance children, we will move a Childs Position to another Node,
//should the Page -> The Actual Child also be moved!?
// => Yes, and there are several approaches to this: Indirection Layer, Periodical Compaction
//wont do this for now

#[derive(Clone)]
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
    pub fn cmp(a: &Key, b: &Key) -> std::cmp::Ordering {
        Serializer::compare(a, b).expect("Key comparison failed")
    }
}

impl BTreeNode {
    fn is_leaf(&self) -> bool {
        self.is_leaf
    }
    fn get_keys_count(&self) -> usize {
        let vec = self.pager_interface.access_page_read(&self, |d, t|{Serializer::read_keys_as_vec(d, t)});
        //let x = &vec.unwrap().len();
        let mut vec = vec.unwrap();
        vec.push(vec![0, 2, 3, 4]);
        self.pager_interface.access_page_write(&self, |d, t|{Serializer::write_keys_vec(&vec, d, t)});
        return 0;
    }
    fn get_keys_from(&self, index: usize) -> Vec<Key> {
        //self.keys[index..].to_vec()
        todo!()
    }
    fn get_key(&self, index: usize) -> Option<&Key> {
        //self.keys.get(index)
        todo!()
    }
    fn set_key(&self, index: usize, key: Key) {
        todo!()
        //self.keys[index] = key;
    }
    fn set_keys(&self, keys: Vec<Key>) {
        //self.keys = keys;
        todo!()
    }
    fn get_keys(&self) -> &Vec<Key> {
        todo!()
        //&self.keys
    }
    fn remove_key(&self, index: usize) -> Key {
        todo!()
        //self.keys.remove(index)
    }
    fn change_key(&self, prev_key: Key, key: Key) {
        todo!()
        //let index = self.keys.iter().position(|x| *x == prev_key).unwrap();
        //self.keys.insert(index, key);
    }
    fn push_key(&self, key: Key) {
        todo!()
        //self.keys.push(key);
        //TODO Save Keys to Memory
    }
    fn extend_keys(&self, keys: Vec<Key>) {
        //self.keys.extend(keys);
        todo!()
    }
    fn truncate_keys(&self, index: usize) {
        //self.keys.truncate(index);
        todo!()
    }
    fn get_child(&self, index: usize) -> Result<BTreeNode, Status> {
        PagerFrontend::get_child(index, &self)
    }

    //TODO should exist? -> we only interface this!?!?!?!?
    //lets keep for now...
    #[deprecated]
    fn get_child_mut(&self, index: usize) -> Result<BTreeNode, Status> {
        PagerFrontend::get_child(index, &self)
    }

    #[deprecated]
    fn get_children_count(&self) -> usize {
        //self.children.len()
        todo!()
    }
    fn set_children(&self, children: Vec<BTreeNode>) {
        //self.children = children;
        todo!()
    }
    fn get_children_from(&self, index: usize) -> Vec<BTreeNode> {
        //self.children[index..].to_vec()
        todo!()
    }
    fn set_child(&self, index: usize, child: BTreeNode) {
        //self.children[index] = child;
        todo!()
    }
    fn remove_child(&self, index: usize) -> BTreeNode {
        //self.children.remove(index)
        todo!()
    }
    fn push_child(&self, child: BTreeNode) {
        //self.children.push(child);
        todo!()
    }
    fn truncate_children(&self, index: usize) {
        //self.children.truncate(index);
        todo!()
    }
    fn extend_over_children(&self, index_from: usize, index_to: usize) {
        //let new_children = self.children[index_to].children.drain(..).collect::<Vec<_>>();
        // self.children[index_from].children.extend(new_children);
        todo!()
    }
    fn extend_over_keys(&self, index_from: usize, index_to: usize) {
        //let new_keys = self.children[index_to].keys.drain(..).collect::<Vec<_>>();;
        //self.children[index_from].keys.extend(new_keys);
        todo!()
    }
    fn insert_key(&self, index: usize, key: Key) {
        //self.keys.insert(index, key);
        todo!()
    }
    fn insert_child(&self, index: usize, key: BTreeNode) {
        //self.children.insert(index, key);
        todo!()
    }
    fn child_insert_key(&self, index: usize, sub_index: usize, key: Key) {
        //self.children[index].keys.insert(sub_index, key);
        todo!()
    }
    fn child_push_key(&self, index: usize, key: Key) {
        //self.children[index].keys.push(key);
        todo!()
    }
    fn child_pop_key(&self, index: usize) -> Option<Key> {
        //self.children[index].keys.pop()
        todo!()
    }
    fn child_pop_first_key(&self, index: usize) -> Option<Key> {
        //Some(self.children[index].keys.remove(0))
        todo!()
    }
    //inserts the last key from children[from_index] at the first position of children[to_index].keys
    fn children_move_key_left(&self, to_index: usize, from_index: usize) {
        //let key = self.children[from_index].keys.pop().unwrap();
        //self.children[to_index].keys.insert(0, key);
        todo!()
    }
    fn children_move_child_left(&self, to_index: usize, from_index: usize) {
        //let child = self.children[from_index].children.pop().unwrap();
        //self.children[to_index].children.insert(0, child);#
        todo!()
    }
    fn children_move_key_right(&self, to_index: usize, from_index: usize) {
        //let key = self.children[from_index].keys.remove(0);
        //self.children[to_index].keys.push(key);#
        todo!()
    }
    fn children_move_child_right(&self, to_index: usize, from_index: usize) {
        //let child = self.children[from_index].children.remove(0);
        //self.children[to_index].children.push(child);
        todo!()
    }
}

pub struct Btree {
    pub root: BTreeNode,
    pub t: usize, // Minimum degree
}

impl Btree {
    pub fn new(t: usize) -> Self {
        todo!()
    }

    //TODO Data to insert
    pub fn insert(&mut self, k: Key) {
        todo!();
        /*let root = &mut self.root;
        let t = self.t;
        if root.get_keys_count() == (2 * self.t) - 1 {
            let root = &mut self.root;
            let mut temp = BtreeNode::new(false);
            temp.push_child(root.clone());
            Btree::split_child(&mut temp, 0, t);
            Btree::insert_non_full(&mut temp, k, t);
            self.root = temp;
        } else {
            Btree::insert_non_full(root, k, t);
        }*/
    }

    fn insert_non_full(x: &BTreeNode, k: Key, t: usize) {
        let mut i = x.get_keys_count() as isize - 1;
        if x.is_leaf() {
            x.push_key(k.clone()); // Add a dummy value
            while i >= 0 && k < *x.get_key(i as usize).unwrap() {
                x.set_key((i + 1) as usize, x.get_key(i as usize).unwrap().clone());
                i -= 1;
            }
            x.set_key((i + 1) as usize, k);
        } else {
            while i >= 0 && k < *x.get_key(i as usize).unwrap() {
                i -= 1;
            }
            let mut i = (i + 1) as usize;
            if x.get_child(i).unwrap().get_keys_count() == (2 * t) - 1 {
                Btree::split_child(x, i, t);
                if k > *x.get_key(i).unwrap() {
                    i += 1;
                }
            }
            Btree::insert_non_full(&x.get_child(i).unwrap(), k, t);
        }
    }

    fn split_child(x: &BTreeNode, i: usize, t: usize) {
        todo!();
        /*
        let mut y = x.get_child_mut(i).unwrap().clone();
        let mut z = BtreeNode::new(y.is_leaf());

        z.set_keys(y.get_keys_from(t));
        x.insert_key(i, y.get_key(t - 1).unwrap().clone());
        y.truncate_keys(t - 1);

        if !y.is_leaf() {
            z.set_children(y.get_children_from(t));
            y.truncate_children(t);
        }

        x.insert_child(i + 1, z);
        *x.get_child_mut(i).unwrap() = y; // Update the child reference after truncation
        */
    }

    //force borrow checker here...
    pub fn delete(&mut self, k: Key) {
        Self::delete_from(&self.root, k, self.t);
    }

    fn delete_from(x: &BTreeNode, k: Key, t: usize) {
        let mut i = 0;
        while i < x.get_keys_count() && k > *x.get_key(i).unwrap() {
            i += 1;
        }

        if x.is_leaf() {
            // Case 1: Node is a leaf
            if i < x.get_keys_count() && k == *x.get_key(i).unwrap() {
                x.remove_key(i);
            }
        } else {
            // Case 2: Key is in an internal node
            if i < x.get_keys_count() && k == *x.get_key(i).unwrap() {
                Btree::delete_internal_node(x, k, i, t);
            } else {
                if x.get_child(i).unwrap().get_keys_count() < t {
                    Btree::fill(x, i, t);
                }
                Btree::delete_from(&mut x.get_child_mut(i).unwrap(), k, t);
            }
        }
    }

    fn delete_internal_node(x: &BTreeNode, k: Key, i: usize, t: usize) {
        if x.get_child(i).unwrap().get_keys_count() >= t {
            let pred = Btree::get_predecessor(&x.get_child(i).unwrap());
            x.set_key(i, pred.clone());
            Btree::delete_from(&mut x.get_child(i).unwrap(), pred, t);
        } else if x.get_child(i + 1).unwrap().get_keys_count() >= t {
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
            cur = cur.get_child(cur.get_children_count() - 1).unwrap().clone();
        }
        cur.get_key(cur.get_keys_count() - 1).unwrap().clone()
    }

    //TODO optimize this!!!!
    fn get_successor(x: &BTreeNode) -> Key {
        let mut cur = x.clone();
        while !cur.is_leaf() {
            cur = cur.get_child(0).unwrap().clone();
        }
        cur.get_key(0).unwrap().clone()
    }

    fn merge(x: &BTreeNode, i: usize, t: usize) {
        let mut child = x.get_child_mut(i).unwrap().clone();

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
        if i != 0 && x.get_child(i - 1).unwrap().get_keys_count() >= t {
            Btree::borrow_from_prev(x, i);
        } else if i != x.get_children_count() - 1 && x.get_child(i + 1).unwrap().get_keys_count() >= t {
            Btree::borrow_from_next(x, i);
        } else {
            if i != x.get_children_count() - 1 {
                Btree::merge(x, i, t);
            } else {
                Btree::merge(x, i - 1, t);
            }
        }
    }

    fn borrow_from_prev(x: &BTreeNode, i: usize) {
        x.children_move_key_left(i, i - 1);

        let parent_key = x.get_key(i - 1).unwrap().clone();
        x.child_insert_key(i, 0, parent_key);

        if !x.get_child(i - 1).unwrap().is_leaf() {
            x.children_move_child_left(i, i - 1);
        }

        let sibling_key = x.child_pop_key(i - 1).unwrap();
        x.set_key(i - 1, sibling_key);
    }

    fn borrow_from_next(x: &BTreeNode, i: usize) {
        x.children_move_key_right(i, i + 1);
        let parent_key = x.get_key(i).unwrap().clone();
        x.child_push_key(i, parent_key);

        let sibling_key = x.child_pop_first_key(i+1).unwrap();
        x.set_key(i, sibling_key);

        if !x.get_child(i + 1).unwrap().is_leaf() {
            x.children_move_child_right(i, i + 1);
        }
    }
}

impl Debug for Btree {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!();
    }
}
