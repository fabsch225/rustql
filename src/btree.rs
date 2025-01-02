use std::fmt::Display;
use std::fmt::{Debug, Formatter};
use crate::compiler::SqlStatementComparisonOperator;
use crate::pager::{Key, PagerAccessor, PagerCore, Position, Row, Schema};
use crate::serializer::Serializer;
use crate::pager_frontend::PagerFrontend;
use crate::status::Status;
use crate::status::Status::InternalExceptionKeyNotFound;
//TODO implement error handling with unwraps or else

//Thought: If we balance children, we will move a Childs Position to another Node,
//should the Page -> The Actual Child also be moved!?
// => Yes, and there are several approaches to this: Indirection Layer, Periodical Compaction
//wont do this for now

#[derive(Clone, Debug)]
pub struct BTreeNode {
    pub page_position: Position,          // Unique ID for the node (corresponds to a page in the pager)
    //#### dont cache this stuff **twice**. use f.ex. pager_interface -> readKeysFromCache() -> ((modify the vector as needed)) -> writeKeysToCache(vec) [or is it not a cache but a buffer!?]
    //pub keys: Vec<Key>,         // Cached keys (loaded from pager)
    //pub children: Vec<Position>,    // Child page IDs (loaded from pager)
    pub pager_interface: PagerAccessor,           // Reference to the pager for disk-backed storage
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
    fn get_keys_from(&self, index: usize) -> Result<(Vec<Key>, Vec<Row>), Status> {
        PagerFrontend::get_keys(&self).map(|v|(v.0[index..].to_vec(), v.1[index..].to_vec()))
    }
    fn get_key(&self, index: usize) -> Result<(Key, Row), Status> {
        PagerFrontend::get_key(index, &self)
    }
    fn set_key(&self, index: usize, key: Key, row: Row) -> Result<(), Status> {
        PagerFrontend::set_key(index, self, key, row)
    }

    fn set_keys(&self, keys: Vec<Key>, rows: Vec<Row>) -> Result<(), Status> {
        PagerFrontend::set_keys(self, keys, rows)
    }

    fn get_keys(&self) -> Result<(Vec<Key>, Vec<Row>), Status> {
       PagerFrontend::get_keys(self)
    }

    fn remove_key(&self, index: usize) -> Result<(Key, Row), Status> {
        let mut keys_and_rows = self.get_keys()?.clone();
        let removed_key = keys_and_rows.0.remove(index);
        let removed_row = keys_and_rows.1.remove(index);
        PagerFrontend::set_keys(self, keys_and_rows.0, keys_and_rows.1)?;
        Ok((removed_key, removed_row))
    }

    fn exchange_key(&self, prev_key: Key, new_key: Key) -> Result<(), Status> {
        let mut keys_and_rows = self.get_keys()?.clone();
        if let Some(index) = keys_and_rows.0.iter().position(|x| *x == prev_key) {
            keys_and_rows.0[index] = new_key;
            PagerFrontend::set_keys(self, keys_and_rows.0, keys_and_rows.1)
        } else {
            Err(Status::InternalExceptionKeyNotFound)
        }
    }

    fn push_key(&self, key: Key, row: Row) -> Result<(), Status> {
        let mut keys_and_rows = self.get_keys()?;
        keys_and_rows.0.push(key);
        keys_and_rows.1.push(row);
        PagerFrontend::set_keys(self, keys_and_rows.0, keys_and_rows.1)
    }

    fn extend_keys(&self, keys: Vec<Key>, rows: Vec<Row>) -> Result<(), Status> {
        let mut keys_and_rows = self.get_keys()?.clone();
        keys_and_rows.0.extend(keys);
        keys_and_rows.1.extend(rows);
        PagerFrontend::set_keys(self, keys_and_rows.0, keys_and_rows.1)
    }

    fn truncate_keys(&self, index: usize) -> Result<(), Status> {
        let mut keys_and_rows = self.get_keys()?.clone();
        keys_and_rows.0.truncate(index);
        keys_and_rows.1.truncate(index);
        PagerFrontend::set_keys(self, keys_and_rows.0, keys_and_rows.1)
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
        let new_keys_and_rows = children[index_to].get_keys()?.clone();
        children[index_from].extend_keys(new_keys_and_rows.0, new_keys_and_rows.1)?;
        PagerFrontend::set_children(self, children)
    }

    fn insert_key(&self, index: usize, key: Key, row: Row) -> Result<(), Status> {
        let mut keys_and_rows = self.get_keys()?.clone();
        //TODO inline the insert function
        keys_and_rows.0.insert(index, key);
        keys_and_rows.1.insert(index, row);
        PagerFrontend::set_keys(self, keys_and_rows.0, keys_and_rows.1)
    }

    fn insert_child(&self, index: usize, child: BTreeNode) -> Result<(), Status> {
        let mut children = PagerFrontend::get_children(self)?;
        children.insert(index, child);
        PagerFrontend::set_children(self, children)
    }

    fn child_insert_key(&self, index: usize, sub_index: usize, key: Key, row: Row) -> Result<(), Status> {
        let mut children = PagerFrontend::get_children(self)?;
        children[index].insert_key(sub_index, key, row)
    }

    fn child_push_key(&self, index: usize, key: Key, row: Row) -> Result<(), Status> {
        let mut children = PagerFrontend::get_children(self)?;
        Ok(children[index].push_key(key, row)?)
    }

    fn child_pop_key(&self, index: usize) -> Result<Option<(Key, Row)>, Status> {
        let mut children = PagerFrontend::get_children(self)?;
        let key_value_pair = children[index].remove_key(children[index].get_keys_count()? - 1)?;
        Ok(Some(key_value_pair))
    }

    fn child_pop_first_key(&self, index: usize) -> Result<Option<(Key, Row)>, Status> {
        let mut children = PagerFrontend::get_children(self)?;
        let key_value_pair = children[index].remove_key(0)?;
        Ok(Some(key_value_pair))
    }

    fn children_move_key_left(&self, to_index: usize, from_index: usize) -> Result<(), Status> {
        let mut children = PagerFrontend::get_children(self)?;
        let key_value_pair = children[from_index].remove_key(children[from_index].get_keys_count()? - 1)?;
        children[to_index].insert_key(0, key_value_pair.0, key_value_pair.1)
    }

    fn children_move_child_left(&self, to_index: usize, from_index: usize) -> Result<(), Status> {
        let mut children = PagerFrontend::get_children(self)?;
        let child = children[from_index].remove_child(children[from_index].get_children_count()? - 1)?;
        children[to_index].insert_child(0, child)
    }

    fn children_move_key_right(&self, to_index: usize, from_index: usize) -> Result<(), Status> {
        let mut children = PagerFrontend::get_children(self)?;
        let key_value_pair = children[from_index].remove_key(0)?;
        children[to_index].push_key(key_value_pair.0, key_value_pair.1)
    }

    fn children_move_child_right(&self, to_index: usize, from_index: usize) -> Result<(), Status> {
        let mut children = PagerFrontend::get_children(self)?;
        let child = children[from_index].remove_child(0)?;
        children[to_index].push_child(child)
    }
}

pub struct Btree {
    pub root: Option<BTreeNode>,
    pub t: usize, // Minimum degree
    pub pager_accessor: PagerAccessor
}

impl Btree {
    pub fn new(t: usize, pager_accessor: PagerAccessor) -> Self {
        let mut root = None;
        //TODO revisit this
        if pager_accessor.has_root() {
            root = Some(PagerFrontend::get_node(pager_accessor.clone(), pager_accessor.read_schema().root).unwrap());
        }
        Btree {
            root,
            t,
            pager_accessor
        }
    }

    fn compare(&self, a: &Key, b: &Key) -> Result<std::cmp::Ordering, Status> {
        Serializer::compare_with_type(a, b, self.pager_accessor.read_schema().key_type)
    }

    pub fn insert(&mut self, k: Key, v: Row) {
        if let Some(ref root) = self.root {
            if root.get_keys_count().unwrap() == (2 * self.t) - 1 {
                let mut new_root = PagerFrontend::create_singular_node(self.pager_accessor.read_schema(), self.pager_accessor.clone(), Serializer::empty_key(&self.pager_accessor.read_schema()), Serializer::empty_row(&self.pager_accessor.read_schema())).unwrap();
                new_root.push_child(root.clone()).unwrap();
                self.split_child(&new_root, 0, self.t, true);

                self.insert_non_full(&new_root, k, v, self.t);
                self.pager_accessor.set_root(&new_root);
                self.root = Some(new_root);
            } else {
                self.insert_non_full(root, k, v, self.t);
            }
        } else {
            let new_root = PagerFrontend::create_singular_node(self.pager_accessor.read_schema(), self.pager_accessor.clone(), k.clone(), v).unwrap();
            self.pager_accessor.set_root(&new_root);
            self.root = Some(new_root);
        }
    }

    fn insert_non_full(&self, x: &BTreeNode, key: Key, row: Row, t: usize) {
        let mut i = x.get_keys_count().unwrap() as isize - 1;
        if x.is_leaf() {
            x.push_key(Serializer::empty_key(&self.pager_accessor.read_schema()), Serializer::empty_row(&self.pager_accessor.read_schema())); // Add a dummy value
            let key_and_row = x.get_key(i as usize).unwrap();
            while i >= 0 && self.compare(&key, &key_and_row.0).unwrap() == std::cmp::Ordering::Less {
                let key_and_row = x.get_key(i as usize).unwrap(); //TODO slight optimization
                x.set_key((i + 1) as usize, key_and_row.0, key_and_row.1);
                i -= 1;
            }
            x.set_key((i + 1) as usize, key, row);
        } else {
            let key_and_row = x.get_key(i as usize).unwrap();
            while i >= 0 && self.compare(&key, &key_and_row.0).unwrap() == std::cmp::Ordering::Less {
                i -= 1;
            }
            let mut i = (i + 1) as usize;
            if x.get_child(i).unwrap().get_keys_count().unwrap() == (2 * t) - 1 {
                self.split_child(x, i, t, false);
                let key_and_row = x.get_key(i).unwrap();
                if self.compare(&key, &key_and_row.0).unwrap() == std::cmp::Ordering::Greater {
                    i += 1;
                }
            }
            self.insert_non_full(&x.get_child(i).unwrap(), key, row, t);
        }
    }

    fn split_child(&self, x: &BTreeNode, i: usize, t: usize, is_root: bool) {
        let mut y = x.get_child(i).unwrap().clone();
        let keys_and_rows = y.get_keys_from(t).unwrap();
        let mut z = PagerFrontend::create_node(self.pager_accessor.read_schema(), y.pager_interface.clone(), keys_and_rows.0, vec![], keys_and_rows.1).unwrap();

        let key_and_row = y.get_key(t - 1).unwrap();
        //TODO suboptimal
        if is_root {
            x.set_key(0, key_and_row.0, key_and_row.1);
        } else {
            x.insert_key(i, key_and_row.0, key_and_row.1);
        }

        if !y.is_leaf() {
            z.set_children(y.get_children_from(t).unwrap()).unwrap();
        }

        y.truncate_keys(t - 1).unwrap();

        x.insert_child(i + 1, z).unwrap();
    }

    //force borrow checker here...
    pub fn delete(&mut self, k: Key) {
        if let Some(ref root) = self.root {
            self.delete_from(root, k, self.t);
        } else {
            panic!();
        }
    }

    fn delete_from(&self, x: &BTreeNode, k: Key, t: usize) {
        let mut i = 0;
        let key_and_row = x.get_key(i).unwrap();
        while i < x.get_keys_count().unwrap() && self.compare(&k, &key_and_row.0).unwrap() == std::cmp::Ordering::Greater {
            i += 1;
        }

        if x.is_leaf() {
            // Case 1: Node is a leaf
            if i < x.get_keys_count().unwrap() && k == x.get_key(i).unwrap().0 {
                x.remove_key(i);
            }
        } else {
            // Case 2: Key is in an internal node
            if i < x.get_keys_count().unwrap() && k == x.get_key(i).unwrap().0 {
                self.delete_internal_node(x, k, i, t);
            } else {
                if x.get_child(i).unwrap().get_keys_count().unwrap() < t {
                    self.fill(x, i, t);
                }
                self.delete_from(&x.get_child(i).unwrap(), k, t);
            }
        }
    }

    fn delete_internal_node(&self, x: &BTreeNode, k: Key, i: usize, t: usize) {
        if x.get_child(i).unwrap().get_keys_count().unwrap() >= t {
            let pred_key_and_row = self.get_predecessor(&x.get_child(i).unwrap()).unwrap();
            x.set_key(i, pred_key_and_row.0.clone(), pred_key_and_row.1);
            self.delete_from(&mut x.get_child(i).unwrap(), pred_key_and_row.0, t);
        } else if x.get_child(i + 1).unwrap().get_keys_count().unwrap() >= t {
            let succ_key_and_row = self.get_successor(&x.get_child(i + 1).unwrap()).unwrap();
            x.set_key(i, succ_key_and_row.0.clone(), succ_key_and_row.1);
            self.delete_from(&mut x.get_child(i + 1).unwrap(), succ_key_and_row.0, t);
        } else {
            self.merge(x, i, t);
            self.delete_from(&mut x.get_child(i).unwrap(), k, t);
        }
    }

    //TODO optimize this!!!!
    fn get_predecessor(&self, x: &BTreeNode) -> Result<(Key, Row), Status> {
        let mut cur = x.clone();
        while !cur.is_leaf() {
            cur = cur.get_child(cur.get_children_count().unwrap() - 1).unwrap().clone();
        }
        cur.get_key(cur.get_keys_count().unwrap() - 1)
    }

    //TODO optimize this!!!!
    fn get_successor(&self, x: &BTreeNode) -> Result<(Key, Row), Status> {
        let mut cur = x.clone();
        while !cur.is_leaf() {
            cur = cur.get_child(0).unwrap().clone();
        }
        cur.get_key(0)
    }

    #[deprecated]
    fn merge(&self, x: &BTreeNode, i: usize, t: usize) {
        let child = x.get_child(i).unwrap().clone();
        let key_and_row = x.get_key(i).unwrap();
        child.push_key(key_and_row.0, key_and_row.1);
        child.extend_over_keys(i, i + 1);

        if !child.is_leaf() {
            x.extend_over_children(i, i + 1);
        }

        x.remove_key(i);
        x.remove_child(i + 1);
        x.set_child(i, child);
    }

    fn fill(&self, x: &BTreeNode, i: usize, t: usize) {
        if i != 0 && x.get_child(i - 1).unwrap().get_keys_count().unwrap() >= t {
            self.borrow_from_prev(x, i);
        } else if i != x.get_children_count().unwrap() - 1 && x.get_child(i + 1).unwrap().get_keys_count().unwrap() >= t {
            self.borrow_from_next(x, i);
        } else {
            if i != x.get_children_count().unwrap() - 1 {
                self.merge(x, i, t);
            } else {
                self.merge(x, i - 1, t);
            }
        }
    }

    #[deprecated]
    fn borrow_from_prev(&self, x: &BTreeNode, i: usize) {
        x.children_move_key_left(i, i - 1);

        let parent_key_and_row = x.get_key(i - 1).unwrap().clone();
        x.child_insert_key(i, 0, parent_key_and_row.0, parent_key_and_row.1);

        if !x.get_child(i - 1).unwrap().is_leaf() {
            x.children_move_child_left(i, i - 1);
        }

        let sibling_key_and_row = x.child_pop_key(i - 1).unwrap().unwrap();
        x.set_key(i - 1, sibling_key_and_row.0, sibling_key_and_row.1);
    }

    #[deprecated]
    fn borrow_from_next(&self, x: &BTreeNode, i: usize) {
        x.children_move_key_right(i, i + 1);
        let parent_key_and_row = x.get_key(i).unwrap().clone();
        x.child_push_key(i, parent_key_and_row.0, parent_key_and_row.1);

        let sibling_key_and_row = x.child_pop_first_key(i+1).unwrap().unwrap();
        x.set_key(i, sibling_key_and_row.0, sibling_key_and_row.1);

        if !x.get_child(i + 1).unwrap().is_leaf() {
            x.children_move_child_right(i, i + 1);
        }
    }

    pub fn scan<C>(&self, collect: &C)
        where C: Fn(&Key, &Row)
    {
        if let Some(ref root) = self.root {
            self.in_order_traversal(root, collect);
        }
    }

    fn in_order_traversal<C>(&self, node: &BTreeNode, collect: &C) -> Result<(), Status>
        where C: Fn(&Key, &Row)
    {
        let key_count = node.get_keys_count().unwrap();
        for i in 0..key_count {
            if !node.is_leaf() {
                let child = node.get_child(i).unwrap();
                self.in_order_traversal(&child, collect)?;
            }
            let key_and_row = node.get_key(i).unwrap();
            collect(&key_and_row.0, &key_and_row.1)
        }
        if !node.is_leaf() {
            let child = node.get_child(key_count).unwrap();
            self.in_order_traversal(&child, collect)?;
        }
        Ok(())
    }

    pub fn find<C>(&self, key: Key, collect: &C) -> Result<(), Status>
        where C: Fn(&Key, &Row)
    {
        if let Some(ref root) = self.root {
            self.find_in_node(root, key, collect)?
        }
        Ok(())
    }

    fn find_in_node<C>(&self, node: &BTreeNode, key: Key, collect: &C) -> Result<(), Status>
        where C: Fn(&Key, &Row)
    {
        let mut i = 0;
        let key_count = node.get_keys_count()?;

        while i < key_count && self.compare(&key, &node.get_key(i)?.0)? == std::cmp::Ordering::Greater {
            i += 1;
        }

        if i < key_count && self.compare(&key, &node.get_key(i)?.0)? == std::cmp::Ordering::Equal {
            let key_and_row = node.get_key(i)?;
            collect(&key_and_row.0, &key_and_row.1);
            return Ok(())
        }

        if node.is_leaf() {
            return Err(InternalExceptionKeyNotFound);
        }

        self.find_in_node(&node.get_child(i)?, key, collect)
    }

    pub fn find_range<C>(&self, a: Key, b: Key, include_a: bool, include_b: bool, collect: &C) -> Result<(), Status>
        where C: Fn(&Key, &Row)
    {
        if let Some(ref root) = self.root {
            self.find_range_in_node(root, a, b, include_a, include_b, collect)?;
        }
        Ok(())
    }

    fn find_range_in_node<C>(&self, node: &BTreeNode, a: Key, b: Key, include_a: bool, include_b: bool, collect: &C) -> Result<(), Status>
        where C: Fn(&Key, &Row)
    {
        let key_count = node.get_keys_count()?;
        for i in 0..key_count {
            let key_and_row = node.get_key(i)?;
            if self.compare(&key_and_row.0, &a)? == std::cmp::Ordering::Less {
                continue;
            }
            if !node.is_leaf() {
                let child = node.get_child(i)?;
                self.find_range_in_node(&child, a.clone(), b.clone(), include_a, include_b, collect)?;
            }
            let in_lower_bound = if include_a { self.compare(&key_and_row.0, &a)? != std::cmp::Ordering::Less } else { self.compare(&key_and_row.0, &a)? == std::cmp::Ordering::Greater };
            let in_upper_bound = if include_b { self.compare(&key_and_row.0, &b)? != std::cmp::Ordering::Greater } else { self.compare(&key_and_row.0, &b)? == std::cmp::Ordering::Less };
            if in_lower_bound && in_upper_bound {
                collect(&key_and_row.0, &key_and_row.1)
            } else if self.compare(&key_and_row.0, &b)? == std::cmp::Ordering::Greater {
                break;
            }
        }
        if !node.is_leaf() {
            let child = node.get_child(key_count)?;
            self.find_range_in_node(&child, a, b, include_a, include_b, collect)?;
        }
        Ok(())
    }
}

impl Display for Btree {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(ref root) = self.root {
            let mut queue = std::collections::VecDeque::new();
            let schema = self.pager_accessor.read_schema();
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
                for (keys, rows) in level_keys {
                    write!(f, "{{")?;
                    for (key, row) in keys.iter().zip(rows.iter()) {
                        write!(f, "[")?;
                        write!(f, "{}", Serializer::format_key(key, &schema).unwrap())?;
                        write!(f, " :: ")?;
                        write!(f, "{}", Serializer::format_row(row, &schema).unwrap())?;
                        write!(f, "]")?;
                    }
                    write!(f, "}}")?;
                }
                writeln!(f, "")?;
            }
        } else {
            writeln!(f, "Tree is empty")?;
        }
        Ok(())
    }
}

