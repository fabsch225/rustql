use std::fmt::{Debug, Formatter};

//TODO implement error handling with unwraps or else

//TODO implement interaction with Node only with getters and setters

//CREATE TABLE
//WHERE

//BTree and BTreeNode will end up as facades for the pager
/*

GOAL:

pub struct BtreeNode {
    pager: &mut Pager
    key: Key
    leaf: bool,
}

 */


#[derive(Clone)]
pub struct BtreeNode<T> {
    keys: Vec<T>,
    children: Vec<BtreeNode<T>>,
    leaf: bool,
}

impl<T: std::cmp::PartialEq + std::clone::Clone> BtreeNode<T> {
    pub fn new(leaf: bool) -> Self {
        BtreeNode {
            keys: Vec::new(),
            children: Vec::new(),
            leaf,
        }
    }

    //for balancing of nodes
}

impl<T: std::cmp::PartialEq + std::clone::Clone> BtreeNode<T> {
    fn is_leaf(&self) -> bool {
        self.leaf
    }
    fn get_keys_count(&self) -> usize {
        self.keys.len()
    }
    fn get_keys_from(&self, index: usize) -> Vec<T> {
        self.keys[index..].to_vec()
    }
    fn get_key(&self, index: usize) -> Option<&T> {
        self.keys.get(index)
    }
    fn set_key(&mut self, index: usize, key: T) {
        self.keys[index] = key;
    }
    fn set_keys(&mut self, keys: Vec<T>) {
        self.keys = keys;
    }
    fn get_keys(&self) -> &Vec<T> {
        &self.keys
    }
    fn remove_key(&mut self, index: usize) -> T {
        self.keys.remove(index)
    }
    fn change_key(&mut self, prev_key: T, key: T) {
        let index = self.keys.iter().position(|x| *x == prev_key).unwrap();
        self.keys.insert(index, key);
    }
    fn push_key(&mut self, key: T) {
        self.keys.push(key);
    }
    fn extend_keys(&mut self, keys: Vec<T>) {
        self.keys.extend(keys);
    }
    fn truncate_keys(&mut self, index: usize) {
        self.keys.truncate(index);
    }
    fn get_child(&self, index: usize) -> Option<&BtreeNode<T>> {
        self.children.get(index)
    }
    fn get_child_mut(&mut self, index: usize) -> Option<&mut BtreeNode<T>> {
        self.children.get_mut(index)
    }
    fn get_children_count(&self) -> usize {
        self.children.len()
    }
    fn set_children(&mut self, children: Vec<BtreeNode<T>>) {
        self.children = children;
    }
    fn get_children_from(&self, index: usize) -> Vec<BtreeNode<T>> {
        self.children[index..].to_vec()
    }
    fn set_child(&mut self, index: usize, child: BtreeNode<T>) {
        self.children[index] = child;
    }
    fn remove_child(&mut self, index: usize) -> BtreeNode<T> {
        self.children.remove(index)
    }
    fn push_child(&mut self, child: BtreeNode<T>) {
        self.children.push(child);
    }
    fn truncate_children(&mut self, index: usize) {
        self.children.truncate(index);
    }
    fn extend_over_children(&mut self, index_from: usize, index_to: usize) {
        let new_children = self.children[index_to].children.drain(..).collect::<Vec<_>>();
        self.children[index_from].children.extend(new_children);
    }
    fn extend_over_keys(&mut self, index_from: usize, index_to: usize) {
        let new_keys = self.children[index_to].keys.drain(..).collect::<Vec<_>>();;
        self.children[index_from].keys.extend(new_keys);
    }
    fn insert_key(&mut self, index: usize, key: T) {
        self.keys.insert(index, key);
    }
    fn insert_child(&mut self, index: usize, key: BtreeNode<T>) {
        self.children.insert(index, key);
    }
    fn child_insert_key(&mut self, index: usize, sub_index: usize, key: T) {
        self.children[index].keys.insert(sub_index, key);
    }
    fn child_push_key(&mut self, index: usize, key: T) {
        self.children[index].keys.push(key);
    }
    fn child_pop_key(&mut self, index: usize) -> Option<T> {
        self.children[index].keys.pop()
    }
    fn child_pop_first_key(&mut self, index: usize) -> Option<T> {
        Some(self.children[index].keys.remove(0))
    }
    //inserts the last key from children[from_index] at the first position of children[to_index].keys
    fn children_move_key_left(&mut self, to_index: usize, from_index: usize) {
        let key = self.children[from_index].keys.pop().unwrap();
        self.children[to_index].keys.insert(0, key);
    }
    fn children_move_child_left(&mut self, to_index: usize, from_index: usize) {
        let child = self.children[from_index].children.pop().unwrap();
        self.children[to_index].children.insert(0, child);
    }
    fn children_move_key_right(&mut self, to_index: usize, from_index: usize) {
        let key = self.children[from_index].keys.remove(0);
        self.children[to_index].keys.push(key);
    }
    fn children_move_child_right(&mut self, to_index: usize, from_index: usize) {
        let child = self.children[from_index].children.remove(0);
        self.children[to_index].children.push(child);
    }
}

pub struct Btree<T: Clone> {
    pub root: BtreeNode<T>,
    pub t: usize, // Minimum degree
}

impl<T: Ord + Clone + Debug> Btree<T> {
    pub fn new(t: usize) -> Self {
        Btree {
            root: BtreeNode::new(true),
            t,
        }
    }

    pub fn insert(&mut self, k: T) {
        let root = &mut self.root;
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
        }
    }

    fn insert_non_full(x: &mut BtreeNode<T>, k: T, t: usize) {
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
            Btree::insert_non_full(&mut x.get_child_mut(i).unwrap(), k, t);
        }
    }

    fn split_child(x: &mut BtreeNode<T>, i: usize, t: usize) {
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
    }


    pub fn delete(&mut self, k: T) {
        Self::delete_from(&mut self.root, k, self.t);
    }

    fn delete_from(x: &mut BtreeNode<T>, k: T, t: usize) {
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
                Btree::delete_from(x.get_child_mut(i).unwrap(), k, t);
            }
        }
    }

    fn delete_internal_node(x: &mut BtreeNode<T>, k: T, i: usize, t: usize) {
        if x.get_child(i).unwrap().get_keys_count() >= t {
            let pred = Btree::get_predecessor(&x.get_child(i).unwrap());
            x.set_key(i, pred.clone());
            Btree::delete_from(x.get_child_mut(i).unwrap(), pred, t);
        } else if x.get_child(i + 1).unwrap().get_keys_count() >= t {
            let succ = Btree::get_successor(&x.get_child(i + 1).unwrap());
            x.set_key(i, succ.clone());
            Btree::delete_from(x.get_child_mut(i + 1).unwrap(), succ, t);
        } else {
            Btree::merge(x, i, t);
            Btree::delete_from(x.get_child_mut(i).unwrap(), k, t);
        }
    }

    fn get_predecessor(x: &BtreeNode<T>) -> T {
        let mut cur = x;
        while !cur.is_leaf() {
            cur = cur.get_child(cur.get_children_count() - 1).unwrap();
        }
        cur.get_key(cur.get_keys_count() - 1).unwrap().clone()
    }

    fn get_successor(x: &BtreeNode<T>) -> T {
        let mut cur = x;
        while !cur.is_leaf() {
            cur = cur.get_child(0).unwrap();
        }
        cur.get_key(0).unwrap().clone()
    }

    fn merge(x: &mut BtreeNode<T>, i: usize, t: usize) {
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

    fn fill(x: &mut BtreeNode<T>, i: usize, t: usize) {
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

    fn borrow_from_prev(x: &mut BtreeNode<T>, i: usize) {
        x.children_move_key_left(i, i - 1);

        let parent_key = x.get_key(i - 1).unwrap().clone();
        x.child_insert_key(i, 0, parent_key);

        if !x.get_child(i - 1).unwrap().is_leaf() {
            x.children_move_child_left(i, i - 1);
        }

        let sibling_key = x.child_pop_key(i - 1).unwrap();
        x.set_key(i - 1, sibling_key);
    }
    /*
        fn borrow_from_prev(x: &mut BtreeNode<T>, i: usize) {
            let sibling_key = x.children[i - 1].keys.pop().unwrap();
            let sibling_child = if !x.children[i - 1].leaf {
                Some(x.children[i - 1].children.pop().unwrap())
            } else {
                None
            };
            let parent_key = &mut x.keys[i - 1];
            let child = &mut x.children[i];
            child.keys.insert(0, parent_key.clone());
            *parent_key = sibling_key;
            if let Some(sibling_child) = sibling_child {
                child.children.insert(0, sibling_child);
            }
        }
     */

    fn borrow_from_next(x: &mut BtreeNode<T>, i: usize) {
        x.children_move_key_right(i, i + 1);
        let parent_key = x.get_key(i).unwrap().clone();
        x.child_push_key(i, parent_key);

        let sibling_key = x.child_pop_first_key(i+1).unwrap();
        x.set_key(i, sibling_key);

        if !x.get_child(i + 1).unwrap().is_leaf() {
            x.children_move_child_right(i, i + 1);
        }
    }

    /*fn borrow_from_next(x: &mut BtreeNode<T>, i: usize) {
        let sibling_key = x.children[i + 1].keys.remove(0);
        let sibling_child = if !x.get_child(i + 1).unwrap().leaf {
            Some(x.get_child(i + 1).unwrap().remove_child(0))
        } else {
            None
        };
        let parent_key = &mut x.keys[i];
        let child = &mut x.get_child(i).unwrap();
        child.keys.push(parent_key.clone());
        *parent_key = sibling_key;
        if let Some(sibling_child) = sibling_child {
            child.children.push(sibling_child);
        }
    }*/
}

impl<T: Clone + Debug> Debug for Btree<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fn print_tree<T: Debug>(
            node: &BtreeNode<T>,
            level: usize,
            f: &mut Formatter<'_>,
        ) -> std::fmt::Result {
            // Print the current level and keys
            writeln!(
                f,
                "Level {} ({} keys): {:?}",
                level,
                node.keys.len(),
                node.keys
            )?;

            // Recursively print child nodes
            if !node.leaf {
                for child in &node.children {
                    print_tree(child, level + 1, f)?;
                }
            }
            Ok(())
        }

        // Start printing from the root
        print_tree(&self.root, 0, f)
    }
}
