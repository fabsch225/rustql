use crate::executor::{Executor, TableSchema};
use crate::pager::{Key, PagerAccessor, Position, Row};
use crate::pager_frontend::PagerFrontend;
use crate::serializer::Serializer;
use crate::status::Status;
use std::env::current_exe;
use std::fmt::Display;
use std::fmt::{Debug, Formatter};

#[derive(Debug)]
pub struct BTreeCursor {
    pub btree: Btree,
    stack: Vec<(BTreeNode, usize)>,
}

impl BTreeCursor {
    pub fn new(btree: Btree) -> Self {
        BTreeCursor {
            btree,
            stack: vec![],
        }
    }

    fn push_rightmost(&mut self, node: &BTreeNode) -> Result<(), Status> {
        let mut current = node.clone();
        loop {
            let count = current.get_keys_count()?;
            if current.is_leaf() {
                self.stack.push((current.clone(), count - 1));
                break;
            } else {
                self.stack.push((current.clone(), count));
                //let x = current.get_children()?[count].clone();
                //println!("{:?}", x);
                current = current.get_child(count)?;
            }
        }
        Ok(())
    }
    fn push_leftmost(&mut self, node: &BTreeNode) -> Result<(), Status> {
        let mut current = node.clone();
        loop {
            // Start at index 0 for every node entered
            self.stack.push((current.clone(), 0));
            if current.is_leaf() {
                break;
            } else {
                current = current.get_child(0)?;
            }
        }
        Ok(())
    }

    pub fn is_valid(&self) -> bool {
        !self.stack.is_empty()
    }

    /// Returns the current (Key, Row) if valid, otherwise Ok(None).
    pub fn current(&self) -> Result<Option<(Key, Row)>, Status> {
        if !self.is_valid() {
            return Ok(None);
        }

        let (ref node, idx) = &self.stack[self.stack.len() - 1];
        let keys_count = node.get_keys_count()?;
        if node.is_leaf() {
            if *idx < keys_count {
                let (k, r) = node.get_key(*idx)?;
                Ok(Some((k, r)))
            } else {
                Ok(None)
            }
        } else {
            if *idx < keys_count {
                let (k, r) = node.get_key(*idx)?;
                Ok(Some((k, r)))
            } else {
                Ok(None)
            }
        }
    }

    pub fn perform_action_on_current<Action>(&self, exec: &Action) -> Result<(), Status>
    where
        Action: Fn(&mut Key, &mut Row) -> Result<bool, Status> + Copy,
    {
        let (ref node, idx) = &self.stack[self.stack.len() - 1];
        //println!("{:?}", node);
        //println!("{:?}", node.get_keys()?);
        let mut key_and_row = node.get_key(*idx)?;
        //TODO -- shouldnt mutate key here. Updating Keys is special: i think delete + reinsert :)
        let modified = exec(&mut key_and_row.0, &mut key_and_row.1)?;
        if modified {
            node.set_key(*idx, key_and_row.0, key_and_row.1)?
        }
        Ok(())
    }

    pub fn move_to_start(&mut self) -> Result<(), Status> {
        self.stack.clear();
        if let Some(root) = self.btree.root.clone() {
            if root.get_keys_count()? > 0 {
                self.push_leftmost(&root)?;
            }
        }
        Ok(())
    }

    pub fn move_to_end(&mut self) -> Result<(), Status> {
        self.stack.clear();
        if let Some(root) = self.btree.root.clone() {
            if root.get_keys_count()? > 0 {
                self.push_rightmost(&root)?;
            }
        }
        Ok(())
    }

    pub fn advance(&mut self) -> Result<(), Status> {
        if self.stack.is_empty() {
            return Ok(());
        }
        {
            let last = self.stack.last_mut().unwrap();
            let node = &last.0;
            let idx = last.1;

            if node.is_leaf() {
                last.1 = idx + 1;
            } else {
                last.1 = idx + 1;
                let right_child = node.get_child(idx + 1)?;
                self.push_leftmost(&right_child)?;
                return Ok(());
            }
        }
        while let Some((node, idx)) = self.stack.last() {
            let keys_count = node.get_keys_count()?;
            if *idx < keys_count {
                return Ok(());
            } else {
                self.stack.pop();
            }
        }
        Ok(())
    }

    pub fn decrease(&mut self) -> Result<(), Status> {
        if self.stack.is_empty() {
            return Ok(());
        }

        {
            let last = self.stack.last_mut().unwrap();
            let node = &last.0;
            let idx = last.1;

            if node.is_leaf() {
                if idx == 0 {
                    last.1 = usize::MAX;
                } else {
                    last.1 = idx - 1;
                }
            } else {
                let left_child = node.get_child(idx)?;
                self.push_rightmost(&left_child)?;
                return Ok(());
            }
        }

        loop {
            if self.stack.is_empty() {
                return Ok(());
            }
            let (node, idx) = self.stack.last().unwrap();
            if *idx != usize::MAX {
                return Ok(());
            } else {
                self.stack.pop();
                if let Some(parent) = self.stack.last_mut() {
                    if parent.1 == 0 {
                        parent.1 = usize::MAX;
                    } else {
                        parent.1 = parent.1 - 1;
                    }
                } else {
                    return Ok(());
                }
            }
        }
    }

    pub fn go_to(&mut self, k: &Key) -> Result<(), Status> {
        self.stack.clear();

        let root = match &self.btree.root {
            Some(r) => r,
            None => return Ok(()),
        };

        if root.get_keys_count()? == 0 {
            return Ok(());
        }

        let mut current = root.clone();

        loop {
            let (keys, _) = current.get_keys()?;
            let mut i = 0usize;
            while i < keys.len() && &keys[i] < k {
                i += 1;
            }

            if i < keys.len() && &keys[i] == k {
                self.stack.push((current, i));
                return Ok(());
            }

            if current.is_leaf() {
                self.stack.clear();
                return Ok(());
            }

            self.stack.push((current.clone(), i));
            current = current.get_child(i)?;
        }
    }

    pub fn go_to_less_than_equal(&mut self, k: &Key) -> Result<(), Status> {
        self.stack.clear();

        let root = match &self.btree.root {
            Some(r) => r,
            None => return Ok(()),
        };

        if root.get_keys_count()? == 0 {
            return Ok(());
        }

        let mut current = root.clone();
        let mut found = false;

        loop {
            let (keys, _) = current.get_keys()?;
            let mut i = 0usize;
            while i < keys.len() && &keys[i] < k {
                i += 1;
            }

            self.stack.push((current.clone(), i));

            if i < keys.len() && &keys[i] == k {
                found = true;
                break;
            }
            if current.is_leaf() {
                break;
            }

            current = current.get_child(i)?;
        }

        if !found {
            if let Some((_, i)) = self.stack.last_mut() {
                if *i > 0 {
                    *i = *i - 1;
                } else {
                    self.stack.clear();
                }
            } else {
               self.stack.clear();
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct BTreeNode {
    pub position: Position,
    pub pager_accessor: PagerAccessor,
    pub table_schema: TableSchema,
}

impl BTreeNode {
    pub fn make_empty(table_schema: &TableSchema, pager_accessor: PagerAccessor) -> Self {
        BTreeNode {
            position: Position::make_empty(),
            pager_accessor,
            table_schema: table_schema.clone(),
        }
    }
    pub fn new(
        position: Position,
        pager_accessor: PagerAccessor,
        table_schema: TableSchema,
    ) -> Self {
        BTreeNode {
            position,
            pager_accessor,
            table_schema,
        }
    }
    //TODO error handling! i.e. return -> Result<bool, Status>
    fn is_leaf(&self) -> bool {
        PagerFrontend::is_leaf(&self).unwrap()
    }

    fn get_keys_count(&self) -> Result<usize, Status> {
        PagerFrontend::get_keys_count(&self)
    }

    fn get_children_count(&self) -> Result<usize, Status> {
        PagerFrontend::get_children_count(&self)
    }

    fn get_keys_from(&self, index: usize) -> Result<(Vec<Key>, Vec<Row>), Status> {
        PagerFrontend::get_keys(&self).map(|v| (v.0[index..].to_vec(), v.1[index..].to_vec()))
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

    /// Removes a Key. The Children are JUST shifted to the left
    #[deprecated]
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

    //mind the naming, it is wrong (index_from / index_to)
    fn extend_over_children(&self, index_from: usize, index_to: usize) -> Result<(), Status> {
        let mut children = PagerFrontend::get_children(self)?;
        let new_children = children[index_from].get_children()?.clone();
        children[index_to].extend_children(new_children)
        //PagerFrontend::set_children(self, children)
    }

    fn extend_over_keys(&self, index_from: usize, index_to: usize) -> Result<(), Status> {
        let mut children = PagerFrontend::get_children(self)?;
        let new_keys_and_rows = children[index_from].get_keys()?.clone();
        children[index_to].extend_keys(new_keys_and_rows.0, new_keys_and_rows.1)
        //PagerFrontend::set_children(self, children)
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
}

#[derive(Debug, Clone)]
pub struct Btree {
    pub root: Option<BTreeNode>,
    pub t: usize,
    pub pager_accessor: PagerAccessor, //both of these could be references
    pub table_schema: TableSchema,
}

impl Btree {
    pub fn init(
        t: usize,
        pager_accessor: PagerAccessor,
        table_schema: TableSchema,
    ) -> Result<Self, Status> {
        let root = Some(PagerFrontend::get_node(
            pager_accessor.clone(),
            table_schema.clone(),
            table_schema.root.clone(),
        )?);
        Ok(Btree {
            root,
            t,
            pager_accessor,
            table_schema,
        })
    }

    fn compare(&self, a: &Key, b: &Key) -> Result<std::cmp::Ordering, Status> {
        Serializer::compare_with_type(a, b, &self.table_schema.get_key_type()?)
    }

    pub fn insert(&mut self, k: Key, v: Row) -> Result<(), Status> {
        if let Some(ref root) = self.root {
            if root.get_keys_count()? == (2 * self.t) - 1 {
                let mut new_root = PagerFrontend::create_node(
                    self.table_schema.clone(),
                    self.pager_accessor.clone(),
                    None,
                    vec![Serializer::empty_key(&self.table_schema)?],
                    vec![root.position.clone()],
                    vec![Serializer::empty_row(&self.table_schema)?],
                )?;
                self.split_child(&new_root, 0, self.t, true).unwrap();
                self.insert_non_full(&new_root, k, v, self.t)?;

                PagerFrontend::switch_nodes(
                    &self.table_schema,
                    self.pager_accessor.clone(),
                    &root,
                    &new_root,
                )?;
            } else {
                self.insert_non_full(root, k, v, self.t)?;
            }
        } else {
            panic!("Root is None");
        }
        Ok(())
    }

    fn insert_non_full(
        &self,
        x: &BTreeNode,
        key: Key,
        row: Row,
        t: usize,
    ) -> Result<(), Status> {
        let mut i = x.get_keys_count()? as isize - 1;
        if x.is_leaf() {
            x.push_key(
                Serializer::empty_key(&self.table_schema)?,
                Serializer::empty_row(&self.table_schema)?,
            )?; // Add a dummy value TODO: should this be here? the BTree should call BTreeNode methods !?
            while i >= 0
                && self.compare(&key, &x.get_key(i as usize)?.0)? == std::cmp::Ordering::Less
            {
                let key_and_row = x.get_key(i as usize)?;
                x.set_key((i + 1) as usize, key_and_row.0, key_and_row.1)?;
                i -= 1;
            }
            x.set_key((i + 1) as usize, key.clone(), row)?;
        } else {
            while i >= 0
                && self.compare(&key, &x.get_key(i as usize)?.0)? == std::cmp::Ordering::Less
            {
                i -= 1;
            }
            let mut i = (i + 1) as usize;
            if x.get_child(i)?.get_keys_count()? == (2 * t) - 1 {
                self.split_child(x, i, t, false)?;
                let key_and_row = x.get_key(i)?;
                if self.compare(&key, &key_and_row.0)? == std::cmp::Ordering::Greater {
                    i += 1;
                }
            }
            self.insert_non_full(&x.get_child(i)?, key, row, t)?;
        }
        Ok(())
    }

    fn split_child(&self, x: &BTreeNode, i: usize, t: usize, is_root: bool) -> Result<(), Status> {
        let mut y = x.get_child(i)?.clone();
        let keys_and_rows = y.get_keys_from(t)?;
        let mut z = PagerFrontend::create_node(
            //TODO: should this be here? the BTree should call BTreeNode methods !?
            self.table_schema.clone(),
            y.pager_accessor.clone(),
            None, //Some(x), //TODO the hint-functionality is wrong!!!
            keys_and_rows.0,
            vec![],
            keys_and_rows.1,
        )?;
        let key_and_row = y.get_key(t - 1).unwrap();
        //TODO suboptimal (works super fine)
        if is_root {
            x.set_key(0, key_and_row.0, key_and_row.1).unwrap();
        } else {
            x.insert_key(i, key_and_row.0, key_and_row.1).unwrap();
        }
        if !y.is_leaf() {
            z.set_children(y.get_children_from(t)?).unwrap();
        }
        x.insert_child(i + 1, z).unwrap();
        y.truncate_keys(t - 1).unwrap();

        Ok(())
    }

    pub fn delete(&mut self, k: Key) -> Result<(), Status> {
        let mut new_root = None;
        if let Some(ref root) = self.root {
            new_root = self.delete_from(root, k, self.t)?;
            if new_root.is_some() {
                PagerFrontend::switch_nodes(
                    &self.table_schema,
                    self.pager_accessor.clone(),
                    &root,
                    &new_root.clone().unwrap(),
                )?;
            }
        } else {
            return Err(Status::InternalExceptionNoRoot);
        }
        Ok(())
    }

    fn delete_from(&self, x: &BTreeNode, k: Key, t: usize) -> Result<Option<BTreeNode>, Status> {
        let mut i = 0;
        let mut new_root = None;
        while i < x.get_keys_count()?
            && self.compare(&k, &x.get_key(i)?.0)? == std::cmp::Ordering::Greater
        {
            i += 1;
        }

        if x.is_leaf() {
            if i < x.get_keys_count()? && k == x.get_key(i)?.0 {
                let ch = x.get_children()?;
                x.remove_key(i)?;
                x.set_children(ch);
            }
        } else {
            if i < x.get_keys_count()? && k == x.get_key(i)?.0 {
                self.delete_internal_node(x, k, i, t)?;
            } else {
                if x.get_child(i)?.get_keys_count()? < t {
                    new_root = self.fill(x, i, t)?;
                }
                if i == x.get_children_count()? {
                    i -= 1;
                }

                self.delete_from(&x.get_child(i)?, k, t).map(|r| -> () {
                    if r.is_some() {
                        new_root = r;
                    }
                })?;
            }
        }
        Ok(new_root)
    }

    fn delete_internal_node(
        &self,
        x: &BTreeNode,
        k: Key,
        i: usize,
        t: usize,
    ) -> Result<Option<BTreeNode>, Status> {
        if x.get_child(i)?.get_keys_count()? >= t {
            let pred_key_and_row = self.get_predecessor(&x.get_child(i)?)?;
            x.set_key(i, pred_key_and_row.0.clone(), pred_key_and_row.1)?;
            let nr = self.delete_from(&mut x.get_child(i)?, pred_key_and_row.0, t)?;
            Ok(nr)
        } else if x.get_child(i + 1)?.get_keys_count()? >= t {
            let succ_key_and_row = self.get_successor(&x.get_child(i + 1)?)?; //Intentional!!!
            x.set_key(i, succ_key_and_row.0.clone(), succ_key_and_row.1)?;
            self.delete_from(&mut x.get_child(i + 1)?, succ_key_and_row.0, t)
        } else {
            let nr = self.merge(x, i, t)?;
            self.delete_from(&mut x.get_child(i)?, k, t)
        }
    }

    fn get_predecessor(&self, x: &BTreeNode) -> Result<(Key, Row), Status> {
        let mut cur = x.clone();
        while !cur.is_leaf() {
            cur = cur.get_child(cur.get_children_count()? - 1)?.clone();
        }
        cur.get_key(cur.get_keys_count()? - 1)
    }

    fn get_successor(&self, x: &BTreeNode) -> Result<(Key, Row), Status> {
        let mut cur = x.clone();
        while !cur.is_leaf() {
            cur = cur.get_child(0)?.clone();
        }
        cur.get_key(0)
    }

    fn merge(&self, x: &BTreeNode, i: usize, t: usize) -> Result<Option<BTreeNode>, Status> {
        let child = x.get_child(i)?;
        let key_and_row = x.get_key(i)?;
        child.push_key(key_and_row.0, key_and_row.1)?;
        x.extend_over_keys(i + 1, i)?;
        if !child.is_leaf() {
            x.extend_over_children(i + 1, i)?;
        }
        let mut children = x.get_children()?;
        x.remove_key(i)?;
        children.remove(i + 1);
        x.set_children(children)?;

        if x.get_keys_count()? == 0 {
            return Ok(Some(child));
        }
        Ok(None)
    }

    fn fill(&self, x: &BTreeNode, i: usize, t: usize) -> Result<Option<BTreeNode>, Status> {
        let mut new_root = None;
        if i != 0 && x.get_child(i - 1)?.get_keys_count()? >= t {
            self.borrow_from_prev(x, i)?;
        } else if i != x.get_children_count()? - 1 && x.get_child(i + 1)?.get_keys_count()? >= t {
            self.borrow_from_next(x, i)?;
        } else {
            if i != x.get_children_count()? - 1 {
                new_root = self.merge(x, i, t)?;
            } else {
                new_root = self.merge(x, i - 1, t)?;
            }
        }
        Ok(new_root)
    }

    fn borrow_from_prev(&self, x: &BTreeNode, i: usize) -> Result<(), Status> {
        let mut child = x.get_child(i)?;
        let mut sibling = x.get_child(i - 1)?;
        let k = x.get_key(i - 1)?;
        child.insert_key(0, k.0, k.1)?;
        //ToDo why is this so ugly? -> Clean Up
        let mut sibling_children = sibling.get_children()?;
        let mut sibling_keys = sibling.get_keys()?;
        let opt_last_sibling_key = sibling_keys.0.pop();
        let opt_last_sibling_row = sibling_keys.1.pop();
        let mut last_sibling_key;
        let mut last_sibling_row;
        if opt_last_sibling_row.is_some() && opt_last_sibling_key.is_some() {
            last_sibling_key = opt_last_sibling_key.unwrap();
            last_sibling_row = opt_last_sibling_row.unwrap();
        } else {
            return Err(Status::InternalExceptionIndexOutOfRange);
        }
        sibling.set_keys(sibling_keys.0, sibling_keys.1)?;
        x.set_key(i - 1, last_sibling_key, last_sibling_row)?;
        if !child.is_leaf() {
            let sc = sibling_children.pop().unwrap();
            sibling.set_children(sibling_children)?;
            child.insert_child(0, sc)?;
        }
        Ok(())
    }

    fn borrow_from_next(&self, x: &BTreeNode, i: usize) -> Result<(), Status> {
        let mut child = x.get_child(i)?;
        let mut sibling = x.get_child(i + 1)?;
        let k = x.get_key(i)?;
        child.push_key(k.0, k.1)?;
        let mut sibling_children = sibling.get_children()?;
        let sk = sibling.remove_key(0)?;

        x.set_key(i, sk.0, sk.1)?;
        if !child.is_leaf() {
            let sc = sibling_children.remove(0);
            sibling.set_children(sibling_children)?;
            child.push_child(sc)?;
        }

        Ok(())
    }
    
    pub fn tomb_cleanup(&mut self) -> Result<(), Status> {
        if let Some(ref root) = self.root {
            self.cleanup_node(root, self.t)?;

            if root.get_keys_count()? == 0 {
                if !root.is_leaf() {
                    let new_root = root.get_child(0)?.clone();
                    PagerFrontend::set_table_root(
                        &self.table_schema,
                        self.pager_accessor.clone(),
                        &new_root,
                    )?;
                    self.root = Some(new_root);
                } else {
                    PagerFrontend::clear_table_root(
                        &self.table_schema,
                        self.pager_accessor.clone(),
                    )?;
                }
            }
        }
        Ok(())
    }

    fn cleanup_node(&self, node: &BTreeNode, t: usize) -> Result<(), Status> {
        let mut i = 0;
        while i < node.get_keys_count()? {
            let key_and_row = node.get_key(i)?;
            //ToDo This should be a node Method
            if Serializer::is_tomb(&key_and_row.0, &self.table_schema)? {
                if node.is_leaf() {
                    let ch = node.get_children()?;
                    node.remove_key(i)?;
                    node.set_children(ch)?
                } else {
                    self.cleanup_internal_node(node, i, t)?;
                }
            } else {
                i += 1;
            }
        }
        if !node.is_leaf() {
            for j in 0..node.get_children_count()? {
                self.cleanup_node(&node.get_child(j)?, t)?;
            }
        }

        Ok(())
    }

    fn cleanup_internal_node(&self, node: &BTreeNode, i: usize, t: usize) -> Result<(), Status> {
        let left_child = node.get_child(i)?.clone();
        let right_child = node.get_child(i + 1)?.clone();
        if left_child.get_keys_count()? >= t {
            let pred_key_and_row = self.get_predecessor(&left_child)?;
            node.set_key(i, pred_key_and_row.0.clone(), pred_key_and_row.1)?;
            self.delete_from(&left_child, pred_key_and_row.0, t)?;
        } else if right_child.get_keys_count()? >= t {
            let succ_key_and_row = self.get_successor(&right_child)?;
            node.set_key(i, succ_key_and_row.0.clone(), succ_key_and_row.1)?;
            self.delete_from(&right_child, succ_key_and_row.0, t)?;
        } else {
            self.merge(node, i, t)?;
        }
        Ok(())
    }

    pub fn scan<Action>(&self, exec: &Action) -> Result<(), Status>
    where
        Action: Fn(&mut Key, &mut Row) -> Result<bool, Status> + Copy,
    {
        if let Some(ref root) = self.root {
            return self.in_order_traversal(root, exec);
        }
        Ok(())
    }

    fn in_order_traversal<Action>(&self, node: &BTreeNode, exec: &Action) -> Result<(), Status>
    where
        Action: Fn(&mut Key, &mut Row) -> Result<bool, Status> + Copy,
    {
        let key_count = node.get_keys_count()?;
        for i in 0..key_count {
            if !node.is_leaf() {
                let child = node.get_child(i)?;
                self.in_order_traversal(&child, exec)?;
            }
            let mut key_and_row = node.get_key(i)?;
            //TODO -- shouldnt mutate key here. Updating Keys is special: i think delete + reinsert :)
            let modified = exec(&mut key_and_row.0, &mut key_and_row.1)?;
            if modified {
                node.set_key(i, key_and_row.0, key_and_row.1)?
            }
        }
        if !node.is_leaf() {
            let child = node.get_child(key_count)?;
            self.in_order_traversal(&child, exec)?;
        }
        Ok(())
    }

    pub fn find<Action>(&self, key: Key, exec: &Action) -> Result<(), Status>
    where
        Action: Fn(&mut Key, &mut Row) -> Result<bool, Status> + Copy,
    {
        if let Some(ref root) = self.root {
            self.find_in_node(root, key, exec)?
        }
        Ok(())
    }

    fn find_in_node<Action>(&self, node: &BTreeNode, key: Key, exec: &Action) -> Result<(), Status>
    where
        Action: Fn(&mut Key, &mut Row) -> Result<bool, Status> + Copy,
    {
        let mut i = 0;
        let key_count = node.get_keys_count()?;

        while i < key_count
            && self.compare(&key, &node.get_key(i)?.0)? == std::cmp::Ordering::Greater
        {
            i += 1;
        }

        if i < key_count && self.compare(&key, &node.get_key(i)?.0)? == std::cmp::Ordering::Equal {
            let mut key_and_row = node.get_key(i)?;
            let modified = exec(&mut key_and_row.0, &mut key_and_row.1)?;
            if modified {
                node.set_key(i, key_and_row.0, key_and_row.1)?
            }
            return Ok(());
        }

        if node.is_leaf() {
            return Err(Status::InternalExceptionKeyNotFound);
        }

        self.find_in_node(&node.get_child(i)?, key, exec)
    }

    pub fn find_range<Action>(
        &self,
        a: Key,
        b: Key,
        include_a: bool,
        include_b: bool,
        exec: &Action,
    ) -> Result<(), Status>
    where
        Action: Fn(&mut Key, &mut Row) -> Result<bool, Status> + Copy,
    {
        if let Some(ref root) = self.root {
            self.find_range_in_node(root, a, b, include_a, include_b, exec)?;
        }
        Ok(())
    }

    fn find_range_in_node<Action>(
        &self,
        node: &BTreeNode,
        a: Key,
        b: Key,
        include_a: bool,
        include_b: bool,
        exec: &Action,
    ) -> Result<(), Status>
    where
        Action: Fn(&mut Key, &mut Row) -> Result<bool, Status> + Copy,
    {
        let key_count = node.get_keys_count()?;
        for i in 0..key_count {
            let key_and_row = node.get_key(i)?;
            if self.compare(&key_and_row.0, &a)? == std::cmp::Ordering::Less {
                continue;
            }
            if !node.is_leaf() {
                let child = node.get_child(i)?;
                self.find_range_in_node(&child, a.clone(), b.clone(), include_a, include_b, exec)?;
            }
            let in_lower_bound = if include_a {
                self.compare(&key_and_row.0, &a)? != std::cmp::Ordering::Less
            } else {
                self.compare(&key_and_row.0, &a)? == std::cmp::Ordering::Greater
            };
            let in_upper_bound = if include_b {
                self.compare(&key_and_row.0, &b)? != std::cmp::Ordering::Greater
            } else {
                self.compare(&key_and_row.0, &b)? == std::cmp::Ordering::Less
            };
            if in_lower_bound && in_upper_bound {
                let mut key_and_row = node.get_key(i)?;
                let modified = exec(&mut key_and_row.0, &mut key_and_row.1)?;
                if modified {
                    node.set_key(i, key_and_row.0, key_and_row.1)?
                }
            } else if self.compare(&key_and_row.0, &b)? == std::cmp::Ordering::Greater {
                break;
            }
        }
        if !node.is_leaf() {
            let child = node.get_child(key_count)?;
            self.find_range_in_node(&child, a, b, include_a, include_b, exec)?;
        }
        Ok(())
    }
}

impl Display for Btree {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(ref root) = self.root {
            let mut queue = std::collections::VecDeque::new();
            let schema = &self.table_schema;
            queue.push_back(root.clone());

            writeln!(f, "Btree Level-Order Traversal:")?;
            while !queue.is_empty() {
                let level_size = queue.len();
                let mut level = Vec::new();

                for _ in 0..level_size {
                    if let Some(node) = queue.pop_front() {
                        let keys = node.get_keys().unwrap();
                        let children = node.get_children().unwrap();
                        level.push((keys.0, keys.1, children.clone()));

                        if !node.is_leaf() {
                            for child in children {
                                queue.push_back(child);
                            }
                        }
                    }
                }
                for (keys, rows, children) in level {
                    write!(f, "{{")?;
                    for (key, row) in keys.iter().zip(rows.iter()) {
                        write!(f, "[")?;
                        if Serializer::is_tomb(key, &schema).unwrap() {
                            write!(f, "X")?;
                        }
                        write!(f, "{}", Serializer::format_key(key, &schema).unwrap())?;
                        write!(f, " :: ")?;
                        write!(f, "{}", Serializer::format_row(row, &schema).unwrap())?;
                        write!(f, "]")?;
                    }
                    write!(f, "[")?;
                    for (child) in children {
                        write!(f, "{:?}", child.position)?;
                        write!(f, " , ")?;
                    }
                    write!(f, "]")?;
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
