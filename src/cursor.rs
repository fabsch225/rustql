use crate::btree::{BTreeNode, Btree};
use crate::pager::{Key, Row};
use crate::status::Status;

#[derive(Debug, Clone)]
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

        let (node, idx) = &self.stack[self.stack.len() - 1];
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
        let (node, idx) = &self.stack[self.stack.len() - 1];
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
