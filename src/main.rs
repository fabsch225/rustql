use rustql::btree::Btree;
use rustql::executor::Executor;
use rustql::pager::{Field, PagerCore};

/// # Thoughts on the completed Project
/// The Schema will contain multiple tables -> Table ID and multiple indices
/// A Table consists of an ID Field, and multiple Row Fields -> FieldID
/// # TODOS
/// - Put Schema in an Arc Pointer!
/// - Think about a Smart Vector, that handles caching / sync in the background, an implement the BTree on a Byte Vector.
///       How is this different from the current approach? This would be less abstracted
/// - Remove Schema Information from BTree Node, is stored in the PagerAccessor. In the future, each BTreeNode will store a table id
/// - Think about how to store / cache is_leaf information. The current state is horrible...
///
/// ## IMMEDIATE NEXT STEPS
///

//Important: Our BTrees always start at position 1. Root is Position 1.

//C in/out
//Executor <-> Parser
//B-Tree
//PagerFrontend -> PagerCore
//Disk

const T: usize = 3;

fn main() {
    /*let p = PagerCore::init_from_file("./default.db.bin").expect("Unable to open database");
    let schema = p.schema.as_ref().clone();
    let mut b = Btree::new(T, p.clone());

    let mut row = vec![0u8; schema.clone().row_length];
    row[0..9].copy_from_slice(b"Mock Name");

    for i in 0..7 {
        b.insert(vec![0, 0, 0, i], row.clone());
        println!("{}", b)
    }

    b.delete(vec![0, 0, 0, 2]);
    println!("{}", b);

    //println!("{:?}", b.scan())
    */

    let e = Executor::init("./default.db.bin", T);
    for i in 0..7 {
        e.exec(format!("INSERT INTO table (Id, Name) VALUES ({}, Fabian)", i));
    }
}
