use std::io;
use std::io::Write;
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
/// - implement the select from query on b-tree level
/// - think about a data-frame struct

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


    let e = Executor::init("./default.db.bin", T);
    for i in 0..30 {
        e.exec(format!("INSERT INTO table (Id, Name) VALUES ({}, 'Test Name Nummer {}')", i, i));
    }

    let r = e.exec(format!("INSERT INTO table (Id, Name) VALUES ({}, 'Test Name Nummer {}')", 2, 22));
    println!("{}", r);
    let r = e.exec(format!("SELECT Id, Name FROM table"));
    println!("{}", r);
    let r = e.exec(format!("SELECT Id, Name FROM table WHERE Name = 'Test Name Nummer 29'"));
    println!("{}", r);
    let r =  e.exec(format!("SELECT Id FROM table WHERE Name > 'Test Name Nummer 23'"));
    println!("{}", r);
     */

    let executor = Executor::init("./default.db.bin", T);
/*
    for i in 0..5 {
        executor.exec(format!("INSERT INTO table (Id, Name) VALUES ({}, 'Test Name Nummer {}')", i, i));
    }

    executor.exit();
*/
    executor.exec("SELECT * FROM table WHERE Id = 3".to_string());

    loop {
        io::stdout().flush().unwrap();

        let mut command = String::new();
        io::stdin().read_line(&mut command).unwrap();
        let command = command.trim();

        if command.eq_ignore_ascii_case("exit") {
            executor.exit();
            break;
        }

        let result = executor.exec(command.to_string());
        println!("{}", result);
    }
}
