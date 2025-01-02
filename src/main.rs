use std::io;
use std::io::Write;
use rustql::executor::Executor;

/// # Thoughts on the completed Project
/// The Schema will contain multiple tables -> Table ID and multiple indices
/// A Table consists of an ID Field, and multiple Row Fields -> FieldID
///
/// # NEXT STEPS
/// - organize is_leaf flag usage, streamline usage / caching
/// - implement DELETE -> how to optimize disk space? -> flag: deleted, then shift everything to the left? -> rather expensive!
/// - implement dirty flags in the cache? why? if we optimize disk usage at the same time, why bother?
///     - we already have the PagerAccessor::write_page - hook -> use that, standardizing this is clean
/// - implement indices
/// - implement multiple tables
/// - optimize disk usage further
///
/// ## Bugs
/// - SELECT * FROM table WHERE age >= 80 -> returns empty, > 80 works
/// - SELECT * FROM table WHERE id = 0 -> returns empty, = 2  works
///     - WHERE id < 1 does not work
///
/// # Virtual Memory Strategy for working with multiple things
/// Each table has a property 'offset' of type Position
/// - as well as a modified
/// To calculate a position on Disk, add the offset
/// if we insert bytes somewhere, just increase that tables offset


//C in/out
//Executor <-> Parser
//B-Tree
//PagerFrontend / PagerAccessor -> PagerCore
//Disk

const BTREE_NODE_SIZE: usize = 3; //this means a maximum of 5 keys per node
const TOMB_THRESHOLD: usize = 10; //10 percent

fn main() {
    let executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);

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
