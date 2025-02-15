use std::io;
use std::io::Write;
use rustql::executor::Executor;

/// # Thoughts on the completed Project
/// The Schema will contain multiple tables -> Table ID and multiple indices
/// A Table consists of an ID Field, and multiple Row Fields -> FieldID
///
/// # NEXT STEPS
/// - how to optimize disk space? -> flag: deleted, then shift everything to the left? -> rather expensive!
/// - implement multiple tables
///     - next: change bytes_to_schema, and schema_to_bytes, init_from_file
///     - Gameplan: implement the Schema struct, and change PagerCore.read_schema to return always index 0.
///     - Refactor Paging: use a variable Pagesize
///         - Position => (PageNumber, Position on page)
///     - Store the Schema in a Master Table
///     - change how the btree is instantiated
///     - make the final changes to the executor
/// - implement nullable values (groundwork is layed)
/// - implement indices
/// - optimize disk usage further
///
/// ## Bugs
/// - SELECT * FROM table WHERE age >= 80 -> returns empty, > 80 works (pretty sure thats fixed)
/// - SELECT * FROM table WHERE id = 0 -> returns empty, = 2  works  (pretty sure thats fixed)
///     - WHERE id < 1 does not work (pretty sure thats fixed)
///
/// # Virtual Memory Strategy for working with multiple things
/// Each table has a property 'offset' of type Position
/// - as well as a modified
/// To calculate a position on Disk, add the offset
/// if we insert bytes somewhere, just increase that tables offset

//current status, note to future self
// - changing executor + pager to work with master_table, bigger pages etc


//C in/out
//Executor <-> Parser
//B-Tree
//PagerFrontend / PagerAccessor -> PagerCore
//Disk

const BTREE_NODE_SIZE: usize = 3; //this means a maximum of 5 keys per node
const TOMB_THRESHOLD: usize = 10; //10 percent

fn main() {
    let executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
    executor.exec("CREATE TABLE table (id Integer, name String)".to_string());
    for i in 0..300 {
        executor.exec(format!("INSERT INTO table (id, name) VALUES ({}, 'Kunde Nummer {}')", i, i * 3));
    }
    println!("running RustSQL shell...");
    loop {
        io::stdout().flush().unwrap();

        let mut command = String::new();
        io::stdin().read_line(&mut command).unwrap();
        let command = command.trim();

        if command.eq_ignore_ascii_case("exit") {
            executor.exit();
            break;
        }
        if command.eq_ignore_ascii_case("debug") {
            executor.debug();
        } else {
            let result = executor.exec(command.to_string());
            println!("{}", result);
        }
    }
}
