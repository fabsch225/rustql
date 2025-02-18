use rustql::executor::Executor;
use std::io;
use std::io::Write;

/// # Thoughts on the completed Project
/// The Schema will contain multiple tables -> Table ID and multiple indices
/// A Table consists of an ID Field, and multiple Row Fields -> FieldID
///
/// # NEXT STEPS
/// - how to optimize disk space? -> VACUUM
/// - implement multiple tables
///     - [next]:
///         - if i save to disk, the db wont load again :(
///         - update the cached schema when creating a table (OR re-create the schema from the master table every time)
///         - implement an update to the page's free_space field
///         - change the pager's cache: the hashmap should contain position indices as keys, not positions (think: the current implementation *could* be fine)
///     - [Gameplan]: implement the Schema struct, and change PagerCore.read_schema to return always index 0.
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
/// - Pages just like sqlite



//Architecture

//IO in/out
//Parser -> Planner -> Executor
//B-Tree
//PagerFrontend <-> PagerAccessor <-> PagerCore
//File on Disk

const BTREE_NODE_SIZE: usize = 3; //this means a maximum of 5 keys per node
const TOMB_THRESHOLD: usize = 10; //10 percent

fn main() {
    let mut executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
    executor.exec("CREATE TABLE table (id Integer, name String)".to_string());
    for i in 0..300 {
        let query = format!(
            "INSERT INTO table (id, name) VALUES ({}, 'Kunde Nummer {}')",
            i,
            i * 3
        );
        println!("{}", query);
        executor.exec(query);
        //debug does a loop !?!?!?
        executor.debug(Some("table"));
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
        let v = command.split(" ").collect::<Vec<&str>>();
        if v[0].eq_ignore_ascii_case("debug") {
            if v.len() == 2 {
                let parameter = v[1];
                executor.debug(Some(parameter));
            } else {
                executor.debug(None);
            }
        } else {
            let result = executor.exec(command.to_string());
            println!("{}", result);
        }
    }
}
