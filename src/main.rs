use rustql::executor::Executor;
use std::io;
use std::io::Write;

/// # NEXT STEPS
/// - varchar
/// - implement a lookup table in the schema (instead of search the table_index for a name)
/// - implement methods for memory saving
///     1. enable the Parent-Hint during the split
///     2. implement overflow pages, and use the free_space parameter correctly (dont assume maximum length of a node)
///     3. set a max cache size
///     4. VACUUM
/// # Gameplan:
/// - constraints (unique, nullable)
/// - create a more ambitious executer -> "real" compilation + virtual-machine -- this would enable subqueries etc
/// - joins + constraints: primary key, foreign key,
/// - indices
/// - views i.e. virtual tables / virtual b-trees (is this necessary for joins also?)
///
///
/// # Virtual Memory Strategy for working with multiple things
/// - Pages just like sqlite
/// - How to detect, if a page is not used anymore?
///
/// # Architecture
///
/// - IO in/out
/// - Parser -> Planner -> Executor
/// - B-Tree
/// - PagerFrontend <-> PagerAccessor <-> PagerCore
/// - File on Disk

const BTREE_NODE_SIZE: usize = 3; //this means a maximum of 5 keys per node
const TOMB_THRESHOLD: usize = 10; //10 percent

fn main() {
    let mut executor = Executor::init("./default.db.bin", BTREE_NODE_SIZE);
    println!("running RustSQL shell...");
    /*
    for i in 0..30 {
        executor.exec(format!("CREATE TABLE users{} (id Integer, name String))", i));
    }
    for i in 0..50 {
        executor.exec(format!("INSERT INTO users{} (id, name) VALUES ({}, 'Fabian')", i % 4, i));
    }*/
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
