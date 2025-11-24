use rustql::executor::QueryExecutor;
use std::io;
use std::io::Write;

/// # NEXT STEPS
/// - Fix Serializer: Accessing Data on Pages with Cell>0.
///     - Add Tests
/// - Improve Space Management
///     - Implement Different Page Types.
///         1. Key Page
///         2. Row Page
///         3. Overflow Page
///     - PagerProxy could handle this (with Serializer callback if overflow)?
///     - Implement FindSpace Method in the Pager? in the BTree? (probably better for locality (do this in the range of the Key))
/// - Refactor DataFrames
///     -  [x] implement Joins and Setoperations There -> Break up the Executor Struct
/// - Fix the Parser -> tests::test_join_subquery_with_union
/// - varchar
/// - implement a lookup table in the schema (instead of search the table_index for a name)
/// - implement methods for memory saving
///     1. enable the Parent-Hint during the split
///     2. implement overflow pages, and use the free_space parameter correctly (dont assume maximum length of a node)
///     3. set a max cache size
///     4. VACUUM
/// # Gameplan:
/// - [x] create an Iterator-Pattern on the BTree, add a cursor, implement this in the executor, preferably before joins etc
/// - autosaving, autocleanup, auto-vacuum (?)
///     - the pager should store a translation layer for positions
///     - Pages that have been accessed close to each other should be stored in proximity
///     - when the page is written to disk, the translation layer is applied. Then we would have to collect and update each reference to that page
///         - can this be done in the background?
/// - constraints (unique, nullable), primary key, foreign key,
/// - [x] create a more ambitious executer -> "real" compilation + virtual-machine -- this would enable subqueries etc
/// - [x] joins, setops
/// - indices
/// - [x] views i.e. virtual tables / virtual b-trees (is this necessary for joins also?) ---> DataFrame ^^
///
/// # Virtual Memory Strategy for working with multiple things
/// - Pages just like sqlite
/// - Maybe a freelist for pages? (free table ?)
/// - How to detect, if a page is not used anymore?
///    1. perform a tomb cleanup
///    2. if the first node has 0 keys, the page is unused
///
/// # Architecture
///
/// - IO in/out
/// - Parser -> Planner -> Executor
/// - B-Tree
/// - PagerProxy <-> PagerAccessor <-> PagerCore
/// - File on Disk

const BTREE_NODE_SIZE: usize = 3; //this means a maximum of 5 keys per node
pub const TOMB_THRESHOLD: usize = 10; //10 percent

fn main() {
    let mut executor = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
    println!("running RustSQL shell...");

    executor.prepare(format!(
        "CREATE TABLE Users (id Integer, name String, place String))"
    ));

    for i in 0..50 {
        executor.prepare(format!(
            "INSERT INTO Users (id, name, place) VALUES ({}, 'User {}', 'Place {}')",
            i,
            i,
            i % 4
        ));
    }
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
            let result = executor.prepare(command.to_string());
            println!("{}", result);
        }
    }
}

//SELECT * FROM Namen INNER JOIN Orders ON Namen.Id = Orders.Id
//SELECT * FROM Namen INNER JOIN (SELECT Dest, Id FROM Orders) ON Namen.Id = Orders.Id
