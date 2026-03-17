use rustql::executor::{self, QueryExecutor};
use std::io;
use std::io::Write;

/// # NEXT STEPS
/// - [x] Fix Serializer: Accessing Data on Pages with Cell>0.
///     - [x] Add Tests
/// - Improve Space Management
///     - Implement Different Page Types.
///         1. Key Page
///         2. Row Page
///         3. Overflow Page
///     - PagerProxy could handle this (with Serializer callback if overflow)?
///     - Implement FindSpace Method in the Pager? in the BTree? (probably better for locality (do this in the range of the Key))
/// - Refactor DataFrames
///     -  [x] implement Joins and Setoperations There -> Break up the Executor Struct
/// - [x] Fix the Parser -> tests::test_join_subquery_with_union
/// - [x] varchar
/// - implement a lookup table in the schema (instead of search the table_index for a name)
/// - implement methods for memory saving
///     - [x] enable the Parent-Hint during the split
///     - [x] implement overflow pages, and use the free_space parameter correctly (dont assume maximum length of a node)
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
/// - [x] Pages just like sqlite
///     1. Main Pages
///     2. Overflow Pages
///     3. Data Pages (Strings and Varchars)
/// - [x] Maybe a freelist for pages? (free table ?)
/// - How to detect, if a page is not used anymore?
///    1. perform a tomb cleanup
///    2. if the first node has 0 keys, the page is unused

const BTREE_NODE_SIZE: usize = 3; //this means a maximum of 5 keys per node
pub const TOMB_THRESHOLD: usize = 10; //10 percent

fn main() {
    let mut exec = QueryExecutor::init("./default.db.bin", BTREE_NODE_SIZE);
    println!("running RustSQL shell...");

    exec.prepare("CREATE TABLE A (id Integer, v Integer)".into());
    exec.prepare("CREATE TABLE B (id Integer, v Integer)".into());
    exec.prepare("CREATE TABLE C (id Integer, v Integer)".into());
    exec.prepare("CREATE TABLE D (id Integer, v Integer)".into());
    exec.prepare("CREATE TABLE Lg (id Integer, name String, city String, age Integer, birth_date Date, description Varchar(512))".into());

    fn large_string(n: usize) -> String {
        "x".repeat(n)
    }

    for i in 1..=100 {
        exec.prepare(format!("INSERT INTO Lg VALUES ({}, 'Name{} - {}', 'City{} - {}', {}, '2000-01-01', 'Description for {} is {}')", i,
        large_string(100), i, large_string(200), i, i, i, large_string(400)));
    }

    // A: 1..50
    for i in 1..=10 {
        exec.prepare(format!("INSERT INTO A VALUES ({}, {})", i, i * 2));
    }
    // B: 25..75
    for i in 25..=75 {
        exec.prepare(format!("INSERT INTO B VALUES ({}, {})", i, i * 3));
    }
    // C: 40..90
    for i in 40..=90 {
        exec.prepare(format!("INSERT INTO C VALUES ({}, {})", i, i * 4));
    }
    // D: 10..60
    for i in 10..=60 {
        exec.prepare(format!("INSERT INTO D VALUES ({}, {})", i, i * 5));
    }
   
    let query = r#"
        SELECT A.id FROM (SELECT A.id FROM A INNER JOIN D ON D.id = A.id UNION SELECT B.id FROM B WHERE B.v > 5) INTERSECT (SELECT C.id FROM C INNER JOIN Lg ON Lg.age = C.id)
    "#;
    exec.plan(query.to_string());
    loop {
        if handle_cli(&mut exec) {
            break;
        }
    }
}

fn handle_cli(executor: &mut QueryExecutor) -> bool {
    io::stdout().flush().unwrap();

    let mut command = String::new();
    io::stdin().read_line(&mut command).unwrap();
    let command = command.trim();

    if command.eq_ignore_ascii_case("exit") {
        executor.exit();
        return true;
    }
    let v = command.split(" ").collect::<Vec<&str>>();
    if v[0].eq_ignore_ascii_case("debug") {
        if v.len() == 2 {
            let parameter = v[1];
            executor.debug(Some(parameter));
        } else {
            executor.debug(None);
        }
    } else if v[0].eq_ignore_ascii_case("help") {
        println!("Available commands:");
        println!("  debug - Show debug information. If parameter is provided, show specific information.");
        println!("  exit - Exit the shell and save the database.");
        println!("  help - Show this help message.");
        println!("  plan - Show the execution plan for a query. Usage: plan <SQL_QUERY>");
        println!("  debug_pager - Show the internal state of the pager.");
    } else if v[0].eq_ignore_ascii_case("plan") {
        let result = executor.plan(command.to_string().replace("plan ", ""));
        println!("{}", result);
    } else if v[0].eq_ignore_ascii_case("debug_pager") {
        executor.debug_pager();
    } else {
        let result = executor.prepare(command.to_string());
        println!("{}", result);
    }
    return false;
}

//SELECT * FROM Namen INNER JOIN Orders ON Namen.Id = Orders.Id
//SELECT * FROM Namen INNER JOIN (SELECT Dest, Id FROM Orders) ON Namen.Id = Orders.Id
