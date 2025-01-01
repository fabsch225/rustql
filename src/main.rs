use std::io;
use std::io::Write;
use rustql::executor::Executor;

/// # Thoughts on the completed Project
/// The Schema will contain multiple tables -> Table ID and multiple indices
/// A Table consists of an ID Field, and multiple Row Fields -> FieldID
/// # TODOS
/// - Think about how to store / cache is_leaf information. The current state is horrible...
///
/// ## IMMEDIATE NEXT STEPS
/// - implement DELETE
/// - implement indices
/// - implement multiple tables
/// - optimize disk usage
///
/// ## Bugs
/// - SELECT * FROM table WHERE age >= 80 -> returns empty, > 80 works
/// - SELECT * FROM table WHERE id = 0 -> returns empty, = 2  works
///     - WHERE id < 1 does not work

//C in/out
//Executor <-> Parser
//B-Tree
//PagerFrontend / PagerAccessor -> PagerCore
//Disk

const T: usize = 3;

fn main() {
    let executor = Executor::init("./default.db.bin", T);

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
