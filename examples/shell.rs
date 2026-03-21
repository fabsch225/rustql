use std::io::{self, Write};

use rustql::executor::QueryExecutor;

fn main() {
    let mut exec = QueryExecutor::init("testdb", 10);
    println!("running RustSQL shell...");
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
        println!(
            "  debug - Show debug information. If parameter is provided, show specific information."
        );
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
