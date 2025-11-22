use rustql::parser::*;
use std::io;
use std::io::Write;
use rustql::executor::{Executor, QueryResult};
use rustql::planner::{CompiledQuery, Planner};

fn main() {
    let mut executor = Executor::init("./default.db.bin", 3);
    executor.exec("CREATE TABLE test (id Integer, name String)".to_string());
    println!("Queries will be compiled against the following Schema:");
    executor.debug(None);
    loop {
        print!("Enter query: ");
        io::stdout().flush().unwrap();

        let mut query = String::new();

        match io::stdin().read_line(&mut query) {
            Ok(_) => {
                let trimmed_query = query.trim();
                if trimmed_query.is_empty() {
                    println!("Exiting...");
                    break;
                }

                let mut p = Parser::new(trimmed_query.to_string());

                match p.parse_query() {
                    Ok(parsed) => {
                        println!("{:?}", parsed);
                        let planner_result = Planner::plan(&executor.schema, parsed);
                        match planner_result {
                            Ok(compiled_query) => {
                                println!("{:?}", compiled_query);
                            }
                            Err(e) => println!("Error compiling query: {}", e)
                        }
                    },
                    Err(e) => println!("Error parsing query: {:?}", e),
                }
            }
            Err(e) => {
                println!("Failed to read input: {:?}", e);
                break;
            }
        }
    }
}
// SELECT * FROM (SELECT * FROM x INNER JOIN y ON x.a = y.b) INNER JOIN z ON z.a = y.b