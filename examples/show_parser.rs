use std::io;
use std::io::Write;
use rustql::parser::*;

fn main() {
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
                    Ok(parsed) => println!("{:?}", parsed),
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