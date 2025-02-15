# Rust-SQL
- Relational Database in Rust
- currently, there is only ever one table, CREATE TABLE drops the old one

# Running the Database
- [install Rust / Cargo](https://rustup.rs/) 
- to run the database, use
```
cargo run
```
- write SQL queries in c-in, read the results from c-out
- `exit` saves the Database to disk

# Currently implemented SQL
- CREATE TABLE ..., DROP TABLE ...
- INSERT INTO ...
- SELECT ... WHERE x <= 2 AND ... AND (only AND)
- DELETE ... WHERE ...
