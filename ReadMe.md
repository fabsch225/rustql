# Rust-SQL
- Relational Database in Rust
- currently, there is ever only one table, CREATE TABLE drops the old one

# Running the Database
- to run the database, use
```
cargo run
```
- write SQL queries in c-in, read the results from c-out
- `exit` saves the Database to disk

# Currently implemented SQL
- CREATE TABLE (tablename will always be ignored, there is one table)
- INSERT INTO ...
- SELECT ... WHERE x <= 2 AND ... AND (only AND)