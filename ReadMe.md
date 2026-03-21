# Rust-SQL
- Relational database in rust
- Broad testsuite
- Persistance to disk
- Basic concurrency: table-level locking, multiple readers and one writer per table. connect via tcp.
    - On a Deadlock, all transactions are rejected  
- Can run embedded or as a server
- ToDo: Vacuuming

# Running the Database
- [install Rust / Cargo](https://rustup.rs/) 
- to run the database, use
```
cargo run
```
- write SQL queries in c-in, read the results from c-out
- `exit` saves the database to disk
- see `help` for other commands
- see `./java/...` for a jdbc driver, which connects to the database via sockets (type 4 driver)

# Currently implemented SQL
- CREATE TABLE ..., DROP TABLE ...
- INSERT INTO ...
- SELECT ... / DELETE FROM ... WHERE ... AND / OR / XOR ( ... )
- UPDATE ... SET ... = ... WHERE ...
- INNER JOIN (=JOIN) and NATURAL JOIN (also "inner")
- Subqueries
- CREATE INDEX ... ON ... (...), DROP INDEX ...
- Setoperations: UNION, ALL, INTERSECT, EXCEPT (=MINUS)
- BEGIN TRANSACTION, ROLLBACK, COMMIT

# Architecture

### Server
1. Java-clients with the included jdbc type 4 driver or other implementation of the TCP protocol
2. Server
3. PagerAccessor (virtual transactions per client-thread (or real transactions if the client explicitly starts them))
4. QueryExecutor (shared pager)
5. ...

### Embedded
1. Rust-program or RustQL-Shell
2. QueryExecutor -uses-> Parser and Planner (1st treewalker and optimizer: pushes projections and filters)
3. Dataframe (lazy evaluation of planned tree. 2nd treewalker)
4. BTreeCursor
5. BTree
6. BTreeNode (interface from the btree to the pagerproxy)
7. PagerProxy (get and set rows and keys and children of nodes)
8. PageManager (manages data and overflow pages)
9. PagerAccessor (access control and transaction management)
10. Pager (basically a hashmap and writes to file)