# Rust-SQL
- Relational database in rust
- Broad testsuite
- Persistance to disk
- Basic concurrency: table-level locking, multiple readers OR one writer. connect via tcp.

# Running the Database
- [install Rust / Cargo](https://rustup.rs/) 
- to run the database, use
```
cargo run
```
- write SQL queries in c-in, read the results from c-out
- `exit` saves the database to disk
- see `help` for other CLI commands
- see `./java/...` for a jdbc driver, which connects to the database via sockets (type 4 driver)

# Currently implemented SQL
- CREATE TABLE ..., DROP TABLE ...
- INSERT INTO ...
- SELECT ... WHERE x <= 2 AND ... AND (only AND)
- DELETE ... WHERE ...
- INNER JOIN and JOIN (natural (inner) join)
- Subqueries
- CREATE INDEX ... ON ... (...), DROP INDEX ...
- Setoperations: UNION, ALL, INTERSECT, EXCEPT (=MINUS)
- BEGIN TRANSACTION, ROLLBACK, COMMIT

# Architecture
1. Server
2. QueryExecutor -uses-> Parser and Planner (1st treewalker and optimizer: pushes projections and filters)
3. Dataframe (lazy evaluation planned tree. 2nd treewalker)
4. BTreeCursor
5. BTree
6. BTreeNode (interface from the btree to the pagerproxy)
7. PagerProxy (get and set rows and keys and children of nodes)
8. PageManager (manages data and overflow pages)
9. PagerAccessor (access control and transaction management)
10. Pager (basically a hashmap and writes to file)