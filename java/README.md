# RustQL JDBC Type 4 Driver

This is a pure Java (Type 4) JDBC driver that talks directly to the RustQL TCP server.

## JDBC URL

- `jdbc:rustql://127.0.0.1:5544`
- Default port is `5544` if omitted.

Optional property:
- `timeoutMs` (default `5000`)

## Build

From this folder:

- `mvn package`

## Demo

Run the Rust server first (from the project root):

- `cargo run --example tcp_server`

Then run the Java demo (from this folder):

- `mvn -q exec:java -Dexec.mainClass=com.rustql.demo.Demo`
