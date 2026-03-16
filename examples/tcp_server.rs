use rustql::server::serve_tcp;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let bind_addr = args
        .get(1)
        .map(String::as_str)
        .unwrap_or("127.0.0.1:5544");
    let db_path = args
        .get(2)
        .map(String::as_str)
        .unwrap_or("./server.db.bin");
    let btree_node_width = 10;

    if let Err(e) = serve_tcp(bind_addr, db_path, btree_node_width) {
        eprintln!("server failed: {e}");
    }
}
