use rustql::server::serve_tcp;

fn main() {
    let bind_addr = "127.0.0.1:5544";
    let db_path = "./server.db.bin";
    let btree_node_width = 10;

    if let Err(e) = serve_tcp(bind_addr, db_path, btree_node_width) {
        eprintln!("server failed: {e}");
    }
}
