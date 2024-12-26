use rustql::btree::Btree;
use rustql::pager::PagerCore;

///TODOS
/// - [ ] Put Schema in an Arc Pointer!

//C in/out
//Parser
//B+ Tree
//PagerFrontend
//Serializer
//PagerAccessor -> PagerCore
//Disk

const T: usize = 3;

fn main() {
    let p = PagerCore::init_from_file("./default.db.bin").expect("Unable to open database");
}
