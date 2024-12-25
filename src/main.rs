use rustql::btree::Btree;
use rustql::pager::Pager;

///TODOS
/// - [ ] Unify Error Handling: add where there is none, replace Option<> with Result<> and a correct status code


//C in/out
//Parser
//B+ Tree
//(PageReader)
//Pager
//(Serializer)
//Disk

const T: usize = 3;

fn main() {
    let p = Pager::init_from_file("./default.db.bin").expect("Unable to open database");
}
