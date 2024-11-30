mod btree;
mod table;
mod pager;
mod status;

use btree::Btree;

const T: usize = 3;

fn main() {
    let mut t = Btree::new(T);

    /*
    t.insert(9);
    t.insert(9);
    t.insert(10);
    t.insert(11);
    t.insert(15);
    t.insert(16);
    t.insert(17);
    t.insert(18);
    t.insert(20);
    t.insert(23);
    */

    for i in 0..10 {
        t.insert((i, 2 * i));
    }

    print!("{:?}", t);
    t.delete((8,16));
    print!("{:?}", t);;
}
