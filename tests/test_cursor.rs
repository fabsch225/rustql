mod tests {
    use rustql::{
        btree::{Btree, BTreeCursor},
        pager::{PagerCore, PagerAccessor, Key, Row, Position, Type},
        executor::{TableSchema, Field}
    };
    use rand::{seq::SliceRandom, Rng};

    fn make_int_key(k: i32) -> Vec<u8> {
        let mut v = Vec::with_capacity(5);
        v.push(0x01);
        v.extend_from_slice(&k.to_be_bytes());
        v
    }

    fn make_row(k: i32) -> Vec<u8> {
        let mut v = vec![0u8; 256];
        let b = k.to_be_bytes();
        v[0] = b[0];
        v[1] = b[1];
        v[2] = b[2];
        v[3] = b[3];
        v
    }

    fn extract_int_from_key(k: &Vec<u8>) -> i32 {
        let b = [k[1], k[2], k[3], k[4]];
        i32::from_be_bytes(b)
    }

    fn extract_int_from_row(r: &Vec<u8>) -> i32 {
        let b = [r[0], r[1], r[2], r[3]];
        i32::from_be_bytes(b)
    }

    fn get_schema() -> TableSchema {
        TableSchema {
            next_position: Position::new(0, 0),
            root: Position::new(0, 0),
            col_count: 2,
            key_and_row_length: 260,
            key_length: 4,
            key_type: Type::Integer,
            row_length: 256,
            fields: vec![
                Field { name: "Id".to_string(), field_type: Type::Integer },
                Field { name: "Name".to_string(), field_type: Type::String },
            ],
            table_type: 0,
            entry_count: 0,
        }
    }

    fn make_tree() -> Btree {
        let pager = PagerCore::init_from_file("./default.db.bin").unwrap();
        Btree::init(3, pager, get_schema()).unwrap()
    }

    fn collect_cursor_values(tree: &Btree) -> Vec<(i32, i32)> {
        let mut c = BTreeCursor::new(tree.clone());
        let mut out = vec![];
        while c.is_valid() {
            let (k, r) = c.current().unwrap().unwrap();
            out.push((extract_int_from_key(&k), extract_int_from_row(&r)));
            c.advance().unwrap();
        }
        out
    }

    #[test]
    fn test_01_basic_insert() {
        let mut t = make_tree();
        let keys = vec![10,20,5,6,12,30,7,17];
        for k in &keys {
            t.insert(make_int_key(*k), make_row(*k)).unwrap();
        }
        let vals = collect_cursor_values(&t);
        let mut expected: Vec<(i32,i32)> = keys.iter().map(|k| (*k,*k)).collect();
        expected.sort_by_key(|x| x.0);
        assert_eq!(vals, expected);
    }

    #[test]
    fn test_02_stress_insert() {
        let mut t = make_tree();
        let mut rng = rand::thread_rng();
        let random_keys: Vec<i32> = (0..1000).map(|_| rng.gen_range(0..10000)).collect();
        let mut unique = random_keys.clone();
        unique.sort();
        unique.dedup();
        for k in &unique {
            t.insert(make_int_key(*k), make_row(*k)).unwrap();
        }
        let vals = collect_cursor_values(&t);
        let expected: Vec<(i32,i32)> = unique.iter().map(|k| (*k,*k)).collect();
        assert_eq!(vals.len(), expected.len());
        assert_eq!(vals, expected);
    }

    #[test]
    fn test_03_stress_delete() {
        let mut t = make_tree();
        let mut keys: Vec<i32> = (0..500).collect();
        keys.shuffle(&mut rand::thread_rng());
        for k in &keys {
            t.insert(make_int_key(*k), make_row(*k)).unwrap();
        }
        let to_delete = &keys[0..250];
        let mut remaining: Vec<(i32,i32)> = keys[250..].iter().map(|k| (*k,*k)).collect();
        remaining.sort_by_key(|x| x.0);
        for k in to_delete {
            t.delete(make_int_key(*k)).unwrap();
        }
        let vals = collect_cursor_values(&t);
        assert_eq!(vals, remaining);
    }

    #[test]
    fn test_04_cursor_validity_during_empty() {
        let mut t = make_tree();
        let mut c = BTreeCursor::new(t.clone());
        assert!(!c.is_valid());
        assert!(c.current().unwrap().is_none());
        t.insert(make_int_key(10), make_row(10)).unwrap();
        t.delete(make_int_key(10)).unwrap();
        c = BTreeCursor::new(t.clone());
        assert!(!c.is_valid());
    }

    #[test]
    fn test_05_bidirectional_traversal() {
        let mut t = make_tree();
        let keys = vec![10,20,5,15,25,30];
        for k in &keys { t.insert(make_int_key(*k), make_row(*k)).unwrap(); }
        let mut expected: Vec<(i32,i32)> = keys.iter().map(|k| (*k,*k)).collect();
        expected.sort_by_key(|x| x.0);
        let mut c = BTreeCursor::new(t.clone());
        c.move_to_end().unwrap();
        let (k,r)=c.current().unwrap().unwrap();
        assert_eq!((extract_int_from_key(&k),extract_int_from_row(&r)), expected.last().unwrap().clone());
        let mut rev = vec![];
        while c.is_valid() {
            let (k,r)=c.current().unwrap().unwrap();
            rev.push((extract_int_from_key(&k),extract_int_from_row(&r)));
            c.decrease().unwrap();
        }
        assert_eq!(collect_cursor_values(&t), expected);
        assert_eq!(rev, expected.iter().rev().cloned().collect::<Vec<_>>());
    }

    #[test]
    fn test_06_zigzag_movement() {
        let mut t = make_tree();
        for k in 1..=5 { t.insert(make_int_key(k), make_row(k)).unwrap(); }
        let mut c = BTreeCursor::new(t.clone());
        c.move_to_start().unwrap();
        assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0), 1);
        c.advance().unwrap();
        assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0), 2);
        c.advance().unwrap();
        assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0), 3);
        c.decrease().unwrap();
        assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0), 2);
        c.decrease().unwrap();
        assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0), 1);
        c.decrease().unwrap();
        assert!(!c.is_valid());
    }

    #[test]
    fn test_07_large_random_zigzag() {
        let mut t = make_tree();
        let mut keys: Vec<i32> = (0..200).collect();
        keys.shuffle(&mut rand::thread_rng());
        for k in &keys { t.insert(make_int_key(*k), make_row(*k)).unwrap(); }
        let expected: Vec<(i32,i32)> = {
            let mut v: Vec<(i32,i32)> = keys.iter().map(|k| (*k,*k)).collect();
            v.sort_by_key(|x|x.0);
            v
        };
        let mut c = BTreeCursor::new(t.clone());
        let mut idx = 0usize;
        for _ in 0..1000 {
            let dir = if rand::random::<bool>() {1} else {-1};
            let steps = rand::thread_rng().gen_range(1..=5);
            if dir == 1 {
                let possible = 199 - idx;
                let real = steps.min(possible);
                for _ in 0..real { c.advance().unwrap(); }
                idx += real;
            } else {
                let real = steps.min(idx);
                for _ in 0..real { c.decrease().unwrap(); }
                idx -= real;
            }
            if idx < 200 {
                assert!(c.is_valid());
                let (k,r)=c.current().unwrap().unwrap();
                let p=(extract_int_from_key(&k),extract_int_from_row(&r));
                assert_eq!(p, expected[idx]);
            }
        }
    }

    #[test]
    fn test_08_boundary_zigzag() {
        let mut t = make_tree();
        for k in 0..5 { t.insert(make_int_key(k), make_row(k)).unwrap(); }
        let mut c = BTreeCursor::new(t.clone());
        c.advance().unwrap();
        c.decrease().unwrap();
        assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0),0);
        c.decrease().unwrap();
        assert!(!c.is_valid());
        c.move_to_start().unwrap();
        assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0),0);
        c.move_to_end().unwrap();
        assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0),4);
        c.decrease().unwrap();
        c.advance().unwrap();
        assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0),4);
        c.advance().unwrap();
        assert!(!c.is_valid());
    }

    #[test]
    fn test_09_go_to_less_than_equal_found() {
        let mut t = make_tree();
        let keys = vec![10,20,5,15,25,30];
        for k in &keys { t.insert(make_int_key(*k), make_row(*k)).unwrap(); }
        let mut c = BTreeCursor::new(t.clone());
        c.go_to_less_than_equal(&make_int_key(5)).unwrap();
        assert!(c.is_valid());
        assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0),5);
        c.go_to_less_than_equal(&make_int_key(20)).unwrap();
        assert!(c.is_valid());
        assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0),20);
        c.go_to_less_than_equal(&make_int_key(30)).unwrap();
        assert!(c.is_valid());
        assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0),30);
    }

    #[test]
    fn test_10_go_to_less_than_equal_not_found_predecessor() {
        let mut t = make_tree();
        for k in [10,20,30] { t.insert(make_int_key(k), make_row(k)).unwrap(); }
        let mut c = BTreeCursor::new(t.clone());
        c.go_to_less_than_equal(&make_int_key(15)).unwrap();
        assert!(c.is_valid());
        assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0),10);
        c.advance().unwrap();
        assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0),20);
        c.go_to_less_than_equal(&make_int_key(35)).unwrap();
        assert!(c.is_valid());
        assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0),30);
        c.advance().unwrap();
        assert!(!c.is_valid());
    }

    #[test]
    fn test_11_go_to_less_than_equal_not_found_invalid() {
        let mut t = make_tree();
        for k in [10,20,30] { t.insert(make_int_key(k), make_row(k)).unwrap(); }
        let mut c = BTreeCursor::new(t.clone());
        c.go_to_less_than_equal(&make_int_key(5)).unwrap();
        assert!(!c.is_valid());
        c.move_to_start().unwrap();
        assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0),10);
    }

    #[test]
    fn test_12_go_to_found() {
        let mut t = make_tree();
        let keys = vec![10,20,5,15,25,30];
        for k in keys { t.insert(make_int_key(k), make_row(k)).unwrap(); }
        let mut c = BTreeCursor::new(t.clone());
        c.go_to(&make_int_key(5)).unwrap();
        assert!(c.is_valid());
        assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0),5);
        c.go_to(&make_int_key(20)).unwrap();
        assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0),20);
        c.go_to(&make_int_key(30)).unwrap();
        assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0),30);
    }

    #[test]
    fn test_13_go_to_not_found_invalid() {
        let mut t = make_tree();
        for k in [10,20,30] { t.insert(make_int_key(k), make_row(k)).unwrap(); }
        let mut c = BTreeCursor::new(t.clone());
        c.go_to(&make_int_key(15)).unwrap();
        assert!(!c.is_valid());
        c.go_to_less_than_equal(&make_int_key(5)).unwrap();
        assert!(!c.is_valid());
        c.go_to(&make_int_key(5)).unwrap();
        assert!(!c.is_valid());
        c.go_to_less_than_equal(&make_int_key(10)).unwrap();
        assert!(c.is_valid());
        assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0),10);
        c.go_to(&make_int_key(20)).unwrap();
        assert!(c.is_valid());
        assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0),20);
    }

    #[test]
    fn test_14_stress_go_to_and_traversal() {
        let mut t = make_tree();
        let mut keys: Vec<i32> = (1000..2500).collect();
        keys.shuffle(&mut rand::thread_rng());
        for k in &keys { t.insert(make_int_key(*k), make_row(*k)).unwrap(); }
        let mut expected: Vec<(i32,i32)> = keys.iter().map(|k| (*k,*k)).collect();
        expected.sort_by_key(|x| x.0);
        let mut c = BTreeCursor::new(t.clone());
        for _ in 0..500 {
            let target = expected[rand::thread_rng().gen_range(0..expected.len())].0;
            let idx = expected.iter().position(|p| p.0 == target).unwrap();
            c.go_to(&make_int_key(target)).unwrap();
            assert!(c.is_valid());
            assert_eq!(extract_int_from_key(&c.current().unwrap().unwrap().0), target);
            let dec = rand::thread_rng().gen_range(1..=5);
            let real = dec.min(idx);
            for _ in 0..real { c.decrease().unwrap(); }
            let after_dec = idx - real;
            if after_dec < expected.len() {
                assert!(c.is_valid());
                let (k,r)=c.current().unwrap().unwrap();
                assert_eq!((extract_int_from_key(&k),extract_int_from_row(&r)),
                           expected[after_dec]);
            }
            let mut cur = after_dec;
            let adv = 10;
            let real_adv = adv.min(expected.len() - 1 - cur);
            for _ in 0..real_adv { c.advance().unwrap(); }
            cur += real_adv;
            if cur < expected.len() {
                let (k,r)=c.current().unwrap().unwrap();
                assert_eq!((extract_int_from_key(&k),extract_int_from_row(&r)),
                           expected[cur]);
            } else {
                c.advance().unwrap();
                assert!(!c.is_valid());
            }
        }
    }
}