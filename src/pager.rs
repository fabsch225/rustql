//will end up probably using  unsafe rust
//for now, just use vectors

use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;
use crate::status::Status;
use crate::status::Status::{InternalExceptionIndexOutOfRange, InternalSuccess};
use crate::table::TableSchema;

#[derive(PartialEq)]
pub enum Type {
    String,
    Integer,
    //Double, future feature
    Date,
    Boolean,
    Null,
    //Blob    future feature
}

const STRING_SIZE: usize = 255;
const INTEGER_SIZE: usize = 16;
const DATE_SIZE: usize = 32;
const BOOLEAN_SIZE: usize = 1;
const NULL_SIZE: usize = 1;
const TYPE_SIZE: usize = 4; //cant have more than 15 datatypes :)

impl Type {
    fn from_bits(bits: &[bool]) -> Option<Self> {
        match bits {
            [false, false, false, false] => Some(Type::String),
            [false, false, false, true] => Some(Type::Integer),
            [false, false, true, false] => Some(Type::Date),
            [false, false, true, true] => Some(Type::Boolean),
            [false, true, false, false] => Some(Type::Null),
            _ => None,
        }
    }

    fn get_size(bits: &[bool]) -> Option<usize> {
        match bits {
            [false, false, false, false] => Some(STRING_SIZE),
            [false, false, false, true] => Some(INTEGER_SIZE),
            [false, false, true, false] => Some(DATE_SIZE),
            [false, false, true, true] => Some(BOOLEAN_SIZE),
            [false, true, false, false] => Some(NULL_SIZE),
            _ => None,
        }
    }

    fn get_size_of_type(ty: &Type) -> Option<usize> {
        match ty {
            Type::String => Some(STRING_SIZE),
            Type::Integer => Some(INTEGER_SIZE),
            Type::Date => Some(DATE_SIZE),
            Type::Boolean => Some(BOOLEAN_SIZE),
            Type::Null => Some(NULL_SIZE)
        }
    }
}

//a page is a node of the b tree
//a page contains a fixed maximum of rows/nodes (T)
//each row is part of the same table, the schema is stored elsewhere

type Page = Vec<bool>;

//first TYPE_SIZE bits specify the data type
//rest must be the length of the size of the type

type Key = Vec<bool>;

struct PageReader {}

impl PageReader {
    pub fn get_key(index: usize, page: &Page, schema: TableSchema) -> (Status, Option<Key>){
        //first 8 bits is the number of page is the number of keys / rows
        let size: usize = Serializer::bits_to_int(&page[..8]) as usize;
        if index > size {
            return (InternalExceptionIndexOutOfRange, None);
        }
        //next 8 bits is a flag
        //next bits are the keys
        let (status, id_type) = schema.get_id_type();
        if status != InternalSuccess {
            return (status, None);
        }
        let key_size = Type::get_size_of_type(id_type.unwrap()).unwrap();
        //let keys_length = key_size * size;
        let index_sized = 16 + key_size * index;

        (InternalSuccess, Some(page[index_sized..(index_sized + key_size)].to_owned()))
        //for i in (16..keys_length).step_by(size) {}
    }
    pub fn get_child() {

    }
}


/*
pub struct Pager {
    pub cache: HashMap<Key, Page>,
}

impl Pager {
    pub fn get(&self, key: &Key) -> Option<&Page> {
        self.cache.get(key)
    }
}
*/


pub struct Serializer {

}

impl Serializer {
    //TODO Error handling
    pub fn compare(a: &Key, b: &Key) -> std::cmp::Ordering {
        let type_a_bits = &a[..TYPE_SIZE];
        let type_b_bits = &b[..TYPE_SIZE];

        let type_a = Type::from_bits(type_a_bits).expect("Invalid type bits in key A");
        let type_b = Type::from_bits(type_b_bits).expect("Invalid type bits in key B");

        if type_a != type_b {
            return std::cmp::Ordering::Equal;
        }

        match type_a {
            Type::String => Self::compare_strings(&a[TYPE_SIZE..], &b[TYPE_SIZE..]),
            Type::Integer => Self::compare_integers(&a[TYPE_SIZE..], &b[TYPE_SIZE..]),
            Type::Date => Self::compare_dates(&a[TYPE_SIZE..], &b[TYPE_SIZE..]),
            Type::Boolean => Self::compare_booleans(&a[TYPE_SIZE..], &b[TYPE_SIZE..]),
            Type::Null => std::cmp::Ordering::Equal,
        }
    }

    fn compare_strings(a: &[bool], b: &[bool]) -> std::cmp::Ordering {
        let str_a = Self::bits_to_ascii(a);
        let str_b = Self::bits_to_ascii(b);
        str_a.cmp(&str_b)
    }

    fn compare_integers(a: &[bool], b: &[bool]) -> std::cmp::Ordering {
        let int_a = Self::bits_to_int(a);
        let int_b = Self::bits_to_int(b);
        int_a.cmp(&int_b)
    }

    fn compare_dates(a: &[bool], b: &[bool]) -> std::cmp::Ordering {
        let date_a = Self::bits_to_date(a);
        let date_b = Self::bits_to_date(b);
        date_a.cmp(&date_b)
    }

    fn compare_booleans(a: &[bool], b: &[bool]) -> std::cmp::Ordering {
        let bool_a = Self::bits_to_bool(a);
        let bool_b = Self::bits_to_bool(b);
        bool_a.cmp(&bool_b)
    }

    fn bits_to_ascii(bits: &[bool]) -> String {
        bits.chunks(8)
            .map(|byte| {
                let mut value = 0u8;
                for (i, bit) in byte.iter().enumerate() {
                    if *bit {
                        value |= 1 << (7 - i);
                    }
                }
                value as char
            })
            .take_while(|&c| c != '\0') // Stop at null terminator
            .collect()
    }

    fn bits_to_int(bits: &[bool]) -> i32 {
        let mut value = 0i32;
        for bit in bits {
            value = (value << 1) | (*bit as i32);
        }
        value
    }

    fn bits_to_date(bits: &[bool]) -> (i32, i32, i32) {
        let year = Self::bits_to_int(&bits[0..16]); // First 16 bits for year
        let month = Self::bits_to_int(&bits[16..20]); // Next 4 bits for month
        let day = Self::bits_to_int(&bits[20..24]); // Next 4 bits for day
        (year, month, day)
    }

    fn bits_to_bool(bits: &[bool]) -> bool {
        bits.get(0).copied().unwrap_or(false)
    }
}
