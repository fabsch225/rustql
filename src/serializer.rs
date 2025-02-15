//also look at pager.rs for comments

use crate::executor::TableSchema;
use crate::pager::{
    Flag, Key, PageData, Position, Row, Type, BOOLEAN_SIZE, DATE_SIZE, INTEGER_SIZE,
    INTEGER_SIZE_WITHOUT_FLAG, NULL_SIZE, POSITION_SIZE, ROW_NAME_SIZE, STRING_SIZE,
    TABLE_NAME_SIZE, TYPE_SIZE,
};
use crate::status::Status;
use crate::status::Status::{
    InternalExceptionIndexOutOfRange, InternalExceptionInvalidColCount,
    InternalExceptionInvalidRowLength, InternalExceptionInvalidSchema,
    InternalExceptionKeyNotFound, InternalExceptionTypeMismatch, InternalSuccess, Success,
};
use std::fs::File;
use std::io::Read;

/// # Responsibilities
/// - Execute basic operations on the pages
/// - Convert RustQl Datatypes to Strings / Rust-Datatypes
/// - Manage flags with Getters / Setters

pub struct Serializer {}

impl Serializer {
    pub(crate) fn get_size_of_type(ty: &Type) -> Result<usize, Status> {
        match ty {
            Type::String => Ok(STRING_SIZE),
            Type::Integer => Ok(INTEGER_SIZE),
            Type::Date => Ok(DATE_SIZE),
            Type::Boolean => Ok(BOOLEAN_SIZE),
            Type::Null => Ok(NULL_SIZE),
        }
    }

    pub fn init_page_data(keys: Vec<Key>, data: Vec<Row>) -> PageData {
        let len = keys.len();
        let children: Vec<Position> = vec![Position::make_empty(); len + 1];
        Serializer::init_page_data_with_children(keys, children, data)
    }

    //TODO Error handling
    pub fn init_page_data_with_children(
        keys: Vec<Key>,
        children: Vec<Position>,
        mut data: Vec<Row>,
    ) -> PageData {
        let mut result = PageData::new();
        let len = keys.len();
        result.push(len as u8);
        result.push(Serializer::create_flag(true));

        for key in keys {
            result.extend(key);
        }

        for child in children {
            result.extend(Serializer::position_to_bytes(child));
        }

        for row in data {
            result.extend(row);
        }

        result
    }

    pub fn is_dirty(data: &PageData) -> Result<bool, Status> {
        Ok(Self::byte_to_bool_at_position(data[1], 0))
    }
    pub fn set_is_dirty(data: &mut PageData, new_value: bool) -> Result<(), Status> {
        Self::write_byte_at_position(&mut data[1], 0, new_value);
        Ok(())
    }
    pub fn is_leaf(data: &PageData) -> Result<bool, Status> {
        Ok(Self::byte_to_bool_at_position(data[1], 1))
    }
    pub fn set_is_leaf(data: &mut PageData, new_value: bool) -> Result<(), Status> {
        Self::write_byte_at_position(&mut data[1], 1, new_value);
        Ok(())
    }
    pub fn is_deleted(data: &PageData) -> Result<bool, Status> {
        Ok(Self::byte_to_bool_at_position(data[1], 2))
    }
    pub fn set_is_deleted(data: &mut PageData, new_value: bool) -> Result<(), Status> {
        Self::write_byte_at_position(&mut data[1], 2, new_value);
        Ok(())
    }
    pub fn is_tomb(key: &Key, schema: &TableSchema) -> Result<bool, Status> {
        Self::get_flag_at_position(key, 0, &schema.key_type)
    }
    pub fn set_is_tomb(key: &mut Key, value: bool, schema: &TableSchema) -> Result<(), Status> {
        Self::set_flag_at_position(key, 0, value, &schema.key_type)
    }
    pub fn is_null(field: &Vec<u8>, field_type: &Type) -> Result<bool, Status> {
        Self::get_flag_at_position(field, 0, field_type)
    }
    pub fn set_is_null(field: &mut Vec<u8>, value: bool, field_type: &Type) -> Result<(), Status> {
        Self::set_flag_at_position(field, 0, value, field_type)
    }

    //the expansion methods also expand the rows and children of course.
    pub fn expand_keys_by(
        expand_size: usize,
        page: &mut PageData,
        schema: &TableSchema,
    ) -> Result<usize, Status> {
        let original_size = page[0] as usize; //old number of keys
        let original_num_children = original_size;
        let key_length = schema.key_length;
        let keys_offset = 2 + key_length * original_size;
        let new_keys_offset = expand_size * key_length;
        let new_children_start = new_keys_offset + original_num_children * POSITION_SIZE;
        let new_children_offset = expand_size * POSITION_SIZE;
        page.splice(keys_offset..keys_offset, vec![0; new_keys_offset]);
        page.splice(
            new_children_start..new_children_start,
            vec![0; new_children_offset],
        );
        page[0] = (original_size + expand_size) as u8;

        Ok(keys_offset)
    }

    pub fn expand_keys_with_vec(expansion: &Vec<Key>, page: &mut PageData, schema: &TableSchema) {
        let initial_offset =
            Self::expand_keys_by(expansion.len(), page, schema).expect("handle this! #x3qnnx");
        let mut current_offset = initial_offset;
        for data in expansion {
            page.splice(current_offset..current_offset, data.iter().cloned());
            current_offset += data.len();
        }
    }

    pub fn read_key(index: usize, page: &PageData, schema: &TableSchema) -> Result<Key, Status> {
        let size: usize = page[0] as usize;
        if index > size {
            panic!("why");
            return Err(InternalExceptionIndexOutOfRange);
        }
        let key_length = schema.key_length;
        let index_sized = 2 + key_length * index;
        Ok(page[index_sized..(index_sized + key_length)].to_owned())
    }

    pub fn read_child(
        index: usize,
        page: &PageData,
        schema: &TableSchema,
    ) -> Result<Position, Status> {
        let num_children = page[0] as usize + 1;
        if index > num_children {
            return Err(InternalExceptionIndexOutOfRange);
        }
        let key_length = schema.key_length;
        let start_pos = 2 + (num_children - 1) * key_length + index * POSITION_SIZE;
        let end_pos = start_pos + POSITION_SIZE;
        if end_pos > page.len() {
            panic!("why");
            return Err(InternalExceptionIndexOutOfRange);
        }
        //TODO think about approaches to some more Serializer methods like this
        Ok(Serializer::bytes_to_position(
            <&[u8; POSITION_SIZE]>::try_from(&page[start_pos..end_pos]).unwrap(),
        ))
    }

    pub fn write_key(
        index: usize,
        key: &Key,
        page: &mut PageData,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        let num_keys = page[0] as usize;
        if index >= num_keys {
            panic!("why");
            return Err(InternalExceptionIndexOutOfRange);
        }
        let key_length = schema.key_length;
        let list_start_pos = 2; // Start position of keys in the page
        let start_pos = list_start_pos + index * key_length;
        let end_pos = start_pos + key_length;
        page[start_pos..end_pos].copy_from_slice(key);
        Ok(())
    }

    pub fn write_child(
        index: usize,
        child: Position,
        page: &mut PageData,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        let num_keys = page[0] as usize;
        let num_children = num_keys + 1;
        if index >= num_children {
            panic!("why");
            return Err(InternalExceptionIndexOutOfRange);
        }
        let key_length = schema.key_length;
        let list_start_pos = 2 + (num_keys * key_length);
        let start_pos = list_start_pos + index * POSITION_SIZE;
        let end_pos = start_pos + POSITION_SIZE;
        let child_bytes = Serializer::position_to_bytes(child.clone());
        page[start_pos..end_pos].copy_from_slice(&child_bytes);
        if child.is_empty() {
            Self::set_is_leaf(page, false)?;
        }
        Ok(())
    }

    pub fn read_keys_as_vec(page: &PageData, schema: &TableSchema) -> Result<Vec<Key>, Status> {
        let mut result: Vec<Key> = Vec::new();
        let num_keys = page[0] as usize;
        let key_length = schema.key_length;
        let list_start_pos = 2; //TODO: Parameterize this somehow
        for i in 0..num_keys {
            let start_pos = list_start_pos + i * key_length;
            let end_pos = start_pos + key_length;
            result.push(page[start_pos..end_pos].to_owned());
        }
        Ok(result)
    }

    pub fn read_children_as_vec(
        page: &PageData,
        schema: &TableSchema,
    ) -> Result<Vec<Position>, Status> {
        let mut result: Vec<Position> = Vec::new();
        let num_keys = page[0] as usize;
        let key_length = schema.key_length;
        let list_start_pos = 2 + (num_keys * key_length);
        for i in 0..(num_keys + 1) {
            let start_pos = list_start_pos + i * POSITION_SIZE;
            let end_pos = start_pos + POSITION_SIZE;
            let child_position = Serializer::bytes_to_position(
                <&[u8; POSITION_SIZE]>::try_from(&page[start_pos..end_pos]).unwrap(),
            );
            if child_position.is_empty() {
                break; //0 means, none. we can break because of btree structure
            }
            result.push(child_position);
        }
        Ok(result)
    }

    ///will adjust number of keys, delete children if necessary
    /// - the original data will be intact, but empty rows will be padded.
    pub fn write_keys_vec(
        keys: &Vec<Key>,
        page: &mut PageData,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        let num_keys = page[0] as usize;
        if num_keys != keys.len() {
            return Self::write_keys_vec_resize(keys, page, schema);
        }
        Self::set_is_leaf(page, false)?;
        let key_length = schema.key_length;
        let list_start_pos = 2;
        for i in 0..num_keys {
            let start_pos = list_start_pos + i * key_length;
            let end_pos = start_pos + key_length;
            page.splice(start_pos..end_pos, keys[i].to_vec());
        }
        Ok(())
    }

    pub fn write_keys_vec_resize_with_rows(
        keys: &Vec<Key>,
        rows: &Vec<Row>,
        page: &mut PageData,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        if keys.len() != rows.len() {
            panic!("keys and rows must have same len")
        }
        Self::write_keys_vec_resize(keys, page, schema)?;
        Self::write_data_by_vec(page, rows, schema)
    }

    pub fn write_keys_vec_resize(
        keys: &Vec<Key>,
        page: &mut PageData,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        let orig_num_keys = page[0] as usize;
        let new_num_keys = keys.len();
        //assert!(new_num_keys != 0); this is allowed, if we change the root afterwards
        let key_length = schema.key_length;
        let data_length = schema.row_length;
        let increase = new_num_keys > orig_num_keys;

        let keys_start = 2;
        let orig_keys_end = keys_start + orig_num_keys * key_length;
        let new_keys_end = keys_start + new_num_keys * key_length;
        if increase {
            page.splice(
                orig_keys_end..orig_keys_end,
                vec![0; (new_num_keys - orig_num_keys) * key_length],
            );
        } else {
            page.drain(new_keys_end..orig_keys_end);
        }

        for (i, key) in keys.iter().enumerate() {
            let start_pos = keys_start + i * key_length;
            let end_pos = start_pos + key_length;
            page[start_pos..end_pos].copy_from_slice(key);
        }

        let children_start = new_keys_end;
        let orig_children_end = children_start + (orig_num_keys + 1) * POSITION_SIZE;
        let new_children_end = children_start + (new_num_keys + 1) * POSITION_SIZE;
        if increase {
            page.splice(
                orig_children_end..orig_children_end,
                vec![0; (new_num_keys - orig_num_keys) * POSITION_SIZE],
            );
        } else {
            page.drain(new_children_end..orig_children_end);
        }

        let data_start = new_children_end;
        let orig_data_end = data_start + orig_num_keys * data_length;
        let new_data_end = data_start + new_num_keys * data_length;
        if increase {
            page.splice(
                orig_data_end..orig_data_end,
                vec![0; (new_num_keys - orig_num_keys) * data_length],
            );
        } else {
            page.drain(new_data_end..orig_data_end);
        }

        page[0] = new_num_keys as u8;

        Ok(())
    }

    ///will panic if wrong length
    pub fn write_children_vec(
        children: &Vec<Position>,
        page: &mut PageData,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        let num_keys = page[0] as usize;
        let key_length = schema.key_length;
        let list_start_pos = 2 + (num_keys * key_length);
        let mut check_for_leaf = true;
        for (i, child) in children.iter().enumerate() {
            if i > num_keys + 1 {
                panic!("cannot extend children without extending keys first: we have at least {} children, but {} keys", i, num_keys)
            }
            let start_pos = list_start_pos + i * POSITION_SIZE;
            let end_pos = start_pos + POSITION_SIZE;
            page.splice(
                start_pos..end_pos,
                Serializer::position_to_bytes(child.clone()).to_vec(),
            );
            if child.is_empty() && check_for_leaf {
                check_for_leaf = false;
                Self::set_is_leaf(page, false)?;
            }
        }
        Ok(())
    }

    pub fn read_data_by_key(
        page: &PageData,
        key: Key,
        schema: &TableSchema,
    ) -> Result<Row, Status> {
        let keys = Self::read_keys_as_vec(&page, schema)?;
        let index = keys.iter().position(|k| k == &key);
        if let Some(index) = index {
            Self::read_data_by_index(page, index, schema)
        } else {
            Err(InternalExceptionKeyNotFound)
        }
    }

    pub fn read_data_by_index(
        page: &PageData,
        index: usize,
        schema: &TableSchema,
    ) -> Result<Row, Status> {
        let num_keys = page[0] as usize;
        let key_length = schema.key_length;
        let keys_start = 2;
        let children_start = keys_start + num_keys * key_length;
        let data_start = children_start + (num_keys + 1) * POSITION_SIZE;
        let data_length = schema.row_length;
        let start = data_start + index * data_length;
        let end = start + data_length;
        Ok(page[start..end].to_vec())
    }

    pub fn write_data_by_key(
        page: &mut PageData,
        key: Key,
        row: Row,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        let keys = Self::read_keys_as_vec(&page, schema)?;
        let index = keys.iter().position(|k| k == &key);
        if let Some(index) = index {
            Self::write_data_by_index(page, index, row, schema)
        } else {
            Err(InternalExceptionKeyNotFound)
        }
    }

    pub fn write_data_by_index(
        page: &mut PageData,
        index: usize,
        row: Row,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        let num_keys = page[0] as usize;
        let key_length = schema.key_length;
        let keys_start = 2;
        let children_start = keys_start + num_keys * key_length;
        let data_start = children_start + (num_keys + 1) * POSITION_SIZE;
        let data_length = schema.row_length;
        let start = data_start + index * data_length;
        let end = start + data_length;

        if row.len() != data_length {
            return Err(InternalExceptionInvalidRowLength);
        }

        if end > page.len() {
            panic!("why");
            return Err(InternalExceptionIndexOutOfRange);
        }

        page[start..end].copy_from_slice(&row);
        Ok(())
    }

    pub fn read_data_by_vec(page: &PageData, schema: &TableSchema) -> Result<Vec<Row>, Status> {
        let num_keys = page[0] as usize;
        let key_length = schema.key_length;
        let keys_start = 2;
        let children_start = keys_start + num_keys * key_length;
        let data_start = children_start + (num_keys + 1) * POSITION_SIZE;
        let data_length = schema.row_length;

        let mut rows = Vec::new();
        for index in 0..num_keys {
            let start = data_start + index * data_length;
            let end = start + data_length;
            if end > page.len() {
                return Err(InternalExceptionIndexOutOfRange);
            }
            rows.push(page[start..end].to_vec());
        }
        Ok(rows)
    }

    pub fn write_data_by_vec(
        page: &mut PageData,
        rows: &Vec<Row>,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        let num_keys = page[0] as usize;
        if rows.len() != num_keys {
            return Err(InternalExceptionInvalidColCount);
        }

        let key_length = schema.key_length;
        let keys_start = 2;
        let children_start = keys_start + num_keys * key_length;
        let data_start = children_start + (num_keys + 1) * POSITION_SIZE;
        let data_length = schema.row_length;
        for (index, row) in rows.iter().enumerate() {
            if row.len() != data_length {
                return Err(InternalExceptionInvalidRowLength);
            }
            let start = data_start + index * data_length;
            let end = start + data_length;
            if end > page.len() {
                return Err(InternalExceptionIndexOutOfRange);
            }
            page[start..end].copy_from_slice(row);
        }
        Ok(())
    }

    //TODO i dont know if this is up-to-date
    #[deprecated]
    pub fn verify_schema(schema: &TableSchema) -> Result<(), Status> {
        let computed_data_length: usize = schema
            .fields
            .iter()
            .map(|field| Self::get_size_of_type(&field.field_type).unwrap_or(0))
            .sum::<usize>()
            - schema.key_length;

        if computed_data_length != schema.row_length {
            return Err(InternalExceptionInvalidSchema);
        }
        if let Some(key_field) = schema.fields.get(0) {
            let key_field_size = Self::get_size_of_type(&key_field.field_type).unwrap_or(0);
            if key_field_size != schema.key_length {
                return Err(InternalExceptionInvalidSchema);
            }
        } else {
            return Err(InternalExceptionInvalidSchema);
        }

        let computed_row_length: usize = schema
            .fields
            .iter()
            .map(|field| Self::get_size_of_type(&field.field_type).unwrap_or(0))
            .sum();

        if computed_row_length != schema.whole_row_length {
            return Err(InternalExceptionInvalidSchema);
        }
        if schema.fields.len() != schema.col_count {
            return Err(InternalExceptionInvalidSchema);
        }
        Ok(())
    }

    //TODO adjust for NULL-flag
    pub fn infinity(field_type: &Type) -> Vec<u8> {
        match field_type {
            Type::String => vec![u8::MAX; STRING_SIZE],
            Type::Integer => vec![0x7F; INTEGER_SIZE], // Max positive value for signed integer
            Type::Date => vec![0xFF; DATE_SIZE],       // Max value for date
            Type::Boolean => vec![1],                  // True as infinity for boolean
            Type::Null => vec![0],                     // Null has no concept of infinity
        }
    }

    pub fn negative_infinity(field_type: &Type) -> Vec<u8> {
        match field_type {
            Type::String => vec![u8::MIN; STRING_SIZE],
            Type::Integer => vec![0x80; INTEGER_SIZE], // Min negative value for signed integer
            Type::Date => vec![0x00; DATE_SIZE],       // Min value for date
            Type::Boolean => vec![0],                  // False as negative infinity for boolean
            Type::Null => vec![0],                     // Null has no concept of negative infinity
        }
    }

    pub fn set_flag_at_position(
        v: &mut Vec<u8>,
        position: u8,
        value: bool,
        field_type: &Type,
    ) -> Result<(), Status> {
        match field_type {
            Type::Null => Err(Status::InternalExceptionInvalidFieldType),
            Type::Boolean => Ok(Self::write_byte_at_position(&mut v[0], position, value)),
            _ => Ok(Self::write_byte_at_position(
                &mut v[Self::get_size_of_type(&field_type)? - 1],
                position,
                value,
            )),
        }
    }

    pub fn get_flag_at_position(
        v: &Vec<u8>,
        position: u8,
        field_type: &Type,
    ) -> Result<bool, Status> {
        match field_type {
            Type::Null => Err(Status::InternalExceptionInvalidFieldType),
            Type::Boolean => Ok(Self::byte_to_bool_at_position(v[0], position)),
            _ => Ok(Self::byte_to_bool_at_position(
                v[Self::get_size_of_type(field_type)? - 1],
                position,
            )),
        }
    }

    pub fn compare_with_type(
        a: &Vec<u8>,
        b: &Vec<u8>,
        field_type: &Type,
    ) -> Result<std::cmp::Ordering, Status> {
        match field_type {
            Type::String => Ok(Self::compare_strings(
                <[u8; STRING_SIZE]>::try_from(a.to_vec()).unwrap(),
                <[u8; STRING_SIZE]>::try_from(b.to_vec()).unwrap(),
            )),
            Type::Integer => Ok(Self::compare_integers(
                <[u8; INTEGER_SIZE]>::try_from(a.to_vec()).unwrap(),
                <[u8; INTEGER_SIZE]>::try_from(b.to_vec()).unwrap(),
            )),
            Type::Date => Ok(Self::compare_dates(
                <[u8; DATE_SIZE]>::try_from(a.to_vec()).unwrap(),
                <[u8; DATE_SIZE]>::try_from(b.to_vec()).unwrap(),
            )),
            Type::Boolean => Ok(Self::compare_booleans(a[1], b[1])),
            Type::Null => Ok(std::cmp::Ordering::Equal),
        }
    }

    pub fn create_flag(is_leaf: bool) -> Flag {
        let bits = [false, is_leaf, true, true, true, true, true, true];
        Serializer::bits_to_bytes(&bits)[0]
    }

    pub fn compare_strings(a: [u8; STRING_SIZE], b: [u8; STRING_SIZE]) -> std::cmp::Ordering {
        let str_a = Self::bytes_to_ascii(a);
        let str_b = Self::bytes_to_ascii(b);
        str_a.cmp(&str_b)
    }

    pub fn compare_integers(a: [u8; INTEGER_SIZE], b: [u8; INTEGER_SIZE]) -> std::cmp::Ordering {
        let int_a = Self::bytes_to_int(a);
        let int_b = Self::bytes_to_int(b);

        int_a.cmp(&int_b)
    }

    pub fn compare_dates(a: [u8; DATE_SIZE], b: [u8; DATE_SIZE]) -> std::cmp::Ordering {
        let date_a = Self::bytes_to_date(a);
        let date_b = Self::bytes_to_date(b);
        date_a.cmp(&date_b)
    }

    pub fn compare_booleans(a: u8, b: u8) -> std::cmp::Ordering {
        let bool_a = Self::byte_to_bool(a);
        let bool_b = Self::byte_to_bool(b);
        bool_a.cmp(&bool_b)
    }

    pub fn format_row(row: &Row, table_schema: &TableSchema) -> Result<String, Status> {
        let mut result: String = "".to_string();
        let mut index = 0;
        let mut skip = true;
        for field in table_schema.fields.clone() {
            if skip {
                skip = false;
                continue;
            } //first field is the key TODO change this
            let field_type = field.field_type;
            let len = Serializer::get_size_of_type(&field_type).unwrap();
            result += &*Serializer::format_field(&row[index..(index + len)].to_vec(), &field_type)?;
            result += "; "
        }
        if result.len() > 2 {
            result.truncate(result.len() - 2);
        }
        Ok(result)
    }

    pub fn format_key(key: &Key, schema: &TableSchema) -> Result<String, Status> {
        Self::format_field(key, &schema.key_type)
    }

    pub fn format_field(bytes: &Vec<u8>, field_type: &Type) -> Result<String, Status> {
        match field_type {
            Type::String => Ok(Self::format_string(
                <[u8; STRING_SIZE]>::try_from(bytes.clone()).expect("wrong len for type String"),
            )),
            Type::Date => Ok(Self::format_date(
                <[u8; DATE_SIZE]>::try_from(bytes.clone()).expect("wrong len for type Date"),
            )),
            Type::Integer => Ok(Self::format_int(
                <[u8; INTEGER_SIZE]>::try_from(bytes.clone()).expect("wrong len for type Integer"),
            )),
            Type::Boolean => Ok(Self::format_bool(&bytes[0])),
            _ => Err(InternalExceptionTypeMismatch),
        }
    }

    pub fn format_string(bytes: [u8; STRING_SIZE]) -> String {
        String::from_utf8(bytes.iter().copied().take_while(|&b| b != 0).collect()).unwrap()
    }

    pub fn format_int(bytes: [u8; INTEGER_SIZE]) -> String {
        let int_value = Self::bytes_to_int(bytes);
        int_value.to_string()
    }

    pub fn format_date(bytes: [u8; DATE_SIZE]) -> String {
        let (year, month, day) = Self::bytes_to_date(bytes);
        format!("{:04}-{:02}-{:02}", year, month, day)
    }

    pub fn format_bool(byte: &u8) -> String {
        if byte & 1 != 0 {
            "true".to_string()
        } else {
            "false".to_string()
        }
    }

    pub fn parse_string(s: &str) -> [u8; STRING_SIZE] {
        let mut bytes = [0u8; STRING_SIZE];
        for (i, &c) in s.as_bytes().iter().take(STRING_SIZE).enumerate() {
            bytes[i] = c;
        }
        bytes
    }

    pub fn parse_int(s: &str) -> Result<[u8; INTEGER_SIZE], Status> {
        let int_value: i32 = s.parse().map_err(|_| Status::CannotParseInteger)?;
        Ok(Self::int_to_bytes(int_value))
    }

    pub fn parse_date(s: &str) -> Result<[u8; DATE_SIZE], Status> {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() < 2 {
            Err(Status::CannotParseDate)?
        }
        let year: i32 = parts[0].parse().map_err(|_| Status::CannotParseDate)?;
        let month: i32 = parts[1].parse().map_err(|_| Status::CannotParseDate)?;
        let day: i32 = parts[2].parse().map_err(|_| Status::CannotParseDate)?;
        Self::date_to_bytes(year, month, day)
    }

    pub fn parse_bool(s: &str) -> Result<u8, Status> {
        if s.to_ascii_lowercase() == "true" {
            Ok(1)
        } else if s.to_ascii_lowercase() == "false" {
            Ok(0)
        } else {
            Err(Status::CannotParseBoolean)
        }
    }

    pub fn bytes_to_ascii(bytes: [u8; STRING_SIZE]) -> String {
        bytes[0..STRING_SIZE - 1]
            .iter()
            .map(|&byte| byte as char)
            .take_while(|&c| c != '\0')
            .collect()
    }

    pub fn ascii_to_bytes(ascii: &str) -> [u8; STRING_SIZE] {
        let mut bytes = [0u8; STRING_SIZE];
        for (i, &c) in ascii.as_bytes().iter().take(STRING_SIZE - 1).enumerate() {
            bytes[i] = c;
        }
        bytes
    }

    pub fn bytes_to_position(bytes: &[u8; POSITION_SIZE]) -> Position {
        //byte 0, 1 -> page
        //byte 2, 3 -> cell
        let page = bytes[0] as u16 + 8 * bytes[1] as u16;
        let cell = bytes[2] as u16 + 8 * bytes[3] as u16;
        Position::new(page, cell)
    }

    pub fn position_to_bytes(position: Position) -> [u8; POSITION_SIZE] {
        let mut bytes = [0u8; POSITION_SIZE];
        bytes[0] = (position.page >> 8) as u8;
        bytes[1] = (position.page & 0xFF) as u8;
        bytes[2] = (position.cell >> 8) as u8;
        bytes[3] = (position.cell & 0xFF) as u8;
        bytes
    }

    pub fn bytes_to_int(bytes: [u8; INTEGER_SIZE]) -> i32 {
        let mut value = 0i32;
        for i in 0..INTEGER_SIZE - 1 {
            value = (value << 8) | (bytes[i] as i32);
        }
        value
    }

    pub fn int_to_bytes(value: i32) -> [u8; INTEGER_SIZE] {
        let mut bytes = [0u8; INTEGER_SIZE];
        for i in 0..INTEGER_SIZE - 1 {
            bytes[INTEGER_SIZE - 2 - i] = ((value >> (i * 8)) & 0xFF) as u8;
        }
        bytes
    }

    pub fn date_to_bytes(year: i32, month: i32, day: i32) -> Result<[u8; DATE_SIZE], Status> {
        if !(month >= 1 && month <= 12 && day >= 1 && day <= 31 && year > 0) {
            Err(Status::CannotParseIllegalDate)?
        };

        let mut bytes = [0u8; DATE_SIZE];
        bytes[0] = (year >> 8) as u8;
        bytes[1] = (year & 0xFF) as u8;
        bytes[2] = month as u8;
        bytes[3] = day as u8;

        Ok(bytes)
    }

    pub fn bytes_to_date(bytes: [u8; DATE_SIZE]) -> (i32, i32, i32) {
        let year = ((bytes[0] as i32) << 8) | (bytes[1] as i32);
        let month = bytes[2] as i32;
        let day = bytes[3] as i32;

        (year, month, day)
    }

    pub fn byte_to_bool(byte: u8) -> bool {
        byte & 1 != 0
    }

    pub fn byte_to_bool_at_position(byte: u8, pos: u8) -> bool {
        (byte & (1 << pos)) != 0
    }

    pub fn write_byte_at_position(byte: &mut u8, pos: u8, value: bool) {
        if value {
            *byte |= 1 << pos;
        } else {
            *byte &= !(1 << pos);
        }
    }

    pub fn bytes_to_bits(bytes: &[u8]) -> Vec<bool> {
        bytes
            .iter()
            .flat_map(|byte| (0..8).rev().map(move |i| (byte & (1 << i)) != 0))
            .collect()
    }

    pub fn bits_to_bytes(bits: &[bool]) -> Vec<u8> {
        bits.chunks(8)
            .map(|chunk| {
                chunk
                    .iter()
                    .enumerate()
                    .fold(0u8, |byte, (i, &bit)| byte | ((bit as u8) << (7 - i)))
            })
            .collect()
    }

    fn type_to_byte(field_type: &Type) -> u8 {
        match field_type {
            Type::Null => 0,
            Type::Integer => 1,
            Type::String => 2,
            Type::Date => 3,
            Type::Boolean => 4,
        }
    }

    pub fn byte_to_type(word: u8) -> Option<Type> {
        match word {
            0 => Some(Type::Null),
            1 => Some(Type::Integer),
            2 => Some(Type::String),
            3 => Some(Type::Date),
            4 => Some(Type::Boolean),
            _ => None,
        }
    }
}
