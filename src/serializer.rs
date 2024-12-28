//also look at pager.rs for comments

use crate::btree::BTreeNode;
use crate::pager::{Field, Flag, Key, PageContainer, PageData, PagerAccessor, Position, Row, TableSchema, Type, BOOLEAN_SIZE, DATE_SIZE, INTEGER_SIZE, NULL_SIZE, POSITION_SIZE, ROW_NAME_SIZE, STRING_SIZE, TYPE_SIZE};
use crate::status::Status;
use crate::status::Status::{InternalExceptionIndexOutOfRange, InternalExceptionInvalidColCount, InternalExceptionInvalidRowLength, InternalExceptionInvalidSchema, InternalExceptionKeyNotFound, InternalSuccess, Success};

pub struct Serializer {}

impl Serializer {
    fn get_size_of_type(ty: &Type) -> Option<usize> {
        match ty {
            Type::String => Some(STRING_SIZE),
            Type::Integer => Some(INTEGER_SIZE),
            Type::Date => Some(DATE_SIZE),
            Type::Boolean => Some(BOOLEAN_SIZE),
            Type::Null => Some(NULL_SIZE),
            _ => None,
        }
    }
    //TODO Error Handling
    //TODO think more about architecture -- where to move this for example!?
    pub fn create_table_schema_from_bytes(
        bytes: &[u8],
        row_count: usize,
    ) -> Result<TableSchema, Status> {
        let mut fields = Vec::new();
        let mut offset = 0;
        let mut length = 0;
        let mut key_length = 0;
        while offset < bytes.len() {
            if offset + 1 > bytes.len() {
                return Err(Status::InternalExceptionInvalidSchema);
            }
            let field_type = bytes[offset];
            offset += 1;
            if offset + 16 > bytes.len() {
                return Err(Status::InternalExceptionInvalidSchema);
            }
            let name_bytes = &bytes[offset..offset + ROW_NAME_SIZE];
            offset += ROW_NAME_SIZE;
            let name =
                String::from_utf8(name_bytes.iter().copied().take_while(|&b| b != 0).collect())
                    .map_err(|_| Status::InternalExceptionInvalidSchema)?;
            let field_type =
                Serializer::parse_type(field_type).ok_or(Status::InternalExceptionInvalidSchema)?;
            let field_length = Self::get_size_of_type(&field_type).unwrap();
            if key_length == 0 {
                key_length = field_length;
            }
            length += field_length;
            fields.push(Field { field_type, name });
        }

        Ok(TableSchema {
            col_count: row_count,
            row_length: length,
            key_length,
            key_type: fields[0].field_type.clone(),
            data_length: length - key_length,
            fields,
        })
    }

    pub fn init_page_data(keys: Vec<Key>, data: Vec<Row>) -> PageData {
        let len = keys.len();
        let children: Vec<Position> = vec![0; len + 1];
        Serializer::init_page_data_with_children(keys, children, data)
    }

    //TODO Error handling
    pub fn init_page_data_with_children(keys: Vec<Key>, children: Vec<Position>, data: Vec<Row>) -> PageData {
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

    pub fn is_leaf(data: &PageData) -> Result<bool, Status> {
        Ok(Self::byte_to_bool_at_position(data[1], 1))
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
            return Err(InternalExceptionIndexOutOfRange);
        }
        //TODO think about approaches to some more Serializer methods like this
        Ok(Serializer::bytes_to_position(
            <&[u8; POSITION_SIZE]>::try_from(&page[start_pos..end_pos]).unwrap(),
        ))
    }
    pub fn write_key(index: usize, key: &Key, page: &mut PageData, schema: &TableSchema) -> Status {
        let num_keys = page[0] as usize;
        if index >= num_keys {
            return InternalExceptionIndexOutOfRange;
        }
        let key_length = schema.key_length;
        let list_start_pos = 2; // Start position of keys in the page
        let start_pos = list_start_pos + index * key_length;
        let end_pos = start_pos + key_length;
        page[start_pos..end_pos].copy_from_slice(key);
        InternalSuccess
    }
    pub fn write_child(
        index: usize,
        child: Position,
        page: &mut PageData,
        schema: &TableSchema,
    ) -> Status {
        let num_keys = page[0] as usize;
        let num_children = num_keys + 1;
        if index >= num_children {
            return InternalExceptionIndexOutOfRange;
        }
        let key_length = schema.key_length;
        let list_start_pos = 2 + (num_keys * key_length);
        let start_pos = list_start_pos + index * POSITION_SIZE;
        let end_pos = start_pos + POSITION_SIZE;
        let child_bytes = Serializer::position_to_bytes(child);
        page[start_pos..end_pos].copy_from_slice(&child_bytes);
        InternalSuccess
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
            if child_position == 0 {
                break; //0 means, none. we can break because of btree structure
            }
            result.push(child_position);
        }
        println!("{:?}", result);
        Ok(result)
    }
    pub fn write_keys_vec(keys: &Vec<Key>, page: &mut PageData, schema: &TableSchema) -> Status {
        let num_keys = page[0] as usize;
        let key_length = schema.key_length;
        let list_start_pos = 2;
        for i in 0..num_keys {
            let start_pos = list_start_pos + i * key_length;
            let end_pos = start_pos + key_length;
            page.splice(start_pos..end_pos, keys[i].to_vec());
        }
        InternalSuccess
    }
    pub fn write_children_vec(
        children: &Vec<Position>,
        page: &mut PageData,
        schema: &TableSchema,
    ) -> Status {
        let num_keys = page[0] as usize;
        let key_length = schema.key_length;
        let list_start_pos = 2 + (num_keys * key_length);
        for (i, child) in children.iter().enumerate() {
            let start_pos = list_start_pos + i * POSITION_SIZE;
            let end_pos = start_pos + POSITION_SIZE;
            page.splice(start_pos..end_pos, Serializer::position_to_bytes(*child).to_vec());
        }
        InternalSuccess
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
        let data_length = schema.data_length;
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
        let data_length = schema.data_length;
        let start = data_start + index * data_length;
        let end = start + data_length;

        if row.len() != data_length {
            return Err(InternalExceptionInvalidRowLength);
        }

        if start >= page.len() || end > page.len() {
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
        let data_length = schema.data_length;

        println!("{:?}", page);

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
        rows: Vec<Row>,
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
        let data_length = schema.data_length;

        for (index, row) in rows.iter().enumerate() {
            if row.len() != data_length {
                return Err(InternalExceptionInvalidRowLength);
            }
            let start = data_start + index * data_length;
            let end = start + data_length;
            if start >= page.len() || end > page.len() {
                return Err(InternalExceptionIndexOutOfRange);
            }
            page[start..end].copy_from_slice(row);
        }
        Ok(())
    }

    pub fn get_data(page: &PageContainer, index: usize, schema: TableSchema) -> Vec<u8> {
        let num_keys = page.data[0] as usize;
        let header_length = num_keys * schema.key_length + (num_keys + 1) * POSITION_SIZE;
        let offset = header_length + index * schema.data_length;

        page.data[offset..offset + schema.data_length].to_vec()
    }

    pub fn verify_schema(schema: &TableSchema) -> Result<(), Status> {
        let computed_data_length: usize = schema.fields.iter()
            .map(|field| Self::get_size_of_type(&field.field_type).unwrap_or(0))
            .sum::<usize>() - schema.key_length;

        if computed_data_length != schema.data_length {
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

        let computed_row_length: usize = schema.fields.iter()
            .map(|field| Self::get_size_of_type(&field.field_type).unwrap_or(0))
            .sum();

        if computed_row_length != schema.row_length {
            return Err(InternalExceptionInvalidSchema);
        }
        if schema.fields.len() != schema.col_count {
            return Err(InternalExceptionInvalidSchema);
        }
        Ok(())
    }

    //TODO Error handling
    pub fn compare(a: &Key, b: &Key) -> Result<std::cmp::Ordering, Status> {
        let type_a_byte = a[0];
        let type_b_byte = b[0];

        let type_a = Serializer::parse_type(type_a_byte);
        let type_b = Serializer::parse_type(type_b_byte);

        if !type_a.is_some() || !type_b.is_some() {
            return Err(Status::InternalExceptionInvalidSchema);
        }

        if type_a != type_b {
            return Err(Status::InternalExceptionTypeMismatch);
        }
        let final_type = type_a.unwrap();
        let size = Serializer::get_size_of_type(&final_type).unwrap();
        let end_position = TYPE_SIZE + size;

        match final_type {
            Type::String => Ok(Self::compare_strings(
                <&[u8; 256]>::try_from(&a[TYPE_SIZE..end_position]).unwrap(),
                <&[u8; 256]>::try_from(&b[TYPE_SIZE..end_position]).unwrap(),
            )),
            Type::Integer => Ok(Self::compare_integers(
                <&[u8; 4]>::try_from(&a[TYPE_SIZE..end_position]).unwrap(),
                <&[u8; 4]>::try_from(&b[TYPE_SIZE..end_position]).unwrap(),
            )),
            Type::Date => Ok(Self::compare_dates(
                <&[u8; 3]>::try_from(&a[TYPE_SIZE..end_position]).unwrap(),
                <&[u8; 3]>::try_from(&b[TYPE_SIZE..end_position]).unwrap(),
            )),
            Type::Boolean => Ok(Self::compare_booleans(a[1], b[1])),
            Type::Null => Ok(std::cmp::Ordering::Equal),
        }
    }

    pub fn create_flag(is_leaf: bool) -> Flag {
        let bits = [false, is_leaf, true, true, true, true, true, true];
        Serializer::bits_to_bytes(&bits)[0]
    }

    pub fn compare_strings(a: &[u8; STRING_SIZE], b: &[u8; STRING_SIZE]) -> std::cmp::Ordering {
        let str_a = Self::bytes_to_ascii(a);
        let str_b = Self::bytes_to_ascii(b);
        str_a.cmp(&str_b)
    }

    pub fn compare_integers(a: &[u8; INTEGER_SIZE], b: &[u8; INTEGER_SIZE]) -> std::cmp::Ordering {
        let int_a = Self::bytes_to_int(a);
        let int_b = Self::bytes_to_int(b);
        int_a.cmp(&int_b)
    }

    pub fn compare_dates(a: &[u8; DATE_SIZE], b: &[u8; DATE_SIZE]) -> std::cmp::Ordering {
        let date_a = Self::bytes_to_date(a);
        let date_b = Self::bytes_to_date(b);
        date_a.cmp(&date_b)
    }

    pub fn compare_booleans(a: u8, b: u8) -> std::cmp::Ordering {
        let bool_a = Self::byte_to_bool(a);
        let bool_b = Self::byte_to_bool(b);
        bool_a.cmp(&bool_b)
    }

    pub fn bytes_to_ascii(bytes: &[u8; STRING_SIZE]) -> String {
        bytes
            .iter()
            .map(|&byte| byte as char)
            .take_while(|&c| c != '\0')
            .collect()
    }

    pub fn ascii_to_bytes(ascii: &str) -> [u8; STRING_SIZE] {
        let mut bytes = [0u8; STRING_SIZE];
        for (i, &c) in ascii.as_bytes().iter().take(STRING_SIZE).enumerate() {
            bytes[i] = c;
        }
        bytes
    }

    pub fn bytes_to_position(bytes: &[u8; POSITION_SIZE]) -> Position {
        let mut value = 0i32;
        for &byte in bytes {
            value = (value << 8) | (byte as i32);
        }
        value
    }

    pub fn position_to_bytes(position: Position) -> [u8; POSITION_SIZE] {
        let mut bytes = [0u8; POSITION_SIZE];
        for i in 0..POSITION_SIZE {
            bytes[POSITION_SIZE - 1 - i] = ((position >> (i * 8)) & 0xFF) as u8;
        }
        bytes
    }

    pub fn bytes_to_int(bytes: &[u8; INTEGER_SIZE]) -> i32 {
        let mut value = 0i32;
        for &byte in bytes {
            value = (value << 8) | (byte as i32);
        }
        value
    }

    pub fn int_to_bytes(value: i32) -> [u8; INTEGER_SIZE] {
        let mut bytes = [0u8; INTEGER_SIZE];
        for i in 0..INTEGER_SIZE {
            bytes[INTEGER_SIZE - 1 - i] = ((value >> (i * 8)) & 0xFF) as u8;
        }
        bytes
    }

    pub fn bytes_to_int_variable_length(bytes: &[u8]) -> i32 {
        let mut value = 0i32;
        for &byte in bytes {
            value = (value << 8) | (byte as i32);
        }
        value
    }

    pub fn bytes_to_date(bytes: &[u8; DATE_SIZE]) -> (i32, i32, i32) {
        let year = ((bytes[0] as i32) << 8) | (bytes[1] as i32);

        let month = ((bytes[2] >> 4) & 0b1111) as i32;
        let day = (bytes[2] & 0b1111) as i32;

        (year, month, day)
    }

    pub fn date_to_bytes(year: i32, month: i32, day: i32) -> [u8; DATE_SIZE] {
        let mut bytes = [0u8; DATE_SIZE];
        bytes[0] = (year >> 8) as u8;
        bytes[1] = (year & 0xFF) as u8;
        bytes[2] = ((month << 4) as u8) | (day as u8);
        bytes
    }

    pub fn byte_to_bool(byte: u8) -> bool {
        byte & 1 != 0
    }
    pub fn byte_to_bool_at_position(byte: u8, pos: u8) -> bool {
        (byte & (1 << pos)) != 0
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

    pub fn parse_type(word: u8) -> Option<Type> {
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