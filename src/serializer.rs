//also look at pager.rs for comments

use crate::executor::TableSchema;
use crate::pager::{
    FieldMeta, Flag, Key, KeyMeta, NodeFlag, PageContainer, PageData, PageFlag, Position, Row,
    Type, BOOLEAN_SIZE, DATE_SIZE, INTEGER_SIZE, NODE_METADATA_SIZE, NULL_SIZE, PAGE_SIZE,
    POSITION_SIZE, STRING_SIZE,
};
use crate::status::Status;
use crate::status::Status::{
    InternalExceptionIndexOutOfRange, InternalExceptionInvalidColCount,
    InternalExceptionInvalidRowLength, InternalExceptionInvalidSchema,
    InternalExceptionKeyNotFound, InternalExceptionTypeMismatch,
};

/// # Responsibilities
/// - Execute operations on the pages
/// - Convert RustSQl Datatypes to Strings / Rust-Datatypes
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

    pub fn init_page_data(
        keys: Vec<Key>,
        data: Vec<Row>,
        table_schema: &TableSchema,
    ) -> Result<PageData, Status> {
        let len = keys.len();
        let children: Vec<Position> = vec![Position::make_empty(); len + 1];
        Self::init_page_data_with_children(keys, children, data, table_schema)
    }

    pub fn init_page_data_with_children(
        keys: Vec<Key>,
        children: Vec<Position>,
        data: Vec<Row>,
        table_schema: &TableSchema,
    ) -> Result<PageData, Status> {
        let num_keys = keys.len();
        if children.len() > num_keys + 1 {
            panic!("keys and children must be l_c <= l_k + 1");
            return Err(InternalExceptionInvalidColCount);
        }
        if data.len() != num_keys {
            panic!("data and keys must have same length");
            return Err(InternalExceptionInvalidColCount);
        }
        let mut result = [0u8; PAGE_SIZE];

        //let node_size = 1 + num_keys * table_schema.key_length + (num_keys + 1) * POSITION_SIZE + num_keys * table_schema.row_length;
        //let free_space = PAGE_SIZE - node_size;
        result[0] = num_keys as u8;
        Self::write_byte_at_position(&mut result[1], NodeFlag::Leaf as u8, children.is_empty());

        let mut current_pos = NODE_METADATA_SIZE;

        for key in keys {
            if key.len() != table_schema.get_key_length()? {
                return Err(InternalExceptionInvalidRowLength);
            }
            result[current_pos..current_pos + key.len()].copy_from_slice(&key);
            current_pos += key.len();
        }
        for child in children {
            let child_bytes = Serializer::position_to_bytes(child);
            result[current_pos..current_pos + POSITION_SIZE].copy_from_slice(&child_bytes);
            current_pos += POSITION_SIZE;
        }
        for row in data {
            if row.len() != table_schema.get_row_length()? {
                return Err(InternalExceptionInvalidRowLength);
            }
            result[current_pos..current_pos + row.len()].copy_from_slice(&row);
            current_pos += row.len();
        }

        Ok(result)
    }

    pub fn is_dirty(page_container: &PageContainer) -> Result<bool, Status> {
        Ok(Self::byte_to_bool_at_position(page_container.flag, 0))
    }
    pub fn set_is_dirty(page_container: &mut PageContainer, new_value: bool) -> Result<(), Status> {
        Self::write_byte_at_position(&mut page_container.flag, 0, new_value);
        Ok(())
    }
    pub fn is_leaf(
        page_data: &PageData,
        position: &Position,
        table_schema: &TableSchema,
    ) -> Result<bool, Status> {
        let location = Self::find_position_offset(page_data, &position, &table_schema)?;
        Ok(Self::byte_to_bool_at_position(
            page_data[location + 1],
            NodeFlag::Leaf as u8,
        ))
    }
    pub fn set_is_leaf(
        page_data: &mut PageData,
        position: &Position,
        table_schema: &TableSchema,
        new_value: bool,
    ) -> Result<(), Status> {
        let location = Self::find_position_offset(page_data, &position, &table_schema)?;
        Self::write_byte_at_position(
            &mut page_data[location + 1],
            NodeFlag::Leaf as u8,
            new_value,
        );
        Ok(())
    }
    pub fn is_deleted(page_container: &PageContainer) -> Result<bool, Status> {
        Ok(Self::byte_to_bool_at_position(
            page_container.flag,
            PageFlag::Deleted as u8,
        ))
    }
    pub fn set_is_deleted(
        page_container: &mut PageContainer,
        new_value: bool,
    ) -> Result<(), Status> {
        Self::write_byte_at_position(&mut page_container.flag, PageFlag::Deleted as u8, new_value);
        Ok(())
    }
    pub fn is_tomb(key: &Key, schema: &TableSchema) -> Result<bool, Status> {
        Self::get_flag_at_position(key, KeyMeta::Tomb as u8, &schema.get_key_type()?)
    }
    pub fn set_is_tomb(key: &mut Key, value: bool, schema: &TableSchema) -> Result<(), Status> {
        Self::set_flag_at_position(key, KeyMeta::Tomb as u8, value, &schema.get_key_type()?)
    }
    pub fn is_null(field: &Vec<u8>, field_type: &Type) -> Result<bool, Status> {
        Self::get_flag_at_position(field, FieldMeta::Null as u8, field_type)
    }
    pub fn set_is_null(field: &mut Vec<u8>, value: bool, field_type: &Type) -> Result<(), Status> {
        Self::set_flag_at_position(field, FieldMeta::Null as u8, value, field_type)
    }

    pub fn find_position_offset(
        page: &PageData,
        position: &Position,
        schema: &TableSchema,
    ) -> Result<usize, Status> {
        let mut offset = 0; //[num keys not included]

        for _ in 0..position.cell() {
            let num_keys = page[offset] as usize;
            offset += NODE_METADATA_SIZE;
            offset += num_keys * schema.get_key_and_row_length()? + (num_keys + 1) * POSITION_SIZE;
        }

        Ok(offset)
    }

    pub fn copy_node(
        table_schema: &TableSchema,
        position_dest: &Position,
        position_source: &Position,
        page_dest: &mut PageData,
        page_source: &PageData,
    ) -> Result<(), Status> {
        let offset_dest = Self::find_position_offset(page_dest, position_dest, table_schema)?;
        let offset_source = Self::find_position_offset(page_source, position_source, table_schema)?;
        let num_keys = page_source[offset_source] as usize;
        let key_length = table_schema.get_key_length()?;
        let row_length = table_schema.get_row_length()?;
        let full_size = NODE_METADATA_SIZE
            + num_keys * table_schema.get_key_and_row_length()?
            + (num_keys + 1) * POSITION_SIZE;
        page_dest[offset_dest..offset_dest + full_size]
            .copy_from_slice(&page_source[offset_source..offset_source + full_size]);
        Ok(())
    }

    pub fn switch_nodes(
        table_schema: &TableSchema,
        position_a: &Position,
        position_b: &Position,
        page_a: &mut PageData,
        page_b: Option<&mut PageData>,
    ) -> Result<(), Status> {
        //case one: both nodes are on the same page
        if page_b.is_none() {
            assert_eq!(position_a.page(), position_b.page());
            assert_ne!(position_a.cell(), position_b.cell());
            let mut position_a = position_a.clone();
            let mut position_b = position_b.clone();
            if position_a.cell() > position_b.cell() {
                position_a.swap(&mut position_b);
            } //position_a is now the smaller one
            let offset_a = Self::find_position_offset(page_a, &position_a, &table_schema)?;
            let num_keys_a = page_a[offset_a] as usize;
            let offset_b = Self::find_position_offset(page_a, &position_b, &table_schema)?;
            let num_keys_b = page_a[offset_b] as usize;
            let full_size_a = NODE_METADATA_SIZE
                + num_keys_a * table_schema.get_key_and_row_length()?
                + (num_keys_a + 1) * POSITION_SIZE;
            let full_size_b = NODE_METADATA_SIZE
                + num_keys_b * table_schema.get_key_and_row_length()?
                + (num_keys_b + 1) * POSITION_SIZE;

            let mut temp = vec![0u8; full_size_a];
            temp.copy_from_slice(&page_a[offset_a..offset_a + full_size_a]);
            let shift_for_a = full_size_b as isize - full_size_a as isize;
            Self::shift_page(page_a, offset_a + full_size_a, shift_for_a)?;
            page_a.copy_within(offset_b..offset_b + full_size_b, offset_a);
            page_a[offset_b..offset_b + full_size_a].copy_from_slice(&temp);
            Self::shift_page(page_a, offset_b + full_size_a, -shift_for_a)?;
        } else {
            assert_ne!(position_a.page(), position_b.page());
            let page_b = page_b.unwrap();
            let offset_a = Self::find_position_offset(page_a, position_a, &table_schema)?;
            let num_keys_a = page_a[offset_a] as usize;
            let offset_b = Self::find_position_offset(page_b, position_b, &table_schema)?;
            let num_keys_b = page_b[offset_b] as usize;
            let full_size_a = NODE_METADATA_SIZE
                + num_keys_a * table_schema.get_key_and_row_length()?
                + (num_keys_a + 1) * POSITION_SIZE;
            let full_size_b = NODE_METADATA_SIZE
                + num_keys_b * table_schema.get_key_and_row_length()?
                + (num_keys_b + 1) * POSITION_SIZE;

            let mut temp = vec![0u8; full_size_a];
            temp.copy_from_slice(&page_a[offset_a..offset_a + full_size_a]);
            let shift_for_a = full_size_b as isize - full_size_a as isize;
            Self::shift_page(page_a, offset_a + full_size_a, shift_for_a)?;
            page_a[offset_a..offset_a + full_size_b]
                .copy_from_slice(&page_b[offset_b..offset_b + full_size_b]);

            let shift_for_b = full_size_a as isize - full_size_b as isize;
            Self::shift_page(page_b, offset_b + full_size_b, shift_for_b)?;
            page_b[offset_b..offset_b + full_size_a].copy_from_slice(&temp);
        }
        Ok(())
    }

    //TODO not all of these are used in the end

    //the expansion methods also expand the rows and children of course.
    pub fn expand_keys_by(
        expand_size: usize,
        page: &mut PageData,
        position: &Position,
        schema: &TableSchema,
    ) -> Result<usize, Status> {
        assert!(expand_size > 0);
        let mut offset = Self::find_position_offset(page, position, schema)?;
        let num_keys = page[offset] as usize;
        offset += NODE_METADATA_SIZE;
        let key_length = schema.get_key_length()?;
        let row_length = schema.get_row_length()?;
        let total_length = key_length + row_length;
        let page_length = page.len();
        offset += num_keys * key_length;
        if page_length < offset + expand_size * total_length {
            return Err(InternalExceptionIndexOutOfRange);
        }
        for i in (offset..page_length - expand_size * total_length).rev() {
            page[i + expand_size * total_length] = page[i];
        }
        for i in 0..expand_size {
            for j in 0..total_length {
                page[offset + i * total_length + j] = 0;
            }
        }
        page[0] += expand_size as u8;
        Ok(offset)
    }

    pub fn expand_keys_with_vec(
        expansion: &Vec<Key>,
        page: &mut PageData,
        position: &Position,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        let initial_offset = Self::expand_keys_by(expansion.len(), page, position, schema)?;
        let mut current_offset = initial_offset;
        for data in expansion {
            page[current_offset..current_offset + data.len()].copy_from_slice(data);
            current_offset += data.len();
        }
        Ok(())
    }

    pub fn read_key(
        index: usize,
        page: &PageData,
        position: &Position,
        schema: &TableSchema,
    ) -> Result<Key, Status> {
        let offset = Self::find_position_offset(page, position, schema)?;
        let size: usize = page[offset] as usize;
        let key_length = schema.get_key_length()?;

        if position.cell() >= size {
            return Err(InternalExceptionIndexOutOfRange);
        }

        let index_sized = offset + NODE_METADATA_SIZE + key_length * index;
        Ok(page[index_sized..(index_sized + key_length)].to_owned())
    }

    pub fn read_child(
        index: usize,
        page: &PageData,
        position: &Position,
        schema: &TableSchema,
    ) -> Result<Position, Status> {
        let offset = Self::find_position_offset(page, position, schema)?;
        let num_keys = page[offset] as usize;
        let num_children = num_keys + 1;

        if index >= num_children {
            return Err(InternalExceptionIndexOutOfRange);
        }

        let key_length = schema.get_key_length()?;
        let children_start = offset + NODE_METADATA_SIZE + num_keys * key_length;
        let start_pos = children_start + index * POSITION_SIZE;
        let end_pos = start_pos + POSITION_SIZE;
        let result = Serializer::bytes_to_position(
            <&[u8; POSITION_SIZE]>::try_from(&page[start_pos..end_pos]).unwrap(),
        );
        //this assertion works but in one test. comment it in if there are problems
        //assert!(!result.is_empty());
        Ok(result)
    }

    pub fn write_key(
        index: usize,
        page: &mut PageData,
        position: &Position,
        key: &Key,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        let offset = Self::find_position_offset(page, position, schema)?;
        let num_keys = page[offset] as usize;

        if index >= num_keys {
            return Err(InternalExceptionIndexOutOfRange);
        }

        let key_length = schema.get_key_length()?;
        let list_start_pos = offset + NODE_METADATA_SIZE;
        let start_pos = list_start_pos + index * key_length;
        let end_pos = start_pos + key_length;

        page[start_pos..end_pos].copy_from_slice(key);
        Ok(())
    }

    pub fn write_child(
        index: usize,
        page: &mut PageData,
        position: &Position,
        child: Position,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        let offset = Self::find_position_offset(page, position, schema)?;
        let num_keys = page[offset] as usize;
        let num_children = num_keys + 1;

        if index >= num_children {
            return Err(InternalExceptionIndexOutOfRange);
        }

        let key_length = schema.get_key_length()?;
        let children_start = offset + NODE_METADATA_SIZE + num_keys * key_length;
        let start_pos = children_start + index * POSITION_SIZE;
        let end_pos = start_pos + POSITION_SIZE;

        let child_bytes = Serializer::position_to_bytes(child.clone());
        page[start_pos..end_pos].copy_from_slice(&child_bytes);
        /*
        if child.is_empty() {
            Self::set_is_leaf(page, false)?;
        }*/
        Ok(())
    }

    pub fn read_keys_as_vec(
        page: &PageData,
        position: &Position,
        schema: &TableSchema,
    ) -> Result<Vec<Key>, Status> {
        let offset = Self::find_position_offset(page, position, schema)?;
        let num_keys = page[offset] as usize;
        let key_length = schema.get_key_length()?;
        let list_start_pos = offset + NODE_METADATA_SIZE;

        let mut result = Vec::with_capacity(num_keys);
        for i in 0..num_keys {
            let start_pos = list_start_pos + i * key_length;
            let end_pos = start_pos + key_length;
            result.push(page[start_pos..end_pos].to_owned());
        }
        Ok(result)
    }

    pub fn read_children_as_vec(
        page: &PageData,
        position: &Position,
        schema: &TableSchema,
    ) -> Result<Vec<Position>, Status> {
        let offset = Self::find_position_offset(page, position, schema)?;
        let num_keys = page[offset] as usize;
        let key_length = schema.get_key_length()?;
        let children_start = offset + NODE_METADATA_SIZE + num_keys * key_length;

        let mut result = Vec::new();
        for i in 0..(num_keys + 1) {
            let start_pos = children_start + i * POSITION_SIZE;
            let end_pos = start_pos + POSITION_SIZE;
            let child_position = Serializer::bytes_to_position(
                <&[u8; POSITION_SIZE]>::try_from(&page[start_pos..end_pos]).unwrap(),
            );
            if child_position.is_empty() {
                break;
            }
            result.push(child_position);
        }
        Ok(result)
    }

    /// will adjust number of keys, delete children if necessary
    ///  - the original data will be intact, but empty rows will be padded.
    pub fn write_keys_vec(
        keys: &Vec<Key>,
        page: &mut PageData,
        position: &Position,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        let offset = Self::find_position_offset(page, position, schema)?;
        let old_num_keys = page[offset] as usize;
        let new_num_keys = keys.len();

        if old_num_keys != new_num_keys {
            return Self::write_keys_vec_resize(keys, page, position, schema);
        }

        let key_length = schema.get_key_length()?;
        let list_start_pos = offset + NODE_METADATA_SIZE;

        for (i, key) in keys.iter().enumerate() {
            let start_pos = list_start_pos + i * key_length;
            let end_pos = start_pos + key_length;
            page[start_pos..end_pos].copy_from_slice(key);
        }

        Ok(())
    }
    pub fn write_keys_vec_resize_with_rows(
        keys: &Vec<Key>,
        rows: &Vec<Row>,
        page: &mut PageData,
        position: &Position,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        if keys.len() != rows.len() {
            panic!("keys and rows must have same len")
        }
        Self::write_keys_vec_resize(keys, page, position, schema)?;
        Self::write_data_by_vec(page, position, rows, schema)
    }

    /// - children and data will persist in its original form, but cut-off / padded if node is resized
    pub fn write_keys_vec_resize(
        keys: &Vec<Key>,
        page: &mut PageData,
        position: &Position,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        let offset = Self::find_position_offset(page, position, schema)?;
        //println!("write_keys_vec_resize before: {:?}", page);
        let orig_num_keys = page[offset] as usize;
        let new_num_keys = keys.len();
        let key_length = schema.get_key_length()?;
        let keys_start = offset + NODE_METADATA_SIZE;
        let orig_children_start = keys_start + orig_num_keys * key_length;
        let orig_children_end = orig_children_start + (orig_num_keys + 1) * POSITION_SIZE;
        let orig_data_start = orig_children_start + (orig_num_keys + 1) * POSITION_SIZE;

        if new_num_keys > orig_num_keys {
            //shift data
            let row_offset = (new_num_keys - orig_num_keys) as isize * key_length as isize
                + POSITION_SIZE as isize;
            let children_offset = (new_num_keys - orig_num_keys) as isize * key_length as isize;
            Self::shift_page(page, orig_children_end, row_offset)?;
            Self::shift_page_block(
                page,
                orig_children_start,
                orig_children_end + children_offset as usize,
                children_offset,
            )?;
        }

        for (i, key) in keys.iter().enumerate() {
            let start_pos = keys_start + i * key_length;
            let end_pos = start_pos + key_length;
            page[start_pos..end_pos].copy_from_slice(key);
        }

        //shrink remaining page
        if new_num_keys < orig_num_keys {
            let children_offset = (orig_num_keys - new_num_keys) * key_length;
            Self::shift_page_block(
                page,
                orig_children_start,
                orig_data_start,
                -(children_offset as isize),
            )?;
            let offset = (orig_num_keys - new_num_keys) * POSITION_SIZE + children_offset;
            //println!("data (and rest page) offset: -{}", offset);
            Self::shift_page(page, orig_children_end, -(offset as isize))?;
        }

        page[offset] = new_num_keys as u8;

        //println!("write_keys_vec_resize after: {:?}", page);

        Ok(())
    }

    pub fn write_children_vec(
        children: &Vec<Position>,
        page: &mut PageData,
        position: &Position,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        let offset = Self::find_position_offset(page, position, schema)?;
        let num_keys = page[offset] as usize;
        let key_length = schema.get_key_length()?;
        let children_start = offset + NODE_METADATA_SIZE + num_keys * key_length;

        if children.len() > num_keys + 1 {
            panic!("children must be less or equal to num_keys + 1");
            return Err(InternalExceptionInvalidColCount);
        }

        let mut check_for_leaf = true;
        for (i, child) in children.iter().enumerate() {
            assert!(children.len() > 0);
            let start_pos = children_start + i * POSITION_SIZE;
            let end_pos = start_pos + POSITION_SIZE;
            page[start_pos..end_pos].copy_from_slice(&Serializer::position_to_bytes(child.clone()));

            if check_for_leaf && !child.is_empty() {
                check_for_leaf = false;
                Self::set_is_leaf(page, position, &schema, false)?;
            }
        }
        Ok(())
    }

    pub fn read_data_by_key(
        page: &PageData,
        position: &Position,
        key: Key,
        schema: &TableSchema,
    ) -> Result<Row, Status> {
        let keys = Self::read_keys_as_vec(page, position, schema)?;
        let index = keys
            .iter()
            .position(|k| k == &key)
            .ok_or(InternalExceptionKeyNotFound)?;
        Self::read_data_by_index(index, page, position, schema)
    }

    pub fn read_data_by_index(
        index: usize,
        page: &PageData,
        position: &Position,
        schema: &TableSchema,
    ) -> Result<Row, Status> {
        let offset = Self::find_position_offset(page, position, schema)?;
        let num_keys = page[offset] as usize;

        if index >= num_keys {
            return Err(InternalExceptionIndexOutOfRange);
        }

        let key_length = schema.get_key_length()?;
        let data_start =
            offset + NODE_METADATA_SIZE + num_keys * key_length + (num_keys + 1) * POSITION_SIZE;
        let data_length = schema.get_row_length()?;
        let start = data_start + index * data_length;
        let end = start + data_length;

        Ok(page[start..end].to_vec())
    }

    pub fn write_data_by_key(
        page: &mut PageData,
        position: &Position,
        key: Key,
        row: Row,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        let keys = Self::read_keys_as_vec(page, position, schema)?;
        let index = keys
            .iter()
            .position(|k| k == &key)
            .ok_or(InternalExceptionKeyNotFound)?;
        Self::write_data_by_index(index, page, position, row, schema)
    }

    pub fn write_data_by_index(
        index: usize,
        page: &mut PageData,
        position: &Position,
        row: Row,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        let offset = Self::find_position_offset(page, position, schema)?;
        let num_keys = page[offset] as usize;

        if index >= num_keys {
            return Err(InternalExceptionIndexOutOfRange);
        }

        let key_length = schema.get_key_length()?;
        let data_start =
            offset + NODE_METADATA_SIZE + num_keys * key_length + (num_keys + 1) * POSITION_SIZE;
        let data_length = schema.get_row_length()?;

        if row.len() != data_length {
            return Err(InternalExceptionInvalidRowLength);
        }

        let start = data_start + index * data_length;
        let end = start + data_length;
        page[start..end].copy_from_slice(&row);
        Ok(())
    }

    pub fn read_data_as_vec(
        page: &PageData,
        position: &Position,
        schema: &TableSchema,
    ) -> Result<Vec<Row>, Status> {
        let offset = Self::find_position_offset(page, position, schema)?;
        let num_keys = page[offset] as usize;
        let key_length = schema.get_key_length()?;
        let data_start =
            offset + NODE_METADATA_SIZE + num_keys * key_length + (num_keys + 1) * POSITION_SIZE;
        let data_length = schema.get_row_length()?;

        let mut rows = Vec::with_capacity(num_keys);
        for i in 0..num_keys {
            let start = data_start + i * data_length;
            let end = start + data_length;
            rows.push(page[start..end].to_vec());
        }
        Ok(rows)
    }

    pub fn write_data_by_vec(
        page: &mut PageData,
        position: &Position,
        rows: &Vec<Row>,
        schema: &TableSchema,
    ) -> Result<(), Status> {
        let offset = Self::find_position_offset(page, position, schema)?;
        let num_keys = page[offset] as usize;

        assert_eq!(rows.len(), num_keys);
        if rows.len() != num_keys {
            panic!("rows and keys must have same length");
            return Err(InternalExceptionInvalidColCount);
        }

        let key_length = schema.get_key_length()?;
        let data_start =
            offset + NODE_METADATA_SIZE + num_keys * key_length + (num_keys + 1) * POSITION_SIZE;
        let data_length = schema.get_row_length()?;

        for (i, row) in rows.iter().enumerate() {
            if row.len() != data_length {
                return Err(InternalExceptionInvalidRowLength);
            }
            let start = data_start + i * data_length;
            let end = start + data_length;
            page[start..end].copy_from_slice(row);
        }
        Ok(())
    }

    ///inclusive start
    pub fn shift_page(page: &mut PageData, start: usize, offset: isize) -> Result<(), Status> {
        if offset == 0 {
            return Ok(());
        }

        let page_len = page.len();
        if start >= page_len {
            return Err(InternalExceptionIndexOutOfRange);
        }

        if offset > 0 {
            let offset = offset as usize;
            if start + offset >= page_len {
                return Err(InternalExceptionIndexOutOfRange);
            }
            Self::shift(page, start, page_len, offset, true);
        } else {
            let offset = (-offset) as usize;
            if start < offset {
                return Err(InternalExceptionIndexOutOfRange);
            }
            Self::shift(page, start, page_len, offset, false);
        }

        Ok(())
    }

    /// - inclusive start
    /// - inclusive end
    pub fn shift_page_block(
        page: &mut PageData,
        start: usize,
        end: usize,
        offset: isize,
    ) -> Result<(), Status> {
        if offset == 0 {
            return Ok(());
        }

        let page_len = page.len();
        if start <= 0 || end >= page_len || end <= start {
            panic!("invalid shift");
            return Err(InternalExceptionIndexOutOfRange);
        }

        if offset > 0 {
            let offset = offset as usize;
            if start + offset >= end {
                panic!("invalid shift");
                return Err(InternalExceptionIndexOutOfRange);
            }
            Self::shift(page, start, end, offset, true);
        } else {
            let offset = (-offset) as usize;

            if start < offset {
                panic!("invalid shift");
                return Err(InternalExceptionIndexOutOfRange);
            }
            Self::shift(page, start, end, offset, false);
        }

        Ok(())
    }

    //optimize for large blocks? no, memcopy
    fn shift(page: &mut [u8], start: usize, end: usize, offset: usize, right: bool) {
        if right {
            page.copy_within(start..end - offset, start + offset);
            page[start..start + offset].fill(0);
        } else {
            page.copy_within(start..end, start - offset);
            page[end - offset..end].fill(0);
        }
    }

    //TODO i dont know if this is up-to-date
    #[deprecated]
    pub fn verify_schema(schema: &TableSchema) -> Result<(), Status> {
        let computed_data_length: usize = schema
            .fields
            .iter()
            .map(|field| Self::get_size_of_type(&field.field_type).unwrap_or(0))
            .sum::<usize>()
            - schema.get_key_length()?;

        if computed_data_length != schema.get_row_length()? {
            return Err(InternalExceptionInvalidSchema);
        }
        if let Some(key_field) = schema.fields.get(0) {
            let key_field_size = Self::get_size_of_type(&key_field.field_type).unwrap_or(0);
            if key_field_size != schema.get_key_length()? {
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

        if computed_row_length != schema.get_key_and_row_length()? {
            return Err(InternalExceptionInvalidSchema);
        }
        if schema.fields.len() != schema.get_col_count()? {
            return Err(InternalExceptionInvalidSchema);
        }
        Ok(())
    }

    pub fn empty_key(schema: &TableSchema) -> Result<Key, Status> {
        Ok(vec![0; schema.get_key_length()?])
    }

    pub fn empty_row(schema: &TableSchema) -> Result<Row, Status> {
        Ok(vec![0; schema.get_row_length()?])
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
            Type::Integer => Ok(Self::write_byte_at_position(&mut v[0], position, value)),
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
            Type::Integer => Ok(Self::byte_to_bool_at_position(v[0], position)),
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

    pub fn create_node_flag(is_leaf: bool) -> Flag {
        let mut flag = 0u8;
        Self::write_byte_at_position(&mut flag, NodeFlag::Leaf as u8, is_leaf);
        flag
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
        Self::format_field(key, &schema.get_key_type()?)
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

    pub fn get_field_on_row(
        row: &Vec<u8>,
        index: usize,
        table_schema: &TableSchema,
    ) -> Result<Vec<u8>, Status> {
        let field_type = &table_schema.fields[index].field_type;
        let len = Serializer::get_size_of_type(field_type).unwrap();
        let mut start = 0;
        for i in 0..index {
            start += Serializer::get_size_of_type(&table_schema.fields[i].field_type).unwrap();
        }
        Ok(row[start..(start + len)].to_vec())
    }

    pub fn format_field_on_row(
        row: &Vec<u8>,
        index: usize,
        table_schema: &TableSchema,
    ) -> Result<String, Status> {
        let field_type = &table_schema.fields[index].field_type;
        let bytes = Self::get_field_on_row(row, index, table_schema)?;
        Self::format_field(&bytes, field_type)
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
        // byte 0, 1 -> page (big-endian)
        // byte 2, 3 -> cell (big-endian)
        let page = ((bytes[0] as u16) << 8) | (bytes[1] as u16);
        let cell = ((bytes[2] as u16) << 8) | (bytes[3] as u16);

        Position::new(page as usize, cell as usize)
    }

    pub fn position_to_bytes(position: Position) -> [u8; POSITION_SIZE] {
        let mut bytes = [0u8; POSITION_SIZE];
        bytes[0] = (position.page() >> 8) as u8;
        bytes[1] = (position.page() & 0xFF) as u8;
        bytes[2] = (position.cell() >> 8) as u8;
        bytes[3] = (position.cell() & 0xFF) as u8;
        bytes
    }

    pub fn int_to_bytes(value: i32) -> [u8; INTEGER_SIZE] {
        let mut bytes = [0u8; INTEGER_SIZE];
        bytes[0] = 0;

        for i in 0..4 {
            bytes[INTEGER_SIZE - 1 - i] = ((value >> (i * 8)) & 0xFF) as u8;
        }

        bytes
    }

    pub fn bytes_to_int(bytes: [u8; INTEGER_SIZE]) -> i32 {
        let mut value = 0i32;

        for i in 1..INTEGER_SIZE {
            value = (value << 8) | (bytes[i] as i32);
        }

        value
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
