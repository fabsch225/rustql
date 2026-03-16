use crate::btree::BTreeNode;
use crate::pager::{
    Key, NODE_METADATA_SIZE, PAGE_SIZE, POSITION_SIZE, PagerAccessor, Position, Row, Type,
};
use crate::schema::TableSchema;
use crate::serializer::Serializer;
use crate::status::Status;
use std::collections::HashSet;

pub struct PagerProxy {}

impl PagerProxy {
    const INLINE_STRING_PREFIX_LEN: usize = 12;
    const EXTERNAL_PAGE_HEADER_SIZE: usize = 8;
    const EXTERNAL_PTR_OFFSET: usize = Self::INLINE_STRING_PREFIX_LEN + 1;
    const EXTERNAL_LEN_OFFSET: usize = Self::EXTERNAL_PTR_OFFSET + POSITION_SIZE;
    const EXTERNAL_MARKER_OFFSET: usize = Self::EXTERNAL_LEN_OFFSET + 2;
    const EXTERNAL_ORIG_FLAG_OFFSET: usize = Self::EXTERNAL_MARKER_OFFSET + 1;
    const EXTERNAL_MARKER: u8 = 0xA5;
    const EXTERNAL_META_MIN_FIELD_LEN: usize = Self::EXTERNAL_ORIG_FLAG_OFFSET + 1;
    const EXTERNAL_DEPRECATED_OFFSET: usize = 4;
    const EXTERNAL_OWNER_ROOT_OFFSET: usize = 5;
    const EXTERNAL_MAGIC_OFFSET: usize = 7;
    const EXTERNAL_PAGE_MAGIC: u8 = 0xD1;

    fn external_page_payload_capacity() -> usize {
        PAGE_SIZE - Self::EXTERNAL_PAGE_HEADER_SIZE
    }

    fn can_externalize_field(field_len: usize) -> bool {
        field_len >= Self::EXTERNAL_META_MIN_FIELD_LEN
    }

    fn field_string_payload_len(field_bytes: &[u8]) -> usize {
        let payload_len = field_bytes.len().saturating_sub(1);
        field_bytes[..payload_len]
            .iter()
            .position(|b| *b == 0)
            .unwrap_or(payload_len)
    }

    fn read_next_page_index_from_payload_page(page_data: &[u8; PAGE_SIZE]) -> usize {
        u16::from_be_bytes([page_data[0], page_data[1]]) as usize
    }

    fn read_payload_owner_root_page(page_data: &[u8; PAGE_SIZE]) -> usize {
        u16::from_be_bytes([
            page_data[Self::EXTERNAL_OWNER_ROOT_OFFSET],
            page_data[Self::EXTERNAL_OWNER_ROOT_OFFSET + 1],
        ]) as usize
    }

    fn is_payload_page_candidate(page: &crate::pager::PageContainer) -> Result<bool, Status> {
        let is_data = Serializer::is_data_page(page)?;
        let is_overflow = Serializer::is_overflow_page(page)?;
        Ok((is_data || is_overflow) && page.data[Self::EXTERNAL_MAGIC_OFFSET] == Self::EXTERNAL_PAGE_MAGIC)
    }

    fn is_payload_page_deprecated(page_data: &[u8; PAGE_SIZE]) -> bool {
        page_data[Self::EXTERNAL_DEPRECATED_OFFSET] == 1
    }

    fn collect_external_payload_heads_from_rows(
        schema: &TableSchema,
        rows: &Vec<Row>,
    ) -> Result<HashSet<usize>, Status> {
        let mut heads = HashSet::new();
        for row in rows {
            let _ = Serializer::map_row_non_key_fields_with_callback(
                row,
                schema,
                |_, field_type, field_bytes| {
                    if Self::is_field_externalized(field_type, field_bytes)? {
                        let ptr_slice = &field_bytes
                            [Self::EXTERNAL_PTR_OFFSET..Self::EXTERNAL_PTR_OFFSET + POSITION_SIZE];
                        let ptr = Serializer::bytes_to_position(
                            <&[u8; POSITION_SIZE]>::try_from(ptr_slice)
                                .expect("slice length checked"),
                        );
                        if !ptr.is_empty() {
                            heads.insert(ptr.page());
                        }
                    }
                    Ok(field_bytes.to_vec())
                },
            )?;
        }
        Ok(heads)
    }

    fn mark_payload_chain_deprecated(
        pager_interface: PagerAccessor,
        start: Position,
    ) -> Result<(), Status> {
        if start.is_empty() {
            return Ok(());
        }

        let mut current_page = start.page();
        let mut guard = 0usize;
        while current_page > 0 {
            if guard > 1024 {
                return Err(Status::InternalExceptionPageCorrupted);
            }
            guard += 1;

            let pos = Position::new(current_page, 0);
            let page_read = pager_interface.access_pager_write(|p| p.access_page_read(&pos))?;
            if !Self::is_payload_page_candidate(&page_read)? {
                break;
            }

            let next_page = Self::read_next_page_index_from_payload_page(&page_read.data);
            pager_interface.access_pager_write(|p| {
                let page = p.access_page_write(&pos)?;
                page.data[Self::EXTERNAL_DEPRECATED_OFFSET] = 1;
                Ok(())
            })?;
            current_page = next_page;
        }

        Ok(())
    }

    fn deprecate_removed_payload_chains(
        pager_interface: PagerAccessor,
        schema: &TableSchema,
        old_rows: &Vec<Row>,
        new_rows: &Vec<Row>,
    ) -> Result<(), Status> {
        let old_heads = Self::collect_external_payload_heads_from_rows(schema, old_rows)?;
        let new_heads = Self::collect_external_payload_heads_from_rows(schema, new_rows)?;

        for page_id in old_heads.difference(&new_heads) {
            Self::mark_payload_chain_deprecated(
                pager_interface.clone(),
                Position::new(*page_id, 0),
            )?;
        }
        Ok(())
    }

    fn take_deprecated_payload_page(
        pager_interface: PagerAccessor,
        owner_root_page: usize,
    ) -> Result<Option<usize>, Status> {
        let next_page_idx = pager_interface.get_next_page_index();
        for page_idx in 1..next_page_idx {
            let pos = Position::new(page_idx, 0);
            let page = pager_interface.access_pager_write(|p| p.access_page_read(&pos))?;

            if !Self::is_payload_page_candidate(&page)? {
                continue;
            }

            let owner = Self::read_payload_owner_root_page(&page.data);
            if owner != owner_root_page || !Self::is_payload_page_deprecated(&page.data) {
                continue;
            }

            return Ok(Some(page_idx));
        }

        Ok(None)
    }

    fn is_field_externalized(field_type: &Type, field_bytes: &[u8]) -> Result<bool, Status> {
        if !matches!(field_type, Type::String | Type::Varchar(_)) {
            return Ok(false);
        }
        if !Self::can_externalize_field(field_bytes.len()) {
            return Ok(false);
        }

        let flagged = Serializer::is_external(&field_bytes.to_vec(), field_type)?;
        if !flagged {
            return Ok(false);
        }

        let marker_ok = field_bytes[Self::EXTERNAL_MARKER_OFFSET] == Self::EXTERNAL_MARKER;
        let ptr_slice = &field_bytes[Self::EXTERNAL_PTR_OFFSET..Self::EXTERNAL_PTR_OFFSET + POSITION_SIZE];
        let ptr = Serializer::bytes_to_position(
            <&[u8; POSITION_SIZE]>::try_from(ptr_slice).expect("slice length checked"),
        );
        let tail_len = u16::from_be_bytes([
            field_bytes[Self::EXTERNAL_LEN_OFFSET],
            field_bytes[Self::EXTERNAL_LEN_OFFSET + 1],
        ]) as usize;

        Ok(marker_ok && !ptr.is_empty() && tail_len > 0)
    }

    pub fn write_payload_to_data_pages(
        pager_interface: PagerAccessor,
        payload: &[u8],
        owner_root_page: usize,
    ) -> Result<Position, Status> {
        if payload.is_empty() {
            return Ok(Position::make_empty());
        }

        let chunk_size = Self::external_page_payload_capacity();
        let chunks: Vec<&[u8]> = payload.chunks(chunk_size).collect();
        let mut next_page_index = 0usize;

        for chunk_index in (0..chunks.len()).rev() {
            let chunk = chunks[chunk_index];
            let is_data_page = chunk_index == 0;
            let page_idx = if let Some(reused) =
                Self::take_deprecated_payload_page(pager_interface.clone(), owner_root_page)?
            {
                reused
            } else {
                pager_interface.access_pager_write(|p| p.create_page())?
            };
            let pos = Position::new(page_idx, 0);

            pager_interface.access_pager_write(|p| {
                let page = p.access_page_write(&pos)?;
                page.data = [0u8; PAGE_SIZE];

                page.data[0..2].copy_from_slice(&(next_page_index as u16).to_be_bytes());
                page.data[2..4].copy_from_slice(&(chunk.len() as u16).to_be_bytes());
                page.data[Self::EXTERNAL_DEPRECATED_OFFSET] = 0;
                page.data[Self::EXTERNAL_OWNER_ROOT_OFFSET..Self::EXTERNAL_OWNER_ROOT_OFFSET + 2]
                    .copy_from_slice(&(owner_root_page as u16).to_be_bytes());
                page.data[Self::EXTERNAL_MAGIC_OFFSET] = Self::EXTERNAL_PAGE_MAGIC;
                page.data[Self::EXTERNAL_PAGE_HEADER_SIZE
                    ..Self::EXTERNAL_PAGE_HEADER_SIZE + chunk.len()]
                    .copy_from_slice(chunk);

                Serializer::set_is_data_page(page, is_data_page)?;
                Serializer::set_is_overflow_page(page, !is_data_page)?;
                Ok(())
            })?;

            next_page_index = page_idx;
        }

        Ok(Position::new(next_page_index, 0))
    }

    pub fn read_payload_from_pages(
        pager_interface: PagerAccessor,
        start: Position,
    ) -> Result<Vec<u8>, Status> {
        if start.is_empty() {
            return Ok(vec![]);
        }

        let mut payload = Vec::new();
        let mut current_page = start.page();
        let mut guard = 0usize;

        while current_page > 0 {
            if guard > 1024 {
                return Err(Status::InternalExceptionPageCorrupted);
            }
            guard += 1;

            let pos = Position::new(current_page, 0);
            let page = pager_interface.access_pager_write(|p| p.access_page_read(&pos))?;
            let is_new_header = page.data[Self::EXTERNAL_MAGIC_OFFSET] == Self::EXTERNAL_PAGE_MAGIC;
            let header_size = if is_new_header {
                Self::EXTERNAL_PAGE_HEADER_SIZE
            } else {
                4
            };

            if is_new_header && Self::is_payload_page_deprecated(&page.data) {
                return Err(Status::InternalExceptionPageCorrupted);
            }
            let chunk_len = u16::from_be_bytes([page.data[2], page.data[3]]) as usize;
            if chunk_len > PAGE_SIZE.saturating_sub(header_size) {
                return Err(Status::InternalExceptionPageCorrupted);
            }

            let chunk_start = header_size;
            let chunk_end = chunk_start + chunk_len;
            payload.extend_from_slice(&page.data[chunk_start..chunk_end]);

            current_page = Self::read_next_page_index_from_payload_page(&page.data);
        }

        Ok(payload)
    }

    fn encode_row_for_external_storage(
        node: &BTreeNode,
        row: &Row,
    ) -> Result<(Row, bool), Status> {
        let mut used_external = false;
        let encoded = Serializer::map_row_non_key_fields_with_callback(
            row,
            &node.table_schema,
            |_, field_type, field_bytes| {
                if !matches!(field_type, Type::String | Type::Varchar(_)) {
                    return Ok(field_bytes.to_vec());
                }

                if !Self::can_externalize_field(field_bytes.len()) {
                    return Ok(field_bytes.to_vec());
                }

                let payload_len = Self::field_string_payload_len(field_bytes);
                if payload_len <= Self::INLINE_STRING_PREFIX_LEN {
                    return Ok(field_bytes.to_vec());
                }

                let tail =
                    &field_bytes[Self::INLINE_STRING_PREFIX_LEN..payload_len.min(field_bytes.len())];
                let external_pos = Self::write_payload_to_data_pages(
                    node.pager_accessor.clone(),
                    tail,
                    node.table_schema.root.page(),
                )?;
                let mut out = vec![0u8; field_bytes.len()];
                out[..Self::INLINE_STRING_PREFIX_LEN]
                    .copy_from_slice(&field_bytes[..Self::INLINE_STRING_PREFIX_LEN]);
                out[Self::INLINE_STRING_PREFIX_LEN] = 0;
                out[field_bytes.len() - 1] = field_bytes[field_bytes.len() - 1];

                let ptr_bytes = Serializer::position_to_bytes(external_pos);
                out[Self::EXTERNAL_PTR_OFFSET..Self::EXTERNAL_PTR_OFFSET + POSITION_SIZE]
                    .copy_from_slice(&ptr_bytes);
                out[Self::EXTERNAL_LEN_OFFSET..Self::EXTERNAL_LEN_OFFSET + 2]
                    .copy_from_slice(&(tail.len() as u16).to_be_bytes());
                out[Self::EXTERNAL_MARKER_OFFSET] = Self::EXTERNAL_MARKER;
                out[Self::EXTERNAL_ORIG_FLAG_OFFSET] = field_bytes[field_bytes.len() - 1];

                Serializer::set_is_external(&mut out, true, field_type)?;
                used_external = true;
                Ok(out)
            },
        )?;

        Ok((encoded, used_external))
    }

    fn decode_row_from_external_storage(node: &BTreeNode, row: &Row) -> Result<Row, Status> {
        Serializer::map_row_non_key_fields_with_callback(row, &node.table_schema, |_, field_type, field_bytes| {
            if !matches!(field_type, Type::String | Type::Varchar(_)) {
                return Ok(field_bytes.to_vec());
            }

            if !Self::is_field_externalized(field_type, field_bytes)? {
                return Ok(field_bytes.to_vec());
            }

            let encoded = field_bytes.to_vec();
            if encoded.len() <= Self::EXTERNAL_MARKER_OFFSET || !Self::can_externalize_field(field_bytes.len()) {
                return Ok(field_bytes.to_vec());
            }

            let ptr_slice = &field_bytes[Self::EXTERNAL_PTR_OFFSET..Self::EXTERNAL_PTR_OFFSET + POSITION_SIZE];
            let ptr = Serializer::bytes_to_position(
                <&[u8; POSITION_SIZE]>::try_from(ptr_slice).expect("slice length checked"),
            );
            let tail_len = u16::from_be_bytes([
                field_bytes[Self::EXTERNAL_LEN_OFFSET],
                field_bytes[Self::EXTERNAL_LEN_OFFSET + 1],
            ]) as usize;

            let mut tail = match Self::read_payload_from_pages(node.pager_accessor.clone(), ptr) {
                Ok(t) => t,
                Err(_) => return Ok(field_bytes.to_vec()),
            };
            if tail.len() > tail_len {
                tail.truncate(tail_len);
            }

            let mut out = vec![0u8; field_bytes.len()];
            let payload_capacity = out.len().saturating_sub(1);

            let inline_len = Self::INLINE_STRING_PREFIX_LEN.min(payload_capacity);
            out[..inline_len].copy_from_slice(&field_bytes[..inline_len]);

            let tail_copy_start = inline_len;
            let tail_copy_len = tail
                .len()
                .min(payload_capacity.saturating_sub(tail_copy_start));
            if tail_copy_len > 0 {
                out[tail_copy_start..tail_copy_start + tail_copy_len]
                    .copy_from_slice(&tail[..tail_copy_len]);
            }

            let out_last_idx = out.len() - 1;
            out[out_last_idx] = field_bytes[Self::EXTERNAL_ORIG_FLAG_OFFSET];
            Ok(out)
        })
    }

    fn update_node_external_flag(
        page_data: &mut [u8; PAGE_SIZE],
        node: &BTreeNode,
        encoded_rows: &Vec<Row>,
    ) -> Result<(), Status> {
        let has_external = encoded_rows
            .iter()
            .try_fold(false, |acc, row| {
                if acc {
                    return Ok::<bool, Status>(true);
                }
                let mut found = false;
                let _ = Serializer::map_row_non_key_fields_with_callback(
                    row,
                    &node.table_schema,
                    |_, field_type, field_bytes| {
                        if Self::is_field_externalized(field_type, field_bytes)? {
                            found = true;
                        }
                        Ok(field_bytes.to_vec())
                    },
                )?;
                Ok(found)
            })?;
        Serializer::set_has_external_data(
            page_data,
            &node.position,
            &node.table_schema,
            has_external,
        )
    }

    fn node_size_for_num_keys(schema: &TableSchema, num_keys: usize) -> Result<usize, Status> {
        let key_length = schema.get_key_length()?;
        let row_length = schema.get_row_length()?;
        Ok(NODE_METADATA_SIZE
            + num_keys * (key_length + row_length)
            + (num_keys + 1) * POSITION_SIZE)
    }

    fn is_large_node_mode(schema: &TableSchema) -> Result<bool, Status> {
        let has_varchar = schema
            .fields
            .iter()
            .any(|f| matches!(f.field_type, Type::Varchar(_)));
        Ok(has_varchar && schema.get_node_size_in_bytes()? > PAGE_SIZE)
    }

    fn reserved_pages_for_node(schema: &TableSchema) -> Result<usize, Status> {
        let max_node_size = schema.get_node_size_in_bytes()?;
        Ok(std::cmp::max(1, max_node_size.div_ceil(PAGE_SIZE)))
    }

    fn decode_node_blob(
        schema: &TableSchema,
        blob: &[u8],
    ) -> Result<(u8, u8, Vec<Key>, Vec<Position>, Vec<Row>), Status> {
        if blob.len() < NODE_METADATA_SIZE {
            return Err(Status::InternalExceptionPageCorrupted);
        }
        let num_keys = blob[0] as usize;
        let flag = blob[1];
        let key_len = schema.get_key_length()?;
        let row_len = schema.get_row_length()?;
        let expected = Self::node_size_for_num_keys(schema, num_keys)?;
        if blob.len() < expected {
            return Err(Status::InternalExceptionPageCorrupted);
        }

        let mut offset = NODE_METADATA_SIZE;
        let mut keys = Vec::with_capacity(num_keys);
        for _ in 0..num_keys {
            keys.push(blob[offset..offset + key_len].to_vec());
            offset += key_len;
        }

        let mut children = Vec::with_capacity(num_keys + 1);
        for _ in 0..(num_keys + 1) {
            let pos = Serializer::bytes_to_position(
                <&[u8; POSITION_SIZE]>::try_from(&blob[offset..offset + POSITION_SIZE])
                    .map_err(|_| Status::InternalExceptionPageCorrupted)?,
            );
            if !pos.is_empty() {
                children.push(pos);
            }
            offset += POSITION_SIZE;
        }

        let mut rows = Vec::with_capacity(num_keys);
        for _ in 0..num_keys {
            rows.push(blob[offset..offset + row_len].to_vec());
            offset += row_len;
        }

        Ok((num_keys as u8, flag, keys, children, rows))
    }

    fn encode_node_blob(
        schema: &TableSchema,
        num_keys: usize,
        flag: u8,
        keys: &Vec<Key>,
        children: &Vec<Position>,
        rows: &Vec<Row>,
    ) -> Result<Vec<u8>, Status> {
        let key_len = schema.get_key_length()?;
        let row_len = schema.get_row_length()?;
        if keys.len() != num_keys || rows.len() != num_keys {
            return Err(Status::InternalExceptionInvalidColCount);
        }
        if children.len() > num_keys + 1 {
            return Err(Status::InternalExceptionInvalidColCount);
        }

        let total = Self::node_size_for_num_keys(schema, num_keys)?;
        let mut blob = vec![0u8; total];
        blob[0] = num_keys as u8;
        blob[1] = flag;

        let mut offset = NODE_METADATA_SIZE;
        for key in keys {
            if key.len() != key_len {
                return Err(Status::InternalExceptionInvalidRowLength);
            }
            blob[offset..offset + key_len].copy_from_slice(key);
            offset += key_len;
        }

        for idx in 0..(num_keys + 1) {
            let child = if idx < children.len() {
                children[idx].clone()
            } else {
                Position::make_empty()
            };
            let raw = Serializer::position_to_bytes(child);
            blob[offset..offset + POSITION_SIZE].copy_from_slice(&raw);
            offset += POSITION_SIZE;
        }

        for row in rows {
            if row.len() != row_len {
                return Err(Status::InternalExceptionInvalidRowLength);
            }
            blob[offset..offset + row_len].copy_from_slice(row);
            offset += row_len;
        }

        Ok(blob)
    }

    fn read_node_blob(node: &BTreeNode) -> Result<Vec<u8>, Status> {
        if !Self::is_large_node_mode(&node.table_schema)? {
            let page = node
                .pager_accessor
                .access_pager_write(|p| p.access_page_read(&node.position))?;
            let node_size = Self::node_size_for_num_keys(&node.table_schema, page.data[0] as usize)?;
            return Ok(page.data[0..node_size].to_vec());
        }

        let first = node
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&node.position))?;
        let num_keys = first.data[0] as usize;
        let node_size = Self::node_size_for_num_keys(&node.table_schema, num_keys)?;
        let pages = node_size.div_ceil(PAGE_SIZE);
        let mut blob = vec![0u8; node_size];

        for i in 0..pages {
            let pos = Position::new(node.position.page() + i, 0);
            let page = node.pager_accessor.access_pager_write(|p| p.access_page_read(&pos))?;
            let start = i * PAGE_SIZE;
            let end = std::cmp::min(start + PAGE_SIZE, node_size);
            blob[start..end].copy_from_slice(&page.data[..(end - start)]);
        }

        Ok(blob)
    }

    fn write_node_blob(node: &BTreeNode, blob: &[u8]) -> Result<(), Status> {
        if !Self::is_large_node_mode(&node.table_schema)? {
            let mut page = node
                .pager_accessor
                .access_pager_write(|p| p.access_page_read(&node.position))?;
            page.data.fill(0);
            page.data[0..blob.len()].copy_from_slice(blob);
            return node.pager_accessor.access_page_write(node, |d| {
                d.data = page.data;
                Ok(())
            });
        }

        let pages = blob.len().div_ceil(PAGE_SIZE);
        let reserved = Self::reserved_pages_for_node(&node.table_schema)?;
        if pages > reserved {
            return Err(Status::InternalExceptionIndexOutOfRange);
        }

        for i in 0..reserved {
            let pos = Position::new(node.position.page() + i, 0);
            node.pager_accessor.access_pager_write(|p| {
                let page = p.access_page_write(&pos)?;
                page.data.fill(0);
                if i < pages {
                    let start = i * PAGE_SIZE;
                    let end = std::cmp::min(start + PAGE_SIZE, blob.len());
                    page.data[0..(end - start)].copy_from_slice(&blob[start..end]);
                }
                Serializer::set_is_overflow_page(page, i > 0)?;
                Serializer::set_is_data_page(page, false)?;
                Ok(())
            })?;
        }

        Ok(())
    }

    fn count_nodes_on_page(schema: &TableSchema, page_data: &[u8; PAGE_SIZE]) -> Result<usize, Status> {
        let mut count = 0usize;
        let mut offset = 0usize;

        while offset + NODE_METADATA_SIZE <= PAGE_SIZE {
            let num_keys = page_data[offset] as usize;
            let flag = page_data[offset + 1];

            // Unused tail of page
            if num_keys == 0 && flag == 0 {
                break;
            }

            let node_size = Self::node_size_for_num_keys(schema, num_keys)?;
            if offset + node_size > PAGE_SIZE {
                return Err(Status::InternalExceptionIndexOutOfRange);
            }

            count += 1;
            offset += node_size;
        }

        Ok(count)
    }

    pub fn clear_table_root(
        table_schema: &TableSchema,
        pager_interface: PagerAccessor,
    ) -> Result<(), Status> {
        let src_page =
            Serializer::init_page_data_with_children(vec![], vec![], vec![], &table_schema)?;
        let root = BTreeNode {
            position: table_schema.root.clone(),
            pager_accessor: pager_interface.clone(),
            table_schema: table_schema.clone(),
        };
        pager_interface.access_page_write(&root, |pc| {
            Serializer::copy_node(
                &table_schema,
                &root.position,
                &Position::make_empty(),
                &mut pc.data,
                &src_page,
            )?;
            Ok(())
        })
    }

    pub fn set_table_root(
        schema: &TableSchema,
        pager_interface: PagerAccessor,
        node: &BTreeNode,
    ) -> Result<(), Status> {
        let root = BTreeNode {
            position: schema.root.clone(),
            pager_accessor: pager_interface.clone(),
            table_schema: schema.clone(),
        };
        let src_page = pager_interface.access_page_read(node, |pc| Ok(pc.clone()))?;
        pager_interface.access_page_write(&root, |pc| {
            Serializer::copy_node(
                &schema,
                &root.position,
                &node.position,
                &mut pc.data,
                &src_page.data,
            )?;
            Ok(())
        })
    }
    pub fn create_empty_node_on_new_page(
        schema: &TableSchema,
        pager_interface: PagerAccessor,
    ) -> Result<BTreeNode, Status> {
        if Self::is_large_node_mode(schema)? {
            let reserved = Self::reserved_pages_for_node(schema)?;
            let first_page = pager_interface.access_pager_write(|p| p.create_page())?;
            for _ in 1..reserved {
                let _ = pager_interface.access_pager_write(|p| p.create_page())?;
            }
            let position = Position::new(first_page, 0);
            return Self::create_empty_node_at_position(schema, pager_interface, position);
        }

        let page = pager_interface.access_pager_write(|p| p.create_page())?;
        let position = Position::new(page, 0);
        Self::create_empty_node_at_position(schema, pager_interface, position)
    }

    fn create_empty_node_at_position(
        schema: &TableSchema,
        pager_interface: PagerAccessor,
        position: Position,
    ) -> Result<BTreeNode, Status> {
        let node = BTreeNode {
            position,
            pager_accessor: pager_interface.clone(),
            table_schema: schema.clone(),
        };

        if Self::is_large_node_mode(schema)? {
            let blob = Self::encode_node_blob(
                schema,
                0,
                Serializer::create_node_flag(true),
                &vec![],
                &vec![],
                &vec![],
            )?;
            Self::write_node_blob(&node, &blob)?;
            return Ok(node);
        }

        //create the inital node-flag (set is_leaf to true)
        pager_interface.access_page_write(&node, |d| {
            let node_offset = Serializer::find_position_offset(&d.data, &node.position, schema)?;
            d.data[node_offset + 1] = Serializer::create_node_flag(true);
            Ok(())
        })?;
        Ok(node)
    }

    /// - switches nodes within pages
    /// - in both, parameters, the children are updated
    /// - still, if not carefully used, this could still create cyclic references
    pub fn switch_nodes(
        schema: &TableSchema,
        pager_interface: PagerAccessor,
        node1: &BTreeNode,
        node2: &BTreeNode,
    ) -> Result<(), Status> {
        if Self::is_large_node_mode(schema)? {
            let blob1 = Self::read_node_blob(node1)?;
            let blob2 = Self::read_node_blob(node2)?;
            Self::write_node_blob(node1, &blob2)?;
            Self::write_node_blob(node2, &blob1)?;
        } else {
        let switch_on_same_page = node1.position.page() == node2.position.page();
        if switch_on_same_page {
            pager_interface.access_page_write(node1, |p| {
                Serializer::switch_nodes(
                    schema,
                    &node1.position,
                    &node2.position,
                    &mut p.data,
                    None,
                )?;
                Ok(())
            })?;
        } else {
            pager_interface.access_pager_write(|p| {
                let mut page1 = p.access_page_read(&node1.position)?;
                let mut page2 = p.access_page_write(&node2.position)?;
                Serializer::switch_nodes(
                    schema,
                    &node1.position,
                    &node2.position,
                    &mut page1.data,
                    Some(&mut page2.data),
                )?;
                let mut page1_write = p.access_page_write(&node1.position)?;
                page1_write.data = page1.data;
                Ok(())
            })?;
        }
        }
        //search and replace children (only in these 2 nodes)
        let node_1_position = node1.position.clone();
        let node_2_position = node2.position.clone();
        //we assume neither of the nodes had itself as a child on input
        //mind that now the nodes are switched:
        let mut node1_children = PagerProxy::get_children(node1)?;
        let mut change_to_node1_children = false;
        for i in 0..node1_children.len() {
            if node1_children[i].position == node_1_position {
                node1_children[i].position = node_2_position.clone();
                change_to_node1_children = true;
                break; //might as well
            }
        }
        if change_to_node1_children {
            PagerProxy::set_children(node1, node1_children.clone())?;
        }
        let mut node2_children = PagerProxy::get_children(node2)?;
        let mut change_to_node2_children = false;
        for i in 0..node2_children.len() {
            if node2_children[i].position == node_2_position {
                node2_children[i].position = node_1_position.clone();
                change_to_node2_children = true;
                break; //might as well
            }
        }
        if change_to_node2_children {
            PagerProxy::set_children(node2, node2_children)?;
        }

        Ok(())
    }
    pub fn create_node(
        schema: TableSchema,
        pager_interface: PagerAccessor,
        _page_hint: Option<&BTreeNode>,
        keys: Vec<Key>,
        children: Vec<Position>,
        data: Vec<Row>,
    ) -> Result<BTreeNode, Status> {
        if Self::is_large_node_mode(&schema)? {
            let new_node = Self::create_empty_node_on_new_page(&schema, pager_interface.clone())?;
            Self::set_keys_and_children_as_positions(&new_node, keys, children)?;
            Self::set_data(&new_node, data)?;
            return Ok(new_node);
        }

        if !children.is_empty() {
            let new_node = Self::create_empty_node_on_new_page(&schema, pager_interface.clone())?;
            Self::set_keys_and_children_as_positions(&new_node, keys, children)?;
            Self::set_data(&new_node, data)?;
            return Ok(new_node);
        }

        let page_capacity = schema.max_nodes_per_page()?;

        let mut chosen_position: Option<Position> = None;
        for (page, advertised_free_slots) in &schema.free_list {
            if *advertised_free_slots == 0 {
                continue;
            }
            let used_slots = pager_interface
                .access_pager_write(|p| p.access_page_read(&Position::new(*page, 0)))
                .and_then(|pc| Self::count_nodes_on_page(&schema, &pc.data))?;

            if used_slots < page_capacity {
                chosen_position = Some(Position::new(*page, used_slots));
                break;
            }
        }

        let new_node = if let Some(position) = chosen_position {
            Self::create_empty_node_at_position(&schema, pager_interface.clone(), position)?
        } else {
            Self::create_empty_node_on_new_page(&schema, pager_interface.clone())?
        };

        Self::set_keys_and_children_as_positions(&new_node, keys, children)?;    
        Self::set_data(&new_node, data)?;
       
        Ok(new_node)
    }

    pub fn create_node_without_children(
        schema: TableSchema,
        pager_interface: PagerAccessor,
        parent: Option<&BTreeNode>,
        key: Key,
        data: Row,
    ) -> Result<BTreeNode, Status> {
        Self::create_node(
            schema,
            pager_interface,
            parent,
            vec![key],
            vec![],
            vec![data],
        )
    }

    pub fn is_leaf(btree_node: &BTreeNode) -> Result<bool, Status> {
        let position = &btree_node.position;
        let interface = btree_node.pager_accessor.clone();
        let table_schema = &btree_node.table_schema;
        interface.access_page_read(btree_node, |page_container| {
            Serializer::is_leaf(&page_container.data, position, table_schema)
        })
    }

    pub fn get_keys_count(node: &BTreeNode) -> Result<usize, Status> {
        if Self::is_large_node_mode(&node.table_schema)? {
            return Self::get_keys_encoded(node).map(|(k, _)| k.len());
        }
        //TODO this is very suboptimal
        node.pager_accessor
            .access_page_read(&node, |page_container| {
                Serializer::read_keys_as_vec(
                    &page_container.data,
                    &node.position,
                    &node.table_schema,
                )
                .map(|v| v.len())
            })
    }

    pub fn get_children_count(node: &BTreeNode) -> Result<usize, Status> {
        if Self::is_large_node_mode(&node.table_schema)? {
            return Self::get_children(node).map(|v| v.len());
        }
        node.pager_accessor
            .access_page_read(&node, |page_container| {
                Serializer::read_children_as_vec(
                    &page_container.data,
                    &node.position,
                    &node.table_schema,
                )
                .map(|v| v.len())
            })
    }

    //this seems useless XD
    pub fn get_node(
        pager_accessor: PagerAccessor,
        table_schema: TableSchema,
        position: Position,
    ) -> Result<BTreeNode, Status> {
        Ok(BTreeNode {
            position,
            pager_accessor,
            table_schema,
        })
    }

    pub fn get_child(index: usize, parent: &BTreeNode) -> Result<BTreeNode, Status> {
        if Self::is_large_node_mode(&parent.table_schema)? {
            let children = Self::get_children(parent)?;
            if children.is_empty() {
                return Err(Status::InternalExceptionIndexOutOfRange);
            }
            let chosen = if index < children.len() {
                children[index].clone()
            } else {
                children.last().cloned().unwrap()
            };
            return Ok(chosen);
        }

        let (mut position, children) = parent.pager_accessor.access_page_read(parent, |page| {
            let position = Serializer::read_child(
                index,
                &page.data,
                &parent.position,
                &parent.table_schema,
            )
            .unwrap_or_else(|_| Position::make_empty());
            let children = Serializer::read_children_as_vec(
                &page.data,
                &parent.position,
                &parent.table_schema,
            )?;
            Ok((position, children))
        })?;

        if position.is_empty() {
            if children.is_empty() {
                return Err(Status::InternalExceptionIndexOutOfRange);
            }
            position = if index < children.len() {
                children[index].clone()
            } else {
                children.last().cloned().unwrap()
            };
        }

        Ok(BTreeNode {
            position,
            pager_accessor: parent.pager_accessor.clone(),
            table_schema: parent.table_schema.clone(),
        })
    }

    pub fn set_child(index: usize, parent: &BTreeNode, child: BTreeNode) -> Result<(), Status> {
        if Self::is_large_node_mode(&parent.table_schema)? {
            let blob = Self::read_node_blob(parent)?;
            let (num_keys, mut flag, keys, mut children, rows) =
                Self::decode_node_blob(&parent.table_schema, &blob)?;

            if index >= children.len() {
                return Err(Status::InternalExceptionIndexOutOfRange);
            }

            children[index] = child.position;
            if !children.is_empty() {
                Serializer::write_byte_at_position(&mut flag, 1, false);
            }

            let new_blob = Self::encode_node_blob(
                &parent.table_schema,
                num_keys as usize,
                flag,
                &keys,
                &children,
                &rows,
            )?;
            return Self::write_node_blob(parent, &new_blob);
        }

        let mut page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;

        let mut children_positions =
            Serializer::read_children_as_vec(&page.data, &parent.position, &parent.table_schema)?;
        if index >= children_positions.len() {
            return Err(Status::InternalExceptionIndexOutOfRange);
        }
        children_positions[index] = child.position;

        Serializer::write_children_vec(
            &children_positions,
            &mut page.data,
            &parent.position,
            &parent.table_schema,
        )?;

        parent.pager_accessor.access_page_write(parent, |d| {
            d.data = page.data;
            Ok(())
        })
    }

    pub fn get_children(parent: &BTreeNode) -> Result<Vec<BTreeNode>, Status> {
        if Self::is_large_node_mode(&parent.table_schema)? {
            let blob = Self::read_node_blob(parent)?;
            let (_, _, _, children, _) = Self::decode_node_blob(&parent.table_schema, &blob)?;
            return Ok(children
                .into_iter()
                .map(|position| BTreeNode {
                    position,
                    pager_accessor: parent.pager_accessor.clone(),
                    table_schema: parent.table_schema.clone(),
                })
                .collect());
        }

        let positions = parent.pager_accessor.access_page_read(parent, |page| {
            Serializer::read_children_as_vec(&page.data, &parent.position, &parent.table_schema)
        })?;

        let mut result = vec![];

        for position in positions {
            result.push(BTreeNode {
                position: position,
                pager_accessor: parent.pager_accessor.clone(),
                table_schema: parent.table_schema.clone(),
            })
        }

        Ok(result)
    }

    pub fn set_children(parent: &BTreeNode, children: Vec<BTreeNode>) -> Result<(), Status> {
        Self::set_children_as_positions(
            parent,
            children.iter().map(|c| c.position.clone()).collect(),
        )
    }

    pub fn set_children_as_positions(
        parent: &BTreeNode,
        children: Vec<Position>,
    ) -> Result<(), Status> {
        if Self::is_large_node_mode(&parent.table_schema)? {
            let blob = Self::read_node_blob(parent)?;
            let (num_keys, mut flag, keys, _, rows) =
                Self::decode_node_blob(&parent.table_schema, &blob)?;
            if !children.is_empty() {
                Serializer::write_byte_at_position(&mut flag, 1, false);
            }
            let new_blob = Self::encode_node_blob(
                &parent.table_schema,
                num_keys as usize,
                flag,
                &keys,
                &children,
                &rows,
            )?;
            return Self::write_node_blob(parent, &new_blob);
        }

        let mut page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;

        Serializer::write_children_vec(
            &children,
            &mut page.data,
            &parent.position,
            &parent.table_schema,
        )?;

        parent.pager_accessor.access_page_write(parent, |mut d| {
            d.data = page.data;
            Ok(())
        })
    }

    pub fn get_keys(parent: &BTreeNode) -> Result<(Vec<Key>, Vec<Row>), Status> {
        let (keys, data_encoded) = Self::get_keys_encoded(parent)?;

        let mut data = Vec::with_capacity(data_encoded.len());
        for row in data_encoded {
            data.push(Self::decode_row_from_external_storage(parent, &row)?);
        }
        Ok((keys, data))
    }

    pub fn get_keys_encoded(parent: &BTreeNode) -> Result<(Vec<Key>, Vec<Row>), Status> {
        if Self::is_large_node_mode(&parent.table_schema)? {
            let blob = Self::read_node_blob(parent)?;
            let (_, _, keys, _, rows) = Self::decode_node_blob(&parent.table_schema, &blob)?;
            return Ok((keys, rows));
        }
        parent.pager_accessor.access_page_read(parent, |page| {
            let keys =
                Serializer::read_keys_as_vec(&page.data, &parent.position, &parent.table_schema)?;
            let data_encoded =
                Serializer::read_data_as_vec(&page.data, &parent.position, &parent.table_schema)?;
            Ok((keys, data_encoded))
        })
    }

    pub fn set_keys(parent: &BTreeNode, keys: Vec<Key>, data: Vec<Row>) -> Result<(), Status> {
        let mut encoded_data = Vec::with_capacity(data.len());
        for row in data.iter() {
            encoded_data.push(Self::encode_row_for_external_storage(parent, row)?.0);
        }
        Self::set_keys_encoded(parent, keys, encoded_data)
    }

    pub fn set_keys_encoded(parent: &BTreeNode, keys: Vec<Key>, encoded_data: Vec<Row>) -> Result<(), Status> {
        if Self::is_large_node_mode(&parent.table_schema)? {
            let blob = Self::read_node_blob(parent)?;
            let (_, mut flag, _, mut children, _) =
                Self::decode_node_blob(&parent.table_schema, &blob)?;
            if children.len() > keys.len() + 1 {
                children.truncate(keys.len() + 1);
            }
            if children.is_empty() {
                Serializer::write_byte_at_position(&mut flag, 1, true);
            }
            let new_blob = Self::encode_node_blob(
                &parent.table_schema,
                keys.len(),
                flag,
                &keys,
                &children,
                &encoded_data,
            )?;
            return Self::write_node_blob(parent, &new_blob);
        }

        let mut page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;
        let was_leaf = Serializer::is_leaf(&page.data, &parent.position, &parent.table_schema)?;

        Serializer::write_keys_vec_resize_with_rows(
            &keys,
            &encoded_data,
            &mut page.data,
            &parent.position,
            &parent.table_schema,
        )?;

        if was_leaf {
            Serializer::write_children_vec(
                &vec![],
                &mut page.data,
                &parent.position,
                &parent.table_schema,
            )?;
            Serializer::set_is_leaf(
                &mut page.data,
                &parent.position,
                &parent.table_schema,
                true,
            )?;
        }

        Self::update_node_external_flag(&mut page.data, parent, &encoded_data)?;

        parent.pager_accessor.access_page_write(parent, |d| {
            d.free_space = PAGE_SIZE
                - (parent.table_schema.get_key_and_row_length()? + NODE_METADATA_SIZE) * keys.len();
            d.data = page.data;
            Ok(())
        })?;

        Ok(())
    }

    pub fn get_key(index: usize, parent: &BTreeNode) -> Result<(Key, Row), Status> {
        let (key, data_encoded) = Self::get_key_encoded(index, parent)?;

        let data = Self::decode_row_from_external_storage(parent, &data_encoded)?;
        Ok((key, data))
    }

    pub fn get_key_encoded(index: usize, parent: &BTreeNode) -> Result<(Key, Row), Status> {
        if Self::is_large_node_mode(&parent.table_schema)? {
            let (keys, rows) = Self::get_keys_encoded(parent)?;
            if index >= keys.len() || index >= rows.len() {
                return Err(Status::InternalExceptionIndexOutOfRange);
            }
            return Ok((keys[index].clone(), rows[index].clone()));
        }

        parent.pager_accessor.access_page_read(parent, |page| {
            let key =
                Serializer::read_key(index, &page.data, &parent.position, &parent.table_schema)?;
            let data_encoded = Serializer::read_data_by_index(
                index,
                &page.data,
                &parent.position,
                &parent.table_schema,
            )?;
            Ok((key, data_encoded))
        })
    }

    pub fn set_key(index: usize, parent: &BTreeNode, key: Key, data: Row) -> Result<(), Status> {
        let encoded_data = Self::encode_row_for_external_storage(parent, &data)?.0;
        Self::set_key_encoded(index, parent, key, encoded_data)
    }

    pub fn set_key_encoded(index: usize, parent: &BTreeNode, key: Key, encoded_data: Row) -> Result<(), Status> {
        if Self::is_large_node_mode(&parent.table_schema)? {
            let (mut keys, mut rows) = Self::get_keys_encoded(parent)?;
            if index >= keys.len() || index >= rows.len() {
                return Err(Status::InternalExceptionIndexOutOfRange);
            }
            keys[index] = key;
            rows[index] = encoded_data;
            return Self::set_keys_encoded(parent, keys, rows);
        }

        let mut page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;
        let old_rows =
            Serializer::read_data_as_vec(&page.data, &parent.position, &parent.table_schema)?;
        Serializer::write_key(
            index,
            &mut page.data,
            &parent.position,
            &key,
            &parent.table_schema,
        )
        .unwrap();
        Serializer::write_data_by_index(
            index,
            &mut page.data,
            &parent.position,
            encoded_data,
            &parent.table_schema,
        )
        .unwrap();

        let rows_now =
            Serializer::read_data_as_vec(&page.data, &parent.position, &parent.table_schema)?;
        Self::update_node_external_flag(&mut page.data, parent, &rows_now)?;

        parent.pager_accessor.access_page_write(parent, |d| {
            d.data = page.data;
            Ok(())
        })?;

        Self::deprecate_removed_payload_chains(
            parent.pager_accessor.clone(),
            &parent.table_schema,
            &old_rows,
            &rows_now,
        )?;

        Ok(())
    }

    pub fn get_keys_and_children(parent: &BTreeNode) -> Result<(Vec<Key>, Vec<BTreeNode>), Status> {
        let (keys, positions) = parent.pager_accessor.access_page_read(parent, |page| {
            let keys =
                Serializer::read_keys_as_vec(&page.data, &parent.position, &parent.table_schema)?;

            let positions = Serializer::read_children_as_vec(
                &page.data,
                &parent.position,
                &parent.table_schema,
            )?;
            Ok((keys, positions))
        })?;

        let mut children = vec![];

        for position in positions {
            children.push(BTreeNode {
                position,
                pager_accessor: parent.pager_accessor.clone(),
                table_schema: parent.table_schema.clone(),
            })
        }

        Ok((keys, children))
    }

    pub fn set_keys_and_children(
        parent: &BTreeNode,
        keys: Vec<Key>,
        children: Vec<BTreeNode>,
    ) -> Result<(), Status> {
        Self::set_children_as_positions(
            parent,
            children.iter().map(|c| c.position.clone()).collect(),
        )
    }

    pub fn set_keys_and_children_as_positions(
        parent: &BTreeNode,
        keys: Vec<Key>,
        children: Vec<Position>,
    ) -> Result<(), Status> {
        if Self::is_large_node_mode(&parent.table_schema)? {
            let mut rows = Self::get_data_encoded(parent)?;
            if rows.len() > keys.len() {
                rows.truncate(keys.len());
            }
            while rows.len() < keys.len() {
                rows.push(Serializer::empty_row(&parent.table_schema)?);
            }
            let mut flag = Serializer::create_node_flag(children.is_empty());
            if !children.is_empty() {
                Serializer::write_byte_at_position(&mut flag, 1, false);
            }
            let new_blob = Self::encode_node_blob(
                &parent.table_schema,
                keys.len(),
                flag,
                &keys,
                &children,
                &rows,
            )?;
            return Self::write_node_blob(parent, &new_blob);
        }

        let mut page = parent
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&parent.position))?;
        Serializer::write_keys_vec(
            &keys,
            &mut page.data,
            &parent.position,
            &parent.table_schema,
        )?;
        Serializer::write_children_vec(
            &children,
            &mut page.data,
            &parent.position,
            &parent.table_schema,
        )?;
        parent.pager_accessor.access_page_write(parent, |d| {
            d.free_space = PAGE_SIZE
                - (parent.table_schema.get_key_and_row_length()? + NODE_METADATA_SIZE) * keys.len();
            d.data = page.data;
            Ok(())
        })?;
        Ok(())
    }

    pub fn get_data(node: &BTreeNode) -> Result<Vec<Row>, Status> {
        let encoded = Self::get_data_encoded(node)?;

        let mut decoded = Vec::with_capacity(encoded.len());
        for row in encoded {
            decoded.push(Self::decode_row_from_external_storage(node, &row)?);
        }
        Ok(decoded)
    }

    pub fn get_data_encoded(node: &BTreeNode) -> Result<Vec<Row>, Status> {
        if Self::is_large_node_mode(&node.table_schema)? {
            let blob = Self::read_node_blob(node)?;
            let (_, _, _, _, rows) = Self::decode_node_blob(&node.table_schema, &blob)?;
            return Ok(rows);
        }

        node.pager_accessor.access_page_read(node, |page| {
            Serializer::read_data_as_vec(&page.data, &node.position, &node.table_schema)
        })
    }

    pub fn set_data(node: &BTreeNode, data: Vec<Row>) -> Result<(), Status> {
        let mut encoded_data = Vec::with_capacity(data.len());
        for row in data.iter() {
            encoded_data.push(Self::encode_row_for_external_storage(node, row)?.0);
        }
        Self::set_data_encoded(node, encoded_data)
    }

    pub fn set_data_encoded(node: &BTreeNode, encoded_data: Vec<Row>) -> Result<(), Status> {
        if Self::is_large_node_mode(&node.table_schema)? {
            let blob = Self::read_node_blob(node)?;
            let (num_keys, flag, keys, children, old_rows) =
                Self::decode_node_blob(&node.table_schema, &blob)?;
            if encoded_data.len() != num_keys as usize {
                return Err(Status::InternalExceptionInvalidColCount);
            }
            let new_blob = Self::encode_node_blob(
                &node.table_schema,
                num_keys as usize,
                flag,
                &keys,
                &children,
                &encoded_data,
            )?;
            Self::write_node_blob(node, &new_blob)?;
            Self::deprecate_removed_payload_chains(
                node.pager_accessor.clone(),
                &node.table_schema,
                &old_rows,
                &encoded_data,
            )?;
            return Ok(());
        }

        let mut page = node
            .pager_accessor
            .access_pager_write(|p| p.access_page_read(&node.position))?;
        let old_rows =
            Serializer::read_data_as_vec(&page.data, &node.position, &node.table_schema)?;

        Serializer::write_data_by_vec(&mut page.data, &node.position, &encoded_data, &node.table_schema)?;
        Self::update_node_external_flag(&mut page.data, node, &encoded_data)?;
        node.pager_accessor.access_page_write(node, |d| {
            d.data = page.data;
            Ok(())
        })?;

        Self::deprecate_removed_payload_chains(
            node.pager_accessor.clone(),
            &node.table_schema,
            &old_rows,
            &encoded_data,
        )?;

        Ok(())
    }
}
