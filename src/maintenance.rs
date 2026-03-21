use crate::btree::Btree;
use crate::cursor::BTreeCursor;
use crate::dataframe::RowSource;
use crate::debug::Status;
use crate::executor::{QueryExecutor, QueryResult, MASTER_TABLE_NAME};
use crate::pager::{Key, PageData, Position, Row, Type};
use crate::pager_proxy::PagerProxy;
use crate::planner::{Planner, SqlConditionOpCode};
use crate::schema::TableSchema;
use crate::serializer::Serializer;
use std::collections::HashSet;

impl QueryExecutor {
    pub(crate) fn encode_free_list_top_10(table: &TableSchema) -> String {
        let mut entries = table.free_list.clone();
        entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        entries.truncate(10);
        entries
            .iter()
            .map(|(page, slots)| format!("{}:{}", page, slots))
            .collect::<Vec<String>>()
            .join(",")
    }

    pub(crate) fn index_table_name(base_table: &str, column: &str) -> String {
        format!("_{}_{}", base_table, column)
    }

    pub(crate) fn parse_index_table_name(index_table_name: &str) -> Option<(String, String)> {
        if !index_table_name.starts_with('_') {
            return None;
        }

        let rest = &index_table_name[1..];
        let (base, col) = rest.rsplit_once('_')?;
        if base.is_empty() || col.is_empty() {
            return None;
        }
        Some((base.to_string(), col.to_string()))
    }

    pub(crate) fn find_index_table_id_for_base_column(
        &self,
        base_table: &str,
        column: &str,
    ) -> Option<usize> {
        let index_name = self
            .schema
            .index_definitions
            .iter()
            .find(|idx| idx.base_table == base_table && idx.column_name == column)
            .map(|idx| idx.index_name.clone())?;
        Planner::find_table_id(&self.schema, &index_name).ok()
    }

    pub(crate) fn should_index_field(field_type: &Type) -> bool {
        matches!(
            field_type,
            Type::Integer | Type::String | Type::Varchar(_) | Type::Date
        )
    }

    pub(crate) fn insert_row_into_indices(
        &mut self,
        table_id: usize,
        key: &Key,
        row: &Row,
    ) -> Result<(), QueryResult> {
        if table_id == 0 || table_id >= self.schema.tables.len() {
            return Ok(());
        }

        let base = self.schema.tables[table_id].clone();
        if base.name.starts_with('_') {
            return Ok(());
        }

        let full_row = Serializer::reconstruct_row(key, row, &base).map_err(QueryResult::err)?;

        for (field_idx, field) in base.fields.iter().enumerate() {
            if field_idx == base.key_position || !Self::should_index_field(&field.field_type) {
                continue;
            }

            let index_table_id =
                match self.find_index_table_id_for_base_column(&base.name, &field.name) {
                    Some(id) => id,
                    None => continue,
                };

            let index_schema = self.schema.tables[index_table_id].clone();
            let idx_key =
                Serializer::get_field_on_row(&full_row, field_idx, &base).map_err(QueryResult::err)?;
            let base_pk = Serializer::get_field_on_row(&full_row, base.key_position, &base)
                .map_err(QueryResult::err)?;

            let mut index_btree = Btree::init(
                index_schema.btree_order,
                self.pager_accessor.clone(),
                index_schema,
            )
            .map_err(QueryResult::err)?;
            index_btree
                .insert(idx_key, base_pk)
                .map_err(QueryResult::err)?;
        }

        Ok(())
    }

    pub(crate) fn rebuild_indices_for_table_id(&mut self, table_id: usize) -> Result<(), QueryResult> {
        if table_id == 0 || table_id >= self.schema.tables.len() {
            return Ok(());
        }

        let base = self.schema.tables[table_id].clone();
        if base.name.starts_with('_') {
            return Ok(());
        }

        let base_key_pos = base.key_position;

        for (field_idx, field) in base.fields.iter().enumerate() {
            if field_idx == base_key_pos || !Self::should_index_field(&field.field_type) {
                continue;
            }

            let index_table_id =
                match self.find_index_table_id_for_base_column(&base.name, &field.name) {
                    Some(id) => id,
                    None => continue,
                };

            let index_schema = self.schema.tables[index_table_id].clone();
            PagerProxy::clear_table_root(&index_schema, self.pager_accessor.clone())
                .map_err(QueryResult::err)?;

            let mut base_source = self
                .create_scan_source(table_id, SqlConditionOpCode::SelectFTS, None)
                .map_err(QueryResult::err)?;

            let mut index_btree = Btree::init(
                index_schema.btree_order,
                self.pager_accessor.clone(),
                index_schema.clone(),
            )
            .map_err(QueryResult::err)?;

            base_source.reset().map_err(QueryResult::err)?;
            while let Some(base_row) = base_source.next().map_err(QueryResult::err)? {
                let idx_key =
                    Serializer::get_field_on_row(&base_row, field_idx, &base).map_err(QueryResult::err)?;
                let base_pk = Serializer::get_field_on_row(&base_row, base_key_pos, &base)
                    .map_err(QueryResult::err)?;
                index_btree
                    .insert(idx_key, base_pk)
                    .map_err(QueryResult::err)?;
            }
        }

        Ok(())
    }

    fn count_nodes_on_page(&self, schema: &TableSchema, page_data: PageData) -> Result<usize, Status> {
        let mut effective_schema = schema.clone();
        if effective_schema.btree_order == 0 {
            effective_schema.btree_order = self.btree_node_width;
        }

        let has_varchar = schema
            .fields
            .iter()
            .any(|f| matches!(f.field_type, Type::Varchar(_)));
        if has_varchar && effective_schema.get_node_size_in_bytes()? > crate::pager::PAGE_SIZE {
            return Ok(if page_data[0] == 0 && page_data[1] == 0 {
                0
            } else {
                1
            });
        }

        let mut count = 0usize;
        let mut offset = 0usize;
        let key_length = schema.get_key_length()?;
        let row_length = schema.get_row_length()?;

        while offset + 2 <= crate::pager::PAGE_SIZE {
            let num_keys = page_data[offset] as usize;
            let flag = page_data[offset + 1];

            if num_keys == 0 && flag == 0 {
                break;
            }

            let node_size = 2 + num_keys * (key_length + row_length) + (num_keys + 1) * 4;
            if offset + node_size > crate::pager::PAGE_SIZE {
                return Err(Status::InternalExceptionIndexOutOfRange);
            }

            count += 1;
            offset += node_size;
        }

        Ok(count)
    }

    pub(crate) fn collect_btree_pages(
        &self,
        node: &crate::btree::BTreeNode,
        pages: &mut HashSet<usize>,
    ) -> Result<(), Status> {
        if !pages.insert(node.position.page()) {
            return Ok(());
        }
        for child in PagerProxy::get_children(node)? {
            self.collect_btree_pages(&child, pages)?;
        }
        Ok(())
    }

    pub(crate) fn mark_pages_as_deleted(&self, pages: &HashSet<usize>) -> Result<(), Status> {
        for page in pages {
            let pos = Position::new(*page, 0);
            self.pager_accessor.access_pager_write(|p| {
                p.with_page_write(&pos, |page_container| {
                    Serializer::set_is_deleted(page_container, true)
                })
            })?;
        }
        Ok(())
    }

    pub(crate) fn refresh_table_free_list(&mut self, table_id: usize) -> Result<(), Status> {
        if table_id >= self.schema.tables.len() {
            return Ok(());
        }
        let table_schema = self.schema.tables[table_id].clone();
        let btree = Btree::init(
            table_schema.btree_order,
            self.pager_accessor.clone(),
            table_schema.clone(),
        )?;
        let root = btree.root.ok_or(Status::InternalExceptionNoRoot)?;

        let mut pages = HashSet::new();
        self.collect_btree_pages(&root, &mut pages)?;

        let capacity = table_schema.max_nodes_per_page()?;
        let mut free_list = Vec::new();

        for page in pages {
            let page_data = self
                .pager_accessor
                .access_pager_write(|p| p.access_page_read(&Position::new(page, 0)))?;
            let used = self.count_nodes_on_page(&table_schema, page_data.data)?;
            let free = capacity.saturating_sub(used);
            free_list.push((page, free));
        }

        free_list.sort_by_key(|(page, _)| *page);
        self.schema.tables[table_id].free_list = free_list;
        Ok(())
    }

    pub(crate) fn refresh_all_free_lists(&mut self) {
        for table_id in 1..self.schema.tables.len() {
            if let Err(e) = self.refresh_table_free_list(table_id) {
                eprintln!(
                    "Failed to refresh free-list for table {}: {:?}",
                    table_id, e
                );
            }
        }
    }

    pub(crate) fn persist_free_lists_to_system_table(&mut self) -> Result<(), QueryResult> {
        let tables_to_persist: Vec<TableSchema> =
            self.schema.tables.iter().skip(1).cloned().collect();
        for table in tables_to_persist {
            let update_query = format!(
                "UPDATE {} SET free_list = '{}' WHERE name = '{}'",
                MASTER_TABLE_NAME,
                Self::encode_free_list_top_10(&table).replace("'", "''"),
                table.name.replace("'", "''")
            );
            self.run_query_internal(&update_query, true)?;
        }

        Ok(())
    }
}
