use crate::btree::Btree;
use crate::dataframe::DataFrame;
use crate::executor::{QueryExecutor, QueryResult};
use crate::pager::Position;
use crate::planner::{CompiledQuery, PlanNode, Planner};
use crate::serializer::Serializer;
use std::cmp::PartialEq;
use std::fmt::{Debug, Display, Formatter};

#[derive(PartialEq, Debug, Clone)]
pub enum Status {
    //status codes to be sent to the end user
    Error,
    Success,
    ExceptionSchemaUnclear,
    ExceptionFileNotFoundOrPermissionDenied,
    ExceptionQueryMisformed,

    //internal status codes
    CacheMiss,
    InternalSuccess,
    InternalExceptionTypeMismatch,
    InternalExceptionIndexOutOfRange,
    InternalExceptionFileNotFound,
    InternalExceptionReadFailed,
    InternalExceptionWriteFailed,
    InternalExceptionInvalidFieldType,
    InternalExceptionInvalidSchema,
    InternalExceptionInvalidFieldName,
    InternalExceptionInvalidFieldValue,
    InternalExceptionKeyNotFound,
    InternalExceptionInvalidRowLength,
    InternalExceptionInvalidColCount,
    InternalExceptionPagerMismatch,
    InternalExceptionNoRoot,
    InternalExceptionCacheDenied,
    InternalExceptionPageCorrupted,
    CannotParseDate,
    CannotParseInteger,
    CannotParseBoolean,
    CannotParseIllegalDate,
    InternalExceptionPagerWriteLock,
    InternalExceptionCompilerError,
    InternalExceptionIntegrityCheckFailed,
    InternalExceptionFileWriteError,
    InternalExceptionFileAlreadyExists,
    InternalExceptionFileOpenFailed,
    ExceptionTableAlreadyExists,
    InternalExceptionDBCreationFailed,
    DataFrameJoinError,
    NotImplemented,
    CursorError,
    InternalError,
}

impl Display for Btree {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(ref root) = self.root {
            let mut queue = std::collections::VecDeque::new();
            let schema = &self.table_schema;
            queue.push_back(root.clone());

            writeln!(f, "Btree Level-Order Traversal:")?;
            while !queue.is_empty() {
                let level_size = queue.len();
                let mut level = Vec::new();

                for _ in 0..level_size {
                    if let Some(node) = queue.pop_front() {
                        let keys = node.get_keys().unwrap();
                        let children = node.get_children().unwrap();
                        level.push((keys.0, keys.1, children.clone()));

                        if !node.is_leaf() {
                            for child in children {
                                queue.push_back(child);
                            }
                        }
                    }
                }
                for (keys, rows, children) in level {
                    write!(f, "{{")?;
                    for (key, row) in keys.iter().zip(rows.iter()) {
                        write!(f, "[")?;
                        if Serializer::is_tomb(key, &schema).unwrap() {
                            write!(f, "X")?;
                        }
                        write!(f, "{}", Serializer::format_key(key, &schema).unwrap())?;
                        write!(f, " :: ")?;
                        write!(f, "{}", Serializer::format_row(row, &schema).unwrap())?;
                        write!(f, "]")?;
                    }
                    write!(f, "[")?;
                    for (child) in children {
                        write!(f, "{:?}", child.position)?;
                        write!(f, " , ")?;
                    }
                    write!(f, "]")?;
                    write!(f, "}}")?;
                }
                writeln!(f, "")?;
            }
        } else {
            writeln!(f, "Tree is empty")?;
        }
        Ok(())
    }
}

impl Planner {
    pub fn render_plan_node(node: &PlanNode, prefix: &str, is_last: bool, out: &mut String) {
        let branch = if is_last { "└─" } else { "├─" };
        let next_prefix = if is_last {
            format!("{}  ", prefix)
        } else {
            format!("{}│ ", prefix)
        };

        match node {
            PlanNode::SeqScan {
                table_id,
                table_name,
                operation,
                conditions,
                index_table_id,
                index_on_column,
            } => {
                out.push_str(&format!(
                    "{}{} SeqScan table='{}' id={} op={:?} index_table_id={:?} index_col={:?}\n",
                    prefix, branch, table_name, table_id, operation, index_table_id, index_on_column
                ));
                if conditions.is_empty() {
                    out.push_str(&format!("{}  conditions: []\n", next_prefix));
                } else {
                    for (idx, (op, value)) in conditions.iter().enumerate() {
                        out.push_str(&format!(
                            "{}  cond[{}]: {} {}\n",
                            next_prefix,
                            idx,
                            Serializer::format_condition_op(op),
                            Serializer::format_value_preview(value)
                        ));
                    }
                }
            }
            PlanNode::Filter { source, conditions } => {
                out.push_str(&format!("{}{} Filter\n", prefix, branch));
                if conditions.is_empty() {
                    out.push_str(&format!("{}  conditions: []\n", next_prefix));
                } else {
                    for (idx, (op, value)) in conditions.iter().enumerate() {
                        out.push_str(&format!(
                            "{}  cond[{}]: {} {}\n",
                            next_prefix,
                            idx,
                            Serializer::format_condition_op(op),
                            Serializer::format_value_preview(value)
                        ));
                    }
                }
                Self::render_plan_node(source, &next_prefix, true, out);
            }
            PlanNode::Project { source, fields } => {
                out.push_str(&format!("{}{} Project\n", prefix, branch));
                let fields_repr = fields
                    .iter()
                    .map(|f| format!("{}.{}", f.table_name, f.name))
                    .collect::<Vec<String>>()
                    .join(", ");
                out.push_str(&format!("{}  fields: [{}]\n", next_prefix, fields_repr));
                Self::render_plan_node(source, &next_prefix, true, out);
            }
            PlanNode::Join {
                left,
                right,
                join_type,
                left_join_op,
                right_join_op,
                conditions,
            } => {
                out.push_str(&format!(
                    "{}{} Join type={:?} left_op={:?} right_op={:?}\n",
                    prefix, branch, join_type, left_join_op, right_join_op
                ));
                for (idx, (l, r)) in conditions.iter().enumerate() {
                    out.push_str(&format!(
                        "{}  on[{}]: {}.{} = {}.{}\n",
                        next_prefix, idx, l.table_name, l.name, r.table_name, r.name
                    ));
                }
                Self::render_plan_node(left, &next_prefix, false, out);
                Self::render_plan_node(right, &next_prefix, true, out);
            }
            PlanNode::SetOperation { op, left, right } => {
                out.push_str(&format!("{}{} SetOperation {:?}\n", prefix, branch, op));
                Self::render_plan_node(left, &next_prefix, false, out);
                Self::render_plan_node(right, &next_prefix, true, out);
            }
        }
    }

    pub fn render_compiled_query(compiled: &CompiledQuery) -> String {
        match compiled {
            CompiledQuery::Select(q) => {
                let mut out = String::from("CompiledQuery::Select\n");
                Self::render_plan_node(&q.plan, "", true, &mut out);
                out
            }
            CompiledQuery::Insert(q) => format!(
                "CompiledQuery::Insert\n└─ table_id={} key={} bytes row={} bytes",
                q.table_id,
                q.data.0.len(),
                q.data.1.len()
            ),
            CompiledQuery::Delete(q) => {
                let mut out = format!(
                    "CompiledQuery::Delete\n└─ table_id={} op={:?}\n",
                    q.table_id, q.operation
                );
                for (idx, (op, value)) in q.conditions.iter().enumerate() {
                    out.push_str(&format!(
                        "   cond[{}]: {} {}\n",
                        idx,
                        Serializer::format_condition_op(op),
                        Serializer::format_value_preview(value)
                    ));
                }
                out
            }
            CompiledQuery::Update(q) => {
                let mut out = format!(
                    "CompiledQuery::Update\n└─ table_id={} op={:?}\n",
                    q.table_id, q.operation
                );
                for (idx, (op, value)) in q.conditions.iter().enumerate() {
                    out.push_str(&format!(
                        "   cond[{}]: {} {}\n",
                        idx,
                        Serializer::format_condition_op(op),
                        Serializer::format_value_preview(value)
                    ));
                }
                for (idx, (field_idx, value)) in q.assignments.iter().enumerate() {
                    out.push_str(&format!(
                        "   set[{}]: field[{}] = {}\n",
                        idx,
                        field_idx,
                        Serializer::format_value_preview(value)
                    ));
                }
                out
            }
            CompiledQuery::CreateTable(q) => format!(
                "CompiledQuery::CreateTable\n└─ table='{}' fields={}",
                q.table_name,
                q.schema.fields.len()
            ),
            CompiledQuery::CreateIndex(q) => format!(
                "CompiledQuery::CreateIndex\n└─ index='{}' base='{}' column='{}' table='{}'",
                q.index_name,
                q.base_table_name,
                q.column_name,
                q.schema.name
            ),
            CompiledQuery::DropTable(q) => {
                format!("CompiledQuery::DropTable\n└─ table_id={}", q.table_id)
            }
            CompiledQuery::DropIndex(q) => {
                format!("CompiledQuery::DropIndex\n└─ table_id={}", q.table_id)
            }
        }
    }
}

impl QueryExecutor {
    pub fn debug_readonly(&self, table: Option<&str>) {
        if table.is_none() {
            println!(
                "System Table: {}",
                Btree::init(
                    self.schema.tables[0].btree_order,
                    self.pager_accessor.clone(),
                    self.schema.tables[0].clone()
                )
                .unwrap()
            );
        } else {
            let table_name = table.unwrap();
            let table_id = Planner::find_table_id(&self.schema, table_name).unwrap();
            let table_schema = self.schema.tables[table_id].clone();
            let btree = Btree::init(
                table_schema.btree_order,
                self.pager_accessor.clone(),
                table_schema.clone(),
            )
            .unwrap();
            println!("Table: {}", btree);
        }
    }

    pub fn debug(&mut self, table: Option<&str>) {
        if table.is_none() {
            println!("Cached Schema: {:?}", self.schema);
            println!(
                "System Table: {}",
                Btree::init(
                    self.schema.tables[0].btree_order,
                    self.pager_accessor.clone(),
                    self.schema.tables[0].clone()
                )
                .unwrap()
            );
        } else {
            let table_name = table.unwrap();
            let table_id = Planner::find_table_id(&self.schema, table_name).expect("Invalid Tablename");
            let table_schema = self.schema.tables[table_id].clone();
            let btree = Btree::init(
                table_schema.btree_order,
                self.pager_accessor.clone(),
                table_schema.clone(),
            )
            .unwrap();
            println!("Table: {}", btree);
        }
        println!(
            "Checking Integrity... Is {}",
            self.check_integrity().is_ok()
        );
        println!(
            "System Table Data: \n {}",
            self.prepare("SELECT * FROM rustsql_master".to_string())
        );
    }

    pub fn debug_pager(&self) {
        let next_page = self.pager_accessor.get_next_page_index();
        println!(
            "Pager Status: next_page_index={}, total_pages={}",
            next_page,
            next_page.saturating_sub(1)
        );

        for page_idx in 1..next_page {
            let pos = Position::new(page_idx, 0);
            match self
                .pager_accessor
                .access_pager_write(|p| p.access_page_read(&pos))
            {
                Ok(page) => {
                    let dirty = Serializer::byte_to_bool_at_position(page.flag, 0);
                    let deleted = Serializer::is_deleted(&page).unwrap_or(false);
                    let data_page = Serializer::is_data_page(&page).unwrap_or(false);
                    let overflow_page = Serializer::is_overflow_page(&page).unwrap_or(false);
                    let used_bytes = page.data.iter().filter(|b| **b != 0).count();
                    let page_kind = if data_page {
                        "data"
                    } else if overflow_page {
                        "overflow"
                    } else {
                        "node"
                    };

                    println!(
                        "page={:>4} kind={:<8} free_space={:>4} flag=0b{:08b} dirty={} deleted={} non_zero_bytes={}",
                        page_idx,
                        page_kind,
                        page.free_space,
                        page.flag,
                        dirty,
                        deleted,
                        used_bytes
                    );
                }
                Err(err) => {
                    println!("page={:>4} ERROR: {:?}", page_idx, err);
                }
            }
        }
    }

    pub fn plan(&self, query: String) -> QueryResult {
        let compiled_query = match self.compile_query(&query) {
            Ok(c) => c,
            Err(e) => return e,
        };

        let rendered = Planner::render_compiled_query(&compiled_query);
        println!("{}", rendered);
        QueryResult::return_data(DataFrame::msg("Query planned successfully. See debug output for details."))
    }
}