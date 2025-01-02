#[derive(Debug)]
pub struct ParsedInsertQuery {
    pub table_name: String,
    pub fields: Vec<String>,
    pub values: Vec<String>,
}

//for the single Table DB:
#[derive(Debug)]
pub struct ParsedSelectQuery {
    pub table_name: String,
    pub result: Vec<String>,

    //conditions can be expressed like this:
    //SELECT [] FROM table WHERE a = "xx" AND ... AND z > 34
    pub conditions: Vec<(String, String, String)>
}

#[derive(Debug)]
pub struct ParsedDropQuery {
    pub table_name: String
}

#[derive(Debug)]
pub struct ParsedCreateTableQuery {
    pub table_name: String,
    pub table_fields: Vec<String>,
    pub table_types: Vec<String>
}

#[derive(Debug)]
pub struct ParsedDeleteQuery {
    pub table_name: String,
    pub conditions: Vec<(String, String, String)>,
}

#[derive(Debug)]
pub enum ParsedQuery {
    CreateTable(ParsedCreateTableQuery),
    DropTable(ParsedDropQuery),
    Select(ParsedSelectQuery),
    Insert(ParsedInsertQuery),
    Delete(ParsedDeleteQuery)
}

pub struct Lexer {
    input: String,
    position: usize,
}

impl Lexer {
    pub fn new(input: String) -> Self {
        Self {
            input,
            position: 0,
        }
    }

    pub fn next_token(&mut self) -> Option<String> {
        self.skip_whitespace();
        if self.position >= self.input.len() {
            return None;
        }

        let current_char = self.input.chars().nth(self.position)?;

        if current_char == '(' || current_char == ')' || current_char == ',' {
            self.position += 1;
            return Some(current_char.to_string());
        }

        if current_char == '\'' {
            return self.read_quoted_token();
        }

        let start = self.position;
        while self.position < self.input.len() {
            let c = self.input.chars().nth(self.position).unwrap();
            if c.is_whitespace() || c == '(' || c == ')' || c == ',' {
                break;
            }
            self.position += 1;
        }

        Some(self.input[start..self.position].to_string())
    }

    fn read_quoted_token(&mut self) -> Option<String> {
        self.position += 1;
        let start = self.position;

        while self.position < self.input.len() {
            let c = self.input.chars().nth(self.position).unwrap();
            if c == '\'' {
                let token = &self.input[start..self.position];
                self.position += 1;
                return Some(token.to_string());
            }
            self.position += 1;
        }

        None
    }

    fn skip_whitespace(&mut self) {
        while self.position < self.input.len() && self.input[self.position..=self.position].chars().all(|c| c.is_whitespace()) {
            self.position += 1;
        }
    }
}


pub struct Parser {
    lexer: Lexer,
}

impl Parser {
    pub fn new(query: String) -> Self {
        Self {
            lexer: Lexer::new(query),
        }
    }

    pub fn parse_query(&mut self) -> Result<ParsedQuery, String> {
        let statement_type = self
            .lexer
            .next_token()
            .ok_or_else(|| "Expected a query type".to_string())?;
        match statement_type.to_uppercase().as_str() {
            "CREATE" => self.parse_create_table(),
            "DROP" => self.parse_drop_table(),
            "SELECT" => self.parse_select(),
            "INSERT" => self.parse_insert(),
            "DELETE" => self.parse_delete(),
            _ => Err(format!("Unknown statement type: {}", statement_type)),
        }
    }

    fn parse_create_table(&mut self) -> Result<ParsedQuery, String> {
        self.expect_token("TABLE")?;
        let table_name = self.lexer.next_token().ok_or_else(|| "Expected table name".to_string())?;
        self.expect_token("(")?;
        let mut fields = Vec::new();
        let mut types = Vec::new();
        loop {
            let field_name = self
                .lexer
                .next_token()
                .ok_or_else(|| "Expected field name or closing ')'".to_string())?;

            if field_name == ")" {
                return Err("Invalid name: )".to_string());
            }

            let field_type = self
                .lexer
                .next_token()
                .ok_or_else(|| "Expected field type".to_string())?;

            fields.push(field_name);

            if field_type == ")" {
                return Err("Invalid type: )".to_string());
            }

            types.push(field_type);

            match self.lexer.next_token().as_deref() {
                Some(",") => continue,
                Some(")") => break,
                _ => return Err("Expected ',' or ')' in field definition".to_string()),
            }
        }

        Ok(ParsedQuery::CreateTable(ParsedCreateTableQuery {
            table_name,
            table_fields: fields,
            table_types: types
        }))
    }


    fn parse_drop_table(&mut self) -> Result<ParsedQuery, String> {
        self.expect_token("TABLE")?;
        let table_name = self
            .lexer
            .next_token()
            .ok_or_else(|| "Expected table name".to_string())?;
        Ok(ParsedQuery::DropTable(ParsedDropQuery { table_name }))
    }

    fn parse_select(&mut self) -> Result<ParsedQuery, String> {
        let mut fields = Vec::new();
        loop {
            let token = self.lexer.next_token().ok_or_else(|| "Expected field or FROM".to_string())?;
            if token.to_uppercase() == "FROM" {
                break;
            }
            fields.push(token);

            let next_token = self.lexer.next_token();
            if let Some(",") = next_token.clone().as_deref() {
                continue;
            } else if let Some("FROM") = next_token.clone().as_deref() {
                break;
            } else {
                return Err(format!("Expected 'FROM', but found '{}'", next_token.expect("Expected 'FROM'")));
            }
        }

        let table_name = self
            .lexer
            .next_token()
            .ok_or_else(|| "Expected table name".to_string())?;

        let mut conditions = Vec::new();
        self.parse_where_conditions(&mut conditions)?;

        Ok(ParsedQuery::Select(ParsedSelectQuery {
            table_name,
            result: fields,
            conditions,
        }))
    }

    fn parse_insert(&mut self) -> Result<ParsedQuery, String> {
        self.expect_token("INTO")?;
        let table_name = self
            .lexer
            .next_token()
            .ok_or_else(|| "Expected table name".to_string())?;

        if table_name == "(" {
            return Err("Expected table name".to_string())
        }

        self.expect_token("(")?;
        let mut fields = Vec::new();
        loop {
            let field_name = self
                .lexer
                .next_token()
                .ok_or_else(|| "Expected field name".to_string())?;
            if field_name == ")" {
                return Err("Expected field name".to_string());
            }
            fields.push(field_name);

            match self.lexer.next_token().as_deref() {
                Some(",") => continue,
                Some(")") => break,
                _ => return Err("Expected ',' or ')' after field name".to_string()),
            }
        }

        self.expect_token("VALUES")?;
        self.expect_token("(")?;
        let mut values = Vec::new();
        loop {
            let value = self
                .lexer
                .next_token()
                .ok_or_else(|| "Expected value".to_string())?;
            if value == ")" {
                return Err("Expected value".to_string());
            }
            values.push(value);

            match self.lexer.next_token().as_deref() {
                Some(",") => continue,
                Some(")") => break,
                _ => return Err("Expected ',' or ')' after value".to_string()),
            }
        }

        if fields.len() != values.len() {
            return Err(format!(
                "Mismatched fields and values count: {} fields, {} values",
                fields.len(),
                values.len()
            ));
        }

        Ok(ParsedQuery::Insert(ParsedInsertQuery {
            table_name,
            fields,
            values,
        }))
    }

    pub fn parse_delete(&mut self) -> Result<ParsedQuery, String> {
        self.expect_token("FROM")?;
        let table_name = self
            .lexer
            .next_token()
            .ok_or_else(|| "Expected table name".to_string())?;

        let mut conditions = Vec::new();
        self.parse_where_conditions(&mut conditions)?;

        Ok(ParsedQuery::Delete(ParsedDeleteQuery {
            table_name,
            conditions,
        }))
    }

    fn parse_where_conditions(&mut self, conditions: &mut Vec<(String, String, String)>) -> Result<(), String> {
        if let Some(token) = self.lexer.next_token() {
            if token.to_uppercase() == "WHERE" {
                loop {
                    let field_name = self
                        .lexer
                        .next_token()
                        .ok_or_else(|| "Expected field name in condition".to_string())?;
                    let operator = self.lexer.next_token().ok_or_else(|| "Expected comparison operator".to_string())?;
                    let value = self.lexer.next_token().ok_or_else(|| "Expected value in condition".to_string())?;
                    conditions.push((field_name, operator, value));

                    match self.lexer.next_token().as_deref() {
                        Some("AND") => continue,
                        None => break,
                        _ => return Err("Expected 'AND' or end of conditions".to_string()),
                    }
                }
            } else {
                return Err("Expected 'WHERE' or end of query".to_string());
            }
        }
        Ok(())
    }

    fn expect_token(&mut self, expected: &str) -> Result<(), String> {
        let token = self.lexer.next_token().ok_or_else(|| {
            format!("Expected '{}', but reached end of input", expected)
        })?;
        if token.to_uppercase() != expected.to_uppercase() {
            return Err(format!("Expected '{}', but found '{}'", expected, token));
        }
        Ok(())
    }
}