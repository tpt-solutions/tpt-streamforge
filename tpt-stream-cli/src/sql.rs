//! `tptforge sql "SELECT ..."` — a SQL frontend lowering onto pipeline stages.
//!
//! Supported subset (single table):
//!
//! ```sql
//! SELECT projections
//! FROM 'file.csv'            -- csv/jsonl/json/tptcol by extension, .gz ok
//! [WHERE <comparison expr>]  -- identifiers, literals, AND/OR/NOT, =,==,<>,!=,<,<=,>,>=, + - * / %
//! [GROUP BY col, ...]
//! [ORDER BY col [ASC|DESC], ...]
//! [LIMIT n]
//! ```
//!
//! Aggregates map onto the engine's naming: `SUM(x)` -> `sum_x`,
//! `COUNT(*)` -> `count_all`, `COUNT(x)` -> `count_x`, etc. Aliases rename
//! the output columns. Unhandled SQL raises a clear error, never silently
//! wrong results.
//!
//! This is a small hand-rolled tokenizer + recursive-descent parser (not a
//! general-purpose SQL grammar) so the CLI doesn't need an Apache-2.0-only
//! dependency (`sqlparser` has no MIT option) for a deliberately tiny subset.

use anyhow::{bail, Context, Result};
use tpt_stream_core::agg::AggSpec;
use tpt_stream_core::Pipeline;

const KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "GROUP",
    "BY",
    "ORDER",
    "ASC",
    "DESC",
    "LIMIT",
    "AS",
    "AND",
    "OR",
    "NOT",
    "NULL",
    "TRUE",
    "FALSE",
    "IS",
    "JOIN",
    "DISTINCT",
    "WITH",
    "UNION",
    "INTERSECT",
    "EXCEPT",
    "BETWEEN",
    "IN",
    "LIKE",
];

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Keyword(String),
    Ident(String),
    Number(String),
    Str(String),
    Op(String),
}

/// Turn SQL source text into a flat token stream.
fn lex(sql: &str) -> Result<Vec<Tok>> {
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;
    let mut out = Vec::new();
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if c == '\'' || c == '"' {
            let quote = c;
            i += 1;
            let mut s = String::new();
            loop {
                if i >= chars.len() {
                    bail!("unterminated string literal");
                }
                if chars[i] == quote {
                    if i + 1 < chars.len() && chars[i + 1] == quote {
                        s.push(quote);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                s.push(chars[i]);
                i += 1;
            }
            out.push(Tok::Str(s));
            continue;
        }
        if c.is_ascii_digit() {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                i += 1;
            }
            out.push(Tok::Number(chars[start..i].iter().collect()));
            continue;
        }
        if c.is_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            let upper = word.to_ascii_uppercase();
            if KEYWORDS.contains(&upper.as_str()) {
                out.push(Tok::Keyword(upper));
            } else {
                out.push(Tok::Ident(word));
            }
            continue;
        }
        let two: String = chars[i..(i + 2).min(chars.len())].iter().collect();
        if ["==", "<>", "!=", "<=", ">="].contains(&two.as_str()) {
            out.push(Tok::Op(two));
            i += 2;
            continue;
        }
        if "()=<>+-*/%,.".contains(c) {
            out.push(Tok::Op(c.to_string()));
            i += 1;
            continue;
        }
        bail!("unexpected character {c:?} in SQL");
    }
    Ok(out)
}

#[derive(Debug, Clone, Copy)]
enum BinOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Mul,
    Div,
    Mod,
    And,
    Or,
}

#[derive(Debug, Clone, Copy)]
enum UnOp {
    Not,
    Minus,
    Plus,
}

#[derive(Debug, Clone)]
enum FuncArg {
    Star,
    Column(String),
}

#[derive(Debug, Clone)]
enum Expr {
    Ident(String),
    Number(String),
    Str(String),
    Bool(bool),
    Null,
    BinaryOp(Box<Expr>, BinOp, Box<Expr>),
    UnaryOp(UnOp, Box<Expr>),
    Nested(Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    IsTrue(Box<Expr>),
    IsFalse(Box<Expr>),
    Func(String, FuncArg),
}

#[derive(Debug, Clone)]
enum ProjectionItem {
    Wildcard,
    Named(Expr, String),
    Unnamed(Expr),
}

const CMP_OPS: &[&str] = &["=", "==", "<>", "!=", "<", "<=", ">", ">="];

struct SqlParser {
    tokens: Vec<Tok>,
    pos: usize,
}

impl SqlParser {
    fn peek(&self) -> Option<&Tok> {
        self.tokens.get(self.pos)
    }

    fn eof(&self) -> bool {
        self.pos >= self.tokens.len()
    }

    fn advance(&mut self) {
        self.pos += 1;
    }

    fn advance_owned(&mut self) -> Result<Tok> {
        let t = self
            .tokens
            .get(self.pos)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unexpected end of SQL"))?;
        self.pos += 1;
        Ok(t)
    }

    fn peek_keyword(&self, kw: &str) -> bool {
        matches!(self.peek(), Some(Tok::Keyword(k)) if k == kw)
    }

    fn peek_op(&self, op: &str) -> bool {
        matches!(self.peek(), Some(Tok::Op(o)) if o == op)
    }

    fn peek_op_any(&self, ops: &[&str]) -> Option<String> {
        if let Some(Tok::Op(o)) = self.peek() {
            if ops.contains(&o.as_str()) {
                return Some(o.clone());
            }
        }
        None
    }

    fn expect_keyword(&mut self, kw: &str) -> Result<()> {
        if self.peek_keyword(kw) {
            self.advance();
            Ok(())
        } else {
            bail!("expected {kw}, found {:?}", self.peek())
        }
    }

    fn expect_ident(&mut self) -> Result<String> {
        match self.advance_owned()? {
            Tok::Ident(s) => Ok(s),
            other => bail!("expected an identifier, found {other:?}"),
        }
    }

    fn parse_pipeline(&mut self) -> Result<Pipeline> {
        if self.peek_keyword("WITH") {
            bail!("CTEs (WITH) are not supported");
        }
        self.expect_keyword("SELECT")?;
        if self.peek_keyword("DISTINCT") {
            bail!("SELECT DISTINCT is not supported");
        }
        let projection = self.parse_projection_list()?;
        self.expect_keyword("FROM")
            .context("query needs a FROM clause")?;
        let path = self.parse_table()?;
        if self.peek_keyword("JOIN") {
            bail!("SQL JOIN is not supported (use the join stage in YAML)");
        }
        let selection = if self.peek_keyword("WHERE") {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };
        let group_by = if self.peek_keyword("GROUP") {
            self.advance();
            self.expect_keyword("BY")?;
            self.parse_ident_list()?
        } else {
            Vec::new()
        };
        let order_by = if self.peek_keyword("ORDER") {
            self.advance();
            self.expect_keyword("BY")?;
            self.parse_order_list()?
        } else {
            Vec::new()
        };
        let limit = if self.peek_keyword("LIMIT") {
            self.advance();
            Some(self.parse_number_literal()?)
        } else {
            None
        };
        if self.peek_keyword("UNION") || self.peek_keyword("INTERSECT") || self.peek_keyword("EXCEPT")
        {
            bail!("UNION/INTERSECT/EXCEPT are not supported");
        }
        if !self.eof() {
            bail!("unsupported trailing SQL near {:?}", self.peek());
        }

        let mut pipeline = Pipeline::new();

        if let Some(expr) = &selection {
            let text = convert_expr(expr)?;
            pipeline.filter_expr(&text);
        }

        let aggregate_query = !group_by.is_empty() || projection_has_aggregate(&projection);

        if aggregate_query {
            // Outputs in projection order: [group columns...] + [aggregates...].
            let mut aggs: Vec<AggSpec> = Vec::new();
            let mut agg_outputs: Vec<(String, String)> = Vec::new(); // (generated, alias)
            for item in &projection {
                match item {
                    ProjectionItem::Wildcard => {
                        bail!("wildcards with aggregates are not supported");
                    }
                    ProjectionItem::Named(expr, alias) => {
                        push_agg(expr, alias.clone(), &group_by, &mut aggs, &mut agg_outputs)?;
                    }
                    ProjectionItem::Unnamed(expr) => {
                        let alias = default_alias(expr)?;
                        push_agg(expr, alias, &group_by, &mut aggs, &mut agg_outputs)?;
                    }
                }
            }
            if aggs.is_empty() {
                // GROUP BY without aggregates: distinct keys, keep first.
                let refs: Vec<&str> = group_by.iter().map(|s| s.as_str()).collect();
                pipeline.select(&refs);
                pipeline.dedup(&refs);
            } else {
                let group_refs: Vec<&str> = group_by.iter().map(|s| s.as_str()).collect();
                pipeline.aggregate(&group_refs, &aggs);
                // map_expr replaces the whole schema; agg_outputs already carries
                // group columns projected in the SELECT list, in order.
                let pairs: Vec<(&str, &str)> = agg_outputs
                    .iter()
                    .map(|(generated, alias)| (alias.as_str(), generated.as_str()))
                    .collect();
                pipeline.map_expr(&pairs);
            }
        } else {
            // Plain projection: a final map of (output column, expression).
            // Computed expressions (`amount * 2 AS doubled`) run through the map
            // stage; plain columns are identity entries. `*` keeps everything.
            let mut wildcard = false;
            let mut final_map: Vec<(String, String)> = Vec::new();
            let mut needed: Vec<String> = Vec::new();
            for item in &projection {
                match item {
                    ProjectionItem::Wildcard => wildcard = true,
                    ProjectionItem::Named(expr, alias) => {
                        let text = convert_expr(expr)?;
                        needed.extend(collect_identifiers(expr));
                        final_map.push((alias.clone(), text));
                    }
                    ProjectionItem::Unnamed(expr) => match expr {
                        Expr::Ident(col) => {
                            needed.push(col.clone());
                            final_map.push((col.clone(), col.clone()));
                        }
                        other => {
                            bail!("computed projection {other:?} needs an alias: add `AS name`")
                        }
                    },
                }
            }
            if wildcard {
                if !final_map.is_empty() {
                    let pairs: Vec<(&str, &str)> = final_map
                        .iter()
                        .map(|(to, from)| (to.as_str(), from.as_str()))
                        .collect();
                    pipeline.map_expr(&pairs);
                }
            } else {
                let mut needed = needed;
                needed.sort();
                needed.dedup();
                let refs: Vec<&str> = needed.iter().map(|s| s.as_str()).collect();
                pipeline.select(&refs);
                let pairs: Vec<(&str, &str)> = final_map
                    .iter()
                    .map(|(to, from)| (to.as_str(), from.as_str()))
                    .collect();
                pipeline.map_expr(&pairs);
            }
        }

        apply_order(&mut pipeline, &order_by)?;

        if let Some(n) = limit {
            pipeline.limit(n as usize);
        }

        // The FROM source is attached last so the builder calls above keep the
        // simple `&mut Pipeline` chaining order.
        pipeline.read_csv(&path);
        Ok(pipeline)
    }

    fn parse_table(&mut self) -> Result<String> {
        let mut path = match self.advance_owned()? {
            Tok::Str(s) => s,
            Tok::Ident(s) => s,
            other => bail!("expected a table name or quoted file path, found {other:?}"),
        };
        if !path.contains('.') && !path.contains("://") {
            path.push_str(".csv");
        }
        Ok(path)
    }

    fn parse_projection_list(&mut self) -> Result<Vec<ProjectionItem>> {
        let mut items = vec![self.parse_projection_item()?];
        while self.peek_op(",") {
            self.advance();
            items.push(self.parse_projection_item()?);
        }
        Ok(items)
    }

    fn parse_projection_item(&mut self) -> Result<ProjectionItem> {
        if self.peek_op("*") {
            self.advance();
            return Ok(ProjectionItem::Wildcard);
        }
        let expr = self.parse_expr()?;
        if self.peek_keyword("AS") {
            self.advance();
            let alias = self.expect_ident()?;
            Ok(ProjectionItem::Named(expr, alias))
        } else {
            Ok(ProjectionItem::Unnamed(expr))
        }
    }

    fn parse_ident_list(&mut self) -> Result<Vec<String>> {
        let mut out = vec![self.expect_ident()?];
        while self.peek_op(",") {
            self.advance();
            out.push(self.expect_ident()?);
        }
        Ok(out)
    }

    fn parse_order_list(&mut self) -> Result<Vec<(String, bool)>> {
        let mut out = vec![self.parse_order_item()?];
        while self.peek_op(",") {
            self.advance();
            out.push(self.parse_order_item()?);
        }
        Ok(out)
    }

    fn parse_order_item(&mut self) -> Result<(String, bool)> {
        let col = self.expect_ident()?;
        let desc = if self.peek_keyword("ASC") {
            self.advance();
            false
        } else if self.peek_keyword("DESC") {
            self.advance();
            true
        } else {
            false
        };
        Ok((col, desc))
    }

    fn parse_number_literal(&mut self) -> Result<i64> {
        match self.advance_owned()? {
            Tok::Number(n) => n.parse::<i64>().context("LIMIT must be an integer"),
            other => bail!("LIMIT must be a number, found {other:?}"),
        }
    }

    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr> {
        let mut left = self.parse_and()?;
        while self.peek_keyword("OR") {
            self.advance();
            let right = self.parse_and()?;
            left = Expr::BinaryOp(Box::new(left), BinOp::Or, Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr> {
        let mut left = self.parse_not()?;
        while self.peek_keyword("AND") {
            self.advance();
            let right = self.parse_not()?;
            left = Expr::BinaryOp(Box::new(left), BinOp::And, Box::new(right));
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expr> {
        if self.peek_keyword("NOT") {
            self.advance();
            let inner = self.parse_not()?;
            return Ok(Expr::UnaryOp(UnOp::Not, Box::new(inner)));
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Expr> {
        let left = self.parse_additive()?;
        let left = self.parse_is_postfix(left)?;
        if let Some(op_text) = self.peek_op_any(CMP_OPS) {
            self.advance();
            let right = self.parse_additive()?;
            let right = self.parse_is_postfix(right)?;
            let op = match op_text.as_str() {
                "=" | "==" => BinOp::Eq,
                "<>" | "!=" => BinOp::NotEq,
                "<" => BinOp::Lt,
                "<=" => BinOp::LtEq,
                ">" => BinOp::Gt,
                ">=" => BinOp::GtEq,
                _ => unreachable!(),
            };
            return Ok(Expr::BinaryOp(Box::new(left), op, Box::new(right)));
        }
        Ok(left)
    }

    fn parse_is_postfix(&mut self, base: Expr) -> Result<Expr> {
        if self.peek_keyword("IS") {
            self.advance();
            if self.peek_keyword("NOT") {
                self.advance();
                self.expect_keyword("NULL")?;
                return Ok(Expr::IsNotNull(Box::new(base)));
            } else if self.peek_keyword("NULL") {
                self.advance();
                return Ok(Expr::IsNull(Box::new(base)));
            } else if self.peek_keyword("TRUE") {
                self.advance();
                return Ok(Expr::IsTrue(Box::new(base)));
            } else if self.peek_keyword("FALSE") {
                self.advance();
                return Ok(Expr::IsFalse(Box::new(base)));
            }
            bail!("unsupported IS predicate");
        }
        Ok(base)
    }

    fn parse_additive(&mut self) -> Result<Expr> {
        let mut left = self.parse_multiplicative()?;
        loop {
            if self.peek_op("+") {
                self.advance();
                let right = self.parse_multiplicative()?;
                left = Expr::BinaryOp(Box::new(left), BinOp::Plus, Box::new(right));
            } else if self.peek_op("-") {
                self.advance();
                let right = self.parse_multiplicative()?;
                left = Expr::BinaryOp(Box::new(left), BinOp::Minus, Box::new(right));
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary()?;
        loop {
            if self.peek_op("*") {
                self.advance();
                let right = self.parse_unary()?;
                left = Expr::BinaryOp(Box::new(left), BinOp::Mul, Box::new(right));
            } else if self.peek_op("/") {
                self.advance();
                let right = self.parse_unary()?;
                left = Expr::BinaryOp(Box::new(left), BinOp::Div, Box::new(right));
            } else if self.peek_op("%") {
                self.advance();
                let right = self.parse_unary()?;
                left = Expr::BinaryOp(Box::new(left), BinOp::Mod, Box::new(right));
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr> {
        if self.peek_op("-") {
            self.advance();
            let inner = self.parse_unary()?;
            return Ok(Expr::UnaryOp(UnOp::Minus, Box::new(inner)));
        }
        if self.peek_op("+") {
            self.advance();
            let inner = self.parse_unary()?;
            return Ok(Expr::UnaryOp(UnOp::Plus, Box::new(inner)));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Expr> {
        match self.advance_owned()? {
            Tok::Op(op) if op == "(" => {
                let inner = self.parse_or()?;
                if !self.peek_op(")") {
                    bail!("expected closing parenthesis, found {:?}", self.peek());
                }
                self.advance();
                Ok(Expr::Nested(Box::new(inner)))
            }
            Tok::Number(n) => Ok(Expr::Number(n)),
            Tok::Str(s) => Ok(Expr::Str(s)),
            Tok::Keyword(k) if k == "TRUE" => Ok(Expr::Bool(true)),
            Tok::Keyword(k) if k == "FALSE" => Ok(Expr::Bool(false)),
            Tok::Keyword(k) if k == "NULL" => Ok(Expr::Null),
            Tok::Ident(name) => {
                if self.peek_op("(") {
                    self.advance();
                    let arg = if self.peek_op("*") {
                        self.advance();
                        FuncArg::Star
                    } else {
                        FuncArg::Column(self.expect_ident()?)
                    };
                    if !self.peek_op(")") {
                        bail!(
                            "expected closing parenthesis in function call, found {:?}",
                            self.peek()
                        );
                    }
                    self.advance();
                    Ok(Expr::Func(name, arg))
                } else {
                    Ok(Expr::Ident(name))
                }
            }
            other => bail!("unsupported expression token {other:?}"),
        }
    }
}

/// Lower a SQL query to a pipeline.
pub fn build_sql_pipeline(sql: &str) -> Result<Pipeline> {
    let tokens = lex(sql).context("parsing SQL")?;
    let mut parser = SqlParser { tokens, pos: 0 };
    parser.parse_pipeline()
}

fn agg_output_name(func: &str, arg: &str) -> Option<String> {
    match func {
        "sum" => Some(format!("sum_{arg}")),
        "avg" => Some(format!("avg_{arg}")),
        "count" if arg == "*" => Some("count_all".to_string()),
        "count" => Some(format!("count_{arg}")),
        "min" => Some(format!("min_{arg}")),
        "max" => Some(format!("max_{arg}")),
        _ => None,
    }
}

fn projection_has_aggregate(projection: &[ProjectionItem]) -> bool {
    projection.iter().any(|item| match item {
        ProjectionItem::Named(expr, _) | ProjectionItem::Unnamed(expr) => is_aggregate(expr),
        ProjectionItem::Wildcard => false,
    })
}

fn is_aggregate(expr: &Expr) -> bool {
    matches!(expr, Expr::Func(name, _) if {
        matches!(name.to_ascii_lowercase().as_str(), "sum" | "avg" | "count" | "min" | "max")
    })
}

/// Handle one aggregate-path projection item.
fn push_agg(
    expr: &Expr,
    alias: String,
    group_by: &[String],
    aggs: &mut Vec<AggSpec>,
    agg_outputs: &mut Vec<(String, String)>,
) -> Result<()> {
    if let Expr::Func(name, arg) = expr {
        let func = name.to_ascii_lowercase();
        let arg_name = match arg {
            FuncArg::Star => "*".to_string(),
            FuncArg::Column(c) => c.clone(),
        };
        let spec = match func.as_str() {
            "sum" => AggSpec::sum(&arg_name),
            "avg" => AggSpec::avg(&arg_name),
            "count" if arg_name == "*" => AggSpec::count_all("count_all"),
            "count" => AggSpec::count(&arg_name),
            "min" => AggSpec::min(&arg_name),
            "max" => AggSpec::max(&arg_name),
            other => bail!("unsupported aggregate function {other:?}"),
        };
        let generated = agg_output_name(&func, &arg_name).unwrap_or_else(|| alias.clone());
        aggs.push(spec);
        agg_outputs.push((generated, alias));
        return Ok(());
    }
    if let Expr::Ident(col) = expr {
        if !group_by.contains(col) {
            bail!("column {col:?} must appear in GROUP BY or be aggregated");
        }
        agg_outputs.push((col.clone(), alias));
        return Ok(());
    }
    bail!("only columns and aggregate functions are supported in the projection")
}

fn default_alias(expr: &Expr) -> Result<String> {
    if let Expr::Func(name, arg) = expr {
        let func = name.to_ascii_lowercase();
        let arg_name = match arg {
            FuncArg::Star => "*".to_string(),
            FuncArg::Column(c) => c.clone(),
        };
        if let Some(n) = agg_output_name(&func, &arg_name) {
            return Ok(n);
        }
    }
    as_simple_column(expr)
}

fn as_simple_column(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Ident(name) => Ok(name.clone()),
        other => {
            bail!("unsupported projection expression {other:?}: use a plain column or an aggregate")
        }
    }
}

fn apply_order(pipeline: &mut Pipeline, order_by: &[(String, bool)]) -> Result<()> {
    if order_by.is_empty() {
        return Ok(());
    }
    let all_desc = order_by.iter().all(|(_, d)| *d);
    let all_asc = order_by.iter().all(|(_, d)| !*d);
    let columns: Vec<&str> = order_by.iter().map(|(c, _)| c.as_str()).collect();
    if all_desc {
        pipeline.sort_by_desc(&columns);
    } else if all_asc {
        pipeline.sort_by(&columns);
    } else {
        bail!("mixed ASC/DESC ordering is not supported");
    }
    Ok(())
}

/// Collect every column identifier referenced by a SQL expression.
fn collect_identifiers(expr: &Expr) -> Vec<String> {
    fn walk(expr: &Expr, out: &mut Vec<String>) {
        match expr {
            Expr::Ident(name) => out.push(name.clone()),
            Expr::BinaryOp(left, _, right) => {
                walk(left, out);
                walk(right, out);
            }
            Expr::UnaryOp(_, e) => walk(e, out),
            Expr::Nested(inner) => walk(inner, out),
            Expr::IsFalse(e) | Expr::IsTrue(e) | Expr::IsNull(e) | Expr::IsNotNull(e) => {
                walk(e, out)
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    walk(expr, &mut out);
    out
}

/// Convert a SQL WHERE/projection expression into the engine's expression text.
fn convert_expr(expr: &Expr) -> Result<String> {
    Ok(match expr {
        Expr::Ident(name) => name.clone(),
        Expr::Number(n) => n.clone(),
        Expr::Str(s) => format!("'{}'", s.replace('\'', "''")),
        Expr::Bool(b) => b.to_string(),
        Expr::Null => "null".to_string(),
        Expr::BinaryOp(left, op, right) => {
            let core_op = match op {
                BinOp::Eq => "==",
                BinOp::NotEq => "!=",
                BinOp::Lt => "<",
                BinOp::LtEq => "<=",
                BinOp::Gt => ">",
                BinOp::GtEq => ">=",
                BinOp::Plus => "+",
                BinOp::Minus => "-",
                BinOp::Mul => "*",
                BinOp::Div => "/",
                BinOp::Mod => "%",
                BinOp::And => "and",
                BinOp::Or => "or",
            };
            format!(
                "{} {} {}",
                convert_expr(left)?,
                core_op,
                convert_expr(right)?
            )
        }
        Expr::UnaryOp(op, e) => {
            let core_op = match op {
                UnOp::Not => "not ",
                UnOp::Minus => "-",
                UnOp::Plus => "+",
            };
            format!("{}({})", core_op, convert_expr(e)?)
        }
        Expr::Nested(inner) => format!("({})", convert_expr(inner)?),
        Expr::IsFalse(e) => format!("({}) == false", convert_expr(e)?),
        Expr::IsTrue(e) => format!("({}) == true", convert_expr(e)?),
        Expr::IsNotNull(e) => format!("not (({}) == null)", convert_expr(e)?),
        Expr::IsNull(e) => format!("({}) == null", convert_expr(e)?),
        Expr::Func(name, _) => bail!("unsupported function call {name:?} in this position"),
    })
}
