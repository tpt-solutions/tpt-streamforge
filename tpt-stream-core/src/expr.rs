//! Minimal row-expression language shared by the C FFI and the Python wrapper.
//!
//! Grammar (precedence climbing):
//! ```text
//! or      := and ( "or" and )*
//! and     := cmp ( "and" cmp )*
//! cmp     := add ( ( "==" | "!=" | "<" | "<=" | ">" | ">=" ) add )*
//! add     := mul ( ( "+" | "-" ) mul )*
//! mul     := unary ( ( "*" | "/" | "%" ) unary )*
//! unary   := ( "-" | "not" | "!" ) unary | primary
//! primary := number | string | "true" | "false" | column
//!          | "(" or ")" | func "(" or ( "," or )* ")"
//! ```
//!
//! Column names follow `[A-Za-z_][A-Za-z0-9_]*`. A column that does not exist
//! evaluates to null. Arithmetic widens to the "largest" numeric type
//! (int32 -> int64 -> float64) and string `+` concatenates.

use crate::row::Row;
use crate::value::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExprErrorKind {
    Unexpected(char),
    UnterminatedString,
    UnexpectedEnd,
    InvalidNumber,
}

#[derive(Debug, Clone)]
pub struct ExprError {
    pub kind: ExprErrorKind,
    pub pos: usize,
}

impl std::fmt::Display for ExprError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "expression error at byte {}: {:?}", self.pos, self.kind)
    }
}

impl std::error::Error for ExprError {}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Null,
    BoolLit(bool),
    IntLit(i64),
    FloatLit(f64),
    StrLit(String),
    Column(String),
    Neg(Box<Expr>),
    Not(Box<Expr>),
    Binary {
        op: BinOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Call {
        name: String,
        args: Vec<Expr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}

/// Parse an expression string. Returns the AST or an `ExprError` with the byte
/// offset where parsing failed.
pub fn parse(input: &str) -> Result<Expr, ExprError> {
    let tokens = tokenize(input)?;
    let mut parser = Parser { tokens, pos: 0 };
    let expr = parser.parse_or()?;
    if parser.pos < parser.tokens.len() {
        return Err(ExprError {
            kind: ExprErrorKind::Unexpected(tok_char(&parser.tokens[parser.pos].0)),
            pos: parser.tokens[parser.pos].1,
        });
    }
    Ok(expr)
}

fn tok_char(tok: &Tok) -> char {
    match tok {
        Tok::Op(op) => op.chars().next().unwrap_or('?'),
        Tok::LParen => '(',
        Tok::RParen => ')',
        Tok::Comma => ',',
        Tok::Num(v) => v.to_string().chars().next().unwrap_or('0'),
        Tok::Float(v) => v.to_string().chars().next().unwrap_or('0'),
        Tok::Str(s) => s.chars().next().unwrap_or('\''),
        Tok::Ident(s) => s.chars().next().unwrap_or('_'),
    }
}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Num(i64),   // integer literal
    Float(f64), // float literal
    Str(String),
    Ident(String),
    Op(String), // two-char ops kept whole: ==, !=, <=, >=
    LParen,
    RParen,
    Comma,
}

fn tokenize(input: &str) -> Result<Vec<(Tok, usize)>, ExprError> {
    let bytes = input.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i] as char;
        match c {
            ' ' | '\t' | '\n' | '\r' => i += 1,
            '(' => {
                out.push((Tok::LParen, i));
                i += 1;
            }
            ')' => {
                out.push((Tok::RParen, i));
                i += 1;
            }
            ',' => {
                out.push((Tok::Comma, i));
                i += 1;
            }
            '"' | '\'' => {
                let quote = c;
                let start = i;
                i += 1;
                let mut s = String::new();
                let mut closed = false;
                while i < bytes.len() {
                    let b = bytes[i];
                    if b as char == quote {
                        i += 1;
                        closed = true;
                        break;
                    }
                    if b == b'\\' && i + 1 < bytes.len() {
                        let esc = bytes[i + 1];
                        i += 2;
                        s.push(match esc {
                            b'n' => '\n',
                            b't' => '\t',
                            b'r' => '\r',
                            b'\\' => '\\',
                            b'"' => '"',
                            b'\'' => '\'',
                            other => other as char,
                        });
                        continue;
                    }
                    s.push(b as char);
                    i += 1;
                }
                if !closed {
                    return Err(ExprError {
                        kind: ExprErrorKind::UnterminatedString,
                        pos: start,
                    });
                }
                out.push((Tok::Str(s), start));
            }
            '0'..='9' => {
                let start = i;
                let mut is_float = false;
                while i < bytes.len() {
                    let b = bytes[i];
                    if (b as char).is_ascii_digit() {
                        i += 1;
                    } else if b == b'.' {
                        is_float = true;
                        i += 1;
                    } else {
                        break;
                    }
                }
                let text = &input[start..i];
                if is_float {
                    match text.parse::<f64>() {
                        Ok(v) => out.push((Tok::Float(v), start)),
                        Err(_) => {
                            return Err(ExprError {
                                kind: ExprErrorKind::InvalidNumber,
                                pos: start,
                            })
                        }
                    }
                } else {
                    match text.parse::<i64>() {
                        Ok(v) => out.push((Tok::Num(v), start)),
                        Err(_) => {
                            return Err(ExprError {
                                kind: ExprErrorKind::InvalidNumber,
                                pos: start,
                            })
                        }
                    }
                }
            }
            'a'..='z' | 'A'..='Z' | '_' => {
                let start = i;
                while i < bytes.len()
                    && ((bytes[i] as char).is_ascii_alphanumeric() || bytes[i] as char == '_')
                {
                    i += 1;
                }
                out.push((Tok::Ident(input[start..i].to_string()), start));
            }
            '=' | '!' | '<' | '>' => {
                let start = i;
                if i + 1 < bytes.len()
                    && ((c == '=' && bytes[i + 1] == b'=') || bytes[i + 1] == b'=')
                {
                    out.push((Tok::Op(input[start..i + 2].to_string()), start));
                    i += 2;
                } else if matches!(c, '<' | '>' | '!') {
                    out.push((Tok::Op(c.to_string()), start));
                    i += 1;
                } else {
                    return Err(ExprError {
                        kind: ExprErrorKind::Unexpected(c),
                        pos: start,
                    });
                }
            }
            '+' | '-' | '*' | '/' | '%' => {
                out.push((Tok::Op(c.to_string()), i));
                i += 1;
            }
            other => {
                return Err(ExprError {
                    kind: ExprErrorKind::Unexpected(other),
                    pos: i,
                })
            }
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Parser (pratt / precedence climbing)
// ---------------------------------------------------------------------------

struct Parser {
    tokens: Vec<(Tok, usize)>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&(Tok, usize)> {
        self.tokens.get(self.pos)
    }

    fn bump(&mut self) -> Option<(Tok, usize)> {
        let t = self.tokens.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn err(&self, kind: ExprErrorKind) -> ExprError {
        let pos = self
            .tokens
            .get(self.pos)
            .map(|(_, p)| *p)
            .unwrap_or(input_end_pos());
        ExprError { kind, pos }
    }

    fn is_keyword(tok: &Tok, word: &str) -> bool {
        matches!(tok, Tok::Ident(w) if w == word) || matches!(tok, Tok::Op(op) if op == word)
    }

    fn parse_or(&mut self) -> Result<Expr, ExprError> {
        let mut left = self.parse_and()?;
        while let Some((ref tok, _)) = self.peek() {
            if Self::is_keyword(tok, "or") {
                self.bump();
                let right = self.parse_and()?;
                left = Expr::Binary {
                    op: BinOp::Or,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, ExprError> {
        let mut left = self.parse_cmp()?;
        while let Some((ref tok, _)) = self.peek() {
            if Self::is_keyword(tok, "and") {
                self.bump();
                let right = self.parse_cmp()?;
                left = Expr::Binary {
                    op: BinOp::And,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_cmp(&mut self) -> Result<Expr, ExprError> {
        let mut left = self.parse_add()?;
        loop {
            let op = match self.peek() {
                Some((Tok::Op(ref op), _))
                    if matches!(op.as_str(), "==" | "!=" | "<" | "<=" | ">" | ">=") =>
                {
                    op.clone()
                }
                _ => break,
            };
            self.bump();
            let right = self.parse_add()?;
            left = Expr::Binary {
                op: binop_from_str(&op).unwrap(),
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_add(&mut self) -> Result<Expr, ExprError> {
        let mut left = self.parse_mul()?;
        loop {
            let op = match self.peek() {
                Some((Tok::Op(ref op), _)) if op == "+" || op == "-" => op.clone(),
                _ => break,
            };
            self.bump();
            let right = self.parse_mul()?;
            left = Expr::Binary {
                op: if op == "+" { BinOp::Add } else { BinOp::Sub },
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_mul(&mut self) -> Result<Expr, ExprError> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Some((Tok::Op(ref op), _)) if matches!(op.as_str(), "*" | "/" | "%") => op.clone(),
                _ => break,
            };
            self.bump();
            let right = self.parse_unary()?;
            left = Expr::Binary {
                op: match op.as_str() {
                    "*" => BinOp::Mul,
                    "/" => BinOp::Div,
                    _ => BinOp::Rem,
                },
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr, ExprError> {
        if let Some((ref tok, _)) = self.peek() {
            let op = match tok {
                Tok::Op(op) => op.clone(),
                Tok::Ident(w) if Self::is_keyword(tok, "not") => "not".to_string(),
                _ => String::new(),
            };
            match op.as_str() {
                "-" => {
                    self.bump();
                    let inner = self.parse_unary()?;
                    return Ok(Expr::Neg(Box::new(inner)));
                }
                "!" => {
                    self.bump();
                    let inner = self.parse_unary()?;
                    return Ok(Expr::Not(Box::new(inner)));
                }
                "not" => {
                    self.bump();
                    let inner = self.parse_unary()?;
                    return Ok(Expr::Not(Box::new(inner)));
                }
                _ => {}
            }
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Expr, ExprError> {
        let (tok, _pos) = match self.bump() {
            Some(t) => t,
            None => return Err(self.err(ExprErrorKind::UnexpectedEnd)),
        };
        match tok {
            Tok::Num(v) => Ok(Expr::IntLit(v)),
            Tok::Float(v) => Ok(Expr::FloatLit(v)),
            Tok::Str(s) => Ok(Expr::StrLit(s)),
            Tok::LParen => {
                let inner = self.parse_or()?;
                match self.bump() {
                    Some((Tok::RParen, _)) => Ok(inner),
                    _ => Err(self.err(ExprErrorKind::UnexpectedEnd)),
                }
            }
            Tok::Ident(name) => {
                if name == "true" {
                    Ok(Expr::BoolLit(true))
                } else if name == "false" {
                    Ok(Expr::BoolLit(false))
                } else if name == "null" || name == "nil" {
                    Ok(Expr::Null)
                } else if let Some((Tok::LParen, _)) = self.peek() {
                    self.bump();
                    let mut args = Vec::new();
                    if let Some((Tok::RParen, _)) = self.peek() {
                        self.bump();
                    } else {
                        loop {
                            args.push(self.parse_or()?);
                            match self.peek() {
                                Some((Tok::Comma, _)) => {
                                    self.bump();
                                }
                                Some((Tok::RParen, _)) => {
                                    self.bump();
                                    break;
                                }
                                _ => return Err(self.err(ExprErrorKind::UnexpectedEnd)),
                            }
                        }
                    }
                    Ok(Expr::Call { name, args })
                } else {
                    Ok(Expr::Column(name))
                }
            }
            _ => Err(self.err(ExprErrorKind::UnexpectedEnd)),
        }
    }
}

fn input_end_pos() -> usize {
    0
}

fn binop_from_str(op: &str) -> Option<BinOp> {
    Some(match op {
        "==" => BinOp::Eq,
        "!=" => BinOp::Ne,
        "<" => BinOp::Lt,
        "<=" => BinOp::Le,
        ">" => BinOp::Gt,
        ">=" => BinOp::Ge,
        _ => return None,
    })
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

impl Expr {
    /// Evaluate against a row. Missing columns and null operands propagate to
    /// null for numeric/string results; comparisons yield bool (or null when an
    /// operand is null).
    pub fn eval(&self, row: &Row) -> Value {
        match self {
            Expr::Null => Value::Null,
            Expr::BoolLit(b) => Value::Bool(*b),
            Expr::IntLit(v) => Value::Int64(*v),
            Expr::FloatLit(v) => Value::Float64(*v),
            Expr::StrLit(s) => Value::Utf8(s.clone()),
            Expr::Column(name) => row.get(name).unwrap_or(Value::Null),
            Expr::Neg(inner) => match inner.eval(row) {
                Value::Int32(v) => Value::Int32(-v),
                Value::Int64(v) => Value::Int64(-v),
                Value::Float32(v) => Value::Float32(-v),
                Value::Float64(v) => Value::Float64(-v),
                _ => Value::Null,
            },
            Expr::Not(inner) => match inner.eval(row) {
                Value::Bool(b) => Value::Bool(!b),
                _ => Value::Null,
            },
            Expr::Binary { op, left, right } => {
                let l = left.eval(row);
                let r = right.eval(row);
                eval_binary(*op, &l, &r)
            }
            Expr::Call { name, args } => eval_call(name, args, row),
        }
    }

    /// Convenience: `eval` then treat truthy (bool true) as a filter match.
    pub fn matches(&self, row: &Row) -> bool {
        matches!(self.eval(row), Value::Bool(true))
    }
}

fn as_f64(v: &Value) -> Option<f64> {
    Some(match v {
        Value::Int32(x) => *x as f64,
        Value::Int64(x) => *x as f64,
        Value::Float32(x) => *x as f64,
        Value::Float64(x) => *x,
        _ => return None,
    })
}

fn as_int(v: &Value) -> Option<i64> {
    Some(match v {
        Value::Int32(x) => *x as i64,
        Value::Int64(x) => *x,
        Value::Float32(x) => *x as i64,
        Value::Float64(x) => *x as i64,
        _ => return None,
    })
}

fn is_numeric(v: &Value) -> bool {
    matches!(
        v,
        Value::Int32(_) | Value::Int64(_) | Value::Float32(_) | Value::Float64(_)
    )
}

/// Ordering for non-numeric Value pairs (strings and bools). Unrelated types
/// compare only by equality.
fn compare_values(l: &Value, r: &Value) -> Option<std::cmp::Ordering> {
    match (l, r) {
        (Value::Utf8(a), Value::Utf8(b)) => Some(a.cmp(b)),
        (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
        _ if l == r => Some(std::cmp::Ordering::Equal),
        _ => None,
    }
}

fn eval_binary(op: BinOp, l: &Value, r: &Value) -> Value {
    // Logical operators use three-valued (Kleene) logic and must be handled
    // before the null short-circuit: `true OR <null>` is true, `false AND
    // <null>` is false, and only a fully-undetermined operand combination
    // yields null.
    match op {
        BinOp::And => {
            if *l == Value::Bool(false) || *r == Value::Bool(false) {
                return Value::Bool(false);
            }
            if *l == Value::Null || *r == Value::Null {
                return Value::Null;
            }
            return Value::Bool(bool_arg(l) && bool_arg(r));
        }
        BinOp::Or => {
            if *l == Value::Bool(true) || *r == Value::Bool(true) {
                return Value::Bool(true);
            }
            if *l == Value::Null || *r == Value::Null {
                return Value::Null;
            }
            return Value::Bool(bool_arg(l) || bool_arg(r));
        }
        _ => {}
    }
    // Null short-circuits: comparisons to null -> null.
    if matches!(l, Value::Null) || matches!(r, Value::Null) {
        return Value::Null;
    }
    match op {
        BinOp::And | BinOp::Or => unreachable!("logical ops handled above"),
        BinOp::Add => {
            // String concatenation.
            if let (Value::Utf8(a), Value::Utf8(b)) = (l, r) {
                return Value::Utf8(format!("{a}{b}"));
            }
            if is_numeric(l) && is_numeric(r) {
                return arithmetic(BinOp::Add, l, r);
            }
            Value::Null
        }
        BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Rem => {
            if is_numeric(l) && is_numeric(r) {
                return arithmetic(op, l, r);
            }
            Value::Null
        }
        BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
            let eq = if is_numeric(l) && is_numeric(r) {
                as_f64(l).unwrap().partial_cmp(&as_f64(r).unwrap())
            } else {
                compare_values(l, r)
            };
            let result = match op {
                BinOp::Eq => eq == Some(std::cmp::Ordering::Equal),
                BinOp::Ne => eq != Some(std::cmp::Ordering::Equal),
                BinOp::Lt => eq == Some(std::cmp::Ordering::Less),
                BinOp::Le => eq
                    .map(|e| e != std::cmp::Ordering::Greater)
                    .unwrap_or(false),
                BinOp::Gt => eq == Some(std::cmp::Ordering::Greater),
                BinOp::Ge => eq.map(|e| e != std::cmp::Ordering::Less).unwrap_or(false),
                _ => unreachable!(),
            };
            Value::Bool(result)
        }
    }
}

fn arithmetic(op: BinOp, l: &Value, r: &Value) -> Value {
    let any_float = matches!(l, Value::Float32(_) | Value::Float64(_))
        || matches!(r, Value::Float32(_) | Value::Float64(_));
    if any_float {
        let a = as_f64(l).unwrap_or(0.0);
        let b = as_f64(r).unwrap_or(0.0);
        let v = match op {
            BinOp::Add => a + b,
            BinOp::Sub => a - b,
            BinOp::Mul => a * b,
            BinOp::Div => a / b,
            BinOp::Rem => a % b,
            _ => 0.0,
        };
        Value::Float64(v)
    } else {
        let a = as_int(l).unwrap_or(0);
        let b = as_int(r).unwrap_or(0);
        let v = match op {
            BinOp::Add => a.wrapping_add(b),
            BinOp::Sub => a.wrapping_sub(b),
            BinOp::Mul => a.wrapping_mul(b),
            BinOp::Rem => a % b,
            BinOp::Div => {
                if b == 0 {
                    return Value::Null;
                }
                a / b
            }
            _ => 0,
        };
        Value::Int64(v)
    }
}

fn bool_arg(v: &Value) -> bool {
    matches!(v, Value::Bool(true))
}

fn eval_call(name: &str, args: &[Expr], row: &Row) -> Value {
    let values: Vec<Value> = args.iter().map(|a| a.eval(row)).collect();
    match name {
        "coalesce" => values
            .into_iter()
            .find(|v| !matches!(v, Value::Null))
            .unwrap_or(Value::Null),
        "abs" => match values.first() {
            Some(Value::Int32(v)) => Value::Int32(v.abs()),
            Some(Value::Int64(v)) => Value::Int64(v.abs()),
            Some(Value::Float32(v)) => Value::Float32(v.abs()),
            Some(Value::Float64(v)) => Value::Float64(v.abs()),
            _ => Value::Null,
        },
        "sqrt" => match values.first() {
            Some(v) if is_numeric(v) => as_f64(v)
                .map(|x| Value::Float64(x.sqrt()))
                .unwrap_or(Value::Null),
            _ => Value::Null,
        },
        "min" | "max" => {
            let mut best: Option<Value> = None;
            for v in values {
                let better = match (&best, &v) {
                    (None, _) => true,
                    (Some(b), _) => {
                        let cmp = if is_numeric(b) && is_numeric(&v) {
                            as_f64(b).unwrap().partial_cmp(&as_f64(&v).unwrap())
                        } else {
                            compare_values(b, &v)
                        };
                        if name == "min" {
                            cmp == Some(std::cmp::Ordering::Greater)
                        } else {
                            cmp == Some(std::cmp::Ordering::Less)
                        }
                    }
                };
                if better {
                    best = Some(v);
                }
            }
            best.unwrap_or(Value::Null)
        }
        "upper" => match values.first() {
            Some(Value::Utf8(s)) => Value::Utf8(s.to_uppercase()),
            _ => Value::Null,
        },
        "lower" => match values.first() {
            Some(Value::Utf8(s)) => Value::Utf8(s.to_lowercase()),
            _ => Value::Null,
        },
        "length" => match values.first() {
            Some(Value::Utf8(s)) => Value::Int32(s.chars().count() as i32),
            Some(v) if is_numeric(v) => Value::Int32(as_f64(v).unwrap_or(0.0) as i32),
            _ => Value::Null,
        },
        "trim" => match values.first() {
            Some(Value::Utf8(s)) => Value::Utf8(s.trim().to_string()),
            _ => Value::Null,
        },
        "if" => {
            if args.len() == 3 {
                if bool_arg(&values[0]) {
                    values[1].clone()
                } else {
                    values[2].clone()
                }
            } else {
                Value::Null
            }
        }
        _ => Value::Null,
    }
}

/// Evaluate against a row; missing columns and null operands propagate to
/// numeric/string results; comparisons yield bool (or null when an operand is null).
pub fn eval(expr: &Expr, row: &Row) -> Value {
    expr.eval(row)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::Column;
    use crate::row::Row;
    use crate::table::RecordBatch;
    use crate::value::DataType;

    fn row(values: &[(&str, Value)]) -> RecordBatch {
        let mut columns: Vec<Column> = Vec::new();
        for (name, v) in values {
            let dtype = match v {
                Value::Int32(_) => DataType::Int32,
                Value::Int64(_) => DataType::Int64,
                Value::Float64(_) => DataType::Float64,
                Value::Bool(_) => DataType::Bool,
                _ => DataType::Utf8,
            };
            let mut col = Column::new(*name, dtype, 1);
            col.push(v.clone());
            columns.push(col);
        }
        RecordBatch::new(columns)
    }

    fn parse_eval(expr: &str, batch: &RecordBatch) -> Value {
        let r = Row::new(batch, 0);
        parse(expr).unwrap().eval(&r)
    }

    #[test]
    fn arithmetic() {
        let batch = row(&[("a", Value::Int32(6)), ("b", Value::Int32(4))]);
        assert_eq!(parse_eval("a * b + 1", &batch), Value::Int64(25));
        assert_eq!(parse_eval("a / 2", &batch), Value::Int64(3));
        assert_eq!(parse_eval("a % 3", &batch), Value::Int64(0));
        assert_eq!(parse_eval("a / 0", &batch), Value::Null);
        assert_eq!(parse_eval("(a + b) * 2", &batch), Value::Int64(20));
    }

    #[test]
    fn float_and_string_ops() {
        let batch = row(&[("x", Value::Float64(1.5)), ("s", Value::Utf8("ab".into()))]);
        assert_eq!(parse_eval("x + 1", &batch), Value::Float64(2.5));
        assert_eq!(parse_eval("s + 'cd'", &batch), Value::Utf8("abcd".into()));
        assert_eq!(parse_eval("upper(s)", &batch), Value::Utf8("AB".into()));
        assert_eq!(parse_eval("length(s)", &batch), Value::Int32(2));
        assert_eq!(
            parse_eval("trim('  pad  ')", &batch),
            Value::Utf8("pad".into())
        );
    }

    #[test]
    fn three_valued_logic() {
        let batch = row(&[("a", Value::Null), ("flag", Value::Bool(true))]);
        // true OR (col == <null>) must still be true.
        assert_eq!(
            parse_eval("flag == true or a > 1", &batch),
            Value::Bool(true)
        );
        assert_eq!(
            parse_eval("a > 1 or flag == true", &batch),
            Value::Bool(true)
        );
        // false AND <null> is false; <null> AND <true> is null.
        assert_eq!(
            parse_eval("flag == false and a > 1", &batch),
            Value::Bool(false)
        );
        assert_eq!(parse_eval("a > 1 and flag == true", &batch), Value::Null);
    }

    #[test]
    fn comparisons_and_logic() {
        let batch = row(&[("score", Value::Int32(85))]);
        assert_eq!(
            parse_eval("score > 60 and score < 90", &batch),
            Value::Bool(true)
        );
        assert_eq!(
            parse_eval("score == 85 or score == 1", &batch),
            Value::Bool(true)
        );
        assert_eq!(parse_eval("not (score > 100)", &batch), Value::Bool(true));
        assert_eq!(parse_eval("score >= 85", &batch), Value::Bool(true));
        assert_eq!(parse_eval("score != 1", &batch), Value::Bool(true));
    }

    #[test]
    fn null_propagation() {
        let batch = row(&[("a", Value::Int32(1))]);
        assert_eq!(parse_eval("missing_col + 1", &batch), Value::Null);
        assert_eq!(parse_eval("a == null", &batch), Value::Null);
        assert_eq!(
            parse_eval("coalesce(missing_col, a)", &batch),
            Value::Int32(1)
        );
    }

    #[test]
    fn call_min_max_if() {
        let batch = row(&[("a", Value::Int32(3)), ("b", Value::Int32(9))]);
        assert_eq!(parse_eval("min(a, b)", &batch), Value::Int32(3));
        assert_eq!(parse_eval("max(a, b)", &batch), Value::Int32(9));
        assert_eq!(
            parse_eval("if(a < b, 'yes', 'no')", &batch),
            Value::Utf8("yes".into())
        );
    }

    #[test]
    fn malformed_input() {
        assert!(parse("").is_err());
        assert!(parse("(a +").is_err());
        assert!(parse("a == ").is_err());
        assert!(parse("'unterminated").is_err());
        assert!(parse("a @ b").is_err());
    }
}
