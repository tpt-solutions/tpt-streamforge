//! `tptforge sql "SELECT ..."` — a SQL frontend lowering onto pipeline stages.
//!
//! Supported subset (single table):
//!
//! ```sql
//! SELECT projections
//! FROM 'file.csv'            -- csv/jsonl/json/tptcol by extension, .gz ok
//! [WHERE <comparison expr>]  -- identifiers, literals, AND/OR/NOT, =,<>,<,<=,>,>=, + - * / %
//! [GROUP BY col, ...]
//! [ORDER BY col [ASC|DESC], ...]
//! [LIMIT n]
//! ```
//!
//! Aggregates map onto the engine's naming: `SUM(x)` -> `sum_x`,
//! `COUNT(*)` -> `count_all`, `COUNT(x)` -> `count_x`, etc. Aliases rename
//! the output columns. Unhandled SQL raises a clear error, never silently
//! wrong results.

use anyhow::{bail, Context, Result};
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, Function, FunctionArg, FunctionArgExpr, GroupByExpr,
    Query, SelectItem, SetExpr, Statement, TableFactor, Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use tpt_stream_core::agg::AggSpec;
use tpt_stream_core::Pipeline;

/// Which aggregate output column the engine produces for a SQL function.
fn agg_output_name(func: &str, arg: &str) -> Option<String> {
    match func.to_ascii_lowercase().as_str() {
        "sum" => Some(format!("sum_{arg}")),
        "avg" => Some(format!("avg_{arg}")),
        "count" if arg == "*" => Some("count_all".to_string()),
        "count" => Some(format!("count_{arg}")),
        "min" => Some(format!("min_{arg}")),
        "max" => Some(format!("max_{arg}")),
        _ => None,
    }
}

/// First argument of a function call, if any (sqlparser wraps the argument
/// list in `FunctionArguments`).
fn first_arg(f: &Function) -> Option<&FunctionArg> {
    match &f.args {
        sqlparser::ast::FunctionArguments::List(list) => list.args.first(),
        _ => None,
    }
}

/// Lower a SQL query to a pipeline.
pub fn build_sql_pipeline(sql: &str) -> Result<Pipeline> {
    let statements = Parser::parse_sql(&GenericDialect {}, sql).context("parsing SQL")?;
    let [statement] = statements.as_slice() else {
        bail!("exactly one statement is supported");
    };
    let Statement::Query(query) = statement else {
        bail!("only SELECT queries are supported");
    };
    lower_query(query)
}

fn lower_query(query: &Query) -> Result<Pipeline> {
    if query.with.is_some() {
        bail!("CTEs (WITH) are not supported");
    }
    if matches!(*query.body, SetExpr::SetOperation { .. }) {
        bail!("UNION/INTERSECT/EXCEPT are not supported");
    }
    let limit = query.limit.as_ref().map(sql_number).transpose()?;
    let SetExpr::Select(select) = &*query.body else {
        bail!("only a plain SELECT is supported");
    };
    if select.distinct.is_some() {
        bail!("SELECT DISTINCT is not supported");
    }
    if !select.lateral_views.is_empty() {
        bail!("LATERAL VIEWS are not supported");
    }

    // FROM: exactly one table, given as a quoted file path or bare name.
    let from = select
        .from
        .first()
        .ok_or_else(|| anyhow::anyhow!("query needs a FROM clause"))?;
    if select.from.len() > 1 {
        bail!("multiple FROM tables are not supported (use the join stage in YAML)");
    }
    if !from.joins.is_empty() {
        bail!("SQL JOIN is not supported (use the join stage in YAML)");
    }
    let table = match &from.relation {
        TableFactor::Table { name, .. } => name
            .0
            .last()
            .map(|i| i.value.clone())
            .ok_or_else(|| anyhow::anyhow!("empty table name"))?,
        other => bail!("unsupported FROM relation: {other:?}"),
    };
    let mut path = table;
    if !path.contains('.') && !path.contains("://") {
        path.push_str(".csv");
    }

    let mut pipeline = Pipeline::new();

    // WHERE -> engine expression text.
    if let Some(where_expr) = &select.selection {
        let text = convert_expr(where_expr)?;
        pipeline.filter_expr(&text);
    }

    let group_by: Vec<String> = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => exprs
            .iter()
            .map(|e| match e {
                SqlExpr::Identifier(ident) => Ok(ident.value.clone()),
                other => bail!("GROUP BY supports plain columns only, got {other:?}"),
            })
            .collect::<Result<_>>()?,
        _ => bail!("only plain GROUP BY expressions are supported"),
    };
    let aggregate_query = !group_by.is_empty() || projection_has_aggregate(&select.projection);

    if aggregate_query {
        // Outputs in projection order: [group columns...] + [aggregates...].
        let mut aggs: Vec<AggSpec> = Vec::new();
        let mut agg_outputs: Vec<(String, String)> = Vec::new(); // (generated, alias)
        for item in &select.projection {
            match item {
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                    bail!("wildcards with aggregates are not supported");
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    push_agg(
                        expr,
                        alias.value.clone(),
                        &group_by,
                        &mut aggs,
                        &mut agg_outputs,
                    )?;
                }
                SelectItem::UnnamedExpr(expr) => {
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
        for item in &select.projection {
            match item {
                SelectItem::Wildcard(_) => wildcard = true,
                SelectItem::QualifiedWildcard(_, _) => {
                    bail!("qualified wildcards are not supported")
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let text = convert_expr(expr)?;
                    needed.extend(collect_identifiers(expr));
                    final_map.push((alias.value.clone(), text));
                }
                SelectItem::UnnamedExpr(expr) => match expr {
                    SqlExpr::Identifier(ident) => {
                        let col = ident.value.clone();
                        needed.push(col.clone());
                        final_map.push((col.clone(), col));
                    }
                    other => bail!("computed projection {other:?} needs an alias: add `AS name`"),
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

    apply_order(&mut pipeline, query)?;

    if let Some(n) = limit {
        pipeline.limit(n as usize);
    }

    // The FROM source is attached last so the builder calls above keep the
    // simple `&mut Pipeline` chaining order.
    pipeline.read_csv(&path);
    Ok(pipeline)
}

fn projection_has_aggregate(projection: &[SelectItem]) -> bool {
    projection.iter().any(|item| match item {
        SelectItem::ExprWithAlias { expr, .. } | SelectItem::UnnamedExpr(expr) => {
            is_aggregate(expr)
        }
        _ => false,
    })
}

fn is_aggregate(expr: &SqlExpr) -> bool {
    matches!(expr, SqlExpr::Function(f) if {
        let name = f.name.0.last().map(|i| i.value.to_ascii_lowercase());
        matches!(name.as_deref(), Some("sum") | Some("avg") | Some("count") | Some("min") | Some("max"))
    })
}

/// Handle one aggregate-path projection item.
fn push_agg(
    expr: &SqlExpr,
    alias: String,
    group_by: &[String],
    aggs: &mut Vec<AggSpec>,
    agg_outputs: &mut Vec<(String, String)>,
) -> Result<()> {
    if let SqlExpr::Function(f) = expr {
        let func = f
            .name
            .0
            .last()
            .map(|i| i.value.to_ascii_lowercase())
            .ok_or_else(|| anyhow::anyhow!("empty function name"))?;
        let arg = match first_arg(f) {
            Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard)) => "*".to_string(),
            Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) => as_simple_column(e)?,
            Some(FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_))) => {
                bail!("qualified wildcards in aggregates are not supported")
            }
            _ => bail!("unsupported aggregate argument"),
        };
        let spec = match func.as_str() {
            "sum" => AggSpec::sum(&arg),
            "avg" => AggSpec::avg(&arg),
            "count" if arg == "*" => AggSpec::count_all("count_all"),
            "count" => AggSpec::count(&arg),
            "min" => AggSpec::min(&arg),
            "max" => AggSpec::max(&arg),
            other => bail!("unsupported aggregate function {other:?}"),
        };
        let generated = agg_output_name(&func, &arg).unwrap_or_else(|| alias.clone());
        aggs.push(spec);
        agg_outputs.push((generated, alias));
        return Ok(());
    }
    if let SqlExpr::Identifier(ident) = expr {
        let col = ident.value.clone();
        if !group_by.contains(&col) {
            bail!("column {col:?} must appear in GROUP BY or be aggregated");
        }
        agg_outputs.push((col.clone(), alias));
        return Ok(());
    }
    bail!("only columns and aggregate functions are supported in the projection")
}

fn default_alias(expr: &SqlExpr) -> Result<String> {
    if let SqlExpr::Function(f) = expr {
        let func = f
            .name
            .0
            .last()
            .map(|i| i.value.to_ascii_lowercase())
            .unwrap_or_default();
        let arg = first_arg(f).map(|a| match a {
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => "*".to_string(),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                as_simple_column(e).unwrap_or_else(|_| "expr".to_string())
            }
            _ => "expr".to_string(),
        });
        if let Some(name) = agg_output_name(&func, &arg.unwrap_or_default()) {
            return Ok(name);
        }
    }
    as_simple_column(expr)
}

fn as_simple_column(expr: &SqlExpr) -> Result<String> {
    match expr {
        SqlExpr::Identifier(ident) => Ok(ident.value.clone()),
        SqlExpr::CompoundIdentifier(parts) if parts.len() == 1 => Ok(parts[0].value.clone()),
        other => {
            bail!("unsupported projection expression {other:?}: use a plain column or an aggregate")
        }
    }
}

fn apply_order(pipeline: &mut Pipeline, query: &Query) -> Result<()> {
    let Some(order_by) = &query.order_by else {
        return Ok(());
    };
    if order_by.exprs.is_empty() {
        return Ok(());
    }
    let mut specs: Vec<(String, bool)> = Vec::new();
    for ob in &order_by.exprs {
        let col = as_simple_column(&ob.expr)?;
        let desc = matches!(ob.asc, Some(false));
        if ob.nulls_first.is_some() {
            bail!("NULLS FIRST/LAST is not supported");
        }
        specs.push((col, desc));
    }
    let all_desc = specs.iter().all(|(_, d)| *d);
    let all_asc = specs.iter().all(|(_, d)| !*d);
    let columns: Vec<&str> = specs.iter().map(|(c, _)| c.as_str()).collect();
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
fn collect_identifiers(expr: &SqlExpr) -> Vec<String> {
    fn walk(expr: &SqlExpr, out: &mut Vec<String>) {
        match expr {
            SqlExpr::Identifier(ident) => out.push(ident.value.clone()),
            SqlExpr::CompoundIdentifier(parts) => {
                if let Some(last) = parts.last() {
                    out.push(last.value.clone());
                }
            }
            SqlExpr::BinaryOp { left, right, .. } => {
                walk(left, out);
                walk(right, out);
            }
            SqlExpr::UnaryOp { expr, .. } => walk(expr, out),
            SqlExpr::Nested(inner) => walk(inner, out),
            SqlExpr::IsFalse(e)
            | SqlExpr::IsTrue(e)
            | SqlExpr::IsNull(e)
            | SqlExpr::IsNotNull(e) => walk(e, out),
            _ => {}
        }
    }
    let mut out = Vec::new();
    walk(expr, &mut out);
    out
}

/// Convert a SQL WHERE expression into the engine's expression text.
fn convert_expr(expr: &SqlExpr) -> Result<String> {
    Ok(match expr {
        SqlExpr::Identifier(ident) => ident.value.clone(),
        SqlExpr::Value(v) => match v {
            SqlValue::Number(n, _) => n.clone(),
            SqlValue::SingleQuotedString(s) => format!("'{}'", s.replace('\'', "''")),
            SqlValue::DoubleQuotedString(s) => format!("'{}'", s.replace('\'', "''")),
            SqlValue::Boolean(b) => b.to_string(),
            SqlValue::Null => "null".to_string(),
            other => bail!("unsupported literal {other:?}"),
        },
        SqlExpr::BinaryOp { left, op, right } => {
            let core_op = match op {
                BinaryOperator::Eq => "==",
                BinaryOperator::NotEq => "!=",
                BinaryOperator::Lt => "<",
                BinaryOperator::LtEq => "<=",
                BinaryOperator::Gt => ">",
                BinaryOperator::GtEq => ">=",
                BinaryOperator::Plus => "+",
                BinaryOperator::Minus => "-",
                BinaryOperator::Multiply => "*",
                BinaryOperator::Divide => "/",
                BinaryOperator::Modulo => "%",
                BinaryOperator::And => "and",
                BinaryOperator::Or => "or",
                other => bail!("unsupported SQL operator {other}"),
            };
            format!(
                "{} {} {}",
                convert_expr(left)?,
                core_op,
                convert_expr(right)?
            )
        }
        SqlExpr::UnaryOp { op, expr } => {
            use sqlparser::ast::UnaryOperator as U;
            let core_op = match op {
                U::Not => "not ",
                U::Minus => "-",
                U::Plus => "+",
                other => bail!("unsupported unary operator {other:?}"),
            };
            format!("{}({})", core_op, convert_expr(expr)?)
        }
        SqlExpr::Nested(inner) => format!("({})", convert_expr(inner)?),
        SqlExpr::IsFalse(e) => format!("({}) == false", convert_expr(e)?),
        SqlExpr::IsTrue(e) => format!("({}) == true", convert_expr(e)?),
        SqlExpr::IsNotNull(e) => format!("not (({}) == null)", convert_expr(e)?),
        SqlExpr::IsNull(e) => format!("({}) == null", convert_expr(e)?),
        other => bail!("unsupported WHERE expression: {other:?}"),
    })
}

fn sql_number(e: &SqlExpr) -> Result<i64> {
    match e {
        SqlExpr::Value(v) => match v {
            SqlValue::Number(n, _) => Ok(n.parse::<i64>().context("LIMIT must be an integer")?),
            other => bail!("LIMIT must be a number, got {other:?}"),
        },
        other => bail!("LIMIT must be a plain number, got {other:?}"),
    }
}
