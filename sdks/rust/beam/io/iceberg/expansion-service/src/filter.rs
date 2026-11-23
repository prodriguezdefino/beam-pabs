/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! The SQL `filter` of an Iceberg read, as an Iceberg predicate.

use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value, ValueWithSpan};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

/// Parses a SQL boolean expression into a predicate. The expression can use `=`, `!=`, `<>`,
/// `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`, `IS [NOT] NULL`, `[NOT] IN (...)` and a prefix
/// `LIKE 'abc%'`. Each comparison has one column and one literal.
///
/// # Errors
///
/// Fails if the expression does not parse or uses another form.
pub fn parse_filter(sql: &str) -> Result<Predicate, String> {
    let invalid = |e: &dyn std::fmt::Display| format!("Invalid filter '{sql}': {e}");
    let mut parser = Parser::new(&GenericDialect {})
        .try_with_sql(sql)
        .map_err(|e| invalid(&e))?;
    let expr = parser.parse_expr().map_err(|e| invalid(&e))?;
    let next = parser.peek_token().token;
    if next != Token::EOF {
        return Err(invalid(&format!("unexpected '{next}'")));
    }
    predicate(&expr).map_err(|e| invalid(&e))
}

fn predicate(expr: &Expr) -> Result<Predicate, String> {
    match expr {
        Expr::Nested(inner) => predicate(inner),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => Ok(!predicate(expr)?),
        Expr::BinaryOp { left, op, right } => binary(left, op, right),
        Expr::IsNull(column) => Ok(Reference::new(column_name(column)?).is_null()),
        Expr::IsNotNull(column) => Ok(Reference::new(column_name(column)?).is_not_null()),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let column = Reference::new(column_name(expr)?);
            let values = list.iter().map(literal).collect::<Result<Vec<_>, _>>()?;
            Ok(if *negated {
                column.is_not_in(values)
            } else {
                column.is_in(values)
            })
        }
        Expr::Like {
            negated,
            any: false,
            expr,
            pattern,
            escape_char: None,
        } => like(&column_name(expr)?, pattern, *negated),
        other => Err(format!("'{other}' is not supported")),
    }
}

fn binary(left: &Expr, op: &BinaryOperator, right: &Expr) -> Result<Predicate, String> {
    match op {
        BinaryOperator::And => return Ok(predicate(left)?.and(predicate(right)?)),
        BinaryOperator::Or => return Ok(predicate(left)?.or(predicate(right)?)),
        _ => {}
    }
    // A literal on the left reverses the comparison: `5 < id` is `id > 5`.
    let (column, value, reversed) = match (column_name(left), column_name(right)) {
        (Ok(column), Err(_)) => (column, literal(right)?, false),
        (Err(_), Ok(column)) => (column, literal(left)?, true),
        (Ok(_), Ok(_)) | (Err(_), Err(_)) => {
            return Err("a comparison needs one column and one literal".to_string());
        }
    };
    let column = Reference::new(column);
    match (op, reversed) {
        (BinaryOperator::Eq, _) => Ok(column.equal_to(value)),
        (BinaryOperator::NotEq, _) => Ok(column.not_equal_to(value)),
        (BinaryOperator::Lt, false) | (BinaryOperator::Gt, true) => Ok(column.less_than(value)),
        (BinaryOperator::LtEq, false) | (BinaryOperator::GtEq, true) => {
            Ok(column.less_than_or_equal_to(value))
        }
        (BinaryOperator::Gt, false) | (BinaryOperator::Lt, true) => Ok(column.greater_than(value)),
        (BinaryOperator::GtEq, false) | (BinaryOperator::LtEq, true) => {
            Ok(column.greater_than_or_equal_to(value))
        }
        (other, _) => Err(format!("the operator '{other}' is not supported")),
    }
}

/// `LIKE` with no wildcard is `=`, and with one trailing `%` it is a prefix match.
fn like(column: &str, pattern: &Expr, negated: bool) -> Result<Predicate, String> {
    let Expr::Value(ValueWithSpan {
        value: Value::SingleQuotedString(pattern),
        ..
    }) = pattern
    else {
        return Err(format!("the LIKE pattern '{pattern}' is not a string"));
    };
    let column = Reference::new(column);
    if pattern.contains('_') {
        return Err(format!("the wildcard '_' in '{pattern}' is not supported"));
    }
    match pattern.strip_suffix('%') {
        None if !pattern.contains('%') => Ok(if negated {
            column.not_equal_to(Datum::string(pattern))
        } else {
            column.equal_to(Datum::string(pattern))
        }),
        Some(prefix) if !prefix.is_empty() && !prefix.contains('%') => Ok(if negated {
            column.not_starts_with(Datum::string(prefix))
        } else {
            column.starts_with(Datum::string(prefix))
        }),
        Some(_) | None => Err(format!(
            "the LIKE pattern '{pattern}' is not supported: use a prefix such as 'abc%'"
        )),
    }
}

fn column_name(expr: &Expr) -> Result<String, String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => Ok(parts
            .iter()
            .map(|part| part.value.as_str())
            .collect::<Vec<_>>()
            .join(".")),
        Expr::Nested(inner) => column_name(inner),
        other => Err(format!("'{other}' is not a column")),
    }
}

fn literal(expr: &Expr) -> Result<Datum, String> {
    match expr {
        Expr::Nested(inner) => literal(inner),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            Expr::Value(ValueWithSpan {
                value: Value::Number(number, _),
                ..
            }) => number_literal(&format!("-{number}")),
            other => Err(format!("'-{other}' is not a number")),
        },
        Expr::Value(ValueWithSpan { value, .. }) => match value {
            Value::Number(number, _) => number_literal(number),
            Value::SingleQuotedString(text) => Ok(Datum::string(text)),
            Value::Boolean(flag) => Ok(Datum::bool(*flag)),
            other => Err(format!("the literal '{other}' is not supported")),
        },
        other => Err(format!("'{other}' is not a literal")),
    }
}

/// An integer literal is a `long`, and a literal with a fraction or an exponent a `double`.
fn number_literal(number: &str) -> Result<Datum, String> {
    if number.contains(['.', 'e', 'E']) {
        number
            .parse::<f64>()
            .map(Datum::double)
            .map_err(|e| format!("'{number}': {e}"))
    } else {
        number
            .parse::<i64>()
            .map(Datum::long)
            .map_err(|e| format!("'{number}': {e}"))
    }
}
