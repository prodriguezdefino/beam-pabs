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

//! [`RowWalk`]: a row schema compiled once to read many encoded rows, with no [`Row`] and no
//! copy of the field values.
//!
//! [`Row`]: crate::schema::Row

use super::decode::read_presence;
use super::encoding_order;
use super::header::{bit_is_null, read_row_header};
use crate::coders::standard::{
    VarIntCoder, decode_iterable, read_be_i32, read_bool, read_count, read_f32, read_f64, read_i8,
    read_i16, read_i32_varint, read_nested_bytes, read_timestamp,
};
use crate::coders::traits::CoderError;
use crate::schema::{AtomicType, FieldType, Schema, TypeInfo, URN_DECIMAL, URN_MILLIS_INSTANT};

/// One field value of an encoded row, borrowed from the row bytes.
///
/// A `millis_instant` value is `Int64` milliseconds. Other logical types give the value of
/// their representation type, except `decimal`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum WireValue<'a> {
    Boolean(bool),
    Byte(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    String(&'a str),
    Bytes(&'a [u8]),
    /// A `decimal` value: the scale and the big-endian two's-complement unscaled value.
    Decimal {
        scale: i64,
        unscaled: &'a [u8],
    },
}

/// A tree of operations with the shape of a row schema, which [`RowWalk::compile`] reads.
///
/// [`RowWalk`] and [`RowVisitor`] keep the row wire rules in this module, so a format crate
/// such as `apache-beam-io-avro` only writes its own bytes. They are in `beam::internals` for
/// those crates; they are not a pipeline API.
pub trait RowOps {
    /// What the visitor gets with each value.
    type Op;

    fn op(&self) -> Self::Op;
    /// The child at schema index `index` of a row, index 0 of an array or iterable (the
    /// element), or index 0 and 1 of a map (the key and the value).
    fn child(&self, index: usize) -> Option<&Self>;
}

/// Receives the values of an encoded row from [`RowWalk::walk`], in wire order.
pub trait RowVisitor {
    /// The operation that [`RowOps::op`] gave for the value.
    type Op;
    /// What [`begin_container`](Self::begin_container) gives to the matching end call.
    type Mark;
    /// The error of the visitor. A wire error converts into it.
    type Error: From<CoderError>;

    /// Receives a null row field or container element.
    fn null(&mut self, op: &Self::Op) -> Result<(), Self::Error>;
    /// Receives the start of a row field or container element that is not null. A map key
    /// gets no `null` or `present` call.
    fn present(&mut self, op: &Self::Op);
    /// Receives an atomic or logical value.
    fn value(&mut self, op: &Self::Op, value: WireValue<'_>) -> Result<(), Self::Error>;
    /// Starts an array, iterable or map. `count` is `None` for an iterable in chunks, whose
    /// count is known only at [`end_container`](Self::end_container).
    fn begin_container(
        &mut self,
        op: &Self::Op,
        count: Option<usize>,
    ) -> Result<Self::Mark, Self::Error>;
    /// Ends the container that `mark` started. It had `count` elements.
    fn end_container(
        &mut self,
        op: &Self::Op,
        mark: Self::Mark,
        count: usize,
    ) -> Result<(), Self::Error>;
}

/// A row schema compiled with one operation for each value.
pub struct RowWalk<T>(WalkRow<T>);

/// The fields of a row in wire order. The index of a field is its null bitmask position.
struct WalkRow<T> {
    fields: Box<[WalkNode<T>]>,
}

struct WalkNode<T> {
    op: T,
    /// A nullable container element has a presence byte.
    nullable: bool,
    wire: Wire<T>,
}

/// The wire form of a value, after logical types resolve.
enum Wire<T> {
    Boolean,
    Byte,
    Int16,
    Int32,
    Int64,
    Float,
    Double,
    String,
    Bytes,
    MillisInstant,
    Decimal,
    Iterable(Box<WalkNode<T>>),
    Map(Box<WalkNode<T>>, Box<WalkNode<T>>),
    Row(WalkRow<T>),
}

impl<T> RowWalk<T> {
    /// Compiles `schema` with the operations of `root`, whose children are the row fields.
    ///
    /// # Errors
    ///
    /// Fails when `root` has no child for a value of `schema`.
    pub fn compile<O: RowOps<Op = T>>(schema: &Schema, root: &O) -> Result<Self, CoderError> {
        compile_row(schema, root).map(Self)
    }

    /// Reads one encoded row from `reader` and gives its values to `visitor`.
    ///
    /// # Errors
    ///
    /// Fails when the bytes do not hold a row of the compiled schema, or the visitor fails.
    pub fn walk<V: RowVisitor<Op = T>>(
        &self,
        reader: &mut &[u8],
        visitor: &mut V,
    ) -> Result<(), V::Error> {
        walk_row(&self.0, reader, visitor)
    }
}

fn compile_row<O: RowOps>(schema: &Schema, ops: &O) -> Result<WalkRow<O::Op>, CoderError> {
    let order = encoding_order(schema);
    let fields = (0..schema.num_fields())
        .map(|position| {
            let index = order.as_ref().map_or(position, |o| o[position]);
            compile_node(&schema.fields[index].field_type, child(ops, index)?)
        })
        .collect::<Result<_, _>>()?;
    Ok(WalkRow { fields })
}

fn compile_node<O: RowOps>(field_type: &FieldType, ops: &O) -> Result<WalkNode<O::Op>, CoderError> {
    Ok(WalkNode {
        op: ops.op(),
        nullable: field_type.nullable,
        wire: compile_wire(field_type, ops)?,
    })
}

fn compile_wire<O: RowOps>(field_type: &FieldType, ops: &O) -> Result<Wire<O::Op>, CoderError> {
    Ok(match &field_type.type_info {
        TypeInfo::Atomic(atomic) => match atomic {
            AtomicType::Byte => Wire::Byte,
            AtomicType::Int16 => Wire::Int16,
            AtomicType::Int32 => Wire::Int32,
            AtomicType::Int64 => Wire::Int64,
            AtomicType::Float => Wire::Float,
            AtomicType::Double => Wire::Double,
            AtomicType::String => Wire::String,
            AtomicType::Boolean => Wire::Boolean,
            AtomicType::Bytes => Wire::Bytes,
        },
        TypeInfo::Array(elem_type) | TypeInfo::Iterable(elem_type) => {
            Wire::Iterable(Box::new(compile_node(elem_type, child(ops, 0)?)?))
        }
        TypeInfo::Map(k_type, v_type) => Wire::Map(
            Box::new(compile_node(k_type, child(ops, 0)?)?),
            Box::new(compile_node(v_type, child(ops, 1)?)?),
        ),
        TypeInfo::Row(inner_schema) => Wire::Row(compile_row(inner_schema, ops)?),
        TypeInfo::Logical {
            urn,
            representation,
            ..
        } => match urn.as_str() {
            URN_DECIMAL => Wire::Decimal,
            URN_MILLIS_INSTANT => Wire::MillisInstant,
            _ => compile_wire(representation, ops)?,
        },
    })
}

fn child<O: RowOps>(ops: &O, index: usize) -> Result<&O, CoderError> {
    ops.child(index).ok_or_else(|| {
        CoderError::Format(format!("The row operations have no child at index {index}"))
    })
}

fn walk_row<V: RowVisitor>(
    row: &WalkRow<V::Op>,
    reader: &mut &[u8],
    visitor: &mut V,
) -> Result<(), V::Error> {
    let null_bits = read_row_header(row.fields.len(), reader)?;
    if null_bits.is_empty() {
        return row.fields.iter().try_for_each(|field| {
            visitor.present(&field.op);
            walk_value(field, reader, visitor)
        });
    }
    row.fields
        .iter()
        .enumerate()
        .try_for_each(|(position, field)| {
            if bit_is_null(null_bits, position) {
                visitor.null(&field.op)
            } else {
                visitor.present(&field.op);
                walk_value(field, reader, visitor)
            }
        })
}

fn walk_value<V: RowVisitor>(
    node: &WalkNode<V::Op>,
    reader: &mut &[u8],
    visitor: &mut V,
) -> Result<(), V::Error> {
    let io = CoderError::from;
    let value = match &node.wire {
        Wire::Boolean => WireValue::Boolean(read_bool(reader)?),
        Wire::Byte => WireValue::Byte(read_i8(reader).map_err(io)?),
        Wire::Int16 => WireValue::Int16(read_i16(reader).map_err(io)?),
        Wire::Int32 => WireValue::Int32(read_i32_varint(reader)?),
        Wire::Int64 => WireValue::Int64(VarIntCoder::decode_varint(reader).map_err(io)?),
        Wire::Float => WireValue::Float(read_f32(reader).map_err(io)?),
        Wire::Double => WireValue::Double(read_f64(reader).map_err(io)?),
        Wire::String => WireValue::String(
            std::str::from_utf8(read_nested_bytes(reader)?)
                .map_err(|e| CoderError::Format(format!("Invalid UTF-8 string: {e}")))?,
        ),
        Wire::Bytes => WireValue::Bytes(read_nested_bytes(reader)?),
        Wire::MillisInstant => WireValue::Int64(read_timestamp(reader).map_err(io)?),
        Wire::Decimal => WireValue::Decimal {
            scale: VarIntCoder::decode_varint(reader).map_err(io)?,
            unscaled: read_nested_bytes(reader)?,
        },
        Wire::Iterable(element) => return walk_iterable(node, element, reader, visitor),
        Wire::Map(key, value) => {
            let count = read_count(reader)?;
            let mark = visitor.begin_container(&node.op, Some(count))?;
            (0..count).try_for_each(|_| {
                walk_value(key, reader, visitor)?;
                walk_element(value, reader, visitor)
            })?;
            return visitor.end_container(&node.op, mark, count);
        }
        Wire::Row(row) => return walk_row(row, reader, visitor),
    };
    visitor.value(&node.op, value)
}

fn walk_iterable<V: RowVisitor>(
    node: &WalkNode<V::Op>,
    element: &WalkNode<V::Op>,
    reader: &mut &[u8],
    visitor: &mut V,
) -> Result<(), V::Error> {
    // Read the count without a move, so that a counted iterable starts with it.
    let count = read_be_i32(&mut &**reader).map_err(CoderError::from)?;
    let mark = visitor.begin_container(&node.op, usize::try_from(count).ok())?;
    // The iterable decoder carries only a `CoderError`, so keep a visitor error here.
    let mut visitor_error = None;
    let elements = decode_iterable(reader, |r| {
        walk_element(element, r, visitor).map_err(|error| {
            visitor_error = Some(error);
            CoderError::Format(String::new())
        })
    });
    if let Some(error) = visitor_error {
        return Err(error);
    }
    visitor.end_container(&node.op, mark, elements?.len())
}

fn walk_element<V: RowVisitor>(
    node: &WalkNode<V::Op>,
    reader: &mut &[u8],
    visitor: &mut V,
) -> Result<(), V::Error> {
    if !node.nullable || read_presence(reader)? {
        visitor.present(&node.op);
        walk_value(node, reader, visitor)
    } else {
        visitor.null(&node.op)
    }
}
