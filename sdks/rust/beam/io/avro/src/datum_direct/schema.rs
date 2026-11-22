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

//! Maps an Avro record schema and a Beam [`Schema`] to the write plan of the direct encoder.

use std::collections::HashMap;
use std::sync::Arc;

use apache_avro::Schema as AvroSchema;
use apache_avro::schema::{Name, ResolvedSchema};
use beam::internals::RowOps;
use beam::schema::{
    AtomicType, FieldType, Schema, TypeInfo, URN_DATE, URN_DECIMAL, URN_MICROS_INSTANT,
    URN_MILLIS_INSTANT,
};

const ROOT_ROW: &str = "row";

/// How to write one Beam value as Avro. The plan tree has the shape of the Beam schema.
pub(super) struct ValuePlan {
    pub(super) op: WriteOp,
    /// The fields of a record or `micros_instant` row in schema order, the item of an array,
    /// or the key and the value of a map.
    pub(super) children: Box<[ValuePlan]>,
}

/// How to write one value, without the plans of its children.
#[derive(Clone)]
pub(super) struct WriteOp {
    /// Field path for error messages.
    pub(super) name: String,
    pub(super) beam_nullable: bool,
    pub(super) avro_null: AvroNull,
    pub(super) kind: ValueKind,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum AvroNull {
    /// Non-union Avro type: no branch tag, null is an error.
    None,
    /// `["null", T]`: null writes `0x00`, present writes `0x02`.
    NullFirst,
    /// `[T, "null"]`: present writes `0x00`, null writes `0x02`.
    NullSecond,
}

#[derive(Clone)]
pub(super) enum ValueKind {
    Boolean,
    ByteToInt,
    Int16ToInt,
    Int32ToInt,
    Int64ToLong,
    Float,
    FloatToDouble,
    Double,
    String,
    Bytes,
    Enum(Arc<HashMap<String, i32>>),
    Date,
    MillisInstant,
    /// A `micros_instant` row. Its children are its `seconds` and `micros` fields.
    MicrosInstant,
    InstantPart(InstantPart),
    Array,
    Map,
    Record,
}

/// A field of a `micros_instant` row. The Avro value needs both fields.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum InstantPart {
    Seconds,
    Micros,
}

impl RowOps for ValuePlan {
    type Op = WriteOp;

    fn op(&self) -> WriteOp {
        self.op.clone()
    }

    fn child(&self, index: usize) -> Option<&Self> {
        self.children.get(index)
    }
}

/// Compiles the plan of the root record, or returns `None` outside the supported subset.
pub(super) fn compile(avro_schema: &str, row_schema: &Schema) -> Option<ValuePlan> {
    let parsed = AvroSchema::parse_str(avro_schema).ok()?;
    let resolved = ResolvedSchema::try_from(&parsed).ok()?;
    let compiler = Compiler {
        names: resolved.get_names(),
    };
    let AvroSchema::Record(record) = &parsed else {
        return None;
    };
    let avro_fields: Vec<&AvroSchema> = record.fields.iter().map(|f| &f.schema).collect();
    let children = compiler.record(ROOT_ROW, row_schema, &avro_fields)?;
    Some(ValuePlan {
        op: WriteOp {
            name: ROOT_ROW.to_string(),
            beam_nullable: false,
            avro_null: AvroNull::None,
            kind: ValueKind::Record,
        },
        children,
    })
}

/// Compiles plans with the named types of one parsed Avro schema.
struct Compiler<'a> {
    names: &'a HashMap<Name, &'a AvroSchema>,
}

impl Compiler<'_> {
    /// Follows a named reference to its definition.
    fn resolve<'s>(&'s self, avro: &'s AvroSchema) -> Option<&'s AvroSchema> {
        match avro {
            AvroSchema::Ref { name } => self.names.get(name).copied(),
            other => Some(other),
        }
    }

    fn record(
        &self,
        parent: &str,
        schema: &Schema,
        avro_fields: &[&AvroSchema],
    ) -> Option<Box<[ValuePlan]>> {
        if schema.fields.len() != avro_fields.len() || has_reordered_positions(schema) {
            return None;
        }
        schema
            .fields
            .iter()
            .zip(avro_fields)
            .map(|(beam_field, avro_field)| {
                let name = if parent == ROOT_ROW {
                    beam_field.name.clone()
                } else {
                    format!("{parent}.{}", beam_field.name)
                };
                self.value(name, &beam_field.field_type, avro_field)
            })
            .collect()
    }

    fn value(&self, name: String, beam_type: &FieldType, avro: &AvroSchema) -> Option<ValuePlan> {
        let (avro_null, inner) = match self.resolve(avro)? {
            AvroSchema::Union(union) => match union.variants() {
                [AvroSchema::Null, inner] => (AvroNull::NullFirst, inner),
                [inner, AvroSchema::Null] => (AvroNull::NullSecond, inner),
                _ => return None,
            },
            other => (AvroNull::None, other),
        };
        let inner = self.resolve(inner)?;
        if matches!(inner, AvroSchema::Null | AvroSchema::Union(_)) {
            return None;
        }
        let (kind, children) = self.kind(&name, &beam_type.type_info, inner)?;
        Some(ValuePlan {
            op: WriteOp {
                name,
                beam_nullable: beam_type.nullable,
                avro_null,
                kind,
            },
            children,
        })
    }

    fn kind(
        &self,
        name: &str,
        type_info: &TypeInfo,
        avro: &AvroSchema,
    ) -> Option<(ValueKind, Box<[ValuePlan]>)> {
        let leaf = |kind| Some((kind, Box::default()));
        match unwrap_custom_logical(type_info)? {
            TypeInfo::Atomic(atomic) => atomic_kind(*atomic, avro).and_then(leaf),
            TypeInfo::Array(beam_item) | TypeInfo::Iterable(beam_item) => {
                let AvroSchema::Array(array) = avro else {
                    return None;
                };
                let item = self.value(format!("{name}[]"), beam_item, &array.items)?;
                Some((ValueKind::Array, Box::new([item])))
            }
            TypeInfo::Map(beam_key, beam_value) => {
                let AvroSchema::Map(map) = avro else {
                    return None;
                };
                if !is_string_key(beam_key) {
                    return None;
                }
                let key = ValuePlan {
                    op: WriteOp {
                        name: format!("{name}.key"),
                        beam_nullable: false,
                        avro_null: AvroNull::None,
                        kind: ValueKind::String,
                    },
                    children: Box::default(),
                };
                let value = self.value(format!("{name}.value"), beam_value, &map.types)?;
                Some((ValueKind::Map, Box::new([key, value])))
            }
            TypeInfo::Row(beam_schema) => {
                let AvroSchema::Record(record) = avro else {
                    return None;
                };
                let avro_fields: Vec<&AvroSchema> =
                    record.fields.iter().map(|f| &f.schema).collect();
                let fields = self.record(name, beam_schema, &avro_fields)?;
                Some((ValueKind::Record, fields))
            }
            TypeInfo::Logical {
                urn,
                representation,
                ..
            } => logical_kind(name, urn, representation, avro),
        }
    }
}

fn atomic_kind(atomic: AtomicType, avro: &AvroSchema) -> Option<ValueKind> {
    let int_or_long = matches!(avro, AvroSchema::Int | AvroSchema::Long);
    match atomic {
        AtomicType::Boolean => matches!(avro, AvroSchema::Boolean).then_some(ValueKind::Boolean),
        AtomicType::Byte => int_or_long.then_some(ValueKind::ByteToInt),
        AtomicType::Int16 => int_or_long.then_some(ValueKind::Int16ToInt),
        AtomicType::Int32 => int_or_long.then_some(ValueKind::Int32ToInt),
        AtomicType::Int64 => matches!(avro, AvroSchema::Long).then_some(ValueKind::Int64ToLong),
        AtomicType::Float => match avro {
            AvroSchema::Float => Some(ValueKind::Float),
            AvroSchema::Double => Some(ValueKind::FloatToDouble),
            _ => None,
        },
        AtomicType::Double => matches!(avro, AvroSchema::Double).then_some(ValueKind::Double),
        AtomicType::String => match avro {
            AvroSchema::String => Some(ValueKind::String),
            AvroSchema::Enum(schema) => compile_enum(&schema.symbols),
            _ => None,
        },
        AtomicType::Bytes => matches!(avro, AvroSchema::Bytes).then_some(ValueKind::Bytes),
    }
}

/// Compiles the three logical types that [`unwrap_custom_logical`] keeps.
fn logical_kind(
    name: &str,
    urn: &str,
    representation: &FieldType,
    avro: &AvroSchema,
) -> Option<(ValueKind, Box<[ValuePlan]>)> {
    let is_int64 = !representation.nullable
        && matches!(
            representation.type_info,
            TypeInfo::Atomic(AtomicType::Int64)
        );
    let millis = matches!(
        avro,
        AvroSchema::TimestampMillis | AvroSchema::LocalTimestampMillis
    );
    let micros = matches!(
        avro,
        AvroSchema::TimestampMicros | AvroSchema::LocalTimestampMicros
    );
    match urn {
        URN_DATE if is_int64 && matches!(avro, AvroSchema::Date) => {
            Some((ValueKind::Date, Box::default()))
        }
        URN_MILLIS_INSTANT if is_int64 && millis => {
            Some((ValueKind::MillisInstant, Box::default()))
        }
        URN_MICROS_INSTANT if micros => compile_micros_instant(name, representation),
        _ => None,
    }
}

/// Avro writes record fields in schema order, so the row bytes must use the same order.
fn has_reordered_positions(schema: &Schema) -> bool {
    let position = |index: usize| schema.fields[index].encoding_position.unwrap_or(0);
    schema.encoding_positions_set
        && (1..schema.fields.len()).any(|index| position(index - 1) > position(index))
}

/// Returns the representation of a logical type that the plan does not know, or `None` when
/// the direct path cannot write it.
fn unwrap_custom_logical(type_info: &TypeInfo) -> Option<&TypeInfo> {
    let TypeInfo::Logical {
        urn,
        representation,
        ..
    } = type_info
    else {
        return Some(type_info);
    };
    match urn.as_str() {
        URN_DATE | URN_MILLIS_INSTANT | URN_MICROS_INSTANT => Some(type_info),
        URN_DECIMAL => None,
        _ if representation.nullable => None,
        _ => unwrap_custom_logical(&representation.type_info),
    }
}

fn is_string_key(key: &FieldType) -> bool {
    !key.nullable
        && unwrap_custom_logical(&key.type_info)
            .is_some_and(|t| matches!(t, TypeInfo::Atomic(AtomicType::String)))
}

fn compile_enum(symbols: &[String]) -> Option<ValueKind> {
    let indices = symbols
        .iter()
        .enumerate()
        .map(|(index, symbol)| Some((symbol.clone(), i32::try_from(index).ok()?)))
        .collect::<Option<HashMap<_, _>>>()?;
    Some(ValueKind::Enum(Arc::new(indices)))
}

/// Compiles a `micros_instant` row: an `INT64` `seconds` field, then an `INT64` or `INT32`
/// `micros` field. The row bytes can hold the two fields in either order.
fn compile_micros_instant(
    name: &str,
    representation: &FieldType,
) -> Option<(ValueKind, Box<[ValuePlan]>)> {
    if representation.nullable {
        return None;
    }
    let TypeInfo::Row(schema) = &representation.type_info else {
        return None;
    };
    let [seconds, micros] = schema.fields.as_slice() else {
        return None;
    };
    let valid = !seconds.field_type.nullable
        && !micros.field_type.nullable
        && matches!(
            seconds.field_type.type_info,
            TypeInfo::Atomic(AtomicType::Int64)
        )
        && matches!(
            micros.field_type.type_info,
            TypeInfo::Atomic(AtomicType::Int64 | AtomicType::Int32)
        );
    let part = |part| ValuePlan {
        op: WriteOp {
            name: name.to_string(),
            beam_nullable: false,
            avro_null: AvroNull::None,
            kind: ValueKind::InstantPart(part),
        },
        children: Box::default(),
    };
    valid.then(|| {
        let parts = [part(InstantPart::Seconds), part(InstantPart::Micros)];
        (
            ValueKind::MicrosInstant,
            Box::new(parts) as Box<[ValuePlan]>,
        )
    })
}
