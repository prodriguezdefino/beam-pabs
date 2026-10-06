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

//! A check for floats that have no JSON form.
//!
//! `serde_json` writes NaN and infinity as `null`, so the Managed configuration could lose a
//! value with no error. The check walks the value through serde and finds such floats first.

use std::fmt;

use serde::Serialize;
use serde::ser;

/// Returns an error if `value` holds a NaN or infinite `f32` or `f64`.
pub(crate) fn check_finite(value: &impl Serialize) -> Result<(), NonFinite> {
    value.serialize(FiniteCheck)
}

/// The error for a float that has no JSON form.
#[derive(Debug)]
pub(crate) struct NonFinite(String);

impl fmt::Display for NonFinite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for NonFinite {}

impl ser::Error for NonFinite {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self(msg.to_string())
    }
}

fn float(value: f64) -> Result<(), NonFinite> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(NonFinite(format!("{value} has no JSON form")))
    }
}

/// A serializer that keeps no output and fails on a non-finite float.
struct FiniteCheck;

impl ser::Serializer for FiniteCheck {
    type Ok = ();
    type Error = NonFinite;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, _: bool) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_i8(self, _: i8) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_i16(self, _: i16) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_i32(self, _: i32) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_i64(self, _: i64) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_u8(self, _: u8) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_u16(self, _: u16) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_u32(self, _: u32) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_u64(self, _: u64) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_f32(self, v: f32) -> Result<(), NonFinite> {
        float(f64::from(v))
    }
    fn serialize_f64(self, v: f64) -> Result<(), NonFinite> {
        float(v)
    }
    fn serialize_char(self, _: char) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_str(self, _: &str) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_bytes(self, _: &[u8]) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_none(self) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<(), NonFinite> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
    ) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<(), NonFinite> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        value: &T,
    ) -> Result<(), NonFinite> {
        value.serialize(self)
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Self, NonFinite> {
        Ok(self)
    }
    fn serialize_tuple(self, _: usize) -> Result<Self, NonFinite> {
        Ok(self)
    }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self, NonFinite> {
        Ok(self)
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self, NonFinite> {
        Ok(self)
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self, NonFinite> {
        Ok(self)
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self, NonFinite> {
        Ok(self)
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self, NonFinite> {
        Ok(self)
    }
}

impl ser::SerializeSeq for FiniteCheck {
    type Ok = ();
    type Error = NonFinite;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), NonFinite> {
        value.serialize(FiniteCheck)
    }
    fn end(self) -> Result<(), NonFinite> {
        Ok(())
    }
}

impl ser::SerializeTuple for FiniteCheck {
    type Ok = ();
    type Error = NonFinite;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), NonFinite> {
        value.serialize(FiniteCheck)
    }
    fn end(self) -> Result<(), NonFinite> {
        Ok(())
    }
}

impl ser::SerializeTupleStruct for FiniteCheck {
    type Ok = ();
    type Error = NonFinite;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), NonFinite> {
        value.serialize(FiniteCheck)
    }
    fn end(self) -> Result<(), NonFinite> {
        Ok(())
    }
}

impl ser::SerializeTupleVariant for FiniteCheck {
    type Ok = ();
    type Error = NonFinite;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), NonFinite> {
        value.serialize(FiniteCheck)
    }
    fn end(self) -> Result<(), NonFinite> {
        Ok(())
    }
}

impl ser::SerializeMap for FiniteCheck {
    type Ok = ();
    type Error = NonFinite;
    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), NonFinite> {
        key.serialize(FiniteCheck)
    }
    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), NonFinite> {
        value.serialize(FiniteCheck)
    }
    fn end(self) -> Result<(), NonFinite> {
        Ok(())
    }
}

impl ser::SerializeStruct for FiniteCheck {
    type Ok = ();
    type Error = NonFinite;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), NonFinite> {
        value
            .serialize(FiniteCheck)
            .map_err(|NonFinite(e)| NonFinite(format!("field '{key}': {e}")))
    }
    fn end(self) -> Result<(), NonFinite> {
        Ok(())
    }
}

impl ser::SerializeStructVariant for FiniteCheck {
    type Ok = ();
    type Error = NonFinite;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), NonFinite> {
        value
            .serialize(FiniteCheck)
            .map_err(|NonFinite(e)| NonFinite(format!("field '{key}': {e}")))
    }
    fn end(self) -> Result<(), NonFinite> {
        Ok(())
    }
}
