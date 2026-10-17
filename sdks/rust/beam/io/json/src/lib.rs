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

//! JSON and Beam rows: Beam schemas from JSON Schemas, and rows to and from JSON objects.
//!
//! Connectors that read or write JSON records use this crate. A pipeline step can also use it,
//! through `beam::io::json` with the `json` feature of `beam`.
//!
//! The schema mapping is the mapping of `JsonUtils` in the Java SDK, so a JSON Schema gives the
//! same Beam schema in both SDKs:
//!
//! | JSON Schema | Beam |
//! |---|---|
//! | `object` with `properties` | `ROW` |
//! | `integer` | `INT64` |
//! | `number` | `DOUBLE` |
//! | `string` | `STRING` |
//! | `boolean` | `BOOLEAN` |
//! | `array` with one `items` schema | `ARRAY` |
//!
//! The properties in `required` come first, in their order. They are not nullable unless the
//! property has `"nullable": true` or a `null` type. The other properties follow in name order
//! and are nullable. `$ref` and tuple arrays are not supported.
//!
//! A row becomes an object with one member for each field. `BYTES` values are base64 strings.
//! A map becomes an object, so its keys must be strings or integers. A logical type is written
//! and read as its representation.
//!
//! [`JsonReader`] reads each object directly into a row: the schema selects the field of each
//! member, and no intermediate JSON tree is built. [`write_json`] writes a row directly into a
//! buffer.

mod read;
mod schema;
mod write;

pub use read::JsonReader;
pub use schema::schema_from_json_schema;
pub use write::write_json;
