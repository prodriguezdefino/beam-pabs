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

//! Messages and gRPC client of `google.bigtable.v2`, generated from the vendored googleapis
//! protos. Bytes fields are [`bytes::Bytes`], so decoded chunks share the gRPC buffer.

mod range;

pub use self::range::RowRangeExt;

pub use googleapis::google::bigtable::v2::*;
pub use googleapis::google::protobuf::{Any, BytesValue, Duration, StringValue};
pub use googleapis::google::rpc::{RetryInfo, Status};
