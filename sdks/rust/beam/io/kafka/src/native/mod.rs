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

//! The native Kafka connector, on librdkafka, with the `native` feature.
//!
//! [`read`] reads the records of a topic into rows, and [`write`] writes rows to a topic.
//! [`format`] maps record values to rows. [`client`] holds the connection to a cluster, and
//! [`auth`] the Google authentication of Google Cloud Managed Service for Apache Kafka.

pub mod auth;
pub mod client;
pub mod codec;
pub mod context;
pub mod read;
pub mod write;
