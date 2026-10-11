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

//! Tests for [`beam::Error`], the error type that user hooks return.

use beam::{Error, Result};

#[test]
fn converts_from_std_errors_and_strings() {
    let io: Error = std::io::Error::other("disk full").into();
    assert_eq!(io.to_string(), "disk full");
    assert!(
        io.source()
            .and_then(|s| s.downcast_ref::<std::io::Error>())
            .is_some()
    );

    let owned: Error = String::from("owned").into();
    let borrowed: Error = "borrowed".into();
    assert_eq!(owned.to_string(), "owned");
    assert_eq!(borrowed.to_string(), "borrowed");
    assert!(Error::msg("plain").source().is_none());
}

#[test]
fn context_prefixes_and_keeps_source() {
    let err = Error::from(std::io::Error::other("not found")).context("reading config");
    assert_eq!(err.to_string(), "reading config: not found");
    assert!(err.source().is_some());
}

#[test]
fn question_mark_into_string_plumbing() {
    fn user() -> Result<i32> {
        Ok("nope".parse::<i32>()?)
    }
    fn plumbing() -> std::result::Result<i32, String> {
        Ok(user()?)
    }
    assert_eq!(plumbing().unwrap_err(), "invalid digit found in string");
}
