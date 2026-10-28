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

//! A sink for the benchmarks of one handler.

use std::hint::black_box;

use beam::coders::WindowedHeader;
use beam::internals::ElementSink;

/// Drops every element. Keeps the header and the size, so the work is not optimized out.
pub(crate) struct DropSink;

impl ElementSink for DropSink {
    fn push(&mut self, element: Vec<u8>) -> Result<(), String> {
        black_box(element);
        Ok(())
    }

    fn push_windowed_bytes(
        &mut self,
        header: &WindowedHeader,
        element: &[u8],
    ) -> Result<(), String> {
        black_box((header.as_bytes().len(), element.len()));
        Ok(())
    }
}
