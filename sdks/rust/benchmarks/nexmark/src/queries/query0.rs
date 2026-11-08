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

//! Query 0 (`PASSTHROUGH`): coder round-trip benchmark transform.

use beam::coders::{Coder, Context};
use beam::prelude::*;
use serde::{Deserialize, Serialize};

use crate::model::{Event, EventCoder};

/// Query 0 transform: encodes and decodes every [`Event`] through [`EventCoder`].
#[derive(Clone, Copy, Debug, Default)]
pub struct Query0;

impl PTransform<PCollection<Event>> for Query0 {
    type Output = PCollection<Event>;

    fn expand(self, input: &PCollection<Event>) -> PCollection<Event> {
        input.par_do("Serialize", Query0Fn::default())
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Register)]
struct Query0Fn {
    #[serde(skip)]
    buf: Vec<u8>,
}

impl Clone for Query0Fn {
    fn clone(&self) -> Self {
        Self { buf: Vec::new() }
    }
}

impl DoFn for Query0Fn {
    type In = Event;
    type Out = Event;

    fn process_element(
        &mut self,
        element: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        self.buf.clear();
        EventCoder.encode(&element, &mut self.buf, Context::WholeStream)?;
        let byte_len = i64::try_from(self.buf.len()).unwrap_or(i64::MAX);
        Metrics::counter("Query0", "bytes").inc_by(byte_len);
        Metrics::counter("Query0", "events").inc();
        let mut reader: &[u8] = &self.buf;
        let decoded = EventCoder.decode(&mut reader, Context::WholeStream)?;
        ctx.emit(decoded)
    }
}
