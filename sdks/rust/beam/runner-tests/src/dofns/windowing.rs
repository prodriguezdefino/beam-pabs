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

//! DoFns that set and read element timestamps and windows.

use beam::prelude::*;
use beam::transforms::{ProcessContext, Windowed};

#[derive(Clone, Default)]
pub struct AssignTimestampDoFn;

impl DoFn for AssignTimestampDoFn {
    type In = (String, (i64, i64));
    type Out = (String, i64);

    fn process_element(
        &mut self,
        element: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let (k, (v, ts)) = element;
        ctx.output((k, v)).at(Instant::from_millis(ts)).emit()
    }
}

#[derive(Clone, Default, serde::Serialize, serde::Deserialize, Register)]
pub struct ValidatesObserveWindowDoFn;

impl DoFn for ValidatesObserveWindowDoFn {
    type In = Windowed<(String, i64)>;
    type Out = (String, (i64, i64));

    fn process_element(
        &mut self,
        Windowed {
            value: (k, v),
            window,
        }: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let win = window.interval().ok_or("expected IntervalWindow")?;
        ctx.emit((k, (win.start.as_millis(), v)))
    }
}
