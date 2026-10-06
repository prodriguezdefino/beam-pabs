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

//! Side input reads from a `DoFn`.

use super::ProcessContext;
use crate::values::{SideInputScope, SideInputs};

impl<'a, T> ProcessContext<'a, T> {
    /// Reads `views` (one view, or a tuple, array or `Vec` of them) in the current window. See
    /// [`SideInputs`] for the value of each view. Each view must also be returned from
    /// `side_inputs()`.
    pub fn side_input<'s, S: SideInputs>(&self, views: &'s S) -> crate::Result<S::Values<'s>>
    where
        'a: 's,
    {
        if self.header.is_multi_window() {
            return Err(
                "Side inputs read from an element in several windows: return the views from \
                 `side_inputs()` so the element is processed once per window"
                    .to_string()
                    .into(),
            );
        }
        views.read(self.side_input_scope())
    }

    /// The reader and main window that side input reads use.
    pub(crate) fn side_input_scope(&self) -> SideInputScope<'a> {
        SideInputScope::new(self.reader, self.header.window_bytes())
    }
}
