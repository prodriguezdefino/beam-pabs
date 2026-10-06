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

//! `CombineGlobally`: a whole-collection combine built on `CombinePerKey`.

use std::sync::Arc;

use super::{CombineFn, CombinePerKey};
use crate::transforms::{
    Create, DisplayDataBuilder, DoFn, HasDisplayData, Map, PTransform, ParDo, ProcessContext,
};
use crate::values::{PCollection, PCollectionView};
use crate::windowing::is_globally_windowed;

/// Aggregates all values of a [`PCollection`] with a [`CombineFn`].
///
/// Keys each element with `()` and uses [`CombinePerKey`], so it gets partial combining
/// and runner lifting. An empty input gives the identity value, so the output always has
/// exactly one element; [`without_defaults`](Self::without_defaults) emits nothing instead.
/// With defaults, `expand` panics if the input is not in the global window.
pub struct CombineGlobally<CF> {
    name: String,
    combine_fn: Arc<CF>,
    insert_default: bool,
}

impl<CF: CombineFn> CombineGlobally<CF> {
    pub fn new(name: impl Into<String>, combine_fn: CF) -> Self {
        Self::from_arc(name, Arc::new(combine_fn))
    }

    /// Like `new`, with a shared `combine_fn`.
    pub fn from_arc(name: impl Into<String>, combine_fn: Arc<CF>) -> Self {
        Self {
            name: name.into(),
            combine_fn,
            insert_default: true,
        }
    }

    /// Emits nothing for an empty input, not the identity value.
    ///
    /// Call this for input outside the global window. Only the global window always
    /// exists; in other windowing an empty window does not exist, so there is no window
    /// and no time for the default.
    pub fn without_defaults(mut self) -> Self {
        self.insert_default = false;
        self
    }
}

impl<CF: CombineFn> HasDisplayData for CombineGlobally<CF> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "CombineGlobally");
        builder.add_text("name", &self.name);
        builder.add_text("combine_fn", std::any::type_name::<CF>());
    }
}

/// Emits the identity value of the combine when the input was empty.
///
/// The main input is one synthetic element, so the DoFn runs once. It emits the result
/// from the side input, or the identity value if there is no result.
struct InjectDefaultFn<CF: CombineFn> {
    combine_fn: Arc<CF>,
    view: PCollectionView<CF::Output>,
}

impl<CF: CombineFn> Clone for InjectDefaultFn<CF> {
    fn clone(&self) -> Self {
        Self {
            combine_fn: Arc::clone(&self.combine_fn),
            view: self.view.clone(),
        }
    }
}

impl<CF: CombineFn> DoFn for InjectDefaultFn<CF> {
    type In = ();
    type Out = CF::Output;

    fn process_element(
        &mut self,
        _seed: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> crate::Result {
        match ctx.side_input_iter(&self.view)?.into_iter().next() {
            Some(combined) => ctx.emit(combined),
            None => ctx.emit(
                self.combine_fn
                    .extract_output(self.combine_fn.create_accumulator()),
            ),
        }
    }
}

impl<CF> PTransform<PCollection<CF::Input>> for CombineGlobally<CF>
where
    CF: CombineFn,
{
    type Output = PCollection<CF::Output>;

    fn expand(&self, input: &PCollection<CF::Input>) -> PCollection<CF::Output> {
        let name = &self.name;
        let combined = input
            .apply(Map::new(
                format!("{name}/KeyWithVoid"),
                |element: CF::Input| ((), element),
            ))
            .apply(CombinePerKey::from_arc(
                format!("{name}/Combine"),
                Arc::clone(&self.combine_fn),
            ))
            .apply(Map::new(
                format!("{name}/ExtractValues"),
                |(_key, val): ((), CF::Output)| val,
            ));

        if !self.insert_default {
            return combined;
        }

        assert!(
            is_globally_windowed(input),
            "CombineGlobally '{name}' would emit a default value for an empty input, which \
             only means something in the global window: a non-global window that nothing \
             arrived in is a window that does not exist, so there is nowhere to put the \
             default. Call .without_defaults() to emit nothing for an empty window instead."
        );

        // A DoFn on an empty input never runs, so read the result as a side input.
        let view = combined.as_iter();
        input
            .pipeline()
            .apply(Create::new(format!("{name}/DefaultSeed"), vec![()]))
            .apply(
                ParDo::new(
                    format!("{name}/InjectDefault"),
                    InjectDefaultFn {
                        combine_fn: Arc::clone(&self.combine_fn),
                        view: view.clone(),
                    },
                )
                .with_side_input(&view),
            )
    }
}
