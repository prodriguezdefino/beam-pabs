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

//! Adapters that build a [`CombineFn`] from other combine functions: [`On`] reads the input
//! from a field, [`MapOutput`] converts the output, and [`Compose`] runs 2 to 6 combine
//! functions over one input in one pass.

use std::marker::PhantomData;

use super::CombineFn;
use crate::coders::DefaultCoder;

mod sealed {
    pub trait Sealed {}
}

/// A part of a [`Compose`]: an [`On`] or another [`Compose`]. Sealed.
///
/// A part adds a borrowed element, so all parts read the same element and no part clones it.
pub trait ComposePart: CombineFn + sealed::Sealed {
    /// Adds a borrowed element to `accumulator`.
    fn add_borrowed(
        &self,
        accumulator: Self::Accum,
        input: &Self::Input,
    ) -> crate::Result<Self::Accum>;
}

/// A [`CombineFn`] over `I` that gives each element to `CF` through `extract`. Make it with
/// [`CombineFn::on`].
pub struct On<CF, I, F> {
    combine_fn: CF,
    extract: F,
    _marker: PhantomData<fn(&I)>,
}

impl<CF, I, F> On<CF, I, F> {
    pub(super) fn new(combine_fn: CF, extract: F) -> Self {
        Self {
            combine_fn,
            extract,
            _marker: PhantomData,
        }
    }
}

impl<CF, I, F> CombineFn for On<CF, I, F>
where
    CF: CombineFn,
    I: DefaultCoder,
    F: Fn(&I) -> CF::Input + Send + Sync + 'static,
{
    type Input = I;
    type Accum = CF::Accum;
    type Output = CF::Output;

    fn create_accumulator(&self) -> crate::Result<Self::Accum> {
        self.combine_fn.create_accumulator()
    }

    fn add_input(&self, accumulator: Self::Accum, input: I) -> crate::Result<Self::Accum> {
        self.add_borrowed(accumulator, &input)
    }

    fn merge_accumulators(&self, accumulators: Vec<Self::Accum>) -> crate::Result<Self::Accum> {
        self.combine_fn.merge_accumulators(accumulators)
    }

    fn extract_output(&self, accumulator: Self::Accum) -> crate::Result<Self::Output> {
        self.combine_fn.extract_output(accumulator)
    }
}

impl<CF, I, F> sealed::Sealed for On<CF, I, F> {}

impl<CF, I, F> ComposePart for On<CF, I, F>
where
    CF: CombineFn,
    I: DefaultCoder,
    F: Fn(&I) -> CF::Input + Send + Sync + 'static,
{
    fn add_borrowed(&self, accumulator: Self::Accum, input: &I) -> crate::Result<Self::Accum> {
        self.combine_fn
            .add_input(accumulator, (self.extract)(input))
    }
}

/// A [`CombineFn`] that converts the output of `CF` with `map`. Make it with
/// [`CombineFn::map_output`].
pub struct MapOutput<CF, F> {
    combine_fn: CF,
    map: F,
}

impl<CF, F> MapOutput<CF, F> {
    pub(super) fn new(combine_fn: CF, map: F) -> Self {
        Self { combine_fn, map }
    }
}

impl<CF, F, O> CombineFn for MapOutput<CF, F>
where
    CF: CombineFn,
    O: DefaultCoder,
    F: Fn(CF::Output) -> O + Send + Sync + 'static,
{
    type Input = CF::Input;
    type Accum = CF::Accum;
    type Output = O;

    fn create_accumulator(&self) -> crate::Result<Self::Accum> {
        self.combine_fn.create_accumulator()
    }

    fn add_input(&self, accumulator: Self::Accum, input: CF::Input) -> crate::Result<Self::Accum> {
        self.combine_fn.add_input(accumulator, input)
    }

    fn merge_accumulators(&self, accumulators: Vec<Self::Accum>) -> crate::Result<Self::Accum> {
        self.combine_fn.merge_accumulators(accumulators)
    }

    fn extract_output(&self, accumulator: Self::Accum) -> crate::Result<O> {
        self.combine_fn.extract_output(accumulator).map(&self.map)
    }
}

/// Runs 2 to 6 combine functions over one input in one pass.
///
/// Each part is an [`On`] that reads its value from the element, or another `Compose` for
/// more than 6 results. The accumulator and the output are tuples with one value for each
/// part, in order:
///
/// ```
/// use beam::transforms::{CombineFn, Compose, Max, Min, Sum};
///
/// struct Trip {
///     miles: f64,
///     seconds: i64,
/// }
///
/// // Output: (trips, total miles, (shortest, longest) trip in seconds).
/// let stats = Compose::new((
///     Sum::new().on(|_: &Trip| 1i64),
///     Sum::new().on(|trip: &Trip| trip.miles),
///     Compose::new((
///         Min::new().on(|trip: &Trip| trip.seconds),
///         Max::new().on(|trip: &Trip| trip.seconds),
///     )),
/// ));
/// # let _ = stats;
/// ```
pub struct Compose<P> {
    parts: P,
}

impl<P> Compose<P> {
    /// Creates the combine function from a tuple of parts.
    pub fn new(parts: P) -> Self {
        Self { parts }
    }
}

impl<P> sealed::Sealed for Compose<P> {}

/// Implements [`CombineFn`] and [`ComposePart`] for [`Compose`] over one tuple arity.
macro_rules! compose {
    ($(($P:ident, $acc:ident, $i:tt)),+) => {
        impl<I, $($P),+> CombineFn for Compose<($($P,)+)>
        where
            I: DefaultCoder,
            $($P: ComposePart<Input = I>,)+
            ($($P::Accum,)+): DefaultCoder,
            ($($P::Output,)+): DefaultCoder,
        {
            type Input = I;
            type Accum = ($($P::Accum,)+);
            type Output = ($($P::Output,)+);

            fn create_accumulator(&self) -> crate::Result<Self::Accum> {
                Ok(($(self.parts.$i.create_accumulator()?,)+))
            }

            fn add_input(&self, accumulator: Self::Accum, input: I) -> crate::Result<Self::Accum> {
                self.add_borrowed(accumulator, &input)
            }

            fn merge_accumulators(
                &self,
                accumulators: Vec<Self::Accum>,
            ) -> crate::Result<Self::Accum> {
                let capacity = accumulators.len();
                let columns = accumulators.into_iter().fold(
                    ($(Vec::<$P::Accum>::with_capacity(capacity),)+),
                    |mut columns, ($($acc,)+)| {
                        $(columns.$i.push($acc);)+
                        columns
                    },
                );
                Ok(($(self.parts.$i.merge_accumulators(columns.$i)?,)+))
            }

            fn extract_output(
                &self,
                ($($acc,)+): Self::Accum,
            ) -> crate::Result<Self::Output> {
                Ok(($(self.parts.$i.extract_output($acc)?,)+))
            }
        }

        impl<I, $($P),+> ComposePart for Compose<($($P,)+)>
        where
            I: DefaultCoder,
            $($P: ComposePart<Input = I>,)+
            ($($P::Accum,)+): DefaultCoder,
            ($($P::Output,)+): DefaultCoder,
        {
            fn add_borrowed(
                &self,
                ($($acc,)+): Self::Accum,
                input: &I,
            ) -> crate::Result<Self::Accum> {
                Ok(($(self.parts.$i.add_borrowed($acc, input)?,)+))
            }
        }
    };
}

compose!((P0, a0, 0), (P1, a1, 1));
compose!((P0, a0, 0), (P1, a1, 1), (P2, a2, 2));
compose!((P0, a0, 0), (P1, a1, 1), (P2, a2, 2), (P3, a3, 3));
compose!(
    (P0, a0, 0),
    (P1, a1, 1),
    (P2, a2, 2),
    (P3, a3, 3),
    (P4, a4, 4)
);
compose!(
    (P0, a0, 0),
    (P1, a1, 1),
    (P2, a2, 2),
    (P3, a3, 3),
    (P4, a4, 4),
    (P5, a5, 5)
);
