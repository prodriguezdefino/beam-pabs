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

//! Built-in numeric combine functions.

use std::marker::PhantomData;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::CombineFn;
use crate::coders::DefaultCoder;

mod sealed {
    pub trait Sealed {}
}

/// A number that [`Sum`], [`Max`], [`Min`] and [`Mean`] accept: `i32`, `i64`, `f64` or
/// [`Decimal`]. Sealed.
pub trait Numeric: DefaultCoder + Copy + sealed::Sealed {
    /// The type of the mean: [`Decimal`] for `Decimal` values, else `f64`.
    type Mean: Numeric;

    /// The identity of [`Sum`].
    const ZERO: Self;
    /// The identity of [`Max`].
    const LOWEST: Self;
    /// The identity of [`Min`].
    const HIGHEST: Self;

    /// Adds two values. An integer or decimal overflow gives `None`, so the result is the
    /// same in debug and release builds. Floats do not overflow: they go to infinity.
    fn checked_add(self, other: Self) -> Option<Self>;

    /// The larger value. A float NaN loses to any number.
    fn larger(self, other: Self) -> Self;

    /// The smaller value. A float NaN loses to any number.
    fn smaller(self, other: Self) -> Self;

    /// Converts the value to the type of its mean.
    fn to_mean(self) -> Self::Mean;

    /// Divides a sum by a count. The mean of no values is NaN for `f64` and zero for
    /// `Decimal`.
    fn mean_of(sum: Self::Mean, count: i64) -> Self::Mean;
}

/// The `f64` mean of `count` values that add up to `sum`.
fn float_mean(sum: f64, count: i64) -> f64 {
    match count {
        0 => f64::NAN,
        count => sum / count as f64,
    }
}

macro_rules! integer_numeric {
    ($($t:ty),+) => {$(
        impl sealed::Sealed for $t {}

        impl Numeric for $t {
            type Mean = f64;

            const ZERO: Self = 0;
            const LOWEST: Self = <$t>::MIN;
            const HIGHEST: Self = <$t>::MAX;

            fn checked_add(self, other: Self) -> Option<Self> {
                <$t>::checked_add(self, other)
            }

            fn larger(self, other: Self) -> Self {
                Ord::max(self, other)
            }

            fn smaller(self, other: Self) -> Self {
                Ord::min(self, other)
            }

            /// The mean adds `f64` values, so a large sum does not overflow.
            fn to_mean(self) -> f64 {
                self as f64
            }

            fn mean_of(sum: f64, count: i64) -> f64 {
                float_mean(sum, count)
            }
        }
    )+};
}

integer_numeric!(i32, i64);

impl sealed::Sealed for f64 {}

impl Numeric for f64 {
    type Mean = f64;

    const ZERO: Self = 0.0;
    const LOWEST: Self = f64::NEG_INFINITY;
    const HIGHEST: Self = f64::INFINITY;

    fn checked_add(self, other: Self) -> Option<Self> {
        Some(self + other)
    }

    fn larger(self, other: Self) -> Self {
        f64::max(self, other)
    }

    fn smaller(self, other: Self) -> Self {
        f64::min(self, other)
    }

    fn to_mean(self) -> f64 {
        self
    }

    fn mean_of(sum: f64, count: i64) -> f64 {
        float_mean(sum, count)
    }
}

impl sealed::Sealed for Decimal {}

impl Numeric for Decimal {
    type Mean = Decimal;

    const ZERO: Self = Decimal::ZERO;
    const LOWEST: Self = Decimal::MIN;
    const HIGHEST: Self = Decimal::MAX;

    fn checked_add(self, other: Self) -> Option<Self> {
        Decimal::checked_add(self, other)
    }

    fn larger(self, other: Self) -> Self {
        Ord::max(self, other)
    }

    fn smaller(self, other: Self) -> Self {
        Ord::min(self, other)
    }

    fn to_mean(self) -> Decimal {
        self
    }

    fn mean_of(sum: Decimal, count: i64) -> Decimal {
        match count {
            0 => Decimal::ZERO,
            count => sum / Decimal::from(count),
        }
    }
}

/// Defines a unit-like combine function over a [`Numeric`] type. The type is inferred from
/// the input, so `Sum::new()` sums a `PCollection<i64>` or a `PCollection<f64>`.
macro_rules! numeric_combine_fn {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
        #[serde(bound = "")]
        pub struct $name<T>(#[serde(skip)] PhantomData<T>);

        impl<T> $name<T> {
            /// Creates the combine function. The input type sets `T`.
            pub const fn new() -> Self {
                Self(PhantomData)
            }
        }
    };
}

numeric_combine_fn!(
    /// Adds the values. An integer or decimal overflow is an error and fails the bundle.
    Sum
);

numeric_combine_fn!(
    /// Computes the largest value. The result for no values is the lowest value of the type:
    /// `MIN` for an integer or decimal and negative infinity for `f64`.
    Max
);

numeric_combine_fn!(
    /// Computes the smallest value. The result for no values is the highest value of the
    /// type: `MAX` for an integer or decimal and infinity for `f64`.
    Min
);

numeric_combine_fn!(
    /// Computes the arithmetic mean: an `f64` for integer and float values, and an exact
    /// [`Decimal`] for decimal values. The mean of no values is NaN, or zero for `Decimal`.
    Mean
);

/// The type name without its module path, for example `Decimal`.
fn short_type_name<T>() -> &'static str {
    let name = std::any::type_name::<T>();
    name.rsplit("::").next().unwrap_or(name)
}

fn checked_sum<T: Numeric>(left: T, right: T) -> crate::Result<T> {
    left.checked_add(right)
        .ok_or_else(|| crate::Error::msg(format!("Sum overflowed {}", short_type_name::<T>())))
}

impl<T: Numeric> CombineFn for Sum<T> {
    type Input = T;
    type Accum = T;
    type Output = T;

    fn create_accumulator(&self) -> crate::Result<T> {
        Ok(T::ZERO)
    }

    fn add_input(&self, accumulator: T, input: T) -> crate::Result<T> {
        checked_sum(accumulator, input)
    }

    fn merge_accumulators(&self, accumulators: Vec<T>) -> crate::Result<T> {
        accumulators.into_iter().try_fold(T::ZERO, checked_sum)
    }

    fn extract_output(&self, accumulator: T) -> crate::Result<T> {
        Ok(accumulator)
    }
}

impl<T: Numeric> CombineFn for Max<T> {
    type Input = T;
    type Accum = T;
    type Output = T;

    fn create_accumulator(&self) -> crate::Result<T> {
        Ok(T::LOWEST)
    }

    fn add_input(&self, accumulator: T, input: T) -> crate::Result<T> {
        Ok(accumulator.larger(input))
    }

    fn merge_accumulators(&self, accumulators: Vec<T>) -> crate::Result<T> {
        Ok(accumulators.into_iter().fold(T::LOWEST, T::larger))
    }

    fn extract_output(&self, accumulator: T) -> crate::Result<T> {
        Ok(accumulator)
    }
}

impl<T: Numeric> CombineFn for Min<T> {
    type Input = T;
    type Accum = T;
    type Output = T;

    fn create_accumulator(&self) -> crate::Result<T> {
        Ok(T::HIGHEST)
    }

    fn add_input(&self, accumulator: T, input: T) -> crate::Result<T> {
        Ok(accumulator.smaller(input))
    }

    fn merge_accumulators(&self, accumulators: Vec<T>) -> crate::Result<T> {
        Ok(accumulators.into_iter().fold(T::HIGHEST, T::smaller))
    }

    fn extract_output(&self, accumulator: T) -> crate::Result<T> {
        Ok(accumulator)
    }
}

/// Adds two partial means: their sums and their counts.
fn merge_means<M: Numeric>(
    (left_sum, left_count): (M, i64),
    (right_sum, right_count): (M, i64),
) -> crate::Result<(M, i64)> {
    Ok((
        checked_sum(left_sum, right_sum)?,
        checked_sum(left_count, right_count)?,
    ))
}

impl<T: Numeric> CombineFn for Mean<T>
where
    (T::Mean, i64): DefaultCoder,
{
    type Input = T;
    type Accum = (T::Mean, i64);
    type Output = T::Mean;

    fn create_accumulator(&self) -> crate::Result<Self::Accum> {
        Ok((T::Mean::ZERO, 0))
    }

    fn add_input(&self, accumulator: Self::Accum, input: T) -> crate::Result<Self::Accum> {
        merge_means(accumulator, (input.to_mean(), 1))
    }

    fn merge_accumulators(&self, accumulators: Vec<Self::Accum>) -> crate::Result<Self::Accum> {
        accumulators
            .into_iter()
            .try_fold((T::Mean::ZERO, 0), merge_means)
    }

    fn extract_output(&self, (sum, count): Self::Accum) -> crate::Result<T::Mean> {
        Ok(T::mean_of(sum, count))
    }
}
