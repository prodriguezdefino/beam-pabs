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

//! Registrations of the SDK combine functions for the standard element types.
//!
//! A registered combine function travels in the graph with the values of the launcher, so
//! the worker does not take it from the rebuilt pipeline. Each instantiation needs its own
//! registration, because the worker finds a type by its id. The ids start with
//! `beam::transforms::`. Other element types use [`register!`](crate::register).

use rust_decimal::Decimal;

use super::{
    AnySample, ApproximateQuantiles, ApproximateUnique, Max, Mean, Min, Natural, Reversed, Sample,
    Sum, Top,
};

/// Registers each combine function for each element type, with the id
/// `beam::transforms::<Function><<type>>`.
macro_rules! register_each {
    ($($combine_fn:ident),+ => $types:tt) => {
        $(register_each!(@types $combine_fn $types);)+
    };
    (@types $combine_fn:ident [$($t:ty),+]) => {
        $(crate::register!(
            $combine_fn<$t>,
            id = concat!("beam::transforms::", stringify!($combine_fn), "<", stringify!($t), ">")
        );)+
    };
}

/// Registers each ordered combine function for each element type and both named orderings,
/// with the id `beam::transforms::<Function><<type>, <Ordering>>`.
macro_rules! register_ordered {
    ($($combine_fn:ident),+ => $types:tt) => {
        $(register_ordered!(@types $combine_fn $types);)+
    };
    (@types $combine_fn:ident [$($t:ty),+]) => {
        $(
            register_ordered!(@one $combine_fn $t, Natural);
            register_ordered!(@one $combine_fn $t, Reversed);
        )+
    };
    (@one $combine_fn:ident $t:ty, $ordering:ident) => {
        crate::register!(
            $combine_fn<$t, $ordering>,
            id = concat!(
                "beam::transforms::",
                stringify!($combine_fn),
                "<",
                stringify!($t),
                ", ",
                stringify!($ordering),
                ">"
            )
        );
    };
}

// The `Numeric` types.
register_each!(Sum, Max, Min, Mean => [i32, i64, f64, Decimal]);

// Any element type with a standard coder.
register_each!(Sample, AnySample, ApproximateUnique => [i32, i64, f64, bool, String, Vec<u8>, Decimal]);

// `Natural` and `Reversed` need `Ord`, so `f64` has no named ordering.
register_ordered!(Top, ApproximateQuantiles => [i32, i64, bool, String, Vec<u8>, Decimal]);
