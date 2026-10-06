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

//! Coder for tuples of 3 to 6 values, written as right-nested KVs.

use std::sync::Arc;

use crate::coders::URN_KV;
use crate::coders::traits::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};

/// Coder for tuples of 3 to 6 values. `(K, V)` uses [`KvCoder`](super::KvCoder).
///
/// The wire format is the right-nested KV: `(A, B, C)` is `KV<A, KV<B, C>>`. Each value
/// except the last is nested, and the last takes the enclosing context. The pipeline proto
/// records the nested `beam:coder:kv:v1` coders, so runners decode the tuple without a
/// length prefix. The coder reads and writes the flat tuple and builds no nested pairs.
#[derive(Clone, Debug)]
pub struct TupleCoder<T, C> {
    coders: C,
    _marker: std::marker::PhantomData<T>,
}

impl<T, C> TupleCoder<T, C> {
    /// Creates a coder from a tuple with one coder for each value.
    pub fn new(coders: C) -> Self {
        Self {
            coders,
            _marker: std::marker::PhantomData,
        }
    }
}

/// Registers right-nested KV coders over `init` and `last`, inner pair first. `init` holds
/// the IDs of all values except the last, in tuple order.
fn register_nested_kv<R: CoderRegistry + ?Sized, const N: usize>(
    registry: &R,
    init: [String; N],
    last: String,
) -> String {
    init.into_iter().rev().fold(last, |inner, outer| {
        registry.register_coder(URN_KV, vec![outer, inner])
    })
}

/// Implements [`Coder`] for [`TupleCoder`] and [`DefaultCoder`] for one tuple arity. The
/// last value is separate because it takes the enclosing context.
macro_rules! tuple_coder {
    ($(($T:ident, $C:ident, $i:tt)),+ ; ($L:ident, $LC:ident, $l:tt)) => {
        impl<$($T, $C,)+ $L, $LC> Coder<($($T,)+ $L)> for TupleCoder<($($T,)+ $L), ($($C,)+ $LC)>
        where
            $($T: Send + Sync + 'static, $C: Coder<$T>,)+
            $L: Send + Sync + 'static,
            $LC: Coder<$L>,
        {
            fn urn(&self) -> &'static str {
                URN_KV
            }

            fn encode(
                &self,
                value: &($($T,)+ $L),
                writer: &mut Vec<u8>,
                context: Context,
            ) -> Result<(), CoderError> {
                $(self.coders.$i.encode(&value.$i, writer, Context::Nested)?;)+
                self.coders.$l.encode(&value.$l, writer, context)
            }

            fn decode(
                &self,
                reader: &mut &[u8],
                context: Context,
            ) -> Result<($($T,)+ $L), CoderError> {
                Ok((
                    $(self.coders.$i.decode(reader, Context::Nested)?,)+
                    self.coders.$l.decode(reader, context)?,
                ))
            }
        }

        impl<$($T: DefaultCoder,)+ $L: DefaultCoder> DefaultCoder for ($($T,)+ $L) {
            type Coder = TupleCoder<
                ($($T,)+ $L),
                ($(<$T as DefaultCoder>::Coder,)+ <$L as DefaultCoder>::Coder),
            >;

            fn coder() -> Self::Coder {
                TupleCoder::new(($($T::coder(),)+ $L::coder()))
            }

            fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
                $(self.$i.encode_element(writer)?;)+
                self.$l.encode_element(writer)
            }

            fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
                Ok(($($T::decode_element(reader)?,)+ $L::decode_element(reader)?))
            }

            fn decode_element_with_schema(
                reader: &mut &[u8],
                schema: Option<&Arc<crate::schema::Schema>>,
            ) -> Result<Self, CoderError> {
                Ok((
                    $($T::decode_element_with_schema(reader, schema)?,)+
                    $L::decode_element_with_schema(reader, schema)?,
                ))
            }

            fn decode_element_with_context(
                reader: &mut &[u8],
                schema: Option<&Arc<crate::schema::Schema>>,
                state_reader: Option<&Arc<dyn crate::coders::iterable::StateStreamReader>>,
            ) -> Result<Self, CoderError> {
                Ok((
                    $($T::decode_element_with_context(reader, schema, state_reader)?,)+
                    $L::decode_element_with_context(reader, schema, state_reader)?,
                ))
            }

            fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
                let init = [$($T::register_coder(registry),)+];
                let last = $L::register_coder(registry);
                register_nested_kv(registry, init, last)
            }

            fn register_value_coder<R: CoderRegistry + ?Sized>(&self, registry: &R) -> String {
                let init = [$(self.$i.register_value_coder(registry),)+];
                let last = self.$l.register_value_coder(registry);
                register_nested_kv(registry, init, last)
            }
        }
    };
}

tuple_coder!((T0, C0, 0), (T1, C1, 1); (T2, C2, 2));
tuple_coder!((T0, C0, 0), (T1, C1, 1), (T2, C2, 2); (T3, C3, 3));
tuple_coder!((T0, C0, 0), (T1, C1, 1), (T2, C2, 2), (T3, C3, 3); (T4, C4, 4));
tuple_coder!(
    (T0, C0, 0), (T1, C1, 1), (T2, C2, 2), (T3, C3, 3), (T4, C4, 4);
    (T5, C5, 5)
);
