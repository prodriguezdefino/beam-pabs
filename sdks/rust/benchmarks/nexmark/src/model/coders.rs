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

//! Direct binary coders (`beam:coder:bytes:v1` nested framing) for Nexmark model types.

use std::io;

use beam::Instant;
use beam::coders::{
    Coder, CoderError, CoderRegistry, Context, DefaultCoder, URN_BYTES, VarIntCoder,
};

use super::{
    Auction, AuctionBid, AuctionCount, AuctionPrice, Bid, BidsPerSession, CategoryPrice, Done,
    Event, IdNameReserve, NameCityStateId, Person, SellerPrice,
};

trait RawWire: Sized {
    fn encode_raw(&self, writer: &mut Vec<u8>) -> Result<(), CoderError>;
    fn decode_raw(reader: &mut &[u8]) -> Result<Self, CoderError>;
}

impl RawWire for i64 {
    fn encode_raw(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        VarIntCoder::encode_varint(*self, writer).map_err(CoderError::Io)
    }

    fn decode_raw(reader: &mut &[u8]) -> Result<Self, CoderError> {
        VarIntCoder::decode_varint(reader).map_err(CoderError::Io)
    }
}

impl RawWire for bool {
    fn encode_raw(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        i32::from(*self).encode_element(writer)
    }

    fn decode_raw(reader: &mut &[u8]) -> Result<Self, CoderError> {
        match i32::decode_element(reader)? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(CoderError::Format(format!("invalid boolean flag: {other}"))),
        }
    }
}

impl RawWire for String {
    fn encode_raw(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        let len = i64::try_from(self.len()).map_err(|_| {
            CoderError::Format(format!("string length {} overflows i64", self.len()))
        })?;
        VarIntCoder::encode_varint(len, writer)?;
        writer.extend_from_slice(self.as_bytes());
        Ok(())
    }

    fn decode_raw(reader: &mut &[u8]) -> Result<Self, CoderError> {
        let len = VarIntCoder::decode_varint(reader)?;
        let len = usize::try_from(len)
            .map_err(|_| CoderError::Format(format!("negative string length: {len}")))?;
        let bytes = take_bytes(reader, len, "string")?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|e| CoderError::Format(format!("invalid UTF-8 in model string: {e}")))
    }
}

/// Encodes `Instant` as 8 big-endian bytes with the sign bit flipped for byte order.
impl RawWire for Instant {
    fn encode_raw(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        let shifted = (self.as_millis() as u64) ^ (1_u64 << 63);
        writer.extend_from_slice(&shifted.to_be_bytes());
        Ok(())
    }

    fn decode_raw(reader: &mut &[u8]) -> Result<Self, CoderError> {
        let head = take_bytes(reader, 8, "instant")?;
        let buf: [u8; 8] = head.try_into().expect("take_bytes returns an 8-byte slice");
        let millis = (u64::from_be_bytes(buf) ^ (1_u64 << 63)) as i64;
        Ok(Instant::from_millis(millis))
    }
}

impl RawWire for Event {
    fn encode_raw(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        match self {
            Self::Person(person) => {
                0_i32.encode_element(writer)?;
                person.encode_raw(writer)
            }
            Self::Auction(auction) => {
                1_i32.encode_element(writer)?;
                auction.encode_raw(writer)
            }
            Self::Bid(bid) => {
                2_i32.encode_element(writer)?;
                bid.encode_raw(writer)
            }
        }
    }

    fn decode_raw(reader: &mut &[u8]) -> Result<Self, CoderError> {
        match i32::decode_element(reader)? {
            0 => Person::decode_raw(reader).map(Self::Person),
            1 => Auction::decode_raw(reader).map(Self::Auction),
            2 => Bid::decode_raw(reader).map(Self::Bid),
            other => Err(CoderError::Format(format!("invalid Event tag: {other}"))),
        }
    }
}

fn take_bytes<'a>(reader: &mut &'a [u8], len: usize, label: &str) -> Result<&'a [u8], CoderError> {
    if reader.len() < len {
        return Err(CoderError::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("expected {len} {label} bytes, {} left", reader.len()),
        )));
    }
    let (head, rest) = reader.split_at(len);
    *reader = rest;
    Ok(head)
}

fn encode_framed(
    writer: &mut Vec<u8>,
    context: Context,
    encode_raw: impl FnOnce(&mut Vec<u8>) -> Result<(), CoderError>,
) -> Result<(), CoderError> {
    match context {
        Context::WholeStream => encode_raw(writer),
        Context::Nested => {
            let len_pos = writer.len();
            writer.push(0);
            encode_raw(writer)?;
            let payload_len = i64::try_from(writer.len() - len_pos - 1)
                .map_err(|_| CoderError::Format("nested payload overflows i64".to_owned()))?;
            if payload_len < 0x80 {
                writer[len_pos] = payload_len as u8;
            } else {
                let mut hdr = Vec::with_capacity(10);
                VarIntCoder::encode_varint(payload_len, &mut hdr)?;
                writer.splice(len_pos..=len_pos, hdr);
            }
            Ok(())
        }
    }
}

fn decode_framed<T>(
    reader: &mut &[u8],
    context: Context,
    decode_raw: impl FnOnce(&mut &[u8]) -> Result<T, CoderError>,
) -> Result<T, CoderError> {
    match context {
        Context::WholeStream => {
            let value = decode_raw(reader)?;
            if !reader.is_empty() {
                return Err(CoderError::Format(format!(
                    "trailing {} bytes in whole-stream decode",
                    reader.len()
                )));
            }
            Ok(value)
        }
        Context::Nested => {
            let len = VarIntCoder::decode_varint(reader)?;
            let len = usize::try_from(len)
                .map_err(|_| CoderError::Format(format!("negative nested length: {len}")))?;
            let mut payload = take_bytes(reader, len, "nested")?;
            let value = decode_raw(&mut payload)?;
            if !payload.is_empty() {
                return Err(CoderError::Format(format!(
                    "trailing {} bytes in nested payload",
                    payload.len()
                )));
            }
            Ok(value)
        }
    }
}

macro_rules! impl_hand_coder {
    ($coder:ident for $ty:ty) => {
        #[doc = concat!("Direct binary coder for [`", stringify!($ty), "`].")]
        #[derive(Clone, Copy, Debug, Default)]
        pub struct $coder;

        impl Coder<$ty> for $coder {
            fn urn(&self) -> &'static str {
                URN_BYTES
            }

            fn encode(
                &self,
                value: &$ty,
                writer: &mut Vec<u8>,
                context: Context,
            ) -> Result<(), CoderError> {
                encode_framed(writer, context, |out| value.encode_raw(out))
            }

            fn decode(&self, reader: &mut &[u8], context: Context) -> Result<$ty, CoderError> {
                decode_framed(reader, context, <$ty>::decode_raw)
            }
        }

        impl DefaultCoder for $ty {
            type Coder = $coder;

            fn coder() -> Self::Coder {
                $coder
            }

            fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
                $coder.encode(self, writer, Context::Nested)
            }

            fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
                $coder.decode(reader, Context::Nested)
            }

            fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
                registry.register_coder(URN_BYTES, Vec::new())
            }
        }
    };
}

macro_rules! impl_struct_coder {
    ($coder:ident for $ty:ident { $($field:ident),+ $(,)? }) => {
        impl RawWire for $ty {
            fn encode_raw(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
                $(self.$field.encode_raw(writer)?;)+
                Ok(())
            }

            fn decode_raw(reader: &mut &[u8]) -> Result<Self, CoderError> {
                Ok(Self {
                    $($field: RawWire::decode_raw(reader)?,)+
                })
            }
        }

        impl_hand_coder!($coder for $ty);
    };
}

impl_struct_coder!(PersonCoder for Person {
    id, name, email_address, credit_card, city, state, date_time, extra,
});
impl_struct_coder!(AuctionCoder for Auction {
    id, item_name, description, initial_bid, reserve, date_time, expires, seller, category, extra,
});
impl_struct_coder!(BidCoder for Bid {
    auction, bidder, price, date_time, extra,
});
impl_hand_coder!(EventCoder for Event);
impl_struct_coder!(AuctionBidCoder for AuctionBid { auction, bid });
impl_struct_coder!(AuctionCountCoder for AuctionCount { auction, num });
impl_struct_coder!(AuctionPriceCoder for AuctionPrice { auction, price });
impl_struct_coder!(BidsPerSessionCoder for BidsPerSession { person_id, bids_per_session });
impl_struct_coder!(CategoryPriceCoder for CategoryPrice { category, price, is_last });
impl_struct_coder!(DoneCoder for Done { message });
impl_struct_coder!(IdNameReserveCoder for IdNameReserve { id, name, reserve });
impl_struct_coder!(NameCityStateIdCoder for NameCityStateId { name, city, state, id });
impl_struct_coder!(SellerPriceCoder for SellerPrice { seller, price });
