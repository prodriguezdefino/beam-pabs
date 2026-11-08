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

use std::fmt::Write as _;

use beam::Instant;
use beam::coders::{Coder, Context, DefaultCoder};
use nexmark::config::BASE_TIME_MS;
use nexmark::model::{
    Auction, AuctionBid, AuctionCount, AuctionPrice, Bid, BidsPerSession, CategoryPrice, Done,
    Event, IdNameReserve, KnownSize, NameCityStateId, Person, SellerPrice,
};

fn hex_encode(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
}

fn assert_coder_wire_and_round_trip<T>(value: &T, expected_hex: &str, expected_size: u64)
where
    T: DefaultCoder + KnownSize + PartialEq + std::fmt::Debug,
{
    assert_eq!(value.estimated_size_bytes(), expected_size);

    let coder = T::coder();
    let mut whole = Vec::new();
    coder
        .encode(value, &mut whole, Context::WholeStream)
        .expect("whole-stream encode");
    assert_eq!(hex_encode(&whole), expected_hex);

    let mut whole_reader: &[u8] = &whole;
    let decoded_whole = coder
        .decode(&mut whole_reader, Context::WholeStream)
        .expect("whole-stream decode");
    assert!(whole_reader.is_empty());
    assert_eq!(&decoded_whole, value);

    let mut nested = Vec::new();
    value.encode_element(&mut nested).expect("nested encode 1");
    value.encode_element(&mut nested).expect("nested encode 2");

    let mut nested_reader: &[u8] = &nested;
    let first = T::decode_element(&mut nested_reader).expect("nested decode 1");
    let second = T::decode_element(&mut nested_reader).expect("nested decode 2");
    assert!(nested_reader.is_empty());
    assert_eq!(&first, value);
    assert_eq!(&second, value);
}

#[test]
fn all_model_coders_match_wire_vectors_and_sizes() {
    let person = Person {
        id: 1_000,
        name: "Peter Shultz".to_owned(),
        email_address: "abc@def.com".to_owned(),
        credit_card: "1234 5678 9012 3456".to_owned(),
        city: "Phoenix".to_owned(),
        state: "AZ".to_owned(),
        date_time: Instant::from_millis(BASE_TIME_MS),
        extra: "extra_p".to_owned(),
    };
    let auction = Auction {
        id: 1_001,
        item_name: "item".to_owned(),
        description: "desc".to_owned(),
        initial_bid: 500,
        reserve: 1_200,
        date_time: Instant::from_millis(BASE_TIME_MS),
        expires: Instant::from_millis(BASE_TIME_MS + 5_000),
        seller: 1_000,
        category: 10,
        extra: "extra_a".to_owned(),
    };
    let bid = Bid {
        auction: 1_001,
        bidder: 1_002,
        price: 900,
        date_time: Instant::from_millis(BASE_TIME_MS + 1_000),
        extra: "extra_b".to_owned(),
    };

    let person_hex = "e8070c5065746572205368756c747a0b616263406465662e636f6d13313233342035363738203930313220333435360750686f656e697802415a8000014e8f02c4000765787472615f70";
    let auction_hex =
        "e907046974656d0464657363f403b0098000014e8f02c4008000014e8f02d788e8070a0765787472615f61";
    let bid_hex = "e907ea0784078000014e8f02c7e80765787472615f62";

    assert_coder_wire_and_round_trip(&person, person_hex, 80);
    assert_coder_wire_and_round_trip(&auction, auction_hex, 74);
    assert_coder_wire_and_round_trip(&bid, bid_hex, 40);
    assert_coder_wire_and_round_trip(&Event::Person(person), &format!("00{person_hex}"), 81);

    let auction_bid = AuctionBid { auction, bid };
    assert_coder_wire_and_round_trip(
        &Event::Auction(auction_bid.auction.clone()),
        &format!("01{auction_hex}"),
        75,
    );
    assert_coder_wire_and_round_trip(
        &Event::Bid(auction_bid.bid.clone()),
        &format!("02{bid_hex}"),
        41,
    );
    assert_coder_wire_and_round_trip(&auction_bid, &format!("{auction_hex}{bid_hex}"), 114);

    assert_coder_wire_and_round_trip(
        &AuctionCount {
            auction: 1_001,
            num: 42,
        },
        "e9072a",
        16,
    );
    assert_coder_wire_and_round_trip(
        &AuctionPrice {
            auction: 1_001,
            price: 900,
        },
        "e9078407",
        16,
    );
    assert_coder_wire_and_round_trip(
        &BidsPerSession {
            person_id: 1_002,
            bids_per_session: 7,
        },
        "ea0707",
        16,
    );
    assert_coder_wire_and_round_trip(
        &CategoryPrice {
            category: 10,
            price: 850,
            is_last: true,
        },
        "0ad20601",
        17,
    );
    assert_coder_wire_and_round_trip(
        &CategoryPrice {
            category: 12,
            price: 450,
            is_last: false,
        },
        "0cc20300",
        17,
    );
    assert_coder_wire_and_round_trip(
        &Done {
            message: "done message".to_owned(),
        },
        "0c646f6e65206d657373616765",
        12,
    );
    assert_coder_wire_and_round_trip(
        &IdNameReserve {
            id: 1_001,
            name: "item".to_owned(),
            reserve: 1_200,
        },
        "e907046974656db009",
        21,
    );
    assert_coder_wire_and_round_trip(
        &NameCityStateId {
            name: "Peter Shultz".to_owned(),
            city: "Phoenix".to_owned(),
            state: "AZ".to_owned(),
            id: 1_000,
        },
        "0c5065746572205368756c747a0750686f656e697802415ae807",
        32,
    );
    assert_coder_wire_and_round_trip(
        &SellerPrice {
            seller: 1_000,
            price: 777,
        },
        "e8078906",
        16,
    );
}

#[test]
fn nested_multibyte_length_and_negative_timestamps_round_trip() {
    let large_auction = Event::Auction(Auction {
        id: 5_000,
        item_name: "vintage camera".to_owned(),
        description: "a".repeat(220),
        initial_bid: 100,
        reserve: 250,
        date_time: Instant::from_millis(-5_000),
        expires: Instant::from_millis(0),
        seller: 1_005,
        category: 11,
        extra: "b".repeat(350),
    });

    let mut buf = Vec::new();
    large_auction
        .encode_element(&mut buf)
        .expect("encode large nested event");
    let mut reader: &[u8] = &buf;
    let decoded = Event::decode_element(&mut reader).expect("decode large nested event");
    assert!(reader.is_empty());
    assert_eq!(decoded, large_auction);

    let p_neg = Bid {
        auction: 1,
        bidder: 1,
        price: 1,
        date_time: Instant::from_millis(-1),
        extra: String::new(),
    };
    let p_zero = Bid {
        date_time: Instant::from_millis(0),
        ..p_neg.clone()
    };
    let mut b1 = Vec::new();
    let mut b2 = Vec::new();
    Bid::coder()
        .encode(&p_neg, &mut b1, Context::WholeStream)
        .expect("encode neg");
    Bid::coder()
        .encode(&p_zero, &mut b2, Context::WholeStream)
        .expect("encode zero");
    assert!(
        b1 < b2,
        "sign-bit-flipped Instant must sort negative before zero"
    );
}
