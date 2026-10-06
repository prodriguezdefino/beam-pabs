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

//! Integration tests for the Row Schemas example pipeline.

use beam::coders::DefaultCoder;
use std::sync::{Arc, Mutex};

use beam::prelude::*;
use beam::schema::{TypeInfo, URN_DATE, URN_DECIMAL, URN_MICROS_INSTANT};
use row_schemas::{CustomerRecord, FinancialSummary, build_row_schemas_pipeline, sample_customers};
use rust_decimal::Decimal;

#[test]
fn test_customer_record_schema_structure() {
    let schema = CustomerRecord::beam_schema();

    assert_eq!(schema.fields.len(), 9);

    let field_names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(
        field_names,
        vec![
            "id",
            "name",
            "tier",
            "address",
            "balance",
            "signup_date",
            "last_login",
            "tags",
            "phone",
        ]
    );

    let addr_field = schema.field("address").expect("address field must exist");
    match &addr_field.field_type.type_info {
        TypeInfo::Row(sub_schema) => {
            let sub_names: Vec<&str> = sub_schema.fields.iter().map(|f| f.name.as_str()).collect();
            assert_eq!(sub_names, vec!["street", "city", "postal_code"]);
        }
        other => panic!("expected TypeInfo::Row, got {other:?}"),
    }

    let balance_field = schema.field("balance").expect("balance field must exist");
    match &balance_field.field_type.type_info {
        TypeInfo::Logical { urn, .. } => assert_eq!(urn, URN_DECIMAL),
        other => panic!("expected TypeInfo::Logical(URN_DECIMAL), got {other:?}"),
    }

    let date_field = schema
        .field("signup_date")
        .expect("signup_date field must exist");
    match &date_field.field_type.type_info {
        TypeInfo::Logical { urn, .. } => assert_eq!(urn, URN_DATE),
        other => panic!("expected TypeInfo::Logical(URN_DATE), got {other:?}"),
    }

    let login_field = schema
        .field("last_login")
        .expect("last_login field must exist");
    match &login_field.field_type.type_info {
        TypeInfo::Logical { urn, .. } => assert_eq!(urn, URN_MICROS_INSTANT),
        other => panic!("expected TypeInfo::Logical(URN_MICROS_INSTANT), got {other:?}"),
    }

    let phone_field = schema.field("phone").expect("phone field must exist");
    assert!(phone_field.field_type.nullable);
}

#[test]
fn test_customer_record_wire_coder_roundtrip() {
    let records = sample_customers();
    for rec in records {
        let mut encoded = Vec::new();
        rec.encode_element(&mut encoded)
            .expect("encoding customer record");

        let mut slice = encoded.as_slice();
        let decoded = CustomerRecord::decode_element(&mut slice).expect("decoding customer record");

        assert_eq!(decoded, rec);
    }
}

#[tokio::test]
async fn test_row_schemas_pipeline_on_prism() {
    let p = Pipeline::new();
    let results = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&results);

    let summary = build_row_schemas_pipeline(&p, sample_customers());
    let _ = summary.inspect("Capture", move |s: &FinancialSummary| {
        captured.lock().unwrap().push(s.clone());
    });

    let res = p.run().await;
    assert!(res.is_ok(), "pipeline execution failed: {res:?}");

    let got = results.lock().unwrap().clone();
    assert_eq!(got.len(), 1);

    let summary = &got[0];
    assert_eq!(summary.total_customers, 3);
    assert_eq!(summary.premium_customers, 2);
    assert_eq!(summary.total_balance, Decimal::new(2047050, 2)); // $20,470.50
    assert_eq!(summary.average_balance, Decimal::new(682350, 2)); // $6,823.50
}
