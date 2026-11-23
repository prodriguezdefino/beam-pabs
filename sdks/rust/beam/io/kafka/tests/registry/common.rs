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

//! A fake Confluent Schema Registry: an HTTP server on a local port with fixed answers, which
//! keeps the requests that it gets.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use beam::Instant;
use beam::prelude::*;
use beam::schema::{Row, Schema};
use beam_io_kafka::codec::{Encoder, RecordRef, SchemaRegistry};

/// A request that the fake registry got.
#[derive(Clone, Debug)]
pub(crate) struct Request {
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) authorization: Option<String>,
    pub(crate) content_type: Option<String>,
    pub(crate) body: String,
}

/// An answer of the fake registry: the method and the path it answers, the status and the
/// JSON body.
pub(crate) struct Route {
    pub(crate) method: &'static str,
    pub(crate) path: String,
    pub(crate) status: u16,
    pub(crate) body: String,
}

impl Route {
    pub(crate) fn get(path: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            method: "GET",
            path: path.into(),
            status: 200,
            body: body.into(),
        }
    }

    pub(crate) fn post(path: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            method: "POST",
            ..Self::get(path, body)
        }
    }
}

pub(crate) struct FakeRegistry {
    pub(crate) url: String,
    requests: Arc<Mutex<Vec<Request>>>,
}

impl FakeRegistry {
    /// Starts the registry with `routes`. Other requests get a 404.
    pub(crate) fn start(routes: Vec<Route>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a local port");
        let url = format!(
            "http://{}",
            listener.local_addr().expect("the local address")
        );
        let requests = Arc::new(Mutex::new(Vec::new()));
        let kept = Arc::clone(&requests);
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let _answered = answer(stream, &routes, &kept);
            }
        });
        Self { url, requests }
    }

    pub(crate) fn client(&self) -> SchemaRegistry {
        SchemaRegistry::new(&self.url)
    }

    pub(crate) fn requests(&self) -> Vec<Request> {
        self.requests.lock().expect("the requests").clone()
    }

    /// The number of requests to `path`.
    pub(crate) fn calls(&self, path: &str) -> usize {
        self.requests().iter().filter(|r| r.path == path).count()
    }
}

/// Reads one request from `stream`, keeps it in `kept`, and writes the answer of its route. The
/// request is in `kept` before the client gets the answer.
fn answer(stream: TcpStream, routes: &[Route], kept: &Mutex<Vec<Request>>) -> Option<()> {
    let mut reader = BufReader::new(stream.try_clone().ok()?);
    let mut line = String::new();
    reader.read_line(&mut line).ok()?;
    let mut parts = line.split_whitespace();
    let (method, path) = (parts.next()?.to_string(), parts.next()?.to_string());
    let (mut length, mut authorization, mut content_type) = (0, None, None);
    loop {
        let mut header = String::new();
        reader.read_line(&mut header).ok()?;
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        let (name, value) = header.split_once(':')?;
        let value = value.trim().to_string();
        match name.to_ascii_lowercase().as_str() {
            "content-length" => length = value.parse().ok()?,
            "authorization" => authorization = Some(value),
            "content-type" => content_type = Some(value),
            _ => {}
        }
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body).ok()?;
    let (status, reply) = routes
        .iter()
        .find(|route| route.method == method && route.path == path)
        .map_or(
            (404, r#"{"error_code":40403,"message":"Schema not found"}"#),
            |route| (route.status, route.body.as_str()),
        );
    kept.lock().ok()?.push(Request {
        method,
        path,
        authorization,
        content_type,
        body: String::from_utf8(body).ok()?,
    });
    let mut stream = stream;
    write!(
        stream,
        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{reply}",
        reply.len()
    )
    .ok()
}

/// The JSON body of a registry answer with the schema `schema` and, if set, its ID.
pub(crate) fn schema_answer(id: Option<u32>, schema: &str) -> String {
    let mut answer = serde_json::json!({ "schema": schema });
    if let Some(id) = id {
        answer["id"] = id.into();
        answer["version"] = 1.into();
        answer["subject"] = "events-value".into();
    }
    answer.to_string()
}

/// A value in the Confluent wire format: byte 0, the schema ID, then `datum`.
pub(crate) fn framed(id: u32, datum: &[u8]) -> Vec<u8> {
    [&[0][..], &id.to_be_bytes(), datum].concat()
}

pub(crate) fn record(value: &[u8]) -> RecordRef<'_> {
    RecordRef {
        topic: "events",
        partition: 0,
        offset: 1,
        timestamp: Some(Instant::from_millis(1_000)),
        key: None,
        value: Some(value),
        headers: &[],
    }
}

/// Gives `encoder` the schema of rows of `schema`, as the write does.
pub(crate) fn bind<E: Encoder<Input = Row>>(encoder: &mut E, schema: &Arc<Schema>) -> Result {
    let pipeline = Pipeline::new();
    let rows = pipeline
        .apply("Create", Create::new(Vec::<Row>::new()))
        .with_row_schema(schema);
    encoder.elements(&rows).map(|_| ())
}
