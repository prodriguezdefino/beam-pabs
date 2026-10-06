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

//! `tracing` layer that converts events into `BeamFnLogging` `LogEntry` messages.

use std::collections::BTreeMap;
use tracing_subscriber::registry::LookupSpan;

use model::fn_execution::{LogEntry, log_entry};

use super::BeamFnLoggingHandle;

/// `tracing_subscriber::Layer` that turns events into `BeamFnLogging` `LogEntry`s.
#[derive(Clone)]
pub struct BeamFnLoggingLayer {
    handle: BeamFnLoggingHandle,
}

impl BeamFnLoggingLayer {
    /// Creates a tracing layer that forwards events to `handle`.
    pub fn new(handle: BeamFnLoggingHandle) -> Self {
        Self { handle }
    }
}

#[derive(Clone, Default, Debug)]
struct SpanData {
    instruction_id: Option<String>,
    transform_id: Option<String>,
}

/// The source chain of an error, one cause per line. Empty when the error has no source.
fn format_error_chain(err: &(dyn std::error::Error + 'static)) -> String {
    if err.source().is_none() {
        return String::new();
    }
    std::iter::successors(Some(err), |e| e.source())
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
}

/// Transport targets skipped to avoid infinite log recursion.
const TRANSPORT_TARGET_PREFIXES: &[&str] = &[
    "tonic",
    "hyper",
    "h2",
    "rustls",
    "tower",
    "harness::logging",
];

fn is_transport_target(target: &str) -> bool {
    TRANSPORT_TARGET_PREFIXES.iter().any(|prefix| {
        target == *prefix
            || target
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with("::"))
    })
}

impl<S> tracing_subscriber::Layer<S> for BeamFnLoggingLayer
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if let Some(span) = ctx.span(id) {
            let mut visitor = LogVisitor::default();
            attrs.record(&mut visitor);
            span.extensions_mut().insert(SpanData {
                instruction_id: visitor.instruction_id,
                transform_id: visitor.transform_id,
            });
        }
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if let Some(span) = ctx.span(id) {
            let mut visitor = LogVisitor::default();
            values.record(&mut visitor);
            let mut extensions = span.extensions_mut();
            if let Some(data) = extensions.get_mut::<SpanData>() {
                data.instruction_id = visitor
                    .instruction_id
                    .or_else(|| data.instruction_id.take());
                data.transform_id = visitor.transform_id.or_else(|| data.transform_id.take());
            }
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let metadata = event.metadata();
        if is_transport_target(metadata.target()) {
            return;
        }

        let mut visitor = LogVisitor::default();
        event.record(&mut visitor);

        // Take instruction_id and transform_id from active spans if the event has none.
        if (visitor.instruction_id.is_none() || visitor.transform_id.is_none())
            && let Some(current_span) = ctx.lookup_current()
        {
            for span in current_span.scope() {
                if let Some(data) = span.extensions().get::<SpanData>() {
                    visitor.instruction_id = visitor
                        .instruction_id
                        .or_else(|| data.instruction_id.clone());
                    visitor.transform_id =
                        visitor.transform_id.or_else(|| data.transform_id.clone());
                }
            }
        }

        let severity = match *metadata.level() {
            tracing::Level::ERROR => log_entry::severity::Enum::Error,
            tracing::Level::WARN => log_entry::severity::Enum::Warn,
            tracing::Level::INFO => log_entry::severity::Enum::Info,
            tracing::Level::DEBUG => log_entry::severity::Enum::Debug,
            tracing::Level::TRACE => log_entry::severity::Enum::Trace,
        };

        let log_location = match (metadata.file(), metadata.line()) {
            (Some(file), Some(line)) => format!("{file}:{line}"),
            _ => metadata.target().to_string(),
        };

        let thread = std::thread::current()
            .name()
            .unwrap_or("tokio-runtime")
            .to_string();

        let message = match (visitor.message, visitor.error_message) {
            (Some(msg), Some(err)) if msg.contains(&err) => msg,
            (Some(msg), Some(err)) => format!("{msg}: {err}"),
            (Some(msg), None) => msg,
            (None, Some(err)) => err,
            (None, None) => metadata.target().to_string(),
        };

        let trace = visitor.error_trace.unwrap_or_default();

        let custom_data = (!visitor.custom_fields.is_empty()).then_some(prost_types::Struct {
            fields: visitor.custom_fields,
        });

        let entry = LogEntry {
            severity: severity as i32,
            timestamp: Some(super::now_timestamp()),
            message,
            trace,
            instruction_id: visitor.instruction_id.unwrap_or_default(),
            transform_id: visitor.transform_id.unwrap_or_default(),
            log_location,
            thread,
            custom_data,
        };

        self.handle.send(entry);
    }
}

/// Visitor that extracts log message text, Beam metadata, and custom fields.
#[derive(Default)]
struct LogVisitor {
    message: Option<String>,
    instruction_id: Option<String>,
    transform_id: Option<String>,
    error_message: Option<String>,
    error_trace: Option<String>,
    custom_fields: BTreeMap<String, prost_types::Value>,
}

impl tracing::field::Visit for LogVisitor {
    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        let msg = value.to_string();
        let trace = format_error_chain(value);
        if field.name() == "error" || self.error_message.is_none() {
            self.error_message = Some(msg.clone());
            if !trace.is_empty() {
                self.error_trace = Some(trace);
            }
        }
        self.custom_fields.insert(
            field.name().to_string(),
            prost_types::Value {
                kind: Some(prost_types::value::Kind::StringValue(msg)),
            },
        );
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "message" => {
                self.message = Some(format!("{value:?}"));
            }
            "instruction_id" => {
                self.instruction_id = Some(format!("{value:?}").trim_matches('"').to_string());
            }
            "transform_id" => {
                self.transform_id = Some(format!("{value:?}").trim_matches('"').to_string());
            }
            "error" => {
                let s = format!("{value:?}").trim_matches('"').to_string();
                if self.error_message.is_none() {
                    self.error_message = Some(s.clone());
                }
                self.custom_fields.insert(
                    "error".to_string(),
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue(s)),
                    },
                );
            }
            name => {
                self.custom_fields.insert(
                    name.to_string(),
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue(format!("{value:?}"))),
                    },
                );
            }
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "message" => self.message = Some(value.to_string()),
            "instruction_id" => self.instruction_id = Some(value.to_string()),
            "transform_id" => self.transform_id = Some(value.to_string()),
            "error" => {
                if self.error_message.is_none() {
                    self.error_message = Some(value.to_string());
                }
                self.custom_fields.insert(
                    "error".to_string(),
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue(value.to_string())),
                    },
                );
            }
            name => {
                self.custom_fields.insert(
                    name.to_string(),
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue(value.to_string())),
                    },
                );
            }
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        match field.name() {
            "message" => self.message = Some(value.to_string()),
            name => {
                self.custom_fields.insert(
                    name.to_string(),
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::NumberValue(value as f64)),
                    },
                );
            }
        }
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        match field.name() {
            "message" => self.message = Some(value.to_string()),
            name => {
                self.custom_fields.insert(
                    name.to_string(),
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::NumberValue(value as f64)),
                    },
                );
            }
        }
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        match field.name() {
            "message" => self.message = Some(value.to_string()),
            name => {
                self.custom_fields.insert(
                    name.to_string(),
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::BoolValue(value)),
                    },
                );
            }
        }
    }
}
