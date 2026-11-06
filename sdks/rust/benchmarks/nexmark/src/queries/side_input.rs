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

//! Lookup table of the side-input join queries: built in the pipeline or read from text files.

use beam::io::file::textio;
use beam::prelude::*;

/// Source of the `(id, label)` lookup table of the side-input join queries.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum SideInput {
    /// Rows built in the query pipeline.
    #[default]
    Direct,
    /// `id,label` lines read from the text files `<url>*`.
    Csv { url: String },
}

/// The rows of a table with `row_count` rows (at least one). Each label is the decimal id.
pub(crate) fn side_input_table(row_count: i32) -> impl Iterator<Item = (i64, String)> {
    (0..i64::from(row_count.max(1))).map(|id| (id, id.to_string()))
}

/// One `id,label` line of the text table.
pub(crate) fn format_side_input_line((id, label): &(i64, String)) -> String {
    format!("{id},{label}")
}

fn parse_side_input_line(line: &str) -> std::result::Result<(i64, String), String> {
    let (id, label) = line
        .split_once(',')
        .ok_or_else(|| format!("side input line without a comma: {line:?}"))?;
    let id = id
        .parse()
        .map_err(|err| format!("side input id {id:?} is not an integer: {err}"))?;
    Ok((id, label.to_owned()))
}

/// The lookup table of `row_count` rows from `source`.
pub(crate) fn side_input_rows(
    pipeline: &Pipeline,
    row_count: i32,
    source: &SideInput,
) -> PCollection<(i64, String)> {
    match source {
        SideInput::Direct => pipeline.apply("SideInput", Create::new(side_input_table(row_count))),
        SideInput::Csv { url } => pipeline
            .apply("ReadSideInput", textio::Read::new(format!("{url}*")))
            .par_do_fn("ParseSideInput", |line: String, ctx| {
                ctx.emit(parse_side_input_line(&line)?)
            }),
    }
}
