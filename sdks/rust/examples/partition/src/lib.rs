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

//! Partition: splits students into typed tiers with `#[derive(Outputs)]`, processes each
//! tier on its own, and merges the tiers again with Flatten.

use std::str::FromStr;

use beam::prelude::*;
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};

/// Options for this pipeline, on top of the standard Beam options.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
#[command(name = "partition", about = "Apache Beam partition example", version)]
pub struct Args {
    /// Path of the CSV file of scores: `name,score`.
    #[arg(long)]
    pub input: String,

    /// Path of the file to write to.
    #[arg(long, default_value = "/tmp/partition.txt")]
    pub output: String,
}

impl PipelineOptionGroup for Args {}

/// A student and the test score, from 0 to 100.
#[derive(Clone, Debug, PartialEq, BeamRow)]
pub struct Student {
    pub name: String,
    pub score: i64,
}

impl FromStr for Student {
    type Err = String;

    fn from_str(line: &str) -> Result<Self, Self::Err> {
        let (name, score) = line
            .split_once(',')
            .ok_or_else(|| format!("expected name,score: {line:?}"))?;
        let score = score.trim().parse().map_err(|e| format!("{e}: {line:?}"))?;
        Ok(Self {
            name: name.trim().to_string(),
            score,
        })
    }
}

/// The tiers of [`GradeStudents`], one output each.
#[derive(Outputs)]
pub enum Tier {
    Honours(Student),
    Pass(Student),
    Support(Student),
}

impl From<Student> for Tier {
    fn from(student: Student) -> Self {
        match student.score {
            80.. => Tier::Honours(student),
            50.. => Tier::Pass(student),
            _ => Tier::Support(student),
        }
    }
}

/// Gives one `tier: name (score)` line per student. Only honours students get a
/// `with distinction` mark, and only support students a `needs a tutor` note.
pub struct GradeStudents;

impl PTransform<PCollection<Student>> for GradeStudents {
    type Output = PCollection<String>;

    fn expand(&self, students: &PCollection<Student>) -> Self::Output {
        let TierPCollections {
            honours,
            pass,
            support,
        } = students.par_do_fn("ByTier", |student: Student, ctx| {
            ctx.emit(Tier::from(student))
        });

        let format = |tier: &'static str, note: &'static str| {
            move |Student { name, score }: Student| format!("{tier}: {name} ({score}){note}")
        };
        let honours = honours.map("FormatHonours", format("honours", " with distinction"));
        let pass = pass.map("FormatPass", format("pass", ""));
        let support = support.map("FormatSupport", format("support", ", needs a tutor"));

        honours.flatten("MergeTiers", &[&pass, &support])
    }
}
