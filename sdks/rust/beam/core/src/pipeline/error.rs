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

//! Pipeline validation and graph errors.

use thiserror::Error;

/// Error representing an inconsistency or invalid state in a Beam pipeline DAG.
#[derive(Error, Debug, PartialEq, Eq)]
pub enum PipelineError {
    #[error("PCollection '{pcollection_id}' references non-existent coder '{coder_id}'")]
    MissingCoder {
        pcollection_id: String,
        coder_id: String,
    },
    #[error(
        "PCollection '{pcollection_id}' references non-existent windowing strategy '{windowing_strategy_id}'"
    )]
    MissingWindowingStrategy {
        pcollection_id: String,
        windowing_strategy_id: String,
    },
    #[error(
        "PCollection '{pcollection_id}' is the output of a cross-language transform that was \
         never expanded, so its coder and windowing strategy are still placeholders. The \
         pipeline was built without contacting an expansion service - which is correct inside \
         an Fn API worker, where the runner supplies the expanded graph, but not for a graph \
         about to be submitted. Start the expansion service the transform names, or build the \
         pipeline in ExpansionMode::Remote."
    )]
    UnexpandedCrossLanguageOutput { pcollection_id: String },
    #[error(
        "Transform '{transform_id}' input '{input_tag}' references non-existent PCollection '{pcollection_id}'"
    )]
    MissingInputPCollection {
        transform_id: String,
        input_tag: String,
        pcollection_id: String,
    },
    #[error(
        "Transform '{transform_id}' output '{output_tag}' references non-existent PCollection '{pcollection_id}'"
    )]
    MissingOutputPCollection {
        transform_id: String,
        output_tag: String,
        pcollection_id: String,
    },
    #[error("Transform '{transform_id}' references non-existent subtransform '{subtransform_id}'")]
    MissingSubtransform {
        transform_id: String,
        subtransform_id: String,
    },
    #[error("Transform '{transform_id}' references non-existent accumulator coder '{coder_id}'")]
    MissingAccumulatorCoder {
        transform_id: String,
        coder_id: String,
    },
    #[error("Transform '{transform_id}' has invalid CombinePayload: {reason}")]
    InvalidCombinePayload {
        transform_id: String,
        reason: String,
    },
    #[error("Transform '{transform_id}' references non-existent environment '{environment_id}'")]
    MissingEnvironment {
        transform_id: String,
        environment_id: String,
    },
    #[error("Root transform '{transform_id}' is invalid: {reason}")]
    InvalidRootTransform {
        transform_id: String,
        reason: String,
    },
    #[error("PCollection '{pcollection_id}' has no producer transform in the pipeline")]
    NoProducerPCollection { pcollection_id: String },
    #[error("Primitive transform '{transform_id}' has no inputs but has non-source URN '{urn}'")]
    InvalidSourceTransform { transform_id: String, urn: String },
    #[error("Transform '{transform_id}' cannot read side input '{input_tag}': {reason}")]
    UnsupportedSideInput {
        transform_id: String,
        input_tag: String,
        reason: String,
    },
}
