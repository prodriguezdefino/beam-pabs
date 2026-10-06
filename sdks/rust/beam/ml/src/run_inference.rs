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

//! The `RunInference` transform for machine learning inference with micro-batching.

use std::marker::PhantomData;
use std::sync::Arc;

use beam::coders::DefaultCoder;
use beam::transforms::BatchConverter;
use beam::transforms::ProcessContext;
use beam::transforms::failure::OUTPUT_TAG;
use beam::transforms::{BatchElements, Failure, PTransform, TryParDo, WithFailures};
use beam::transforms::{DoFn, ParDo};
use beam::values::PCollection;

use crate::handler::{InferenceArgs, ModelHandler};
use crate::prediction::PredictionResult;

/// Transform that runs machine learning inference over a [`PCollection`].
///
/// Batches elements using bounds from [`ModelHandler`], loads the model on worker
/// startup, invokes inference, and emits [`PredictionResult`] values.
pub struct RunInference<In, Out, H> {
    name: String,
    handler: H,
    inference_args: Option<InferenceArgs>,
    _marker: PhantomData<(In, Out)>,
}

impl<In, Out, H> RunInference<In, Out, H>
where
    In: DefaultCoder,
    Out: DefaultCoder,
    H: ModelHandler<In, Out>,
{
    /// Creates a new `RunInference` transform.
    pub fn new(name: impl Into<String>, handler: H) -> Self {
        Self {
            name: name.into(),
            handler,
            inference_args: None,
            _marker: PhantomData,
        }
    }

    /// Sets optional inference arguments.
    pub fn with_inference_args(mut self, args: InferenceArgs) -> Self {
        self.inference_args = Some(args);
        self
    }

    /// Enables dead-letter queue (DLQ) error handling. Failed elements go to a
    /// secondary output collection, and the bundle does not fail.
    pub fn with_exception_handling(self) -> RunInferenceMulti<In, Out, H> {
        RunInferenceMulti {
            name: self.name,
            handler: self.handler,
            inference_args: self.inference_args,
            _marker: PhantomData,
        }
    }
}

fn batch_elements_for<In, Out, H>(
    name: &str,
    handler: &H,
) -> (BatchElements<In, H::Batch, H::Converter>, H::Converter)
where
    In: DefaultCoder,
    Out: DefaultCoder,
    H: ModelHandler<In, Out>,
{
    let bounds = handler.get_batch_bounds();
    let converter = handler.get_batch_converter();
    let batch_elements = BatchElements::with_converter(
        format!("{name}/BatchElements"),
        bounds.min_batch_size,
        bounds.max_batch_size,
        converter.clone(),
    );
    let batch_elements = bounds
        .max_batch_duration
        .into_iter()
        .fold(batch_elements, BatchElements::with_max_batch_duration);
    (batch_elements, converter)
}

impl<In, Out, H> PTransform<PCollection<In>> for RunInference<In, Out, H>
where
    In: DefaultCoder,
    Out: DefaultCoder,
    H: ModelHandler<In, Out>,
{
    type Output = PCollection<PredictionResult<In, Out>>;

    fn expand(&self, input: &PCollection<In>) -> PCollection<PredictionResult<In, Out>> {
        let (batch_elements, converter) = batch_elements_for(&self.name, &self.handler);
        let batched = input.apply(batch_elements);

        let do_fn = RunInferenceDoFn {
            handler: self.handler.clone(),
            converter,
            inference_args: self.inference_args.clone(),
            model: None,
            _marker: PhantomData,
        };

        batched.apply(ParDo::new(format!("{}/Predict", self.name), do_fn))
    }
}

struct RunInferenceDoFn<In: DefaultCoder, Out: DefaultCoder, H: ModelHandler<In, Out>> {
    handler: H,
    converter: H::Converter,
    inference_args: Option<InferenceArgs>,
    model: Option<Arc<H::Model>>,
    _marker: PhantomData<(In, Out)>,
}

impl<In: DefaultCoder, Out: DefaultCoder, H: ModelHandler<In, Out>> Clone
    for RunInferenceDoFn<In, Out, H>
{
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            converter: self.converter.clone(),
            inference_args: self.inference_args.clone(),
            model: self.model.clone(),
            _marker: PhantomData,
        }
    }
}

impl<In, Out, H> DoFn for RunInferenceDoFn<In, Out, H>
where
    In: DefaultCoder,
    Out: DefaultCoder,
    H: ModelHandler<In, Out>,
{
    type In = H::Batch;
    type Out = PredictionResult<In, Out>;

    fn setup(&mut self) -> beam::Result {
        let loaded = self
            .handler
            .load_model()
            .map_err(|e| e.context("Failed to load model"))?;
        self.model = Some(Arc::new(loaded));
        Ok(())
    }

    fn process_element(
        &mut self,
        batch: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result {
        let model = self
            .model
            .as_ref()
            .ok_or_else(|| "Model was not loaded during setup".to_string())?;

        let predictions = self
            .handler
            .run_inference(&batch, model, self.inference_args.as_ref())
            .map_err(|e| e.context("Inference failed"))?;

        let inputs = self
            .converter
            .explode(batch)
            .map_err(|e| e.context("Failed to explode input batch"))?;

        if inputs.len() != predictions.len() {
            return Err(format!(
                "Model prediction count ({}) does not match input element count ({})",
                predictions.len(),
                inputs.len()
            )
            .into());
        }

        inputs
            .into_iter()
            .zip(predictions)
            .try_for_each(|(input, output)| ctx.emit(PredictionResult::new(input, output)))
    }
}

/// Multi-output version of `RunInference` that routes failed elements to a dead-letter
/// collection, apart from the successful predictions.
pub struct RunInferenceMulti<In, Out, H> {
    name: String,
    handler: H,
    inference_args: Option<InferenceArgs>,
    _marker: PhantomData<(In, Out)>,
}

impl<In, Out, H> PTransform<PCollection<In>> for RunInferenceMulti<In, Out, H>
where
    In: DefaultCoder,
    Out: DefaultCoder,
    H: ModelHandler<In, Out>,
{
    type Output = WithFailures<PredictionResult<In, Out>, Failure<In>>;

    fn expand(&self, input: &PCollection<In>) -> Self::Output {
        let (batch_elements, converter) = batch_elements_for(&self.name, &self.handler);

        let do_fn = RunInferenceMultiDoFn {
            handler: self.handler.clone(),
            converter,
            inference_args: self.inference_args.clone(),
            model: None,
            _marker: PhantomData,
        };

        input.apply(batch_elements).apply(TryParDo::new(
            format!("{}/PredictWithDLQ", self.name),
            do_fn,
        ))
    }
}

struct RunInferenceMultiDoFn<In: DefaultCoder, Out: DefaultCoder, H: ModelHandler<In, Out>> {
    handler: H,
    converter: H::Converter,
    inference_args: Option<InferenceArgs>,
    model: Option<Arc<H::Model>>,
    _marker: PhantomData<(In, Out)>,
}

impl<In: DefaultCoder, Out: DefaultCoder, H: ModelHandler<In, Out>> Clone
    for RunInferenceMultiDoFn<In, Out, H>
{
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            converter: self.converter.clone(),
            inference_args: self.inference_args.clone(),
            model: self.model.clone(),
            _marker: PhantomData,
        }
    }
}

impl<In, Out, H> DoFn for RunInferenceMultiDoFn<In, Out, H>
where
    In: DefaultCoder,
    Out: DefaultCoder,
    H: ModelHandler<In, Out>,
{
    type In = H::Batch;
    type Out = PredictionResult<In, Out>;

    fn setup(&mut self) -> beam::Result {
        let loaded = self
            .handler
            .load_model()
            .map_err(|e| e.context("Failed to load model"))?;
        self.model = Some(Arc::new(loaded));
        Ok(())
    }

    fn process_element(
        &mut self,
        batch: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result {
        let model = self
            .model
            .as_ref()
            .ok_or_else(|| "Model was not loaded during setup".to_string())?;

        match self
            .handler
            .run_inference(&batch, model, self.inference_args.as_ref())
        {
            Ok(predictions) => {
                let inputs = self
                    .converter
                    .explode(batch)
                    .map_err(|e| e.context("Failed to explode batch"))?;

                inputs
                    .into_iter()
                    .zip(predictions)
                    .try_for_each(|(input, output)| {
                        ctx.output(PredictionResult::new(input, output))
                            .to(OUTPUT_TAG)
                            .emit()
                    })
            }
            Err(err) => {
                let err_msg = err.to_string();
                let inputs = self
                    .converter
                    .explode(batch)
                    .map_err(|e| e.context("Failed to explode batch for DLQ"))?;

                inputs
                    .into_iter()
                    .try_for_each(|input| ctx.emit_failure(input, err_msg.clone()))
            }
        }
    }
}
