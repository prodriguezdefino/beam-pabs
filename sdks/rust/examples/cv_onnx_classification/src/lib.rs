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

//! Image classification with MobileNetV2 on ONNX Runtime.
//!
//! The pipeline reads a manifest of image paths, one for each line, on any registered
//! filesystem. It decodes and resizes each image into 3x224x224 RGB bytes, then runs
//! MobileNetV2 (exported to ONNX by `export_model.py`, which puts the scaling and the
//! ImageNet normalization in the model) with `RunInference`. It writes `path,class` lines.
//! Images that cannot be read or decoded go to a dead-letter file.
//!
//! `RunInference` runs in its own stage. With `--inference_accelerator`, only that stage asks
//! for an accelerator, so on Dataflow the image reads run on CPU workers that scale with the
//! backlog and the inference runs on accelerator workers.

use std::io;

use beam::io::file::read_to_bytes;
use beam::ml::onnx::{
    OnnxAdapter, OnnxConfig, OnnxDeviceOptions, OnnxModelHandler, SessionInputs, SessionOutputs,
    ort,
};
use beam::ml::{BatchBounds, ModelHandler, RunInference};
use beam::pipeline::ResourceHints;
use beam::prelude::*;
use beam::transforms::WithResourceHintsExt;
use clap::Args as ClapArgs;
use image::imageops::{self, FilterType};
use serde::{Deserialize, Serialize};

/// Model input tensor name. It must match `INPUT_NAME` in `export_model.py`.
pub const INPUT_NAME: &str = "input";
/// Model output tensor name. It must match `OUTPUT_NAME` in `export_model.py`.
pub const OUTPUT_NAME: &str = "logits";
/// Square input resolution of MobileNetV2.
pub const IMAGE_SIZE: u32 = 224;
/// Number of bytes in one preprocessed CHW RGB image.
pub const PIXELS_PER_IMAGE: usize = 3 * (IMAGE_SIZE as usize) * (IMAGE_SIZE as usize);

/// Options for this pipeline, on top of the standard Beam options.
#[derive(ClapArgs, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "cv_onnx_classification",
    about = "Apache Beam image classification with ONNX Runtime"
)]
pub struct Args {
    /// Manifest of image paths, one for each line.
    #[arg(
        long,
        default_value = "gs://apache-beam-ml/testing/inputs/openimage_50k_benchmark.txt"
    )]
    pub input: String,

    /// Path of the file to write `path,class` lines to.
    #[arg(long, default_value = "/tmp/onnx_predictions.txt")]
    pub output: String,

    /// Path of the file to write `path<TAB>error` lines to, for images that fail to load.
    #[arg(long, default_value = "/tmp/onnx_failures.txt")]
    pub dlq_output: String,

    /// Path or URI of the MobileNetV2 ONNX model that `export_model.py` writes.
    #[arg(long)]
    pub model_path: String,

    /// ONNX Runtime device: `--device`, `--device_id`, `--allow_cpu_fallback`, `--dylib_path`.
    #[command(flatten)]
    #[serde(flatten)]
    pub device: OnnxDeviceOptions,

    /// Accelerator resource hint of the inference step only, for example
    /// `type:nvidia-tesla-t4;count:1;install-nvidia-driver`. The other steps run without it.
    #[arg(long)]
    pub inference_accelerator: Option<String>,

    /// Minimum RunInference batch size.
    #[arg(long, default_value_t = 10)]
    pub min_batch_size: usize,

    /// Maximum RunInference batch size.
    #[arg(long, default_value_t = 100)]
    pub max_batch_size: usize,
}

impl PipelineOptionGroup for Args {}

impl From<&Args> for OnnxConfig {
    fn from(args: &Args) -> Self {
        OnnxConfig::new(&args.model_path)
            .with_device_options(&args.device)
            .with_batch_bounds(BatchBounds::new(args.min_batch_size, args.max_batch_size))
    }
}

/// A preprocessed image: its manifest path and its resized CHW RGB bytes.
#[derive(Clone, Debug, PartialEq, BeamRow)]
pub struct ImageRecord {
    pub image_path: String,
    #[beam(bytes)]
    pub pixel_values: Vec<u8>,
}

/// Reasons why an image cannot be loaded. The failure record holds the path.
#[derive(Debug, thiserror::Error)]
pub enum ImageLoadError {
    #[error("reading image: {0}")]
    Read(#[from] io::Error),
    #[error("decoding image: {0}")]
    Decode(#[from] image::ImageError),
}

/// Decodes image bytes into flattened CHW RGB bytes. The image is converted to RGB and
/// resized to 224x224 (bilinear, no crop), the input size of the model. The model scales
/// and normalizes the bytes itself (see `export_model.py`).
pub fn preprocess_image(bytes: &[u8]) -> Result<Vec<u8>, image::ImageError> {
    let rgb = image::load_from_memory(bytes)?.to_rgb8();
    let resized = imageops::resize(&rgb, IMAGE_SIZE, IMAGE_SIZE, FilterType::Triangle);
    Ok((0..3)
        .flat_map(|channel| resized.pixels().map(move |pixel| pixel.0[channel]))
        .collect())
}

/// Reads the image at `path` through the Beam filesystems and preprocesses it.
pub fn load_image(path: &str) -> Result<ImageRecord, ImageLoadError> {
    let pixel_values = preprocess_image(&read_to_bytes(path)?)?;
    Ok(ImageRecord {
        image_path: path.to_string(),
        pixel_values,
    })
}

/// Index of the largest logit. The first index wins a tie.
pub fn argmax(logits: &[f32]) -> Option<usize> {
    logits
        .iter()
        .enumerate()
        .fold(None, |best: Option<(usize, f32)>, (i, &v)| match best {
            Some((_, max)) if v <= max => best,
            _ => Some((i, v)),
        })
        .map(|(i, _)| i)
}

/// Runs MobileNetV2 on a batch of images and returns the ImageNet class of each image.
#[derive(Clone, Debug, Default)]
pub struct MobileNetV2Adapter;

impl OnnxAdapter<ImageRecord, i64> for MobileNetV2Adapter {
    fn prepare_inputs<'a>(&self, batch: &'a [ImageRecord]) -> Result<SessionInputs<'a, 'a>> {
        if let Some(bad) = batch
            .iter()
            .find(|img| img.pixel_values.len() != PIXELS_PER_IMAGE)
        {
            return Err(format!(
                "image '{}' has {} values, expected {PIXELS_PER_IMAGE}",
                bad.image_path,
                bad.pixel_values.len()
            )
            .into());
        }
        let flat: Vec<u8> = batch
            .iter()
            .flat_map(|img| img.pixel_values.iter().copied())
            .collect();
        let side = IMAGE_SIZE as usize;
        let tensor = ort::value::Tensor::from_array(([batch.len(), 3, side, side], flat))?;
        Ok(ort::inputs![INPUT_NAME => tensor].into())
    }

    fn parse_outputs(
        &self,
        batch: &[ImageRecord],
        outputs: SessionOutputs<'_>,
    ) -> Result<Vec<i64>> {
        let logits = outputs
            .get(OUTPUT_NAME)
            .ok_or_else(|| format!("model has no '{OUTPUT_NAME}' output"))?;
        let (shape, flat) = logits.try_extract_tensor::<f32>()?;
        let num_classes = match **shape {
            [n, classes] if usize::try_from(n) == Ok(batch.len()) && classes > 0 => {
                usize::try_from(classes)?
            }
            _ => {
                return Err(format!(
                    "unexpected '{OUTPUT_NAME}' shape {shape:?} for a batch of {}",
                    batch.len()
                )
                .into());
            }
        };
        flat.chunks_exact(num_classes)
            .map(|row| {
                argmax(row)
                    .ok_or_else(|| "empty logits row".into())
                    .and_then(|class| Ok(i64::try_from(class)?))
            })
            .collect()
    }
}

/// Classifies the images of a manifest into `path,class` lines.
///
/// The output has one line for each image. The failures have one record for each path that
/// cannot be read or decoded. Empty manifest lines are skipped.
pub struct ClassifyImages<H> {
    handler: H,
    inference_hints: ResourceHints,
}

impl<H: ModelHandler<ImageRecord, i64>> ClassifyImages<H> {
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            inference_hints: ResourceHints::new(),
        }
    }

    /// Resource hints of the inference step only, for example an accelerator.
    pub fn with_inference_hints(mut self, hints: ResourceHints) -> Self {
        self.inference_hints = hints;
        self
    }
}

impl<H: ModelHandler<ImageRecord, i64>> PTransform<PCollection<String>> for ClassifyImages<H> {
    type Output = WithFailures<String, Failure<String>>;

    fn expand(&self, paths: &PCollection<String>) -> Self::Output {
        // Spread the paths before the expensive read and decode.
        let images = paths
            .filter("SkipEmptyLines", |line: &String| !line.trim().is_empty())
            .reshuffle("Reshuffle")
            .try_map("LoadImage", |path: &String| load_image(path));
        // The second shuffle keeps the reads out of the inference stage: the reads scale on
        // their own workers, and only the inference runs on workers that match its hints.
        let inference = RunInference::new(self.handler.clone())
            .with_resource_hints(self.inference_hints.clone());
        let output = images
            .output
            .reshuffle("DecoupleReadsFromInference")
            .apply("RunInference", inference)
            .map("FormatPrediction", |res| {
                format!("{},{}", res.input.image_path, res.output)
            });
        WithFailures {
            output,
            failures: images.failures,
        }
    }
}

/// Builds the classification pipeline from [`Args`], run with `options`.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Pipeline {
    let p = Pipeline::create(options);
    let handler = OnnxModelHandler::new(OnnxConfig::from(args), MobileNetV2Adapter);
    let inference_hints = match &args.inference_accelerator {
        Some(accelerator) => ResourceHints::new().with_accelerator(accelerator),
        None => ResourceHints::new(),
    };

    let WithFailures { output, failures } = p
        .apply("ReadManifest", textio::Read::new(&args.input))
        .apply(
            "ClassifyImages",
            ClassifyImages::new(handler).with_inference_hints(inference_hints),
        );
    output.apply(
        "WritePredictions",
        textio::Write::new(&args.output).without_sharding(),
    );
    failures
        .map("FormatFailures", |f| format!("{}\t{}", f.input, f.error))
        .apply(
            "WriteFailures",
            textio::Write::new(&args.dlq_output).without_sharding(),
        );

    p
}
