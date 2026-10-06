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

//! Computer Vision ONNX classification pipeline.
//!
//! Pipeline steps:
//! - Read a manifest of image paths (one per line, any registered filesystem scheme).
//! - For each path, read bytes, decode, convert to RGB, resize to exactly 224x224 (bilinear,
//!   no crop), scale to `[0, 1]`, and normalize with ImageNet mean and standard deviation
//!   into a CHW `f32` tensor.
//! - Run MobileNetV2 (exported to ONNX by `export_model.py`) with dynamic batches of 10 to
//!   100 images.
//! - Write `path,argmax` lines to a single output file.

use std::io;

use beam::coders::{Coder, CoderError, CoderRegistry, Context, DefaultCoder, URN_KV};
use beam::io::file::filesystem::read_to_bytes;
use beam::ml::onnx::{
    OnnxAdapter, OnnxConfig, OnnxDeviceOptions, OnnxModelHandler, SessionInputs, SessionOutputs,
    ort,
};
use beam::ml::{BatchBounds, RunInference};
use beam::prelude::*;
use clap::Args;
use image::imageops::{self, FilterType};
use serde::{Deserialize, Serialize};

pub use beam::ml::onnx;

/// Model input tensor name; must match `INPUT_NAME` in `export_model.py`.
pub const INPUT_NAME: &str = "input";
/// Model output tensor name; must match `OUTPUT_NAME` in `export_model.py`.
pub const OUTPUT_NAME: &str = "logits";
/// Square input resolution expected by MobileNetV2.
pub const IMAGE_SIZE: u32 = 224;
/// Number of `f32` values in one preprocessed CHW image.
pub const PIXELS_PER_IMAGE: usize = 3 * (IMAGE_SIZE as usize) * (IMAGE_SIZE as usize);
/// ImageNet per-channel mean (RGB), as in torchvision's pretrained models.
pub const IMAGENET_MEAN: [f32; 3] = [0.485, 0.456, 0.406];
/// ImageNet per-channel standard deviation (RGB).
pub const IMAGENET_STD: [f32; 3] = [0.229, 0.224, 0.225];

/// Default manifest: 50k OpenImages paths.
pub const DEFAULT_INPUT_IMAGES_MANIFEST: &str =
    "gs://apache-beam-ml/testing/inputs/openimage_50k_benchmark.txt";
pub const DEFAULT_OUTPUT: &str = "/tmp/onnx_predictions.txt";

/// Preprocessed normalized image record (3 x 224 x 224 FP32).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ImageRecord {
    /// Image path exactly as listed in the manifest.
    pub image_path: String,
    /// Flattened CHW pixel values normalized with ImageNet mean and std.
    pub pixel_values: Vec<f32>,
}

/// Wire coder for [`ImageRecord`].
#[derive(Clone, Debug)]
pub struct ImageRecordCoder;

impl Coder<ImageRecord> for ImageRecordCoder {
    fn urn(&self) -> &'static str {
        URN_KV
    }

    fn encode(
        &self,
        element: &ImageRecord,
        writer: &mut dyn io::Write,
        _context: Context,
    ) -> Result<(), CoderError> {
        element.encode_element(writer)
    }

    fn decode(
        &self,
        reader: &mut dyn io::Read,
        _context: Context,
    ) -> Result<ImageRecord, CoderError> {
        ImageRecord::decode_element(reader)
    }
}

impl DefaultCoder for ImageRecord {
    type Coder = ImageRecordCoder;

    fn coder() -> Self::Coder {
        ImageRecordCoder
    }

    fn encode_element(&self, writer: &mut dyn io::Write) -> Result<(), CoderError> {
        self.image_path.encode_element(writer)?;
        let bytes: Vec<u8> = self
            .pixel_values
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        bytes.encode_element(writer)?;
        Ok(())
    }

    fn decode_element(reader: &mut dyn io::Read) -> Result<Self, CoderError> {
        let image_path = String::decode_element(reader)?;
        let bytes = Vec::<u8>::decode_element(reader)?;
        let pixel_values = bytes
            .as_chunks::<4>()
            .0
            .iter()
            .map(|chunk| f32::from_le_bytes(*chunk))
            .collect();
        Ok(Self {
            image_path,
            pixel_values,
        })
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        let str_id = String::register_coder(registry);
        let bytes_id = Vec::<u8>::register_coder(registry);
        registry.register_coder(URN_KV, vec![str_id, bytes_id])
    }
}

/// Errors raised while reading and preprocessing an image.
#[derive(Debug, thiserror::Error)]
pub enum ImageLoadError {
    #[error("reading image '{path}': {source}")]
    Read {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("decoding image '{path}': {source}")]
    Decode {
        path: String,
        #[source]
        source: image::ImageError,
    },
}

/// Decodes image bytes into a flattened CHW tensor: RGB, resize to exactly 224x224
/// (bilinear with antialiasing, no crop), scale to `[0, 1]`, normalize with ImageNet
/// mean and standard deviation.
pub fn preprocess_image(bytes: &[u8]) -> Result<Vec<f32>, image::ImageError> {
    let rgb = image::load_from_memory(bytes)?.to_rgb8();
    let resized = imageops::resize(&rgb, IMAGE_SIZE, IMAGE_SIZE, FilterType::Triangle);
    Ok((0..3)
        .flat_map(|channel| {
            let resized = &resized;
            resized.pixels().map(move |pixel| {
                (f32::from(pixel.0[channel]) / 255.0 - IMAGENET_MEAN[channel])
                    / IMAGENET_STD[channel]
            })
        })
        .collect())
}

/// Reads the image at `path` through Beam's FileSystem registry and preprocesses it.
pub fn load_image(path: &str) -> Result<ImageRecord, ImageLoadError> {
    let bytes = read_to_bytes(path).map_err(|source| ImageLoadError::Read {
        path: path.to_string(),
        source,
    })?;
    let pixel_values = preprocess_image(&bytes).map_err(|source| ImageLoadError::Decode {
        path: path.to_string(),
        source,
    })?;
    Ok(ImageRecord {
        image_path: path.to_string(),
        pixel_values,
    })
}

/// Index of the largest logit (first index on ties).
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

/// Adapter executing MobileNetV2 computer vision classification through ONNX Runtime.
///
/// Produces the predicted ImageNet class index (argmax of the logits) for each image.
#[derive(Clone, Debug, Default)]
pub struct MobileNetV2OnnxAdapter;

impl OnnxAdapter<ImageRecord, i64> for MobileNetV2OnnxAdapter {
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
        let flat: Vec<f32> = batch
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

/// Command line arguments for the Computer Vision ONNX Classification pipeline.
#[derive(Args, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "cv_onnx_classification",
    about = "Apache Beam Rust Computer Vision ONNX Runtime Image Classification Example"
)]
pub struct CvOnnxArgs {
    /// Path to manifest file containing image paths / URLs (local path or `gs://...`).
    #[arg(long, default_value = DEFAULT_INPUT_IMAGES_MANIFEST)]
    pub input: String,

    /// Path to write classification predictions (local path or `gs://...`).
    #[arg(long, default_value = DEFAULT_OUTPUT)]
    pub output: String,

    /// Path or URI of the MobileNetV2 ONNX model, exported from
    /// `imagenet_classification_mobilenet_v2.pt` by `export_model.py` (required).
    #[arg(long)]
    pub model_path: String,

    /// ONNX Runtime device: `--device`, `--device_id`, `--allow_cpu_fallback`, `--dylib_path`.
    #[command(flatten)]
    #[serde(flatten)]
    pub accelerator: OnnxDeviceOptions,

    /// Minimum RunInference batch size.
    #[arg(long, default_value_t = 10)]
    pub min_batch_size: usize,

    /// Maximum RunInference batch size.
    #[arg(long, default_value_t = 100)]
    pub max_batch_size: usize,

    /// Path to write images that could not be read or decoded, as `path<TAB>error`
    /// lines. Defaults to `<output>_failures`.
    #[arg(long)]
    pub dlq_output: Option<String>,
}

impl CvOnnxArgs {
    /// Where failed images are written: `--dlq_output`, or `<output>_failures`.
    pub fn dlq_output(&self) -> String {
        self.dlq_output
            .clone()
            .unwrap_or_else(|| format!("{}_failures", self.output))
    }
}

impl PipelineOptionGroup for CvOnnxArgs {}

/// Writes image-loading failures as `path<TAB>error` lines.
#[derive(Debug, Clone)]
pub struct WriteFailures {
    path: String,
}

impl WriteFailures {
    /// Writes to `path` (local path or `gs://...`).
    pub fn to(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }
}

impl PTransform<PCollection<Failure<String>>> for WriteFailures {
    type Output = PCollection<String>;

    fn expand(&self, failures: &PCollection<Failure<String>>) -> Self::Output {
        failures
            .map("FormatFailures", |f| format!("{}\t{}", f.input, f.error))
            .apply(textio::Write::new("WriteLines", &self.path).without_sharding())
    }
}

/// Builds the complete Computer Vision ONNX Classification pipeline from [`CvOnnxArgs`].
pub fn build_pipeline(options: &PipelineOptions, args: &CvOnnxArgs) -> Pipeline {
    let p = Pipeline::create(options);
    let handler = OnnxModelHandler::new(create_onnx_config(args), MobileNetV2OnnxAdapter);

    p.apply(textio::Read::new("ReadLines", &args.input))
        .filter("FilterEmptyLines", |line: &String| !line.trim().is_empty())
        .reshuffle("ReshuffleUrls")
        .try_map("ReadAndPreprocessImage", |path: &String| load_image(path))
        .failures_to(WriteFailures::to(args.dlq_output()))
        .reshuffle("ReshuffleImages")
        .apply(RunInference::new("OnnxMobileNetV2", handler))
        .map("FormatOutput", |res| {
            format!("{},{}", res.input.image_path, res.output)
        })
        .apply(textio::Write::new("WriteLines", &args.output).without_sharding());

    p
}

/// Creates an [`OnnxConfig`] from command-line arguments.
pub fn create_onnx_config(args: &CvOnnxArgs) -> OnnxConfig {
    OnnxConfig::new(&args.model_path)
        .with_device_options(&args.accelerator)
        .with_batch_bounds(BatchBounds::new(args.min_batch_size, args.max_batch_size))
}
