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

//! Integration tests for the Computer Vision ONNX Classification example pipeline on Prism.

use std::io::Cursor;

use beam::coders::DefaultCoder;
use beam::ml::handler::{BatchBounds, InferenceArgs, ModelHandler};
use beam::ml::onnx::OnnxExecutionProvider;
use beam::ml::{PredictionResult, RunInference};
use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use beam::transforms::VecBatchConverter;
use cv_onnx_classification::{
    CvOnnxArgs, IMAGE_SIZE, IMAGENET_MEAN, IMAGENET_STD, ImageRecord, PIXELS_PER_IMAGE, argmax,
    create_onnx_config, load_image, preprocess_image,
};
use image::{ImageFormat, Rgb, RgbImage};

/// Encodes a solid-color image of the given size as PNG bytes.
fn solid_png(width: u32, height: u32, color: [u8; 3]) -> Vec<u8> {
    let mut bytes = Vec::new();
    RgbImage::from_pixel(width, height, Rgb(color))
        .write_to(&mut Cursor::new(&mut bytes), ImageFormat::Png)
        .expect("encoding PNG");
    bytes
}

fn sample_images() -> Vec<ImageRecord> {
    vec![
        ImageRecord {
            image_path: "gs://bucket/ILSVRC2012_val_00000001.JPEG".to_string(),
            pixel_values: vec![0.5f32; PIXELS_PER_IMAGE],
        },
        ImageRecord {
            image_path: "gs://bucket/ILSVRC2012_val_00000002.JPEG".to_string(),
            pixel_values: vec![0.1f32; PIXELS_PER_IMAGE],
        },
    ]
}

#[test]
fn test_onnx_config_from_flags_and_defaults() {
    let raw_args = vec![
        "cv_onnx_classification",
        "--model_path=gs://bucket/models/mobilenet.onnx",
        "--device=cuda",
        "--device_id=1",
        "--min_batch_size=4",
        "--max_batch_size=16",
    ];

    let (_, args) = beam::options::parse_from::<CvOnnxArgs, _, _>(raw_args);
    let config = create_onnx_config(&args);

    assert_eq!(config.model_path, "gs://bucket/models/mobilenet.onnx");
    assert_eq!(config.batch_bounds, BatchBounds::new(4, 16));
    assert!(!config.allow_cpu_fallback);
    assert_eq!(
        config.execution_provider,
        OnnxExecutionProvider::Cuda { device_id: 1 }
    );

    let (_, default_args) = beam::options::parse_from::<CvOnnxArgs, _, _>([
        "cv_onnx_classification",
        "--model_path=model.onnx",
    ]);
    let default_config = create_onnx_config(&default_args);

    assert_eq!(default_config.model_path, "model.onnx");
    assert_eq!(default_config.batch_bounds, BatchBounds::new(10, 100));
    assert!(!default_config.allow_cpu_fallback);
    assert_eq!(
        default_config.execution_provider,
        OnnxExecutionProvider::Cpu
    );
}

#[test]
fn test_preprocess_resizes_and_normalizes_to_chw() {
    let color = [255u8, 0, 128];
    let pixels = preprocess_image(&solid_png(300, 200, color)).expect("preprocessing");

    assert_eq!(pixels.len(), PIXELS_PER_IMAGE);
    let plane = (IMAGE_SIZE * IMAGE_SIZE) as usize;
    for (channel, values) in pixels.chunks_exact(plane).enumerate() {
        let expected =
            (f32::from(color[channel]) / 255.0 - IMAGENET_MEAN[channel]) / IMAGENET_STD[channel];
        assert!(
            values.iter().all(|v| (v - expected).abs() < 1e-6),
            "channel {channel} should be {expected}"
        );
    }

    assert!(preprocess_image(b"not an image").is_err());
}

#[test]
fn test_load_image_reads_through_filesystem_registry() {
    let path = std::env::temp_dir().join(format!("cv_onnx_test_{}.png", std::process::id()));
    std::fs::write(&path, solid_png(10, 20, [1, 2, 3])).expect("writing PNG");
    let path = path.to_string_lossy().into_owned();

    let record = load_image(&path).expect("loading local image");
    assert_eq!(record.image_path, path);
    assert_eq!(record.pixel_values.len(), PIXELS_PER_IMAGE);

    let err = load_image("/nonexistent/image.jpg").expect_err("missing image must fail");
    assert!(err.to_string().contains("/nonexistent/image.jpg"));
}

#[test]
fn test_argmax_matches_torch_semantics() {
    assert_eq!(argmax(&[0.1, 3.0, -1.0, 2.0]), Some(1));
    assert_eq!(argmax(&[5.0, 5.0, 1.0]), Some(0), "first index wins ties");
    assert_eq!(argmax(&[]), None);
}

#[test]
fn test_image_record_wire_coder_roundtrip() {
    let img = ImageRecord {
        image_path: "gs://bucket/ILSVRC2012_val_00000001.JPEG".to_string(),
        pixel_values: vec![0.123, -0.456, 0.789],
    };

    let mut buf = Vec::new();
    img.encode_element(&mut buf).expect("encoding ImageRecord");

    let mut slice = buf.as_slice();
    let decoded = ImageRecord::decode_element(&mut slice).expect("decoding ImageRecord");

    assert_eq!(decoded, img);
}

/// Mock ONNX classification model handler for verifying pipeline topology on Prism.
#[derive(Clone, Debug, Default)]
struct MockOnnxClassificationHandler;

impl ModelHandler<ImageRecord, i64> for MockOnnxClassificationHandler {
    type Model = ();
    type Batch = Vec<ImageRecord>;
    type Converter = VecBatchConverter<ImageRecord>;

    fn load_model(&self) -> Result<Self::Model> {
        Ok(())
    }

    fn run_inference(
        &self,
        batch: &Self::Batch,
        _model: &Self::Model,
        _inference_args: Option<&InferenceArgs>,
    ) -> Result<Vec<i64>> {
        Ok(batch
            .iter()
            .map(|img| i64::from(img.pixel_values[0] > 0.3))
            .collect())
    }

    fn get_batch_converter(&self) -> Self::Converter {
        VecBatchConverter::new()
    }

    fn get_batch_bounds(&self) -> BatchBounds {
        BatchBounds::new(1, 16)
    }
}

#[tokio::test]
async fn test_cv_onnx_pipeline_on_prism() {
    let p = TestPipeline::new();
    let input = p.apply(Create::new("CreateSampleImages", sample_images()));
    let reshuffled = input.reshuffle("ReshuffleImages");

    let predictions = input.apply(RunInference::new(
        "OnnxMobileNetClassification",
        MockOnnxClassificationHandler,
    ));
    let reshuffled_predictions = reshuffled.apply(RunInference::new(
        "OnnxMobileNetClassificationReshuffled",
        MockOnnxClassificationHandler,
    ));

    let verify_results = |results: &[PredictionResult<ImageRecord, i64>]| {
        let mut lines: Vec<String> = results
            .iter()
            .map(|res| format!("{},{}", res.input.image_path, res.output))
            .collect();
        lines.sort();
        let expected = [
            "gs://bucket/ILSVRC2012_val_00000001.JPEG,1",
            "gs://bucket/ILSVRC2012_val_00000002.JPEG,0",
        ];
        (lines == expected)
            .then_some(())
            .ok_or_else(|| format!("unexpected predictions: {lines:?}").into())
    };

    passert::that("AssertPredictions", &predictions).has_count(2);
    passert::that("AssertPredictions", &predictions).satisfies(verify_results);

    passert::that("AssertReshuffledPredictions", &reshuffled_predictions).has_count(2);
    passert::that("AssertReshuffledPredictions", &reshuffled_predictions).satisfies(verify_results);

    p.run()
        .await
        .expect("pipeline should succeed on Prism runner");
}
