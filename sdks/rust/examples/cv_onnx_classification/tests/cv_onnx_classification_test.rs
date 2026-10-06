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

use std::io::Cursor;

use beam::ml::onnx::{OnnxConfig, OnnxExecutionProvider};
use beam::ml::{BatchBounds, InferenceArgs, ModelHandler};
use beam::pipeline::URN_RESOURCE_ACCELERATOR;
use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use beam::transforms::VecBatchConverter;
use cv_onnx_classification::{
    Args, ClassifyImages, IMAGE_SIZE, ImageRecord, PIXELS_PER_IMAGE, argmax, build_pipeline,
    preprocess_image,
};
use image::{ImageFormat, Rgb, RgbImage};

/// Encodes a solid-color image as PNG bytes.
fn solid_png(width: u32, height: u32, color: [u8; 3]) -> Vec<u8> {
    let mut bytes = Vec::new();
    RgbImage::from_pixel(width, height, Rgb(color))
        .write_to(&mut Cursor::new(&mut bytes), ImageFormat::Png)
        .expect("encoding PNG");
    bytes
}

/// Classifies an image as 1 when its first normalized value is positive, else as 0.
#[derive(Clone)]
struct BrightnessModel;

impl ModelHandler<ImageRecord, i64> for BrightnessModel {
    type Model = ();
    type Batch = Vec<ImageRecord>;
    type Converter = VecBatchConverter<ImageRecord>;

    fn load_model(&self) -> Result<()> {
        Ok(())
    }

    fn run_inference(
        &self,
        batch: &Vec<ImageRecord>,
        _: &(),
        _: Option<&InferenceArgs>,
    ) -> Result<Vec<i64>> {
        Ok(batch
            .iter()
            .map(|img| i64::from(img.pixel_values[0] > 127))
            .collect())
    }

    fn get_batch_converter(&self) -> Self::Converter {
        VecBatchConverter::new()
    }
}

#[tokio::test]
async fn test_classify_images_routes_bad_paths_to_failures() {
    let dir = std::env::temp_dir().join(format!("cv_onnx_test_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("creating temp dir");
    let path = |name: &str| dir.join(name).to_string_lossy().into_owned();
    std::fs::write(path("white.png"), solid_png(30, 20, [255, 255, 255])).expect("writing PNG");
    std::fs::write(path("black.png"), solid_png(20, 30, [0, 0, 0])).expect("writing PNG");
    std::fs::write(path("broken.png"), b"not an image").expect("writing file");

    let p = TestPipeline::new();
    let manifest = ["white.png", "black.png", "broken.png", "missing.png"].map(path);
    let WithFailures { output, failures } = p
        .apply(
            "Create",
            Create::new([manifest.to_vec(), vec![" ".into()]].concat()),
        )
        .apply("ClassifyImages", ClassifyImages::new(BrightnessModel));

    passert::that("AssertPredictions", &output).contains_in_any_order([
        format!("{},1", path("white.png")),
        format!("{},0", path("black.png")),
    ]);
    passert::that(
        "AssertFailedPaths",
        &failures.map("FailedPath", |f| f.input),
    )
    .contains_in_any_order([path("broken.png"), path("missing.png")]);

    p.run().await.expect("pipeline must succeed");
}

#[test]
fn test_onnx_config_from_args() {
    let (_, args) = beam::options::parse_from::<Args, _, _>([
        "cv_onnx_classification",
        "--model_path=gs://bucket/mobilenet.onnx",
        "--device=cuda",
        "--device_id=1",
        "--min_batch_size=4",
        "--max_batch_size=16",
    ]);
    let config = OnnxConfig::from(&args);

    assert_eq!(config.model_path, "gs://bucket/mobilenet.onnx");
    assert_eq!(config.batch_bounds, BatchBounds::new(4, 16));
    assert!(!config.allow_cpu_fallback);
    assert_eq!(
        config.execution_provider,
        OnnxExecutionProvider::Cuda { device_id: 1 }
    );
}

#[test]
fn test_preprocess_resizes_to_chw_bytes() {
    let color = [255u8, 0, 128];
    let pixels = preprocess_image(&solid_png(300, 200, color)).expect("preprocessing");

    assert_eq!(pixels.len(), PIXELS_PER_IMAGE);
    let plane = (IMAGE_SIZE * IMAGE_SIZE) as usize;
    for (channel, values) in pixels.chunks_exact(plane).enumerate() {
        assert!(
            values.iter().all(|&v| v == color[channel]),
            "channel {channel} should be {}",
            color[channel]
        );
    }
}

#[test]
fn test_only_inference_gets_the_accelerator_hint() {
    let accelerator = "type:nvidia-tesla-t4;count:1;install-nvidia-driver";
    let (options, args) = beam::options::parse_from::<Args, _, _>([
        "cv_onnx_classification",
        "--model_path=gs://bucket/mobilenet.onnx",
        &format!("--inference_accelerator={accelerator}"),
    ]);
    let p = build_pipeline(&options, &args);
    let components = p.to_proto().components.expect("pipeline components");
    let accelerator_of = |name: &str| {
        let transform = components
            .transforms
            .values()
            .find(|t| t.unique_name.contains(name) && !t.environment_id.is_empty())
            .unwrap_or_else(|| panic!("no executable transform named {name}"));
        components.environments[&transform.environment_id]
            .resource_hints
            .get(URN_RESOURCE_ACCELERATOR)
            .map(|hint| String::from_utf8_lossy(hint).into_owned())
    };

    assert_eq!(
        accelerator_of("RunInference/").as_deref(),
        Some(accelerator)
    );
    assert_eq!(accelerator_of("LoadImage"), None);
}

#[test]
fn test_argmax_takes_first_index_on_ties() {
    assert_eq!(argmax(&[0.1, 3.0, -1.0, 2.0]), Some(1));
    assert_eq!(argmax(&[5.0, 5.0, 1.0]), Some(0));
    assert_eq!(argmax(&[]), None);
}
