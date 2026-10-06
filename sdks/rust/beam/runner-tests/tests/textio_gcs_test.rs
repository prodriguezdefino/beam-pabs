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

use std::io::{Read, Write};

use beam::prelude::*;
use file::filesystem::{FileSystem, get_filesystem};
use file::textio;
use gcp::GcsFileSystem;
use prism::PrismRunner;

/// Names the bucket the live GCS tests run against.
///
/// GCP coverage is opt-in: it runs only with a bucket set (a real one with Application
/// Default Credentials, or an emulated one with `STORAGE_EMULATOR_HOST`).
const TEST_BUCKET_ENV: &str = "BEAM_TEST_GCS_BUCKET";

/// Returns the bucket to run against, or `None` once it has logged why it is skipping.
///
/// Falling back to another filesystem would let these tests report success without ever
/// reaching GCS, hiding exactly the failures they exist to catch, so an unconfigured or
/// unreachable bucket skips instead.
fn gcs_test_bucket(test_name: &str) -> Option<String> {
    let bucket = match std::env::var(TEST_BUCKET_ENV) {
        Ok(bucket) if !bucket.trim().is_empty() => bucket,
        _ => {
            eprintln!(
                "Skipping {test_name}: {TEST_BUCKET_ENV} is unset. Set it to a writable bucket \
                 reachable through Application Default Credentials, or point \
                 STORAGE_EMULATOR_HOST at a GCS emulator."
            );
            return None;
        }
    };

    match GcsFileSystem::new().exists(&format!("gs://{bucket}")) {
        Ok(true) => Some(bucket),
        Ok(false) => {
            eprintln!(
                "Skipping {test_name}: bucket gs://{bucket} is not visible to the ambient \
                 GCS credentials."
            );
            None
        }
        Err(err) => {
            eprintln!("Skipping {test_name}: GCS is unavailable ({err}).");
            None
        }
    }
}

/// Resolves `gs://` through the registry, proving the scheme is served by GCS itself.
fn gcs_filesystem(path: &str) -> std::sync::Arc<dyn FileSystem> {
    let resolved = get_filesystem(path).expect("gs scheme must be registered");
    assert!(
        format!("{resolved:?}").contains("GcsFileSystem"),
        "gs:// must resolve to GcsFileSystem, got {resolved:?}"
    );
    resolved
}

#[tokio::test]
async fn test_textio_gcs_glob_read_write_round_trip() {
    let Some(bucket) = gcs_test_bucket("test_textio_gcs_glob_read_write_round_trip") else {
        return;
    };
    let pid = std::process::id();

    let shard0 = format!("gs://{bucket}/shards_{pid}/shard-0.txt");
    let shard1 = format!("gs://{bucket}/shards_{pid}/shard-1.txt");
    let pattern = format!("gs://{bucket}/shards_{pid}/shard-*.txt");
    let merged = format!("gs://{bucket}/shards_{pid}/merged.txt");

    let resolved = gcs_filesystem(&pattern);

    {
        let mut w0 = resolved.open_write(&shard0).expect("open write shard0");
        w0.write_all(b"zero\n").unwrap();
        w0.flush().unwrap();

        let mut w1 = resolved.open_write(&shard1).expect("open write shard1");
        w1.write_all(b"one\n").unwrap();
        w1.flush().unwrap();
    }

    let p = Pipeline::new();
    let lines = p.apply(textio::Read::new("TextIO.Read", &pattern));
    let upper = lines.map("ToUpper", |w: String| w.to_uppercase());
    upper.apply(textio::Write::new("TextIO.Write", &merged).without_sharding());

    let res = p.run_with_runner(&PrismRunner::new()).await;
    assert!(res.is_ok(), "pipeline failed: {res:?}");

    let mut reader = resolved.open_read(&merged).expect("open read");
    let mut merged_str = String::new();
    reader.read_to_string(&mut merged_str).unwrap();
    let mut collected: Vec<&str> = merged_str.lines().collect();
    collected.sort();
    assert_eq!(collected, vec!["ONE", "ZERO"]);

    let _ = resolved.remove(&shard0);
    let _ = resolved.remove(&shard1);
    let _ = resolved.remove(&merged);
}
