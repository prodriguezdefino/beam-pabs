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

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use external::expansionx::JavaExpansionArtifact;

const THREADS: usize = 8;
const CHUNKS: usize = 16;
const CHUNK_SIZE: usize = 16 * 1024;
const CHUNK_DELAY: Duration = Duration::from_millis(5);

fn body() -> Vec<u8> {
    (0..CHUNKS * CHUNK_SIZE)
        .map(|i| u8::try_from(i % 251).expect("fits in u8"))
        .collect()
}

fn handle(stream: TcpStream, body: &[u8]) {
    let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));
    let mut line = String::new();
    // Drain request headers.
    while reader.read_line(&mut line).is_ok_and(|n| n > 0) && line != "\r\n" {
        line.clear();
    }
    let mut stream = stream;
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/java-archive\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    if stream.write_all(header.as_bytes()).is_err() {
        return;
    }
    // Slow chunked writes keep concurrent downloads overlapping.
    for chunk in body.chunks(CHUNK_SIZE) {
        if stream
            .write_all(chunk)
            .and_then(|()| stream.flush())
            .is_err()
        {
            return;
        }
        thread::sleep(CHUNK_DELAY);
    }
}

fn spawn_server(body: Arc<Vec<u8>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind local server");
    let addr = listener.local_addr().expect("local addr");
    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let body = Arc::clone(&body);
            thread::spawn(move || handle(stream, &body));
        }
    });
    format!("http://{addr}")
}

fn unique_cache_dir() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("beam_artifact_race_{}_{nanos}", std::process::id()))
}

#[test]
fn test_concurrent_resolve_jar_downloads_do_not_clobber_each_other() {
    let body = Arc::new(body());
    let repo_url = spawn_server(Arc::clone(&body));
    let cache_dir = unique_cache_dir();

    // Custom coordinates skip the prebuilt-JAR lookup in the Beam source tree.
    let artifact = JavaExpansionArtifact::from_target("org.example:race-test:1.0.0")
        .expect("parse coordinates")
        .with_repository_url(repo_url)
        .with_cache_dir(&cache_dir);

    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let artifact = artifact.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                artifact.resolve_jar()
            })
        })
        .collect();

    let results: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().expect("download thread panicked"))
        .collect();

    let expected = cache_dir.join(artifact.jar_name());
    let errors: Vec<String> = results
        .iter()
        .filter_map(|r| r.as_ref().err().map(ToString::to_string))
        .collect();
    assert!(errors.is_empty(), "concurrent downloads failed: {errors:?}");
    for path in results.into_iter().map(Result::unwrap) {
        assert_eq!(path, expected);
    }

    let content = std::fs::read(&expected).expect("read cached jar");
    assert!(
        content == *body,
        "cached jar content differs from served body"
    );

    let leftovers: Vec<PathBuf> = std::fs::read_dir(&cache_dir)
        .expect("read cache dir")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "tmp"))
        .collect();
    assert!(leftovers.is_empty(), "leftover temp files: {leftovers:?}");

    let _ = std::fs::remove_dir_all(&cache_dir);
}
