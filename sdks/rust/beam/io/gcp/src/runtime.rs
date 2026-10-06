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

//! Google Cloud client calls on the shared async I/O runtime of `beam::internals`.
//!
//! The clients use rustls, so each entry point installs the `ring` crypto provider first.

use std::future::Future;
use std::io;
use std::sync::Once;

/// Installs the rustls `ring` provider once for the process. Another provider that is
/// already installed stays.
fn install_crypto_provider() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Returns the shared async I/O runtime. Spawn Google Cloud client tasks on it.
pub fn client_runtime() -> io::Result<&'static tokio::runtime::Runtime> {
    install_crypto_provider();
    beam::internals::io_runtime()
}

/// Waits for `fut` in the context of the shared async I/O runtime, from any runtime context.
pub fn block_on_async<F, T>(fut: F) -> io::Result<T>
where
    F: Future<Output = io::Result<T>>,
{
    install_crypto_provider();
    beam::internals::block_on_io(fut)?
}
