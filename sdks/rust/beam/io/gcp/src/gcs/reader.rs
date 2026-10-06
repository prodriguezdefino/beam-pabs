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

//! Synchronous reads of a GCS object that a background task downloads.

use std::io::{self, BufRead, Read};

use bytes::Bytes;
use tokio::sync::mpsc::{self, Receiver};

use crate::runtime::recv_blocking;

/// Streaming reader that bridges an asynchronous GCS chunk stream into [`std::io::Read`].
#[doc(hidden)]
pub struct GcsStreamReader {
    receiver: Receiver<io::Result<Bytes>>,
    current_chunk: Option<Bytes>,
    offset: usize,
    /// Error received after bytes were copied; returned on the next call.
    pending_error: Option<io::Error>,
}

impl GcsStreamReader {
    pub fn new(receiver: Receiver<io::Result<Bytes>>) -> Self {
        Self {
            receiver,
            current_chunk: None,
            offset: 0,
            pending_error: None,
        }
    }

    fn next_chunk(&mut self) -> Option<io::Result<Bytes>> {
        recv_blocking(&mut self.receiver)
            .unwrap_or_else(|| Some(Err(io::Error::other("GCS stream thread panicked"))))
    }
}

impl Read for GcsStreamReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if let Some(err) = self.pending_error.take() {
            return Err(err);
        }

        let mut total_copied = 0;

        loop {
            if let Some(ref chunk) = self.current_chunk {
                if self.offset < chunk.len() {
                    let to_copy = (chunk.len() - self.offset).min(buf.len() - total_copied);
                    buf[total_copied..total_copied + to_copy]
                        .copy_from_slice(&chunk[self.offset..self.offset + to_copy]);
                    self.offset += to_copy;
                    total_copied += to_copy;

                    if total_copied == buf.len() {
                        return Ok(total_copied);
                    }
                }
                self.current_chunk = None;
                self.offset = 0;
            }

            // With bytes copied, take only queued chunks; do not block.
            if total_copied > 0 {
                match self.receiver.try_recv() {
                    Ok(Ok(chunk)) => {
                        self.current_chunk = Some(chunk);
                        self.offset = 0;
                        continue;
                    }
                    Ok(Err(err)) => {
                        self.pending_error = Some(err);
                        return Ok(total_copied);
                    }
                    Err(
                        mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected,
                    ) => {
                        return Ok(total_copied);
                    }
                }
            }

            match self.next_chunk() {
                Some(Ok(chunk)) => {
                    self.current_chunk = Some(chunk);
                    self.offset = 0;
                }
                Some(Err(err)) => return Err(err),
                None => return Ok(total_copied), // EOF: the sender terminated cleanly.
            }
        }
    }
}

impl BufRead for GcsStreamReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        // Skip zero-length chunks rather than reporting EOF.
        while self.offset >= self.current_chunk.as_ref().map_or(0, |c| c.len()) {
            self.current_chunk = None;
            self.offset = 0;
            if let Some(err) = self.pending_error.take() {
                return Err(err);
            }
            match self.next_chunk() {
                Some(Ok(chunk)) => self.current_chunk = Some(chunk),
                Some(Err(err)) => return Err(err),
                None => return Ok(&[]),
            }
        }
        Ok(self
            .current_chunk
            .as_ref()
            .and_then(|chunk| chunk.get(self.offset..))
            .unwrap_or(&[]))
    }

    fn consume(&mut self, amt: usize) {
        self.offset += amt;
    }
}
