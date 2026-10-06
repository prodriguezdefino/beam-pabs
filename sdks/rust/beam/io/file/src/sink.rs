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

//! The per-file writer contract used by [`WriteFiles`](crate::WriteFiles).
//!
//! [`FileSink::open`] returns a stateful [`FileSinkWriter`] for one output file, which
//! columnar formats need (Parquet buffers row groups and writes a footer on close). Formats
//! without per-file state implement [`FileFormat`] and use [`FormatSink`].

use std::io::Write;
use std::marker::PhantomData;
use std::sync::Arc;

/// Stateless writer of records `T`, with optional header and footer, to a byte stream.
/// Closures `Fn(&T, &mut dyn Write) -> beam::Result` implement it.
pub trait FileFormat<T>: Send + Sync + 'static {
    /// Writes one element.
    fn write_element(&self, element: &T, writer: &mut dyn Write) -> beam::Result;

    /// Written when a file is created.
    fn write_header(&self, _writer: &mut dyn Write) -> beam::Result {
        Ok(())
    }

    /// Written before a file is closed.
    fn write_footer(&self, _writer: &mut dyn Write) -> beam::Result {
        Ok(())
    }
}

impl<T, F> FileFormat<T> for F
where
    F: Fn(&T, &mut dyn Write) -> beam::Result + Send + Sync + 'static,
{
    fn write_element(&self, element: &T, writer: &mut dyn Write) -> beam::Result {
        self(element, writer)
    }
}

/// Plain text: each element on its own line.
#[derive(Clone, Copy, Debug, Default)]
pub struct TextFormat;

impl<T: AsRef<str> + 'static> FileFormat<T> for TextFormat {
    fn write_element(&self, element: &T, writer: &mut dyn Write) -> beam::Result {
        Ok(writeln!(writer, "{}", element.as_ref())?)
    }
}

/// Describes how to write elements `T` into files. Shared across workers and bundles, so it
/// holds only configuration; per-file state goes in the [`FileSinkWriter`].
pub trait FileSink<T>: Send + Sync + 'static {
    /// Starts a new file on `out`, which is buffered. The writer must flush it in
    /// [`FileSinkWriter::finish`].
    fn open(&self, out: Box<dyn Write + Send>) -> beam::Result<Box<dyn FileSinkWriter<T>>>;
}

/// Writes the elements of one file. Created by [`FileSink::open`].
pub trait FileSinkWriter<T>: Send {
    /// Appends one element.
    fn write(&mut self, element: &T) -> beam::Result;

    /// Bytes accepted but not yet written to the stream (default 0). File rolling adds them
    /// to the bytes that reached the stream, for formats that buffer such as Parquet.
    fn buffered_bytes(&self) -> u64 {
        0
    }

    /// Writes any trailer, flushes, and closes the file.
    fn finish(self: Box<Self>) -> beam::Result;
}

impl<T: 'static, S: FileSink<T> + ?Sized> FileSink<T> for Arc<S> {
    fn open(&self, out: Box<dyn Write + Send>) -> beam::Result<Box<dyn FileSinkWriter<T>>> {
        (**self).open(out)
    }
}

/// Adapts a [`FileFormat`] to [`FileSink`]: header on open, footer on finish.
pub struct FormatSink<T, F> {
    format: Arc<F>,
    _marker: PhantomData<fn(&T)>,
}

impl<T, F> FormatSink<T, F> {
    pub fn new(format: F) -> Self {
        Self::from_arc(Arc::new(format))
    }

    pub fn from_arc(format: Arc<F>) -> Self {
        Self {
            format,
            _marker: PhantomData,
        }
    }
}

impl<T, F> Clone for FormatSink<T, F> {
    fn clone(&self) -> Self {
        Self::from_arc(Arc::clone(&self.format))
    }
}

impl<T: 'static, F: FileFormat<T>> FileSink<T> for FormatSink<T, F> {
    fn open(&self, mut out: Box<dyn Write + Send>) -> beam::Result<Box<dyn FileSinkWriter<T>>> {
        self.format.write_header(&mut *out)?;
        Ok(Box::new(FormatSinkWriter {
            format: Arc::clone(&self.format),
            out,
            _marker: PhantomData,
        }))
    }
}

struct FormatSinkWriter<T, F> {
    format: Arc<F>,
    out: Box<dyn Write + Send>,
    _marker: PhantomData<fn(&T)>,
}

impl<T, F: FileFormat<T>> FileSinkWriter<T> for FormatSinkWriter<T, F> {
    fn write(&mut self, element: &T) -> beam::Result {
        self.format.write_element(element, &mut *self.out)
    }

    fn finish(mut self: Box<Self>) -> beam::Result {
        self.format.write_footer(&mut *self.out)?;
        Ok(self.out.flush()?)
    }
}
