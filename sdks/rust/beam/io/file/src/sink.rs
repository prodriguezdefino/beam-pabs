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

use std::io::{self, BufWriter, IntoInnerError, Write};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::compression::{CompressedWriter, Compression};

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

/// The byte stream of one output file: a buffer over a compressor over the file.
pub struct FileOutput {
    inner: BufWriter<CompressedWriter<CountingWriter>>,
    /// Counts the accepted bytes. Only uncompressed files count here.
    accepted: Option<Arc<AtomicU64>>,
}

impl FileOutput {
    /// Buffers writes to `out` and compresses them. Fails on [`Compression::Auto`].
    pub fn new(out: Box<dyn Write + Send>, compression: Compression) -> io::Result<Self> {
        Self::counted(out, compression).map(|(output, _)| output)
    }

    /// Also returns the size of the file so far: the bytes written when uncompressed, else
    /// the compressed bytes that reached `out`.
    pub(crate) fn counted(
        out: Box<dyn Write + Send>,
        compression: Compression,
    ) -> io::Result<(Self, Arc<AtomicU64>)> {
        let size = Arc::new(AtomicU64::new(0));
        let (accepted, stored) = match compression {
            Compression::Uncompressed => (Some(Arc::clone(&size)), None),
            Compression::Auto
            | Compression::Gzip
            | Compression::Zstd
            | Compression::Deflate
            | Compression::Snappy => (None, Some(Arc::clone(&size))),
        };
        let counting = CountingWriter {
            inner: out,
            count: stored,
        };
        let inner = BufWriter::new(compression.writer(counting)?);
        Ok((Self { inner, accepted }, size))
    }

    /// Flushes the buffer, ends the compressed stream and flushes the file.
    pub fn close(self) -> io::Result<()> {
        self.inner
            .into_inner()
            .map_err(IntoInnerError::into_error)?
            .finish()?
            .flush()
    }
}

impl Write for FileOutput {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        add(self.accepted.as_deref(), n);
        Ok(n)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.inner.write_all(buf)?;
        add(self.accepted.as_deref(), buf.len());
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Counts the compressed bytes that reach the file. Only compressed files count here.
struct CountingWriter {
    inner: Box<dyn Write + Send>,
    count: Option<Arc<AtomicU64>>,
}

impl Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        add(self.count.as_deref(), n);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn add(counter: Option<&AtomicU64>, bytes: usize) {
    if let Some(counter) = counter {
        counter.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

/// Whether a [`FileSink`] accepts a stream compressed by [`WriteFiles`](crate::WriteFiles).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamCompression {
    /// The sink writes bytes that can be compressed as a stream.
    Supported,
    /// The format compresses inside the file. The text tells how to set its codec.
    Unsupported(&'static str),
}

/// Describes how to write elements `T` into files. Shared across workers and bundles, so it
/// holds only configuration; per-file state goes in the [`FileSinkWriter`].
pub trait FileSink<T>: Send + Sync + 'static {
    /// Starts a new file on `out`. [`FileSinkWriter::finish`] gives `out` back.
    fn open(&self, out: FileOutput) -> beam::Result<Box<dyn FileSinkWriter<T>>>;

    /// Whether [`WriteFiles::with_compression`](crate::WriteFiles::with_compression) can
    /// compress the files of this sink. Defaults to [`StreamCompression::Supported`].
    fn stream_compression(&self) -> StreamCompression {
        StreamCompression::Supported
    }
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

    /// Writes any trailer and returns the stream. The caller closes it.
    fn finish(self: Box<Self>) -> beam::Result<FileOutput>;
}

impl<T: 'static, S: FileSink<T> + ?Sized> FileSink<T> for Arc<S> {
    fn open(&self, out: FileOutput) -> beam::Result<Box<dyn FileSinkWriter<T>>> {
        (**self).open(out)
    }

    fn stream_compression(&self) -> StreamCompression {
        (**self).stream_compression()
    }
}

/// Adapts a [`FileFormat`] to [`FileSink`]: header on open, footer on finish.
pub struct FormatSink<T, F> {
    format: Arc<F>,
    _marker: PhantomData<fn(&T)>,
}

impl<T, F> FormatSink<T, F> {
    pub fn new(format: F) -> Self {
        Self::from(Arc::new(format))
    }
}

impl<T, F> From<Arc<F>> for FormatSink<T, F> {
    fn from(format: Arc<F>) -> Self {
        Self {
            format,
            _marker: PhantomData,
        }
    }
}

impl<T, F> Clone for FormatSink<T, F> {
    fn clone(&self) -> Self {
        Self::from(Arc::clone(&self.format))
    }
}

impl<T: 'static, F: FileFormat<T>> FileSink<T> for FormatSink<T, F> {
    fn open(&self, mut out: FileOutput) -> beam::Result<Box<dyn FileSinkWriter<T>>> {
        self.format.write_header(&mut out)?;
        Ok(Box::new(FormatSinkWriter {
            format: Arc::clone(&self.format),
            out,
            _marker: PhantomData,
        }))
    }
}

struct FormatSinkWriter<T, F> {
    format: Arc<F>,
    out: FileOutput,
    _marker: PhantomData<fn(&T)>,
}

impl<T, F: FileFormat<T>> FileSinkWriter<T> for FormatSinkWriter<T, F> {
    fn write(&mut self, element: &T) -> beam::Result {
        self.format.write_element(element, &mut self.out)
    }

    fn finish(mut self: Box<Self>) -> beam::Result<FileOutput> {
        self.format.write_footer(&mut self.out)?;
        Ok(self.out)
    }
}
