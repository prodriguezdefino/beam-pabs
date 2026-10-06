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

//! Stream compression codecs for file sources and sinks.

use std::fmt;
use std::io::{self, Cursor, Read, Write};

use beam::schema::BeamEnum;
use flate2::Compression as FlateCompression;
use flate2::read::{MultiGzDecoder, ZlibDecoder};
use flate2::write::{GzEncoder, ZlibEncoder};
use snap::read::FrameDecoder as SnappyDecoder;
use snap::write::FrameEncoder as SnappyEncoder;
use zstd::stream::read::Decoder as ZstdDecoder;
use zstd::stream::write::Encoder as ZstdEncoder;

const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

/// Stream compression for reading and writing files.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, BeamEnum)]
#[beam(crate = "::beam")]
pub enum Compression {
    /// Detects the codec from the file extension when reading. Writes reject it.
    #[default]
    Auto,
    /// Uncompressed bytes.
    Uncompressed,
    /// Gzip (`.gz`, `.gzip`). Reads accept concatenated members.
    Gzip,
    /// Zstandard (`.zst`, `.zstd`).
    Zstd,
    /// Zlib-wrapped deflate (`.deflate`, `.zlib`).
    Deflate,
    /// Snappy framing format (`.sz`).
    Snappy,
}

impl Compression {
    /// The codecs that compress, in the order that [`Self::detect`] tries them.
    pub const CODECS: [Self; 4] = [Self::Gzip, Self::Zstd, Self::Deflate, Self::Snappy];

    /// File extensions of the codec. Writes append the first one.
    pub fn extensions(self) -> &'static [&'static str] {
        match self {
            Self::Auto | Self::Uncompressed => &[],
            Self::Gzip => &[".gz", ".gzip"],
            Self::Zstd => &[".zst", ".zstd"],
            Self::Deflate => &[".deflate", ".zlib"],
            Self::Snappy => &[".sz"],
        }
    }

    /// Detects the codec from the extension of `path`.
    pub fn detect(path: &str) -> Self {
        Self::CODECS
            .into_iter()
            .find(|codec| codec.matches(path))
            .unwrap_or(Self::Uncompressed)
    }

    /// Reports whether `path` ends with one of [`Self::extensions`], ignoring case.
    pub fn matches(self, path: &str) -> bool {
        self.extensions().iter().any(|ext| has_suffix(path, ext))
    }

    /// Resolves [`Self::Auto`] from `path`, or keeps an explicit codec.
    pub fn resolve(self, path: &str) -> Self {
        match self {
            Self::Auto => Self::detect(path),
            codec @ (Self::Uncompressed
            | Self::Gzip
            | Self::Zstd
            | Self::Deflate
            | Self::Snappy) => codec,
        }
    }

    /// Reports whether the file at `path` is compressed under this mode.
    pub fn is_compressed(self, path: &str) -> bool {
        self.resolve(path) != Self::Uncompressed
    }

    /// Appends the first of [`Self::extensions`] to `path` unless `path` already matches.
    pub fn append_suffix(self, path: impl Into<String>) -> String {
        let path = path.into();
        match self.extensions().first().copied() {
            Some(suffix) if !self.matches(&path) => path + suffix,
            Some(_) | None => path,
        }
    }

    /// Wraps `reader` in a decompressor. An empty stream reads as empty.
    ///
    /// A Gzip stream without the Gzip magic bytes is read as is: object stores can
    /// decompress `Content-Encoding: gzip` objects in transit.
    pub fn reader(self, reader: Box<dyn Read + Send>) -> io::Result<Box<dyn Read + Send>> {
        match self {
            Self::Auto => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Resolve Compression::Auto against a file path before opening a reader",
            )),
            Self::Uncompressed => Ok(reader),
            Self::Gzip => decoder(reader, |gzip_magic, stream| {
                Ok(if gzip_magic {
                    Box::new(MultiGzDecoder::new(stream))
                } else {
                    Box::new(stream)
                })
            }),
            Self::Zstd => decoder(reader, |_, stream| Ok(Box::new(ZstdDecoder::new(stream)?))),
            Self::Deflate => decoder(reader, |_, stream| Ok(Box::new(ZlibDecoder::new(stream)))),
            Self::Snappy => decoder(reader, |_, stream| Ok(Box::new(SnappyDecoder::new(stream)))),
        }
    }

    /// Wraps `writer` in a compressor. Fails on [`Self::Auto`].
    pub fn writer<W: Write>(self, writer: W) -> io::Result<CompressedWriter<W>> {
        match self {
            Self::Auto => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Compression::Auto is not supported for writing; choose a specific codec",
            )),
            Self::Uncompressed => Ok(CompressedWriter::Uncompressed(writer)),
            Self::Gzip => Ok(CompressedWriter::Gzip(GzEncoder::new(
                writer,
                FlateCompression::default(),
            ))),
            Self::Zstd => Ok(CompressedWriter::Zstd(ZstdEncoder::new(writer, 0)?)),
            Self::Deflate => Ok(CompressedWriter::Deflate(ZlibEncoder::new(
                writer,
                FlateCompression::default(),
            ))),
            Self::Snappy => Ok(CompressedWriter::Snappy(Box::new(SnappyEncoder::new(
                writer,
            )))),
        }
    }

    /// The error for a byte-range read of the compressed file at `path`.
    pub(crate) fn byte_range_error(self, path: &str) -> io::Error {
        io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "'{path}' is {self} compressed, and compressed files cannot be read by byte range"
            ),
        )
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Auto => "AUTO",
            Self::Uncompressed => "UNCOMPRESSED",
            Self::Gzip => "GZIP",
            Self::Zstd => "ZSTD",
            Self::Deflate => "DEFLATE",
            Self::Snappy => "SNAPPY",
        })
    }
}

fn has_suffix(path: &str, suffix: &str) -> bool {
    path.len()
        .checked_sub(suffix.len())
        .and_then(|start| path.get(start..))
        .is_some_and(|tail| tail.eq_ignore_ascii_case(suffix))
}

/// The peeked start of a stream, chained to the rest.
type Peeked = io::Chain<Cursor<Vec<u8>>, Box<dyn Read + Send>>;

/// Peeks the first bytes of `reader`, then calls `decode` with whether they are the Gzip
/// magic bytes. An empty stream skips `decode`, because some decoders fail on it.
fn decoder(
    mut reader: Box<dyn Read + Send>,
    decode: impl FnOnce(bool, Peeked) -> io::Result<Box<dyn Read + Send>>,
) -> io::Result<Box<dyn Read + Send>> {
    let mut head = Vec::with_capacity(GZIP_MAGIC.len());
    reader
        .by_ref()
        .take(GZIP_MAGIC.len() as u64)
        .read_to_end(&mut head)?;
    if head.is_empty() {
        return Ok(Box::new(io::empty()));
    }
    let gzip_magic = head == GZIP_MAGIC;
    decode(gzip_magic, Cursor::new(head).chain(reader))
}

/// A [`Write`] that compresses with one codec. `flush` keeps the stream open; call
/// [`finish`](Self::finish) to end it.
pub enum CompressedWriter<W: Write> {
    /// Passes the bytes through.
    Uncompressed(W),
    /// Gzip encoder.
    Gzip(GzEncoder<W>),
    /// Zstandard encoder.
    Zstd(ZstdEncoder<'static, W>),
    /// Zlib encoder.
    Deflate(ZlibEncoder<W>),
    /// Snappy frame encoder.
    Snappy(Box<SnappyEncoder<W>>),
}

impl<W: Write> CompressedWriter<W> {
    /// Writes the end of the compressed stream and returns the inner writer, unflushed.
    pub fn finish(self) -> io::Result<W> {
        match self {
            Self::Uncompressed(writer) => Ok(writer),
            Self::Gzip(encoder) => encoder.finish(),
            Self::Zstd(encoder) => encoder.finish(),
            Self::Deflate(encoder) => encoder.finish(),
            Self::Snappy(encoder) => encoder
                .into_inner()
                .map_err(snap::write::IntoInnerError::into_error),
        }
    }
}

impl<W: Write> Write for CompressedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Uncompressed(writer) => writer.write(buf),
            Self::Gzip(encoder) => encoder.write(buf),
            Self::Zstd(encoder) => encoder.write(buf),
            Self::Deflate(encoder) => encoder.write(buf),
            Self::Snappy(encoder) => encoder.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Uncompressed(writer) => writer.flush(),
            Self::Gzip(encoder) => encoder.flush(),
            Self::Zstd(encoder) => encoder.flush(),
            Self::Deflate(encoder) => encoder.flush(),
            Self::Snappy(encoder) => encoder.flush(),
        }
    }
}
