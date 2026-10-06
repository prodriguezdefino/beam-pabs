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

//! The error type returned by user code: [`Error`] and its [`Result`] alias.

use std::fmt;

/// The boxed form of any standard error, as kept by [`Error::source`].
type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// The error returned by every fallible user hook (`DoFn` methods, state, timers, side inputs,
/// emits). `?` converts from any [`std::error::Error`] and from `String`/`&str`, and keeps the
/// original error as [`source`](Self::source). An `Err` from a `DoFn` fails the bundle; use
/// [`TryMap`](crate::transforms::TryMap) or [`TryParDo`](crate::transforms::TryParDo) for
/// dead-letter routing.
///
/// This type does not implement [`std::error::Error`], so one blanket `From` covers both
/// standard errors and strings.
///
/// ```
/// fn parse(line: &str) -> beam::Result<i64> {
///     let n: i64 = line.trim().parse()?;
///     if n < 0 {
///         return Err(format!("negative value: {n}").into());
///     }
///     Ok(n)
/// }
/// assert_eq!(parse(" 7 ").unwrap(), 7);
/// assert_eq!(parse("-1").unwrap_err().to_string(), "negative value: -1");
/// assert!(parse("x").unwrap_err().source().is_some());
/// ```
pub struct Error {
    message: String,
    source: Option<BoxError>,
}

/// `Result<T, beam::Error>`; `E` may be given explicitly (`Result<T, E>`).
pub type Result<T = (), E = Error> = std::result::Result<T, E>;

impl Error {
    /// Builds an error from a message alone, with no underlying source.
    pub fn msg(message: impl fmt::Display) -> Self {
        Self {
            message: message.to_string(),
            source: None,
        }
    }

    /// Prefixes the message with `context`, for example `"reading config: file not found"`.
    /// Keeps the source.
    pub fn context(self, context: impl fmt::Display) -> Self {
        Self {
            message: format!("{context}: {}", self.message),
            source: self.source,
        }
    }

    /// The error this one was converted from. Downcast it to recover the original type:
    /// `err.source().and_then(|e| e.downcast_ref::<std::io::Error>())`.
    pub fn source(&self) -> Option<&(dyn std::error::Error + Send + Sync + 'static)> {
        self.source.as_deref()
    }
}

impl<E: Into<BoxError>> From<E> for Error {
    fn from(error: E) -> Self {
        let source = error.into();
        Self {
            message: source.to_string(),
            source: Some(source),
        }
    }
}

/// Lets SDK plumbing that reports errors as text propagate an [`Error`] with `?`.
impl From<Error> for String {
    fn from(error: Error) -> Self {
        error.message
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

/// Prints the message followed by the causes behind the source, one per line, which is
/// what `fn main() -> beam::Result` shows on failure.
impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)?;
        let mut cause = self.source().and_then(|s| s.source());
        if cause.is_some() {
            f.write_str("\n\nCaused by:")?;
        }
        while let Some(err) = cause {
            write!(f, "\n    {err}")?;
            cause = err.source();
        }
        Ok(())
    }
}
