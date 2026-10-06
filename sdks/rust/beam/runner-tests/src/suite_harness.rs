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

//! Custom test harness for the Prism and Dataflow `ValidatesRunner` binaries.
//!
//! Evaluates [`RUNNER_SKIPS`](crate::RUNNER_SKIPS) for the active `(runner, mode)` at
//! startup so mode-specific skips report as real `ignored` tests and can run on demand
//! with `--ignored` or `--include-ignored`.

use std::any::Any;
use std::io::{self, Write};
use std::num::NonZeroUsize;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::registry::{
    RunnerKind, SuiteMode, SuiteRunner, VALIDATES_RUNNER_TESTS, ValidatesRunnerTest, run_test,
    runner_skip_reason,
};

/// How the suite runner treats tests listed in [`RUNNER_SKIPS`](crate::RUNNER_SKIPS).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RunIgnored {
    /// Report skipped tests as `ignored` without running them.
    #[default]
    Skip,
    /// Run both normal and skipped tests (`--include-ignored`).
    Include,
    /// Run only skipped tests (`--ignored`).
    Only,
}

/// Parsed command-line arguments of a `ValidatesRunner` test binary.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SuiteArgs {
    /// Runs only the tests whose name contains one of these.
    pub filters: Vec<String>,
    /// Skips the tests whose name contains one of these (`--skip`).
    pub skip_filters: Vec<String>,
    /// Matches the filters against the whole name (`--exact`).
    pub exact: bool,
    /// What to do with the tests that `RUNNER_SKIPS` lists.
    pub ignored: RunIgnored,
    /// Lists the tests and runs none (`--list`).
    pub list: bool,
    /// How many tests run at the same time (`--test-threads`).
    pub threads: Option<usize>,
}

impl SuiteArgs {
    /// Parses `libtest`-compatible flags from `args` (excluding `argv[0]`).
    pub fn parse<I, S>(args: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut parsed = Self::default();
        let mut iter = args.into_iter().map(Into::into);
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--ignored" => parsed.ignored = RunIgnored::Only,
                "--include-ignored" => parsed.ignored = RunIgnored::Include,
                "--exact" => parsed.exact = true,
                "--list" => parsed.list = true,
                "--nocapture" | "--show-output" | "-q" | "--quiet" | "--no-fail-fast"
                | "--test" => {}
                "--test-threads" => {
                    let value = iter.next().ok_or("--test-threads requires a value")?;
                    parsed.threads = Some(parse_threads(&value)?);
                }
                "--skip" => {
                    let value = iter.next().ok_or("--skip requires a filter")?;
                    parsed.skip_filters.push(value);
                }
                "--color" | "--format" | "--logfile" => {
                    let _ = iter
                        .next()
                        .ok_or_else(|| format!("{arg} requires a value"))?;
                }
                _ if let Some(threads) = arg.strip_prefix("--test-threads=") => {
                    parsed.threads = Some(parse_threads(threads)?);
                }
                _ if let Some(filter) = arg.strip_prefix("--skip=") => {
                    parsed.skip_filters.push(filter.to_string());
                }
                _ if arg.starts_with("--color=")
                    || arg.starts_with("--format=")
                    || arg.starts_with("--logfile=") => {}
                _ if arg.starts_with('-') => {
                    return Err(format!("unrecognized option: {arg}"));
                }
                _ => parsed.filters.push(arg),
            }
        }
        Ok(parsed)
    }
}

fn parse_threads(raw: &str) -> Result<usize, String> {
    raw.parse::<NonZeroUsize>()
        .map(NonZeroUsize::get)
        .map_err(|_| format!("invalid --test-threads value: {raw}"))
}

/// Action to take for one [`ValidatesRunnerTest`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TestDisposition {
    /// A filter leaves the test out.
    FilteredOut,
    /// The runner skips the test, for this reason.
    Ignored(&'static str),
    /// The test runs.
    Run,
}

/// Classifies `test` for `(runner, mode)` under `args`.
pub fn classify_test(
    test: &ValidatesRunnerTest,
    runner: RunnerKind,
    mode: SuiteMode,
    args: &SuiteArgs,
) -> TestDisposition {
    let full_name = format!("{}::{}", test.suite, test.id);
    let matches = |pattern: &str| {
        if args.exact {
            full_name == pattern || test.id == pattern
        } else {
            full_name.contains(pattern)
        }
    };
    let included = args.filters.is_empty() || args.filters.iter().any(|f| matches(f));
    let excluded = args.skip_filters.iter().any(|s| matches(s));
    if !included || excluded {
        return TestDisposition::FilteredOut;
    }
    let skip = runner_skip_reason(test.id, runner, mode);
    match (args.ignored, skip) {
        (RunIgnored::Skip, Some(reason)) => TestDisposition::Ignored(reason),
        (RunIgnored::Skip, None) | (RunIgnored::Include, _) | (RunIgnored::Only, Some(_)) => {
            TestDisposition::Run
        }
        (RunIgnored::Only, None) => TestDisposition::FilteredOut,
    }
}

/// Runs the registered `ValidatesRunner` suite in `mode`, printing `libtest` output.
pub fn run_validates_runner_suite<R, F>(
    mode: SuiteMode,
    default_threads: Option<usize>,
    make_runner: F,
) -> ExitCode
where
    R: SuiteRunner + 'static,
    F: Fn(&'static str) -> R + Send + Sync + 'static,
{
    let args = match SuiteArgs::parse(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(err) => {
            eprintln!("error: {err}");
            return ExitCode::from(101);
        }
    };

    let mut filtered_out = 0usize;
    let mut ignored_tests = Vec::new();
    let mut runnable = Vec::new();
    for test in VALIDATES_RUNNER_TESTS {
        match classify_test(test, R::KIND, mode, &args) {
            TestDisposition::FilteredOut => filtered_out += 1,
            TestDisposition::Ignored(reason) => ignored_tests.push((test, reason)),
            TestDisposition::Run => runnable.push(test),
        }
    }

    if args.list {
        for (test, _) in &ignored_tests {
            println!("{}::{}: test", test.suite, test.id);
        }
        for test in &runnable {
            println!("{}::{}: test", test.suite, test.id);
        }
        return ExitCode::SUCCESS;
    }

    let concurrency = resolve_concurrency(args.threads, default_threads);
    println!("\nrunning {} tests", ignored_tests.len() + runnable.len());
    for (test, reason) in &ignored_tests {
        println!("test {}::{} ... ignored, {reason}", test.suite, test.id);
    }
    let _ = io::stdout().flush();

    let started = Instant::now();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("error: cannot start the tokio runtime: {err}");
            return ExitCode::from(101);
        }
    };
    let (passed, failures) =
        runtime.block_on(execute_tests(runnable, concurrency, Arc::new(make_runner)));

    if !failures.is_empty() {
        println!("\nfailures:\n");
        for (name, message) in &failures {
            println!("---- {name} ----\n{message}\n");
        }
        println!("failures:");
        for (name, _) in &failures {
            println!("    {name}");
        }
    }

    let status = if failures.is_empty() { "ok" } else { "FAILED" };
    println!(
        "\ntest result: {status}. {passed} passed; {} failed; {} ignored; 0 measured; \
         {filtered_out} filtered out; finished in {:.2}s\n",
        failures.len(),
        ignored_tests.len(),
        started.elapsed().as_secs_f64(),
    );
    if failures.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(101)
    }
}

fn resolve_concurrency(cli_threads: Option<usize>, default_threads: Option<usize>) -> usize {
    cli_threads
        .or_else(|| {
            std::env::var("RUST_TEST_THREADS")
                .ok()
                .and_then(|v| v.parse::<NonZeroUsize>().ok())
                .map(NonZeroUsize::get)
        })
        .or(default_threads)
        .or_else(|| {
            std::thread::available_parallelism()
                .ok()
                .map(NonZeroUsize::get)
        })
        .unwrap_or(4)
}

async fn execute_tests<R, F>(
    tests: Vec<&'static ValidatesRunnerTest>,
    concurrency: usize,
    make_runner: Arc<F>,
) -> (usize, Vec<(String, String)>)
where
    R: SuiteRunner + 'static,
    F: Fn(&'static str) -> R + Send + Sync + 'static,
{
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut join_set = JoinSet::new();
    for test in tests {
        let sem = Arc::clone(&semaphore);
        let factory = Arc::clone(&make_runner);
        join_set.spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore stays open");
            let name = format!("{}::{}", test.suite, test.id);
            // The factory can panic on a missing setting, so it runs in the inner task too.
            let inner = tokio::spawn(async move { run_test(test, &factory(test.id)).await });
            let outcome = match inner.await {
                Ok(()) => Ok(()),
                Err(err) if err.is_panic() => Err(panic_message(err.into_panic())),
                Err(err) => Err(err.to_string()),
            };
            (name, outcome)
        });
    }

    let mut passed = 0usize;
    let mut failures = Vec::new();
    while let Some(joined) = join_set.join_next().await {
        let (name, outcome) = joined.expect("outer task catches inner panics");
        match outcome {
            Ok(()) => {
                passed += 1;
                println!("test {name} ... ok");
            }
            Err(message) => {
                println!("test {name} ... FAILED");
                failures.push((name, message));
            }
        }
        let _ = io::stdout().flush();
    }
    failures.sort_by(|a, b| a.0.cmp(&b.0));
    (passed, failures)
}

fn panic_message(payload: Box<dyn Any + Send + 'static>) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
        .unwrap_or_else(|| "test panicked".to_string())
}
