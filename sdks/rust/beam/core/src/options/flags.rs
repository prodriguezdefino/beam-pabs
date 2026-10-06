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

//! Command-line helpers that use the schema that clap declares.
//!
//! One command line has flags for many option groups from different crates. Each group
//! parses only the flags that *its* clap schema declares, never guessing from the name.

use std::collections::HashSet;

/// Returns the long names, short names and aliases that a clap [`Command`](clap::Command)
/// declares.
fn declared_flags(cmd: &clap::Command) -> HashSet<String> {
    cmd.get_arguments()
        .flat_map(|arg| {
            let long = arg.get_long().map(str::to_string).into_iter();
            let short = arg.get_short().map(|c| c.to_string()).into_iter();
            let aliases = arg
                .get_all_aliases()
                .into_iter()
                .flatten()
                .map(str::to_string);
            long.chain(short).chain(aliases)
        })
        .collect()
}

/// Adds an alias for each long flag with the other word separator, so `--max_retries` and
/// `--max-retries` are the same flag. Beam launchers and runners use `snake_case`, but clap
/// derives `kebab-case`. Applied to each command, so fields need no aliases of their own.
pub(super) fn with_separator_aliases(cmd: clap::Command) -> clap::Command {
    let alternates: Vec<(clap::Id, Vec<String>)> = cmd
        .get_arguments()
        .filter_map(|arg| {
            let long = arg.get_long()?;
            let declared: HashSet<&str> = std::iter::once(long)
                .chain(arg.get_all_aliases().into_iter().flatten())
                .collect();
            let missing: Vec<String> = [long.replace('-', "_"), long.replace('_', "-")]
                .into_iter()
                .filter(|name| !declared.contains(name.as_str()))
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            (!missing.is_empty()).then(|| (arg.get_id().clone(), missing))
        })
        .collect();
    alternates.into_iter().fold(cmd, |cmd, (id, names)| {
        cmd.mut_arg(id, |arg| arg.aliases(names))
    })
}

/// Returns the flag name of a token: `--a=b` and `--a` give `a`.
fn flag_name(token: &str) -> Option<&str> {
    token
        .strip_prefix('-')
        .map(|s| s.trim_start_matches('-'))
        .filter(|s| !s.is_empty())
        .and_then(|s| s.split('=').next())
}

/// Splits arguments (without the program name) into one token group per flag, with the
/// separate value of `--flag value`. A next token that starts with `-` is not a value.
fn flag_tokens(args: &[String]) -> Vec<Vec<String>> {
    let mut iter = args.iter().peekable();
    std::iter::from_fn(|| {
        let arg = iter.next()?;
        let value = (flag_name(arg).is_some()
            && !arg.contains('=')
            && iter.peek().is_some_and(|next| !next.starts_with('-')))
        .then(|| iter.next().cloned())
        .flatten();
        Some(std::iter::once(arg.clone()).chain(value).collect())
    })
    .collect()
}

/// Returns the program name and only those flags of `raw_args` that `cmd` declares.
pub(super) fn filter_args_for_command(cmd: &clap::Command, raw_args: &[String]) -> Vec<String> {
    let declared = declared_flags(cmd);
    let binary_name = raw_args
        .first()
        .cloned()
        .unwrap_or_else(|| "options".to_string());

    std::iter::once(binary_name)
        .chain(
            flag_tokens(raw_args.get(1..).unwrap_or_default())
                .into_iter()
                .filter(|tokens| flag_name(&tokens[0]).is_some_and(|n| declared.contains(n)))
                .flatten(),
        )
        .collect()
}

/// Returns, in command-line order, the flags in `raw_args` (program name first) that no
/// command declares. They belong to no group, so they reach neither workers nor display data.
pub(super) fn undeclared_flags<'a>(
    commands: impl IntoIterator<Item = &'a clap::Command>,
    raw_args: &[String],
) -> Vec<String> {
    let declared: HashSet<String> = commands.into_iter().flat_map(declared_flags).collect();
    flag_tokens(raw_args.get(1..).unwrap_or_default())
        .into_iter()
        .filter_map(|tokens| flag_name(&tokens[0]).map(str::to_string))
        .filter(|name| !declared.contains(name) && name != "help" && name != "version")
        .collect()
}

/// Keeps only the arguments that `T` declares. The command is built first, so clap's
/// generated `--help` and `--version` count as declared.
fn declared_args<T, I, A>(args: I) -> Vec<String>
where
    T: clap::CommandFactory,
    I: IntoIterator<Item = A>,
    A: Into<std::ffi::OsString>,
{
    let raw = os_args(args);
    let mut cmd = with_separator_aliases(T::command());
    cmd.build();
    filter_args_for_command(&cmd, &raw)
}

pub(super) fn os_args<I, A>(args: I) -> Vec<String>
where
    I: IntoIterator<Item = A>,
    A: Into<std::ffi::OsString>,
{
    args.into_iter()
        .map(|arg| arg.into().to_string_lossy().into_owned())
        .collect()
}

/// Parses the arguments of a standalone clap program, such as the container boot program.
///
/// Ignores flags that `T` does not declare. Pipelines must use
/// [`parse`](crate::options::parse), which also sends their options to the workers.
/// On a parse error, clap prints the error and exits the process.
pub fn parse_args<T: clap::Parser>() -> T {
    parse_args_from(std::env::args_os())
}

/// Identical to [`parse_args`], but parses `args`, whose first item is the program name.
pub fn parse_args_from<T, I, A>(args: I) -> T
where
    T: clap::Parser,
    I: IntoIterator<Item = A>,
    A: Into<std::ffi::OsString>,
{
    try_parse_args_from(args).unwrap_or_else(|e| e.exit())
}

/// Identical to [`parse_args_from`], but returns the error and does not exit.
pub fn try_parse_args_from<T, I, A>(args: I) -> Result<T, clap::Error>
where
    T: clap::Parser,
    I: IntoIterator<Item = A>,
    A: Into<std::ffi::OsString>,
{
    let matches =
        with_separator_aliases(T::command())
            .try_get_matches_from(declared_args::<T, _, _>(args))?;
    T::from_arg_matches(&matches)
}
