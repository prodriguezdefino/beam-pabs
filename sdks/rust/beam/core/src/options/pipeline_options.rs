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

//! The core pipeline options type and the resolution of typed option groups.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, PoisonError, RwLock};

use clap::{Args, CommandFactory, FromArgMatches, Parser};
use serde::{Deserialize, Serialize};

use crate::options::convert::json_to_prost_struct;
use crate::options::flags::{
    filter_args_for_command, os_args, undeclared_flags, with_separator_aliases,
};
use crate::options::groups::{
    HarnessOptions, OptionGroupRegistration, OptionsError, PipelineOptionGroup,
    ResourceHintsOptions,
};
use crate::options::snapshot::{GroupSnapshot, OptionsSnapshot, SDK_OPTIONS_OPTION};
use crate::pipeline::constants::OPTION_NAMESPACE_CORE;
use crate::transforms::display_data::DisplayDataItem;

/// Core options that control how any runner executes a pipeline.
///
/// Read all other options as typed [`PipelineOptionGroup`]s with [`view_as`](Self::view_as).
/// A driver parses them from its command line; a worker deserializes the driver's
/// [`OptionsSnapshot`].
#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
pub struct PipelineOptions {
    /// Runner that executes the pipeline, for example `prism` or `dataflow`. A runner is
    /// available only if the binary links its crate.
    #[arg(long, default_value = "prism")]
    pub runner: String,

    /// Execute the pipeline in streaming mode.
    #[arg(
        long,
        num_args = 0..=1,
        default_missing_value = "true",
        default_value_t = false
    )]
    pub streaming: bool,

    /// Endpoint of a running job service, for example `http://localhost:8073`. If omitted, a
    /// runner that manages its own service starts one.
    #[arg(long)]
    pub endpoint: Option<String>,

    /// Name of the job. The default value depends on the runner.
    #[arg(long, alias = "jobName")]
    pub job_name: Option<String>,

    /// Fn API worker flags that the runner's container boot program sets. They describe this
    /// process, not the job, so they are never serialized.
    #[command(flatten)]
    #[serde(skip)]
    pub harness: HarnessOptions,

    #[arg(skip)]
    #[serde(skip)]
    source: OptionSource,

    /// Resolved option groups, shared by all clones of these options.
    #[arg(skip)]
    #[serde(skip)]
    groups: GroupCache,
}

impl PartialEq for PipelineOptions {
    fn eq(&self, other: &Self) -> bool {
        self.runner == other.runner
            && self.streaming == other.streaming
            && self.endpoint == other.endpoint
            && self.job_name == other.job_name
            && self.harness == other.harness
            && self.source == other.source
    }
}

impl Eq for PipelineOptions {}

impl Default for PipelineOptions {
    fn default() -> Self {
        Self {
            runner: "prism".to_string(),
            streaming: false,
            endpoint: None,
            job_name: None,
            harness: HarnessOptions::default(),
            source: OptionSource::default(),
            groups: GroupCache::default(),
        }
    }
}

impl PipelineOptions {
    /// Returns options for `runner` with all other settings at their defaults.
    pub fn with_runner(runner: impl Into<String>) -> Self {
        Self {
            runner: runner.into(),
            ..Self::default()
        }
    }

    /// Parses options from an argument list whose first item is the program name. With
    /// `--options_file` (passed to a worker by the container boot program), the options come
    /// from the driver's snapshot. On invalid arguments, prints the error and exits.
    pub fn parse_from<I, A>(args: I) -> Self
    where
        I: IntoIterator<Item = A>,
        A: Into<std::ffi::OsString>,
    {
        Self::try_parse_from(args).unwrap_or_else(|e| e.exit())
    }

    /// Identical to [`parse_from`](Self::parse_from), but returns the error and does not exit.
    pub fn try_parse_from<I, A>(args: I) -> Result<Self, ParseError>
    where
        I: IntoIterator<Item = A>,
        A: Into<std::ffi::OsString>,
    {
        let raw = os_args(args);
        let harness = parse_group::<HarnessOptions>(&raw)?;
        match harness.options_file.clone() {
            Some(path) => Ok(Self::from_snapshot(OptionsSnapshot::load(&path)?, harness)?),
            None => {
                let cmd = core_command();
                let filtered = filter_args_for_command(&cmd, &raw);
                let core = Self::from_arg_matches(&cmd.try_get_matches_from(filtered)?)?;
                Ok(core.with_command_line(raw))
            }
        }
    }

    /// Parses the process command line. On invalid arguments, prints the error and exits.
    pub fn from_args() -> Self {
        Self::parse_from(std::env::args_os())
    }

    /// Restores options from a driver's snapshot, for the process that `harness` describes.
    /// Option groups are not validated again, because the driver validated them.
    pub fn from_snapshot(
        snapshot: OptionsSnapshot,
        harness: HarnessOptions,
    ) -> Result<Self, OptionsError> {
        let core = snapshot
            .group(group_key::<Self>())
            .map(GroupSnapshot::restore::<Self>)
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            harness,
            source: OptionSource::Snapshot(Arc::new(snapshot)),
            groups: GroupCache::default(),
            ..core
        })
    }

    /// Uses the command line `raw` (program name first) as the source of option groups.
    fn with_command_line(self, raw: Vec<String>) -> Self {
        Self {
            source: OptionSource::CommandLine(raw.into()),
            groups: GroupCache::default(),
            ..self
        }
    }

    /// Reads option group `G`.
    ///
    /// The first read resolves and caches `G`, so `G` is part of the job
    /// [`snapshot`](Self::snapshot) and all later reads agree. A driver parses and validates
    /// `G`; a worker deserializes the driver's value, or gets the defaults if the driver
    /// never read `G`.
    ///
    /// ```
    /// use clap::Args;
    /// use serde::{Deserialize, Serialize};
    /// use beam::options::{PipelineOptionGroup, PipelineOptions};
    ///
    /// #[derive(Args, Clone, Debug, Serialize, Deserialize)]
    /// struct CustomOptions {
    ///     #[arg(long)]
    ///     tag: Option<String>,
    /// }
    ///
    /// impl PipelineOptionGroup for CustomOptions {}
    ///
    /// let opts = PipelineOptions::parse_from(["app", "--tag=prod"]);
    /// let custom: CustomOptions = opts.view_as().expect("parsed");
    /// assert_eq!(custom.tag.as_deref(), Some("prod"));
    /// ```
    pub fn view_as<G: PipelineOptionGroup>(&self) -> Result<G, OptionsError> {
        self.groups.get::<G>().map_or_else(
            || {
                let group = self.source.resolve::<G>()?;
                self.groups.insert(group.clone());
                Ok(group)
            },
            Ok,
        )
    }

    /// Validates and sets option group `G` in code. This value overrides the command line.
    pub fn set<G: PipelineOptionGroup>(&self, group: G) -> Result<(), OptionsError> {
        group.validate()?;
        self.groups.insert(group);
        Ok(())
    }

    /// Returns `true` if option group `G` is resolved or set.
    pub fn contains<G: 'static>(&self) -> bool {
        self.groups.contains::<G>()
    }

    /// Returns the resource hints of the pipeline from [`ResourceHintsOptions`].
    pub fn resource_hints(
        &self,
    ) -> Result<crate::pipeline::resources::ResourceHints, OptionsError> {
        let opts = self.view_as::<ResourceHintsOptions>()?;
        crate::pipeline::resources::ResourceHints::from_options(&opts).map_err(|e| {
            OptionsError::Validation {
                group: "ResourceHintsOptions",
                message: e.to_string(),
            }
        })
    }

    /// Returns the typed options of this job, which workers receive: the core options and each
    /// group that was read or registered with [`OptionGroupRegistration`]. Command-line flags
    /// that no group declares are dropped with a warning, which also shows typos.
    pub fn snapshot(&self) -> Result<OptionsSnapshot, OptionsError> {
        inventory::iter::<OptionGroupRegistration>
            .into_iter()
            .try_for_each(|registration| (registration.resolve)(self))?;

        if let OptionSource::CommandLine(raw) = &self.source {
            let commands: Vec<clap::Command> = std::iter::once(core_command())
                .chain(self.groups.commands())
                .collect();
            let ignored = undeclared_flags(&commands, raw);
            if !ignored.is_empty() {
                tracing::warn!(
                    "Ignoring flags that no option group declares: {}",
                    ignored.join(", ")
                );
            }
        }

        let core = (
            group_key::<Self>().to_string(),
            GroupSnapshot::of(OPTION_NAMESPACE_CORE, self)?,
        );
        Ok(OptionsSnapshot::from_groups(
            std::iter::once(Ok(core))
                .chain(self.groups.snapshots())
                .collect::<Result<Vec<_>, _>>()?,
        ))
    }

    /// Returns display data for each option of the job [`snapshot`](Self::snapshot).
    pub fn display_data(&self) -> Result<Vec<DisplayDataItem>, OptionsError> {
        self.snapshot().map(|snapshot| snapshot.display_data())
    }

    /// Converts the job [`snapshot`](Self::snapshot) to the protobuf `Struct` that a portable
    /// JobService sends to each SDK harness. Each key uses the `beam:option:<name>:v1` URN
    /// format so cross-language harnesses can read options directly, while the Rust harness
    /// reads the full typed snapshot from [`SDK_OPTIONS_OPTION`].
    pub fn to_proto_struct(&self) -> Result<prost_types::Struct, OptionsError> {
        let snapshot = self.snapshot()?;
        let fields: serde_json::Map<String, serde_json::Value> = snapshot
            .flat_options()?
            .into_iter()
            .map(|(key, value)| (option_urn(&key), value))
            .chain(std::iter::once((
                option_urn(SDK_OPTIONS_OPTION),
                serde_json::Value::String(snapshot.encode()),
            )))
            .collect();
        Ok(json_to_prost_struct(&serde_json::Value::Object(fields)))
    }
}

/// Parses the command line of a pipeline into [`PipelineOptions`] and its option group `T`.
/// On invalid arguments, prints a usage message and exits the process.
///
/// ```no_run
/// use clap::Args;
/// use serde::{Deserialize, Serialize};
/// use beam::options::PipelineOptionGroup;
///
/// #[derive(Args, Clone, Debug, Serialize, Deserialize)]
/// struct WordCountArgs {
///     #[arg(long)]
///     output: String,
/// }
/// impl PipelineOptionGroup for WordCountArgs {}
///
/// let (options, args) = beam::options::parse::<WordCountArgs>();
/// let pipeline = beam::pipeline::Pipeline::create(&options);
/// ```
///
/// A driver parses `T` and the core options together, so `--help` describes both. Flags for
/// other groups, runners or launchers stay for [`PipelineOptions::view_as`]. A worker
/// deserializes the exact `T` that its driver resolved, so it builds the same graph.
pub fn parse<T: PipelineOptionGroup>() -> (PipelineOptions, T) {
    parse_from(std::env::args_os())
}

/// Identical to [`parse`], but parses `args`, whose first item is the program name.
pub fn parse_from<T, I, A>(args: I) -> (PipelineOptions, T)
where
    T: PipelineOptionGroup,
    I: IntoIterator<Item = A>,
    A: Into<std::ffi::OsString>,
{
    try_parse_from(args).unwrap_or_else(|e| e.exit())
}

/// Identical to [`parse_from`], but returns the error and does not exit. A worker without a
/// usable `--options_file` is an error.
pub fn try_parse_from<T, I, A>(args: I) -> Result<(PipelineOptions, T), ParseError>
where
    T: PipelineOptionGroup,
    I: IntoIterator<Item = A>,
    A: Into<std::ffi::OsString>,
{
    let raw = os_args(args);
    let harness = parse_group::<HarnessOptions>(&raw)?;

    if harness.is_worker() {
        let path = harness
            .options_file
            .clone()
            .ok_or_else(|| OptionsError::Snapshot {
                message: "started as a worker (--worker) without --options_file: the container \
                      boot program passes the job's options snapshot with it"
                    .to_string(),
            })?;
        let options = PipelineOptions::from_snapshot(OptionsSnapshot::load(&path)?, harness)?;
        let args = options.view_as::<T>()?;
        return Ok((options, args));
    }

    let program = raw.first().map_or("pipeline", String::as_str);
    let cmd = with_separator_aliases(T::augment_args(
        PipelineOptions::command()
            .bin_name(program.to_string())
            .next_help_heading("Pipeline options"),
    ));
    let mut declared = cmd.clone();
    declared.build();
    let matches = cmd.try_get_matches_from(filter_args_for_command(&declared, &raw))?;

    let args = T::from_arg_matches(&matches)?;
    let options = PipelineOptions::from_arg_matches(&matches)?.with_command_line(raw);
    options.set(args.clone())?;
    Ok((options, args))
}

/// The reason that a command line did not parse into options.
#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    /// The arguments do not match the declared flags, or the user gave `--help` or `--version`.
    #[error(transparent)]
    Cli(#[from] clap::Error),
    /// The options are not valid, or the snapshot of a worker is not usable.
    #[error(transparent)]
    Options(#[from] OptionsError),
}

impl ParseError {
    /// Prints the error, or the requested help, and exits the process.
    pub fn exit(self) -> ! {
        match self {
            Self::Cli(e) => e.exit(),
            Self::Options(e) => {
                eprintln!("error: {e}");
                std::process::exit(2)
            }
        }
    }
}

/// The source of option groups. Each variant is one process role.
#[derive(Clone, Debug, PartialEq)]
enum OptionSource {
    /// A driver's command line, program name first.
    CommandLine(Arc<[String]>),
    /// A driver's typed snapshot, which a worker receives.
    Snapshot(Arc<OptionsSnapshot>),
}

impl Default for OptionSource {
    fn default() -> Self {
        Self::CommandLine(Arc::from([]))
    }
}

impl OptionSource {
    fn resolve<G: PipelineOptionGroup>(&self) -> Result<G, OptionsError> {
        match self {
            Self::CommandLine(raw) => {
                let group = parse_group::<G>(raw)?;
                group.validate()?;
                Ok(group)
            }
            Self::Snapshot(snapshot) => snapshot
                .group(group_key::<G>())
                .map_or_else(|| parse_group::<G>(&[]), GroupSnapshot::restore),
        }
    }
}

/// The snapshot key of a group: its full type name, not the overridable
/// [`PipelineOptionGroup::group_name`]. A worker runs the driver's binary, so the name is
/// stable and unique.
fn group_key<G: 'static>() -> &'static str {
    std::any::type_name::<G>()
}

fn core_command() -> clap::Command {
    with_separator_aliases(PipelineOptions::command())
}

fn group_command<G: Args>() -> clap::Command {
    with_separator_aliases(G::augment_args(clap::Command::new("options")))
}

/// Parses group `G` from the flags in `raw` that `G` declares.
fn parse_group<G: Args>(raw: &[String]) -> Result<G, OptionsError> {
    let parse_error = |e: clap::Error| OptionsError::ParseError {
        group: std::any::type_name::<G>(),
        message: e.to_string(),
    };
    let cmd = group_command::<G>();
    let matches = cmd
        .clone()
        .try_get_matches_from(filter_args_for_command(&cmd, raw))
        .map_err(parse_error)?;
    G::from_arg_matches(&matches).map_err(parse_error)
}

/// Returns the snapshot key and contents of a type-erased group value.
type SnapshotFn = fn(&dyn Any) -> Result<(String, GroupSnapshot), OptionsError>;

/// A resolved option group and the functions that use it without its static type.
struct CachedGroup {
    value: Box<dyn Any + Send + Sync>,
    snapshot: SnapshotFn,
    command: fn() -> clap::Command,
}

impl CachedGroup {
    fn of<G: PipelineOptionGroup>(group: G) -> Self {
        Self {
            value: Box::new(group),
            snapshot: snapshot_group::<G>,
            command: group_command::<G>,
        }
    }
}

fn snapshot_group<G: PipelineOptionGroup>(
    value: &dyn Any,
) -> Result<(String, GroupSnapshot), OptionsError> {
    let group = value
        .downcast_ref::<G>()
        .expect("a cached option group is stored under its own TypeId");
    Ok((
        group_key::<G>().to_string(),
        GroupSnapshot::of(G::NAMESPACE, group)?,
    ))
}

/// Resolved option groups, keyed by type. All clones of the options share the cache.
#[derive(Clone, Default)]
struct GroupCache(Arc<RwLock<HashMap<TypeId, CachedGroup>>>);

impl GroupCache {
    fn get<G: Clone + 'static>(&self) -> Option<G> {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&TypeId::of::<G>())
            .and_then(|cached| cached.value.downcast_ref::<G>().cloned())
    }

    fn insert<G: PipelineOptionGroup>(&self, group: G) {
        self.0
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(TypeId::of::<G>(), CachedGroup::of(group));
    }

    fn contains<G: 'static>(&self) -> bool {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(&TypeId::of::<G>())
    }

    fn snapshots(&self) -> Vec<Result<(String, GroupSnapshot), OptionsError>> {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(|cached| (cached.snapshot)(cached.value.as_ref()))
            .collect()
    }

    fn commands(&self) -> Vec<clap::Command> {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(|cached| (cached.command)())
            .collect()
    }
}

impl fmt::Debug for GroupCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let groups = self.0.read().unwrap_or_else(PoisonError::into_inner).len();
        f.debug_struct("GroupCache")
            .field("groups", &groups)
            .finish()
    }
}

/// Returns the URN that a portable runner uses as key for a pipeline option.
fn option_urn(key: &str) -> String {
    format!("beam:option:{key}:v1")
}
