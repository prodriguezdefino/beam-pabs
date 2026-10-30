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

//! Query reads: a dry-run job gives the schema, a query job writes the result to a temporary
//! table, and the read deletes that table when all streams are read.

use std::io;

use beam::schema::{BeamRow, Schema};
use google_cloud_bigquery_v2::client::{DatasetService, JobService, TableService};
use google_cloud_bigquery_v2::model::{
    Dataset, DatasetReference, EncryptionConfiguration, Job, JobConfiguration,
    JobConfigurationQuery, JobReference, JobStatistics2, TableReference,
};
use tokio::sync::OnceCell;

use super::ReadOptions;
use crate::bigquery::storage::schema::table_schema_to_beam_schema;
use crate::runtime::block_on_async;

/// The default table expiration of a temporary dataset: one day.
const TEMP_TABLE_EXPIRATION_MS: i64 = 24 * 60 * 60 * 1000;

/// The scheduling priority of a query job.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum QueryPriority {
    #[default]
    Batch,
    Interactive,
}

impl QueryPriority {
    /// The name in configurations: `BATCH` or `INTERACTIVE`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Batch => "BATCH",
            Self::Interactive => "INTERACTIVE",
        }
    }
}

impl std::str::FromStr for QueryPriority {
    type Err = String;

    /// Parses a name of [`as_str`](Self::as_str), in any case.
    fn from_str(name: &str) -> Result<Self, String> {
        match name.to_ascii_uppercase().as_str() {
            "BATCH" => Ok(Self::Batch),
            "INTERACTIVE" => Ok(Self::Interactive),
            _ => Err(format!(
                "Unknown query priority '{name}': expected BATCH or INTERACTIVE"
            )),
        }
    }
}

/// The BigQuery clients of a query read.
#[derive(Clone, Debug)]
pub(super) struct QueryClients {
    pub(super) jobs: JobService,
    pub(super) datasets: DatasetService,
    pub(super) tables: TableService,
}

static QUERY_CLIENTS: OnceCell<QueryClients> = OnceCell::const_new();

impl QueryClients {
    /// Returns `given`, or the process-wide clients with Application Default Credentials.
    /// Call it on the client runtime only.
    async fn or_default(given: Option<Self>) -> io::Result<Self> {
        if let Some(clients) = given {
            return Ok(clients);
        }
        QUERY_CLIENTS
            .get_or_try_init(|| async {
                Ok::<_, io::Error>(Self {
                    jobs: JobService::builder()
                        .build()
                        .await
                        .map_err(io::Error::other)?,
                    datasets: DatasetService::builder()
                        .build()
                        .await
                        .map_err(io::Error::other)?,
                    tables: TableService::builder()
                        .build()
                        .await
                        .map_err(io::Error::other)?,
                })
            })
            .await
            .cloned()
    }
}

/// The temporary table that holds the result of a query read. `created_dataset` is true when
/// the read also created the dataset, so the cleanup deletes the dataset too.
#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
pub(super) struct TempTable {
    pub(super) project: String,
    pub(super) dataset: String,
    pub(super) table: String,
    pub(super) created_dataset: bool,
}

impl TempTable {
    pub(super) fn table_spec(&self) -> String {
        format!("{}.{}.{}", self.project, self.dataset, self.table)
    }
}

/// Returns the Beam schema of the query result, from a dry-run job.
pub(super) fn query_schema(
    options: &ReadOptions,
    query: &str,
    clients: Option<QueryClients>,
) -> beam::Result<Schema> {
    billing_project(options).map_err(beam::Error::from)?;
    let (options, query) = (options.clone(), query.to_string());
    let stats = block_on_async(async move {
        let clients = QueryClients::or_default(clients).await?;
        dry_run(&options, &query, &clients).await
    })
    .map_err(|e| beam::Error::from(e).context("The BigQuery dry-run job failed"))?;
    table_schema_to_beam_schema(&stats.schema.unwrap_or_default().fields)
}

/// Runs the query into a new temporary table. Without a temporary dataset in `options`, it
/// creates one in the location of the query.
pub(super) fn run_query(
    options: &ReadOptions,
    query: &str,
    clients: Option<QueryClients>,
) -> io::Result<TempTable> {
    let (options, query) = (options.clone(), query.to_string());
    block_on_async(async move {
        let clients = QueryClients::or_default(clients).await?;
        let project = billing_project(&options)?.to_string();
        let location = match &options.query_location {
            Some(location) => Some(location.clone()),
            None => source_location(&options, &query, &clients).await?,
        };
        // The job id must be new for each attempt: BigQuery skips a job with a known id.
        let id = uuid::Uuid::new_v4().simple().to_string();
        let table = format!("temp_table_beam_bq_job_QUERY_{id}");
        let temp = match &options.temp_dataset {
            Some(spec) => {
                let (project, dataset) = parse_dataset(spec, &project)?;
                TempTable {
                    project,
                    dataset,
                    table,
                    created_dataset: false,
                }
            }
            None => {
                let temp = TempTable {
                    project: project.clone(),
                    dataset: format!("temp_dataset_beam_bq_job_QUERY_{id}"),
                    table,
                    created_dataset: true,
                };
                create_dataset(&clients, &temp, location.as_deref()).await?;
                temp
            }
        };
        let job = query_job(&options, &query, &temp, &project, &id, location);
        let result = clients
            .jobs
            .insert_job()
            .set_project_id(&project)
            .set_job(job)
            .into_job_poller()
            .until_done()
            .await;
        if let Err(e) = result {
            if let Err(cleanup) = delete(&clients, &temp).await {
                tracing::warn!("Cannot delete the temporary table of a failed query: {cleanup}");
            }
            return Err(io::Error::other(e));
        }
        Ok(temp)
    })
}

/// Deletes the temporary table, or its dataset when the read created the dataset.
pub(super) fn delete_temp_table(temp: &TempTable, clients: Option<QueryClients>) -> io::Result<()> {
    let temp = temp.clone();
    block_on_async(async move {
        let clients = QueryClients::or_default(clients).await?;
        delete(&clients, &temp).await
    })
}

/// The project of the query jobs. Query reads have no table project to fall back on.
fn billing_project(options: &ReadOptions) -> io::Result<&str> {
    options
        .billing_project
        .as_deref()
        .filter(|project| !project.is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "A BigQuery query read needs a project: set billing_project or --project",
            )
        })
}

fn query_config(options: &ReadOptions, query: &str) -> JobConfigurationQuery {
    JobConfigurationQuery::new()
        .set_query(query)
        .set_use_legacy_sql(options.use_legacy_sql)
        .set_flatten_results(options.flatten_results)
}

fn job_reference(project: &str, location: Option<String>) -> JobReference {
    JobReference::new()
        .set_project_id(project)
        .set_or_clear_location(location)
}

async fn dry_run(
    options: &ReadOptions,
    query: &str,
    clients: &QueryClients,
) -> io::Result<JobStatistics2> {
    let project = billing_project(options)?;
    let job = Job::new()
        .set_job_reference(job_reference(project, options.query_location.clone()))
        .set_configuration(
            JobConfiguration::new()
                .set_dry_run(true)
                .set_query(query_config(options, query)),
        );
    let job = clients
        .jobs
        .insert_job()
        .set_project_id(project)
        .set_job(job)
        .send()
        .await
        .map_err(io::Error::other)?;
    Ok(job.statistics.and_then(|s| s.query).unwrap_or_default())
}

/// The location of the dataset of the first table that the query reads.
async fn source_location(
    options: &ReadOptions,
    query: &str,
    clients: &QueryClients,
) -> io::Result<Option<String>> {
    let stats = dry_run(options, query, clients).await?;
    let Some(table) = stats.referenced_tables.first() else {
        return Ok(None);
    };
    let dataset = clients
        .datasets
        .get_dataset()
        .set_project_id(&table.project_id)
        .set_dataset_id(&table.dataset_id)
        .send()
        .await
        .map_err(io::Error::other)?;
    Ok(Some(dataset.location).filter(|location| !location.is_empty()))
}

async fn create_dataset(
    clients: &QueryClients,
    temp: &TempTable,
    location: Option<&str>,
) -> io::Result<()> {
    let dataset = Dataset::new()
        .set_dataset_reference(
            DatasetReference::new()
                .set_project_id(&temp.project)
                .set_dataset_id(&temp.dataset),
        )
        .set_location(location.unwrap_or_default())
        .set_default_table_expiration_ms(TEMP_TABLE_EXPIRATION_MS);
    clients
        .datasets
        .insert_dataset()
        .set_project_id(&temp.project)
        .set_dataset(dataset)
        .send()
        .await
        .map(drop)
        .map_err(io::Error::other)
}

fn query_job(
    options: &ReadOptions,
    query: &str,
    temp: &TempTable,
    project: &str,
    id: &str,
    location: Option<String>,
) -> Job {
    let mut config = query_config(options, query)
        .set_priority(options.query_priority.as_str())
        .set_allow_large_results(true)
        .set_create_disposition("CREATE_IF_NEEDED")
        .set_write_disposition("WRITE_TRUNCATE")
        .set_destination_table(
            TableReference::new()
                .set_project_id(&temp.project)
                .set_dataset_id(&temp.dataset)
                .set_table_id(&temp.table),
        );
    if let Some(key) = &options.kms_key {
        config = config.set_destination_encryption_configuration(
            EncryptionConfiguration::new().set_kms_key_name(key),
        );
    }
    Job::new()
        .set_job_reference(
            job_reference(project, location).set_job_id(format!("beam_bq_job_QUERY_{id}")),
        )
        .set_configuration(JobConfiguration::new().set_query(config))
}

/// Deletes `temp`. A table or dataset that is already gone is not an error.
async fn delete(clients: &QueryClients, temp: &TempTable) -> io::Result<()> {
    let result = if temp.created_dataset {
        clients
            .datasets
            .delete_dataset()
            .set_project_id(&temp.project)
            .set_dataset_id(&temp.dataset)
            .set_delete_contents(true)
            .send()
            .await
    } else {
        clients
            .tables
            .delete_table()
            .set_project_id(&temp.project)
            .set_dataset_id(&temp.dataset)
            .set_table_id(&temp.table)
            .send()
            .await
    };
    match result {
        Err(e) if e.http_status_code() != Some(404) => Err(io::Error::other(e)),
        _ => Ok(()),
    }
}

/// Parses `[project:]dataset` or `project.dataset`. A dataset without a project is in
/// `default_project`.
fn parse_dataset(spec: &str, default_project: &str) -> io::Result<(String, String)> {
    let (project, dataset) = spec
        .rsplit_once([':', '.'])
        .unwrap_or((default_project, spec));
    if project.is_empty() || dataset.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Invalid temporary dataset '{spec}': expected [project:]dataset"),
        ));
    }
    Ok((project.to_string(), dataset.to_string()))
}
