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

//! Streaming `SampleRowKeys` coalescer and [`SampleKeyRangesFn`] `DoFn`.
//!
//! For tables with millions of tablets, materializing one initial restriction per tablet in
//! the pipeline graph or a single `Reshuffle` step would overwhelm runner control planes.
//! [`SampleCoalescer`] folds `SampleRowKeysResponse` messages in bounded memory into at most
//! `max_initial_bundles` **super-ranges**, each retaining up to `MAX_INTERIOR_TABLETS`
//! interior [`TabletBoundary`](super::restriction::TabletBoundary) split points for parallel
//! SDF expansion (`split_restriction`) and tablet-aware Liquid Sharding
//! (`KeyRangeTracker::try_split`).

mod coalesce;

use std::sync::Arc;
use std::time::Duration;

use beam::retry::{Decision, RetryWaits};
use beam::transforms::{DoFn, ProcessContext};

pub use self::coalesce::{SampleCoalescer, shuffle_restrictions};
use super::restriction::KeyRangeRestriction;
use crate::bigtable::METRICS;
use crate::bigtable::client::{BigtableClient, RoutingCookies};
use crate::bigtable::proto::{RowRange, RowRangeExt as _, SampleRowKeysRequest};
use crate::bigtable::retry::{DEFAULT_RETRY, decision, wait_before_retry};
use crate::bigtable::target::ReadTarget;
use crate::runtime::block_on_async;

/// Default maximum number of super-range bundles emitted by [`SampleKeyRangesFn`].
pub const DEFAULT_MAX_INITIAL_BUNDLES: usize = 16_384;

/// `DoFn` that calls `Bigtable.SampleRowKeys` after an `Impulse` and emits coalesced,
/// deterministically shuffled [`KeyRangeRestriction`] super-ranges.
#[derive(Clone)]
pub struct SampleKeyRangesFn {
    pub target: ReadTarget,
    pub app_profile_id: Option<String>,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub max_initial_bundles: usize,
    pub sub_split_tablets: bool,
    pub attempt_timeout: Duration,
    pub endpoint: Option<String>,
    pub client: Option<Arc<BigtableClient>>,
}

impl DoFn for SampleKeyRangesFn {
    type In = Vec<u8>;
    type Out = KeyRangeRestriction;

    fn process_element(
        &mut self,
        _impulse: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result {
        let SampleKeyRangesFn {
            target,
            app_profile_id,
            start_key,
            end_key,
            max_initial_bundles,
            sub_split_tablets,
            attempt_timeout,
            endpoint,
            client,
        } = &*self;
        let app_profile = app_profile_id.as_deref();
        let retry_policy = DEFAULT_RETRY;
        let waits = RetryWaits::default();

        let sampled = block_on_async(async {
            let client = match client {
                Some(c) => Arc::clone(c),
                None => Arc::new(BigtableClient::connect(endpoint.as_deref()).await?),
            };
            let routing_header = target.routing_header(app_profile);
            let mut cookies = RoutingCookies::default();
            let mut attempt = 0u32;
            loop {
                let mut request = SampleRowKeysRequest::default();
                target.apply_to_sample(&mut request, app_profile);
                if !start_key.is_empty() || !end_key.is_empty() {
                    request.row_range = Some(RowRange::half_open(
                        bytes::Bytes::copy_from_slice(start_key),
                        bytes::Bytes::copy_from_slice(end_key),
                    ));
                }
                let coalescer = SampleCoalescer::new(
                    start_key.clone(),
                    end_key.clone(),
                    *max_initial_bundles,
                    *sub_split_tablets,
                );
                match collect_samples(
                    &client,
                    request,
                    &routing_header,
                    *attempt_timeout,
                    &mut cookies,
                    coalescer,
                )
                .await
                {
                    Ok(ranges) => return Ok((client, ranges)),
                    Err(status)
                        if retry_policy.allows_retry(attempt)
                            && decision(&status) != Decision::Stop =>
                    {
                        let resource = target.resource_name();
                        let cause = status.to_string();
                        wait_before_retry(
                            retry_policy,
                            attempt,
                            decision(&status),
                            &waits,
                            "SampleRowKeys",
                            &resource,
                            &cause,
                        )
                        .await;
                        attempt += 1;
                    }
                    Err(status) => {
                        return Err(std::io::Error::other(format!(
                            "Bigtable SampleRowKeys failed for {}: {}",
                            target.resource_name(),
                            status
                        )));
                    }
                }
            }
        });
        waits.record(METRICS);
        let (client, restrictions) = sampled?;
        self.client = Some(client);

        restrictions
            .into_iter()
            .try_for_each(|restriction| ctx.emit(restriction))
    }
}

async fn collect_samples(
    client: &BigtableClient,
    request: SampleRowKeysRequest,
    routing_header: &str,
    attempt_timeout: Duration,
    cookies: &mut RoutingCookies,
    mut coalescer: SampleCoalescer,
) -> Result<Vec<KeyRangeRestriction>, tonic::Status> {
    let mut stream = tokio::time::timeout(
        attempt_timeout,
        client.sample_row_keys(request, routing_header, cookies),
    )
    .await
    .map_err(|_| tonic::Status::deadline_exceeded("SampleRowKeys connect timed out"))??;

    while let Some(sample) = tokio::time::timeout(attempt_timeout, stream.message())
        .await
        .map_err(|_| tonic::Status::deadline_exceeded("SampleRowKeys stream timed out"))?
        .inspect_err(|status| cookies.extract(status.metadata()))?
    {
        coalescer.feed(&sample);
    }

    Ok(coalescer.finish())
}
