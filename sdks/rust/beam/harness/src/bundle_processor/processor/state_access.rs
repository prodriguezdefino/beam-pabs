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

//! A bundle's access to runner state.

use std::sync::Arc;

use tracing::debug;

use super::BundleRequest;
use crate::bundle_processor::cache::BundlePlan;

/// A bundle's access to runner state: side inputs, user state and state-backed iterables.
pub(super) struct StateAccess {
    /// The bundle's channel. Its clones below add to its GET totals.
    pub(super) channel: Option<crate::state::StateChannel>,
    pub(super) stream_reader: Option<Arc<dyn beam::coders::StateStreamReader>>,
    pub(super) side_inputs: Option<Arc<dyn beam::internals::SideInputReader>>,
    pub(super) user_state: Option<crate::user_state::BundleUserState>,
    /// User state scoped once per operator (not per call), in graph operator order.
    pub(super) scoped: Vec<Arc<dyn beam::internals::UserStateBackend>>,
}
impl StateAccess {
    pub(super) fn new(
        request: &BundleRequest<'_>,
        plan: &BundlePlan,
        clients: &crate::state::StateClients,
        cache: &Arc<crate::state::StateCache>,
    ) -> Self {
        let &BundleRequest {
            instruction_id,
            descriptor,
            cache_tokens,
            ..
        } = request;
        let channel = clients.channel(instruction_id, descriptor);
        let stream_reader = channel
            .as_ref()
            .map(|ch| Arc::new(ch.clone()) as Arc<dyn beam::coders::StateStreamReader>);
        let side_inputs = plan.side_inputs().map(|specs| {
            let reader =
                crate::state::FnApiSideInputReader::new(Arc::clone(specs), channel.clone())
                    .with_shared_cache(Arc::clone(cache), &cache_tokens.side_input_tokens);
            Arc::new(reader) as Arc<dyn beam::internals::SideInputReader>
        });
        let user_state_cache =
            cache_tokens
                .user_state_token
                .clone()
                .map(|token| crate::user_state::UserStateCache {
                    cache: Arc::clone(cache),
                    token,
                });
        let user_state = plan
            .user_state()
            .zip(channel.clone())
            .map(|(specs, channel)| {
                crate::user_state::BundleUserState::new(
                    Arc::clone(specs),
                    channel,
                    user_state_cache,
                )
            });
        let scoped = user_state
            .as_ref()
            .map(|state| {
                plan.graph()
                    .operator_ids
                    .iter()
                    .map(|t_id| state.scoped(t_id))
                    .collect()
            })
            .unwrap_or_default();
        Self {
            channel,
            stream_reader,
            side_inputs,
            user_state,
            scoped,
        }
    }

    /// Logs the State API GET totals of the bundle at debug level.
    pub(super) fn log_get_totals(&self, instruction_id: &str) {
        let Some(channel) = &self.channel else {
            return;
        };
        let crate::state::StateGetTotals { pages, bytes, wait } = channel.state_get_totals();
        debug!(
            "State GET totals of instruction '{instruction_id}': {pages} pages, {bytes} bytes, \
             {} ms waiting",
            wait.as_millis()
        );
    }
}
