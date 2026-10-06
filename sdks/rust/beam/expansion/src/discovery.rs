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

//! Finds the providers that the binary links.

use crate::{SchemaTransformProvider, SchemaTransformRegistration};

fn providers() -> impl Iterator<Item = Box<dyn SchemaTransformProvider>> {
    inventory::iter::<SchemaTransformRegistration>
        .into_iter()
        .map(|reg| (reg.provider)())
}

/// Returns the linked provider with the URN `identifier`.
pub(super) fn find_provider(identifier: &str) -> Option<Box<dyn SchemaTransformProvider>> {
    providers().find(|provider| provider.identifier() == identifier)
}

/// Returns the provider of the Managed transform `identifier`: the provider with this URN, or
/// else the one provider that lists it in its Managed identifiers.
pub(super) fn find_managed_provider(
    identifier: &str,
) -> Result<Box<dyn SchemaTransformProvider>, String> {
    if let Some(provider) = find_provider(identifier) {
        return Ok(provider);
    }
    let mut claims =
        providers().filter(|provider| provider.managed_identifiers().contains(&identifier));
    match (claims.next(), claims.next()) {
        (Some(provider), None) => Ok(provider),
        (None, _) => Err(format!(
            "No SchemaTransformProvider found for the Managed identifier '{identifier}'"
        )),
        (Some(first), Some(second)) => Err(format!(
            "The Managed identifier '{identifier}' has two providers: '{}' and '{}'",
            first.identifier(),
            second.identifier()
        )),
    }
}
