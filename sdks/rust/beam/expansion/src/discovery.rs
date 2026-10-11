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

/// Returns the linked provider with the URN `identifier`.
pub(super) fn find_provider(identifier: &str) -> Option<Box<dyn SchemaTransformProvider>> {
    inventory::iter::<SchemaTransformRegistration>
        .into_iter()
        .map(|reg| (reg.provider)())
        .find(|provider| provider.identifier() == identifier)
}
