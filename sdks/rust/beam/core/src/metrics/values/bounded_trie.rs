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

//! The value of a bounded trie metric.

use std::collections::{BTreeMap, BTreeSet};

use model::pipeline as proto;
use prost::Message;

/// The bound of a [`BoundedTrieValue`] that a user metric creates.
pub const BOUNDED_TRIE_DEFAULT_BOUND: usize = 100;

/// One node of a bounded trie. `size` counts the paths that end at or below the node.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TrieNode {
    children: BTreeMap<String, TrieNode>,
    truncated: bool,
    size: usize,
}

impl Default for TrieNode {
    fn default() -> Self {
        Self {
            children: BTreeMap::new(),
            truncated: false,
            size: 1,
        }
    }
}

impl TrieNode {
    /// Adds the path `segments` below this node. Returns the change of `size`.
    fn add(&mut self, segments: &[&str]) -> isize {
        let Some((head, tail)) = segments.split_first().filter(|_| !self.truncated) else {
            return 0;
        };
        let was_leaf = self.children.is_empty();
        let mut delta = 0;
        let child = self.children.entry((*head).to_string()).or_insert_with(|| {
            delta = isize::from(!was_leaf);
            TrieNode::default()
        });
        delta += child.add(tail);
        self.resize(delta);
        delta
    }

    /// Adds the paths of `other`. Returns the change of `size`.
    fn merge(&mut self, other: &TrieNode) -> isize {
        if self.truncated || other.children.is_empty() && !other.truncated {
            return 0;
        }
        if other.truncated {
            return self.truncate();
        }
        let delta = if self.children.is_empty() {
            self.children = other.children.clone();
            other.size as isize - self.size as isize
        } else {
            other.children.iter().fold(0, |delta, (segment, theirs)| {
                delta
                    + match self.children.get_mut(segment) {
                        Some(ours) => ours.merge(theirs),
                        None => {
                            self.children.insert(segment.clone(), theirs.clone());
                            theirs.size as isize
                        }
                    }
            })
        };
        self.resize(delta);
        delta
    }

    /// Removes one level below the largest subtree. Returns the change of `size`.
    fn trim(&mut self) -> isize {
        let Some(largest) = self.children.values_mut().max_by_key(|child| child.size) else {
            return 0;
        };
        if largest.size == 1 {
            return self.truncate();
        }
        let delta = largest.trim();
        self.resize(delta);
        delta
    }

    /// Replaces the children with a truncated marker. Returns the change of `size`.
    fn truncate(&mut self) -> isize {
        let delta = 1 - self.size as isize;
        self.truncated = true;
        self.children.clear();
        self.size = 1;
        delta
    }

    fn resize(&mut self, delta: isize) {
        self.size = self.size.saturating_add_signed(delta);
    }

    /// Appends each path below this node to `out`, with its truncated flag.
    fn paths(&self, prefix: &mut Vec<String>, out: &mut BTreeSet<(Vec<String>, bool)>) {
        if self.children.is_empty() {
            out.insert((prefix.clone(), self.truncated));
            return;
        }
        self.children.iter().for_each(|(segment, child)| {
            prefix.push(segment.clone());
            child.paths(prefix, out);
            prefix.pop();
        });
    }

    fn to_proto(&self) -> proto::BoundedTrieNode {
        proto::BoundedTrieNode {
            truncated: self.truncated,
            children: self
                .children
                .iter()
                .map(|(segment, child)| (segment.clone(), child.to_proto()))
                .collect(),
        }
    }

    fn from_proto(node: &proto::BoundedTrieNode) -> Self {
        let children: BTreeMap<String, TrieNode> = node
            .children
            .iter()
            .map(|(segment, child)| (segment.clone(), TrieNode::from_proto(child)))
            .collect();
        let size = children
            .values()
            .map(|child| child.size)
            .sum::<usize>()
            .max(1);
        Self {
            children,
            truncated: node.truncated,
            size,
        }
    }
}

/// A set of paths of string segments that keeps at most `bound` paths. Past the bound, it
/// replaces the deepest branches of the largest subtree with truncated markers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedTrieValue {
    bound: usize,
    root: Option<TrieNode>,
}

impl Default for BoundedTrieValue {
    fn default() -> Self {
        Self::new(BOUNDED_TRIE_DEFAULT_BOUND)
    }
}

impl BoundedTrieValue {
    /// An empty trie that keeps at most `bound` paths (at least 1).
    pub fn new(bound: usize) -> Self {
        Self {
            bound: bound.max(1),
            root: None,
        }
    }

    /// Adds the path `segments`.
    pub fn add(&mut self, segments: &[&str]) {
        self.root
            .get_or_insert_with(TrieNode::default)
            .add(segments);
        self.trim();
    }

    /// Adds the paths of `other`. The smaller bound applies.
    pub fn merge(&mut self, other: &Self) {
        self.bound = self.bound.min(other.bound);
        if let Some(theirs) = &other.root {
            match &mut self.root {
                Some(ours) => {
                    ours.merge(theirs);
                }
                None => self.root = Some(theirs.clone()),
            }
        }
        self.trim();
    }

    fn trim(&mut self) {
        if let Some(root) = &mut self.root {
            while root.size > self.bound {
                root.trim();
            }
        }
    }

    /// Each path with a flag that is `true` if the trie truncated the path.
    pub fn paths(&self) -> BTreeSet<(Vec<String>, bool)> {
        let mut out = BTreeSet::new();
        if let Some(root) = &self.root {
            root.paths(&mut Vec::new(), &mut out);
        }
        out
    }

    /// The maximum number of paths.
    pub fn bound(&self) -> usize {
        self.bound
    }

    /// Encodes the trie as Fn API bytes: a serialized `BoundedTrie` message.
    pub fn encode_payload(&self) -> Vec<u8> {
        proto::BoundedTrie {
            bound: i32::try_from(self.bound).unwrap_or(i32::MAX),
            root: self.root.as_ref().map(TrieNode::to_proto),
            singleton: Vec::new(),
        }
        .encode_to_vec()
    }

    /// Decodes a trie from a serialized `BoundedTrie` message.
    pub fn decode_payload(payload: &[u8]) -> Result<Self, String> {
        let trie = proto::BoundedTrie::decode(payload)
            .map_err(|e| format!("Failed to decode a bounded trie: {e}"))?;
        let mut value = Self {
            bound: usize::try_from(trie.bound).unwrap_or(1).max(1),
            root: trie.root.as_ref().map(TrieNode::from_proto),
        };
        if !trie.singleton.is_empty() {
            let segments: Vec<&str> = trie.singleton.iter().map(String::as_str).collect();
            value.add(&segments);
        }
        Ok(value)
    }
}
