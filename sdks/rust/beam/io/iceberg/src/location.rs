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

//! The locations of new data files of a table.
//!
//! With `write.object-storage.enabled`, each file goes under directories from a hash of its
//! name, so that the files of a table spread over the key space of the bucket. The hash and
//! its directories are those of `ObjectStoreLocationProvider` in the Iceberg Java library, so
//! a table has one layout for all its writers.

use std::io::Cursor;

use iceberg::spec::{PartitionKey, TableMetadata};
use iceberg::writer::file_writer::location_generator::LocationGenerator;

const DATA_PATH: &str = "write.data.path";
const OBJECT_STORAGE_PATH: &str = "write.object-storage.path";
const FOLDER_STORAGE_PATH: &str = "write.folder-storage.path";
const OBJECT_STORAGE_ENABLED: &str = "write.object-storage.enabled";
const PARTITIONED_PATHS: &str = "write.object-storage.partitioned-paths";

/// The bits of the hash in a path, and their split into directories.
const HASH_BITS: u32 = 20;
const HASH_DIRS: [usize; 4] = [4, 4, 4, 8];

/// The layout of the paths under the data location.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Layout {
    /// `{data}/{partition}/{file}`.
    Plain,
    /// `{data}/{hash}/{partition}/{file}`, or `{data}/{hash}-{file}` with no partition path.
    Hashed { partition_paths: bool },
}

/// Gives the location of each new data file of a table.
#[derive(Clone, Debug)]
pub struct TableLocations {
    data: String,
    layout: Layout,
}

impl TableLocations {
    /// The locations that the properties of `metadata` set.
    pub fn new(metadata: &TableMetadata) -> Self {
        let properties = metadata.properties();
        let enabled = |key: &str, default: bool| {
            properties
                .get(key)
                .map_or(default, |value| value.eq_ignore_ascii_case("true"))
        };
        let layout = if enabled(OBJECT_STORAGE_ENABLED, false) {
            Layout::Hashed {
                partition_paths: enabled(PARTITIONED_PATHS, true),
            }
        } else {
            Layout::Plain
        };
        let configured = match layout {
            Layout::Hashed { .. } => properties
                .get(DATA_PATH)
                .or_else(|| properties.get(OBJECT_STORAGE_PATH))
                .or_else(|| properties.get(FOLDER_STORAGE_PATH)),
            Layout::Plain => properties
                .get(DATA_PATH)
                .or_else(|| properties.get(FOLDER_STORAGE_PATH)),
        };
        let data = configured.map_or_else(
            || format!("{}/data", metadata.location().trim_end_matches('/')),
            |path| path.trim_end_matches('/').to_string(),
        );
        Self { data, layout }
    }
}

impl LocationGenerator for TableLocations {
    fn generate_location(&self, partition_key: Option<&PartitionKey>, file_name: &str) -> String {
        let partition = partition_key
            .filter(|key| !PartitionKey::is_effectively_none(Some(*key)))
            .map(PartitionKey::to_path);
        match (self.layout, partition) {
            (Layout::Plain, Some(partition)) => format!("{}/{partition}/{file_name}", self.data),
            (Layout::Plain, None) => format!("{}/{file_name}", self.data),
            (
                Layout::Hashed {
                    partition_paths: true,
                },
                Some(partition),
            ) => {
                let name = format!("{partition}/{file_name}");
                format!("{}/{}/{name}", self.data, hash_dirs(&name))
            }
            (
                Layout::Hashed {
                    partition_paths: true,
                },
                None,
            ) => {
                format!("{}/{}/{file_name}", self.data, hash_dirs(file_name))
            }
            (
                Layout::Hashed {
                    partition_paths: false,
                },
                _,
            ) => {
                format!("{}/{}-{file_name}", self.data, hash_dirs(file_name))
            }
        }
    }
}

/// The directories of the 20 low bits of the murmur3 hash of `name`: `bbbb/bbbb/bbbb/bbbbbbbb`.
pub fn hash_dirs(name: &str) -> String {
    #[expect(clippy::expect_used, reason = "Cursor<&[u8]> read cannot fail")]
    let hash = murmur3::murmur3_32(&mut Cursor::new(name.as_bytes()), 0)
        .expect("a hash of an in-memory buffer does not fail");
    let bits = format!(
        "{:0width$b}",
        hash & ((1 << HASH_BITS) - 1),
        width = HASH_BITS as usize
    );
    HASH_DIRS
        .iter()
        .scan(0, |start, length| {
            let dir = &bits[*start..*start + length];
            *start += length;
            Some(dir)
        })
        .collect::<Vec<_>>()
        .join("/")
}
