/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.gradle.rust.benchmark.io

import groovy.transform.CompileStatic

/** Context passed to the {@code configure} closure of each benchmark. */
@CompileStatic
class RunContext {
  String shape
  boolean reads
  boolean writes
  String pipeline
  String reader
  String writer
  String gcpProject
  String region
  String tempLocation

  String flag(String name) {
    return RustIoBenchmarkSupport.benchFlag(pipeline, name)
  }
}
