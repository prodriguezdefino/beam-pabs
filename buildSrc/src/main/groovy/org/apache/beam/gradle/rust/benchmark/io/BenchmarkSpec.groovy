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

/** Definition of one I/O Dataflow benchmark task. */
@CompileStatic
class BenchmarkSpec {
  String task
  String compileTask
  String dir
  String label
  String jobPrefix
  String pythonScript
  String mainClass
  String counter
  List<String> counterShapes = []
  Closure<Map<String, Object>> io
  String rustPackage
  List<String> rustShapes = []
  String expansionImageTask
  String expansionImageName
  String defaultShape = 'count'
  List<String> shapes = []
  List<String> readShapes
  List<String> writeShapes = []
  Map<String, List<String>> readers = [:]
  Map<String, List<String>> writers = [:]
  Closure<Boolean> hasProducer
  Map<String, String> producerDefaults = [:]
  Closure<Map<String, Object>> producer
  String description
  Closure<Map<String, Object>> configure
  Closure<List<String>> after
}
