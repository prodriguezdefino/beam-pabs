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
package org.apache.beam.gradle.rust

import groovy.transform.CompileStatic
import org.gradle.api.GradleException
import org.gradle.api.Project

/** BigQuery benchmark on Dataflow ({@code bigQueryBenchmark}). */
@CompileStatic
class RustBigQueryBenchmark {

  static void register(RustIoBenchmarkSupport support) {
    Project project = support.build.project
    support.registerDataflowBenchmark(new RustIoBenchmarkSupport.BenchmarkSpec(
        task: 'bigQueryBenchmark',
        compileTask: 'compileBigQueryJavaBenchmark',
        dir: 'bigquery',
        label: 'bigquery',
        jobPrefix: 'bq',
        pythonScript: 'bigquery_benchmark.py',
        mainClass: 'BigQueryBenchmark',
        counter: 'rows',
        counterShapes: support.benchPipeline == 'rust' ? ['count', 'copy'] : ['copy'],
        io: { String shape -> [namespace: (Object) 'e2e', rows: (Object) 'rows'] },
        rustPackage: 'apache-beam-benchmark-bigquery',
        rustShapes: ['count', 'copy'],
        shapes: ['count', 'per_key', 'copy'],
        writeShapes: ['copy'],
        readers: [
          python: ['python', 'java', 'rust'],
          java  : ['java-native', 'java-managed', 'rust'],
          rust  : ['rust'],
        ],
        writers: [
          python: ['java', 'rust'],
          java  : ['java-native', 'java-managed', 'rust'],
          rust  : ['rust'],
        ],
        description: 'Read a BigQuery table on Dataflow, and for -Pshape=copy write it to a new table ' +
            '([-Ppipeline=python|java|rust] [-Pshape=count|per_key|copy] ' +
            '-Preader=python|java|rust (Python) or java-native|java-managed|rust (Java) or rust (Rust) ' +
            '[-Pwriter=java|rust (Python) or java-native|java-managed|rust (Java) or rust (Rust), copy only] ' +
            '[-Ptable=<spec>] [-ProwRestriction=<filter>] [-Pquery=<sql>] [-PselectedFields=a,b] ' +
            '[-Pkey=language] [-PexpectedRows=<n>] [-PnativeOutput=tablerow|row] [-PnativeFormat=avro|arrow] ' +
            '[-Pcompression=NONE|LZ4_FRAME|ZSTD] [-Pdataset=<project.dataset>] ' +
            '[-Psemantics=exactly_once|at_least_once] [-PdistinctColumn=revision_id] [-PkeepTable] ' +
            '[-PnumWorkers=4] [-PmaxNumWorkers=<n>] [-PworkerMachineType=n2-standard-4] ' +
            '[-PextraArgs="<pipeline flags>"])',
        configure: { RustIoBenchmarkSupport.RunContext run ->
          String query = project.findProperty('query')?.toString()
          String selectedFields = project.findProperty('selectedFields')?.toString()
          String rowRestriction = project.findProperty('rowRestriction')?.toString()
          String compression = (run.reader == 'rust' ? project.findProperty('compression') : null)?.toString()
          String compressionLabel = (compression ?: 'none').toLowerCase().replace('_', '-')
          String nativeOutput = (project.findProperty('nativeOutput') ?: (run.writes ? 'row' : 'tablerow')).toString()
          String nativeFormat = (project.findProperty('nativeFormat') ?: 'avro').toString()
          String readerLabel = run.reader == 'java-native'
              ? "java-native-${nativeOutput}-${nativeFormat}".toString() : run.reader
          List<String> args = []
          if (run.reader == 'java-native') {
            args.add("--nativeOutput=${nativeOutput}".toString())
            args.add("--nativeFormat=${nativeFormat}".toString())
          }
          if (compression != null && !compression.isEmpty()) {
            args.add("--compression=${compression}".toString())
          }
          Map<String, String> labels = [
            compression: compressionLabel,
            source     : query ? 'query' : 'table',
          ]
          Map<String, Object> spec = [:]

          if (run.writes) {
            if (query || selectedFields) {
              throw new GradleException(
                  '-Pshape=copy reads whole rows of -Ptable: remove -Pquery and -PselectedFields.')
            }
            if (run.reader == 'java-native' && nativeOutput != 'row') {
              throw new GradleException(
                  '-Pshape=copy reads Beam rows: remove -PnativeOutput or set it to row.')
            }
            if (run.reader == 'python' && run.writer == 'rust') {
              throw new GradleException('The Python reader gives dicts, and the Rust writer takes Beam rows. ' +
                  'Use -Pwriter=java with -Preader=python.')
            }
            String source = support.benchProp('table')
            String dataset = support.benchProp('dataset')
            if (!source || !dataset) {
              throw new GradleException('Set -Ptable and -Pdataset, or example.benchmark.table and ' +
                  'example.benchmark.dataset in sdks/rust/.local/sdk.properties.')
            }
            String semantics = (project.findProperty('semantics') ?: 'exactly_once').toString()
            if (!['exactly_once', 'at_least_once'].contains(semantics)) {
              throw new GradleException(
                  "Unknown semantics '${semantics}': use exactly_once or at_least_once.")
            }
            String destination = "${dataset}.{job_name_underscore}"
            args.add("${run.flag('table')}=${source}".toString())
            args.add("${run.flag('destination')}=${destination}".toString())
            args.add("--semantics=${semantics}".toString())
            if (rowRestriction != null && !rowRestriction.isEmpty()) {
              args.add("${run.flag('row_restriction')}=${rowRestriction}".toString())
            }
            labels['semantics'] = semantics.replace('_', '-')
            spec['source'] = source
            spec['destination'] = destination
            spec['semantics'] = semantics
            spec['rowRestriction'] = rowRestriction
            spec['gcpProject'] = run.gcpProject
          } else {
            if (project.findProperty('semantics') != null || project.findProperty('dataset') != null) {
              throw new GradleException(
                  "-Psemantics and -Pdataset apply to -Pshape=copy only, not to ${run.shape}.")
            }
            String table = !query ? support.benchProp('table') : project.findProperty('table')?.toString()
            if (table != null && !table.isEmpty()) {
              args.add("${run.flag('table')}=${table}".toString())
            }
            Map<String, String> argMap = new LinkedHashMap<>([
              query         : 'query',
              selectedFields: 'selected_fields',
              rowRestriction: 'row_restriction',
              key           : 'key',
            ])
            if (run.pipeline != 'rust') {
              argMap['expectedRows'] = 'expected_rows'
            }
            args.addAll(support.benchArgs(run, argMap))
          }
          if (rowRestriction != null && !rowRestriction.isEmpty()) {
            labels['restricted'] = 'true'
          }
          spec['readerLabel'] = readerLabel
          spec['labels'] = labels
          spec['args'] = args
          return spec
        },
        after: { Map<String, Object> spec ->
          List<String> args = [
            "${support.benchVenv.absolutePath}/bin/python".toString(),
            "${support.build.rustDir.absolutePath}/benchmarks/tools/check_table.py".toString(),
            "--table=${spec['destination']}".toString(),
            "--expected_table=${spec['source']}".toString(),
            "--distinct_column=${project.findProperty('distinctColumn') ?: 'revision_id'}".toString(),
            "--project=${spec['gcpProject']}".toString(),
          ]
          if (spec['rowRestriction']) {
            args.add("--expected_filter=${spec['rowRestriction']}".toString())
          }
          if (spec['semantics'] == 'at_least_once') {
            args.add('--at_least')
          }
          if (!support.benchmarks.isSet('keepTable')) {
            args.add('--drop')
          }
          return args
        }))
  }
}
