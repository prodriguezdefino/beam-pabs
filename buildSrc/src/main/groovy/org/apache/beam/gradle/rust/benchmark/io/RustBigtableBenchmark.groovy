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
import org.gradle.api.GradleException
import org.gradle.api.Project

/** Cloud Bigtable benchmark on Dataflow ({@code bigtableBenchmark}). */
@CompileStatic
class RustBigtableBenchmark {

  static void register(RustIoBenchmarkSupport support) {
    Project project = support.build.project
    support.registerDataflowBenchmark(new BenchmarkSpec(
        task: 'bigtableBenchmark',
        compileTask: 'compileBigtableJavaBenchmark',
        dir: 'bigtable',
        label: 'bigtable',
        jobPrefix: 'bt',
        pythonScript: 'bigtable_benchmark.py',
        mainClass: 'BigtableBenchmark',
        counter: 'rows',
        counterShapes: ['bt_to_gcs', 'gcs_to_bt'],
        io: { String shape -> [namespace: (Object) 'e2e', rows: (Object) 'rows'] },
        rustPackage: 'apache-beam-benchmark-bigtable',
        rustShapes: ['bt_to_gcs', 'gcs_to_bt'],
        defaultShape: 'bt_to_gcs',
        shapes: ['bt_to_gcs', 'gcs_to_bt'],
        readShapes: ['bt_to_gcs'],
        writeShapes: ['gcs_to_bt'],
        readers: [
          python: ['java', 'rust'],
          java  : ['java-native', 'java-schema', 'rust'],
          rust  : ['rust'],
        ],
        writers: [
          python: ['java', 'rust'],
          java  : ['java-native', 'java-schema', 'rust'],
          rust  : ['rust'],
        ],
        description: 'Read or write a Cloud Bigtable table on Dataflow ' +
            '([-Ppipeline=python|java|rust] [-Pshape=bt_to_gcs|gcs_to_bt] ' +
            '[-Preader=java|rust (Python) or java-native|java-schema|rust (Java) or rust (Rust), bt_to_gcs] ' +
            '[-Pwriter=java|rust (Python) or java-native|java-schema|rust (Java) or rust (Rust), gcs_to_bt] ' +
            '[-PbigtableProject=<project>] -PbigtableInstance=<instance> -PbigtableTable=<table> ' +
            '[-PbigtableMaterializedView=<view> (Rust bt_to_gcs)] ' +
            '[-PappProfile=<profile>] [-Pflatten=true|false] [-Pinput=<gs://...>, gcs_to_bt] ' +
            '[-Poutput=<gs://...>, bt_to_gcs] [-PexpectedRows=<n>] ' +
            '[-PnumWorkers=4] [-PmaxNumWorkers=<n>] [-PworkerMachineType=n2-standard-4] ' +
            '[-PextraArgs="<pipeline flags>"])',
        configure: { RunContext run ->
          String bigtableProject = support.benchProp('bigtableProject') ?: run.gcpProject
          String bigtableInstance = support.benchProp('bigtableInstance')
          String bigtableTable = support.benchProp('bigtableTable')
          String materializedView = project.findProperty('bigtableMaterializedView')?.toString()
          if (!bigtableProject || !bigtableInstance || !(bigtableTable || materializedView)) {
            throw new GradleException('Set -PbigtableInstance and -PbigtableTable (or ' +
                'example.benchmark.bigtableInstance and example.benchmark.bigtableTable in ' +
                'sdks/rust/.local/sdk.properties), or -PbigtableMaterializedView.')
          }
          if (materializedView && (run.pipeline != 'rust' || run.writes)) {
            throw new GradleException(
                '-PbigtableMaterializedView applies only to -Ppipeline=rust -Pshape=bt_to_gcs.')
          }
          String rawAppProfile = (project.findProperty('appProfile')
              ?: support.benchProp('bigtableAppProfile'))?.toString()
          String appProfile = (rawAppProfile && rawAppProfile != 'none') ? rawAppProfile : null
          String connector = run.writes ? run.writer : run.reader
          if (appProfile && !['rust', 'java-native'].contains(connector)) {
            String flagName = run.writes ? 'writer' : 'reader'
            throw new GradleException("${flagName.capitalize()} '${connector}' does not support -PappProfile: " +
                "use -P${flagName}=rust or -P${flagName}=java-native, or pass -PappProfile=none.")
          }

          String target = materializedView
              ? "${run.flag('bigtable_materialized_view')}=${materializedView}".toString()
              : "${run.flag('bigtable_table')}=${bigtableTable}".toString()
          List<String> args = [
            "${run.flag('bigtable_project')}=${bigtableProject}".toString(),
            "${run.flag('bigtable_instance')}=${bigtableInstance}".toString(),
            target,
          ]
          Map<String, String> labels = [
            app_profile: appProfile ? appProfile.toLowerCase().replace('_', '-') : 'default',
            target     : materializedView ? 'materialized-view' : 'table',
          ]
          Map<String, String> settings = new LinkedHashMap<>()

          if (run.writes) {
            if (project.hasProperty('output')) {
              throw new GradleException('-Poutput applies to -Pshape=bt_to_gcs only, not to gcs_to_bt.')
            }
            String input = (project.findProperty('input') ?: support.benchProp('bigtableInput'))?.toString()
            if (!input) {
              throw new GradleException('gcs_to_bt needs -Pinput=<gs://...> ' +
                  '(or example.benchmark.bigtableInput in sdks/rust/.local/sdk.properties).')
            }
            args.add("--input=${input}".toString())
          } else {
            if (project.hasProperty('input')) {
              throw new GradleException('-Pinput applies to -Pshape=gcs_to_bt only, not to bt_to_gcs.')
            }
            String flatten = (project.findProperty('flatten') ?: 'true').toString().toLowerCase()
            if (!['true', 'false'].contains(flatten)) {
              throw new GradleException("Unknown flatten '${flatten}': use true or false.")
            }
            String output = (project.findProperty('output')
                ?: "${run.tempLocation}/bigtable_benchmark/{job_name}/out").toString()
            args.add("${run.flag('flatten')}=${flatten}".toString())
            args.add("--output=${output}".toString())
            labels['flatten'] = flatten
            settings['flatten'] = flatten
          }
          settings['app_profile'] = appProfile ?: 'default'
          if (appProfile) {
            args.add("${run.flag('app_profile')}=${appProfile}".toString())
          }
          return [
            readerLabel: (Object) run.reader,
            labels     : labels,
            settings   : settings,
            args       : args,
          ]
        }))
  }
}
