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
package org.apache.beam.gradle.rust.xlang

import groovy.transform.CompileStatic
import org.gradle.api.Action
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.process.ExecSpec

/** Go cross-language ValidatesRunner tasks. */
@CompileStatic
class RustXlangGoTasks {

  static void register(RustXlangTasks xlang) {
    Project project = xlang.build.project
    if (project.gradle.startParameter.taskNames.any { String name ->
      name.contains('CrossLanguageRunnerGo')
    }) {
      project.evaluationDependsOn(':sdks:go:test')
    }

    String goSdkImage = xlang.registerSdkDataflowImageTask([
      sdk        : 'Go',
      imageSuffix: 'go_sdk',
      gradleTask : ':sdks:go:container:docker',
    ])

    xlang.registerXlangSuiteTasks(
        [
          sdk         : 'Go',
          testExamples: 'Prefix,GroupBy',
          dependsOn   : [':runners:prism:goPrepare'],
          dataflowHint: '[-PgoSdkImage=<image>]',
        ],
        { DefaultTask task ->
          task.doLast {
            xlang.withPrismJobServer { int jobPort ->
              runGoSuite(xlang, xlang.testingImage, [
                '--runner=portable',
                "--endpoint=localhost:${jobPort}".toString(),
                '--environment_type=LOOPBACK',
              ])
            }
          }
        } as Action<DefaultTask>,
        { DefaultTask task ->
          task.doLast {
            if (!goSdkImage) {
              throw new GradleException(
                  'Set goSdkImage in sdks/rust/.local/sdk.properties or with -P (run :sdks:rust:goSdkDataflowImage).')
            }
            runGoSuite(xlang, xlang.dataflowImage, xlang.xlangDataflowFlags([
              runner         : 'dataflow',
              tempSuffix     : '/temp',
              machineTypeFlag: 'worker_machine_type',
              extraFlags     : { String temp ->
                [
                  '-timeout', '1h',
                  "--staging_location=${temp}/staging".toString(),
                  '--environment_type=DOCKER',
                  "--environment_config=${goSdkImage}".toString(),
                ]
              },
            ]))
          }
        } as Action<DefaultTask>)
  }

  private static void runGoSuite(RustXlangTasks xlang, String workerImage, List<String> goArgs) {
    List<String> selected = xlang.selectedTests { String name -> "TestXLang_${name}".toString() }
    String pattern = selected != null ? "^(${selected.join('|')})\$".toString() : '^TestXLang_'
    xlang.execWithExpansionService([workerImage: workerImage]) { RustXlangTasks.ServiceHandle service ->
      ExecSpec spec = (ExecSpec) delegate
      spec.workingDir(new File(xlang.build.project.rootDir, 'sdks/go'))
      spec.commandLine([
        './run_with_go_version.sh', 'test', '-v', './test/integration/xlang',
        '-run', pattern,
        "--expansion_addr=test:localhost:${service.port}".toString(),
      ] + goArgs)
    }
  }
}
