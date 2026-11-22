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
import org.apache.beam.gradle.rust.BeamModuleExtras
import org.apache.beam.gradle.rust.RustSdkBuild
import org.gradle.api.Action
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.process.ExecSpec

/** Python cross-language ValidatesRunner tasks. */
@CompileStatic
class RustXlangPythonTasks {

  private static final String PYTHON_SUITE =
      'apache_beam/transforms/validate_runner_xlang_test.py::ValidateRunnerXlangTest'

  static void register(RustXlangTasks xlang) {
    RustSdkBuild build = xlang.build
    Project project = build.project
    String imagePythonVersion = (project.findProperty('pythonVersion') ?: '3.12').toString()

    xlang.pythonSdkImage = xlang.registerSdkDataflowImageTask([
      sdk        : 'Python',
      imageSuffix: "python${imagePythonVersion}_sdk".toString(),
      defaultTag : build.versions.sdkVersion,
      gradleTask : ":sdks:python:container:py${imagePythonVersion.replace('.', '')}:docker".toString(),
      extraArgs  : ["-PpythonVersion=${imagePythonVersion}".toString()],
      extraHint  : '[-PpythonVersion=3.12]',
    ])

    xlang.registerXlangSuiteTasks(
        [
          sdk              : 'Python',
          testExamples     : 'prefix,group_by_key',
          dependsOn        : ['setupXlangPythonEnv'],
          dataflowDependsOn: xlang.pythonSdkImage != null ? [] : [':sdks:python:sdist'],
          prismHint        : '[-PpythonVersion=3.12]',
          dataflowHint     : '[-PxlangParallelism=8] [-PpythonVersion=3.12] [-PpythonSdkImage=<image>]',
        ],
        { DefaultTask task ->
          task.doLast {
            runPythonSuite(xlang, xlang.testingImage, [
              '--runner=PrismRunner',
              '--environment_type=LOOPBACK',
              "--prism_location=${xlang.config.prism.prismHostBinary.absolutePath}".toString(),
            ], new ArrayList<String>())
          }
        } as Action<DefaultTask>,
        { DefaultTask task ->
          task.doLast {
            String parallelism = (project.findProperty('xlangParallelism') ?: '8').toString()
            runPythonSuite(
                xlang,
                xlang.dataflowImage,
                xlang.pythonDataflowPipelineOptions(),
                ["--numprocesses=${parallelism}".toString(), '--timeout=1800'])
          }
        } as Action<DefaultTask>)

    project.tasks.register('validatesCrossLanguageRunnerPythonServicePrism') { Task task ->
      task.group = 'Verification'
      task.description = 'Run the Rust cross-language suite against the Python test expansion service on Prism ' +
          '([-Pfilter=<test name>] [-PtestThreads=<n>] [-PincludeIgnored] [-PpythonVersion=3.12] [-PbuildPythonContainer])'
      task.dependsOn('setupXlangPythonEnv', 'fetchPrism')
      if (project.hasProperty('buildPythonContainer')) {
        task.dependsOn(":sdks:python:container:py${imagePythonVersion.replace('.', '')}:docker")
      }
      task.doLast {
        if (!xlang.dockerReachable()) {
          throw new GradleException(
              'Docker is not reachable. The cross-language suite needs it to start the Python SDK container.')
        }
        String venvPy = "${xlang.xlangVenv.absolutePath}/bin/python"
        ByteArrayOutputStream pyVerOut = new ByteArrayOutputStream()
        project.exec({ ExecSpec spec ->
          spec.commandLine(venvPy, '-c', 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
          spec.standardOutput = pyVerOut
        } as Action<ExecSpec>)
        String venvPyVersion = pyVerOut.toString('UTF-8').trim()
        String localPythonSdkImage = "${BeamModuleExtras.dockerDefaultRepoRoot(project)}/" +
            "${BeamModuleExtras.dockerDefaultRepoPrefix(project)}python${venvPyVersion}_sdk:${build.versions.sdkVersion}"
        if (!xlang.imageExists(localPythonSdkImage)) {
          String pyTaskVer = venvPyVersion.replace('.', '')
          throw new GradleException("${localPythonSdkImage} is not available locally. Build it with " +
              "./gradlew :sdks:python:container:py${pyTaskVer}:docker, or add " +
              "-PpythonVersion=${venvPyVersion} -PbuildPythonContainer.")
        }
        xlang.runXlangTestsAgainstService(
            'Python',
            [venvPy, '-m', 'apache_beam.runners.portability.expansion_service_test',
             '--fully_qualified_name_glob', '*', '-p'],
            xlang.pythonDir,
            'BEAM_PYTHON_TEST_EXPANSION_ADDR',
            ['python_service_test'])
      }
    }
  }

  private static void runPythonSuite(
      RustXlangTasks xlang,
      String workerImage,
      List<String> pipelineOpts,
      List<String> testOpts) {
    List<String> selected = xlang.selectedTests { String name -> "${PYTHON_SUITE}::test_${name}".toString() }
        ?: [PYTHON_SUITE]
    String args = BeamModuleExtras.mapToArgString(xlang.build.project.project(':sdks:python'), [
      pipeline_opts: pipelineOpts,
      test_opts    : testOpts + selected,
      suite        : 'rustXlangValidateRunner',
    ])
    xlang.execWithExpansionService([workerImage: workerImage]) { RustXlangTasks.ServiceHandle service ->
      ExecSpec spec = (ExecSpec) delegate
      spec.workingDir(xlang.pythonDir)
      spec.environment('EXPANSION_PORT', service.port)
      spec.commandLine('sh', '-c', ". ${xlang.xlangVenv.absolutePath}/bin/activate && ./scripts/run_integration_test.sh ${args}")
    }
  }
}
