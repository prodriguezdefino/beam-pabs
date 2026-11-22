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
package org.apache.beam.gradle.rust.runner

import groovy.transform.CompileStatic
import org.apache.beam.gradle.rust.RustConfig
import org.apache.beam.gradle.rust.RustSdkBuild
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.tasks.Exec

/** ValidatesRunner conformance suite tasks on Prism and Google Cloud Dataflow. */
@CompileStatic
class RustValidatesRunnerTasks {

  private static class SuiteSpec {
    String name
    String runner
    boolean streaming
    String description
  }

  private static final List<SuiteSpec> SUITES = [
    new SuiteSpec(
        name: 'validatesRunnerPrism',
        runner: 'prism',
        streaming: false,
        description: 'Run the ValidatesRunner conformance suite on the Prism runner'),
    new SuiteSpec(
        name: 'validatesRunnerPrismStreaming',
        runner: 'prism',
        streaming: true,
        description: 'Run the ValidatesRunner conformance suite in streaming mode on the Prism runner'),
    new SuiteSpec(
        name: 'validatesRunnerDataflow',
        runner: 'dataflow',
        streaming: false,
        description: 'Run the ValidatesRunner conformance suite on Google Cloud Dataflow'),
    new SuiteSpec(
        name: 'validatesRunnerDataflowStreaming',
        runner: 'dataflow',
        streaming: true,
        description: 'Run the ValidatesRunner conformance suite in streaming mode on Google Cloud Dataflow'),
  ]

  static void register(RustDataflowTasks dataflow) {
    RustConfig config = dataflow.config
    RustSdkBuild build = config.build
    RustPrismTasks prism = config.prism
    Project project = build.project

    SUITES.each { SuiteSpec cfg ->
      project.tasks.register(cfg.name, Exec) { Exec task ->
        task.group = 'Verification'
        task.description = "${cfg.description} (-Pfilter=<name> -PtestThreads=<n> -Pignored -PincludeIgnored" +
            (cfg.runner == 'dataflow' ? ' -PextraArgs="<pipeline flags>")' : ')')
        task.workingDir(build.rustDir)
        task.environment(build.versions.cargoEnv())
        if (cfg.streaming) {
          task.environment('BEAM_STREAMING', 'true')
        }

        if (cfg.runner == 'prism') {
          task.dependsOn('fetchPrism')
          task.inputs.file(prism.prismHostBinary)
          task.environment('BEAM_PRISM_PATH', prism.prismHostBinary.absolutePath)
          if (cfg.streaming) {
            task.environment('BEAM_PRISM_STREAMING', 'true')
          }
          task.commandLine(cargoTestCommand(build, 'validates_runner_prism', null))
        } else {
          task.dependsOn('buildWorkspace', 'buildRunnerTestsWorker')
          task.doFirst {
            dataflow.configureDataflowEnvironment(task)
            if (cfg.streaming) {
              task.environment('BEAM_DATAFLOW_STREAMING', 'true')
            }
            String extraArgs = project.findProperty('extraArgs')?.toString()
            if (extraArgs != null && !extraArgs.isEmpty()) {
              task.environment('BEAM_DATAFLOW_EXTRA_ARGS', extraArgs)
            }
            task.commandLine(cargoTestCommand(build, 'validates_runner_dataflow', '4'))
          }
        }
      }
    }

    project.tasks.register('validatesRunnerStreaming') { Task task ->
      task.group = 'Verification'
      task.description = 'Run all ValidatesRunner streaming tasks that need no cloud project (Prism)'
      task.dependsOn('validatesRunnerPrismStreaming')
    }

    project.tasks.register('validatesRunner') { Task task ->
      task.group = 'Verification'
      task.description = 'Run all ValidatesRunner conformance suites that need no cloud project (Prism)'
      task.dependsOn('validatesRunnerPrism')
    }
  }

  private static List<String> cargoTestCommand(
      RustSdkBuild build, String testBinary, String defaultThreads) {
    Project project = build.project
    List<String> filters = project.findProperty('filter')?.toString()?.tokenize() ?: new ArrayList<String>()
    Object rawThreads = project.findProperty('testThreads')
        ?: System.getenv('RUST_TEST_THREADS')
        ?: defaultThreads
    List<String> threadsArg = rawThreads != null
        ? ["--test-threads=${rawThreads}".toString()] : new ArrayList<String>()
    List<String> ignoredFlags = project.hasProperty('ignored')
        ? ['--ignored']
        : (project.hasProperty('includeIgnored') ? ['--include-ignored'] : new ArrayList<String>())
    List<String> args = filters + threadsArg + ignoredFlags
    List<String> cmd = ['cargo', 'test'] + build.workspaceScope + ['--test', testBinary]
    return args.isEmpty() ? cmd : cmd + ['--'] + args
  }
}
