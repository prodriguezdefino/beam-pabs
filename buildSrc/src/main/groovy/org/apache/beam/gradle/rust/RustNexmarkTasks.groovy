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
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.tasks.Exec

/** Nexmark worker build, Prism and Dataflow launchers, and Dataflow benchmark task. */
@CompileStatic
class RustNexmarkTasks {

  private static List<String> nexmarkCliFlags(RustConfig config, RustBenchmarkTasks benchmarks) {
    List<String> flags = []
    String suite = config.resolveProp('nexmark', 'suite')
    if (suite != null && !suite.isEmpty()) {
      flags.add("--suite=${suite}".toString())
    }
    String query = config.resolveProp('nexmark', 'query')
    if (query != null && !query.isEmpty()) {
      flags.add("--query=${query}".toString())
    }
    if (benchmarks.isSet('streaming')) {
      flags.add('--streaming')
    }
    return flags
  }

  static void register(RustContainerTasks containers, RustBenchmarkTasks benchmarks) {
    RustDataflowTasks dataflow = benchmarks.dataflow
    RustConfig config = containers.config
    RustSdkBuild build = config.build
    Project project = build.project

    containers.registerWorkerTask(
        'buildNexmarkWorker',
        'Build Linux worker binary for Nexmark container/Dataflow execution (-PexampleArch=amd64)',
        'nexmark',
        config.workerArch,
        'nexmark')

    project.tasks.register('nexmarkPrism', Exec) { Exec task ->
      task.group = 'Application'
      task.description = 'Run the Nexmark benchmark suite on the local Prism Runner ' +
          '([-Psuite=SMOKE] [-Pquery=0] [-Pstreaming] [-Pdocker] [-PextraArgs=<flags>])'
      task.workingDir(build.rustDir)
      task.dependsOn('fetchPrism')

      Object dockerProp = project.findProperty('docker')
      boolean dockerMode = dockerProp instanceof CharSequence && dockerProp.toString() != 'false'
      if (dockerMode && config.suppliedWorkerBinary('nexmark') == null) {
        task.dependsOn('buildNexmarkWorker')
      }

      task.doFirst {
        String defaultEnv = "${BeamModuleExtras.dockerDefaultRepoRoot(project)}/" +
            "${BeamModuleExtras.dockerDefaultRepoPrefix(project)}rust_sdk:${build.versions.sdkVersion}"
        String envConfig = dockerMode ? config.resolveProp('nexmark', 'environmentConfig', null, defaultEnv) : null
        String supplied = config.suppliedWorkerBinary('nexmark')
        List<String> workerFlag
        if (!dockerMode || supplied == 'none') {
          workerFlag = new ArrayList<String>()
        } else if (supplied != null) {
          workerFlag = ["--worker_binary=${supplied}".toString()]
        } else {
          workerFlag = ["--worker_binary=${config.workerBinaryFor('nexmark', config.workerArch).absolutePath}".toString()]
        }
        List<String> gcpArgs = config.portableGcpArgs('nexmark')
        List<String> envFlags = dockerMode
            ? ['--environment_type=DOCKER', "--environment_config=${envConfig}".toString()] + workerFlag
            : ['--environment_type=LOOPBACK']
        List<String> extraArgs = (config.resolveProp('nexmark', 'extraArgs') ?: '').tokenize(' ')
        List<String> cmd = ['cargo', 'run', '--release', '--bin', 'nexmark', '--', '--runner=PrismRunner'] +
            envFlags + gcpArgs + nexmarkCliFlags(config, benchmarks) + extraArgs

        task.environment(config.launchEnvironment())
        task.commandLine(cmd)
      }
    }

    project.tasks.register('nexmarkDataflow', Exec) { Exec task ->
      task.group = 'Application'
      task.description = 'Run the Nexmark benchmark suite on Google Cloud Dataflow ' +
          '([-Psuite=DEFAULT] [-Pquery=0] [-Pstreaming] [-PextraArgs=<flags>])'
      task.workingDir(build.rustDir)
      if (config.suppliedWorkerBinary('nexmark') == null) {
        task.dependsOn('buildNexmarkWorker')
      }

      task.doFirst {
        task.environment(config.launchEnvironment())
        task.commandLine(dataflow.buildDataflowArgs('nexmark', null, [
          pkg        : 'apache-beam-nexmark',
          bin        : 'nexmark',
          skipIoFlags: true,
          extraFlags : nexmarkCliFlags(config, benchmarks),
        ]))
      }
    }

    project.tasks.register('nexmarkBenchmark') { Task task ->
      task.group = 'Verification'
      task.description = 'Run a Nexmark query on Dataflow for some rounds and report its CPU and throughput metrics ' +
          '([-Pquery=0] [-PnumEvents=1000000] [-Pstreaming] [-Pruns=3] [-Pbaseline=<git ref>] ' +
          '[-PcompareTo=<results.json>] [-PnumWorkers=1] [-PmaxNumWorkers=10] ' +
          '[-PworkerMachineType=n2-standard-4] [-PminCpuPlatform=<platform>] ' +
          '[-PextraArgs=<pipeline flags>] [-PdryRun])'

      task.doLast {
        String query = (project.findProperty('query') ?: '0').toString()
        String numEvents = (project.findProperty('numEvents') ?: '1000000').toString()
        boolean streaming = benchmarks.isSet('streaming')
        String workers = (project.findProperty('numWorkers') ?: '1').toString()
        String maxWorkers = (project.findProperty('maxNumWorkers') ?: '10').toString()
        String machine = (project.findProperty('workerMachineType') ?: 'n2-standard-4').toString()
        String minCpu = project.findProperty('minCpuPlatform')?.toString()
        String experiments = benchmarks.minCpuExperiments()
        List<String> props = [
          '-Pinput=none',
          '-Poutput=none',
          "-PnumWorkers=${workers}".toString(),
          "-PmaxNumWorkers=${maxWorkers}".toString(),
          "-PworkerMachineType=${machine}".toString(),
        ]
        if (!experiments.isEmpty()) {
          props.add("-Pexperiments=${experiments}".toString())
        }
        props.addAll(benchmarks.passthrough(RustBenchmarkTasks.DATAFLOW_PASSTHROUGH))

        List<String> extraParts = ["--query=${query}".toString(), "--num_events=${numEvents}".toString()]
        if (streaming) {
          extraParts.add('--streaming')
        }
        if (project.findProperty('extraArgs') != null) {
          extraParts.add(project.findProperty('extraArgs').toString())
        }
        String extraArgs = extraParts.join(' ')
        String mode = streaming ? 'streaming' : 'batch'
        String summary = "Nexmark query ${query} (${mode}), ${numEvents} events, ${machine}, " +
            "${workers} to ${maxWorkers} workers" + (minCpu ? ", minimum CPU platform ${minCpu}" : '') + '.'

        benchmarks.gitRefVariants { List<Map<String, String>> checkouts ->
          List<Map<String, Object>> variants = checkouts.collect { Map<String, String> checkout ->
            benchmarks.rustDataflowVariant(checkout, 'nexmark', props, extraArgs)
          }
          benchmarks.runDataflowBenchmark([
            task       : 'nexmarkBenchmark',
            describe   : summary,
            workers    : workers,
            countOutput: 'MonitorEvents',
            countName  : 'events',
            counter    : 'events.elements',
          ], variants)
        }
      }
    }
  }
}
