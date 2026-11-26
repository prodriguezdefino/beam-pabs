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
package org.apache.beam.gradle.rust.benchmark

import groovy.transform.CompileStatic
import org.apache.beam.gradle.rust.BeamModuleExtras
import org.apache.beam.gradle.rust.RustConfig
import org.apache.beam.gradle.rust.RustSdkBuild
import org.apache.beam.gradle.rust.container.RustContainerTasks
import org.apache.beam.gradle.rust.runner.RustDataflowTasks
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.Configuration
import org.gradle.api.tasks.Exec
import org.gradle.api.tasks.compile.JavaCompile

/** WatermarkLatency worker build, Prism and Dataflow launchers, and Dataflow benchmark task. */
@CompileStatic
class RustWatermarkLatencyTasks {

  private static final String JAVA_MAIN_CLASS =
      'org.apache.beam.sdk.testing.watermarks.WatermarkLatency'

  private static List<String> cliFlags(RustConfig config) {
    List<String> flags = ['--streaming']
    Map<String, String> propToFlag = [
      numShuffles      : 'num_shuffles',
      inputRatePerSec  : 'input_rate_per_sec',
      numKeys          : 'num_keys',
      windowSizeSecs   : 'window_size_secs',
      summaryWindowSecs: 'summary_window_secs',
      maxReadTimeSecs  : 'max_read_time_secs',
      numEvents        : 'num_events',
      configName       : 'config_name',
    ]
    propToFlag.each { String prop, String flag ->
      String val = config.resolveProp('watermark_latency', prop)
      if (val != null && !val.isEmpty()) {
        flags.add("--${flag}=${val}".toString())
      }
    }
    return flags
  }

  static void register(
      RustContainerTasks containers,
      RustBenchmarkTasks benchmarks,
      Configuration javaBenchmark) {
    RustDataflowTasks dataflow = benchmarks.dataflow
    RustConfig config = containers.config
    RustSdkBuild build = config.build
    Project project = build.project
    File javaClasses = build.buildFile('java-benchmark/watermark_latency/classes')

    project.tasks.register('compileJavaWatermarkLatency', JavaCompile) { JavaCompile task ->
      task.group = 'Build'
      task.description =
          'Compile sdks/java/testing/watermarks WatermarkLatency against a released Java SDK'
      task.source(new File(project.rootDir,
          'sdks/java/testing/watermarks/src/main/java/org/apache/beam/sdk/testing/watermarks/WatermarkLatency.java'))
      task.classpath = javaBenchmark
      task.destinationDirectory.set(javaClasses)
      task.sourceCompatibility = '11'
      task.targetCompatibility = '11'
      task.options.release.set(11)
    }

    containers.registerWorkerTask(
        'buildWatermarkLatencyWorker',
        'Build Linux worker binary for WatermarkLatency container/Dataflow execution (-PexampleArch=amd64)',
        'watermark_latency',
        config.workerArch,
        'watermark_latency')

    project.tasks.register('watermarkLatencyPrism', Exec) { Exec task ->
      task.group = 'Application'
      task.description = 'Run the WatermarkLatency streaming pipeline on the local Prism Runner ' +
          '([-PnumShuffles=3] [-PinputRatePerSec=1000] [-PmaxReadTimeSecs=5] [-Pdocker] [-PextraArgs=<flags>])'
      task.workingDir(build.rustDir)
      task.dependsOn('fetchPrism')

      Object dockerProp = project.findProperty('docker')
      boolean dockerMode = dockerProp instanceof CharSequence && dockerProp.toString() != 'false'
      if (dockerMode && config.suppliedWorkerBinary('watermark_latency') == null) {
        task.dependsOn('buildWatermarkLatencyWorker')
      }

      task.doFirst {
        String defaultEnv = "${BeamModuleExtras.dockerDefaultRepoRoot(project)}/" +
            "${BeamModuleExtras.dockerDefaultRepoPrefix(project)}rust_sdk:${build.versions.sdkVersion}"
        String envConfig = dockerMode
            ? config.resolveProp('watermark_latency', 'environmentConfig', null, defaultEnv)
            : null
        String supplied = config.suppliedWorkerBinary('watermark_latency')
        List<String> workerFlag
        if (!dockerMode || supplied == 'none') {
          workerFlag = new ArrayList<String>()
        } else if (supplied != null) {
          workerFlag = ["--worker_binary=${supplied}".toString()]
        } else {
          workerFlag = ["--worker_binary=${config.workerBinaryFor('watermark_latency', config.workerArch).absolutePath}".toString()]
        }
        List<String> gcpArgs = config.portableGcpArgs('watermark_latency')
        List<String> envFlags = dockerMode
            ? ['--environment_type=DOCKER', "--environment_config=${envConfig}".toString()] + workerFlag
            : ['--environment_type=LOOPBACK']
        List<String> extraArgs = (config.resolveProp('watermark_latency', 'extraArgs') ?: '').tokenize(' ')
        List<String> flags = cliFlags(config)
        if (!flags.any { String f -> f.startsWith('--max_read_time_secs=') || f.startsWith('--num_events=') } &&
            !extraArgs.any { String f -> f.startsWith('--max_read_time_secs') || f.startsWith('--num_events') }) {
          flags.add('--max_read_time_secs=5')
        }
        List<String> cmd = [
          'cargo', 'run', '--release', '--bin', 'watermark_latency', '--', '--runner=PrismRunner',
        ] + envFlags + gcpArgs + flags + extraArgs

        task.environment(config.launchEnvironment())
        task.commandLine(cmd)
      }
    }

    project.tasks.register('watermarkLatencyDataflow', Exec) { Exec task ->
      task.group = 'Application'
      task.description = 'Run the WatermarkLatency streaming pipeline on Google Cloud Dataflow ' +
          '([-PnumShuffles=3] [-PinputRatePerSec=1000] [-PmaxReadTimeSecs=180] [-PextraArgs=<flags>])'
      task.workingDir(build.rustDir)
      if (config.suppliedWorkerBinary('watermark_latency') == null) {
        task.dependsOn('buildWatermarkLatencyWorker')
      }

      task.doFirst {
        task.environment(config.launchEnvironment())
        task.commandLine(dataflow.buildDataflowArgs('watermark_latency', null, [
          pkg        : 'apache-beam-watermark-latency',
          bin        : 'watermark_latency',
          skipIoFlags: true,
          extraFlags : cliFlags(config),
        ]))
      }
    }

    List<String> selectedSdks = ((project.findProperty('sdks') ?: 'rust').toString())
        .split(',')
        .collect { String s -> s.trim().toLowerCase() }
        .findAll { String s -> !s.isEmpty() }

    project.tasks.register('watermarkLatencyBenchmark') { Task task ->
      task.group = 'Verification'
      task.description = 'Run WatermarkLatency on Dataflow for Java and/or Rust and report its metrics ' +
          '([-Psdks=rust|java,rust] [-PnumShuffles=3] [-PinputRatePerSec=1000] [-PnumKeys=1000] ' +
          '[-PrunSeconds=240] [-Pruns=1] [-Pbaseline=<git ref>] [-PcompareTo=<results.json>] ' +
          '[-PnumWorkers=1] [-PmaxNumWorkers=1] [-PworkerMachineType=n2-standard-4] ' +
          '[-PminCpuPlatform=<platform>] [-PextraArgs=<pipeline flags>] [-PdryRun])'
      if (!benchmarks.isSet('dryRun') && selectedSdks.contains('java')) {
        task.dependsOn('compileJavaWatermarkLatency')
      }

      task.doLast {
        selectedSdks.each { String sdk ->
          if (!(sdk in ['java', 'rust'])) {
            throw new GradleException("-Psdks entries must be 'java' or 'rust', not '${sdk}'.")
          }
        }
        if (project.hasProperty('baseline') && selectedSdks != ['rust']) {
          throw new GradleException('-Pbaseline compares git refs of the Rust pipeline: use -Psdks=rust.')
        }
        boolean dryRun = benchmarks.isSet('dryRun')
        String gcpProject = config.gcpProjectFrom('watermark_latency')
            ?: (dryRun ? '<gcpProject>' : null)
        String tempLocation = config.resolveProp('watermark_latency', 'gcpTempLocation', 'GCP_TEMP_LOCATION')
            ?: (dryRun ? '<gcpTempLocation>' : null)
        if (!gcpProject || !tempLocation) {
          throw new GradleException(
              'Set gcpProject and gcpTempLocation in sdks/rust/.local/sdk.properties or with -P.')
        }
        String region = config.resolveProp('watermark_latency', 'gcpRegion', 'GCP_REGION', 'us-central1')
        String numShuffles = (project.findProperty('numShuffles') ?: '3').toString()
        String rate = (project.findProperty('inputRatePerSec') ?: '1000').toString()
        String numKeys = (project.findProperty('numKeys') ?: '1000').toString()
        long runSeconds = (project.findProperty('runSeconds') ?: '240').toString().toLong()
        String workers = (project.findProperty('numWorkers') ?: '1').toString()
        String maxWorkers = (project.findProperty('maxNumWorkers') ?: workers).toString()
        boolean autoscaling = maxWorkers.toInteger() > workers.toInteger()
        String algorithm = autoscaling ? 'THROUGHPUT_BASED' : 'NONE'
        String scaling = autoscaling ? 'autoscaling' : 'fixed'
        String machine = (project.findProperty('workerMachineType') ?: 'n2-standard-4').toString()
        String minCpu = project.findProperty('minCpuPlatform')?.toString()
        String experiments = benchmarks.minCpuExperiments()

        Map<String, String> commonSettings = [
          scaling    : scaling,
          workers    : workers,
          max_workers: maxWorkers,
          machine    : machine,
        ]

        List<String> props = [
          '-Pinput=none',
          '-Poutput=none',
          "-PnumWorkers=${workers}".toString(),
          "-PmaxNumWorkers=${maxWorkers}".toString(),
          "-PautoscalingAlgorithm=${algorithm}".toString(),
          "-PworkerMachineType=${machine}".toString(),
        ]
        if (!experiments.isEmpty()) {
          props.add("-Pexperiments=${experiments}".toString())
        }
        props.addAll(benchmarks.passthrough(
            RustBenchmarkTasks.DATAFLOW_PASSTHROUGH - ['autoscalingAlgorithm']))

        List<String> extraParts = [
          '--streaming',
          "--num_shuffles=${numShuffles}".toString(),
          "--input_rate_per_sec=${rate}".toString(),
          "--num_keys=${numKeys}".toString(),
          '--config_name=Rust',
        ]
        if (project.findProperty('extraArgs') != null) {
          extraParts.add(project.property('extraArgs').toString())
        }
        String rustExtraArgs = extraParts.join(' ')
        String summary = "WatermarkLatency (${selectedSdks.join(' vs ')}, ${numShuffles} shuffles, " +
            "${rate} events/s, ${runSeconds} s RUNNING), ${machine}, ${workers} to ${maxWorkers} workers" +
            (minCpu ? ", minimum CPU platform ${minCpu}" : '') + '.'

        if (selectedSdks == ['rust']) {
          benchmarks.gitRefVariants { List<Map<String, String>> checkouts ->
            List<Map<String, Object>> variants = checkouts.collect { Map<String, String> checkout ->
              Map<String, Object> v = benchmarks.rustDataflowVariant(
                  checkout, 'watermark_latency', props, rustExtraArgs)
              v['wait'] = [drain_running_after_seconds: runSeconds]
              v['settings'] = commonSettings
              return v
            }
            benchmarks.runDataflowBenchmark([
              task       : 'watermarkLatencyBenchmark',
              describe   : summary,
              workers    : workers,
              runs       : 1,
              countOutput: 'Generate',
              countName  : 'events',
              counter    : 'events',
            ], variants)
          }
          return
        }

        File javaArgs = build.buildFile('tmp/benchmarks/watermarkLatencyBenchmark-java.args')
        if (!dryRun) {
          javaArgs.parentFile.mkdirs()
          javaArgs.setText("-cp \"${(project.files(javaClasses) + javaBenchmark).asPath}\"\n", 'UTF-8')
        }
        String javaExps = (['use_runner_v2'] +
            (!experiments.isEmpty() ? [experiments] : new ArrayList<String>())).join(',')
        List<String> javaFlags = [
          '--runner=DataflowRunner',
          '--streaming=true',
          "--project=${gcpProject}".toString(),
          "--region=${region}".toString(),
          "--tempLocation=${tempLocation}".toString(),
          "--numWorkers=${workers}".toString(),
          "--maxNumWorkers=${maxWorkers}".toString(),
          "--autoscalingAlgorithm=${algorithm}".toString(),
          "--workerMachineType=${machine}".toString(),
          '--jobName={job_name}',
          "--experiments=${javaExps}".toString(),
          "--numShuffles=${numShuffles}".toString(),
          "--inputRatePerSec=${rate}".toString(),
          "--numKeys=${numKeys}".toString(),
          '--configName=Java',
        ] + dataflow.dataflowOptionFlags(
            'watermark_latency',
            'java',
            ['numWorkers', 'maxNumWorkers', 'autoscalingAlgorithm', 'workerMachineType',
             'experiments', 'sdkHarnessContainerImageOverrides'])

        Map<String, Object> javaVariant = [
          label   : (Object) 'java',
          describe: 'Java SDK 2.76.0 WatermarkLatency (Runner v2)',
          cwd     : build.rustDir.absolutePath,
          argv    : [
            "${System.getProperty('java.home')}/bin/java".toString(),
            "@${javaArgs.absolutePath}".toString(),
            JAVA_MAIN_CLASS,
          ] + javaFlags + ['--labels={labels_json}'],
          labels  : [sdk: 'java'],
          settings: commonSettings,
          wait    : [drain_running_after_seconds: runSeconds],
        ]

        Map<String, String> currentCheckout = [
          label   : 'rust',
          root    : project.rootDir.absolutePath,
          describe: "Rust SDK (${benchmarks.gitOutput(['rev-parse', '--short', 'HEAD'])})".toString(),
        ]
        Map<String, Object> rustVariant = benchmarks.rustDataflowVariant(
            currentCheckout, 'watermark_latency', props, rustExtraArgs)
        rustVariant['labels'] = [sdk: 'rust']
        rustVariant['settings'] = commonSettings
        rustVariant['wait'] = [drain_running_after_seconds: runSeconds]

        Map<String, Map<String, Object>> byName = [java: javaVariant, rust: rustVariant]
        List<Map<String, Object>> variants = selectedSdks.collect { String s -> byName[s] }
        benchmarks.runDataflowBenchmark([
          task       : 'watermarkLatencyBenchmark',
          describe   : summary,
          workers    : workers,
          runs       : 1,
          current    : variants.last()['label'],
          countOutput: 'Generate',
          countName  : 'events',
        ], variants)
      }
    }
  }
}
