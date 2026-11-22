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
import java.util.regex.Matcher
import org.apache.beam.gradle.rust.RustConfig
import org.apache.beam.gradle.rust.RustSdkBuild
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.tasks.Exec

/** Google Cloud Dataflow runner configuration, option formatting and example launcher. */
@CompileStatic
class RustDataflowTasks {

  private static class OptionSpec {
    String prop
    String env
    String flag
  }

  private static final List<OptionSpec> OPTION_TABLE = [
    new OptionSpec(prop: 'subnetwork', env: 'SUBNETWORK', flag: 'subnetwork'),
    new OptionSpec(prop: 'network', env: 'NETWORK', flag: 'network'),
    new OptionSpec(prop: 'serviceAccountEmail', env: 'SERVICE_ACCOUNT_EMAIL', flag: 'service_account_email'),
    new OptionSpec(prop: 'numWorkers', env: 'NUM_WORKERS', flag: 'num_workers'),
    new OptionSpec(prop: 'maxNumWorkers', env: 'MAX_NUM_WORKERS', flag: 'max_num_workers'),
    new OptionSpec(prop: 'autoscalingAlgorithm', env: 'AUTOSCALING_ALGORITHM', flag: 'autoscaling_algorithm'),
    new OptionSpec(prop: 'workerMachineType', env: 'WORKER_MACHINE_TYPE', flag: 'worker_machine_type'),
    new OptionSpec(prop: 'diskSizeGb', env: 'DISK_SIZE_GB', flag: 'disk_size_gb'),
    new OptionSpec(prop: 'workerDiskType', env: 'WORKER_DISK_TYPE', flag: 'disk_type'),
    new OptionSpec(prop: 'dataflowServiceOptions', env: 'DATAFLOW_SERVICE_OPTIONS', flag: 'dataflow_service_options'),
    new OptionSpec(prop: 'experiments', env: 'EXPERIMENTS', flag: 'experiments'),
    new OptionSpec(prop: 'numberOfWorkerHarnessThreads', env: 'WORKER_HARNESS_THREADS', flag: 'number_of_worker_harness_threads'),
    new OptionSpec(prop: 'sdkHarnessContainerImageOverrides', env: 'SDK_HARNESS_CONTAINER_IMAGE_OVERRIDES', flag: 'sdk_harness_container_image_overrides'),
  ]

  final RustConfig config
  final RustSdkBuild build

  private RustDataflowTasks(RustConfig config) {
    this.config = config
    this.build = config.build
  }

  static RustDataflowTasks register(RustConfig config) {
    RustDataflowTasks dataflow = new RustDataflowTasks(config)
    dataflow.registerLauncherTask()
    return dataflow
  }

  static String optionName(String sdk, String flag) {
    if (sdk == 'java') {
      if (flag == 'service_account_email') {
        return 'serviceAccount'
      }
      if (flag == 'disk_type') {
        return 'workerDiskType'
      }
      return toLowerCamel(flag)
    }
    if (sdk == 'python') {
      if (flag == 'worker_machine_type') {
        return 'machine_type'
      }
      if (flag == 'disk_type') {
        return 'worker_disk_type'
      }
    }
    return flag
  }

  static String toLowerCamel(String snake) {
    Matcher m = (snake =~ /_([a-z])/)
    StringBuffer sb = new StringBuffer()
    while (m.find()) {
      m.appendReplacement(sb, m.group(1).toUpperCase())
    }
    m.appendTail(sb)
    return sb.toString()
  }

  void configureDataflowEnvironment(Exec task) {
    String scope = 'dataflow'
    String gcpProject = config.gcpProjectFrom(scope)
    String gcpTempLocation = config.resolveProp(scope, 'gcpTempLocation', 'GCP_TEMP_LOCATION')
    String sdkContainerImage = config.resolveProp(scope, 'sdkContainerImage', 'SDK_CONTAINER_IMAGE')
    if (!gcpProject || !gcpTempLocation || !sdkContainerImage) {
      throw new GradleException(
          "${task.name} requires gcpProject, gcpTempLocation, and sdkContainerImage. " +
          'Configure them in sdks/rust/.local/sdk.properties or pass with -P ' +
          '(e.g. -PgcpProject=... -PgcpTempLocation=... -PsdkContainerImage=...)')
    }
    String workerBinary = config.resolveProp(scope, 'workerBinary', 'WORKER_BINARY')
        ?: config.workerBinaryFor('beam-runner-tests-worker', config.workerArch).absolutePath
    Map<String, String> env = [
      BEAM_DATAFLOW_PROJECT              : gcpProject,
      BEAM_DATAFLOW_REGION               : config.resolveProp(scope, 'gcpRegion', 'GCP_REGION', 'us-central1'),
      BEAM_DATAFLOW_TEMP_LOCATION        : gcpTempLocation,
      BEAM_DATAFLOW_SDK_CONTAINER_IMAGE  : sdkContainerImage,
      BEAM_DATAFLOW_WORKER_BINARY        : workerBinary,
      BEAM_DATAFLOW_NETWORK              : config.resolveProp(scope, 'network', 'NETWORK'),
      BEAM_DATAFLOW_SUBNETWORK           : config.resolveProp(scope, 'subnetwork', 'SUBNETWORK'),
      BEAM_DATAFLOW_SERVICE_ACCOUNT_EMAIL: config.resolveProp(scope, 'serviceAccountEmail', 'SERVICE_ACCOUNT_EMAIL'),
    ]
    env.each { String name, String value ->
      if (value != null && !value.isEmpty()) {
        task.environment(name, value)
      }
    }
  }

  List<String> dataflowOptionFlags(String scope, String sdk = 'rust', Collection<String> skip = []) {
    List<String> flags = []
    OPTION_TABLE.each { OptionSpec opt ->
      if (opt.prop in skip) {
        return
      }
      String val = opt.prop == 'serviceAccountEmail'
          ? (config.resolveProp(scope, 'serviceAccount') ?: config.resolveProp(scope, opt.prop, opt.env))
          : config.resolveProp(scope, opt.prop, opt.env)
      if (val != null && !val.isEmpty()) {
        flags.add("--${optionName(sdk, opt.flag)}=${val}".toString())
      }
    }
    if (!('usePublicIps' in skip) &&
        config.resolveProp(scope, 'usePublicIps', 'USE_PUBLIC_IPS') == 'false') {
      flags.add(sdk == 'java' ? '--usePublicIps=false' : '--no_use_public_ips')
    }
    return flags
  }

  @SuppressWarnings('unchecked')
  List<String> buildDataflowArgs(String example, File workerBin = null, Map<String, Object> opts = [:]) {
    Project project = build.project
    String gcpProject = config.gcpProjectFrom(example)
    String gcpRegion = config.resolveProp(example, 'gcpRegion', 'GCP_REGION', 'us-central1')
    String gcpTempLocation = config.resolveProp(example, 'gcpTempLocation', 'GCP_TEMP_LOCATION')
    String sdkContainerImage = config.resolveProp(example, 'sdkContainerImage', 'SDK_CONTAINER_IMAGE')

    if (!gcpProject || !gcpTempLocation) {
      throw new GradleException("Missing required GCP parameters for example '${example}'!\n" +
          'Provide -PgcpProject=<project> and -PgcpTempLocation=gs://<bucket>/tmp on the CLI ' +
          "or in sdks/rust/.local/sdk.properties (globally or under example.${example}.*).")
    }

    String supplied = config.suppliedWorkerBinary(example)
    String workerBinPath = supplied == 'none' ? null
        : (supplied ?: (workerBin ?: config.workerBinaryFor(example, config.workerArch)).absolutePath)

    List<String> args = [
      '--runner=DataflowRunner',
      "--project=${gcpProject}".toString(),
      "--region=${gcpRegion}".toString(),
      "--temp_location=${gcpTempLocation}".toString(),
    ]
    if (workerBinPath != null) {
      args.add("--worker_binary=${workerBinPath}".toString())
    }
    if (sdkContainerImage != null && !sdkContainerImage.isEmpty()) {
      args.add("--sdk_container_image=${sdkContainerImage}".toString())
    }

    args.addAll(dataflowOptionFlags(example, 'rust'))

    if (Boolean.TRUE != opts.skipIoFlags) {
      String inputVal = config.resolveProp(example, 'input')
      if (inputVal != null && !inputVal.isEmpty() && inputVal != 'none') {
        args.add("--input=${inputVal}".toString())
      }
      String outputVal = config.resolveProp(example, 'output')
      if (outputVal != 'none') {
        String baseTemp = gcpTempLocation.replaceAll('/+$', '')
        String effectiveOutput = (outputVal != null && !outputVal.isEmpty())
            ? outputVal
            : "${baseTemp}/output/${example}-${System.currentTimeMillis()}/out".toString()
        args.add("--output=${effectiveOutput}".toString())
      }
    }

    String extraArgsFile = config.resolveProp(example, 'extraArgsFile')
    if (extraArgsFile != null && !extraArgsFile.isEmpty()) {
      args.addAll(project.file(extraArgsFile).readLines('UTF-8').findAll { String l -> !l.isEmpty() })
    }
    List<Object> extraFlags = (List<Object>) (opts.extraFlags ?: new ArrayList<Object>())
    extraFlags.each { Object f -> args.add(f.toString()) }
    String extraRaw = config.resolveProp(example, 'extraArgs') ?: ''
    args.addAll(extraRaw.tokenize(' '))

    String pkg = (opts.pkg ?: example).toString()
    String binName = (opts.bin ?: build.workspace.crateBinTargets[pkg]?.find() ?: pkg).toString()
    String features = (config.resolveProp(example, 'exampleFeatures') ?: '').trim()
    List<String> featureFlags = features.isEmpty() ? new ArrayList<String>() : ['--features', features]
    Object cargoProfileProp = project.findProperty('cargoProfile')
    List<String> profileFlags = cargoProfileProp != null
        ? ['--profile', cargoProfileProp.toString()] : new ArrayList<String>()
    List<String> scopeFlags = featureFlags.isEmpty()
        ? ['--bin', binName]
        : ['-p', pkg, '--bin', binName] + featureFlags

    return ['cargo', 'run'] + profileFlags + scopeFlags + ['--'] + args
  }

  private void registerLauncherTask() {
    build.project.tasks.register('dataflow', Exec) { Exec task ->
      task.group = 'Application'
      task.description = 'Run a Rust example on Google Cloud Dataflow (-Pexample=wordcount)'
      task.workingDir(build.rustDir)
      if (config.suppliedWorkerBinary(config.selectedExample) == null) {
        task.dependsOn('buildWorker')
      }
      task.doFirst {
        task.environment(config.launchEnvironment())
        task.commandLine(buildDataflowArgs(config.selectedExample))
      }
    }
  }
}
