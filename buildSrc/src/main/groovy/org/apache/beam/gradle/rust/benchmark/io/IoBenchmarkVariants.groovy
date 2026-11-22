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
import org.apache.beam.gradle.rust.benchmark.RustBenchmarkTasks
import org.gradle.api.GradleException
import org.gradle.api.Project

/** The variants of an I/O benchmark run: the launch command, labels and wait of each job. */
@CompileStatic
class IoBenchmarkVariants {

  final RustIoBenchmarkSupport support

  IoBenchmarkVariants(RustIoBenchmarkSupport support) {
    this.support = support
  }

  @SuppressWarnings('unchecked')
  Map<String, Object> kafkaProducerVariant(
      Map<String, Object> job,
      File kafkaJavaArgs,
      Map<String, Object> opts) {
    String samplePath = support.benchSamplePath(opts['sampleKey'].toString())
    Map<String, String> defaults = (Map<String, String>) job['producerDefaults']
    String rate = support.companionSetting(defaults, 'rate')
    long loadSeconds = support.companionSetting(defaults, 'loadSeconds').toLong()
    String producerWorkers = (support.build.project.findProperty('producerWorkers') ?: '4').toString()
    Map<String, Object> producerJob = new LinkedHashMap<>(job)
    producerJob['shape'] = 'produce'
    producerJob['workers'] = producerWorkers
    producerJob['maxWorkers'] = producerWorkers
    producerJob['autoscaling'] = false
    producerJob['runnerV1'] = false

    String format = (opts['format'] ?: 'JSON').toString()
    List<String> args = [
      "--bootstrapServers=${opts['bootstrapServers']}".toString(),
      "--topic=${opts['topic']}".toString(),
      "--format=${format}".toString(),
      "--rate=${rate}".toString(),
      "--samplePath=${samplePath}".toString(),
    ] + jobFlags('java', producerJob, 'producerExtraArgs')

    List<String> argv = [
      "${System.getProperty('java.home')}/bin/java".toString(),
      "@${kafkaJavaArgs.absolutePath}".toString(),
      'KafkaBenchmark',
    ] + args + ['--labels={labels_json}']

    return [
      label   : (Object) 'producer',
      cwd     : support.build.rustDir.toString(),
      describe: "Kafka producer (${rate} records/s for ${loadSeconds} s)".toString(),
      labels  : [pipeline: 'java', shape: 'produce', rate: rate, format: format.toLowerCase()],
      argv    : argv,
      wait    : [counter: 'rows', namespace: 'produce', drain_after_seconds: loadSeconds],
    ]
  }

  Map<String, Object> waitSpec(BenchmarkSpec cfg, String shape, boolean hasProducer) {
    Project project = support.build.project
    if (support.benchmarks.isSet('detach')) {
      return [detach: (Object) true]
    }
    boolean hasCounter = (cfg.counterShapes ?: new ArrayList<String>()).contains(shape)
    Long idleSeconds = hasProducer
        ? support.companionSetting(cfg.producerDefaults, 'idleAfterLoadSeconds').toLong() : null
    Map<String, Object> raw = [
      counter                     : (Object) (hasCounter ? cfg.counter : null),
      expected_rows               : hasCounter ? project.findProperty('expectedRows')?.toString()?.toLong() : null,
      drain_after_seconds         : project.findProperty('drainAfterSeconds')?.toString()?.toLong(),
      drain_after_producer_seconds: idleSeconds,
      kill_worker_after_seconds   : project.findProperty('killWorkerAfterSeconds')?.toString()?.toLong(),
    ]
    Map<String, Object> out = [:]
    raw.each { String k, Object v ->
      if (v != null) {
        out[k] = v
      }
    }
    return out
  }

  List<String> jobFlags(String pipeline, Map<String, Object> job, String extraArgsProp = 'extraArgs') {
    String experiments = support.benchmarks.minCpuExperiments()
    boolean autoscaling = Boolean.TRUE == job['autoscaling']
    boolean runnerV1 = Boolean.TRUE == job['runnerV1']
    List<String> flags = [
      "--shape=${job['shape']}".toString(),
      '--runner=DataflowRunner',
      "--project=${job['gcpProject']}".toString(),
      "--region=${job['region']}".toString(),
      "${RustIoBenchmarkSupport.benchFlag(pipeline, 'temp_location')}=${job['tempLocation']}".toString(),
      "${RustIoBenchmarkSupport.benchFlag(pipeline, 'num_workers')}=${job['workers']}".toString(),
      "${RustIoBenchmarkSupport.benchFlag(pipeline, 'max_num_workers')}=${job['maxWorkers']}".toString(),
      "${RustIoBenchmarkSupport.benchFlag(pipeline, 'autoscaling_algorithm')}=${autoscaling ? 'THROUGHPUT_BASED' : 'NONE'}".toString(),
      "${RustIoBenchmarkSupport.benchFlag(pipeline, pipeline == 'python' ? 'machine_type' : 'worker_machine_type')}=${job['machine']}".toString(),
      "${RustIoBenchmarkSupport.benchFlag(pipeline, 'job_name')}={job_name}".toString(),
    ]
    if (pipeline == 'java') {
      String v2 = runnerV1 ? 'disable_runner_v2' : 'use_runner_v2'
      String allExperiments = ([v2] + (!experiments.isEmpty() ? [experiments] : new ArrayList<String>())).join(',')
      flags.add("--experiments=${allExperiments}".toString())
    } else {
      if (!experiments.isEmpty()) {
        flags.add("--experiments=${experiments}".toString())
      }
      if (pipeline == 'python') {
        flags.add("--billing_project=${job['gcpProject']}".toString())
      }
    }
    flags.addAll(support.dataflow.dataflowOptionFlags(
        'benchmark',
        pipeline,
        ['numWorkers', 'maxNumWorkers', 'autoscalingAlgorithm', 'workerMachineType',
         'experiments', 'sdkHarnessContainerImageOverrides']))
    flags.addAll(support.build.project.findProperty(extraArgsProp)?.toString()?.tokenize(' ') ?: new ArrayList<String>())
    return flags
  }

  Map<String, Object> sdkLaunchCommand(
      BenchmarkSpec cfg,
      String pipeline,
      List<String> args,
      File javaArgs) {
    if (pipeline == 'java') {
      List<String> argv = [
        "${System.getProperty('java.home')}/bin/java".toString(),
        "@${javaArgs.absolutePath}".toString(),
        cfg.mainClass,
      ] + args + ['--labels={labels_json}']
      return [argv: (Object) argv]
    }
    String script = "${support.build.rustDir.absolutePath}/benchmarks/io/${cfg.dir}/python/${cfg.pythonScript}"
    List<String> argv = ["${support.benchVenv.absolutePath}/bin/python".toString(), script] + args + ['{labels_args}']
    return [argv: (Object) argv]
  }

  @SuppressWarnings('unchecked')
  Map<String, Object> buildVariantMeta(
      BenchmarkSpec cfg,
      Map<String, Object> job,
      String reader,
      String writer,
      boolean hasProducer) {
    String pipeline = support.benchPipeline
    RunContext run = new RunContext(
        shape: job['shape'].toString(),
        reads: reader != null,
        writes: writer != null,
        pipeline: pipeline,
        reader: reader,
        writer: writer,
        gcpProject: job['gcpProject'].toString(),
        region: job['region'].toString(),
        tempLocation: job['tempLocation'].toString())
    boolean usesRust = pipeline != 'rust' && (reader == 'rust' || writer == 'rust')
    boolean runnerV1 = Boolean.TRUE == job['runnerV1']
    if (runnerV1 && (pipeline != 'java' || usesRust)) {
      throw new GradleException('-PrunnerV1 applies only to a Java pipeline without Rust transforms: ' +
          'Rust transforms run on Runner v2.')
    }
    Map<String, Object> spec = cfg.configure != null
        ? (Map<String, Object>) cfg.configure.call(run)
        : new LinkedHashMap<String, Object>()
    String readerLabel = (spec['readerLabel'] ?: reader)?.toString()
    String label = (reader != null && writer != null
        ? "${readerLabel}-to-${writer}" : (writer ?: readerLabel)).toString()

    Map<String, String> roles = [:]
    if (reader != null) {
      roles['reader'] = readerLabel
    }
    if (writer != null) {
      roles['writer'] = writer
    }
    Map<String, String> specLabels = (Map<String, String>) (spec['labels'] ?: [:])
    Map<String, String> labels = [:]
    labels['pipeline'] = pipeline
    labels.putAll(roles)
    labels['shape'] = run.shape
    labels['scaling'] = Boolean.TRUE == job['autoscaling'] ? 'autoscaling' : 'fixed'
    labels.putAll(specLabels)
    if (pipeline == 'java') {
      labels['runner'] = runnerV1 ? 'v1' : 'v2'
    }

    Map<String, String> specSettings = (Map<String, String>) (spec['settings'] ?: [:])
    Map<String, String> settings = new LinkedHashMap<>()
    settings['pipeline'] = pipeline
    settings['shape'] = run.shape
    settings['scaling'] = Boolean.TRUE == job['autoscaling'] ? 'autoscaling' : 'fixed'
    settings['workers'] = job['workers'].toString()
    settings['max_workers'] = job['maxWorkers'].toString()
    settings['machine'] = job['machine'].toString()
    settings.putAll(specSettings)

    Map<String, String> descMap = [:]
    descMap.putAll(roles)
    descMap.putAll(specLabels)
    String describe = descMap.collect { String k, String v -> "${k} ${v}".toString() }.join(', ')

    List<String> afterCmd = (cfg.after != null && writer != null)
        ? ((Iterable<Object>) cfg.after.call(spec)).collect { Object it -> it.toString() }
        : null

    return [
      spec    : (Object) spec,
      usesRust: usesRust,
      label   : label,
      labels  : labels,
      settings: settings,
      describe: describe,
      wait    : waitSpec(cfg, run.shape, hasProducer),
      after   : afterCmd,
    ]
  }

  @SuppressWarnings('unchecked')
  Map<String, Object> benchSdkVariant(
      BenchmarkSpec cfg,
      Map<String, Object> job,
      String reader,
      String writer,
      boolean hasProducer) {
    String pipeline = support.benchPipeline
    Map<String, Object> meta = buildVariantMeta(cfg, job, reader, writer, hasProducer)
    Map<String, Object> spec = (Map<String, Object>) meta['spec']
    List<String> specArgs = ((Iterable<Object>) (spec['args'] ?: [])).collect { Object it -> it.toString() }
    boolean usesRust = Boolean.TRUE == meta['usesRust']

    List<String> args = []
    if (reader != null) {
      args.add("--reader=${reader}".toString())
    }
    if (writer != null) {
      args.add("--writer=${writer}".toString())
    }
    args.addAll(jobFlags(pipeline, job))
    args.addAll(specArgs)
    if (usesRust) {
      args.add("${RustIoBenchmarkSupport.benchFlag(pipeline, 'expansion_service')}=localhost:${job['expansionPort']}".toString())
    }

    Map<String, Object> variant = [
      label   : meta['label'],
      cwd     : support.build.rustDir.toString(),
      labels  : meta['labels'],
      settings: meta['settings'],
      describe: meta['describe'],
      wait    : meta['wait'],
    ]
    variant.putAll(sdkLaunchCommand(cfg, pipeline, args, (File) job['javaArgs']))
    if (meta['after'] != null) {
      variant['after'] = meta['after']
    }
    return variant
  }

  @SuppressWarnings('unchecked')
  Map<String, Object> benchRustVariant(
      BenchmarkSpec cfg,
      Map<String, Object> job,
      Map<String, String> checkout,
      boolean multiRef,
      String reader,
      String writer,
      boolean hasProducer) {
    Map<String, Object> meta = buildVariantMeta(cfg, job, reader, writer, hasProducer)
    Map<String, Object> spec = (Map<String, Object>) meta['spec']
    List<String> specArgs = ((Iterable<Object>) (spec['args'] ?: [])).collect { Object it -> it.toString() }
    String variantLabel = (multiRef ? "${checkout['label']}-${meta['label']}" : meta['label']).toString()

    List<String> rawFlags = ["--shape=${job['shape']}".toString()]
    if (reader != null) {
      rawFlags.add("--reader=${reader}".toString())
    }
    if (writer != null) {
      rawFlags.add("--writer=${writer}".toString())
    }
    rawFlags.addAll(specArgs)
    rawFlags.addAll(support.build.project.findProperty('extraArgs')?.toString()?.tokenize(' ') ?: new ArrayList<String>())

    List<String> staticFlags = rawFlags.findAll { String f -> !f.contains('{') }
    List<String> templateFlags = rawFlags.findAll { String f -> f.contains('{') }

    File argsFile = support.build.buildFile("tmp/benchmarks/${cfg.task}-${variantLabel}-rust.args")
    argsFile.parentFile.mkdirs()
    argsFile.setText(staticFlags.join('\n') + '\n', 'UTF-8')

    String experiments = support.benchmarks.minCpuExperiments()
    List<String> forwarded = RustBenchmarkTasks.DATAFLOW_PASSTHROUGH.findAll { String key ->
      !(key in ['gcpProject', 'gcpRegion', 'gcpTempLocation', 'autoscalingAlgorithm'])
    }.collectMany { String key ->
      String val = key == 'serviceAccountEmail'
          ? (support.benchProp('serviceAccount') ?: support.benchProp(key, 'SERVICE_ACCOUNT_EMAIL'))
          : support.benchProp(key)
      val != null && !val.isEmpty() ? ["-P${key}=${val}".toString()] : new ArrayList<String>()
    }

    boolean autoscaling = Boolean.TRUE == job['autoscaling']
    List<String> props = [
      "-PgcpProject=${job['gcpProject']}".toString(),
      "-PgcpRegion=${job['region']}".toString(),
      "-PgcpTempLocation=${job['tempLocation']}".toString(),
      "-PnumWorkers=${job['workers']}".toString(),
      "-PmaxNumWorkers=${job['maxWorkers']}".toString(),
      "-PautoscalingAlgorithm=${autoscaling ? 'THROUGHPUT_BASED' : 'NONE'}".toString(),
      "-PworkerMachineType=${job['machine']}".toString(),
      '-Pinput=none',
      '-Poutput=none',
      "-PextraArgsFile=${argsFile.absolutePath}".toString(),
    ]
    if (!experiments.isEmpty()) {
      props.add("-Pexperiments=${experiments}".toString())
    }
    props.addAll(forwarded)

    Map<String, Object> base = support.benchmarks.rustDataflowVariant(
        checkout, cfg.rustPackage, props, templateFlags.join(' '))
    Map<String, String> labels = new LinkedHashMap<>((Map<String, String>) meta['labels'])
    if (multiRef) {
      labels['ref'] = checkout['label']
    }
    base['label'] = variantLabel
    base['labels'] = labels
    base['settings'] = meta['settings']
    base['describe'] = multiRef
        ? "${checkout['describe']}, ${meta['describe']}".toString()
        : meta['describe']
    base['wait'] = meta['wait']
    if (meta['after'] != null) {
      base['after'] = meta['after']
    }
    return base
  }
}
