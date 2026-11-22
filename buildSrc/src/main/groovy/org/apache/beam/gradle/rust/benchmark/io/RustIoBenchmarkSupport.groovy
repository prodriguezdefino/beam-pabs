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
import org.apache.beam.gradle.rust.RustConfig
import org.apache.beam.gradle.rust.RustSdkBuild
import org.apache.beam.gradle.rust.benchmark.RustBenchmarkTasks
import org.apache.beam.gradle.rust.container.RustContainerTasks
import org.apache.beam.gradle.rust.io.RustIoTasks
import org.apache.beam.gradle.rust.runner.RustDataflowTasks
import org.apache.beam.gradle.rust.xlang.RustXlangTasks
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.Configuration
import org.gradle.api.file.ConfigurableFileTree
import org.gradle.api.tasks.compile.JavaCompile

/** Shared infrastructure for Dataflow benchmarks of the I/O connectors. */
@CompileStatic
class RustIoBenchmarkSupport {

  private static final Map<String, String> COMPANION_DEFAULTS = [
    rate                : '10000',
    loadSeconds         : '300',
    idleAfterLoadSeconds: '300',
  ]

  static final List<String> IO_BENCHMARK_PACKAGES = [
    'apache-beam-benchmark-bigquery',
    'apache-beam-benchmark-bigtable',
    'apache-beam-benchmark-kafka',
    'apache-beam-benchmark-iceberg',
  ]

  final RustBenchmarkTasks benchmarks
  final RustXlangTasks xlang
  final RustIoTasks ioTasks
  final RustContainerTasks containers
  final RustDataflowTasks dataflow
  final RustConfig config
  final RustSdkBuild build
  final String benchRoot
  final File benchVenv
  final String benchBeamVersion
  final String benchPipeline
  final List<String> benchReaders
  final List<String> benchWriters
  final Configuration javaBenchmark
  final IoBenchmarkVariants variants

  private RustIoBenchmarkSupport(
      RustBenchmarkTasks benchmarks,
      RustXlangTasks xlang,
      RustIoTasks ioTasks) {
    this.benchmarks = benchmarks
    this.xlang = xlang
    this.ioTasks = ioTasks
    this.containers = xlang.containers
    this.dataflow = benchmarks.dataflow
    this.config = xlang.config
    this.build = xlang.build
    Project project = build.project

    benchRoot = config.resolveProp('benchmark', 'dataflowRepositoryRoot')
    benchVenv = build.buildFile('bench-venv')
    benchBeamVersion = (project.findProperty('benchmarkBeamVersion') ?: '2.76.0').toString()
    benchPipeline = (project.findProperty('pipeline') ?: 'python').toString()
    benchReaders = benchChoices('readers', 'reader', 'rust')
    benchWriters = benchChoices('writers', 'writer', null)
    javaBenchmark = BenchmarkEnvironments.javaClasspath(project, benchBeamVersion)
    variants = new IoBenchmarkVariants(this)
  }

  static RustIoBenchmarkSupport register(
      RustBenchmarkTasks benchmarks,
      RustXlangTasks xlang,
      RustIoTasks ioTasks) {
    RustIoBenchmarkSupport support = new RustIoBenchmarkSupport(benchmarks, xlang, ioTasks)
    support.registerSharedTasks()
    RustBigQueryBenchmark.register(support)
    RustBigtableBenchmark.register(support)
    RustKafkaBenchmark.register(support)
    RustIcebergBenchmark.register(support)
    return support
  }

  static String benchFlag(String pipeline, String name) {
    String converted = pipeline == 'java' ? RustDataflowTasks.toLowerCamel(name) : name
    return "--${converted}".toString()
  }

  String benchProp(String prop, String env = null, String fallback = null) {
    return config.resolveProp('benchmark', prop, env, fallback)
  }

  List<String> benchArgs(RunContext run, Map<String, String> propToFlag) {
    Project project = build.project
    List<String> out = []
    propToFlag.each { String prop, String flag ->
      String value = project.findProperty(prop)?.toString()
      if (value != null && !value.isEmpty()) {
        out.add("${run.flag(flag)}=${value}".toString())
      }
    }
    return out
  }

  String companionSetting(Map<String, String> defaults, String prop) {
    Object projectVal = build.project.findProperty(prop)
    if (projectVal != null) {
      return projectVal.toString()
    }
    String fromDefaults = defaults != null ? defaults[prop] : null
    return (fromDefaults != null ? fromDefaults : COMPANION_DEFAULTS[prop]).toString()
  }

  String benchSamplePath(String key) {
    String path = (build.project.findProperty('samplePath')
        ?: config.localProps.getProperty("example.benchmark.${key}"))?.toString()
    if (!path && !benchmarks.isSet('dryRun')) {
      throw new GradleException("Set -PsamplePath=<gs://...> or example.benchmark.${key} in " +
          'sdks/rust/.local/sdk.properties.')
    }
    return path ?: "<${key}>".toString()
  }

  boolean kafkaProducerRequested() {
    return ['samplePath', 'rate', 'loadSeconds', 'sharedProducer'].any { String prop ->
      build.project.hasProperty(prop)
    }
  }

  Map<String, String> sharedProducerConfig() {
    String id = build.project.findProperty('sharedProducer')?.toString()
    if (!id) {
      return null
    }
    return [
      dir  : build.buildFile("tmp/benchmarks/shared-producer/${id}").absolutePath,
      tasks: (build.project.findProperty('sharedProducerTasks') ?: '2').toString(),
    ]
  }

  void registerDataflowBenchmark(BenchmarkSpec cfg) {
    Project project = build.project
    File benchDir = new File(build.rustDir, "benchmarks/io/${cfg.dir}")
    String shape = (project.findProperty('shape') ?: (cfg.defaultShape ?: 'count')).toString()
    List<String> readShapeList = cfg.readShapes != null ? cfg.readShapes : cfg.shapes
    boolean reads = (readShapeList ?: new ArrayList<String>()).contains(shape)
    boolean writes = (cfg.writeShapes ?: new ArrayList<String>()).contains(shape)
    boolean usesRust = benchPipeline != 'rust' &&
        ((reads && benchReaders.contains('rust')) || (writes && benchWriters.contains('rust')))
    boolean hasProducer = cfg.hasProducer != null ? Boolean.TRUE == cfg.hasProducer.call(shape) : false
    boolean dryRun = benchmarks.isSet('dryRun')
    File javaClasses = build.buildFile("java-benchmark/${cfg.dir}/classes")
    File kafkaJavaClasses = build.buildFile('java-benchmark/kafka/classes')

    project.tasks.register(cfg.compileTask, JavaCompile) { JavaCompile task ->
      task.group = 'Build'
      task.description =
          "Compile the ${cfg.dir} Java benchmark against a released Java SDK ([-PbenchmarkBeamVersion=2.76.0])"
      ConfigurableFileTree javaSrc = project.fileTree(new File(benchDir, 'java'))
      javaSrc.include('**/*.java')
      task.source = javaSrc
      task.classpath = javaBenchmark
      task.destinationDirectory.set(javaClasses)
      task.sourceCompatibility = '11'
      task.targetCompatibility = '11'
      task.options.release.set(11)
    }

    project.tasks.register(cfg.task) { Task task ->
      task.group = 'Verification'
      task.description = cfg.description.replaceAll(/\)$/, ' [-Preaders=a,b] [-Pwriters=a,b] [-Pruns=1] ' +
          '[-Pbaseline=<git ref>] [-PcompareTo=<results.json>] [-PdryRun] [-PprepareOnly])')
      if (!dryRun) {
        if (benchPipeline == 'java') {
          task.dependsOn(cfg.compileTask)
        } else if (benchPipeline == 'python') {
          task.dependsOn('setupBenchmarkPythonEnv')
        }
        if (cfg.after != null && writes) {
          task.dependsOn('setupBenchmarkPythonEnv')
        }
        if (hasProducer) {
          task.dependsOn('compileKafkaJavaBenchmark')
        }
        if (usesRust) {
          task.dependsOn(cfg.expansionImageTask ?: 'gcpExpansionServiceDataflowImage')
        }
      }

      task.doLast {
        checkChoices(cfg, shape, reads, writes)
        if (project.hasProperty('baseline') && benchPipeline != 'rust') {
          throw new GradleException('-Pbaseline compares git refs of a Rust pipeline: pass -Ppipeline=rust.')
        }
        String gcpProject = config.gcpProjectFrom('benchmark') ?: (dryRun ? '<gcpProject>' : null)
        String tempLocation = benchProp('gcpTempLocation', 'GCP_TEMP_LOCATION')
            ?: (dryRun ? '<gcpTempLocation>' : null)
        if (!gcpProject || !tempLocation) {
          throw new GradleException(
              'Set gcpProject and gcpTempLocation in sdks/rust/.local/sdk.properties or with -P.')
        }
        String workers = (project.findProperty('numWorkers') ?: '4').toString()
        String maxWorkers = (project.findProperty('maxNumWorkers') ?: workers).toString()
        File javaArgs = build.buildFile("tmp/benchmarks/${cfg.task}-java.args")
        File kafkaJavaArgs = build.buildFile("tmp/benchmarks/${cfg.task}-producer-java.args")
        Map<String, Object> job = [
          shape        : (Object) shape,
          gcpProject   : gcpProject,
          tempLocation : tempLocation,
          region       : benchProp('gcpRegion', 'GCP_REGION', 'us-central1'),
          workers      : workers,
          maxWorkers   : maxWorkers,
          autoscaling  : maxWorkers.toInteger() > workers.toInteger(),
          machine      : (project.findProperty('workerMachineType') ?: 'n2-standard-4').toString(),
          runnerV1     : benchmarks.isSet('runnerV1'),
          javaArgs     : javaArgs,
          kafkaJavaArgs: kafkaJavaArgs,
          expansionPort: '<port>',
        ]
        if (benchPipeline == 'java' && !dryRun) {
          javaArgs.parentFile.mkdirs()
          javaArgs.setText("-cp \"${(project.files(javaClasses) + javaBenchmark).asPath}\"\n", 'UTF-8')
        }
        if (hasProducer && !dryRun) {
          kafkaJavaArgs.parentFile.mkdirs()
          kafkaJavaArgs.setText("-cp \"${(project.files(kafkaJavaClasses) + javaBenchmark).asPath}\"\n", 'UTF-8')
        }
        Map<String, Object> producerInput = new LinkedHashMap<>(job)
        producerInput['producerDefaults'] = cfg.producerDefaults
        Map<String, Object> producer = hasProducer && cfg.producer != null
            ? (Map<String, Object>) cfg.producer.call(producerInput) : null
        Map<String, Object> ioMap = cfg.io != null
            ? (Map<String, Object>) cfg.io.call(shape) : null
        Map<String, Object> fullIo = null
        if (ioMap != null) {
          fullIo = new LinkedHashMap<>()
          fullIo['pipeline'] = benchPipeline
          fullIo.putAll(ioMap)
        }

        List<String> readerChoices = reads ? benchReaders : [(String) null]
        List<String> writerChoices = writes ? benchWriters : [(String) null]

        if (benchPipeline == 'rust') {
          benchmarks.gitRefVariants { List<Map<String, String>> checkouts ->
            boolean multiRef = checkouts.size() > 1
            List<Map<String, Object>> variants = []
            checkouts.each { Map<String, String> checkout ->
              readerChoices.each { String reader ->
                writerChoices.each { String writer ->
                  variants.add(this.variants.benchRustVariant(cfg, job, checkout, multiRef, reader, writer, producer != null))
                }
              }
            }
            executeVariants(cfg, gcpProject, job, workers, maxWorkers, shape, fullIo, producer, variants)
          }
          return
        }

        RustXlangTasks.ServiceHandle service = usesRust && !dryRun
            ? startGcpExpansionService(
                containers.rustImageName(cfg.expansionImageName ?: 'gcp_expansion_service', benchRoot))
            : null
        try {
          if (service != null) {
            job['expansionPort'] = service.port.toString()
          }
          List<Map<String, Object>> variants = []
          readerChoices.each { String reader ->
            writerChoices.each { String writer ->
              variants.add(this.variants.benchSdkVariant(cfg, job, reader, writer, producer != null))
            }
          }
          executeVariants(cfg, gcpProject, job, workers, maxWorkers, shape, fullIo, producer, variants)
        } finally {
          if (service != null) {
            xlang.stopExpansionService(service)
          }
        }
      }
    }
  }

  private void executeVariants(
      BenchmarkSpec cfg,
      String gcpProject,
      Map<String, Object> job,
      String workers,
      String maxWorkers,
      String shape,
      Map<String, Object> fullIo,
      Map<String, Object> producer,
      List<Map<String, Object>> variants) {
    benchmarks.runDataflowBenchmark([
      task      : (Object) cfg.task,
      gcpProject: gcpProject,
      region    : job['region'],
      runs      : 1,
      describe  : "${cfg.label} ${shape}, ${benchPipeline} pipeline, ${job['machine']}, " +
          "${workers} to ${maxWorkers} workers.",
      namePrefix: "${cfg.jobPrefix}-${benchPipeline}-${shape}".toString(),
      current   : variants.last()['label'],
      workers   : workers,
      counter   : (cfg.counterShapes ?: new ArrayList<String>()).contains(shape) ? cfg.counter : null,
      io        : fullIo,
      producer  : producer,
      shared    : producer != null ? sharedProducerConfig() : null,
    ], variants)
  }

  private void registerSharedTasks() {
    Project project = build.project
    BenchmarkEnvironments.registerPythonEnv(project, benchVenv, benchBeamVersion)

    project.tasks.register('buildIoBenchmarkWorkers') { Task task ->
      task.group = 'Build'
      task.description =
          'Build all four Linux I/O benchmark worker binaries in one Cargo invocation (-PexampleArch=amd64)'
      task.inputs.files(config.rustWorkspaceTree)
      task.inputs.property('packages', IO_BENCHMARK_PACKAGES)
      task.inputs.property('arch', config.workerArch)
      task.inputs.property('sdkVersion', build.versions.sdkVersion)
      task.inputs.property('targetCpu', project.findProperty('workerTargetCpu') ?: 'default')
      containers.registerPgoInputs(task)
      task.outputs.files(IO_BENCHMARK_PACKAGES.collect { String pkg ->
        String binName = (build.workspace.crateBinTargets[pkg]?.find() ?: pkg).toString()
        config.workerBinaryFor(binName, config.workerArch)
      })
      task.doLast {
        // Link one fat-LTO binary at a time so concurrent linkers do not exhaust Docker memory.
        IO_BENCHMARK_PACKAGES.each { String pkg ->
          String binName = (build.workspace.crateBinTargets[pkg]?.find() ?: pkg).toString()
          containers.compileWorkers([binName], config.workerArch, build.versions.sdkVersion)
        }
      }
    }
  }

  private List<String> benchChoices(String plural, String single, String fallback) {
    Project project = build.project
    Object raw = project.findProperty(plural) ?: project.findProperty(single) ?: fallback
    if (raw == null) {
      return []
    }
    return raw.toString()
        .tokenize(',')
        .collect { String item -> item.trim() }
        .findAll { String item -> !item.isEmpty() }
        .unique()
  }

  private RustXlangTasks.ServiceHandle startGcpExpansionService(String image) {
    String credentials = config.googleCredentials()
    if (!credentials) {
      throw new GradleException(
          'No Application Default Credentials. Run gcloud auth application-default login.')
    }
    return xlang.startExpansionService([
      image      : image,
      platform   : 'linux/amd64',
      credentials: credentials,
    ])
  }

  private void checkChoices(BenchmarkSpec cfg, String shape, boolean reads, boolean writes) {
    Project project = build.project
    List<String> pipelines = ['python', 'java'] + (cfg.rustPackage ? ['rust'] : new ArrayList<String>())
    if (!pipelines.contains(benchPipeline)) {
      throw new GradleException("Unknown pipeline '${benchPipeline}': use ${pipelines.join(', ')}.")
    }
    if (cfg.shapes && !cfg.shapes.contains(shape)) {
      throw new GradleException("Unknown shape '${shape}': use ${cfg.shapes.join(', ')}.")
    }
    if (benchPipeline == 'rust' && !(cfg.rustShapes ?: new ArrayList<String>()).contains(shape)) {
      throw new GradleException("The Rust pipeline runs only ${(cfg.rustShapes ?: []).join(', ')}, " +
          "not shape '${shape}'.")
    }
    checkRoleChoice(project, shape, reads, 'reader', benchReaders, cfg.readers,
        cfg.readShapes != null ? cfg.readShapes : cfg.shapes)
    checkRoleChoice(project, shape, writes, 'writer', benchWriters, cfg.writers, cfg.writeShapes)
  }

  private void checkRoleChoice(
      Project project,
      String shape,
      boolean used,
      String noun,
      List<String> chosen,
      Map<String, List<String>> knownMap,
      List<String> roleShapes) {
    List<String> knownList = (knownMap ?: [:])[benchPipeline] ?: new ArrayList<String>()
    String valid = knownList.join(', ')
    if (!used && (project.hasProperty(noun) || project.hasProperty("${noun}s"))) {
      throw new GradleException("Shape '${shape}' does not use a ${noun}, so it takes no " +
          "-P${noun} or -P${noun}s. Use a shape that does (${(roleShapes ?: []).join(', ')}) or remove the property.")
    }
    if (used && chosen.isEmpty()) {
      throw new GradleException("Shape '${shape}' needs -P${noun} or -P${noun}s. Use ${valid}.")
    }
    if (used) {
      chosen.findAll { String item -> !knownList.contains(item) }.each { String item ->
        throw new GradleException("Unknown ${noun} '${item}' for a ${benchPipeline} pipeline: use ${valid}.")
      }
    }
  }

}
