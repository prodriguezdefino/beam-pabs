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
import org.gradle.api.Action
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.repositories.MavenArtifactRepository
import org.gradle.api.attributes.AttributeCompatibilityRule
import org.gradle.api.attributes.AttributeContainer
import org.gradle.api.attributes.AttributeMatchingStrategy
import org.gradle.api.attributes.AttributesSchema
import org.gradle.api.attributes.Bundling
import org.gradle.api.attributes.Category
import org.gradle.api.attributes.CompatibilityCheckDetails
import org.gradle.api.attributes.LibraryElements
import org.gradle.api.attributes.Usage
import org.gradle.api.attributes.java.TargetJvmEnvironment
import org.gradle.api.file.ConfigurableFileTree
import org.gradle.api.model.ObjectFactory
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.process.ExecSpec

/** Shared infrastructure for Dataflow benchmarks of the I/O connectors. */
@CompileStatic
class RustIoBenchmarkSupport {

  /** Accepts jars published only with a shadowed variant in the external javaBenchmark classpath. */
  static class ShadowedIsExternal implements AttributeCompatibilityRule<Bundling> {
    @Override
    void execute(CompatibilityCheckDetails<Bundling> details) {
      if (details.consumerValue?.name == Bundling.EXTERNAL &&
          details.producerValue?.name == Bundling.SHADOWED) {
        details.compatible()
      }
    }
  }

  /** Context passed to each benchmark's {@code configure} closure. */
  static class RunContext {
    String shape
    boolean reads
    boolean writes
    String pipeline
    String reader
    String writer
    String gcpProject
    String region
    String tempLocation

    String flag(String name) {
      return benchFlag(pipeline, name)
    }
  }

  /** Definition of one I/O Dataflow benchmark task. */
  static class BenchmarkSpec {
    String task
    String compileTask
    String dir
    String label
    String jobPrefix
    String pythonScript
    String mainClass
    String counter
    List<String> counterShapes = []
    Closure<Map<String, Object>> io
    String rustPackage
    List<String> rustShapes = []
    String expansionImageTask
    String expansionImageName
    String defaultShape = 'count'
    List<String> shapes = []
    List<String> readShapes
    List<String> writeShapes = []
    Map<String, List<String>> readers = [:]
    Map<String, List<String>> writers = [:]
    Closure<Boolean> hasProducer
    Map<String, String> producerDefaults = [:]
    Closure<Map<String, Object>> producer
    String description
    Closure<Map<String, Object>> configure
    Closure<List<String>> after
  }

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
    javaBenchmark = configureJavaBenchmark(project)
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

  @SuppressWarnings('unchecked')
  Map<String, Object> kafkaProducerVariant(
      Map<String, Object> job,
      File kafkaJavaArgs,
      Map<String, Object> opts) {
    String samplePath = benchSamplePath(opts['sampleKey'].toString())
    Map<String, String> defaults = (Map<String, String>) job['producerDefaults']
    String rate = companionSetting(defaults, 'rate')
    long loadSeconds = companionSetting(defaults, 'loadSeconds').toLong()
    String producerWorkers = (build.project.findProperty('producerWorkers') ?: '4').toString()
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
      cwd     : build.rustDir.toString(),
      describe: "Kafka producer (${rate} records/s for ${loadSeconds} s)".toString(),
      labels  : [pipeline: 'java', shape: 'produce', rate: rate, format: format.toLowerCase()],
      argv    : argv,
      wait    : [counter: 'rows', namespace: 'produce', drain_after_seconds: loadSeconds],
    ]
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
                  variants.add(benchRustVariant(cfg, job, checkout, multiRef, reader, writer, producer != null))
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
              variants.add(benchSdkVariant(cfg, job, reader, writer, producer != null))
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

  private Configuration configureJavaBenchmark(Project project) {
    project.repositories.maven({ MavenArtifactRepository repo ->
      repo.url = project.uri('https://packages.confluent.io/maven/')
    } as Action<MavenArtifactRepository>)

    ObjectFactory objects = project.objects
    Configuration cfg = project.configurations.create('javaBenchmark') { Configuration c ->
      c.canBeConsumed = false
      c.attributes({ AttributeContainer attrs ->
        attrs.attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage, Usage.JAVA_RUNTIME))
        attrs.attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category, Category.LIBRARY))
        attrs.attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE,
            objects.named(LibraryElements, LibraryElements.JAR))
        attrs.attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling, Bundling.EXTERNAL))
        attrs.attribute(TargetJvmEnvironment.TARGET_JVM_ENVIRONMENT_ATTRIBUTE,
            objects.named(TargetJvmEnvironment, TargetJvmEnvironment.STANDARD_JVM))
      } as Action<AttributeContainer>)
    }

    project.dependencies.attributesSchema({ AttributesSchema schema ->
      schema.attribute(Bundling.BUNDLING_ATTRIBUTE, { AttributeMatchingStrategy<Bundling> strategy ->
        strategy.compatibilityRules.add(ShadowedIsExternal)
      } as Action<AttributeMatchingStrategy<Bundling>>)
    } as Action<AttributesSchema>)

    [
      'beam-sdks-java-core',
      'beam-sdks-java-managed',
      'beam-sdks-java-io-google-cloud-platform',
      'beam-sdks-java-io-kafka',
      'beam-sdks-java-io-iceberg',
      'beam-runners-google-cloud-dataflow-java',
    ].each { String artifact ->
      project.dependencies.add('javaBenchmark', "org.apache.beam:${artifact}:${benchBeamVersion}".toString())
    }
    project.dependencies.add('javaBenchmark', 'org.apache.iceberg:iceberg-gcp:1.11.0')
    project.dependencies.add('javaBenchmark', 'org.apache.kafka:kafka-clients:3.9.2')
    project.dependencies.add('javaBenchmark', 'com.github.erosb:everit-json-schema:1.14.2')
    project.dependencies.add('javaBenchmark', 'org.apache.arrow:arrow-memory-netty:17.0.0')
    project.dependencies.add('javaBenchmark', 'junit:junit:4.13.2')
    project.dependencies.add('javaBenchmark', 'org.hamcrest:hamcrest:2.1')
    project.dependencies.add('javaBenchmark', 'org.slf4j:slf4j-jdk14:2.0.16')
    return cfg
  }

  private void registerSharedTasks() {
    Project project = build.project
    File benchStamp = new File(benchVenv, 'beam-version')
    boolean refreshBenchVenv = !benchStamp.exists() || benchStamp.getText('UTF-8').trim() != benchBeamVersion

    project.tasks.register('setupBenchmarkPythonEnv') { Task task ->
      task.group = 'Build'
      task.description = 'Create a virtualenv with a released Python SDK for the Dataflow benchmarks ' +
          '([-PbenchmarkBeamVersion=2.76.0] [-PpythonVersion=3.12])'
      task.onlyIf { refreshBenchVenv }
      task.doLast {
        String interpreter = "python${project.findProperty('pythonVersion') ?: '3.12'}"
        project.delete(benchVenv)
        project.exec({ ExecSpec spec ->
          spec.commandLine(interpreter, '-m', 'venv', benchVenv.absolutePath)
        } as Action<ExecSpec>)
        project.exec({ ExecSpec spec ->
          spec.commandLine(
              "${benchVenv.absolutePath}/bin/pip", 'install', '--quiet',
              "apache-beam[gcp]==${benchBeamVersion}")
        } as Action<ExecSpec>)
        benchStamp.setText(benchBeamVersion, 'UTF-8')
      }
    }

    project.tasks.register('buildIoBenchmarkWorkers') { Task task ->
      task.group = 'Build'
      task.description =
          'Build all four Linux I/O benchmark worker binaries in one Cargo invocation (-PexampleArch=amd64)'
      task.inputs.files(config.rustWorkspaceTree)
      task.inputs.property('packages', IO_BENCHMARK_PACKAGES)
      task.inputs.property('arch', config.workerArch)
      task.inputs.property('sdkVersion', build.versions.sdkVersion)
      task.inputs.property('targetCpu', project.findProperty('workerTargetCpu') ?: 'default')
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

  private Map<String, Object> waitSpec(BenchmarkSpec cfg, String shape, boolean hasProducer) {
    Project project = build.project
    if (benchmarks.isSet('detach')) {
      return [detach: (Object) true]
    }
    boolean hasCounter = (cfg.counterShapes ?: new ArrayList<String>()).contains(shape)
    Long idleSeconds = hasProducer
        ? companionSetting(cfg.producerDefaults, 'idleAfterLoadSeconds').toLong() : null
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

  private List<String> jobFlags(String pipeline, Map<String, Object> job, String extraArgsProp = 'extraArgs') {
    String experiments = benchmarks.minCpuExperiments()
    boolean autoscaling = Boolean.TRUE == job['autoscaling']
    boolean runnerV1 = Boolean.TRUE == job['runnerV1']
    List<String> flags = [
      "--shape=${job['shape']}".toString(),
      '--runner=DataflowRunner',
      "--project=${job['gcpProject']}".toString(),
      "--region=${job['region']}".toString(),
      "${benchFlag(pipeline, 'temp_location')}=${job['tempLocation']}".toString(),
      "${benchFlag(pipeline, 'num_workers')}=${job['workers']}".toString(),
      "${benchFlag(pipeline, 'max_num_workers')}=${job['maxWorkers']}".toString(),
      "${benchFlag(pipeline, 'autoscaling_algorithm')}=${autoscaling ? 'THROUGHPUT_BASED' : 'NONE'}".toString(),
      "${benchFlag(pipeline, pipeline == 'python' ? 'machine_type' : 'worker_machine_type')}=${job['machine']}".toString(),
      "${benchFlag(pipeline, 'job_name')}={job_name}".toString(),
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
    flags.addAll(dataflow.dataflowOptionFlags(
        'benchmark',
        pipeline,
        ['numWorkers', 'maxNumWorkers', 'autoscalingAlgorithm', 'workerMachineType',
         'experiments', 'sdkHarnessContainerImageOverrides']))
    flags.addAll(build.project.findProperty(extraArgsProp)?.toString()?.tokenize(' ') ?: new ArrayList<String>())
    return flags
  }

  private Map<String, Object> sdkLaunchCommand(
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
    String script = "${build.rustDir.absolutePath}/benchmarks/io/${cfg.dir}/python/${cfg.pythonScript}"
    List<String> argv = ["${benchVenv.absolutePath}/bin/python".toString(), script] + args + ['{labels_args}']
    return [argv: (Object) argv]
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

  @SuppressWarnings('unchecked')
  private Map<String, Object> buildVariantMeta(
      BenchmarkSpec cfg,
      Map<String, Object> job,
      String reader,
      String writer,
      boolean hasProducer) {
    String pipeline = benchPipeline
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
      describe: describe,
      wait    : waitSpec(cfg, run.shape, hasProducer),
      after   : afterCmd,
    ]
  }

  @SuppressWarnings('unchecked')
  private Map<String, Object> benchSdkVariant(
      BenchmarkSpec cfg,
      Map<String, Object> job,
      String reader,
      String writer,
      boolean hasProducer) {
    String pipeline = benchPipeline
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
      args.add("${benchFlag(pipeline, 'expansion_service')}=localhost:${job['expansionPort']}".toString())
    }

    Map<String, Object> variant = [
      label   : meta['label'],
      cwd     : build.rustDir.toString(),
      labels  : meta['labels'],
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
  private Map<String, Object> benchRustVariant(
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
    rawFlags.addAll(build.project.findProperty('extraArgs')?.toString()?.tokenize(' ') ?: new ArrayList<String>())

    List<String> staticFlags = rawFlags.findAll { String f -> !f.contains('{') }
    List<String> templateFlags = rawFlags.findAll { String f -> f.contains('{') }

    File argsFile = build.buildFile("tmp/benchmarks/${cfg.task}-${variantLabel}-rust.args")
    argsFile.parentFile.mkdirs()
    argsFile.setText(staticFlags.join('\n') + '\n', 'UTF-8')

    String experiments = benchmarks.minCpuExperiments()
    List<String> forwarded = RustBenchmarkTasks.DATAFLOW_PASSTHROUGH.findAll { String key ->
      !(key in ['gcpProject', 'gcpRegion', 'gcpTempLocation', 'autoscalingAlgorithm'])
    }.collectMany { String key ->
      String val = key == 'serviceAccountEmail'
          ? (benchProp('serviceAccount') ?: benchProp(key, 'SERVICE_ACCOUNT_EMAIL'))
          : benchProp(key)
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

    Map<String, Object> base = benchmarks.rustDataflowVariant(
        checkout, cfg.rustPackage, props, templateFlags.join(' '))
    Map<String, String> labels = new LinkedHashMap<>((Map<String, String>) meta['labels'])
    if (multiRef) {
      labels['ref'] = checkout['label']
    }
    base['label'] = variantLabel
    base['labels'] = labels
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
