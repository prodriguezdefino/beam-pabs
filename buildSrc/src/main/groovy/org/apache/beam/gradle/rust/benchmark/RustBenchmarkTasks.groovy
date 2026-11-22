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

import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import java.text.SimpleDateFormat
import org.apache.beam.gradle.rust.RustConfig
import org.apache.beam.gradle.rust.RustSdkBuild
import org.apache.beam.gradle.rust.runner.RustDataflowTasks
import org.gradle.api.Action
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.file.CopySpec
import org.gradle.api.tasks.Exec
import org.gradle.process.ExecSpec

/**
 * Dataflow benchmarks ({@code registerRustBenchmark}, {@code wordcountBenchmark}), local CPU
 * profile comparisons ({@code profileCompare}), and Criterion harness benchmarks
 * ({@code harnessBenchmark}).
 */
@CompileStatic
class RustBenchmarkTasks {

  static final List<String> DATAFLOW_PASSTHROUGH = [
    'gcpProject', 'gcpRegion', 'gcpTempLocation', 'sdkContainerImage',
    'network', 'subnetwork', 'serviceAccountEmail', 'usePublicIps',
    'diskSizeGb', 'workerDiskType', 'autoscalingAlgorithm',
    'dataflowServiceOptions', 'numberOfWorkerHarnessThreads',
    'workerTargetCpu', 'exampleFeatures',
  ]

  private static final List<String> PROFILE_PASSTHROUGH = [
    'pkg', 'bin', 'cargoExample', 'cargoBench', 'exampleFeatures',
  ]

  private static final List<String> HARNESS_BENCHES = [
    'process_bundle', 'data_plane', 'user_state', 'side_input',
    'chain_throughput', 'window_into', 'row_coder', 'logging', 'combine', 'timers',
    'join', 'async_rpc', 'state_cache',
  ]

  final RustDataflowTasks dataflow
  final RustConfig config
  final RustSdkBuild build
  final File benchScripts

  private RustBenchmarkTasks(RustDataflowTasks dataflow) {
    this.dataflow = dataflow
    this.config = dataflow.config
    this.build = config.build
    benchScripts = new File(build.rustDir, 'benchmarks/tools')
  }

  static RustBenchmarkTasks register(RustDataflowTasks dataflow) {
    RustBenchmarkTasks benchmarks = new RustBenchmarkTasks(dataflow)
    benchmarks.registerAll()
    return benchmarks
  }

  boolean isSet(String name) {
    Object value = build.project.findProperty(name)
    return value != null && value.toString() != 'false'
  }

  List<String> passthrough(List<String> names) {
    Project project = build.project
    return names.findAll { String n -> project.findProperty(n) != null }
        .collect { String n -> "-P${n}=${project.findProperty(n)}".toString() }
  }

  String minCpuExperiments() {
    Project project = build.project
    String minCpu = project.findProperty('minCpuPlatform')?.toString()
    String exps = project.findProperty('experiments')?.toString()
    List<String> parts = []
    if (exps != null && !exps.isEmpty()) {
      parts.add(exps)
    }
    if (minCpu != null && !minCpu.isEmpty()) {
      parts.add("min_cpu_platform=${minCpu}".toString())
    }
    return parts.join(',')
  }

  String userPath(String path) {
    File f = new File(path)
    return f.absolute ? path : new File(build.project.gradle.startParameter.currentDir, path).absolutePath
  }

  List<String> commonArgs(Object defaultRuns = 3) {
    String compareTo = build.project.findProperty('compareTo')?.toString()
    List<String> args = ["--runs=${build.project.findProperty('runs') ?: defaultRuns}".toString()]
    if (compareTo != null && !compareTo.isEmpty()) {
      args.add("--compare-to=${userPath(compareTo)}".toString())
    }
    if (isSet('dryRun')) {
      args.add('--dry-run')
    }
    return args
  }

  File writeJson(String name, Object payload) {
    File path = build.buildFile("tmp/benchmarks/${name}.json")
    path.parentFile.mkdirs()
    path.text = JsonOutput.prettyPrint(JsonOutput.toJson(payload))
    return path
  }

  File writeVariants(String taskName, List<Map<String, Object>> variants) {
    return writeJson("${taskName}-variants", variants)
  }

  File reportDir(String taskName) {
    String ts = new SimpleDateFormat('yyyyMMdd-HHmmss').format(new Date())
    return build.buildFile("reports/benchmarks/${taskName}-${ts}")
  }

  String gitOutput(List<String> args) {
    Project project = build.project
    ByteArrayOutputStream out = new ByteArrayOutputStream()
    project.exec({ ExecSpec spec ->
      spec.commandLine(['git', '-C', project.rootDir.absolutePath] + args)
      spec.standardOutput = out
    } as Action<ExecSpec>)
    return out.toString('UTF-8').trim()
  }

  void gitRefVariants(Closure<?> body) {
    Project project = build.project
    Map<String, String> current = [
      label   : 'current',
      root    : project.rootDir.absolutePath,
      describe: "${gitOutput(['rev-parse', '--short', 'HEAD'])} with local changes".toString(),
    ]
    String baseline = project.findProperty('baseline')?.toString()
    if (baseline == null || baseline.isEmpty()) {
      body.call([current])
      return
    }
    String sha = gitOutput(['rev-parse', '--short', '--verify', "${baseline}^{commit}".toString()])
    File tree = build.buildFile('benchmarks/baseline')
    List<String> worktree = ['git', '-C', project.rootDir.absolutePath, 'worktree']
    boolean known = tree.directory && project.exec({ ExecSpec spec ->
      spec.commandLine('git', '-C', tree.absolutePath, 'rev-parse', '--is-inside-work-tree')
      spec.ignoreExitValue = true
      spec.standardOutput = new ByteArrayOutputStream()
      spec.errorOutput = new ByteArrayOutputStream()
    } as Action<ExecSpec>).exitValue == 0

    List<List<String>> commands = known
        ? [
          ['git', '-C', tree.absolutePath, 'checkout', '--force', '--detach', sha],
          ['git', '-C', tree.absolutePath, 'clean', '-fd'],
        ]
        : [
          worktree + ['prune'],
          worktree + ['add', '--force', '--detach', tree.absolutePath, sha],
        ]
    File localDir = new File(build.rustDir, '.local')
    List<Map<String, String>> variants = [
      [label: 'baseline', root: tree.absolutePath, describe: "${baseline} (${sha})".toString()],
      current,
    ]
    if (isSet('dryRun')) {
      commands.each { List<String> cmd -> project.logger.lifecycle("Would run: ${cmd.join(' ')}") }
      project.logger.lifecycle("Would copy ${localDir} to ${tree}/sdks/rust/.local")
      body.call(variants)
      return
    }
    if (!known) {
      project.delete(tree)
    }
    commands.each { List<String> cmd ->
      project.exec({ ExecSpec spec -> spec.commandLine(cmd) } as Action<ExecSpec>)
    }
    if (localDir.exists()) {
      project.copy({ CopySpec spec ->
        spec.from(localDir)
        spec.into(new File(tree, 'sdks/rust/.local'))
      } as Action<CopySpec>)
    }
    body.call(variants)
  }

  @SuppressWarnings('unchecked')
  void runDataflowBenchmark(Map<String, Object> cfg, List<Map<String, Object>> variants) {
    Project project = build.project
    String taskName = cfg.task.toString()
    String gcpProject = (cfg.gcpProject ?: config.gcpProjectFrom('dataflow')
        ?: (isSet('dryRun') ? '<gcpProject>' : null))?.toString()
    if (gcpProject == null || gcpProject.isEmpty()) {
      throw new GradleException(
          "${taskName} needs gcpProject in sdks/rust/.local/sdk.properties or -PgcpProject.")
    }
    String region = (cfg.region ?: config.resolveProp('dataflow', 'gcpRegion', 'GCP_REGION', 'us-central1')).toString()
    List<String> cmd = [
      'python3',
      new File(benchScripts, 'dataflow_job.py').absolutePath,
      'run',
      "--task=${taskName}".toString(),
      "--project=${gcpProject}".toString(),
      "--region=${region}".toString(),
      "--out-dir=${reportDir(taskName).absolutePath}".toString(),
      "--variants=${writeVariants(taskName, variants).absolutePath}".toString(),
      "--describe=${cfg.describe}".toString(),
      "--count-output=${cfg.countOutput ?: ''}".toString(),
      "--count-name=${cfg.countName ?: 'elements'}".toString(),
      "--monitoring-wait=${project.findProperty('monitoringWait') ?: 360}".toString(),
    ]
    if (cfg.counter) {
      cmd.add("--counter=${cfg.counter}".toString())
    }
    if (cfg.namePrefix) {
      cmd.add("--name-prefix=${cfg.namePrefix}".toString())
    }
    if (cfg.current) {
      cmd.add("--current=${cfg.current}".toString())
    }
    if (cfg.workers) {
      cmd.add("--workers=${cfg.workers}".toString())
    }
    if (cfg.io) {
      cmd.add("--io=${JsonOutput.toJson(cfg.io)}".toString())
    }
    if (cfg.producer) {
      cmd.add("--producer=${writeJson("${taskName}-producer", cfg.producer).absolutePath}".toString())
    }
    if (cfg.shared instanceof Map) {
      Map<String, Object> shared = (Map<String, Object>) cfg.shared
      cmd.add("--shared-producer=${shared.dir}".toString())
      cmd.add("--shared-tasks=${shared.tasks}".toString())
    }
    if (isSet('prepareOnly')) {
      cmd.add('--prepare-only')
    }
    cmd.addAll(commonArgs(cfg.runs ?: 3))
    project.exec({ ExecSpec spec -> spec.commandLine(cmd) } as Action<ExecSpec>)
  }

  Map<String, Object> rustDataflowVariant(
      Map<String, String> checkout, String example, List<String> props, String extraArgs) {
    Closure<List<String>> taskCmd = { List<String> first ->
      String joined = (first + (extraArgs ? [extraArgs] : new ArrayList<String>())).join(' ')
      return ["${checkout.root}/gradlew".toString(), '--console=plain', ':sdks:rust:dataflow',
              "-Pexample=${example}".toString()] + props + ["-PextraArgs=${joined}".toString()]
    }
    return [
      label   : checkout.label,
      describe: checkout.describe,
      cwd     : checkout.root,
      prepare : taskCmd.call(["--dataflow_job_file={out_dir}/${checkout.label}-prepare-job.json".toString()]),
      argv    : taskCmd.call(['--job_name={job_name}', '--labels={labels}']),
    ] as Map<String, Object>
  }

  @SuppressWarnings('unchecked')
  void registerRustBenchmark(Map<String, Object> cfg) {
    Project project = build.project
    String taskName = cfg.task.toString()
    String example = cfg.example.toString()
    String inputProperty = (cfg.inputProperty ?: 'inputName').toString()
    Map<String, String> inputsMap = (Map<String, String>) (cfg.inputs ?: [:])

    project.tasks.register(taskName) { Task task ->
      task.group = 'Verification'
      task.description = "${cfg.description} ([-P${inputProperty}=${inputsMap.keySet().join('|')}] " +
          '[-Pinput=<glob>] [-Pruns=3] [-Pbaseline=<git ref>] [-PcompareTo=<results.json>] ' +
          '[-PnumWorkers=1] [-PmaxNumWorkers=10] [-PworkerMachineType=n2-standard-4] ' +
          '[-PminCpuPlatform=<platform>] [-PextraArgs=<pipeline flags>] [-PdryRun])'

      task.doLast {
        String inputName = (project.findProperty(inputProperty) ?: cfg.defaultInput)?.toString()
        String input = project.findProperty('input')?.toString() ?: inputsMap[inputName]
        if (!inputsMap.isEmpty() && (input == null || input.isEmpty())) {
          throw new GradleException(
              "-P${inputProperty} must be ${inputsMap.keySet().join(' or ')}, not '${inputName}'.")
        }
        String workers = (project.findProperty('numWorkers') ?: '1').toString()
        String maxWorkers = (project.findProperty('maxNumWorkers') ?: '10').toString()
        String machine = (project.findProperty('workerMachineType') ?: 'n2-standard-4').toString()
        String minCpu = project.findProperty('minCpuPlatform')?.toString()
        String experiments = minCpuExperiments()
        List<String> props = []
        if (input != null && !input.isEmpty()) {
          props.add("-Pinput=${input}".toString())
        }
        props.addAll([
          "-PnumWorkers=${workers}".toString(),
          "-PmaxNumWorkers=${maxWorkers}".toString(),
          "-PworkerMachineType=${machine}".toString(),
        ])
        if (!experiments.isEmpty()) {
          props.add("-Pexperiments=${experiments}".toString())
        }
        props.addAll(passthrough(DATAFLOW_PASSTHROUGH))

        List<String> extraParts = []
        if (cfg.extraArgs) {
          extraParts.add(cfg.extraArgs.toString())
        }
        if (project.findProperty('extraArgs')) {
          extraParts.add(project.findProperty('extraArgs').toString())
        }
        String extraArgs = extraParts.join(' ')
        String summary = "Example ${example}, input ${input ?: 'default'}, ${machine}, " +
            "${workers} to ${maxWorkers} workers" + (minCpu ? ", minimum CPU platform ${minCpu}" : '') + '.'

        gitRefVariants { List<Map<String, String>> checkouts ->
          Map<String, Object> runCfg = new LinkedHashMap<>(cfg)
          runCfg.put('describe', summary)
          runCfg.put('workers', workers)
          List<Map<String, Object>> variants = checkouts.collect { Map<String, String> checkout ->
            rustDataflowVariant(checkout, example, props, extraArgs)
          }
          runDataflowBenchmark(runCfg, variants)
        }
      }
    }
  }

  private void registerAll() {
    registerRustBenchmark([
      task         : 'wordcountBenchmark',
      description  : 'Run wordcount on Dataflow for some rounds and report its CPU metrics',
      example      : 'wordcount',
      inputProperty: 'shards',
      inputs       : [
        '100': 'gs://apache-beam-samples/wikipedia_edits/wiki_data-0000000000[0-9][0-9].json',
        '300': 'gs://apache-beam-samples/wikipedia_edits/wiki_data-000000000[0-2][0-9][0-9].json',
      ],
      defaultInput : '100',
      countOutput  : 'ExtractWords',
      countName    : 'words',
      counter      : 'line_count',
    ])

    registerProfileCompare()
    registerHarnessBenchmark()
  }

  private void registerProfileCompare() {
    Project project = build.project
    project.tasks.register('profileCompare') { Task task ->
      task.group = 'Verification'
      task.description = 'Profile a Rust example on Prism for some rounds (profilePrism, worker profile, CPU mode) ' +
          'and compare the CPU time ([-Pexample=wordcount] [-Ppkg=.. -Pbin=..|-PcargoExample=..|-PcargoBench=..] ' +
          '[-Pinput=<local glob>] [-Prows=<n>] [-PextraArgs=<flags>] [-Pruns=3] [-Pbaseline=<git ref>] ' +
          '[-PcompareTo=<results.json>] [-Ptop=10] [-PdryRun])'

      task.doLast {
        String example = (project.findProperty('example') ?: 'wordcount').toString()
        String rawInput = project.findProperty('input')?.toString()
        String input = rawInput != null ? userPath(rawInput) : null
        List<String> flagParts = []
        if (input != null) {
          flagParts.add("--input=${input}".toString())
          flagParts.add('--output={run_dir}/out/counts')
        }
        if (project.findProperty('extraArgs') != null) {
          flagParts.add(project.property('extraArgs').toString())
        }
        String flags = flagParts.join(' ')
        List<String> props = [
          "-Pexample=${example}".toString(),
          '-Pmode=cpu',
          '-PcargoProfile=worker',
          '-Prows={rows}',
          '-Pout={run_dir}',
        ] + passthrough(PROFILE_PASSTHROUGH) +
            (flags.isEmpty() ? new ArrayList<String>() : ["-PextraArgs=${flags}".toString()])

        gitRefVariants { List<Map<String, String>> checkouts ->
          List<Map<String, Object>> variants = checkouts.collect { Map<String, String> checkout ->
            [
              label   : checkout.label,
              describe: checkout.describe,
              cwd     : checkout.root,
              argv    : ["${checkout.root}/gradlew".toString(), '--console=plain', ':sdks:rust:profilePrism'] + props,
            ] as Map<String, Object>
          }
          List<String> cmd = [
            'python3',
            new File(benchScripts, 'local_profile.py').absolutePath,
            "--out-dir=${reportDir('profileCompare').absolutePath}".toString(),
            "--variants=${writeVariants('profileCompare', variants).absolutePath}".toString(),
            "--describe=Example ${example}".toString(),
            "--top=${project.findProperty('top') ?: 10}".toString(),
          ]
          if (input != null) {
            cmd.add("--input=${input}".toString())
          }
          if (project.findProperty('rows') != null) {
            cmd.add("--rows=${project.property('rows')}".toString())
          }
          cmd.addAll(commonArgs())
          project.exec({ ExecSpec spec -> spec.commandLine(cmd) } as Action<ExecSpec>)
        }
      }
    }
  }

  private void registerHarnessBenchmark() {
    Project project = build.project
    project.tasks.register('harnessBenchmark', Exec) { Exec task ->
      task.group = 'Verification'
      task.description = 'Run the criterion benchmarks of the harness crate (or another crate with -Ppkg=<crate>) ' +
          "([-Ppkg=apache-beam-harness] [-Pbench=${HARNESS_BENCHES.join('|')}] [-Pfilter=<benchmark id regex>] [-PsmokeTest] " +
          '[-PsaveBaseline=<name>] [-PbaselineName=<name>] [-PcargoProfile=<profile>])'
      task.workingDir(build.rustDir)
      task.environment(build.versions.cargoEnv())

      String pkg = (project.findProperty('pkg') ?: 'apache-beam-harness').toString()
      String bench = project.findProperty('bench')?.toString()
      task.doFirst {
        if (pkg == 'apache-beam-harness' && bench != null && !(bench in HARNESS_BENCHES)) {
          throw new GradleException("-Pbench must be one of ${HARNESS_BENCHES}, not '${bench}'")
        }
        if (pkg != 'apache-beam-harness' && (bench == null || bench.isEmpty())) {
          throw new GradleException("-Pbench is required when -Ppkg is '${pkg}'")
        }
      }

      List<String> criterionArgs = []
      String filter = project.findProperty('filter')?.toString()
      if (filter != null && !filter.isEmpty()) {
        criterionArgs.add(filter)
      }
      if (project.hasProperty('smokeTest')) {
        criterionArgs.add('--test')
      }
      if (project.findProperty('saveBaseline') != null) {
        criterionArgs.addAll(['--save-baseline', project.property('saveBaseline').toString()])
      }
      if (project.findProperty('baselineName') != null) {
        criterionArgs.addAll(['--baseline', project.property('baselineName').toString()])
      }
      List<String> profileArgs = project.findProperty('cargoProfile') != null
          ? ['--profile', project.property('cargoProfile').toString()]
          : new ArrayList<String>()
      List<String> benchFlags = (bench != null && !bench.isEmpty())
          ? ['--bench', bench]
          : HARNESS_BENCHES.collectMany { String b -> ['--bench', b] }
      task.commandLine(['cargo', 'bench'] + build.workspaceScope + profileArgs + benchFlags + ['--'] + criterionArgs)
    }
  }
}
