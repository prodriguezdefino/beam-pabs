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
import java.net.ServerSocket
import java.net.Socket
import org.apache.beam.gradle.rust.BeamModuleExtras
import org.apache.beam.gradle.rust.RustConfig
import org.apache.beam.gradle.rust.RustSdkBuild
import org.apache.beam.gradle.rust.container.RustContainerTasks
import org.gradle.api.Action
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.tasks.Exec
import org.gradle.process.ExecSpec

/** Shared cross-language infrastructure and expansion-service lifecycle helpers. */
@CompileStatic
class RustXlangTasks {

  /** Running Docker container serving a Rust expansion service. */
  static class ServiceHandle {
    int port
    String container
  }

  private static final String VENV_EXTRAS = 'gcp,test'

  final RustContainerTasks containers
  final RustConfig config
  final RustSdkBuild build
  final String xlangArch
  final String testingImage
  final String dataflowRoot
  final String dataflowImage
  final File pythonDir
  final File pythonSdist
  final File xlangVenv
  String pythonSdkImage

  private RustXlangTasks(RustContainerTasks containers) {
    this.containers = containers
    this.config = containers.config
    this.build = config.build
    Project project = build.project

    xlangArch = (project.findProperty('exampleArch')
        ?: BeamModuleExtras.nativeArchitecture(project)).toString()
    testingImage = containers.rustImageName('testing_expansion_service')
    dataflowRoot = config.resolveProp('xlang', 'dataflowRepositoryRoot')
    dataflowImage = dataflowRoot != null
        ? containers.rustImageName('testing_expansion_service', dataflowRoot) : null

    pythonDir = new File(project.rootDir, 'sdks/python')
    pythonSdist = new File(pythonDir, 'build/apache-beam.tar.gz')
    xlangVenv = build.buildFile('xlang-venv')
  }

  static RustXlangTasks register(RustContainerTasks containers) {
    RustXlangTasks xlang = new RustXlangTasks(containers)
    xlang.registerCore()
    RustXlangPythonTasks.register(xlang)
    RustXlangJavaTasks.register(xlang)
    RustXlangGoTasks.register(xlang)
    return xlang
  }

  static void waitForPort(int port, int timeoutSeconds) {
    long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L
    while (System.currentTimeMillis() < deadline) {
      try {
        new Socket('localhost', port).close()
        return
      } catch (IOException ignored) {
        Thread.sleep(500L)
      }
    }
    throw new GradleException("The expansion service did not open port ${port} in ${timeoutSeconds} s.")
  }

  void requireDataflowRoot(String root = dataflowRoot) {
    if (!root) {
      throw new GradleException(
          'Set dataflowRepositoryRoot in sdks/rust/.local/sdk.properties or with -P.')
    }
  }

  List<String> selectedTests(Closure<String> transform) {
    String raw = build.project.findProperty('tests')?.toString()
    if (raw == null || raw.isEmpty()) {
      return null
    }
    return raw.tokenize(',').collect { String item -> transform.call(item.trim()).toString() }
  }

  void registerDataflowExpansionImageTasks(Map<String, Object> cfg) {
    String root = (cfg.containsKey('root') ? cfg['root'] : dataflowRoot)?.toString()
    String imageName = cfg['imageName'].toString()
    String imageTask = cfg['imageTask'].toString()
    containers.registerExpansionImageTasks([
      buildTask       : cfg['buildTask'],
      imageTask       : imageTask,
      bin             : cfg['bin'],
      arch            : 'amd64',
      image           : root != null ? containers.rustImageName(imageName, root) : 'unset',
      baseImage       : root != null ? containers.rustImageName('sdk', root) : null,
      push            : true,
      buildDescription: cfg['buildDescription'],
      imageDescription: cfg['imageDescription'],
    ])
    build.project.tasks.named(imageTask).configure { Task task ->
      task.doFirst { requireDataflowRoot(root) }
    }
  }

  String registerSdkDataflowImageTask(Map<String, Object> cfg) {
    Project project = build.project
    String sdk = cfg['sdk'].toString()
    String sdkLower = sdk.toLowerCase()
    String prop = "${sdkLower}SdkImage"
    String defaultTag = (cfg['defaultTag'] ?: build.versions.sdkVersion).toString()
    String tag = (project.rootProject.findProperty('docker-tag') ?: defaultTag).toString()
    String hint = cfg['extraHint'] ? "${cfg['extraHint']} ".toString() : ''
    String imageSuffix = cfg['imageSuffix'].toString()
    String gradleTask = cfg['gradleTask'].toString()
    @SuppressWarnings('unchecked')
    List<String> extraArgs = ((Iterable<Object>) (cfg['extraArgs'] ?: [])).collect { Object it -> it.toString() }

    project.tasks.register("${sdkLower}SdkDataflowImage", Exec) { Exec task ->
      task.group = 'Build'
      task.description = "Build and push the linux/amd64 ${sdk} SDK container of this tree for the Dataflow " +
          "cross-language suite (${hint}[-PdataflowRepositoryRoot=<registry>])"
      String image = dataflowRoot != null
          ? BeamModuleExtras.containerImageName(
              project,
              "${BeamModuleExtras.dockerDefaultRepoPrefix(project)}${imageSuffix}",
              dataflowRoot,
              tag)
          : null
      task.workingDir(project.rootDir)
      task.commandLine([
        "${project.rootDir.absolutePath}/gradlew".toString(),
        gradleTask,
        '-Pcontainer-architecture-list=amd64',
        "-Pdocker-repository-root=${dataflowRoot}".toString(),
        '-Ppush-containers',
      ] + extraArgs)
      task.doFirst { requireDataflowRoot() }
      task.doLast {
        project.logger.lifecycle(
            "Pushed ${image}. Set example.xlang.${prop}=${image} in sdks/rust/.local/sdk.properties.")
      }
    }
    return config.resolveProp('xlang', prop)
  }

  @SuppressWarnings('unchecked')
  <T extends Task> void registerXlangSuiteTasks(
      Map<String, Object> cfg,
      Class<T> taskType,
      Action<T> configurePrism,
      Action<T> configureDataflow) {
    Project project = build.project
    String sdk = cfg['sdk'].toString()
    String testExamples = cfg['testExamples'].toString()
    List<String> extraDeps = ((Iterable<Object>) (cfg['dependsOn'] ?: []))
        .collect { Object it -> it.toString() }
    List<String> extraPrismDeps = ((Iterable<Object>) (cfg['prismDependsOn'] ?: []))
        .collect { Object it -> it.toString() }
    List<String> extraDataflowDeps = ((Iterable<Object>) (cfg['dataflowDependsOn'] ?: []))
        .collect { Object it -> it.toString() }
    List<String> commonDeps = ['testingExpansionServiceImage'] + extraDeps
    List<String> prismDeps = commonDeps + ['fetchPrism'] + extraPrismDeps
    List<String> dataflowDeps = commonDeps + ['testingExpansionServiceDataflowImage'] + extraDataflowDeps

    String prismHint = cfg['prismHint']?.toString()
    String dataflowHint = cfg['dataflowHint']?.toString()
    String prismHints = (["[-Ptests=${testExamples}]".toString()] +
        (prismHint ? [prismHint] : new ArrayList<String>())).join(' ')
    String dataflowHints = (["[-Ptests=${testExamples}]".toString()] +
        (dataflowHint ? [dataflowHint] : new ArrayList<String>())).join(' ')

    project.tasks.register("validatesCrossLanguageRunner${sdk}Prism", taskType) { T task ->
      task.group = 'Verification'
      task.description =
          "Run the ${sdk} cross-language suite against the Rust test expansion service on Prism (${prismHints})"
      task.dependsOn(prismDeps)
      configurePrism.execute(task)
    }

    project.tasks.register("validatesCrossLanguageRunner${sdk}Dataflow", taskType) { T task ->
      task.group = 'Verification'
      task.description =
          "Run the ${sdk} cross-language suite against the Rust test expansion service on Dataflow (${dataflowHints})"
      task.dependsOn(dataflowDeps)
      configureDataflow.execute(task)
    }
  }

  void registerXlangSuiteTasks(
      Map<String, Object> cfg,
      Action<DefaultTask> configurePrism,
      Action<DefaultTask> configureDataflow) {
    registerXlangSuiteTasks(cfg, DefaultTask, configurePrism, configureDataflow)
  }

  @SuppressWarnings('unchecked')
  List<String> xlangDataflowFlags(Map<String, Object> cfg = [:]) {
    String gcpProject = config.gcpProjectFrom('xlang')
    String gcpTempLocation = config.resolveProp('xlang', 'gcpTempLocation', 'GCP_TEMP_LOCATION')
    if (!gcpProject || !gcpTempLocation) {
      throw new GradleException(
          'Set gcpProject and gcpTempLocation in sdks/rust/.local/sdk.properties or with -P.')
    }
    String region = config.resolveProp('xlang', 'gcpRegion', 'GCP_REGION', 'us-central1')
    String runner = (cfg['runner'] ?: 'TestDataflowRunner').toString()
    String tempFlag = (cfg['tempFlag'] ?: 'temp_location').toString()
    String tempSuffix = (cfg['tempSuffix'] ?: '').toString()
    String numWorkersFlag = (cfg['numWorkersFlag'] ?: 'num_workers').toString()
    String machineTypeFlag = (cfg['machineTypeFlag'] ?: 'machine_type').toString()
    Object extraRaw = cfg['extraFlags']
    List<String> extra
    if (extraRaw instanceof Closure) {
      extra = ((Iterable<Object>) ((Closure) extraRaw).call(gcpTempLocation)).collect { Object it -> it.toString() }
    } else if (extraRaw instanceof Iterable) {
      extra = ((Iterable<Object>) extraRaw).collect { Object it -> it.toString() }
    } else {
      extra = []
    }

    List<String> opts = [
      "--runner=${runner}".toString(),
      "--project=${gcpProject}".toString(),
      "--region=${region}".toString(),
      "--${tempFlag}=${gcpTempLocation}${tempSuffix}".toString(),
      "--${numWorkersFlag}=1".toString(),
    ] + extra
    ['network', 'subnetwork'].each { String key ->
      String value = config.resolveProp('xlang', key)
      if (value != null && !value.isEmpty()) {
        opts.add("--${key}=${value}".toString())
      }
    }
    String machineType = config.resolveProp('xlang', 'workerMachineType')
    if (machineType != null && !machineType.isEmpty()) {
      opts.add("--${machineTypeFlag}=${machineType}".toString())
    }
    return opts
  }

  List<String> pythonDataflowPipelineOptions() {
    List<String> extra = pythonSdkImage != null
        ? ["--sdk_container_image=${pythonSdkImage}".toString(), '--sdk_location=container']
        : ["--sdk_location=${pythonSdist.absolutePath}".toString()]
    return xlangDataflowFlags([extraFlags: extra])
  }

  ServiceHandle startExpansionService(Map<String, Object> cfg = [:]) {
    String image = (cfg['image'] ?: testingImage).toString()
    String workerImage = (cfg['workerImage'] ?: image).toString()
    ServerSocket sock = new ServerSocket(0)
    int port
    try {
      port = sock.localPort
    } finally {
      sock.close()
    }
    String container = "beam-rust-expansion-${port}"
    List<String> command = ['docker', 'run', '-d', '--rm', '--name', container, '-p', "${port}:8097".toString()]
    if (cfg['platform']) {
      command.addAll(['--platform', cfg['platform'].toString()])
    }
    if (cfg['credentials']) {
      command.addAll([
        '-v', "${cfg['credentials']}:/creds.json:ro".toString(),
        '-e', 'GOOGLE_APPLICATION_CREDENTIALS=/creds.json',
      ])
    }
    command.addAll([
      '--entrypoint', '/opt/apache/beam/worker_binary', image,
      '--port', '8097', '--docker-image', workerImage,
    ])
    build.project.exec({ ExecSpec spec ->
      spec.commandLine(command)
    } as Action<ExecSpec>)
    ServiceHandle service = new ServiceHandle(port: port, container: container)
    try {
      waitForPort(port, 60)
    } catch (GradleException e) {
      stopExpansionService(service)
      throw e
    }
    return service
  }

  void stopExpansionService(ServiceHandle service) {
    build.project.exec({ ExecSpec spec ->
      spec.commandLine('docker', 'rm', '-f', service.container)
      spec.ignoreExitValue = true
      spec.standardOutput = new ByteArrayOutputStream()
    } as Action<ExecSpec>)
  }

  void execWithExpansionService(
      Map<String, Object> cfg = [:],
      Closure<?> configureExec) {
    ServiceHandle service = startExpansionService(cfg)
    try {
      String host = config.dockerHost()
      build.project.exec({ ExecSpec spec ->
        if (host != null) {
          spec.environment('DOCKER_HOST', host)
        }
        configureExec.delegate = spec
        configureExec.resolveStrategy = Closure.DELEGATE_FIRST
        configureExec.call(service)
      } as Action<ExecSpec>)
    } finally {
      stopExpansionService(service)
    }
  }

  void withPrismJobServer(Closure<?> body) {
    ServerSocket sock = new ServerSocket(0)
    int jobPort
    try {
      jobPort = sock.localPort
    } finally {
      sock.close()
    }
    ProcessBuilder pb = new ProcessBuilder(
        config.prism.prismHostBinary.absolutePath,
        '--serve_http=false',
        "--job_port=${jobPort}".toString(),
        '--idle_shutdown_timeout=0s')
        .inheritIO()
    String host = config.dockerHost()
    if (host != null) {
      pb.environment().put('DOCKER_HOST', host)
    }
    Process prismProcess = pb.start()
    try {
      waitForPort(jobPort, 30)
      body.call(jobPort)
    } finally {
      prismProcess.destroy()
    }
  }

  boolean dockerReachable() {
    String host = config.dockerHost()
    return build.project.exec({ ExecSpec spec ->
      if (host != null) {
        spec.environment('DOCKER_HOST', host)
      }
      spec.commandLine('docker', 'info')
      spec.standardOutput = new ByteArrayOutputStream()
      spec.errorOutput = new ByteArrayOutputStream()
      spec.ignoreExitValue = true
    } as Action<ExecSpec>).exitValue == 0
  }

  boolean imageExists(String image) {
    String host = config.dockerHost()
    return build.project.exec({ ExecSpec spec ->
      if (host != null) {
        spec.environment('DOCKER_HOST', host)
      }
      spec.commandLine('docker', 'image', 'inspect', image)
      spec.standardOutput = new ByteArrayOutputStream()
      spec.errorOutput = new ByteArrayOutputStream()
      spec.ignoreExitValue = true
    } as Action<ExecSpec>).exitValue == 0
  }

  void runXlangTestsAgainstService(
      String name,
      List<String> command,
      File dir,
      String addrVar,
      List<String> defaultFilters) {
    Project project = build.project
    ServerSocket sock = new ServerSocket(0)
    int port
    try {
      port = sock.localPort
    } finally {
      sock.close()
    }
    File log = build.buildFile("xlang-${name.toLowerCase()}-test-expansion-service.log")
    log.parentFile.mkdirs()
    List<String> procCmd = command + [port.toString()]
    Process service = new ProcessBuilder(procCmd)
        .directory(dir)
        .redirectErrorStream(true)
        .redirectOutput(log)
        .start()
    try {
      waitForPort(port, 120)
      String filterProp = project.findProperty('filter')?.toString()
      List<String> filters = filterProp != null ? filterProp.tokenize() : defaultFilters
      String threads = project.findProperty('testThreads')?.toString()
      List<String> testArgs = new ArrayList<>(filters)
      if (threads != null && !threads.isEmpty()) {
        testArgs.add("--test-threads=${threads}".toString())
      }
      if (project.hasProperty('includeIgnored')) {
        testArgs.add('--include-ignored')
      }
      project.exec({ ExecSpec spec ->
        spec.workingDir(build.rustDir)
        spec.environment(build.versions.cargoEnv())
        spec.environment(addrVar, "localhost:${port}".toString())
        spec.environment('BEAM_PRISM_PATH', config.prism.prismHostBinary.absolutePath)
        spec.commandLine(['cargo', 'test'] + build.workspaceScope + ['--test', 'xlang'] +
            (testArgs.isEmpty() ? new ArrayList<String>() : ['--'] + testArgs))
      } as Action<ExecSpec>)
    } finally {
      service.destroy()
      service.waitFor()
      project.logger.lifecycle("${name} test expansion service log: ${log}")
    }
  }

  private void registerCore() {
    Project project = build.project
    if (project.gradle.startParameter.taskNames.any { String name ->
      name.contains('Python') || name.contains('Xlang')
    }) {
      project.evaluationDependsOn(':sdks:python')
    }

    containers.registerExpansionImageTasks([
      buildTask       : 'buildTestingExpansionService',
      imageTask       : 'testingExpansionServiceImage',
      bin             : 'beam-runner-tests-expansion-service',
      arch            : xlangArch,
      image           : testingImage,
      buildDescription: 'Build the Linux test expansion service binary of the cross-language suites ([-PexampleArch=<arch>])',
      imageDescription: 'Build the test expansion service image of the cross-language suites, beam_rust_testing_expansion_service ([-PexampleArch=<arch>])',
    ])

    registerDataflowExpansionImageTasks([
      buildTask       : 'buildTestingExpansionServiceDataflow',
      imageTask       : 'testingExpansionServiceDataflowImage',
      bin             : 'beam-runner-tests-expansion-service',
      imageName       : 'testing_expansion_service',
      buildDescription: 'Build the linux/amd64 test expansion service binary for Dataflow',
      imageDescription: 'Build and push the linux/amd64 test expansion service image for Dataflow ([-PdataflowRepositoryRoot=<registry>])',
    ])

    File venvStamp = new File(xlangVenv, 'beam-extras')
    boolean refreshVenv = !venvStamp.exists() || venvStamp.getText('UTF-8').trim() != VENV_EXTRAS ||
        project.hasProperty('refreshXlangPythonEnv')
    project.tasks.register('setupXlangPythonEnv') { Task task ->
      task.group = 'Build'
      task.description = 'Create a virtualenv with the Python SDK of this tree for the cross-language suites ' +
          '([-PpythonVersion=3.12] [-PrefreshXlangPythonEnv])'
      task.onlyIf { refreshVenv }
      if (refreshVenv) {
        task.dependsOn(':sdks:python:sdist')
      }
      task.doLast {
        String interpreter = "python${project.findProperty('pythonVersion') ?: '3'}"
        project.delete(xlangVenv)
        project.exec({ ExecSpec spec ->
          spec.commandLine(interpreter, '-m', 'venv', xlangVenv.absolutePath)
        } as Action<ExecSpec>)
        project.exec({ ExecSpec spec ->
          spec.commandLine(
              "${xlangVenv.absolutePath}/bin/pip", 'install', '--quiet',
              "${pythonSdist.absolutePath}[${VENV_EXTRAS}]")
        } as Action<ExecSpec>)
        venvStamp.setText(VENV_EXTRAS, 'UTF-8')
      }
    }
  }
}
