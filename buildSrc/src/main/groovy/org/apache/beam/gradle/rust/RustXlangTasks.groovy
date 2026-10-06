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
import java.net.ServerSocket
import java.net.Socket
import org.gradle.api.Action
import org.gradle.api.GradleException
import org.gradle.api.JavaVersion
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.tasks.Exec
import org.gradle.api.tasks.bundling.AbstractArchiveTask
import org.gradle.process.ExecSpec

/** Cross-language ValidatesRunner tasks and expansion-service lifecycle helpers. */
@CompileStatic
class RustXlangTasks {

  /** Running Docker container serving a Rust expansion service. */
  static class ServiceHandle {
    int port
    String container
  }

  private static final String PYTHON_SUITE =
      'apache_beam/transforms/validate_runner_xlang_test.py::ValidateRunnerXlangTest'
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
  final String imagePythonVersion
  final String pythonSdkImage

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
    imagePythonVersion = (project.findProperty('pythonVersion') ?: '3.12').toString()
    pythonSdkImage = config.resolveProp('xlang', 'pythonSdkImage')
  }

  static RustXlangTasks register(RustContainerTasks containers) {
    RustXlangTasks xlang = new RustXlangTasks(containers)
    xlang.registerAll()
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

  static int freeLocalPort() {
    ServerSocket socket = new ServerSocket(0)
    try {
      return socket.localPort
    } finally {
      socket.close()
    }
  }

  void requireDataflowRoot(String root = dataflowRoot) {
    if (root == null || root.isEmpty()) {
      throw new GradleException(
          'Set dataflowRepositoryRoot in sdks/rust/.local/sdk.properties or with -P.')
    }
  }

  List<String> selectedTests(Closure<String> transform) {
    String raw = build.project.findProperty('tests')?.toString()
    if (raw == null) {
      return null
    }
    return raw.tokenize(',').collect { String item -> transform.call(item.trim()) }
  }

  void registerDataflowExpansionImageTasks(Map<String, Object> cfg) {
    String root = (cfg.containsKey('root') ? cfg.root : dataflowRoot)?.toString()
    String imageName = cfg.imageName.toString()
    containers.registerExpansionImageTasks([
      buildTask       : cfg.buildTask,
      imageTask       : cfg.imageTask,
      bin             : cfg.bin,
      arch            : 'amd64',
      image           : (root != null && !root.isEmpty())
          ? containers.rustImageName(imageName, root) : 'unset',
      baseImage       : (root != null && !root.isEmpty())
          ? containers.rustImageName('sdk', root) : null,
      push            : true,
      buildDescription: cfg.buildDescription,
      imageDescription: cfg.imageDescription,
    ])
    build.project.tasks.named(cfg.imageTask.toString()).configure { Task task ->
      task.doFirst { requireDataflowRoot(root) }
    }
  }

  ServiceHandle startExpansionService(Map<String, Object> cfg = [:]) {
    String image = (cfg.image ?: testingImage).toString()
    String workerImage = (cfg.workerImage ?: image).toString()
    int port = freeLocalPort()
    String container = "beam-rust-expansion-${port}".toString()
    List<String> cmd = ['docker', 'run', '-d', '--rm', '--name', container, '-p', "${port}:8097".toString()]
    if (cfg.platform != null) {
      cmd.addAll(['--platform', cfg.platform.toString()])
    }
    if (cfg.credentials != null) {
      cmd.addAll([
        '-v', "${cfg.credentials}:/creds.json:ro".toString(),
        '-e', 'GOOGLE_APPLICATION_CREDENTIALS=/creds.json',
      ])
    }
    cmd.addAll([
      '--entrypoint', '/opt/apache/beam/worker_binary',
      image,
      '--port', '8097',
      '--docker-image', workerImage,
    ])
    build.project.exec({ ExecSpec spec -> spec.commandLine(cmd) } as Action<ExecSpec>)
    ServiceHandle handle = new ServiceHandle(port: port, container: container)
    try {
      waitForPort(port, 60)
    } catch (GradleException e) {
      stopExpansionService(handle)
      throw e
    }
    return handle
  }

  void stopExpansionService(ServiceHandle service) {
    build.project.exec({ ExecSpec spec ->
      spec.commandLine('docker', 'rm', '-f', service.container)
      spec.ignoreExitValue = true
      spec.standardOutput = new ByteArrayOutputStream()
    } as Action<ExecSpec>)
  }

  void execWithExpansionService(Map<String, Object> cfg = [:], Closure<?> configureExec) {
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
    int port = freeLocalPort()
    File log = build.buildFile("xlang-${name.toLowerCase()}-test-expansion-service.log")
    log.parentFile.mkdirs()
    Process service = new ProcessBuilder(command + [port.toString()])
        .directory(dir)
        .redirectErrorStream(true)
        .redirectOutput(log)
        .start()
    try {
      waitForPort(port, 120)
      List<String> filters = project.findProperty('filter')?.toString()?.tokenize() ?: defaultFilters
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

  private void registerAll() {
    Project project = build.project
    if (project.gradle.startParameter.taskNames.any { String name ->
      name.contains('CrossLanguage') || name.contains('Python') || name.contains('Xlang')
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

    registerSetupPythonEnv()
    registerPythonSuiteTasks()
    registerServicePrismTasks()
  }

  private void registerSetupPythonEnv() {
    Project project = build.project
    File venvStamp = new File(xlangVenv, 'beam-extras')
    boolean refreshVenv = !venvStamp.exists() ||
        venvStamp.text.trim() != VENV_EXTRAS ||
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
          spec.commandLine("${xlangVenv}/bin/pip", 'install', '--quiet', "${pythonSdist}[${VENV_EXTRAS}]")
        } as Action<ExecSpec>)
        venvStamp.text = VENV_EXTRAS
      }
    }
  }

  private void runPythonSuite(String workerImage, List<String> pipelineOpts, List<String> testOpts) {
    Project pythonProject = build.project.project(':sdks:python')
    List<String> selected = selectedTests { String it -> "${PYTHON_SUITE}::test_${it}".toString() }
        ?: [PYTHON_SUITE]
    String args = BeamModuleExtras.mapToArgString(pythonProject, [
      pipeline_opts: pipelineOpts,
      test_opts    : testOpts + selected,
      suite        : 'rustXlangValidateRunner',
    ])
    execWithExpansionService([workerImage: workerImage]) { ServiceHandle service ->
      ExecSpec spec = (ExecSpec) delegate
      spec.workingDir(pythonDir)
      spec.environment('EXPANSION_PORT', service.port)
      spec.commandLine('sh', '-c',
          ". ${xlangVenv.absolutePath}/bin/activate && ./scripts/run_integration_test.sh ${args}")
    }
  }

  private void registerPythonSuiteTasks() {
    Project project = build.project
    project.tasks.register('validatesCrossLanguageRunnerPythonPrism') { Task task ->
      task.group = 'Verification'
      task.description = 'Run the Python cross-language suite against the Rust test expansion service on Prism ' +
          '([-Ptests=prefix,group_by_key] [-PpythonVersion=3.12])'
      task.dependsOn('testingExpansionServiceImage', 'fetchPrism', 'setupXlangPythonEnv')
      task.doLast {
        runPythonSuite(testingImage, [
          '--runner=PrismRunner',
          '--environment_type=LOOPBACK',
          "--prism_location=${config.prism.prismHostBinary.absolutePath}".toString(),
        ], new ArrayList<String>())
      }
    }

    String pythonTag = (project.rootProject.findProperty('docker-tag') ?: build.versions.sdkVersion).toString()
    String pushedPythonImage = dataflowRoot != null
        ? BeamModuleExtras.containerImageName(
            project,
            "${BeamModuleExtras.dockerDefaultRepoPrefix(project)}python${imagePythonVersion}_sdk",
            dataflowRoot,
            pythonTag)
        : null

    project.tasks.register('pythonSdkDataflowImage', Exec) { Exec task ->
      task.group = 'Build'
      task.description = 'Build and push the linux/amd64 Python SDK container of this tree for the Dataflow ' +
          'cross-language suite ([-PpythonVersion=3.12] [-PdataflowRepositoryRoot=<registry>])'
      task.workingDir(project.rootDir)
      task.commandLine(
          new File(project.rootDir, 'gradlew').absolutePath,
          ":sdks:python:container:py${imagePythonVersion.replace('.', '')}:docker",
          '-Pcontainer-architecture-list=amd64',
          "-Pdocker-repository-root=${dataflowRoot}",
          '-Ppush-containers',
          "-PpythonVersion=${imagePythonVersion}")
      task.doFirst { requireDataflowRoot() }
      task.doLast {
        task.logger.lifecycle(
            "Pushed ${pushedPythonImage}. Set example.xlang.pythonSdkImage=${pushedPythonImage} in sdks/rust/.local/sdk.properties.")
      }
    }

    project.tasks.register('validatesCrossLanguageRunnerPythonDataflow') { Task task ->
      task.group = 'Verification'
      task.description = 'Run the Python cross-language suite against the Rust test expansion service on Dataflow ' +
          '([-Ptests=prefix,group_by_key] [-PxlangParallelism=8] [-PpythonVersion=3.12] [-PpythonSdkImage=<image>])'
      task.dependsOn('testingExpansionServiceImage', 'testingExpansionServiceDataflowImage', 'setupXlangPythonEnv')
      if (pythonSdkImage == null) {
        task.dependsOn(':sdks:python:sdist')
      }
      task.doLast {
        String gcpProject = config.gcpProjectFrom('xlang')
        String gcpTempLocation = config.resolveProp('xlang', 'gcpTempLocation', 'GCP_TEMP_LOCATION')
        if (!gcpProject || !gcpTempLocation) {
          throw new GradleException(
              'Set gcpProject and gcpTempLocation in sdks/rust/.local/sdk.properties or with -P.')
        }
        String region = config.resolveProp('xlang', 'gcpRegion', 'GCP_REGION', 'us-central1')
        List<String> opts = [
          '--runner=TestDataflowRunner',
          "--project=${gcpProject}".toString(),
          "--region=${region}".toString(),
          "--temp_location=${gcpTempLocation}".toString(),
          '--num_workers=1',
        ]
        if (pythonSdkImage != null) {
          opts.add("--sdk_container_image=${pythonSdkImage}".toString())
          opts.add('--sdk_location=container')
        } else {
          opts.add("--sdk_location=${pythonSdist.absolutePath}".toString())
        }
        ['network', 'subnetwork'].each { String key ->
          String val = config.resolveProp('xlang', key)
          if (val != null && !val.isEmpty()) {
            opts.add("--${key}=${val}".toString())
          }
        }
        String machineType = config.resolveProp('xlang', 'workerMachineType')
        if (machineType != null && !machineType.isEmpty()) {
          opts.add("--machine_type=${machineType}".toString())
        }
        String parallelism = (project.findProperty('xlangParallelism') ?: '8').toString()
        runPythonSuite(dataflowImage, opts, ["--numprocesses=${parallelism}".toString(), '--timeout=1800'])
      }
    }
  }

  private void registerServicePrismTasks() {
    Project project = build.project
    String javaTestServiceProject = ':sdks:java:testing:expansion-service'
    String javaMajor = JavaVersion.current().majorVersion
    String javaSdkImage = "${BeamModuleExtras.dockerDefaultRepoRoot(project)}/" +
        "${BeamModuleExtras.dockerDefaultRepoPrefix(project)}java${javaMajor}_sdk:${build.versions.sdkVersion}"

    project.tasks.register('validatesCrossLanguageRunnerJavaServicePrism') { Task task ->
      task.group = 'Verification'
      task.description = 'Run the Rust cross-language suite against the Java test expansion service on Prism ' +
          '([-Pfilter=<test name>] [-PtestThreads=<n>] [-PincludeIgnored] [-PbuildJavaContainer])'
      task.dependsOn("${javaTestServiceProject}:buildTestExpansionServiceJar", 'fetchPrism')
      if (project.hasProperty('buildJavaContainer')) {
        task.dependsOn(":sdks:java:container:java${javaMajor}:docker")
      }
      task.doLast {
        if (!dockerReachable()) {
          throw new GradleException(
              'Docker is not reachable. The cross-language suite needs it to start the Java SDK container.')
        }
        if (!imageExists(javaSdkImage)) {
          throw new GradleException("${javaSdkImage} is not available locally. Build it with " +
              "./gradlew :sdks:java:container:java${javaMajor}:docker, or add -PbuildJavaContainer.")
        }
        AbstractArchiveTask jarTask = (AbstractArchiveTask) project.project(javaTestServiceProject)
            .tasks.named('buildTestExpansionServiceJar').get()
        File jar = jarTask.archiveFile.get().asFile
        String java = "${System.getProperty('java.home')}/bin/java"
        runXlangTestsAgainstService(
            'Java', [java, '-jar', jar.absolutePath], build.rustDir,
            'BEAM_JAVA_TEST_EXPANSION_ADDR', new ArrayList<String>())
      }
    }

  }
}
