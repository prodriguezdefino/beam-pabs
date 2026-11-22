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
package org.apache.beam.gradle.rust.io

import groovy.transform.CompileStatic
import java.net.ServerSocket
import java.net.Socket
import org.apache.beam.gradle.rust.RustConfig
import org.apache.beam.gradle.rust.RustSdkBuild
import org.apache.beam.gradle.rust.container.RustContainerTasks
import org.apache.beam.gradle.rust.xlang.RustXlangTasks
import org.gradle.api.Action
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.tasks.Exec
import org.gradle.process.ExecSpec

/** Expansion-service images, integration tests and copy bench of the Rust I/O connectors. */
@CompileStatic
class RustIoTasks {

  final RustXlangTasks xlang
  final RustContainerTasks containers
  final RustConfig config
  final RustSdkBuild build
  final String connectorDataflowRoot

  private RustIoTasks(RustXlangTasks xlang) {
    this.xlang = xlang
    this.containers = xlang.containers
    this.config = xlang.config
    this.build = xlang.build
    connectorDataflowRoot = config.resolveProp('benchmark', 'dataflowRepositoryRoot') ?: xlang.dataflowRoot
  }

  static RustIoTasks register(RustXlangTasks xlang) {
    RustIoTasks io = new RustIoTasks(xlang)
    io.registerGcp()
    io.registerKafka()
    io.registerKafkaCopyBenchmark()
    io.registerIceberg()
    return io
  }

  String requireItGcpProject() {
    String gcpProject = config.gcpProjectFrom('it')
    if (!gcpProject) {
      throw new GradleException('Set gcpProject in sdks/rust/.local/sdk.properties or with -P.')
    }
    return gcpProject
  }

  void runProviderPythonTests(
      String image,
      File testsDir,
      Map<String, String> testEnv,
      String workerImage = null,
      boolean needsCredentials = true) {
    String credentials = needsCredentials ? config.googleCredentials() : null
    if (needsCredentials && !credentials) {
      throw new GradleException(
          'No Application Default Credentials. Run gcloud auth application-default login.')
    }
    List<String> modules = xlang.selectedTests { String name -> "${testsDir.absolutePath}/${name}_test.py".toString() }
        ?: [testsDir.absolutePath]
    String filter = build.project.findProperty('testFilter')?.toString()
    List<String> selection = filter != null && !filter.isEmpty() ? ['-k', filter] + modules : modules
    Map<String, Object> svcCfg = [image: (Object) image]
    if (workerImage != null) {
      svcCfg['workerImage'] = workerImage
    }
    if (credentials != null) {
      svcCfg['credentials'] = credentials
    }
    xlang.execWithExpansionService(svcCfg) { RustXlangTasks.ServiceHandle service ->
      ExecSpec spec = (ExecSpec) delegate
      spec.workingDir(testsDir)
      spec.environment(testEnv)
      spec.environment('EXPANSION_SERVICE', "localhost:${service.port}".toString())
      spec.environment('PRISM_LOCATION', config.prism.prismHostBinary.absolutePath)
      if (credentials != null) {
        spec.environment('GOOGLE_APPLICATION_CREDENTIALS', credentials)
      }
      spec.commandLine(
          ["${xlang.xlangVenv.absolutePath}/bin/python".toString(), '-m', 'pytest', '-v', '-p', 'no:cacheprovider'] +
          selection)
    }
  }

  private void registerGcp() {
    Project project = build.project
    containers.registerExpansionImageTasks([
      buildTask       : 'buildGcpExpansionService',
      imageTask       : 'gcpExpansionServiceImage',
      bin             : 'beam-gcp-expansion-service',
      arch            : config.workerArch,
      image           : containers.rustImageName('gcp_expansion_service'),
      buildDescription: 'Build the Linux expansion service binary of the Google Cloud connectors (-PexampleArch=amd64)',
      imageDescription: 'Build the expansion service image of the Google Cloud connectors, beam_rust_gcp_expansion_service ([-PbaseImage=<image>] [-PexampleArch=amd64] [-Ppush-containers])',
    ])

    xlang.registerDataflowExpansionImageTasks([
      buildTask       : 'buildGcpExpansionServiceDataflow',
      imageTask       : 'gcpExpansionServiceDataflowImage',
      bin             : 'beam-gcp-expansion-service',
      imageName       : 'gcp_expansion_service',
      root            : connectorDataflowRoot,
      buildDescription: 'Build the linux/amd64 expansion service binary of the Google Cloud connectors for Dataflow',
      imageDescription: 'Build and push the linux/amd64 beam_rust_gcp_expansion_service image for Dataflow ([-PdataflowRepositoryRoot=<registry>])',
    ])

    List<String> testExcludes = build.workspace.crateTestExcludes['apache-beam-runner-tests']
        ?: new ArrayList<String>()
    project.tasks.register('bigQueryIntegrationTest', Exec) { Exec task ->
      task.group = 'Verification'
      task.description = 'Read public BigQuery tables with the Storage Read connector on Prism ([-PgcpProject=<project>])'
      task.dependsOn('fetchPrism')
      task.workingDir(build.rustDir)
      task.doFirst {
        task.environment(config.launchEnvironment())
        task.environment('BEAM_TEST_PIPELINE_OPTIONS', "--runner=prism --project=${requireItGcpProject()}".toString())
      }
      task.commandLine(['cargo', 'test'] + build.workspaceScope + testExcludes +
          ['--test', 'io', '--', '--ignored', 'bigquery_storage_it::'])
    }

    String gcpTestingImage = containers.rustImageName('gcp_expansion_service_testing')
    String gcpDataflowImage = xlang.dataflowRoot != null
        ? containers.rustImageName('gcp_expansion_service_testing', xlang.dataflowRoot) : null
    File gcpTestsDir = new File(build.rustDir, 'beam/io/gcp/expansion-service/tests/python')

    containers.registerExpansionImageTasks([
      buildTask       : 'buildGcpExpansionServiceTesting',
      imageTask       : 'gcpExpansionServiceTestingImage',
      bin             : 'beam-gcp-expansion-service',
      arch            : xlang.xlangArch,
      image           : gcpTestingImage,
      buildDescription: 'Build the Linux expansion service binary of the Google Cloud connectors for its Python tests ([-PexampleArch=<arch>])',
      imageDescription: 'Build the image of the Google Cloud connectors for its Python tests, beam_rust_gcp_expansion_service_testing ([-PexampleArch=<arch>])',
    ])

    xlang.registerDataflowExpansionImageTasks([
      buildTask       : 'buildGcpExpansionServiceTestingDataflow',
      imageTask       : 'gcpExpansionServiceTestingDataflowImage',
      bin             : 'beam-gcp-expansion-service',
      imageName       : 'gcp_expansion_service_testing',
      buildDescription: 'Build the linux/amd64 expansion service binary of the Google Cloud connectors for Dataflow',
      imageDescription: 'Build and push the linux/amd64 image of the Google Cloud connectors for its Python tests on Dataflow ([-PdataflowRepositoryRoot=<registry>])',
    ])

    project.tasks.register('gcpExpansionServicePythonTest') { Task task ->
      task.group = 'Verification'
      task.description = 'Run the Python tests of the Google Cloud providers on Prism; ' +
          'gcpExpansionServicePythonTestDataflow runs them on Dataflow ' +
          '([-Ptests=bigquery_storage] [-PtestFilter=<pytest -k>] [-PgcpProject=<project>] [-PpythonVersion=3.12])'
      task.dependsOn('gcpExpansionServiceTestingImage', 'fetchPrism', 'setupXlangPythonEnv')
      task.doLast {
        runProviderPythonTests(gcpTestingImage, gcpTestsDir, [GCP_PROJECT: requireItGcpProject()])
      }
    }

    project.tasks.register('gcpExpansionServicePythonTestDataflow') { Task task ->
      task.group = 'Verification'
      task.description = 'Run the Python tests of the Google Cloud providers on Dataflow ' +
          '([-Ptests=bigquery_storage] [-PtestFilter=<pytest -k>] [-PgcpProject=<project>] [-PpythonVersion=3.12] [-PpythonSdkImage=<image>])'
      task.dependsOn('gcpExpansionServiceTestingImage', 'gcpExpansionServiceTestingDataflowImage', 'setupXlangPythonEnv')
      if (xlang.pythonSdkImage == null) {
        task.dependsOn(':sdks:python:sdist')
      }
      task.doLast {
        runProviderPythonTests(
            gcpTestingImage,
            gcpTestsDir,
            [
              GCP_PROJECT     : requireItGcpProject(),
              PIPELINE_OPTIONS: xlang.pythonDataflowPipelineOptions().join(' '),
            ],
            gcpDataflowImage)
      }
    }
  }

  private void registerKafka() {
    Project project = build.project
    containers.registerExpansionImageTasks([
      buildTask       : 'buildKafkaExpansionService',
      imageTask       : 'kafkaExpansionServiceImage',
      bin             : 'beam-kafka-expansion-service',
      arch            : config.workerArch,
      image           : containers.rustImageName('kafka_expansion_service'),
      buildDescription: 'Build the Linux expansion service binary of the Kafka connector of the Rust SDK (-PexampleArch=amd64)',
      imageDescription: 'Build the expansion service image of the Kafka connector of the Rust SDK, beam_rust_kafka_expansion_service ([-PbaseImage=<image>] [-PexampleArch=amd64] [-Ppush-containers])',
    ])

    xlang.registerDataflowExpansionImageTasks([
      buildTask       : 'buildKafkaExpansionServiceDataflow',
      imageTask       : 'kafkaExpansionServiceDataflowImage',
      bin             : 'beam-kafka-expansion-service',
      imageName       : 'kafka_expansion_service',
      root            : connectorDataflowRoot,
      buildDescription: 'Build the linux/amd64 expansion service binary of the Kafka connector for Dataflow',
      imageDescription: 'Build and push the linux/amd64 beam_rust_kafka_expansion_service image for Dataflow ([-PdataflowRepositoryRoot=<registry>])',
    ])

    String kafkaTestingImage = containers.rustImageName('kafka_expansion_service_testing')
    File kafkaTestsDir = new File(build.rustDir, 'beam/io/kafka/expansion-service/tests/python')

    containers.registerExpansionImageTasks([
      buildTask       : 'buildKafkaExpansionServiceTesting',
      imageTask       : 'kafkaExpansionServiceTestingImage',
      bin             : 'beam-kafka-expansion-service',
      arch            : xlang.xlangArch,
      image           : kafkaTestingImage,
      buildDescription: 'Build the Linux expansion service binary of the Kafka connector of the Rust SDK for its Python tests ([-PexampleArch=<arch>])',
      imageDescription: 'Build the image of the Kafka connector of the Rust SDK for its Python tests, beam_rust_kafka_expansion_service_testing ([-PexampleArch=<arch>])',
    ])

    project.tasks.register('kafkaExpansionServicePythonTest') { Task task ->
      task.group = 'Verification'
      task.description = 'Run the Python tests of the Rust Kafka providers on Prism, against a local broker ' +
          'in Docker ([-Ptests=kafka] [-PtestFilter=<pytest -k>] [-PpythonVersion=3.12])'
      task.dependsOn('kafkaExpansionServiceTestingImage', 'fetchPrism', 'setupXlangPythonEnv')
      task.doLast {
        // Use the VM socket path when Docker runs inside a VM such as Colima.
        String socket = System.getenv('TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE') ?: '/var/run/docker.sock'
        runProviderPythonTests(
            kafkaTestingImage,
            kafkaTestsDir,
            [TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE: socket],
            null,
            false)
      }
    }
  }

  private void registerKafkaCopyBenchmark() {
    Project project = build.project
    project.tasks.register('kafkaCopyBenchmark') { Task task ->
      task.group = 'Verification'
      task.description = 'Run the local Kafka copy throughput bench on Prism; starts a Docker broker when none listens on localhost:9092 ' +
          '([-Precords=1000000] [-Pruns=3] [-Pformats=bytes,json] [-Ppartitions=16] [-PbootstrapServers=localhost:9092] ' +
          '[-PcargoProfile=worker] [-PextraArgs=<flags>])'
      task.dependsOn('fetchPrism')
      task.doLast {
        String host = config.dockerHost()
        String servers = project.findProperty('bootstrapServers')?.toString()
        String container = null
        if (!servers) {
          if (portOpen('localhost', 9092)) {
            servers = 'localhost:9092'
          } else if (xlang.dockerReachable()) {
            ServerSocket sock = new ServerSocket(0)
            int port
            try {
              port = sock.localPort
            } finally {
              sock.close()
            }
            container = "beam-kafka-bench-${port}"
            servers = "localhost:${port}"
            project.exec({ ExecSpec spec ->
              if (host != null) {
                spec.environment('DOCKER_HOST', host)
              }
              spec.commandLine(
                  'docker', 'run', '-d', '--rm', '--name', container, '-p', "${port}:9092".toString(),
                  '-e', 'KAFKA_NODE_ID=1',
                  '-e', 'KAFKA_PROCESS_ROLES=broker,controller',
                  '-e', 'KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093',
                  '-e', "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://${servers}".toString(),
                  '-e', 'KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER',
                  '-e', 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
                  '-e', 'KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093',
                  '-e', 'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1',
                  'apache/kafka:3.9.0')
            } as Action<ExecSpec>)
            try {
              waitForKafkaContainer(container, 60)
            } catch (Exception e) {
              removeContainer(host, container)
              throw e
            }
          } else {
            servers = 'localhost:9092'
          }
        }
        try {
          List<String> benchArgs = ["--bootstrap_servers=${servers}".toString()]
          ['records': 'records', 'runs': 'runs', 'formats': 'formats', 'partitions': 'partitions']
              .each { String prop, String flag ->
                String val = project.findProperty(prop)?.toString()
                if (val != null && !val.isEmpty()) {
                  benchArgs.add("--${flag}=${val}".toString())
                }
              }
          benchArgs.addAll(project.findProperty('extraArgs')?.toString()?.tokenize(' ') ?: new ArrayList<String>())
          String cargoProfile = (project.findProperty('cargoProfile') ?: 'worker').toString()
          println("kafkaCopyBenchmark: Cargo profile ${cargoProfile}")
          project.exec({ ExecSpec spec ->
            spec.workingDir(build.rustDir)
            spec.environment(config.launchEnvironment())
            spec.commandLine(['cargo', 'bench'] + build.workspaceScope +
                ['--profile', cargoProfile, '--bench', 'copy_throughput', '--'] + benchArgs)
          } as Action<ExecSpec>)
        } finally {
          if (container != null) {
            removeContainer(host, container)
          }
        }
      }
    }
  }

  private void registerIceberg() {
    Project project = build.project
    containers.registerExpansionImageTasks([
      buildTask       : 'buildIcebergExpansionService',
      imageTask       : 'icebergExpansionServiceImage',
      bin             : 'beam-iceberg-expansion-service',
      arch            : config.workerArch,
      image           : containers.rustImageName('iceberg_expansion_service'),
      buildDescription: 'Build the Linux expansion service binary of the Iceberg connector of the Rust SDK (-PexampleArch=amd64)',
      imageDescription: 'Build the expansion service image of the Iceberg connector of the Rust SDK, beam_rust_iceberg_expansion_service ([-PbaseImage=<image>] [-PexampleArch=amd64] [-Ppush-containers])',
    ])

    xlang.registerDataflowExpansionImageTasks([
      buildTask       : 'buildIcebergExpansionServiceDataflow',
      imageTask       : 'icebergExpansionServiceDataflowImage',
      bin             : 'beam-iceberg-expansion-service',
      imageName       : 'iceberg_expansion_service',
      root            : connectorDataflowRoot,
      buildDescription: 'Build the linux/amd64 expansion service binary of the Iceberg connector for Dataflow',
      imageDescription: 'Build and push the linux/amd64 beam_rust_iceberg_expansion_service image for Dataflow ([-PdataflowRepositoryRoot=<registry>])',
    ])

    String icebergTestingImage = containers.rustImageName('iceberg_expansion_service_testing')
    File icebergTestsDir = new File(build.rustDir, 'beam/io/iceberg/expansion-service/tests/python')

    containers.registerExpansionImageTasks([
      buildTask       : 'buildIcebergExpansionServiceTesting',
      imageTask       : 'icebergExpansionServiceTestingImage',
      bin             : 'beam-iceberg-expansion-service',
      arch            : xlang.xlangArch,
      image           : icebergTestingImage,
      buildDescription: 'Build the Linux expansion service binary of the Iceberg connector of the Rust SDK for its Python tests ([-PexampleArch=<arch>])',
      imageDescription: 'Build the image of the Iceberg connector of the Rust SDK for its Python tests, beam_rust_iceberg_expansion_service_testing ([-PexampleArch=<arch>])',
    ])

    project.tasks.register('icebergExpansionServicePythonTest') { Task task ->
      task.group = 'Verification'
      task.description = 'Run the Python tests of the Rust Iceberg provider on Prism, against a local mock ' +
          'REST catalog ([-Ptests=iceberg] [-PtestFilter=<pytest -k>] [-PpythonVersion=3.12])'
      task.dependsOn('icebergExpansionServiceTestingImage', 'fetchPrism', 'setupXlangPythonEnv')
      task.doLast {
        runProviderPythonTests(icebergTestingImage, icebergTestsDir, new LinkedHashMap<String, String>(), null, false)
      }
    }
  }

  private static boolean portOpen(String host, int port) {
    try {
      new Socket(host, port).close()
      return true
    } catch (IOException ignored) {
      return false
    }
  }

  private void waitForKafkaContainer(String container, int timeoutSeconds) {
    String host = config.dockerHost()
    long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L
    while (System.currentTimeMillis() < deadline) {
      ByteArrayOutputStream out = new ByteArrayOutputStream()
      build.project.exec({ ExecSpec spec ->
        if (host != null) {
          spec.environment('DOCKER_HOST', host)
        }
        spec.commandLine('docker', 'logs', container)
        spec.standardOutput = out
        spec.errorOutput = out
        spec.ignoreExitValue = true
      } as Action<ExecSpec>)
      if (out.toString('UTF-8').contains('Kafka Server started')) {
        return
      }
      Thread.sleep(500L)
    }
    throw new GradleException("Kafka container ${container} did not start within ${timeoutSeconds} s.")
  }

  private void removeContainer(String host, String container) {
    build.project.exec({ ExecSpec spec ->
      if (host != null) {
        spec.environment('DOCKER_HOST', host)
      }
      spec.commandLine('docker', 'rm', '-f', container)
      spec.ignoreExitValue = true
      spec.standardOutput = new ByteArrayOutputStream()
    } as Action<ExecSpec>)
  }
}
