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
import org.apache.beam.gradle.rust.runner.RustPrismTasks
import org.gradle.api.Action
import org.gradle.api.Project
import org.gradle.api.file.ConfigurableFileTree
import org.gradle.process.ExecResult
import org.gradle.process.ExecSpec

/** Shared example selection, local properties, GCP flags and launch environment. */
@CompileStatic
class RustConfig {
  final RustSdkBuild build
  final RustPrismTasks prism
  final ConfigurableFileTree rustWorkspaceTree
  final Properties localProps = new Properties()
  final String selectedExample
  final String workerArch

  RustConfig(RustSdkBuild build, RustPrismTasks prism) {
    this.build = build
    this.prism = prism
    Project project = build.project

    rustWorkspaceTree = project.fileTree(build.rustDir)
    rustWorkspaceTree.include('**/*.rs', '**/Cargo.toml', 'Cargo.lock')
    rustWorkspaceTree.exclude('target/**', 'container/**', 'build/**')

    File localPropsFile = project.file('.local/sdk.properties')
    if (localPropsFile.exists()) {
      localPropsFile.withInputStream { InputStream input -> localProps.load(input) }
    }

    selectedExample = (project.findProperty('example') ?: 'wordcount').toString()
    workerArch = (project.findProperty('exampleArch') ?: 'amd64').toString()
  }

  File workerBinaryFor(String example, String arch) {
    return build.buildFile("linux_${arch}/${example}")
  }

  /** Resolves {@code -P<prop>}, then {@code example.<scope>.<prop>}, {@code <prop>}, env and fallback. */
  String resolveProp(String scope, String prop, String env = null, String fallback = null) {
    Object projectProp = build.project.findProperty(prop)
    if (projectProp != null) {
      return projectProp.toString()
    }
    String scoped = localProps.getProperty("example.${scope}.${prop}")
    if (scoped != null) {
      return scoped
    }
    String local = localProps.getProperty(prop)
    if (local != null) {
      return local
    }
    String fromEnv = env != null ? System.getenv(env) : null
    return fromEnv != null ? fromEnv : fallback
  }

  String suppliedWorkerBinary(String example) {
    return resolveProp(example, 'workerBinary')
  }

  String gcpProjectFrom(String scope) {
    String configured = resolveProp(scope, 'gcpProject', 'GCP_PROJECT')
    if (configured != null) {
      return configured
    }
    try {
      ByteArrayOutputStream stdout = new ByteArrayOutputStream()
      build.project.exec({ ExecSpec spec ->
        spec.commandLine('gcloud', 'config', 'get-value', 'project')
        spec.standardOutput = stdout
        spec.errorOutput = new ByteArrayOutputStream()
        spec.ignoreExitValue = true
      } as Action<ExecSpec>)
      String value = stdout.toString('UTF-8').trim()
      return (value.isEmpty() || value.contains('(unset)')) ? null : value
    } catch (Exception ignored) {
      return null
    }
  }

  List<String> portableGcpArgs(String scope) {
    String gcpProject = gcpProjectFrom(scope)
    String gcpRegion = resolveProp(scope, 'gcpRegion', 'GCP_REGION')
    String gcpTempLocation = resolveProp(scope, 'gcpTempLocation', 'GCP_TEMP_LOCATION')
    return [
      gcpProject != null ? "--project=${gcpProject}".toString() : null,
      gcpRegion != null ? "--region=${gcpRegion}".toString() : null,
      gcpTempLocation != null ? "--temp_location=${gcpTempLocation}".toString() : null,
    ].findAll { String arg -> arg != null }
  }

  String dockerHost() {
    String configured = System.getenv('DOCKER_HOST')
    if (configured != null) {
      return configured
    }
    try {
      ByteArrayOutputStream stdout = new ByteArrayOutputStream()
      ExecResult result = build.project.exec({ ExecSpec spec ->
        spec.commandLine('docker', 'context', 'inspect', '--format', '{{.Endpoints.docker.Host}}')
        spec.standardOutput = stdout
        spec.errorOutput = new ByteArrayOutputStream()
        spec.ignoreExitValue = true
      } as Action<ExecSpec>)
      String endpoint = stdout.toString('UTF-8').trim()
      return (result.exitValue == 0 && !endpoint.isEmpty()) ? endpoint : null
    } catch (Exception ignored) {
      return null
    }
  }

  String googleCredentials() {
    String configured = System.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if (configured != null) {
      return configured
    }
    File wellKnown = new File(
        System.getProperty('user.home'), '.config/gcloud/application_default_credentials.json')
    return wellKnown.exists() ? wellKnown.absolutePath : null
  }

  Map<String, String> launchEnvironment() {
    Map<String, String> env = [:]
    env.putAll(build.versions.cargoEnv())
    env['BEAM_PRISM_PATH'] = prism.prismHostBinary.absolutePath
    String docker = dockerHost()
    if (docker != null) {
      env['DOCKER_HOST'] = docker
    }
    String credentials = googleCredentials()
    if (credentials != null) {
      env['GOOGLE_APPLICATION_CREDENTIALS'] = credentials
    }
    return env
  }
}
