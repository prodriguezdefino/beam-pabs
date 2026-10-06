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
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.file.ConfigurableFileTree
import org.gradle.api.file.CopySpec
import org.gradle.api.tasks.Exec
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskProvider
import org.gradle.process.ExecSpec

/**
 * Provides the Prism binary for tests and local launches.
 *
 * <p>A release build uses the published Prism binary. A SNAPSHOT build compiles Prism from
 * {@code sdks/go/cmd/prism} with the pinned Go toolchain unless {@code -PprismVersion=<release>}
 * is set.
 */
@CompileStatic
class RustPrismTasks {
  final RustSdkBuild build
  final String hostOs
  final String hostArch
  final boolean prismFromSource
  final String prismVersion
  final File prismCacheDir
  final File prismHostBinary
  final File prismLinuxBinary

  private RustPrismTasks(RustSdkBuild build) {
    this.build = build
    Project project = build.project
    hostOs = System.getProperty('os.name').toLowerCase().contains('mac') ? 'darwin' : 'linux'
    hostArch = System.getProperty('os.arch') in ['aarch64', 'arm64'] ? 'arm64' : 'amd64'
    String versionOverride = project.findProperty('prismVersion')?.toString()
    prismFromSource = versionOverride == null && build.versions.beamVersion.endsWith('-SNAPSHOT')
    prismVersion = versionOverride ?: build.versions.releaseVersion
    prismCacheDir = new File(System.getProperty('user.home'), '.apache_beam/cache/prism/bin')
    prismHostBinary = binaryFor(hostOs)
    prismLinuxBinary = binaryFor('linux')
  }

  /** Registers the {@code fetchPrism} task and wires {@code BEAM_PRISM_PATH} into test tasks. */
  static RustPrismTasks register(RustSdkBuild build) {
    RustPrismTasks prism = new RustPrismTasks(build)
    prism.registerTasks()
    return prism
  }

  File binaryFor(String os) {
    return prismFromSource
        ? build.buildFile("prism/prism-${os}-${hostArch}")
        : new File(prismCacheDir, "apache_beam-v${prismVersion}-prism-${os}-${hostArch}")
  }

  /** Configures a task that builds or downloads the Prism binary for {@code goos}. */
  void configureTask(Task task, File binary, String goos) {
    Project project = build.project
    task.outputs.file(binary)
    task.inputs.property('goos', goos)
    task.inputs.property('goarch', hostArch)
    if (prismFromSource) {
      task.dependsOn(':runners:prism:goPrepare')
      ConfigurableFileTree goSources = project.fileTree(new File(project.rootDir, 'sdks/go'))
      goSources.include('**/*.go')
      task.inputs.files(goSources).withPathSensitivity(PathSensitivity.RELATIVE)
      task.inputs.files(
          new File(project.rootDir, 'sdks/go.mod'),
          new File(project.rootDir, 'sdks/go.sum'))
          .withPathSensitivity(PathSensitivity.RELATIVE)
    } else {
      task.inputs.property('prismVersion', prismVersion)
    }
    task.doLast { provideBinary(binary, goos) }
  }

  private void registerTasks() {
    Project project = build.project
    project.tasks.register('fetchPrism') { Task task ->
      task.group = 'Build'
      task.description = prismFromSource
          ? 'Build a host-native Prism from sdks/go/cmd/prism'
          : "Download the host-native Prism ${prismVersion} into the shared Beam cache"
      configureTask(task, prismHostBinary, hostOs)
    }

    build.testTasks.each { String pkg, TaskProvider<Exec> tp ->
      if ('apache-beam-runner-prism' in build.workspace.crateClosure[pkg]) {
        tp.configure { Exec task ->
          task.dependsOn('fetchPrism')
          task.inputs.file(prismHostBinary).withPathSensitivity(PathSensitivity.NONE)
          task.environment('BEAM_PRISM_PATH', prismHostBinary.absolutePath)
        }
      }
    }

    project.tasks.matching { Task task -> task.name == 'testBenchmarks' }.configureEach { Task task ->
      task.dependsOn('fetchPrism')
      if (task instanceof Exec) {
        ((Exec) task).environment('BEAM_PRISM_PATH', prismHostBinary.absolutePath)
      }
    }
  }

  private void provideBinary(File binary, String goos) {
    if (prismFromSource) {
      compilePrism(binary, goos)
    } else {
      downloadPrism(binary)
    }
  }

  private void downloadPrism(File binary) {
    Project project = build.project
    prismCacheDir.mkdirs()
    File archive = new File(prismCacheDir, "${binary.name}.zip")
    project.exec({ ExecSpec spec ->
      spec.commandLine('curl', '-sSLf', '-o', archive.absolutePath,
          "https://github.com/apache/beam/releases/download/v${prismVersion}/${binary.name}.zip")
    } as Action<ExecSpec>)
    project.copy({ CopySpec spec ->
      spec.from(project.zipTree(archive))
      spec.into(prismCacheDir)
    } as Action<CopySpec>)
    binary.setExecutable(true)
  }

  private void compilePrism(File binary, String goos) {
    Project project = build.project
    binary.parentFile.mkdirs()
    String goCmd = project.project(':runners:prism').extensions.extraProperties.get('goCmd').toString()
    File prismMain = new File(project.rootDir, 'sdks/go/cmd/prism')
    project.exec({ ExecSpec spec ->
      spec.workingDir(new File(project.rootDir, 'sdks'))
      spec.environment('GOOS', goos)
      spec.environment('GOARCH', hostArch)
      spec.environment('CGO_ENABLED', '0')
      spec.commandLine('sh', '-c', "${goCmd} build -o ${binary.absolutePath} ${prismMain.absolutePath}")
    } as Action<ExecSpec>)
  }
}
