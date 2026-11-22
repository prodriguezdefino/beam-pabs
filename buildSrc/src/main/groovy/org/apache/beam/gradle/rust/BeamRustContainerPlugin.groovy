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
import org.apache.beam.gradle.BeamDockerPlugin
import org.apache.beam.gradle.BeamModulePlugin
import org.apache.beam.gradle.rust.container.RustContainerTasks
import org.gradle.api.Action
import org.gradle.api.GradleException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.file.ConfigurableFileTree
import org.gradle.api.file.CopySpec

/** Builds the Rust SDK base container image ({@code :sdks:rust:container}). */
@CompileStatic
class BeamRustContainerPlugin implements Plugin<Project> {

  @Override
  void apply(Project project) {
    project.pluginManager.apply(BeamModulePlugin)
    BeamModuleExtras.applyDockerNature(project)
    project.evaluationDependsOn(':sdks:rust')

    List<String> architectures = BeamModuleExtras.containerArchitectures(project)
    File buildDir = project.layout.buildDirectory.get().asFile
    File targetRoot = new File(buildDir, 'target')

    project.tasks.register('buildBoot') { Task task ->
      task.description = 'Build Rust container boot binary'
      task.inputs.property('containerArchitectures', architectures.join(','))
      ConfigurableFileTree sources = project.fileTree(new File(project.rootDir, 'sdks/rust'))
      sources.include('**/*.rs', '**/Cargo.toml', 'Cargo.lock')
      sources.exclude('target/**', 'container/**')
      task.inputs.files(sources)
      task.outputs.dir(targetRoot)

      task.doFirst { project.delete(targetRoot) }
      task.doLast {
        Project rustProject = project.project(':sdks:rust')
        RustContainerTasks containerTasks = rustProject.extensions.getByType(RustContainerTasks)
        String sdkVersion = rustProject.property('sdk_version').toString()
        File rustBuildDir = rustProject.layout.buildDirectory.get().asFile
        architectures.each { String arch ->
          File archDir = new File(targetRoot, "linux_${arch}")
          archDir.mkdirs()
          containerTasks.compileWorkers(['boot'], arch, sdkVersion)
          project.copy({ CopySpec spec ->
            spec.from(new File(rustBuildDir, "linux_${arch}/boot"))
            spec.into(archDir)
          } as Action<CopySpec>)
        }
        architectures.each { String arch ->
          File boot = new File(targetRoot, "linux_${arch}/boot")
          if (!boot.exists()) {
            throw new GradleException(
                "buildBoot produced no binary for linux_${arch} (expected ${boot}). " +
                'Check the architecture list: -Pcontainer-architecture-list=amd64,arm64')
          }
        }
      }
    }

    boolean pushContainers = project.rootProject.hasProperty('isRelease') ||
        project.rootProject.hasProperty('push-containers')
    String repoRoot = project.rootProject.hasProperty('docker-repository-root')
        ? project.rootProject.property('docker-repository-root').toString()
        : BeamModuleExtras.dockerDefaultRepoRoot(project)
    String tag = project.rootProject.hasProperty('docker-tag')
        ? project.rootProject.property('docker-tag').toString()
        : project.property('sdk_version').toString()

    BeamDockerPlugin.DockerExtension docker = project.extensions.getByType(BeamDockerPlugin.DockerExtension)
    docker.name = BeamModuleExtras.containerImageName(
        project, "${BeamModuleExtras.dockerDefaultRepoPrefix(project)}rust_sdk", repoRoot, tag)
    docker.tags(BeamModuleExtras.containerImageTags(project) as String[])
    docker.files('./build/')
    docker.buildx = BeamModuleExtras.useBuildx(project)
    docker.platform(BeamModuleExtras.containerPlatforms(project) as String[])
    docker.load = BeamModuleExtras.useBuildx(project) && !pushContainers
    docker.push = pushContainers

    project.tasks.named('dockerPrepare').configure { Task task ->
      task.dependsOn('buildBoot')
      task.doFirst { project.delete(new File(buildDir, 'docker')) }
    }

    project.tasks.register('pushAll') { Task task ->
      task.dependsOn(':sdks:rust:container:docker')
    }
  }
}
