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
package org.apache.beam.gradle.rust.container

import groovy.transform.CompileStatic
import java.security.MessageDigest
import org.apache.beam.gradle.rust.BeamModuleExtras
import org.apache.beam.gradle.rust.RustConfig
import org.apache.beam.gradle.rust.RustSdkBuild
import org.gradle.api.Action
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.file.CopySpec
import org.gradle.api.tasks.Exec
import org.gradle.internal.os.OperatingSystem
import org.gradle.process.ExecResult
import org.gradle.process.ExecSpec

/** Container images and Linux worker compilation for the Rust SDK. */
@CompileStatic
class RustContainerTasks {
  final RustConfig config
  final RustSdkBuild build
  final String rustBuilderImage
  final String rustImageRoot
  private final Map<String, String> expansionBinaryTasks = [:]

  private RustContainerTasks(RustConfig config) {
    this.config = config
    this.build = config.build
    Project project = build.project
    rustBuilderImage = (project.findProperty('rustBuilderImage') ?: 'beam_rust_builder:latest').toString()
    rustImageRoot = (project.rootProject.findProperty('docker-repository-root')
        ?: BeamModuleExtras.dockerDefaultRepoRoot(project)).toString()
  }

  /** Registers the worker build and container image tasks. */
  static RustContainerTasks register(RustConfig config) {
    RustContainerTasks tasks = new RustContainerTasks(config)
    tasks.registerAll()
    return tasks
  }

  String rustImageName(String name, String root = rustImageRoot) {
    Project project = build.project
    String tag = (project.rootProject.findProperty('docker-tag') ?: build.versions.sdkVersion).toString()
    return BeamModuleExtras.containerImageName(
        project, "${BeamModuleExtras.dockerDefaultRepoPrefix(project)}rust_${name}", root, tag)
  }

  String defaultBaseImage() {
    return rustImageName('sdk')
  }

  void ensureBuilderImage() {
    Project project = build.project
    ExecResult inspect = project.exec({ ExecSpec spec ->
      spec.commandLine('docker', 'image', 'inspect', rustBuilderImage)
      spec.ignoreExitValue = true
      spec.standardOutput = new ByteArrayOutputStream()
      spec.errorOutput = new ByteArrayOutputStream()
    } as Action<ExecSpec>)
    if (inspect.exitValue != 0) {
      project.logger.lifecycle(
          "Docker image '${rustBuilderImage}' not found locally. Building it from container/Dockerfile.builder...")
      File containerDir = new File(build.rustDir, 'container')
      project.exec({ ExecSpec spec ->
        spec.commandLine('docker', 'build', '-t', rustBuilderImage, '-f',
            new File(containerDir, 'Dockerfile.builder').absolutePath, containerDir.absolutePath)
      } as Action<ExecSpec>)
    }
  }

  void requireLocalImage(String image, String arch) {
    ByteArrayOutputStream stdout = new ByteArrayOutputStream()
    ExecResult inspect = build.project.exec({ ExecSpec spec ->
      spec.commandLine('docker', 'image', 'inspect', '--format', '{{.Architecture}}', image)
      spec.standardOutput = stdout
      spec.errorOutput = new ByteArrayOutputStream()
      spec.ignoreExitValue = true
    } as Action<ExecSpec>)
    String foundArch = stdout.toString('UTF-8').trim()
    if (inspect.exitValue != 0 || foundArch != arch) {
      throw new GradleException("${image} is not available locally for linux/${arch}" +
          (foundArch ? " (found linux/${foundArch})" : '') + '. Build it with ' +
          "./gradlew :sdks:rust:container:docker -Pcontainer-architecture-list=${arch}, " +
          'or pass -PbaseImage=<image>.')
    }
  }

  /** Computes a short content hash so Cargo invalidates PGO builds when a profile file changes. */
  static String sha256Prefix(File file) {
    MessageDigest md = MessageDigest.getInstance('SHA-256')
    file.withInputStream { InputStream input ->
      byte[] buf = new byte[8192]
      int n
      while ((n = input.read(buf)) != -1) {
        md.update(buf, 0, n)
      }
    }
    return md.digest().encodeHex().toString().substring(0, 12)
  }

  /**
   * Fails on {@code -PpgoGenerate}: an instrumented Linux worker writes {@code .profraw} files
   * on the runner, where the host cannot collect them. Train with {@code profilePrism}.
   */
  static void rejectPgoGenerate(Project project) {
    if (!(project.findProperty('pgoGenerate') ?: '').toString().trim().isEmpty()) {
      throw new GradleException('-PpgoGenerate applies only to profilePrism. A Linux worker ' +
          'build takes -PpgoProfile=<merged .profdata>.')
    }
  }

  /** Tracks the optional {@code -PpgoProfile} input of a worker build task. */
  void registerPgoInputs(Task task) {
    Project project = build.project
    task.doFirst { rejectPgoGenerate(project) }
    String pgoProfile = (project.findProperty('pgoProfile') ?: '').toString().trim()
    if (pgoProfile.isEmpty()) {
      task.inputs.property('pgoProfile', 'none')
    } else {
      task.inputs.file(project.file(pgoProfile))
    }
  }

  /** Compiles {@code examples} into Linux worker binaries under {@code build/linux_<arch>/}. */
  void compileWorkers(List<String> examples, String arch, String sdkVer) {
    Project project = build.project
    File outDir = build.buildFile("linux_${arch}")
    outDir.mkdirs()

    Map<String, String> rustTargets = [
      amd64: 'x86_64-unknown-linux-gnu',
      arm64: 'aarch64-unknown-linux-gnu',
    ]
    String rustTarget = rustTargets[arch] ?: 'x86_64-unknown-linux-gnu'
    List<String> targetFlags = ['--workspace'] + examples.collectMany { String bin -> ['--bin', bin] }
    boolean isHostLinux = OperatingSystem.current().isLinux()

    String cargoProfile = 'worker'
    Map<String, String> defaultCpu = [amd64: 'x86-64-v2', arm64: 'generic']
    String targetCpu = (project.findProperty('workerTargetCpu') ?: defaultCpu[arch] ?: 'generic').toString()
    // gemm-f16 (from candle) needs +fp16, which generic aarch64 leaves off when RUSTFLAGS is set.
    String baseRustFlags = "-C target-cpu=${targetCpu}" + (arch == 'arm64' ? ' -C target-feature=+fp16' : '')
    rejectPgoGenerate(project)
    String pgoProfile = (project.findProperty('pgoProfile') ?: '').toString().trim()
    File pgoProfileFile = null
    String pgoProfileHash = null
    if (!pgoProfile.isEmpty()) {
      pgoProfileFile = project.file(pgoProfile)
      if (!pgoProfileFile.isFile()) {
        throw new GradleException("PGO profile not found: ${pgoProfileFile.absolutePath}")
      }
      pgoProfileHash = sha256Prefix(pgoProfileFile)
    }
    boolean pgoActive = pgoProfileFile != null
    Object jobsProp = project.findProperty('workerJobs')
    List<String> jobFlags = jobsProp != null ? ['--jobs', jobsProp.toString()] : new ArrayList<String>()

    if (isHostLinux && arch == BeamModuleExtras.nativeArchitecture(project)) {
      String rustFlags = baseRustFlags
      if (pgoProfileFile != null) {
        File staged = build.buildFile("pgo/${pgoProfileHash}.profdata")
        staged.parentFile.mkdirs()
        if (!staged.isFile()) {
          staged.bytes = pgoProfileFile.bytes
        }
        rustFlags = "${rustFlags} -C profile-use=${staged.absolutePath}"
      }
      List<String> hostTargetFlags = pgoActive ? ['--target', rustTarget] : new ArrayList<String>()
      String hostProfileDir = pgoActive ? "target/${rustTarget}/${cargoProfile}" : "target/${cargoProfile}"
      project.exec({ ExecSpec spec ->
        spec.workingDir(build.rustDir)
        spec.environment(build.versions.cargoEnv())
        spec.environment('RUSTFLAGS', rustFlags)
        spec.commandLine(['cargo', 'build', '--profile', cargoProfile] + hostTargetFlags + jobFlags + targetFlags)
      } as Action<ExecSpec>)
      project.copy({ CopySpec spec ->
        spec.from(new File(build.rustDir, hostProfileDir))
        spec.into(outDir)
        spec.include(examples)
      } as Action<CopySpec>)
    } else {
      ensureBuilderImage()
      File crossTarget = build.buildFile('cross-target')
      crossTarget.mkdirs()

      String rustFlags = baseRustFlags
      List<String> dockerMounts = new ArrayList<String>()
      if (pgoProfileFile != null) {
        String containerProfile = "/tmp/pgo/${pgoProfileHash}.profdata"
        dockerMounts.add('-v')
        dockerMounts.add("${pgoProfileFile.absolutePath}:${containerProfile}:ro".toString())
        rustFlags = "${rustFlags} -C profile-use=${containerProfile}"
      }

      String copyOut = examples
          .collect { String bin ->
            "cp /tmp/target/${rustTarget}/${cargoProfile}/${bin} /workspace/sdks/rust/build/linux_${arch}/${bin}"
          }
          .join(' && ')
      // librdkafka configure reads CC and CXX. Set cross compilers when host arch differs.
      Map<String, String> gnuByArch = [amd64: 'x86_64', arm64: 'aarch64']
      String gnuArch = gnuByArch[arch] ?: 'x86_64'
      String crossCompilers = "case \"\$(uname -m)\" in ${gnuArch}) ;; *) export CC=${gnuArch}-linux-gnu-gcc " +
          "CXX=${gnuArch}-linux-gnu-g++ HOST_CC=gcc HOST_CXX=g++ ;; esac"
      String compileScript = "${crossCompilers} && CARGO_TARGET_DIR=/tmp/target cargo build --profile ${cargoProfile} " +
          "${(jobFlags + targetFlags).join(' ')} --target ${rustTarget} && ${copyOut}"
      List<String> dockerCmd = [
        'docker', 'run', '--rm',
        '-v', "${project.rootDir.absolutePath}:/workspace".toString(),
        '-v', "${crossTarget.absolutePath}:/tmp/target".toString(),
        '-v', 'beam-cargo-registry:/usr/local/cargo/registry',
      ]
      dockerCmd.addAll(dockerMounts)
      dockerCmd.addAll([
        '-w', '/workspace/sdks/rust',
        '-e', "BEAM_SDK_VERSION=${sdkVer}".toString(),
        '-e', "BEAM_RELEASE_VERSION=${build.versions.releaseVersion}".toString(),
        '-e', "BEAM_VERSION=${build.versions.beamVersion}".toString(),
        '-e', "RUSTFLAGS=${rustFlags}".toString(),
        rustBuilderImage,
        'sh', '-c', compileScript,
      ])
      project.exec({ ExecSpec spec ->
        spec.workingDir(project.rootDir)
        spec.commandLine(dockerCmd)
      } as Action<ExecSpec>)
    }
  }

  /** Registers a task that compiles one Linux worker binary into {@code build/linux_<arch>/}. */
  void registerWorkerTask(
      String name,
      String description,
      String bin,
      String arch = config.workerArch,
      String supplyKey = null,
      Closure<Boolean> customOnlyIf = null) {
    Project project = build.project
    project.tasks.register(name) { Task task ->
      task.group = 'Build'
      task.description = description
      if (customOnlyIf != null) {
        task.onlyIf { customOnlyIf.call() }
      } else if (supplyKey != null) {
        task.onlyIf { config.suppliedWorkerBinary(supplyKey) == null }
      }
      task.inputs.files(config.rustWorkspaceTree)
      task.inputs.property('bin', bin)
      task.inputs.property('arch', arch)
      task.inputs.property('sdkVersion', build.versions.sdkVersion)
      task.inputs.property('targetCpu', (project.findProperty('workerTargetCpu') ?: 'default').toString())
      registerPgoInputs(task)
      task.outputs.file(config.workerBinaryFor(bin, arch))

      task.doLast { compileWorkers([bin], arch, build.versions.sdkVersion) }
    }
  }

  /** Registers tasks that build and containerize a Rust expansion service binary. */
  void registerExpansionImageTasks(Map<String, Object> cfg) {
    Project project = build.project
    String bin = cfg.bin.toString()
    String arch = cfg.arch.toString()
    String image = cfg.image.toString()
    String imageTask = cfg.imageTask.toString()
    String key = "${bin}:${arch}"
    String buildTask = expansionBinaryTasks[key]
    if (buildTask == null) {
      buildTask = cfg.buildTask != null
          ? cfg.buildTask.toString()
          : "build${imageTask.substring(0, 1).toUpperCase()}${imageTask.substring(1)}"
      expansionBinaryTasks[key] = buildTask
      registerWorkerTask(buildTask, cfg.buildDescription.toString(), bin, arch)
    }

    project.tasks.register(imageTask) { Task task ->
      task.group = 'Build'
      task.description = cfg.imageDescription.toString()
      task.dependsOn(buildTask)

      task.doLast {
        String defaultBase = defaultBaseImage()
        String baseImage = (cfg.baseImage ?: project.findProperty('baseImage') ?: defaultBase).toString()
        if (baseImage == defaultBase) {
          requireLocalImage(baseImage, arch)
        }

        File context = build.buildFile("expansion-image/${imageTask}")
        project.delete(context)
        project.copy({ CopySpec spec ->
          spec.from(config.workerBinaryFor(bin, arch))
          spec.into(context)
          spec.rename { 'expansion_service' }
        } as Action<CopySpec>)

        project.exec({ ExecSpec spec ->
          spec.commandLine('docker', 'build',
              '--platform', "linux/${arch}",
              '--build-arg', "BASE_IMAGE=${baseImage}",
              '-t', image,
              '-f', new File(build.rustDir, 'container/Dockerfile.expansion').absolutePath,
              context.absolutePath)
        } as Action<ExecSpec>)
        if (Boolean.TRUE == cfg.push || project.rootProject.hasProperty('push-containers')) {
          project.exec({ ExecSpec spec -> spec.commandLine('docker', 'push', image) } as Action<ExecSpec>)
        }
        task.logger.lifecycle("Built ${image}. Start the expansion service with: docker run -p 8097:8097 " +
            "--entrypoint /opt/apache/beam/worker_binary ${image} --port 8097 --docker-image ${image}")
      }
    }
  }

  private void registerAll() {
    Project project = build.project
    project.tasks.register('buildBuilderImage', Exec) { Exec task ->
      task.group = 'Build'
      task.description = 'Build the beam_rust_builder Docker image'
      task.workingDir(new File(build.rustDir, 'container'))
      task.commandLine('docker', 'build', '-t', rustBuilderImage, '-f', 'Dockerfile.builder', '.')
    }

    registerWorkerTask(
        'buildWorker',
        'Build a Linux worker binary for container/Dataflow execution (-Pexample=gaming -PexampleArch=amd64)',
        config.selectedExample,
        config.workerArch,
        null,
        {
          String supplied = config.suppliedWorkerBinary(config.selectedExample)
          return supplied == null ||
              (supplied == 'none' && project.gradle.taskGraph.hasTask("${project.path}:prebakedImage".toString()))
        })

    registerWorkerTask(
        'buildRunnerTestsWorker',
        'Build Linux worker binary for ValidatesRunner Dataflow execution (-PexampleArch=amd64)',
        'beam-runner-tests-worker')

    project.tasks.register('docker') { Task task ->
      task.group = 'Build'
      task.description = 'Build the Rust SDK base container image (apache/beam_rust_sdk)'
      task.dependsOn(':sdks:rust:container:docker')
    }

    project.tasks.register('prebakedImage') { Task task ->
      task.group = 'Build'
      task.description = "Build an example's worker image with its binary pre-baked, " +
          'beam_rust_example_<example>[_flex] (-Pexample=wordcount [-PbaseImage=<image>] [-Pflex] [-Ppush-containers])'
      task.dependsOn('buildWorker')

      task.doLast {
        String example = config.selectedExample
        File dockerfile = project.file("examples/${example}/Dockerfile")
        if (!dockerfile.exists()) {
          throw new GradleException("Example '${example}' has no Dockerfile (expected ${dockerfile}). " +
              'examples/wordcount/Dockerfile shows the layout.')
        }
        String defaultBase = defaultBaseImage()
        String baseImage = config.resolveProp(example, 'baseImage', null, defaultBase)
        Object flexProp = project.findProperty('flex')
        boolean flex = flexProp instanceof CharSequence && flexProp.toString() != 'false'
        String imageName = rustImageName("example_${example}${flex ? '_flex' : ''}")

        if (baseImage == defaultBase) {
          requireLocalImage(baseImage, config.workerArch)
        }

        String supplied = config.suppliedWorkerBinary(example)
        File binary = (supplied != null && supplied != 'none')
            ? project.file(supplied)
            : config.workerBinaryFor(example, config.workerArch)
        if (!binary.isFile()) {
          throw new GradleException("No worker binary at ${binary} to bake into ${imageName}.")
        }
        File context = build.buildFile("prebaked/${example}")
        project.delete(context)
        project.copy({ CopySpec spec ->
          spec.from(binary)
          spec.into(context)
          spec.rename { example }
        } as Action<CopySpec>)

        project.exec({ ExecSpec spec ->
          spec.commandLine('docker', 'build',
              '--platform', "linux/${config.workerArch}",
              '--build-arg', "BASE_IMAGE=${baseImage}",
              '--target', flex ? 'flex' : 'worker',
              '-t', imageName,
              '-f', dockerfile.absolutePath,
              context.absolutePath)
        } as Action<ExecSpec>)
        if (project.rootProject.hasProperty('push-containers')) {
          project.exec({ ExecSpec spec -> spec.commandLine('docker', 'push', imageName) } as Action<ExecSpec>)
        }
        task.logger.lifecycle("Built ${imageName}. Launch with --sdk_container_image=${imageName} and no " +
            '--worker_binary (-PworkerBinary=none with the Gradle launch tasks).')
      }
    }

    registerExpansionImageTasks([
      buildTask       : 'buildExpansionService',
      imageTask       : 'expansionServiceImage',
      bin             : (project.findProperty('expansionBin') ?: 'beam-expansion-service').toString(),
      arch            : config.workerArch,
      image           : rustImageName('expansion_service'),
      buildDescription: 'Build the Linux expansion service binary (-PexampleArch=amd64 [-PexpansionBin=<bin>])',
      imageDescription: 'Build the expansion service image: the SDK base image with the expansion service as the worker binary ([-PbaseImage=<image>] [-PexampleArch=amd64] [-Ppush-containers])',
    ])
  }
}
