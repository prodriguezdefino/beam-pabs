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

import groovy.json.JsonSlurper
import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import org.gradle.api.Action
import org.gradle.api.GradleException
import org.gradle.api.Task
import org.gradle.api.file.ConfigurableFileTree
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.MapProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Exec
import org.gradle.api.tasks.PathSensitivity
import org.gradle.process.ExecSpec
import org.gradle.workers.WorkAction
import org.gradle.workers.WorkParameters
import org.gradle.workers.WorkQueue

/** The build, format, lint, script-test and target-pruning tasks. */
@CompileStatic
class RustLintTasks {

  /** Parameters for {@link ExecCheckWorkAction}. */
  static interface ExecCheckWorkParams extends WorkParameters {
    Property<File> getWorkingDir()
    ListProperty<String> getCommand()
    MapProperty<String, String> getEnvironment()
    RegularFileProperty getStampFile()
    Property<Boolean> getStreamLive()
    Property<String> getLabel()
  }

  /** Runs a check command through Gradle's worker executor and writes its stamp file. */
  abstract static class ExecCheckWorkAction implements WorkAction<ExecCheckWorkParams> {
    @Override
    void execute() {
      ExecCheckWorkParams p = parameters
      ProcessBuilder pb = new ProcessBuilder(p.command.get())
      pb.directory(p.workingDir.get())
      pb.redirectErrorStream(true)
      pb.environment().putAll(p.environment.get())
      Process proc = pb.start()
      StringBuilder captured = new StringBuilder()
      boolean stream = p.streamLive.get()
      proc.inputStream.eachLine('UTF-8') { String line ->
        if (stream) {
          System.out.println(line)
        } else {
          captured.append(line).append('\n')
        }
      }
      int exit = proc.waitFor()
      File stamp = p.stampFile.asFile.orNull
      if (exit != 0) {
        if (!stream && captured.length() > 0) {
          System.err.print(captured.toString())
        }
        if (stamp != null) {
          stamp.delete()
        }
        throw new GradleException("${p.label.get()} failed with exit code ${exit}")
      }
      if (stamp != null) {
        stamp.parentFile.mkdirs()
        stamp.text = 'ok\n'
      }
    }
  }

  /** Parameters for {@link PythonTestWorkAction}. */
  static interface PythonTestWorkParams extends WorkParameters {
    ListProperty<File> getDirs()
    RegularFileProperty getStampFile()
  }

  /** Runs {@code python3 -m unittest discover} across configured script directories. */
  abstract static class PythonTestWorkAction implements WorkAction<PythonTestWorkParams> {
    @Override
    void execute() {
      for (File dir : parameters.dirs.get()) {
        ProcessBuilder pb = new ProcessBuilder(
            'python3', '-m', 'unittest', 'discover', '-s', '.', '-p', '*_test.py')
        pb.directory(dir)
        pb.redirectErrorStream(true)
        Process proc = pb.start()
        String out = proc.inputStream.getText('UTF-8')
        int exit = proc.waitFor()
        if (exit != 0) {
          System.err.print(out)
          parameters.stampFile.get().asFile.delete()
          throw new GradleException("pythonScriptsTest failed in ${dir.name} with exit code ${exit}")
        }
      }
      File stamp = parameters.stampFile.get().asFile
      stamp.parentFile.mkdirs()
      stamp.text = 'ok\n'
    }
  }

  /** The lints that test and benchmark code can break. clippy.toml exempts {@code #[cfg(test)]}. */
  private static final List<String> TEST_CLIPPY_ALLOWS = [
    'unwrap_used',
    'expect_used',
    'panic',
    'missing_panics_doc',
  ].collectMany { String lint -> ['-A', "clippy::${lint}".toString()] }

  static void register(RustSdkBuild build) {
    registerBuildWorkspace(build)
    registerFmt(build)
    registerClippy(build)
    registerClippyExtras(build)
    registerPruneTarget(build)
    registerPythonScriptsTest(build)
    build.cargoTask('cleanSdk', 'Build', 'Remove the cargo target directory', ['clean'])
  }

  private static void registerBuildWorkspace(RustSdkBuild build) {
    build.cargoTask('buildWorkspace', 'Build',
        'Compile all crates in the Rust workspace (or one crate with -Ppkg=<crate>)',
        ['build'] + build.buildCargoScope).configure { Exec task ->
          String pkg = build.selectedPkg
          CargoWorkspace workspace = build.workspace
          task.mustRunAfter('fmt')
          task.inputs.property('beamSdkVersion', build.versions.sdkVersion)
          task.inputs.property('beamReleaseVersion', build.versions.releaseVersion)
          task.inputs.property('cargoScope', build.buildCargoScope)
          task.inputs.files(build.workspaceRootFiles).withPathSensitivity(PathSensitivity.RELATIVE)
          task.inputs.files(build.modelProtoDirs).withPathSensitivity(PathSensitivity.RELATIVE)
          task.inputs.files(build.project.provider {
            if (pkg != null && workspace.crateClosure.containsKey(pkg)) {
              List<Object> inputs = []
              workspace.crateClosure[pkg].each { String dep -> inputs.addAll(workspace.libInputFiles(dep)) }
              inputs.addAll(build.testInputFiles(pkg))
              return inputs
            }
            return build.libAndTestInputs(build.defaultPackages())
          }).withPathSensitivity(PathSensitivity.RELATIVE)
          String buildKey = pkg != null
              ? "${pkg}-${build.onlyFeatures ?: 'default'}".replaceAll('[^a-zA-Z0-9_-]', '_')
              : 'workspace'
          File stampFile = build.buildFile("test-results/cargo/build-${buildKey}.txt")
          task.outputs.file(stampFile)
          // Cargo decides what to compile; Gradle only skips the call when no input changed.
          task.outputs.upToDateWhen { new File(build.rustDir, 'target').exists() }
          task.doLast { RustSdkBuild.writeStamp(stampFile) }
        }
  }

  private static void registerFmt(RustSdkBuild build) {
    build.cargoTask('fmt', 'Verification', 'Format all Rust code (cargo fmt)', ['fmt'])
    build.project.tasks.register('fmtCheck') { Task task ->
      task.group = 'Verification'
      task.description = 'Verify formatting (cargo fmt --check)'
      task.mustRunAfter('fmt')
      task.inputs.files(new File(build.rustDir, 'rustfmt.toml'), new File(build.rustDir, 'Cargo.toml'))
          .withPathSensitivity(PathSensitivity.RELATIVE)
      task.inputs.files(build.libAndTestInputs(build.workspace.topoOrder))
          .withPathSensitivity(PathSensitivity.RELATIVE)
      File stampFile = build.buildFile('test-results/cargo/fmtCheck.txt')
      task.outputs.file(stampFile)
      task.outputs.cacheIf { true }
      task.doLast {
        build.workerExecutor.noIsolation().submit(ExecCheckWorkAction.class, { ExecCheckWorkParams p ->
          p.workingDir.set(build.rustDir)
          p.command.set(['cargo', 'fmt', '--check'])
          p.environment.set(build.versions.cargoEnv())
          p.stampFile.set(stampFile)
          p.streamLive.set(false)
          p.label.set('cargo fmt --check')
        } as Action<ExecCheckWorkParams>)
      }
    }
  }

  private static void registerClippy(RustSdkBuild build) {
    build.project.tasks.register('clippy') { Task task ->
      task.group = 'Verification'
      task.description = 'Run clippy on all targets (-D warnings)'
      task.mustRunAfter('fmt')
      task.inputs.property('beamSdkVersion', build.versions.sdkVersion)
      task.inputs.property('beamReleaseVersion', build.versions.releaseVersion)
      task.inputs.files(build.workspaceRootFiles + [new File(build.rustDir, 'clippy.toml')])
          .withPathSensitivity(PathSensitivity.RELATIVE)
      task.inputs.files(build.modelProtoDirs).withPathSensitivity(PathSensitivity.RELATIVE)
      task.inputs.files(build.project.provider { build.libAndTestInputs(build.defaultPackages()) })
          .withPathSensitivity(PathSensitivity.RELATIVE)
      File stampFile = build.buildFile('test-results/cargo/clippy.txt')
      task.outputs.file(stampFile)
      task.outputs.cacheIf { true }
      task.doLast {
        stampFile.delete()
        Map<String, String> env = build.versions.cargoEnv()
        WorkQueue queue = build.workerExecutor.noIsolation()
        queue.submit(ExecCheckWorkAction.class, { ExecCheckWorkParams p ->
          p.workingDir.set(build.rustDir)
          p.command.set(['cargo', 'clippy'] + build.workspaceScope +
              ['--lib', '--bins', '--examples', '--', '-D', 'warnings'])
          p.environment.set(env)
          p.streamLive.set(true)
          p.label.set('cargo clippy (--lib --bins --examples)')
        } as Action<ExecCheckWorkParams>)
        queue.await()
        queue.submit(ExecCheckWorkAction.class, { ExecCheckWorkParams p ->
          p.workingDir.set(build.rustDir)
          p.command.set(['cargo', 'clippy'] + build.workspaceScope +
              ['--tests', '--benches', '--', '-D', 'warnings'] + TEST_CLIPPY_ALLOWS)
          p.environment.set(env)
          p.stampFile.set(stampFile)
          p.streamLive.set(true)
          p.label.set('cargo clippy (--tests --benches)')
        } as Action<ExecCheckWorkParams>)
      }
    }
  }

  private static void registerClippyExtras(RustSdkBuild build) {
    build.project.tasks.register('clippyFix') { Task task ->
      task.group = 'Verification'
      task.description = 'Apply the machine-applicable lint fixes to all targets (cargo clippy --fix)'
      task.doLast {
        List<String> fixFlags = ['--fix', '--allow-dirty', '--allow-staged', '--allow-no-vcs']
        Map<String, String> env = build.versions.cargoEnv()
        WorkQueue queue = build.workerExecutor.noIsolation()
        queue.submit(ExecCheckWorkAction.class, { ExecCheckWorkParams p ->
          p.workingDir.set(build.rustDir)
          p.command.set(['cargo', 'clippy'] + build.workspaceScope +
              ['--lib', '--bins', '--examples'] + fixFlags)
          p.environment.set(env)
          p.streamLive.set(true)
          p.label.set('cargo clippy --fix (--lib --bins --examples)')
        } as Action<ExecCheckWorkParams>)
        queue.await()
        queue.submit(ExecCheckWorkAction.class, { ExecCheckWorkParams p ->
          p.workingDir.set(build.rustDir)
          p.command.set(['cargo', 'clippy'] + build.workspaceScope +
              ['--tests', '--benches'] + fixFlags + ['--'] + TEST_CLIPPY_ALLOWS)
          p.environment.set(env)
          p.stampFile.set(build.buildFile('tmp/clippyFix.txt'))
          p.streamLive.set(true)
          p.label.set('cargo clippy --fix (--tests --benches)')
        } as Action<ExecCheckWorkParams>)
      }
    }

    build.project.tasks.register('clippyLintReport') { Task task ->
      task.group = 'Verification'
      task.description = 'Count the warnings of candidate clippy lints on all targets ' +
          '(-PclippyLints=match_bool,expect_used,...); writes build/reports/clippy-lints.txt'
      task.doLast {
        List<String> lints = (build.project.findProperty('clippyLints') ?: '')
            .toString()
            .tokenize(',')
            .collect { String l -> l.trim() }
            .findAll { String l -> !l.isEmpty() }
        if (lints.isEmpty()) {
          throw new GradleException(
              'Set -PclippyLints=<lint>,<lint> (clippy lint names without the clippy:: prefix).')
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream()
        List<String> lintFlags = lints.collectMany { String l -> ['-W', "clippy::${l}".toString()] }
        build.project.exec({ ExecSpec spec ->
          spec.workingDir(build.rustDir)
          spec.environment(build.versions.cargoEnv())
          spec.commandLine(['cargo', 'clippy'] + build.workspaceScope +
              ['--all-targets', '--keep-going', '--message-format=json', '--'] + lintFlags)
          spec.errorOutput = new ByteArrayOutputStream()
          spec.standardOutput = out
          spec.ignoreExitValue = true
        } as Action<ExecSpec>)
        File report = build.buildFile('reports/clippy-lints.txt')
        writeClippyLintReport(task, lints, out.toString('UTF-8'), report)
      }
    }
  }

  @CompileDynamic
  private static void writeClippyLintReport(Task task, List<String> lints, String rawJson, File report) {
    JsonSlurper slurper = new JsonSlurper()
    List<Map<String, String>> hits = rawJson.readLines()
        .findAll { String line -> line.startsWith('{') }
        .collect { String line -> slurper.parseText(line) }
        .findAll { def msg -> msg.reason == 'compiler-message' && msg.message?.code?.code }
        .collect { def msg ->
          def span = msg.message.spans.find { it.is_primary } ?: [:]
          [
            lint: msg.message.code.code.toString().replace('clippy::', ''),
            at  : "${span.file_name}:${span.line_start}".toString(),
          ]
        }
        .unique()
    Map<String, List<String>> counts = new LinkedHashMap<>()
    lints.each { String lint ->
      counts[lint] = hits.findAll { Map<String, String> h -> h.lint == lint }
          .collect { Map<String, String> h -> h.at }
    }
    report.parentFile.mkdirs()
    report.text = counts.collect { String lint, List<String> at -> "${lint}: ${at.size()}" }.join('\n') +
        '\n\n' +
        counts.collect { String lint, List<String> at -> (["## ${lint}"] + at.sort()).join('\n') }.join('\n\n') +
        '\n'
    counts.each { String lint, List<String> at -> task.logger.lifecycle("${lint}: ${at.size()}") }
    task.logger.lifecycle("Report: ${report}")
  }

  private static void registerPruneTarget(RustSdkBuild build) {
    build.project.tasks.register('pruneTarget') { Task task ->
      task.group = 'Build'
      task.description = 'Prune superseded binaries, duplicate workspace rlibs and stale incremental dirs in target/ ' +
          '([-PdryRun] [-PkeepExe=1] [-PkeepLib=2] [-PincrementalDays=2])'
      task.doLast {
        Object dryRunProp = build.project.findProperty('dryRun')
        boolean dryRun = dryRunProp != null && dryRunProp.toString() != 'false'
        build.pruneCargoTarget([
          dryRun         : dryRun,
          keepExe        : build.project.findProperty('keepExe'),
          keepLib        : build.project.findProperty('keepLib'),
          incrementalDays: build.project.findProperty('incrementalDays'),
        ])
      }
    }
  }

  private static void registerPythonScriptsTest(RustSdkBuild build) {
    build.project.tasks.register('pythonScriptsTest') { Task task ->
      task.group = 'Verification'
      task.description = 'Run the unit tests (*_test.py) of the Python scripts in sdks/rust'
      task.mustRunAfter('fmt')
      task.inputs.files(build.project.provider {
        build.pythonScriptSubdirs
            .collect { String sub -> new File(build.rustDir, sub) }
            .findAll { File dir -> dir.isDirectory() }
            .collect { File dir ->
              ConfigurableFileTree tree = build.project.fileTree(dir)
              tree.include('**/*.py')
              return tree
            }
      }).withPathSensitivity(PathSensitivity.RELATIVE)
      File stampFile = build.buildFile('test-results/cargo/pythonScriptsTest.txt')
      task.outputs.file(stampFile)
      task.doLast {
        List<File> dirs = build.pythonScriptSubdirs
            .collect { String sub -> new File(build.rustDir, sub) }
            .findAll { File dir -> dir.isDirectory() }
        build.workerExecutor.noIsolation().submit(PythonTestWorkAction.class, { PythonTestWorkParams p ->
          p.dirs.set(dirs)
          p.stampFile.set(stampFile)
        } as Action<PythonTestWorkParams>)
      }
    }
  }
}
