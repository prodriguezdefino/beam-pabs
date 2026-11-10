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
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.process.ExecResult
import org.gradle.process.ExecSpec

/** Local CPU and memory profiling of a Rust pipeline on Prism ({@code profilePrism}). */
@CompileStatic
class RustProfilingTasks {

  private static final int CRITERION_PROFILE_SECONDS = 10

  static void register(RustConfig config) {
    RustSdkBuild build = config.build
    Project project = build.project
    String hostOs = config.prism.hostOs

    project.tasks.register('profilePrism') { Task task ->
      task.group = 'Application'
      task.description = 'Profile a Rust example locally on Prism with samply and/or time ' +
          '(-Pexample=<name> [-Pbin=<bin> -Ppkg=<crate>] [-PcargoExample=<name> | -PcargoBench=<name>] ' +
          '[-PextraArgs="<pipeline flags>"] [-Prows=<n>] [-Pmode=cpu|memory|both] ' +
          '[-PcargoProfile=worker|release] [-Pout=<dir>]). -Pmode=both runs the pipeline two times.'
      task.dependsOn('fetchPrism')

      String mode = (project.findProperty('mode') ?: 'both').toString().toLowerCase()
      String cargoProfile = (project.findProperty('cargoProfile') ?: 'worker').toString()
      if (!(cargoProfile in ['worker', 'release'])) {
        throw new GradleException("-PcargoProfile must be 'worker' or 'release', not '${cargoProfile}'.")
      }
      String profileEnv = "CARGO_PROFILE_${cargoProfile.toUpperCase()}"
      String example = config.selectedExample
      String cargoExample = project.findProperty('cargoExample')?.toString()
      String cargoBench = project.findProperty('cargoBench')?.toString()
      String binArg = (cargoExample ?: cargoBench ?: project.findProperty('bin') ?: example).toString()
      String pkgArg = (project.findProperty('pkg') ?: example).toString()
      Object rowsArg = project.findProperty('rows')
      String outArg = (project.findProperty('out')
          ?: build.buildFile("reports/profile/${binArg}-${System.currentTimeMillis()}").absolutePath).toString()
      File outDir = project.file(outArg)
      String cargoTargetDir = System.getenv('CARGO_TARGET_DIR') ?: new File(build.rustDir, 'target').absolutePath

      File samplyBin = new File(System.getProperty('user.home'), '.cargo/bin/samply')
      String samplyCmd = samplyBin.exists() ? samplyBin.absolutePath : 'samply'

      List<String> extraArgs = (config.resolveProp(example, 'extraArgs') ?: '').tokenize(' ')
      String features = (config.resolveProp(example, 'exampleFeatures') ?: '').trim()
      List<String> featureFlags = features.isEmpty() ? new ArrayList<String>() : ['--features', features]
      List<String> gcpArgs = config.portableGcpArgs(example)

      task.doFirst {
        if (mode in ['cpu', 'both']) {
          try {
            ExecResult res = project.exec({ ExecSpec spec ->
              spec.commandLine(samplyCmd, '--version')
              spec.ignoreExitValue = true
            } as Action<ExecSpec>)
            if (res.exitValue != 0) {
              throw new GradleException("samply not found. Run 'cargo install --locked samply' to install it.")
            }
          } catch (Exception e) {
            throw new GradleException("samply not found. Run 'cargo install --locked samply' to install it.", e)
          }
        }
      }

      task.doLast {
        outDir.mkdirs()
        File profileJson = new File(outDir, 'profile.json.gz')
        File timeTxt = new File(outDir, 'time.txt')
        File summaryTxt = new File(outDir, 'summary.md')

        task.logger.lifecycle("Building binary ${binArg} in ${pkgArg} with the '${cargoProfile}' profile...")
        ByteArrayOutputStream buildOutput = new ByteArrayOutputStream()
        List<String> target = cargoExample != null ? ['--example', cargoExample]
            : (cargoBench != null ? ['--bench', cargoBench, '--message-format=json-render-diagnostics']
                : ['--bin', binArg])
        List<String> scope = featureFlags.isEmpty() ? build.workspaceScope : ['-p', pkgArg] + featureFlags
        List<String> buildCmd = ['cargo', 'build', '--profile', cargoProfile] + scope + target
        project.exec({ ExecSpec spec ->
          spec.workingDir(build.rustDir)
          spec.environment(build.versions.cargoEnv())
          spec.environment("${profileEnv}_DEBUG", 'line-tables-only')
          spec.environment("${profileEnv}_STRIP", 'none')
          if (hostOs == 'darwin') {
            spec.environment("${profileEnv}_SPLIT_DEBUGINFO", 'unpacked')
          }
          spec.commandLine(buildCmd)
          if (cargoBench != null) {
            spec.standardOutput = buildOutput
          }
        } as Action<ExecSpec>)

        Map<String, String> bench = cargoBench != null
            ? benchArtifact(buildOutput.toString('UTF-8'), cargoBench) : null
        String binPath = bench != null
            ? bench.executable
            : new File(cargoExample != null
                ? "${cargoTargetDir}/${cargoProfile}/examples/${cargoExample}"
                : "${cargoTargetDir}/${cargoProfile}/${binArg}").absolutePath

        List<String> benchArgs = (project.findProperty('extraArgs') ?: '').toString().tokenize(' ')
        List<String> profileTime = benchArgs.any { String a -> a.startsWith('--profile-time') }
            ? new ArrayList<String>()
            : ['--profile-time', CRITERION_PROFILE_SECONDS.toString()]
        List<String> runArgs = (bench != null && isCriterionBench(bench.srcPath))
            ? ['--bench'] + profileTime + benchArgs
            : ['--runner=PrismRunner', '--environment_type=LOOPBACK'] + gcpArgs + extraArgs
        task.logger.lifecycle("Arguments: ${runArgs.join(' ')}")

        if (mode in ['cpu', 'both']) {
          task.logger.lifecycle('Running CPU profile (samply)...')
          List<String> cmd = [
            samplyCmd, 'record', '--unstable-presymbolicate', '--save-only',
            '-o', profileJson.absolutePath, binPath,
          ] + runArgs
          project.exec({ ExecSpec spec ->
            spec.workingDir(build.rustDir)
            spec.environment(config.launchEnvironment())
            spec.commandLine(cmd)
          } as Action<ExecSpec>)
        }

        if (mode in ['memory', 'both']) {
          task.logger.lifecycle('Running Memory profile (time)...')
          List<String> timeCmd = hostOs == 'darwin' ? ['/usr/bin/time', '-l'] : ['/usr/bin/time', '-v']
          new FileOutputStream(new File(outDir, 'time.out')).withCloseable { OutputStream outStream ->
            new FileOutputStream(timeTxt).withCloseable { OutputStream errStream ->
              project.exec({ ExecSpec spec ->
                spec.workingDir(build.rustDir)
                spec.environment(config.launchEnvironment())
                spec.commandLine(timeCmd + [binPath] + runArgs)
                spec.standardOutput = outStream
                spec.errorOutput = errStream
              } as Action<ExecSpec>)
            }
          }
        }

        task.logger.lifecycle('Generating summary...')
        String profileArg = mode in ['cpu', 'both'] ? profileJson.absolutePath : 'none'
        List<String> pyArgs = [
          'python3',
          new File(build.rustDir, 'scripts/profiling/summary.py').absolutePath,
          profileArg,
        ]
        if (timeTxt.exists()) {
          pyArgs.addAll(['--time-output', timeTxt.absolutePath])
        }
        if (rowsArg != null) {
          pyArgs.addAll(['--rows', rowsArg.toString()])
        }

        new FileOutputStream(summaryTxt).withCloseable { OutputStream outStream ->
          project.exec({ ExecSpec spec ->
            spec.workingDir(build.rustDir)
            spec.commandLine(pyArgs)
            spec.standardOutput = outStream
          } as Action<ExecSpec>)
        }

        task.logger.lifecycle("Outputs written to: ${outDir}")
        if (mode in ['cpu', 'both']) {
          task.logger.lifecycle("To view CPU profile: samply load ${profileJson}")
        }
        task.logger.lifecycle('Summary:')
        task.logger.lifecycle(summaryTxt.text)
      }
    }
  }

  @CompileDynamic
  private static Map<String, String> benchArtifact(String messages, String name) {
    JsonSlurper slurper = new JsonSlurper()
    def artifact = messages.readLines()
        .findAll { String line -> line.startsWith('{') }
        .collect { String line -> slurper.parseText(line) }
        .find { def msg -> msg.reason == 'compiler-artifact' && msg.target?.name == name && msg.executable }
    if (!artifact) {
      throw new GradleException("cargo did not build an executable for bench '${name}'.")
    }
    return [executable: artifact.executable.toString(), srcPath: artifact.target.src_path?.toString()]
  }

  private static boolean isCriterionBench(String srcPath) {
    return srcPath != null && new File(srcPath).text.contains('criterion_main!')
  }
}
