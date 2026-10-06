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
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.process.ExecSpec

/**
 * Code-quality tasks running inside {@code beam_rust_builder}: {@code coverage},
 * {@code coveragePerTest}, {@code mutants}, {@code testQualityReport}, {@code complexityScorecard}
 * and {@code machete}.
 */
@CompileStatic
class RustQualityTasks {

  private static final String QUALITY_SCRIPTS = '/workspace/sdks/rust/scripts/quality'

  private final RustContainerTasks containers
  private final RustConfig config
  private final RustSdkBuild build

  private RustQualityTasks(RustContainerTasks containers) {
    this.containers = containers
    this.config = containers.config
    this.build = config.build
  }

  static void register(RustContainerTasks containers) {
    new RustQualityTasks(containers).registerAll()
  }

  private static List<String> runScript(String interpreter, String script) {
    return [
      'bash',
      '-c',
      "cp -r ${QUALITY_SCRIPTS} /tmp/quality && exec ${interpreter} /tmp/quality/\"\$0\" \"\$@\"".toString(),
      script,
    ]
  }

  private File testQualityDir(String scope) {
    return build.buildFile("reports/test-quality/${scope}")
  }

  private String inContainer(File f) {
    String rel = build.project.rootDir.canonicalFile.toPath().relativize(f.canonicalFile.toPath()).toString()
    if (rel.startsWith('..')) {
      throw new GradleException("${f} is outside the repository and not visible to the container")
    }
    return "/workspace/${rel}"
  }

  private String selectedPkg() {
    return build.project.findProperty('pkg')?.toString()
  }

  private String requirePkg(String taskName) {
    String p = selectedPkg()
    if (p == null || p.isEmpty()) {
      throw new GradleException(
          "${taskName} needs a crate: -Ppkg=<cargo package>, e.g. -Ppkg=apache-beam-core")
    }
    return p
  }

  private File crateDirFor(String pkg) {
    File found = build.workspace.crateDir[pkg]
    if (found == null) {
      throw new GradleException(
          "No crate named '${pkg}' under sdks/rust (looked for name = \"${pkg}\" in Cargo.toml files)")
    }
    return found
  }

  private List<String> testQualityDocker(List<String> extra = new ArrayList<String>(), boolean withPrism = true) {
    Project project = build.project
    List<String> prism = withPrism ? [
      '-v', "${config.prism.prismLinuxBinary.absolutePath}:/opt/prism/prism:ro".toString(),
      '-e', 'BEAM_PRISM_PATH=/opt/prism/prism',
    ] : new ArrayList<String>()
    Object cpusProp = project.findProperty('cpus')
    List<String> cpus = cpusProp != null ? ['--cpus', cpusProp.toString()] : new ArrayList<String>()
    return ['docker', 'run', '--rm'] + cpus + [
      '-v', "${project.rootDir.absolutePath}:/workspace".toString(),
      '-v', 'beam-cargo-registry:/usr/local/cargo/registry',
    ] + prism + [
      '-e', "BEAM_SDK_VERSION=${build.versions.sdkVersion}".toString(),
      '-e', "BEAM_RELEASE_VERSION=${build.versions.releaseVersion}".toString(),
      '-e', "BEAM_VERSION=${build.versions.beamVersion}".toString(),
      '-w', '/workspace/sdks/rust',
    ] + extra + [containers.rustBuilderImage]
  }

  private void ensureTestQualityImage() {
    containers.ensureBuilderImage()
    ByteArrayOutputStream stdout = new ByteArrayOutputStream()
    build.project.exec({ ExecSpec spec ->
      spec.commandLine('docker', 'image', 'inspect', '--format', '{{json .Config.Env}}', containers.rustBuilderImage)
      spec.standardOutput = stdout
    } as Action<ExecSpec>)
    if (!stdout.toString('UTF-8').contains('BEAM_RUST_BUILDER=1')) {
      throw new GradleException("${containers.rustBuilderImage} predates the BEAM_RUST_BUILDER marker. " +
          'Rebuild it with ./gradlew :sdks:rust:buildBuilderImage')
    }
  }

  private boolean flag(String prop) {
    return build.project.hasProperty(prop) && build.project.property(prop).toString() != 'false'
  }

  private List<String> globs(String prop) {
    return (build.project.findProperty(prop) ?: '').toString()
        .tokenize(',')
        .collect { String it -> it.trim() }
        .findAll { String it -> !it.isEmpty() }
  }

  private void registerAll() {
    registerCoveragePerTest()
    registerCoverage()
    registerMutants()
    registerTestQualityReport()
    registerComplexityScorecard()
    registerMachete()
  }

  private void registerCoveragePerTest() {
    Project project = build.project
    project.tasks.register('coveragePerTest') { Task task ->
      task.group = 'Verification'
      task.description = 'Per-test line coverage of one crate and its redundancy report, in the builder container ' +
          '(-Ppkg=<crate> [-Psrc=<dir>] [-Pfocus=/coders/] [-Pfeatures=<a,b>]) -> build/reports/test-quality/<pkg>/coverage'
      task.dependsOn('fetchPrismLinux')
      task.doLast {
        String pkg = requirePkg('coveragePerTest')
        String src = (project.findProperty('src') ?: '').toString()
        if (src.isEmpty()) {
          src = inContainer(new File(crateDirFor(pkg), 'src')) - '/workspace/'
        } else if (!src.startsWith('sdks/')) {
          src = "sdks/rust/${src}"
        }
        String focus = (project.findProperty('focus') ?: '').toString()
        File outDir = new File(testQualityDir(pkg), 'coverage')
        File covTarget = build.buildFile('cov-target')
        covTarget.mkdirs()
        ensureTestQualityImage()

        List<String> cmd = testQualityDocker([
          '-v', "${covTarget.absolutePath}:/tmp/target-cov".toString(),
          '-e', "COV_FEATURES=${project.findProperty('features') ?: ''}".toString(),
        ]) + runScript('bash', 'run_coverage.sh') + [pkg, src, inContainer(outDir)] +
            (focus.isEmpty() ? new ArrayList<String>() : [focus])
        project.exec({ ExecSpec spec -> spec.commandLine(cmd) } as Action<ExecSpec>)
        task.logger.lifecycle("Per-test coverage for ${pkg} written to ${outDir} " +
            '(tests.tsv, lcov/, overlap_report.txt, redundancy_all.tsv)')
      }
    }
  }

  private void registerCoverage() {
    Project project = build.project
    project.tasks.register('coverage') { Task task ->
      task.group = 'Verification'
      task.description = 'Line coverage of the workspace, or of one crate with -Ppkg=<crate>, in the builder container ' +
          '([-Pfeatures=<a,b>]) -> build/reports/coverage/<pkg|workspace>/{summary.txt,lcov.info,html/}'
      task.dependsOn('fetchPrismLinux')
      task.doLast {
        String pkg = selectedPkg()
        String scope = pkg ?: 'workspace'
        File outDir = build.buildFile("reports/coverage/${scope}")
        outDir.mkdirs()
        File covTarget = build.buildFile('cov-target')
        covTarget.mkdirs()
        Object featuresProp = project.findProperty('features')
        List<String> testArgs = (pkg != null ? ['-p', pkg] : ['--workspace']) +
            (featuresProp != null ? ['--features', featuresProp.toString()] : new ArrayList<String>())
        String out = inContainer(outDir)
        String script = [
          "cargo llvm-cov --no-report ${testArgs.join(' ')}",
          "cargo llvm-cov report --summary-only | tee ${out}/summary.txt",
          "cargo llvm-cov report --lcov --output-path ${out}/lcov.info",
          "cargo llvm-cov report --html --output-dir ${out}",
        ].join(' && ')
        ensureTestQualityImage()

        List<String> cmd = testQualityDocker([
          '-v', "${covTarget.absolutePath}:/tmp/target-cov".toString(),
          '-e', 'CARGO_TARGET_DIR=/tmp/target-cov',
        ]) + ['bash', '-c', "set -eo pipefail; ${script}".toString()]
        project.exec({ ExecSpec spec -> spec.commandLine(cmd) } as Action<ExecSpec>)
        task.logger.lifecycle("Coverage for ${scope} written to ${outDir} (summary.txt, lcov.info, html/index.html)")
      }
    }
  }

  private void registerMutants() {
    Project project = build.project
    project.tasks.register('mutants') { Task task ->
      task.group = 'Verification'
      task.description = 'Mutation testing with a per-test kill matrix, in the builder container ' +
          '(-Ppkg=<crate> and/or -PinDiff=<diff> [-Pfiles=<glob,...>] [-Pexclude=<glob,...>] [-Pjobs=4] ' +
          '[-Ptimeout=60] [-Ptag=<name>] [-Pfeatures=<a,b>] [-PlistOnly] [-PfailOnMissed]) -> ' +
          'build/reports/test-quality/<pkg|workspace>/mutants[-<tag>]'
      task.dependsOn('fetchPrismLinux')
      task.doLast {
        String pkg = selectedPkg()
        String inDiff = project.findProperty('inDiff')?.toString()
        if (!pkg && !inDiff) {
          throw new GradleException('mutants needs -Ppkg=<crate>, -PinDiff=<diff file>, or both. ' +
              'A whole-workspace run without a diff takes many hours.')
        }
        String scope = pkg ?: 'workspace'
        String tag = project.findProperty('tag')?.toString()
        String outName = tag ? "mutants-${tag}".toString() : 'mutants'
        File outDir = new File(testQualityDir(scope), outName)
        outDir.parentFile.mkdirs()

        boolean arm64 = System.getProperty('os.arch') in ['aarch64', 'arm64']
        String rustflags = '-C link-arg=-fuse-ld=mold' + (arm64 ? ' -C target-feature=+fp16' : '')
        List<String> extraMounts = [
          '-v', "${project.rootDir.absolutePath}/model:/model:ro".toString(),
          '-e', "RUSTFLAGS=${rustflags}".toString(),
          '-e', 'CARGO_PROFILE_DEV_DEBUG=0',
          '-e', 'CARGO_PROFILE_TEST_DEBUG=0',
        ]
        Object expVer = project.findProperty('expansionServiceVersion')
        if (expVer != null) {
          extraMounts.addAll(['-e', "BEAM_EXPANSION_SERVICE_VERSION=${expVer}".toString()])
        }
        List<String> args = pkg != null ? ['--package', pkg] : ['--workspace']
        globs('files').each { String g -> args.addAll(['--file', g]) }
        globs('exclude').each { String g -> args.addAll(['--exclude', g]) }
        Object featuresProp = project.findProperty('features')
        if (featuresProp != null) {
          args.addAll(['--features', featuresProp.toString()])
        }

        if (inDiff != null) {
          File diffFile = project.file(inDiff)
          if (!diffFile.isFile()) {
            throw new GradleException("-PinDiff=${inDiff}: not a regular file")
          }
          String text = diffFile.text
              .replaceAll(/(?m)^(diff --git a\/|--- a\/|\+\+\+ b\/)sdks\/rust\//, '$1')
              .replaceAll(/(?m)^(diff --git a\/\S+ b\/)sdks\/rust\//, '$1')
          if (!(text =~ /(?m)^\+\+\+ b\/\S+\.rs\s*$/)) {
            task.logger.lifecycle("${inDiff} changes no Rust sources under sdks/rust; no mutants to test.")
            return
          }
          File staged = build.buildFile("tmp/test-quality/${scope}.diff")
          staged.parentFile.mkdirs()
          staged.text = text
          extraMounts.addAll(['-v', "${staged.absolutePath}:/tmp/in.diff:ro".toString()])
          args.addAll(['--in-diff', '/tmp/in.diff'])
        }

        boolean listOnly = flag('listOnly')
        if (listOnly) {
          args.add('--list')
        } else {
          args.addAll([
            '--test-tool', 'nextest',
            '--cargo-test-arg=--no-fail-fast',
            '--jobs', (project.findProperty('jobs') ?: '4').toString(),
            '--timeout', (project.findProperty('timeout') ?: '60').toString(),
            '--gitignore', 'true',
            '--output', inContainer(outDir),
          ])
        }

        ensureTestQualityImage()
        List<String> cmd = testQualityDocker(extraMounts) +
            runScript('bash', 'run_mutants.sh') +
            [inContainer(outDir), flag('failOnMissed').toString()] + args
        project.exec({ ExecSpec spec -> spec.commandLine(cmd) } as Action<ExecSpec>)
        if (!listOnly) {
          task.logger.lifecycle("Mutation results for ${scope} written to ${outDir} " +
              '(mutants.out/, kill_matrix.txt, kill_matrix.tsv)')
        }
      }
    }
  }

  private void registerTestQualityReport() {
    Project project = build.project
    project.tasks.register('testQualityReport') { Task task ->
      task.group = 'Verification'
      task.description = 'Remove/merge candidates from the coveragePerTest and mutants outputs of one crate ' +
          '(-Ppkg=<crate> [-PtestFileRegex=<regex>] [-PmutantsDirs=<dir,...>]) -> build/reports/test-quality/<pkg>/candidates.tsv'
      task.doLast {
        String pkg = requirePkg('testQualityReport')
        File dir = testQualityDir(pkg)
        File redundancy = new File(dir, 'coverage/redundancy_all.tsv')
        if (!redundancy.isFile()) {
          throw new GradleException(
              "${redundancy} not found; run ./gradlew :sdks:rust:coveragePerTest -Ppkg=${pkg} first")
        }
        Closure<Boolean> hasOutcomes = { File d ->
          new File(d, 'mutants.out/outcomes.json').isFile() || new File(d, 'outcomes.json').isFile()
        }
        File[] children = dir.listFiles() ?: new File[0]
        List<File> mutantDirs = children.findAll { File child ->
          child.isDirectory() && (child.name ==~ /mutants(-.+)?/) && hasOutcomes.call(child)
        }.sort()
        globs('mutantsDirs').each { String p ->
          File d = project.file(p)
          if (!hasOutcomes.call(d)) {
            throw new GradleException("-PmutantsDirs: ${d} holds no cargo-mutants outcomes")
          }
          mutantDirs.add(d)
        }
        if (mutantDirs.isEmpty()) {
          throw new GradleException(
              "No cargo-mutants outcomes under ${dir}; run ./gradlew :sdks:rust:mutants -Ppkg=${pkg} first")
        }
        List<String> args = [
          '--redundancy', inContainer(redundancy),
          '--out', inContainer(new File(dir, 'candidates.tsv')),
        ]
        mutantDirs.each { File mutantDir -> args.addAll(['--mutants', inContainer(mutantDir)]) }
        String regex = project.findProperty('testFileRegex')?.toString()
        if (regex != null && !regex.isEmpty()) {
          args.addAll(['--test-file-regex', regex])
        }
        containers.ensureBuilderImage()
        List<String> cmd = testQualityDocker(new ArrayList<String>(), false) +
            runScript('bash', 'run_report.sh') +
            [inContainer(new File(dir, 'candidates.txt'))] + args
        project.exec({ ExecSpec spec -> spec.commandLine(cmd) } as Action<ExecSpec>)
      }
    }
  }

  private void registerComplexityScorecard() {
    Project project = build.project
    project.tasks.register('complexityScorecard') { Task task ->
      task.group = 'Verification'
      task.description = 'Cross-SDK size and complexity scorecard (scc, lizard, feature points from docs/sdk-parity.md), ' +
          'in the builder container ([-PwithJavaTs]) -> build/reports/quality/scorecard.{md,json}'
      task.doLast {
        File outDir = build.buildFile('reports/quality')
        outDir.mkdirs()
        File md = new File(outDir, 'scorecard.md')
        File json = new File(outDir, 'scorecard.json')
        List<String> args = ['--repo-root', '/workspace', '--out', inContainer(md), '--json-out', inContainer(json)]
        if (flag('withJavaTs')) {
          args.add('--with-java-ts')
        }
        ensureTestQualityImage()
        List<String> cmd = testQualityDocker(new ArrayList<String>(), false) +
            runScript('python3', 'complexity_scorecard.py') + args
        project.exec({ ExecSpec spec -> spec.commandLine(cmd) } as Action<ExecSpec>)
        task.logger.lifecycle("Complexity scorecard written to ${md} and ${json}")
      }
    }
  }

  private void registerMachete() {
    Project project = build.project
    project.tasks.register('machete') { Task task ->
      task.group = 'Verification'
      task.description = 'Check Cargo.toml files for unused dependencies with cargo-machete in the builder container'
      task.doLast {
        ensureTestQualityImage()
        List<String> cmd = testQualityDocker(new ArrayList<String>(), false) + [
          'bash',
          '-c',
          'if ! command -v cargo-machete >/dev/null 2>&1; then ' +
              'echo "cargo-machete is missing from the builder image. Rebuild it with ./gradlew :sdks:rust:buildBuilderImage" >&2; ' +
              'exit 1; ' +
              'fi; ' +
              'exec cargo machete',
        ]
        project.exec({ ExecSpec spec -> spec.commandLine(cmd) } as Action<ExecSpec>)
      }
    }
  }
}
