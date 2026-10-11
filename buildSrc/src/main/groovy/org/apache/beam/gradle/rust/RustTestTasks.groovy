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
import java.util.regex.Matcher
import org.gradle.api.Action
import org.gradle.api.GradleException
import org.gradle.api.Task
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.logging.Logger
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.MapProperty
import org.gradle.api.provider.Provider
import org.gradle.api.provider.Property
import org.gradle.api.services.BuildService
import org.gradle.api.services.BuildServiceParameters
import org.gradle.api.services.BuildServiceSpec
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskProvider
import org.gradle.api.tasks.TaskState
import org.gradle.workers.WorkAction
import org.gradle.workers.WorkParameters

/**
 * The test tasks: {@code compileWorkspaceTests}, {@code testWorkspaceDoctests}, one cacheable
 * {@code test_<pkg>} task for each crate, {@code testSummary} and {@code test}.
 *
 * <p>The inputs of {@code test_<pkg>} are the sources and tests of the crate, and the library
 * sources of the workspace crates that it depends on. A change to a test of one crate does not
 * run the tests of the crates that depend on it.
 */
@CompileStatic
class RustTestTasks {

  /** Limits how many crate test tasks execute in parallel. */
  abstract static class RustTestCoordinatorService implements BuildService<BuildServiceParameters.None> {}

  /** Parameters for {@link CrateTestWorkAction}. */
  static interface CrateTestWorkParams extends WorkParameters {
    Property<String> getPkg()
    Property<File> getRustDir()
    Property<File> getCrateDir()
    RegularFileProperty getManifestFile()
    RegularFileProperty getStampFile()
    Property<Boolean> getUseManifest()
    Property<Boolean> getOnlyFeaturesSet()
    Property<Boolean> getHasDoctests()
    Property<Boolean> getContinueOnFailure()
    ListProperty<String> getFeatureScope()
    ListProperty<String> getTestTargets()
    ListProperty<String> getTestExcludes()
    ListProperty<String> getDoctestExcludes()
    ListProperty<String> getTestArgs()
    MapProperty<String, String> getEnvironment()
  }

  /** Runs a crate's test binaries (or scoped {@code cargo test}) and writes its result and stamp. */
  abstract static class CrateTestWorkAction implements WorkAction<CrateTestWorkParams> {
    static final Object CARGO_LOCK = new Object()
    static final Object PRINT_LOCK = new Object()

    static int runCapturing(List<String> cmd, File dir, Map<String, String> env, StringBuilder out) {
      ProcessBuilder pb = new ProcessBuilder(cmd)
      pb.directory(dir)
      pb.redirectErrorStream(true)
      pb.environment().putAll(env)
      Process proc = pb.start()
      String text = proc.inputStream.getText('UTF-8')
      int exit = proc.waitFor()
      out.append(text)
      return exit
    }

    static File resultFileOf(File stamp) {
      return new File(stamp.parentFile, "${stamp.name}.result")
    }

    @Override
    void execute() {
      runSuite()
    }

    private void runSuite() {
      CrateTestWorkParams p = parameters
      String pkg = p.pkg.get()
      File rustDir = p.rustDir.get()
      File crateDir = p.crateDir.get()
      List<String> testArgs = p.testArgs.get()
      boolean cont = p.continueOnFailure.get()
      List<String> noFailFast = cont ? ['--no-fail-fast'] : new ArrayList<String>()
      Map<String, String> cargoEnv = new LinkedHashMap<>(p.environment.get())
      Map<String, String> binEnv = new LinkedHashMap<>(cargoEnv)
      binEnv.put('CARGO_MANIFEST_DIR', crateDir.absolutePath)

      File stamp = p.stampFile.get().asFile
      File result = resultFileOf(stamp)
      result.delete()
      synchronized (PRINT_LOCK) {
        System.out.println("Running ${pkg}")
        System.out.flush()
      }

      StringBuilder combinedOut = new StringBuilder()
      boolean failedExit = false

      if (p.useManifest.get()) {
        List<String> binaries = []
        File mf = p.manifestFile.get().asFile
        mf.eachLine('UTF-8') { String line ->
          if (!line.isEmpty()) {
            String[] parts = line.split('\t', 3)
            if (parts.length == 3 && parts[0] == pkg) {
              binaries.add(parts[2])
            }
          }
        }
        for (String bin : binaries) {
          int code = runCapturing([bin] + testArgs, crateDir, binEnv, combinedOut)
          if (code != 0) {
            failedExit = true
            if (!cont) {
              break
            }
          }
        }
      } else if (p.onlyFeaturesSet.get()) {
        List<String> cargoCmd = ['cargo', 'test', '-p', pkg] + p.featureScope.get() + noFailFast +
            (testArgs.isEmpty() ? new ArrayList<String>() : ['--'] + testArgs)
        int code
        synchronized (CARGO_LOCK) {
          code = runCapturing(cargoCmd, rustDir, cargoEnv, combinedOut)
        }
        if (code != 0) {
          failedExit = true
        }
      } else {
        List<String> targets = p.testTargets.get()
        if (!targets.isEmpty()) {
          List<String> testCmd = ['cargo', 'test', '--workspace'] + p.testExcludes.get() +
              targets.collectMany { String t -> ['--test', t] } + noFailFast +
              (testArgs.isEmpty() ? new ArrayList<String>() : ['--'] + testArgs)
          int code
          synchronized (CARGO_LOCK) {
            code = runCapturing(testCmd, rustDir, cargoEnv, combinedOut)
          }
          if (code != 0) {
            failedExit = true
          }
        }
        if ((!failedExit || cont) && p.hasDoctests.get()) {
          List<String> docCmd = ['cargo', 'test', '--doc', '--workspace'] + p.doctestExcludes.get() +
              noFailFast + (testArgs.isEmpty() ? new ArrayList<String>() : ['--'] + testArgs)
          int code
          synchronized (CARGO_LOCK) {
            code = runCapturing(docCmd, rustDir, cargoEnv, combinedOut)
          }
          if (code != 0) {
            failedExit = true
          }
        }
      }

      Counts counts = countResults(combinedOut.toString(), testArgs.isEmpty())
      synchronized (PRINT_LOCK) {
        if (failedExit && combinedOut.length() > 0) {
          System.out.print(combinedOut.toString())
          if (!combinedOut.toString().endsWith('\n')) {
            System.out.println()
          }
        }
        System.out.println("Total for ${pkg}: ${counts.passed} passed; ${counts.failed} failed; " +
            "${counts.ignored} ignored; ${counts.filtered} filtered out")
        System.out.flush()
      }

      String line = "passed=${counts.passed} failed=${counts.failed} " +
          "ignored=${counts.ignored} filtered=${counts.filtered}"
      result.parentFile.mkdirs()
      result.text = "${line} status=${failedExit ? 'FAILED' : 'EXECUTED'}\n"
      if (failedExit) {
        stamp.delete()
        throw new GradleException("Tests failed for ${pkg}")
      }
      stamp.parentFile.mkdirs()
      stamp.text = "${line}\n"
    }
  }

  /** The test counts of one crate or doctest suite. */
  private static class Counts {
    int passed
    int failed
    int ignored
    int filtered
    String status
  }

  /** Crates whose tests start a Java expansion service from a shadow JAR built in this repo. */
  private static final Map<String, String> TEST_EXPANSION_SERVICE_JARS = [:]

  static void register(RustSdkBuild build) {
    int testConcurrency = Math.max(
        1, (build.project.findProperty('testConcurrency')?.toString()?.toInteger()) ?: 4)
    Provider<RustTestCoordinatorService> coordinator = build.project.gradle.sharedServices.registerIfAbsent(
        "rustTestCoordinator_${build.project.path.replace(':', '_')}".toString(),
        RustTestCoordinatorService.class,
        { BuildServiceSpec<BuildServiceParameters.None> spec ->
          spec.maxParallelUsages.set(testConcurrency)
        } as Action<BuildServiceSpec<BuildServiceParameters.None>>)
    String defaultThreads = System.getenv('RUST_TEST_THREADS') ?: (testConcurrency > 1
        ? Math.max(2, (int) Math.ceil(Runtime.runtime.availableProcessors() / (double) testConcurrency)).toString()
        : null)

    registerCompileWorkspaceTests(build)
    File doctestStampFile = registerTestWorkspaceDoctests(build, coordinator, defaultThreads)

    Map<String, File> stampFiles = new LinkedHashMap<>()
    for (String pkg : build.workspace.topoOrder) {
      File stamp = stampFile(build, pkg)
      stampFiles[pkg] = stamp
      build.testTasks[pkg] = registerCrateTest(build, pkg, stamp, coordinator, defaultThreads)
    }
    TEST_EXPANSION_SERVICE_JARS.each { String pkg, String jar ->
      build.testTasks[pkg]?.configure { Task task -> task.dependsOn(jar) }
    }
    registerSummary(build, stampFiles, doctestStampFile, coordinator)
    registerTest(build)
  }

  private static void registerCompileWorkspaceTests(RustSdkBuild build) {
    File manifest = build.workspaceTestManifest
    build.project.tasks.register('compileWorkspaceTests') { Task task ->
      task.group = 'Verification'
      task.description = 'Compile all workspace integration test binaries in one Cargo pass'
      task.mustRunAfter('fmt', 'clippy', 'buildWorkspace')
      task.inputs.property('beamSdkVersion', build.versions.sdkVersion)
      task.inputs.property('beamReleaseVersion', build.versions.releaseVersion)
      task.inputs.files(build.workspaceRootFiles).withPathSensitivity(PathSensitivity.RELATIVE)
      task.inputs.files(build.modelProtoDirs).withPathSensitivity(PathSensitivity.RELATIVE)
      task.inputs.files(build.libAndTestInputs(build.workspace.topoOrder))
          .withPathSensitivity(PathSensitivity.RELATIVE)
      task.outputs.file(manifest)
      task.outputs.upToDateWhen {
        new File(build.rustDir, 'target').exists() &&
            manifest.isFile() &&
            manifest.readLines('UTF-8').every { String line ->
              line.isEmpty() || new File(line.split('\t', 3)[2]).isFile()
            }
      }
      task.doLast {
        ProcessBuilder pb = new ProcessBuilder(
            ['cargo', 'test'] + build.workspaceScope + ['--no-run', '--message-format=json'])
        pb.directory(build.rustDir)
        pb.environment().putAll(build.versions.cargoEnv())
        Process proc = pb.start()
        Thread errThread = Thread.start {
          proc.errorStream.eachLine('UTF-8') { String line -> System.err.println(line) }
        }
        JsonSlurper slurper = new JsonSlurper()
        List<String> entries = []
        StringBuilder diagnostics = new StringBuilder()
        proc.inputStream.eachLine('UTF-8') { String line ->
          String trimmed = line.trim()
          if (trimmed.startsWith('{')) {
            collectManifestMessage(slurper.parseText(trimmed), build.workspace.dirToPkg, entries, diagnostics)
          }
        }
        errThread.join()
        int exit = proc.waitFor()
        if (exit != 0) {
          if (diagnostics.length() > 0) {
            System.err.print(diagnostics.toString())
          }
          manifest.delete()
          throw new GradleException("cargo test --workspace --no-run failed with exit code ${exit}")
        }
        entries.sort()
        manifest.parentFile.mkdirs()
        manifest.setText(entries.isEmpty() ? '' : (entries.join('\n') + '\n'), 'UTF-8')
        Object autoPrune = build.project.findProperty('autoPruneTarget')
        if (autoPrune == null || autoPrune.toString() != 'false') {
          build.pruneCargoTarget([quietIfEmpty: true])
        }
      }
    }
  }

  @CompileDynamic
  private static void collectManifestMessage(
      Object msg, Map<String, String> dirToPkg, List<String> entries, StringBuilder diagnostics) {
    if (msg.reason == 'compiler-message' && msg.message?.rendered) {
      if (msg.message.level == 'error') {
        diagnostics.append(msg.message.rendered)
      }
    } else if (msg.reason == 'compiler-artifact' && msg.executable != null && msg.manifest_path) {
      boolean isTestTarget = msg.target?.test == true &&
          (msg.profile?.test == true || (msg.target?.kind instanceof List && 'test' in msg.target.kind))
      if (isTestTarget) {
        String pkg = dirToPkg[new File(msg.manifest_path.toString()).parentFile.canonicalPath]
        if (pkg != null) {
          entries.add("${pkg}\t${msg.target.name}\t${msg.executable}".toString())
        }
      }
    }
  }

  private static File registerTestWorkspaceDoctests(
      RustSdkBuild build,
      Provider<RustTestCoordinatorService> coordinator,
      String defaultThreads) {
    List<String> testArgs = build.testArgs
    File stampFile = build.buildFile(
        "test-results/cargo/doctests/test-workspace${testArgs ? '-filtered' : ''}.txt")
    TaskProvider<Task> tp = build.project.tasks.register('testWorkspaceDoctests') { Task task ->
      task.group = 'Verification'
      task.description = 'Run all workspace Rust doctests in one Cargo pass'
      task.usesService(coordinator)
      task.dependsOn('compileWorkspaceTests')
      task.mustRunAfter('fmt', 'clippy', 'buildWorkspace')
      task.finalizedBy('testSummary')

      task.inputs.property('beamSdkVersion', build.versions.sdkVersion)
      task.inputs.property('beamReleaseVersion', build.versions.releaseVersion)
      task.inputs.property('testArgs', testArgs)
      task.inputs.files(build.workspaceRootFiles).withPathSensitivity(PathSensitivity.RELATIVE)
      task.inputs.files(build.modelProtoDirs).withPathSensitivity(PathSensitivity.RELATIVE)
      task.inputs.files(build.libInputs(build.workspace.topoOrder))
          .withPathSensitivity(PathSensitivity.RELATIVE)

      task.outputs.file(stampFile)
      task.outputs.upToDateWhen { testArgs.isEmpty() && readStamp(stampFile) != null }
      task.outputs.cacheIf { testArgs.isEmpty() }

      task.doLast {
        File result = CrateTestWorkAction.resultFileOf(stampFile)
        result.delete()
        List<String> docCmd = ['cargo', 'test', '--doc'] + build.workspaceScope + build.noFailFast +
            (testArgs.isEmpty() ? new ArrayList<String>() : ['--'] + testArgs)
        Map<String, String> procEnv = baseTestEnv(build, defaultThreads)
        procEnv.putAll(build.doctestExtraEnv)

        synchronized (CrateTestWorkAction.PRINT_LOCK) {
          System.out.println('Running workspace doctests')
          System.out.flush()
        }
        StringBuilder combinedOut = new StringBuilder()
        int exit
        synchronized (CrateTestWorkAction.CARGO_LOCK) {
          exit = CrateTestWorkAction.runCapturing(docCmd, build.rustDir, procEnv, combinedOut)
        }
        boolean failedExit = exit != 0
        Counts counts = countResults(combinedOut.toString(), false)

        synchronized (CrateTestWorkAction.PRINT_LOCK) {
          if (failedExit && combinedOut.length() > 0) {
            System.out.print(combinedOut.toString())
            if (!combinedOut.toString().endsWith('\n')) {
              System.out.println()
            }
          }
          System.out.println("Total for workspace doctests: ${counts.passed} passed; " +
              "${counts.failed} failed; ${counts.ignored} ignored; ${counts.filtered} filtered out")
          System.out.flush()
        }

        String line = "passed=${counts.passed} failed=${counts.failed} " +
            "ignored=${counts.ignored} filtered=${counts.filtered}"
        result.parentFile.mkdirs()
        result.text = "${line} status=${failedExit ? 'FAILED' : 'EXECUTED'}\n"
        if (failedExit) {
          stampFile.delete()
          throw new GradleException('Workspace doctests failed')
        }
        RustSdkBuild.writeStamp(stampFile, "${line}\n")
      }
    }
    build.testWorkspaceDoctestsTask = tp
    return stampFile
  }

  private static Map<String, String> baseTestEnv(RustSdkBuild build, String defaultThreads) {
    Map<String, String> env = new LinkedHashMap<>(build.versions.cargoEnv())
    String javaHome = System.getenv('JAVA_HOME') ?: System.getProperty('java.home')
    if (javaHome != null) {
      env.put('JAVA_HOME', javaHome)
    }
    if (defaultThreads != null && System.getenv('RUST_TEST_THREADS') == null) {
      env.put('RUST_TEST_THREADS', defaultThreads)
    }
    return env
  }

  private static File stampFile(RustSdkBuild build, String pkg) {
    boolean narrowFeatures = build.selectedPkg == pkg && build.onlyFeatures != null
    String featureSuffix = narrowFeatures
        ? "-features-${build.onlyFeatures.replaceAll('[^a-zA-Z0-9_-]', '_')}" : ''
    String filterSuffix = build.testArgs ? '-filtered' : ''
    String modeSuffix = narrowFeatures ? '' : (build.selectedPkg != null ? '-pkg-workspace' : '-workspace')
    return build.buildFile("test-results/cargo/${pkg}/test${modeSuffix}${featureSuffix}${filterSuffix}.txt")
  }

  private static TaskProvider<Task> registerCrateTest(
      RustSdkBuild build,
      String pkg,
      File stampFile,
      Provider<RustTestCoordinatorService> coordinator,
      String defaultThreads) {
    List<String> testArgs = build.testArgs
    boolean narrowFeatures = build.selectedPkg == pkg && build.onlyFeatures != null
    List<String> featureScope = narrowFeatures ? build.featureScope : new ArrayList<String>()
    Set<String> closure = build.workspace.crateClosure[pkg]
    boolean hasDoctests = Boolean.TRUE == build.workspace.crateHasDoctests[pkg]
    List<String> pkgTests = build.workspace.crateTestTargets[pkg] ?: new ArrayList<String>()
    List<String> pkgTestExcludes = build.workspace.crateTestExcludes[pkg] ?: new ArrayList<String>()
    List<String> pkgDoctestExcludes = build.workspace.crateDoctestExcludes[pkg] ?: new ArrayList<String>()

    return build.project.tasks.register("test_${pkg}".toString()) { Task task ->
      task.group = 'Verification'
      task.description = "Run tests for ${pkg}"
      task.usesService(coordinator)
      task.finalizedBy('testSummary')
      task.mustRunAfter('fmt', 'clippy', 'buildWorkspace')
      if (build.selectedPkg != null) {
        task.mustRunAfter('compileWorkspaceTests')
      } else {
        task.dependsOn('compileWorkspaceTests')
      }

      task.inputs.property('pkg', pkg)
      task.inputs.property('beamSdkVersion', build.versions.sdkVersion)
      task.inputs.property('beamReleaseVersion', build.versions.releaseVersion)
      task.inputs.property('featureScope', featureScope)
      task.inputs.property('testArgs', testArgs)
      task.inputs.property('featureUnification', narrowFeatures ? 'crate' : 'workspace')
      task.inputs.files(build.workspaceRootFiles).withPathSensitivity(PathSensitivity.RELATIVE)
      if ('apache-beam-model' in closure) {
        task.inputs.files(build.modelProtoDirs).withPathSensitivity(PathSensitivity.RELATIVE)
      }
      closure.each { String dep ->
        task.inputs.files(build.workspace.libInputFiles(dep)).withPathSensitivity(PathSensitivity.RELATIVE)
      }
      task.inputs.files(build.testInputFiles(pkg)).withPathSensitivity(PathSensitivity.RELATIVE)

      task.outputs.file(stampFile)
      task.outputs.upToDateWhen { testArgs.isEmpty() && readStamp(stampFile) != null }
      task.outputs.cacheIf { testArgs.isEmpty() }

      task.doLast {
        Map<String, String> procEnv = baseTestEnv(build, defaultThreads)
        Map<String, String> extra = build.crateExtraEnv[pkg]
        if (extra != null) {
          procEnv.putAll(extra)
        }
        boolean keepGoing = build.project.gradle.startParameter.continueOnFailure
        build.workerExecutor.noIsolation().submit(CrateTestWorkAction.class, { CrateTestWorkParams p ->
          p.pkg.set(pkg)
          p.rustDir.set(build.rustDir)
          p.crateDir.set(build.workspace.crateDir[pkg])
          p.manifestFile.set(build.workspaceTestManifest)
          p.stampFile.set(stampFile)
          p.useManifest.set(build.selectedPkg == null)
          p.onlyFeaturesSet.set(narrowFeatures)
          p.hasDoctests.set(hasDoctests)
          p.continueOnFailure.set(keepGoing)
          p.featureScope.set(featureScope)
          p.testTargets.set(pkgTests)
          p.testExcludes.set(pkgTestExcludes)
          p.doctestExcludes.set(pkgDoctestExcludes)
          p.testArgs.set(testArgs)
          p.environment.set(procEnv)
        } as Action<CrateTestWorkParams>)
      }
    }
  }

  /** Adds the {@code test result:} lines of the cargo output. */
  private static Counts countResults(String output, boolean noFilter) {
    Counts total = new Counts()
    Matcher m = (output =~
        /test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; \d+ measured; (\d+) filtered out/)
    while (m.find()) {
      int filtered = m.group(4) as int
      // A test that runs its own binary with `--exact` prints a second line with a filter.
      if (noFilter && filtered > 0) {
        continue
      }
      total.passed += m.group(1) as int
      total.failed += m.group(2) as int
      total.ignored += m.group(3) as int
      total.filtered += filtered
    }
    return total
  }

  /** Reads the counts of a stamp file, or null if the file has none. */
  private static Counts readStamp(File stampFile) {
    if (!stampFile.isFile()) {
      return null
    }
    Matcher m = (stampFile.text =~ /passed=(\d+)\s+failed=(\d+)\s+ignored=(\d+)\s+filtered=(\d+)/)
    if (!m.find()) {
      return null
    }
    return new Counts(
        passed: m.group(1) as int,
        failed: m.group(2) as int,
        ignored: m.group(3) as int,
        filtered: m.group(4) as int)
  }

  private static Counts readResultFile(File resultFile) {
    Counts counts = readStamp(resultFile)
    if (counts == null) {
      return null
    }
    Matcher status = (resultFile.text =~ /status=(\w+)/)
    counts.status = status.find() ? status.group(1) : 'EXECUTED'
    return counts
  }

  private static void registerSummary(
      RustSdkBuild build,
      Map<String, File> stampFiles,
      File doctestStampFile,
      Provider<RustTestCoordinatorService> coordinator) {
    build.project.tasks.register('testSummary') { Task task ->
      task.group = 'Verification'
      task.description = 'Print the aggregated per-crate test summary'
      task.usesService(coordinator)
      task.mustRunAfter(build.testTasks.values())
      task.mustRunAfter(build.testWorkspaceDoctestsTask)
      task.doLast {
        List<String> packages = build.selectedPkg != null ? [build.selectedPkg] : build.defaultPackages()
        Map<String, Counts> rows = new LinkedHashMap<>()
        packages.each { String pkg ->
          Counts counts = collectTaskRow(build.testTasks[pkg], stampFiles[pkg])
          if (counts != null) {
            rows[pkg] = counts
          }
        }
        if (build.selectedPkg == null && build.testWorkspaceDoctestsTask != null) {
          Counts docCounts = collectTaskRow(build.testWorkspaceDoctestsTask, doctestStampFile)
          if (docCounts != null) {
            rows['workspace doctests'] = docCounts
          }
        }
        printSummary(task.logger, rows)
      }
    }
  }

  private static Counts collectTaskRow(TaskProvider<Task> tp, File stamp) {
    if (tp == null || stamp == null) {
      return null
    }
    TaskState state = tp.get().state
    if (state.upToDate) {
      Counts cached = readStamp(stamp)
      if (cached != null) {
        cached.status = state.skipMessage ?: 'UP-TO-DATE'
        return cached
      }
      return null
    }
    if (state.executed) {
      return readResultFile(CrateTestWorkAction.resultFileOf(stamp))
    }
    return null
  }

  private static void printSummary(Logger logger, Map<String, Counts> rows) {
    if (rows.isEmpty()) {
      return
    }
    int crateCount = (int) rows.keySet().count { String k -> k != 'workspace doctests' }
    int nameWidth = Math.max(28, rows.keySet().collect { String pkg -> pkg.length() }.max() + 2)
    String separator = '-' * (nameWidth + 46)
    String rowFormat = "%-${nameWidth}s %8d %8d %9d %10d"
    logger.lifecycle(separator)
    logger.lifecycle(String.format("%-${nameWidth}s %8s %8s %9s %10s   %s",
        'Crate', 'Passed', 'Failed', 'Ignored', 'Filtered', 'Status'))
    logger.lifecycle(separator)
    rows.each { String pkg, Counts c ->
      logger.lifecycle(String.format("${rowFormat}   %s", pkg, c.passed, c.failed, c.ignored, c.filtered, c.status))
    }
    logger.lifecycle(separator)
    logger.lifecycle(String.format(rowFormat,
        "Total (${crateCount} ${crateCount == 1 ? 'crate' : 'crates'})".toString(),
        rows.values().sum { Counts c -> c.passed },
        rows.values().sum { Counts c -> c.failed },
        rows.values().sum { Counts c -> c.ignored },
        rows.values().sum { Counts c -> c.filtered }))
    logger.lifecycle(separator)
  }

  private static void registerTest(RustSdkBuild build) {
    build.project.tasks.register('test') { Task task ->
      task.group = 'Verification'
      task.description = 'Run tests (the workspace, or one crate with -Ppkg=<crate>; ' +
          '-PtestArgs=<args> after --; -PtestConcurrency=<n> parallel crates; --continue runs every test binary)'
      task.finalizedBy('testSummary')
      String pkg = build.selectedPkg
      if (pkg == null) {
        task.dependsOn('compileWorkspaceTests', 'testWorkspaceDoctests')
        task.dependsOn(build.project.provider {
          build.defaultPackages().collect { String name -> build.testTasks[name] }
        })
      } else if (build.testTasks.containsKey(pkg)) {
        task.dependsOn(build.testTasks[pkg])
      } else {
        throw new GradleException("Unknown Rust workspace package '${pkg}'. " +
            "Available packages: ${build.workspace.topoOrder.join(', ')}")
      }
    }
  }
}
