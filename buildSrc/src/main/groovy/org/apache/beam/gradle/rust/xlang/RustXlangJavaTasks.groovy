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
package org.apache.beam.gradle.rust.xlang

import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import org.apache.beam.gradle.BeamModulePlugin
import org.apache.beam.gradle.rust.BeamModuleExtras
import org.apache.beam.gradle.rust.RustSdkBuild
import org.gradle.api.Action
import org.gradle.api.GradleException
import org.gradle.api.JavaVersion
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.tasks.bundling.AbstractArchiveTask
import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.testing.TestFilter

/** Java cross-language ValidatesRunner tasks. */
@CompileStatic
class RustXlangJavaTasks {

  private static final String JAVA_XLANG_CLASS =
      'org.apache.beam.sdk.util.construction.ValidateRunnerXlangTest'

  static void register(RustXlangTasks xlang) {
    RustSdkBuild build = xlang.build
    Project project = build.project

    if (project.gradle.startParameter.taskNames.any { String name ->
      name.contains('CrossLanguageRunnerJava')
    }) {
      project.evaluationDependsOn(':sdks:java:core')
      project.evaluationDependsOn(':sdks:java:extensions:python')
      project.evaluationDependsOn(':runners:prism:java')
      project.evaluationDependsOn(':runners:google-cloud-dataflow-java')
    }

    Object testJavaVerProp = project.findProperty('testJavaVersion')
    String imageJavaVersion = testJavaVerProp != null
        ? BeamModulePlugin.getSupportedJavaVersion(testJavaVerProp.toString())
        : BeamModuleExtras.currentJavaVersion(project)

    String javaSdkImage = xlang.registerSdkDataflowImageTask([
      sdk        : 'Java',
      imageSuffix: "${imageJavaVersion}_sdk".toString(),
      gradleTask : ":sdks:java:container:${imageJavaVersion}:docker".toString(),
    ])

    xlang.registerXlangSuiteTasks(
        [
          sdk              : 'Java',
          testExamples     : 'SingleInputOutputTest,GroupByKeyTest',
          dataflowDependsOn: [':runners:google-cloud-dataflow-java:worker:shadowJar'],
          dataflowHint     : '[-PxlangParallelism=8] [-PjavaSdkImage=<image>]',
        ],
        Test,
        { Test task ->
          task.maxParallelForks = 1
          configureJavaXlangTest(xlang, task, ':runners:prism:java', xlang.testingImage) {
            [
              '--runner=TestPrismRunner',
              '--experiments=beam_fn_api',
              '--defaultEnvironmentType=LOOPBACK',
              '--prismLogLevel=warn',
              "--prismLocation=${xlang.config.prism.prismHostBinary.absolutePath}".toString(),
              '--enableWebUI=false',
            ]
          }
        } as Action<Test>,
        { Test task ->
          task.maxParallelForks = (project.findProperty('xlangParallelism') ?: '8').toString().toInteger()
          configureJavaXlangTest(xlang, task, ':runners:google-cloud-dataflow-java', xlang.dataflowImage) {
            xlang.xlangDataflowFlags([
              tempFlag       : 'tempRoot',
              numWorkersFlag : 'numWorkers',
              machineTypeFlag: 'workerMachineType',
              extraFlags     : javaSdkImage != null
                  ? ["--sdkContainerImage=${javaSdkImage}".toString()]
                  : new ArrayList<String>(),
            ])
          }
        } as Action<Test>)

    String javaTestServiceProject = ':sdks:java:testing:expansion-service'
    String javaMajor = JavaVersion.current().majorVersion
    String localJavaSdkImage = "${BeamModuleExtras.dockerDefaultRepoRoot(project)}/" +
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
        if (!xlang.dockerReachable()) {
          throw new GradleException(
              'Docker is not reachable. The cross-language suite needs it to start the Java SDK container.')
        }
        if (!xlang.imageExists(localJavaSdkImage)) {
          throw new GradleException("${localJavaSdkImage} is not available locally. Build it with " +
              "./gradlew :sdks:java:container:java${javaMajor}:docker, or add -PbuildJavaContainer.")
        }
        AbstractArchiveTask jarTask = (AbstractArchiveTask) project.project(javaTestServiceProject)
            .tasks.named('buildTestExpansionServiceJar').get()
        File jar = jarTask.archiveFile.get().asFile
        String java = "${System.getProperty('java.home')}/bin/java"
        xlang.runXlangTestsAgainstService(
            'Java', [java, '-jar', jar.absolutePath], build.rustDir,
            'BEAM_JAVA_TEST_EXPANSION_ADDR', new ArrayList<String>())
      }
    }
  }

  private static void configureJavaXlangTest(
      RustXlangTasks xlang,
      Test task,
      String runnerProject,
      String workerImage,
      Closure<List<String>> pipelineOpts) {
    RustSdkBuild build = xlang.build
    Project project = build.project
    RustXlangTasks.ServiceHandle[] holder = new RustXlangTasks.ServiceHandle[1]
    String testJavaVersion = project.findProperty('testJavaVersion')?.toString()
    String testJavaHome = testJavaVersion != null
        ? project.findProperty("java${testJavaVersion}Home")?.toString() : null

    task.outputs.upToDateWhen { false }
    task.binaryResultsDirectory.set(build.buildFile("test-results/${task.name}/binary"))
    task.reports.html.outputLocation.set(build.buildFile("reports/tests/${task.name}"))
    task.reports.junitXml.outputLocation.set(build.buildFile("test-results/${task.name}"))
    BeamModuleExtras.configureJavaXlangClasspaths(project, task, runnerProject)

    List<String> patterns = xlang.selectedTests { String name -> "${JAVA_XLANG_CLASS}\$${name}".toString() }
        ?: ["${JAVA_XLANG_CLASS}\$*".toString()]
    task.filter({ TestFilter filter ->
      patterns.each { String pat -> filter.includeTestsMatching(pat) }
    } as Action<TestFilter>)
    if (testJavaHome != null && !testJavaHome.isEmpty()) {
      task.executable = "${testJavaHome}/bin/java"
    }
    task.maxHeapSize = '4g'

    task.doFirst {
      String host = xlang.config.dockerHost()
      if (host != null) {
        task.environment('DOCKER_HOST', host)
      }
      RustXlangTasks.ServiceHandle service = xlang.startExpansionService([workerImage: workerImage])
      holder[0] = service
      task.systemProperty('expansionPort', service.port)
      task.systemProperty('expansionJar', '')
      task.systemProperty('beamTestPipelineOptions', JsonOutput.toJson(pipelineOpts.call()))
    }
    task.doLast {
      if (holder[0] != null) {
        xlang.stopExpansionService(holder[0])
      }
    }
  }
}
