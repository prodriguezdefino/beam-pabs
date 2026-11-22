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

package org.apache.beam.gradle.rust.benchmark.io

import groovy.transform.CompileStatic
import org.gradle.api.Action
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.repositories.MavenArtifactRepository
import org.gradle.api.attributes.AttributeCompatibilityRule
import org.gradle.api.attributes.AttributeContainer
import org.gradle.api.attributes.AttributeMatchingStrategy
import org.gradle.api.attributes.AttributesSchema
import org.gradle.api.attributes.Bundling
import org.gradle.api.attributes.Category
import org.gradle.api.attributes.CompatibilityCheckDetails
import org.gradle.api.attributes.LibraryElements
import org.gradle.api.attributes.Usage
import org.gradle.api.attributes.java.TargetJvmEnvironment
import org.gradle.api.model.ObjectFactory
import org.gradle.process.ExecSpec

/** The Java classpath and the Python virtualenv that the Java and Python benchmark launchers use. */
@CompileStatic
class BenchmarkEnvironments {

  /** Accepts jars published only with a shadowed variant in the external javaBenchmark classpath. */
  static class ShadowedIsExternal implements AttributeCompatibilityRule<Bundling> {
    @Override
    void execute(CompatibilityCheckDetails<Bundling> details) {
      if (details.consumerValue?.name == Bundling.EXTERNAL &&
          details.producerValue?.name == Bundling.SHADOWED) {
        details.compatible()
      }
    }
  }

  /** The {@code javaBenchmark} configuration: the released Java SDK jars of {@code beamVersion}. */
  static Configuration javaClasspath(Project project, String beamVersion) {
    project.repositories.maven({ MavenArtifactRepository repo ->
      repo.url = project.uri('https://packages.confluent.io/maven/')
    } as Action<MavenArtifactRepository>)

    ObjectFactory objects = project.objects
    Configuration cfg = project.configurations.create('javaBenchmark') { Configuration c ->
      c.canBeConsumed = false
      c.attributes({ AttributeContainer attrs ->
        attrs.attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage, Usage.JAVA_RUNTIME))
        attrs.attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category, Category.LIBRARY))
        attrs.attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE,
            objects.named(LibraryElements, LibraryElements.JAR))
        attrs.attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling, Bundling.EXTERNAL))
        attrs.attribute(TargetJvmEnvironment.TARGET_JVM_ENVIRONMENT_ATTRIBUTE,
            objects.named(TargetJvmEnvironment, TargetJvmEnvironment.STANDARD_JVM))
      } as Action<AttributeContainer>)
    }

    project.dependencies.attributesSchema({ AttributesSchema schema ->
      schema.attribute(Bundling.BUNDLING_ATTRIBUTE, { AttributeMatchingStrategy<Bundling> strategy ->
        strategy.compatibilityRules.add(ShadowedIsExternal)
      } as Action<AttributeMatchingStrategy<Bundling>>)
    } as Action<AttributesSchema>)

    [
      'beam-sdks-java-core',
      'beam-sdks-java-managed',
      'beam-sdks-java-io-google-cloud-platform',
      'beam-sdks-java-io-kafka',
      'beam-sdks-java-io-iceberg',
      'beam-runners-google-cloud-dataflow-java',
    ].each { String artifact ->
      project.dependencies.add('javaBenchmark', "org.apache.beam:${artifact}:${beamVersion}".toString())
    }
    project.dependencies.add('javaBenchmark', 'org.apache.iceberg:iceberg-gcp:1.11.0')
    // The IcebergIO read needs the Hadoop input formats. The version is the pin of sdks/java/io/iceberg.
    project.dependencies.add('javaBenchmark', 'org.apache.hadoop:hadoop-mapreduce-client-core:3.3.6')
    project.dependencies.add('javaBenchmark', 'org.apache.kafka:kafka-clients:3.9.2')
    project.dependencies.add('javaBenchmark', 'com.github.erosb:everit-json-schema:1.14.2')
    project.dependencies.add('javaBenchmark', 'org.apache.arrow:arrow-memory-netty:17.0.0')
    project.dependencies.add('javaBenchmark', 'junit:junit:4.13.2')
    project.dependencies.add('javaBenchmark', 'org.hamcrest:hamcrest:2.1')
    project.dependencies.add('javaBenchmark', 'org.slf4j:slf4j-jdk14:2.0.16')
    return cfg
  }

  /** Registers {@code setupBenchmarkPythonEnv}, which installs {@code apache-beam[gcp]==beamVersion} in {@code benchVenv}. */
  static void registerPythonEnv(Project project, File benchVenv, String benchBeamVersion) {
    File benchStamp = new File(benchVenv, 'beam-version')
    boolean refreshBenchVenv = !benchStamp.exists() || benchStamp.getText('UTF-8').trim() != benchBeamVersion

    project.tasks.register('setupBenchmarkPythonEnv') { Task task ->
      task.group = 'Build'
      task.description = 'Create a virtualenv with a released Python SDK for the Dataflow benchmarks ' +
          '([-PbenchmarkBeamVersion=2.76.0] [-PpythonVersion=3.12])'
      task.onlyIf { refreshBenchVenv }
      task.doLast {
        String interpreter = "python${project.findProperty('pythonVersion') ?: '3.12'}"
        project.delete(benchVenv)
        project.exec({ ExecSpec spec ->
          spec.commandLine(interpreter, '-m', 'venv', benchVenv.absolutePath)
        } as Action<ExecSpec>)
        project.exec({ ExecSpec spec ->
          spec.commandLine(
              "${benchVenv.absolutePath}/bin/pip", 'install', '--quiet',
              "apache-beam[gcp]==${benchBeamVersion}")
        } as Action<ExecSpec>)
        benchStamp.setText(benchBeamVersion, 'UTF-8')
      }
    }

  }
}
