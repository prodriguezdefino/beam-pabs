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

import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import org.gradle.api.Project

/** Typed wrappers over the dynamic {@code ext} helpers that {@code BeamModulePlugin} defines. */
@CompileStatic
class BeamModuleExtras {

  static void applyDockerNature(Project project) {
    callClosure(project, 'applyDockerNature')
  }

  static String nativeArchitecture(Project project) {
    return callClosure(project, 'nativeArchitecture').toString()
  }

  static List<String> containerArchitectures(Project project) {
    return toStringList(callClosure(project, 'containerArchitectures'))
  }

  static List<String> containerPlatforms(Project project) {
    return toStringList(callClosure(project, 'containerPlatforms'))
  }

  static boolean useBuildx(Project project) {
    return (boolean) callClosure(project, 'useBuildx')
  }

  static String containerImageName(Project project, String name, String root, String tag) {
    return callClosure(project, 'containerImageName', [name: name, root: root, tag: tag]).toString()
  }

  static List<String> containerImageTags(Project project) {
    return toStringList(callClosure(project, 'containerImageTags'))
  }

  private static List<String> toStringList(Object raw) {
    if (raw instanceof Object[]) {
      return ((Object[]) raw).collect { Object it -> it.toString() }
    }
    return ((Iterable<?>) raw).collect { Object it -> it.toString() }
  }

  static String dockerDefaultRepoRoot(Project project) {
    return project.property('docker_image_default_repo_root').toString()
  }

  static String dockerDefaultRepoPrefix(Project project) {
    return project.property('docker_image_default_repo_prefix').toString()
  }

  @CompileDynamic
  private static Object callClosure(Project project, String name, Object arg = null) {
    Closure fn = (Closure) project.extensions.extraProperties.get(name)
    return arg == null ? fn.call() : fn.call(arg)
  }
}
