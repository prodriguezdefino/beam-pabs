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
import java.util.regex.Matcher

/**
 * The crates of the Cargo workspace, their targets, and the workspace crates that each one builds.
 *
 * <p>Gradle uses this model for task inputs and target selection, so that a change to one crate
 * runs only the tasks of the crates that depend on it.
 */
@CompileStatic
class CargoWorkspace {

  /** A dependency of a crate on a workspace crate. */
  static class Dep {
    String alias
    String targetPkg
    boolean optional
    boolean defaultFeatures
    List<String> features
  }

  /** An entry of {@code [workspace.dependencies]} that has a path. */
  private static class WorkspaceDep {
    String pkg
    boolean defaultFeatures
    List<String> features
  }

  /** One entry of a Cargo.toml: the section, the table index, and the entry on one line. */
  private static class TomlEntry {
    String section
    int blockId
    String line
  }

  /** A declared {@code [[bin]]} or {@code [[test]]} target in Cargo.toml. */
  private static class DeclaredTarget {
    String name
    String path
    boolean testEnabled = true
  }

  /** The directory of each crate, by package name. */
  final Map<String, File> crateDir = new LinkedHashMap<>()
  /** The package name of each crate directory, by canonical path. */
  final Map<String, String> dirToPkg = new LinkedHashMap<>()
  /** The workspace crates that each crate depends on, the crate included. */
  final Map<String, Set<String>> crateClosure = new LinkedHashMap<>()
  /** Whether each crate has a library target. */
  final Map<String, Boolean> crateHasLibTarget = new LinkedHashMap<>()
  /** Whether each crate has non-ignored library doctests. */
  final Map<String, Boolean> crateHasDoctests = new LinkedHashMap<>()
  /** The binary target names of each crate. */
  final Map<String, List<String>> crateBinTargets = new LinkedHashMap<>()
  /** The enabled integration test target names of each crate. */
  final Map<String, List<String>> crateTestTargets = new LinkedHashMap<>()
  /** The {@code --exclude <pkg>} flags for crates that share a test target name with each crate. */
  final Map<String, List<String>> crateTestExcludes = new LinkedHashMap<>()
  /** The {@code --exclude <pkg>} flags for all other doctest-enabled crates. */
  final Map<String, List<String>> crateDoctestExcludes = new LinkedHashMap<>()
  /** The crates, each one after the crates that it depends on. */
  final List<String> topoOrder = []

  private final Map<String, WorkspaceDep> workspaceDeps = [:]
  private final Map<String, Map<String, List<String>>> crateFeatures = [:]
  private final Map<String, List<Dep>> crateDeps = [:]
  private final Map<String, List<Dep>> crateDevDeps = [:]
  private final Map<String, List<String>> crateTestRequiredFeatures = [:]
  private final Map<String, List<String>> testTargetOwners = [:]
  private final List<String> doctestEnabledPkgs = []

  /**
   * @param memberTomls the Cargo.toml of each member crate, in a stable order
   * @param selectedPkg the crate of {@code -Ppkg}, or null
   * @param onlyFeatures the features of {@code -PonlyFeatures} for that crate, or null
   */
  CargoWorkspace(File rustDir, Collection<File> memberTomls, String selectedPkg, String onlyFeatures) {
    readWorkspaceDeps(new File(rustDir, 'Cargo.toml'))
    memberTomls.each { File toml -> readPackageName(toml) }
    crateDir.each { String pkg, File dir -> readManifest(pkg, dir) }
    computeExcludes()
    crateDir.keySet().each { String pkg ->
      String features = (selectedPkg == pkg && onlyFeatures != null) ? onlyFeatures : null
      crateClosure[pkg] = new ClosureResolver(pkg, features).resolve()
    }
    Set<String> visited = new HashSet<>()
    crateDir.keySet().sort().each { String pkg -> visitTopo(pkg, visited) }
  }

  /** The library inputs of a crate. They exclude tests, benches and examples. */
  List<File> libInputFiles(String pkg) {
    File dir = crateDir[pkg]
    return [
      new File(dir, 'Cargo.toml'),
      new File(dir, 'build.rs'),
      new File(dir, 'src'),
      new File(dir, 'proto')
    ].findAll { File f -> f.exists() }
  }

  /** The test, bench and example directories of a crate. */
  List<File> testInputDirs(String pkg) {
    File dir = crateDir[pkg]
    return ['tests', 'benches', 'examples'].collect { String sub -> new File(dir, sub) }
        .findAll { File f -> f.exists() }
  }

  /** Reads a Cargo.toml: removes comments and joins each multiline value into one line. */
  private static List<TomlEntry> parseToml(File tomlFile) {
    List<TomlEntry> entries = []
    String section = ''
    int blockId = 0
    StringBuilder pending = new StringBuilder()
    int depth = 0
    for (String raw : tomlFile.readLines('UTF-8')) {
      String clean = raw.replaceAll(/#.*$/, '').trim()
      if (clean.isEmpty()) {
        continue
      }
      if (depth == 0 && clean.startsWith('[') && clean.endsWith(']')) {
        section = clean.replaceAll(/^\[+|\]+$/, '').trim()
        blockId++
        entries.add(new TomlEntry(section: section, blockId: blockId, line: ''))
        continue
      }
      if (pending.length() > 0) {
        pending.append(' ')
      }
      pending.append(clean)
      for (int i = 0; i < clean.length(); i++) {
        int c = clean.charAt(i)
        if ('[{'.indexOf(c) >= 0) {
          depth++
        } else if (']}'.indexOf(c) >= 0) {
          depth = Math.max(0, depth - 1)
        }
      }
      if (depth == 0) {
        entries.add(new TomlEntry(section: section, blockId: blockId, line: pending.toString()))
        pending.setLength(0)
      }
    }
    return entries
  }

  private static boolean isDoctestFenceTag(String info) {
    List<String> tokens = info.isEmpty()
        ? new ArrayList<String>()
        : info.split(/[\s,]+/).findAll { String t -> !t.isEmpty() }.toList()
    if (tokens.any { String t -> t == 'ignore' || t.startsWith('ignore-') }) {
      return false
    }
    if (tokens.isEmpty()) {
      return true
    }
    String first = tokens[0]
    return first == 'rust' ||
        first.startsWith('no_run') ||
        first.startsWith('should_panic') ||
        first.startsWith('compile_fail') ||
        first.startsWith('edition')
  }

  private static boolean fileHasDoctestFence(File rsFile) {
    String text = rsFile.getText('UTF-8')
    if (!text.contains('```') && !text.contains('~~~')) {
      return false
    }
    char fenceChar = 0
    int fenceLen = 0
    for (String rawLine : text.readLines()) {
      String trimmed = rawLine.trim()
      String docBody = null
      if (trimmed.startsWith('//!')) {
        docBody = trimmed.substring(3)
      } else if (trimmed.startsWith('///') && (trimmed.length() == 3 || trimmed.charAt(3) != ('/' as char))) {
        docBody = trimmed.substring(3)
      }
      if (docBody == null) {
        fenceChar = 0
        fenceLen = 0
        continue
      }
      String line = docBody.trim()
      if (line.length() < 3) {
        continue
      }
      char ch = line.charAt(0)
      if (ch != ('`' as char) && ch != ('~' as char)) {
        continue
      }
      int run = 0
      while (run < line.length() && line.charAt(run) == ch) {
        run++
      }
      if (run < 3) {
        continue
      }
      String rest = line.substring(run).trim()
      if (fenceLen == 0) {
        if (ch == ('`' as char) && rest.contains('`')) {
          continue
        }
        if (isDoctestFenceTag(rest)) {
          return true
        }
        fenceChar = ch
        fenceLen = run
      } else if (ch == fenceChar && run >= fenceLen && rest.isEmpty()) {
        fenceChar = 0
        fenceLen = 0
      }
    }
    return false
  }

  private static boolean hasRustDoctestFence(File srcDir) {
    if (!srcDir.isDirectory()) {
      return false
    }
    List<File> stack = [srcDir]
    while (!stack.isEmpty()) {
      File current = stack.remove(stack.size() - 1)
      File[] children = current.listFiles()
      if (children == null) {
        continue
      }
      for (File child : children) {
        if (child.isDirectory()) {
          stack.add(child)
        } else if (child.name.endsWith('.rs') && fileHasDoctestFence(child)) {
          return true
        }
      }
    }
    return false
  }

  private static List<String> quotedStrings(String text) {
    List<String> list = []
    Matcher m = (text =~ /"([^"]+)"/)
    while (m.find()) {
      list.add(m.group(1))
    }
    return list
  }

  private static List<String> featureArray(String rhs) {
    Matcher m = (rhs =~ /features\s*=\s*\[([^\]]*)\]/)
    return m.find() ? quotedStrings(m.group(1)) : new ArrayList<String>()
  }

  private static String firstGroup(String text, String pattern) {
    Matcher m = (text =~ pattern)
    return m.find() ? m.group(1) : null
  }

  private void readWorkspaceDeps(File rootToml) {
    parseToml(rootToml).each { TomlEntry entry ->
      if (entry.section != 'workspace.dependencies' || !entry.line.contains('path =')) {
        return
      }
      String[] kv = entry.line.split('=', 2)
      String alias = kv[0].trim()
      String rhs = kv[1].trim()
      if (firstGroup(rhs, /path\s*=\s*"([^"]+)"/) == null) {
        return
      }
      workspaceDeps[alias] = new WorkspaceDep(
          pkg: firstGroup(rhs, /package\s*=\s*"([^"]+)"/) ?: alias,
          defaultFeatures: !rhs.contains('default-features = false'),
          features: featureArray(rhs))
    }
  }

  private void readPackageName(File toml) {
    TomlEntry nameEntry = parseToml(toml).find { TomlEntry entry ->
      entry.section == 'package' && entry.line.startsWith('name')
    }
    String pkg = nameEntry == null ? null : firstGroup(nameEntry.line, /name\s*=\s*"([^"]+)"/)
    if (pkg != null) {
      crateDir[pkg] = toml.parentFile
      dirToPkg[toml.parentFile.canonicalPath] = pkg
    }
  }

  private void readManifest(String pkg, File dir) {
    crateFeatures[pkg] = [:]
    crateDeps[pkg] = []
    crateDevDeps[pkg] = []
    crateTestRequiredFeatures[pkg] = []

    boolean autolib = true
    boolean autobins = true
    boolean autotests = true
    boolean libDeclared = false
    boolean libDoctestFalse = false
    boolean libProcMacro = false
    Map<Integer, DeclaredTarget> bins = new LinkedHashMap<>()
    Map<Integer, DeclaredTarget> tests = new LinkedHashMap<>()

    parseToml(new File(dir, 'Cargo.toml')).each { TomlEntry entry ->
      String section = entry.section
      String line = entry.line
      if (section == 'lib' && line.isEmpty()) {
        libDeclared = true
        return
      }
      if (line.isEmpty()) {
        return
      }
      if (section == 'package') {
        if (line ==~ /^autolib\s*=\s*false\b.*/) {
          autolib = false
        } else if (line ==~ /^autobins\s*=\s*false\b.*/) {
          autobins = false
        } else if (line ==~ /^autotests\s*=\s*false\b.*/) {
          autotests = false
        }
      } else if (section == 'lib') {
        if (line ==~ /^doctest\s*=\s*false\b.*/) {
          libDoctestFalse = true
        } else if (line ==~ /^proc-macro\s*=\s*true\b.*/) {
          libProcMacro = true
        }
      } else if (section == 'bin') {
        DeclaredTarget target = bins.computeIfAbsent(entry.blockId, { new DeclaredTarget() })
        if (line.startsWith('name')) {
          target.name = firstGroup(line, /name\s*=\s*"([^"]+)"/)
        } else if (line.startsWith('path')) {
          target.path = firstGroup(line, /path\s*=\s*"([^"]+)"/)
        }
      } else if (section == 'test') {
        DeclaredTarget target = tests.computeIfAbsent(entry.blockId, { new DeclaredTarget() })
        if (line.startsWith('name')) {
          target.name = firstGroup(line, /name\s*=\s*"([^"]+)"/)
        } else if (line.startsWith('path')) {
          target.path = firstGroup(line, /path\s*=\s*"([^"]+)"/)
        } else if (line ==~ /^test\s*=\s*false\b.*/) {
          target.testEnabled = false
        } else if (line.startsWith('required-features')) {
          crateTestRequiredFeatures[pkg].addAll(quotedStrings(line))
        }
      } else if (section == 'bench' && line.startsWith('required-features')) {
        crateTestRequiredFeatures[pkg].addAll(quotedStrings(line))
      } else if (section == 'features' && line.contains('=')) {
        String[] kv = line.split('=', 2)
        crateFeatures[pkg][kv[0].trim()] = quotedStrings(kv[1])
      } else if (section == 'dependencies' || section == 'build-dependencies') {
        Dep dep = parseDep(dir, line)
        if (dep != null) {
          crateDeps[pkg].add(dep)
        }
      } else if (section == 'dev-dependencies') {
        Dep dep = parseDep(dir, line)
        if (dep != null) {
          crateDevDeps[pkg].add(dep)
        }
      }
    }

    boolean hasLib = libDeclared || (autolib && new File(dir, 'src/lib.rs').isFile())
    crateHasLibTarget[pkg] = hasLib
    boolean doctestEnabled = hasLib && !libDoctestFalse && !libProcMacro
    if (doctestEnabled) {
      doctestEnabledPkgs.add(pkg)
    }
    crateHasDoctests[pkg] = doctestEnabled && hasRustDoctestFence(new File(dir, 'src'))
    crateBinTargets[pkg] = resolveBinTargets(pkg, dir, bins.values(), autobins)
    crateTestTargets[pkg] = resolveTestTargets(pkg, dir, tests.values(), autotests)
  }

  private static List<String> resolveBinTargets(
      String pkg, File dir, Collection<DeclaredTarget> declared, boolean autobins) {
    List<String> names = []
    Set<String> declaredPaths = new HashSet<>()
    declared.each { DeclaredTarget t ->
      if (t.name != null) {
        names.add(t.name)
        String rel = t.path ?: "src/bin/${t.name}.rs".toString()
        declaredPaths.add(new File(dir, rel).canonicalPath)
      }
    }
    if (autobins) {
      File mainRs = new File(dir, 'src/main.rs')
      if (mainRs.isFile() && !declaredPaths.contains(mainRs.canonicalPath) && !(pkg in names)) {
        names.add(pkg)
      }
      File binDir = new File(dir, 'src/bin')
      File[] children = binDir.isDirectory() ? binDir.listFiles() : null
      if (children != null) {
        children.sort { File f -> f.name }.each { File child ->
          String candName = null
          File candFile = null
          if (child.isFile() && child.name.endsWith('.rs')) {
            candName = child.name.substring(0, child.name.length() - 3)
            candFile = child
          } else if (child.isDirectory() && new File(child, 'main.rs').isFile()) {
            candName = child.name
            candFile = new File(child, 'main.rs')
          }
          if (candName != null && candFile != null &&
              !declaredPaths.contains(candFile.canonicalPath) && !(candName in names)) {
            names.add(candName)
          }
        }
      }
    }
    return names.unique().sort()
  }

  private List<String> resolveTestTargets(
      String pkg, File dir, Collection<DeclaredTarget> declared, boolean autotests) {
    List<String> enabled = []
    Set<String> declaredPaths = new HashSet<>()
    Set<String> declaredNames = new HashSet<>()
    declared.each { DeclaredTarget t ->
      if (t.name == null) {
        return
      }
      declaredNames.add(t.name)
      testTargetOwners.computeIfAbsent(t.name, { new ArrayList<String>() }).add(pkg)
      if (t.testEnabled) {
        enabled.add(t.name)
      }
      if (t.path != null) {
        declaredPaths.add(new File(dir, t.path).canonicalPath)
      } else {
        [new File(dir, "tests/${t.name}.rs"), new File(dir, "tests/${t.name}/main.rs")].each { File cand ->
          if (cand.isFile()) {
            declaredPaths.add(cand.canonicalPath)
          }
        }
      }
    }
    File testsDir = new File(dir, 'tests')
    File[] children = (autotests && testsDir.isDirectory()) ? testsDir.listFiles() : null
    if (children != null) {
      children.sort { File f -> f.name }.each { File child ->
        String candName = null
        File candFile = null
        if (child.isFile() && child.name.endsWith('.rs')) {
          candName = child.name.substring(0, child.name.length() - 3)
          candFile = child
        } else if (child.isDirectory() && new File(child, 'main.rs').isFile()) {
          candName = child.name
          candFile = new File(child, 'main.rs')
        }
        if (candName != null && candFile != null &&
            !declaredPaths.contains(candFile.canonicalPath) && !declaredNames.contains(candName)) {
          testTargetOwners.computeIfAbsent(candName, { new ArrayList<String>() }).add(pkg)
          enabled.add(candName)
        }
      }
    }
    return enabled.unique().sort()
  }

  private void computeExcludes() {
    List<String> sortedDoctestPkgs = doctestEnabledPkgs.unique().sort()
    crateDir.keySet().each { String name ->
      List<String> colliding = crateTestTargets[name]
          .collectMany { String tname -> testTargetOwners[tname] ?: new ArrayList<String>() }
          .findAll { String owner -> owner != name }
          .unique()
          .sort()
      crateTestExcludes[name] = colliding.collectMany { String pkg -> ['--exclude', pkg] }
      crateDoctestExcludes[name] = sortedDoctestPkgs
          .findAll { String pkg -> pkg != name }
          .collectMany { String pkg -> ['--exclude', pkg] }
    }
  }

  /** Returns the dependency of this line, or null if the target is not a workspace crate. */
  private Dep parseDep(File memberDir, String line) {
    if (!line.contains('=')) {
      return null
    }
    String[] kv = line.split('=', 2)
    String alias = kv[0].trim()
    String rhs = kv[1].trim()
    boolean fromWorkspace = rhs.contains('workspace = true')
    String targetPkg = null
    String packageName = firstGroup(rhs, /package\s*=\s*"([^"]+)"/)
    String path = firstGroup(rhs, /path\s*=\s*"([^"]+)"/)
    if (packageName != null && crateDir.containsKey(packageName)) {
      targetPkg = packageName
    } else if (path != null) {
      targetPkg = dirToPkg[new File(memberDir, path).canonicalPath]
    } else if (fromWorkspace && workspaceDeps.containsKey(alias)) {
      targetPkg = workspaceDeps[alias].pkg
    }
    if (targetPkg == null) {
      return null
    }
    WorkspaceDep shared = fromWorkspace ? workspaceDeps[alias] : null
    return new Dep(
        alias: alias,
        targetPkg: targetPkg,
        optional: rhs.contains('optional = true'),
        defaultFeatures: !rhs.contains('default-features = false') && (shared == null || shared.defaultFeatures),
        features: (featureArray(rhs) + (shared == null ? new ArrayList<String>() : shared.features)).unique())
  }

  private void visitTopo(String pkg, Set<String> visited) {
    if (!visited.add(pkg)) {
      return
    }
    crateClosure[pkg].toList().sort().each { String dep ->
      if (dep != pkg && crateDir.containsKey(dep)) {
        visitTopo(dep, visited)
      }
    }
    topoOrder.add(pkg)
  }

  /** Finds the workspace crates that one crate depends on, with the cargo feature rules. */
  private class ClosureResolver {
    private final String rootPkg
    private final String onlyFeatures
    private final Set<String> activeCrates = new LinkedHashSet<>()
    private final Set<String> enabledFeatures = new LinkedHashSet<>()
    private final Set<String> activatedEdges = new LinkedHashSet<>()

    ClosureResolver(String rootPkg, String onlyFeatures) {
      this.rootPkg = rootPkg
      this.onlyFeatures = onlyFeatures
    }

    Set<String> resolve() {
      activateCrate(rootPkg)
      return activeCrates
    }

    private void activateCrate(String pkg) {
      if (!activeCrates.add(pkg)) {
        return
      }
      depsOf(pkg).findAll { Dep d -> !d.optional }.each { Dep d -> activateEdge(pkg, d) }
      if (pkg != rootPkg) {
        return
      }
      (crateDevDeps[pkg] ?: new ArrayList<Dep>()).findAll { Dep d -> !d.optional }
          .each { Dep d -> activateEdge(pkg, d) }
      if (onlyFeatures != null) {
        onlyFeatures.split(',').collect { String f -> f.trim() }.findAll { String f -> !f.isEmpty() }
            .each { String f -> enableFeature(pkg, f) }
      } else {
        enableFeature(pkg, 'default')
        (crateTestRequiredFeatures[pkg] ?: new ArrayList<String>()).each { String f -> enableFeature(pkg, f) }
      }
    }

    private void activateEdge(String fromPkg, Dep d) {
      String edgeKey = "${fromPkg}->${d.alias}:${d.targetPkg}:${d.defaultFeatures}:${d.features.join(',')}"
      if (!activatedEdges.add(edgeKey)) {
        return
      }
      activateCrate(d.targetPkg)
      if (d.defaultFeatures) {
        enableFeature(d.targetPkg, 'default')
      }
      d.features.each { String f -> enableFeature(d.targetPkg, f) }
    }

    private void enableFeature(String pkg, String feature) {
      activateCrate(pkg)
      if (!enabledFeatures.add("${pkg}:${feature}".toString())) {
        return
      }
      depsOf(pkg).findAll { Dep d -> d.optional && d.alias == feature }.each { Dep d -> activateEdge(pkg, d) }
      List<String> tokens = (crateFeatures[pkg] ?: new HashMap<String, List<String>>())[feature]
      (tokens ?: new ArrayList<String>()).each { String token ->
        if (token.startsWith('dep:')) {
          String depAlias = token.substring(4)
          depsOf(pkg).findAll { Dep d -> d.alias == depAlias }.each { Dep d -> activateEdge(pkg, d) }
        } else if (token.contains('/')) {
          String[] parts = token.split('/', 2)
          String depAlias = parts[0].replace('?', '')
          depsOf(pkg).findAll { Dep d -> d.alias == depAlias }.each { Dep d ->
            if (!parts[0].endsWith('?')) {
              activateEdge(pkg, d)
            }
            enableFeature(d.targetPkg, parts[1])
          }
        } else {
          enableFeature(pkg, token)
        }
      }
    }

    private List<Dep> depsOf(String pkg) {
      return crateDeps[pkg] ?: new ArrayList<Dep>()
    }
  }
}
