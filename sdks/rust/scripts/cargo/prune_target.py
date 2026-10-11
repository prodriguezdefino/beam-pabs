#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Prunes superseded binaries, workspace rlibs, and incremental caches in Cargo target directories."""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import shutil
import time

HEX_SUFFIX_RE = re.compile(r"-([0-9a-f]{16})(?:\.|$)")
FINGERPRINT_DIR_RE = re.compile(r"^(.+)-([0-9a-f]{16})$")
CRATE_NAME_RE = re.compile(r'(?m)^name\s*=\s*"([^"]+)"')
LIB_EXTENSIONS = (".rlib", ".dylib", ".so", ".a")
EXE_UNIT_PREFIXES = ("test-", "bin-", "bench-", "example-")


@dataclass(frozen=True)
class ProfileStats:
    profile: str
    removed_paths: int
    reclaimed_bytes: int


def discover_workspace_packages(workspace_root: Path) -> set[str]:
    """Reads workspace crate names from Cargo.toml files under workspace_root."""
    packages: set[str] = set()
    for root, dirs, files in os.walk(workspace_root):
        dirs[:] = [d for d in dirs if d not in ("target", "build", ".git", "container")]
        if "Cargo.toml" not in files:
            continue
        text = (Path(root) / "Cargo.toml").read_text(encoding="utf-8")
        match = CRATE_NAME_RE.search(text)
        if match:
            packages.add(match.group(1))
    return packages


def read_manifest_hashes(manifest_path: Path | None) -> set[str]:
    """Returns 16-hex artifact hashes listed in the workspace test manifest."""
    if manifest_path is None or not manifest_path.is_file():
        return set()
    pinned: set[str] = set()
    for line in manifest_path.read_text(encoding="utf-8").splitlines():
        if not line:
            continue
        parts = line.split("\t", 2)
        if len(parts) == 3:
            match = HEX_SUFFIX_RE.search(Path(parts[2]).name)
            if match:
                pinned.add(match.group(1))
    return pinned


def discover_profile_dirs(target_dir: Path) -> list[Path]:
    """Finds Cargo profile directories under target_dir, including target-triple subdirectories."""
    if not target_dir.is_dir():
        return []
    found: list[Path] = []
    for child in sorted(target_dir.iterdir()):
        if not child.is_dir() or child.name in ("criterion", "tmp", ".fingerprint", "deps", "build"):
            continue
        if (child / ".fingerprint").is_dir() or (child / "incremental").is_dir():
            found.append(child)
            continue
        for sub in sorted(child.iterdir()):
            if sub.is_dir() and ((sub / ".fingerprint").is_dir() or (sub / "incremental").is_dir()):
                found.append(sub)
    return found


def path_size_bytes(path: Path) -> int:
    """Returns the byte size of a file or directory tree without following symlinks."""
    if path.is_symlink():
        return 0
    if path.is_file():
        return path.stat().st_size
    if not path.is_dir():
        return 0
    total = 0
    for root, _, files in os.walk(path, followlinks=False):
        for name in files:
            fp = Path(root) / name
            if not fp.is_symlink():
                total += fp.stat().st_size
    return total


def remove_path(path: Path, dry_run: bool) -> int:
    """Removes a file or directory and returns the reclaimed byte count."""
    size = path_size_bytes(path)
    if dry_run:
        return size
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path, ignore_errors=True)
    else:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
    return size


def pinned_top_level_hashes(profile_dir: Path, deps_by_hash: dict[str, list[Path]]) -> set[str]:
    """Pins hashes in deps/ that share an inode with a top-level binary in profile_dir."""
    top_inodes: set[int] = set()
    for entry in profile_dir.iterdir():
        if entry.is_file() and not entry.is_symlink() and entry.suffix == "":
            top_inodes.add(entry.stat().st_ino)
    if not top_inodes:
        return set()
    pinned: set[str] = set()
    for hx, paths in deps_by_hash.items():
        for p in paths:
            if p.is_file() and not p.is_symlink() and p.stat().st_ino in top_inodes:
                pinned.add(hx)
                break
    return pinned


def classify_unit(unit: str, files: list[Path]) -> str:
    """Classifies a Cargo fingerprint unit into build-script, exe, rlib, rmeta, or other."""
    if "build-script" in unit:
        return "build-script"
    names = [p.name for p in files]
    has_lib = any(n.endswith(LIB_EXTENSIONS) for n in names)
    has_rmeta = any(n.endswith(".rmeta") for n in names)
    if unit.startswith(EXE_UNIT_PREFIXES):
        return "exe"
    has_bare_exe = any(
        ("." not in n or n.endswith(".exe") or n.endswith(".dSYM")) for n in names
    )
    if not has_lib and not has_rmeta and has_bare_exe:
        return "exe"
    if has_lib:
        return "rlib"
    if has_rmeta:
        return "rmeta"
    return "other"


def unit_mtime(fp_entry: Path) -> float:
    """Reads the Cargo invocation mtime for a fingerprint directory."""
    stamp = fp_entry / "invoked.timestamp"
    if stamp.is_file():
        return stamp.stat().st_mtime
    return fp_entry.stat().st_mtime


def transitive_dep_hashes(
    seed_hashes: set[str],
    json_files_by_hash: dict[str, list[Path]],
    hashes_by_fp_hex: dict[str, set[str]],
) -> set[str]:
    """Finds all unit hashes transitively referenced in Cargo fingerprint JSON files."""
    visited = set(seed_hashes)
    stack = list(seed_hashes)
    while stack:
        hx = stack.pop()
        for json_path in json_files_by_hash.get(hx, ()):
            try:
                payload = json.loads(json_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            for dep in payload.get("deps", ()):
                if isinstance(dep, list) and len(dep) >= 4 and isinstance(dep[3], int) and dep[3] >= 0:
                    dep_fp_hex = (dep[3] & 0xFFFFFFFFFFFFFFFF).to_bytes(8, "little").hex()
                    for dep_hx in hashes_by_fp_hex.get(dep_fp_hex, ()):
                        if dep_hx not in visited:
                            visited.add(dep_hx)
                            stack.append(dep_hx)
    return visited


def prune_deps_and_fingerprints(
    profile_dir: Path,
    workspace_pkgs: set[str],
    manifest_hashes: set[str],
    keep_exe: int,
    keep_lib: int,
    dry_run: bool,
) -> tuple[int, int]:
    """Prunes superseded executables and workspace library copies in one profile directory."""
    fp_dir = profile_dir / ".fingerprint"
    deps_dir = profile_dir / "deps"
    if not fp_dir.is_dir() or not deps_dir.is_dir():
        return (0, 0)

    deps_by_hash: dict[str, list[Path]] = defaultdict(list)
    for entry in deps_dir.iterdir():
        match = HEX_SUFFIX_RE.search(entry.name)
        if match:
            deps_by_hash[match.group(1)].append(entry)

    pinned = set(manifest_hashes) | pinned_top_level_hashes(profile_dir, deps_by_hash)
    known_hashes: set[str] = set()
    json_files_by_hash: dict[str, list[Path]] = defaultdict(list)
    hashes_by_fp_hex: dict[str, set[str]] = defaultdict(set)
    groups: dict[tuple[str, str, str], list[tuple[float, str, Path]]] = defaultdict(list)

    for fp_entry in fp_dir.iterdir():
        if not fp_entry.is_dir():
            continue
        match = FINGERPRINT_DIR_RE.match(fp_entry.name)
        if not match:
            continue
        pkg, hx = match.group(1), match.group(2)
        known_hashes.add(hx)
        jsons = sorted(
            f[:-5] for f in os.listdir(fp_entry) if f.endswith(".json") and not f.startswith("dep-")
        )
        for unit_name in jsons:
            json_files_by_hash[hx].append(fp_entry / f"{unit_name}.json")
            fp_hex_file = fp_entry / unit_name
            if fp_hex_file.is_file():
                try:
                    fp_hex = fp_hex_file.read_text(encoding="utf-8").strip()
                except OSError:
                    fp_hex = ""
                if len(fp_hex) == 16:
                    hashes_by_fp_hex[fp_hex].add(hx)
        unit = jsons[0] if jsons else "unknown"
        kind = classify_unit(unit, deps_by_hash.get(hx, []))
        if kind == "build-script":
            continue
        if pkg not in workspace_pkgs and kind != "exe":
            continue
        groups[(pkg, unit, kind)].append((unit_mtime(fp_entry), hx, fp_entry))

    removed = 0
    reclaimed = 0
    hashes_to_keep: set[str] = set(pinned)
    candidates_to_prune: list[tuple[str, Path]] = []

    for (_pkg, _unit, kind), items in groups.items():
        limit = keep_exe if kind == "exe" else keep_lib
        items.sort(key=lambda item: (item[0], item[1]), reverse=True)
        for idx, (_mtime, hx, fp_entry) in enumerate(items):
            if idx < limit or hx in pinned:
                hashes_to_keep.add(hx)
            else:
                candidates_to_prune.append((hx, fp_entry))

    hashes_to_keep = transitive_dep_hashes(hashes_to_keep, json_files_by_hash, hashes_by_fp_hex)

    pruned_hashes: set[str] = set()
    for hx, fp_entry in candidates_to_prune:
        if hx in hashes_to_keep:
            continue
        reclaimed += remove_path(fp_entry, dry_run)
        removed += 1
        if hx not in pruned_hashes:
            pruned_hashes.add(hx)
            for dep_path in deps_by_hash.get(hx, []):
                reclaimed += remove_path(dep_path, dry_run)
                removed += 1

    for hx, paths in deps_by_hash.items():
        if hx in known_hashes or hx in pinned:
            continue
        for dep_path in paths:
            name = dep_path.name
            is_orphan_exe = "." not in name or name.endswith(".exe") or name.endswith(".dSYM")
            if is_orphan_exe:
                reclaimed += remove_path(dep_path, dry_run)
                removed += 1

    return (removed, reclaimed)


def latest_incremental_mtime(inc_entry: Path) -> float:
    """Returns the newest modification time of an incremental directory and its sessions."""
    newest = inc_entry.stat().st_mtime
    for child in inc_entry.iterdir():
        mtime = child.stat().st_mtime
        if mtime > newest:
            newest = mtime
    return newest


def prune_incremental(
    profile_dir: Path,
    keep_lib: int,
    incremental_days: float,
    dry_run: bool,
    now: float | None = None,
) -> tuple[int, int]:
    """Prunes superseded and stale directories under <profile>/incremental."""
    inc_dir = profile_dir / "incremental"
    if not inc_dir.is_dir():
        return (0, 0)

    cutoff = (time.time() if now is None else now) - (incremental_days * 86400.0)
    by_prefix: dict[str, list[tuple[float, Path]]] = defaultdict(list)
    for entry in inc_dir.iterdir():
        if not entry.is_dir():
            continue
        prefix = entry.name.rsplit("-", 1)[0] if "-" in entry.name else entry.name
        by_prefix[prefix].append((latest_incremental_mtime(entry), entry))

    removed = 0
    reclaimed = 0
    for items in by_prefix.values():
        items.sort(key=lambda item: (item[0], item[1].name), reverse=True)
        for idx, (mtime, entry) in enumerate(items):
            keep_active = idx == 0 and keep_lib >= 1
            keep_recent = idx < keep_lib and mtime >= cutoff
            if keep_active or keep_recent:
                continue
            reclaimed += remove_path(entry, dry_run)
            removed += 1
    return (removed, reclaimed)


def prune_target(
    target_dirs: list[Path],
    workspace_pkgs: set[str],
    manifest_path: Path | None = None,
    keep_exe: int = 1,
    keep_lib: int = 2,
    incremental_days: float = 2.0,
    dry_run: bool = False,
    now: float | None = None,
) -> list[ProfileStats]:
    """Prunes all profile directories under target_dirs and returns per-profile totals."""
    manifest_hashes = read_manifest_hashes(manifest_path)
    results: list[ProfileStats] = []
    for target_dir in target_dirs:
        for profile_dir in discover_profile_dirs(target_dir):
            rem_deps, bytes_deps = prune_deps_and_fingerprints(
                profile_dir=profile_dir,
                workspace_pkgs=workspace_pkgs,
                manifest_hashes=manifest_hashes,
                keep_exe=keep_exe,
                keep_lib=keep_lib,
                dry_run=dry_run,
            )
            rem_inc, bytes_inc = prune_incremental(
                profile_dir=profile_dir,
                keep_lib=keep_lib,
                incremental_days=incremental_days,
                dry_run=dry_run,
                now=now,
            )
            label = str(profile_dir.relative_to(target_dir.parent))
            results.append(
                ProfileStats(
                    profile=label,
                    removed_paths=rem_deps + rem_inc,
                    reclaimed_bytes=bytes_deps + bytes_inc,
                )
            )
    return results


def format_bytes(num_bytes: int) -> str:
    """Formats a byte count in human-readable binary units."""
    if num_bytes >= 1024**3:
        return f"{num_bytes / (1024**3):.2f} GiB"
    if num_bytes >= 1024**2:
        return f"{num_bytes / (1024**2):.1f} MiB"
    if num_bytes >= 1024:
        return f"{num_bytes / 1024:.1f} KiB"
    return f"{num_bytes} B"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--workspace-root",
        type=Path,
        required=True,
        help="Path to sdks/rust",
    )
    parser.add_argument(
        "--target-dir",
        type=Path,
        action="append",
        dest="target_dirs",
        help="Cargo target directory to prune (repeatable; defaults to <workspace-root>/target)",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Optional workspace-test-manifest.tsv whose binaries stay pinned",
    )
    parser.add_argument("--keep-exe", type=int, default=1)
    parser.add_argument("--keep-lib", type=int, default=2)
    parser.add_argument("--incremental-days", type=float, default=2.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--quiet-if-empty", action="store_true")
    args = parser.parse_args(argv)

    workspace_root = args.workspace_root.resolve()
    target_dirs = [d.resolve() for d in (args.target_dirs or [workspace_root / "target"])]
    workspace_pkgs = discover_workspace_packages(workspace_root)
    stats = prune_target(
        target_dirs=target_dirs,
        workspace_pkgs=workspace_pkgs,
        manifest_path=args.manifest,
        keep_exe=max(0, args.keep_exe),
        keep_lib=max(0, args.keep_lib),
        incremental_days=max(0.0, args.incremental_days),
        dry_run=args.dry_run,
    )

    total_removed = sum(s.removed_paths for s in stats)
    total_bytes = sum(s.reclaimed_bytes for s in stats)
    if args.quiet_if_empty and total_removed == 0:
        return 0

    mode = "Dry run" if args.dry_run else "Pruned"
    for item in stats:
        if item.removed_paths > 0 or not args.quiet_if_empty:
            print(
                f"{mode} {item.profile}: {item.removed_paths} paths, {format_bytes(item.reclaimed_bytes)}"
            )
    print(f"{mode} total: {total_removed} paths, {format_bytes(total_bytes)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
