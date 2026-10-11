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

import os
from pathlib import Path
import tempfile
import unittest

import prune_target as pt


def write_unit(
    profile_dir: Path,
    pkg: str,
    hx: str,
    unit: str,
    mtime: float,
    dep_files: dict[str, int],
    dsym: bool = False,
) -> None:
    fp = profile_dir / ".fingerprint" / f"{pkg}-{hx}"
    fp.mkdir(parents=True, exist_ok=True)
    (fp / f"{unit}.json").write_text("{}", encoding="utf-8")
    stamp = fp / "invoked.timestamp"
    stamp.write_text("", encoding="utf-8")
    os.utime(stamp, (mtime, mtime))

    deps = profile_dir / "deps"
    deps.mkdir(parents=True, exist_ok=True)
    for name, size in dep_files.items():
        p = deps / name
        p.write_bytes(b"x" * size)
        os.utime(p, (mtime, mtime))
    if dsym:
        dsym_dir = deps / f"coders-{hx}.dSYM" / "Contents"
        dsym_dir.mkdir(parents=True, exist_ok=True)
        (dsym_dir / "DWARF").write_bytes(b"d" * 64)


class TestPruneTarget(unittest.TestCase):
    def test_prunes_old_executables_and_pins_manifest_and_hardlinks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ws = Path(tmp)
            (ws / "Cargo.toml").write_text('[package]\nname = "apache-beam-core"\n', encoding="utf-8")
            dbg = ws / "target" / "debug"

            h_old = "1111111111111111"
            h_pinned = "2222222222222222"
            h_new = "3333333333333333"
            for idx, hx in enumerate([h_old, h_pinned, h_new], start=1):
                write_unit(
                    dbg,
                    "apache-beam-core",
                    hx,
                    "test-integration-test-coders",
                    1000.0 + idx * 10,
                    {
                        f"coders-{hx}": 256,
                        f"coders-{hx}.d": 16,
                        f"coders-{hx}.beam.cgu.00.rcgu.o": 128,
                    },
                    dsym=True,
                )

            h_link = "4444444444444444"
            h_newer_bin = "5555555555555555"
            write_unit(
                dbg,
                "apache-beam-core",
                h_link,
                "bin-wordcount",
                1000.0,
                {f"wordcount-{h_link}": 100},
            )
            os.link(dbg / "deps" / f"wordcount-{h_link}", dbg / "wordcount")
            write_unit(
                dbg,
                "apache-beam-core",
                h_newer_bin,
                "bin-wordcount",
                2000.0,
                {f"wordcount-{h_newer_bin}": 100},
            )

            manifest = ws / "manifest.tsv"
            manifest.write_text(
                f"apache-beam-core\tcoders\t{dbg / 'deps' / f'coders-{h_pinned}'}\n",
                encoding="utf-8",
            )

            dry = pt.prune_target(
                target_dirs=[ws / "target"],
                workspace_pkgs={"apache-beam-core"},
                manifest_path=manifest,
                keep_exe=1,
                keep_lib=1,
                dry_run=True,
            )
            self.assertGreater(dry[0].reclaimed_bytes, 0)
            self.assertTrue((dbg / "deps" / f"coders-{h_old}").exists())

            pt.prune_target(
                target_dirs=[ws / "target"],
                workspace_pkgs={"apache-beam-core"},
                manifest_path=manifest,
                keep_exe=1,
                keep_lib=1,
                dry_run=False,
            )

            self.assertFalse((dbg / "deps" / f"coders-{h_old}").exists())
            self.assertFalse((dbg / "deps" / f"coders-{h_old}.dSYM").exists())
            self.assertFalse((dbg / ".fingerprint" / f"apache-beam-core-{h_old}").exists())
            self.assertTrue((dbg / "deps" / f"coders-{h_pinned}").exists())
            self.assertTrue((dbg / "deps" / f"coders-{h_new}").exists())
            self.assertTrue((dbg / "deps" / f"wordcount-{h_link}").exists())
            self.assertTrue((dbg / "deps" / f"wordcount-{h_newer_bin}").exists())

    def test_preserves_third_party_libs_and_separates_workspace_rlib_and_rmeta(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ws = Path(tmp)
            dbg = ws / "target" / "debug"

            for idx, hx in enumerate(["aaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb", "cccccccccccccccc"]):
                write_unit(
                    dbg,
                    "syn",
                    hx,
                    "lib-syn",
                    100.0 + idx,
                    {f"libsyn-{hx}.rlib": 64, f"libsyn-{hx}.rmeta": 32},
                )

            write_unit(
                dbg,
                "apache-beam-core",
                "0000000000000001",
                "lib-beam",
                100.0,
                {"libbeam-0000000000000001.rlib": 512, "libbeam-0000000000000001.rmeta": 64},
            )
            write_unit(
                dbg,
                "apache-beam-core",
                "0000000000000002",
                "lib-beam",
                200.0,
                {"libbeam-0000000000000002.rlib": 512, "libbeam-0000000000000002.rmeta": 64},
            )
            write_unit(
                dbg,
                "apache-beam-core",
                "0000000000000003",
                "lib-beam",
                300.0,
                {"libbeam-0000000000000003.rmeta": 64},
            )
            write_unit(
                dbg,
                "apache-beam-core",
                "0000000000000004",
                "build-script-build-script-build",
                50.0,
                {"build_script_build-0000000000000004": 128},
            )

            pt.prune_target(
                target_dirs=[ws / "target"],
                workspace_pkgs={"apache-beam-core"},
                keep_exe=1,
                keep_lib=1,
            )

            for hx in ["aaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb", "cccccccccccccccc"]:
                self.assertTrue((dbg / "deps" / f"libsyn-{hx}.rlib").exists())

            self.assertFalse((dbg / "deps" / "libbeam-0000000000000001.rlib").exists())
            self.assertTrue((dbg / "deps" / "libbeam-0000000000000002.rlib").exists())
            self.assertTrue((dbg / "deps" / "libbeam-0000000000000003.rmeta").exists())
            self.assertTrue((dbg / "deps" / "build_script_build-0000000000000004").exists())

    def test_prunes_stale_incremental_and_discovers_triple_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ws = Path(tmp)
            prof = ws / "target" / "x86_64-unknown-linux-gnu" / "worker"
            inc = prof / "incremental"
            now = 1_000_000.0

            active = inc / "beam-3new"
            recent = inc / "beam-2recent"
            stale = inc / "beam-1stale"
            for path, age_hours in [(active, 100.0), (recent, 12.0), (stale, 72.0)]:
                sess = path / "s-123"
                sess.mkdir(parents=True)
                (sess / "dep-graph.bin").write_bytes(b"i" * 128)
                ts = now - age_hours * 3600.0
                os.utime(sess, (ts, ts))
                os.utime(path, (ts, ts))

            # Give `active` the newest timestamp so it stays even when keep_lib=2.
            os.utime(active / "s-123", (now - 3600.0, now - 3600.0))

            stats = pt.prune_target(
                target_dirs=[ws / "target"],
                workspace_pkgs=set(),
                keep_lib=2,
                incremental_days=2.0,
                now=now,
            )
            self.assertEqual(len(stats), 1)
            self.assertTrue(active.exists())
            self.assertTrue(recent.exists())
            self.assertFalse(stale.exists())


if __name__ == "__main__":
    unittest.main()
