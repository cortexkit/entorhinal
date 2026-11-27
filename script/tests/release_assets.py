"""Exercise the release asset format and verification without building Rust."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from zipfile import ZipFile

from script.release import assets


REVISION = "a" * 40
VERSION = "0.1.0"
BINARY = "ck-entorhinal"
TARGETS = ("darwin-arm64", "linux-x64", "linux-arm64", "windows-x64", "windows-arm64")


class ReleaseAssets(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        self.source = self.root / "source"
        self.source.mkdir()
        self.built = self.root / "built"

    def pack(self, target):
        assets.pack(argparse.Namespace(source=self.source, directory=self.built, target=target))

    def args(self, target):
        return argparse.Namespace(directory=self.built, target=target, revision=REVISION, version=VERSION)

    def build_fixture(self, *, version=VERSION, manifest_fields=None):
        executable = self.source / BINARY
        manifest = {
            "module_id": "entorhinal", "module_version": VERSION, "registration_channel": True,
            "provenance": {"build_git_sha": REVISION},
        }
        if manifest_fields:
            manifest.update(manifest_fields)
        executable.write_text(f'''#!/usr/bin/env python3
import pathlib, sys
assert pathlib.Path(sys.argv[0]).name == "ck-entorhinal"
if sys.argv[1] == "--version":
    print("ck-entorhinal {version}")
elif sys.argv[1] == "--manifest":
    print({json.dumps(manifest)!r})
else:
    sys.exit(2)
''', encoding="utf-8")
        executable.chmod(0o755)
        self.pack("linux-x64")

    def test_checksum_follows_verifying_the_archived_binary(self):
        self.build_fixture()
        # Verification has to use the packaged bytes, not the build directory.
        (self.source / BINARY).unlink()
        assets.verify_target(self.args("linux-x64"))
        archive = self.built / "ck-entorhinal-linux-x64.zip"
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        self.assertEqual(archive.with_suffix(".zip.sha256").read_bytes(), f"{digest}  {archive.name}\n".encode())

    def test_bad_binary_metadata_leaves_no_checksum(self):
        cases = (
            ("9.8.7", {}, "unexpected --version"),
            (VERSION, {"module_id": "other"}, "must declare entorhinal"),
            (VERSION, {"module_version": "9.8.7"}, "must declare entorhinal"),
            (VERSION, {"registration_channel": 1}, "registration_channel true"),
            (VERSION, {"provenance": {"build_git_sha": "b" * 40}}, "--manifest provenance reports"),
            (VERSION, {"provenance": {}}, "--manifest provenance reports None"),
        )
        for index, (version, fields, message) in enumerate(cases):
            with self.subTest(version=version, fields=fields):
                self.built = self.root / f"built-{index}"
                self.build_fixture(version=version, manifest_fields=fields)
                with self.assertRaisesRegex(ValueError, message):
                    assets.verify_target(self.args("linux-x64"))
                self.assertEqual(list(self.built.glob("*.sha256")), [])

    def test_verify_refuses_a_zip_with_a_placement_link(self):
        self.build_fixture()
        archive = self.built / "ck-entorhinal-linux-x64.zip"
        with ZipFile(archive, "a") as zipped:
            zipped.writestr("ck-projects", BINARY)
        with self.assertRaisesRegex(ValueError, "expected only a nonempty ck-entorhinal"):
            assets.verify_target(self.args("linux-x64"))
        self.assertEqual(list(self.built.glob("*.sha256")), [])

    def make_inventory(self):
        for target in TARGETS:
            name = BINARY + (".exe" if target.startswith("windows-") else "")
            if target == "linux-x64":
                self.build_fixture()
            else:
                path = self.source / name
                path.write_text(f"fixture for {target}\n", encoding="utf-8")
                path.chmod(0o755)
                self.pack(target)
            archive = self.built / f"{BINARY}-{target}.zip"
            with ZipFile(archive) as zipped:
                self.assertEqual(zipped.namelist(), [name])
            digest = hashlib.sha256(archive.read_bytes()).hexdigest()
            archive.with_suffix(".zip.sha256").write_bytes(f"{digest}  {archive.name}\n".encode())

    def assemble(self, output, *, dry_run=False):
        assets.assemble(argparse.Namespace(directory=self.built, output=output, version=VERSION,
                                            dry_run=dry_run))

    def test_rehearsal_and_release_keep_all_targets_but_release_needs_a_floor(self):
        self.make_inventory()
        dry = self.root / "dry"
        release = self.root / "release"
        for floor in ("", "01.2.3", "1.2.3-alpha"):
            with self.subTest(floor=floor), patch.dict(os.environ, {"ENTORHINAL_REQUIRES_CORE": floor}):
                with self.assertRaisesRegex(ValueError, "ENTORHINAL_REQUIRES_CORE must be set"):
                    self.assemble(release)
        with patch.dict(os.environ, {"ENTORHINAL_REQUIRES_CORE": ""}):
            self.assemble(dry, dry_run=True)
        with patch.dict(os.environ, {"ENTORHINAL_REQUIRES_CORE": "1.2.3"}):
            self.assemble(release)
        manifest = {"binaries": {BINARY: {"reports": VERSION}}}
        published = json.loads((release / "release-manifest.json").read_text())
        self.assertEqual(json.loads((dry / "release-manifest.json").read_text()), manifest)
        self.assertEqual(published, {**manifest, "requires_core": "1.2.3"})
        expected = {f"ck-entorhinal-{target}.zip{suffix}" for target in TARGETS for suffix in ("", ".sha256")}
        expected.add("release-manifest.json")
        self.assertEqual({p.name for p in dry.iterdir()}, expected)
        self.assertEqual({p.name for p in release.iterdir()}, expected)

        with ZipFile(release / "ck-entorhinal-linux-x64.zip") as zipped:
            executable = Path(zipped.extract(BINARY, self.root / "unpacked"))
        executable.chmod(0o755)
        reported = subprocess.check_output([str(executable), "--version"], text=True)
        self.assertIn(published["binaries"][BINARY]["reports"], reported)

    def test_assembly_refuses_an_unexpected_asset(self):
        self.make_inventory()
        (self.built / "ck-projects-linux-x64.zip").touch()
        with patch.dict(os.environ, {"ENTORHINAL_REQUIRES_CORE": "1.2.3"}):
            with self.assertRaisesRegex(ValueError, "wrong asset inventory.*ck-projects-linux-x64.zip"):
                self.assemble(self.root / "release")

    def test_assembly_refuses_legacy_inventory_without_windows_arm64(self):
        self.make_inventory()
        for suffix in (".zip", ".zip.sha256"):
            (self.built / f"ck-entorhinal-windows-arm64{suffix}").rename(self.built / f"ck-entorhinal-darwin-x64{suffix}")
        with patch.dict(os.environ, {"ENTORHINAL_REQUIRES_CORE": "1.2.3"}):
            with self.assertRaisesRegex(ValueError, "missing .*windows-arm64.*unexpected .*darwin-x64"):
                self.assemble(self.root / "release")

    def test_assembly_refuses_corrupt_windows_archives(self):
        for arch in ("x64", "arm64"):
            with self.subTest(arch=arch), patch.dict(os.environ, {"ENTORHINAL_REQUIRES_CORE": "1.2.3"}):
                self.built = self.root / f"built-{arch}"
                self.make_inventory()
                archive = self.built / f"ck-entorhinal-windows-{arch}.zip"
                archive.write_bytes(b"not the archived binary")
                with self.assertRaisesRegex(ValueError, rf"{archive.name}.sha256: checksum does not match"):
                    self.assemble(self.root / f"release-{arch}")
