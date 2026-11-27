"""Package and verify Entorhinal's installer assets on the release runners."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import tempfile
from zipfile import ZIP_DEFLATED, BadZipFile, ZipFile


BINARY = "ck-entorhinal"
TARGETS = ("darwin-arm64", "linux-x64", "linux-arm64", "windows-x64", "windows-arm64")
TRIPLET = r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"


def archive_path(directory, target):
    return directory / f"{BINARY}-{target}.zip"


def member_name(target):
    return f"{BINARY}.exe" if target.startswith("windows-") else BINARY


def pack(args):
    name = member_name(args.target)
    args.directory.mkdir(parents=True, exist_ok=True)
    # The other CLI faces are placement links, not files in the archive.
    with ZipFile(archive_path(args.directory, args.target), "x", ZIP_DEFLATED) as archive:
        archive.write(args.source / name, arcname=name)


def checksum(archive):
    digest = hashlib.sha256()
    with archive.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"{digest.hexdigest()}  {archive.name}\n".encode("ascii")


def archive_member(archive, target):
    expected = member_name(target)
    entries = archive.infolist()
    if len(entries) != 1 or entries[0].filename != expected or not entries[0].file_size:
        raise ValueError(f"{archive.filename}: expected only a nonempty {expected} at the ZIP root")
    if not target.startswith("windows-"):
        mode = entries[0].external_attr >> 16
        if not stat.S_ISREG(mode) or not mode & stat.S_IXUSR:
            raise ValueError(f"{archive.filename}: {expected} must be a regular executable")
    return entries[0]


def probe(path, flag):
    return subprocess.check_output([str(path), flag], text=True, timeout=15).strip()


def check_binary(executable, version, revision, *, label=None):
    validate_revision(revision)
    label = label or executable
    executable = executable.resolve(strict=True)
    reported = probe(executable, "--version")
    if reported != f"{BINARY} {version}":
        raise ValueError(f"{label}: unexpected --version: {reported!r}, expected {BINARY} {version}")
    manifest = json.loads(probe(executable, "--manifest"))
    if (manifest["module_id"] != "entorhinal" or manifest["module_version"] != version
            or manifest["registration_channel"] is not True):
        raise ValueError(f"{label}: --manifest must declare entorhinal {version} with registration_channel true")
    # Entorhinal's --version reports only the crate version. Release builds
    # stamp the Git SHA in --manifest, and only for a clean tree.
    stamped = manifest.get("provenance", {}).get("build_git_sha")
    if stamped != revision:
        raise ValueError(f"{label}: --manifest provenance reports {stamped!r}, expected {revision}")
    print(f"{label}: {reported} (manifest commit {stamped})")


def verify_target(args):
    path = archive_path(args.directory, args.target)
    with tempfile.TemporaryDirectory() as scratch, ZipFile(path) as archive:
        member = archive_member(archive, args.target)
        executable = Path(scratch) / member.filename
        with archive.open(member) as source, executable.open("xb") as destination:
            shutil.copyfileobj(source, destination)
        executable.chmod(0o755)
        check_binary(executable, args.version, args.revision, label=path)
    path.with_suffix(".zip.sha256").write_bytes(checksum(path))


def validate_revision(revision):
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("expected a full 40-character Git commit SHA")


def core_floor(dry_run):
    floor = os.environ.get("ENTORHINAL_REQUIRES_CORE", "")
    if not floor and dry_run:
        return None
    if not re.fullmatch(TRIPLET, floor):
        raise ValueError("ENTORHINAL_REQUIRES_CORE must be set to the confirmed MAJOR.MINOR.PATCH floor before publishing")
    return floor


def assemble(args):
    floor = core_floor(args.dry_run)
    archives = [archive_path(args.directory, target) for target in TARGETS]
    files = {path.name for path in archives} | {f"{path.name}.sha256" for path in archives}
    actual = {path.name for path in args.directory.iterdir()}
    if actual != files:
        raise ValueError(f"wrong asset inventory: missing {sorted(files - actual)}; unexpected {sorted(actual - files)}")
    args.output.mkdir(parents=True)
    for target in TARGETS:
        path = archive_path(args.directory, target)
        sidecar = path.with_suffix(".zip.sha256")
        if sidecar.read_bytes() != checksum(path):
            raise ValueError(f"{sidecar}: checksum does not match the ZIP")
        with ZipFile(path) as archive:
            archive_member(archive, target)
            if archive.testzip() is not None:
                raise ValueError(f"{path}: ZIP integrity check failed")
        shutil.copyfile(path, args.output / path.name)
        shutil.copyfile(sidecar, args.output / sidecar.name)

    # ck setup looks for reports in --version, not in --manifest.
    manifest: dict[str, object] = {"binaries": {BINARY: {"reports": args.version}}}
    if floor is not None:
        manifest["requires_core"] = floor
    (args.output / "release-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"Verified {len(archives)} ZIPs and sidecars; prepared {len(list(args.output.iterdir()))} files in {args.output}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    packaging = commands.add_parser("pack")
    packaging.add_argument("source", type=Path)
    packaging.add_argument("directory", type=Path)
    packaging.add_argument("target", choices=TARGETS)
    binary_check = commands.add_parser("check-binary")
    binary_check.add_argument("executable", type=Path)
    binary_check.add_argument("version")
    binary_check.add_argument("revision")
    verification = commands.add_parser("verify-target")
    verification.add_argument("directory", type=Path)
    verification.add_argument("target", choices=TARGETS)
    verification.add_argument("version")
    verification.add_argument("revision")
    assembly = commands.add_parser("assemble")
    assembly.add_argument("directory", type=Path)
    assembly.add_argument("output", type=Path)
    assembly.add_argument("version")
    assembly.add_argument("--dry-run", action="store_true")
    commands.add_parser("check-floor")
    args = parser.parse_args()
    try:
        if args.command == "pack":
            pack(args)
        elif args.command == "check-binary":
            check_binary(args.executable, args.version, args.revision)
        elif args.command == "verify-target":
            verify_target(args)
        elif args.command == "assemble":
            assemble(args)
        elif args.command == "check-floor":
            print(f"requires_core: {core_floor(dry_run=False)}")
    except (BadZipFile, KeyError, OSError, TypeError, ValueError, subprocess.SubprocessError) as error:
        parser.exit(1, f"release assets: {error}\n")


if __name__ == "__main__":
    main()
