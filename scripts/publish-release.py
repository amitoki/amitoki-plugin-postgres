#!/usr/bin/env python3
"""検証済みのプラグイン・manifest・SQLを版タグから公開する。"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tomllib

ROOT = Path(__file__).resolve().parents[1]
# 現在のプラグインCIがネイティブでビルド・検証するCPU。
TARGET = "x86_64-unknown-linux-gnu"


def output(*arguments):
    return subprocess.check_output(arguments, cwd=ROOT, text=True).strip()


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def verify_package(directory, *, release_version, schema):
    manifest_path = directory / f"plugin-{TARGET}.json"
    binary = directory / f"amitoki-plugin-postgres-{TARGET}"
    package = json.loads(manifest_path.read_text())
    if package["target"] != TARGET or package["binary"] != "amitoki-plugin-postgres":
        raise ValueError("配布物のCPUまたは実行ファイル名が違います")
    manifest = package["manifest"]
    if manifest["name"] != "postgres" or manifest["version"] != release_version:
        raise ValueError("manifestと公開する版が一致しません")
    if digest(binary) != package["sha256"]:
        raise ValueError("実行ファイルのSHA256が一致しません")
    # ActionsのArtifactから復元すると実行権限が失われる。
    binary.chmod(0o755)
    if json.loads(output(str(binary), "--describe")) != manifest:
        raise ValueError("実行ファイルとmanifestが一致しません")
    packaged_schema = directory / "schema.sql"
    if packaged_schema.read_bytes() != schema.read_bytes():
        raise ValueError("配布するSQLとタグのSQLが一致しません")
    embedded_schema = subprocess.check_output([str(binary), "--schema"])
    if embedded_schema != schema.read_bytes():
        raise ValueError("実行ファイルに埋め込まれたSQLが一致しません")
    assets = [binary, manifest_path, packaged_schema]
    checksums = directory / "SHA256SUMS"
    checksums.write_text("".join(f"{digest(path)}  {path.name}\n" for path in assets))
    return [*assets, checksums]


def release_preflight(tag):
    release_version = tomllib.loads((ROOT / "Cargo.toml").read_text())["package"]["version"]
    if tag != f"v{release_version}":
        raise ValueError("タグとCargo.tomlの版が一致しません")
    commit = output("git", "rev-parse", "HEAD")
    if output("git", "rev-parse", f"refs/tags/{tag}^{{commit}}") != commit:
        raise ValueError("チェックアウトしたcommitとタグが一致しません")
    subprocess.run(["git", "merge-base", "--is-ancestor", commit, "origin/main"], cwd=ROOT, check=True)
    notes = ROOT / f"docs/releases/{tag}.md"
    if not notes.is_file():
        raise ValueError("リリースノートがありません")
    return release_version, notes


def verify_uploaded(release, *, assets, notes):
    expected = {path.name: f"sha256:{digest(path)}" for path in assets}
    uploaded = {asset["name"]: asset["digest"] for asset in release["assets"]}
    if uploaded != expected or release["body"].strip() != notes.read_text().strip():
        raise ValueError("GitHub上の配布物またはノートが一致しません。公開済み版は上書きしません")


def publish(directory, *, tag, repository):
    release_version, notes = release_preflight(tag)
    assets = verify_package(directory, release_version=release_version, schema=ROOT / "schema.sql")
    base = ["gh", "release"]
    view = [*base, "view", tag, "--repo", repository, "--json", "isDraft,body,assets"]
    existing = subprocess.run(view, capture_output=True, text=True, check=False)
    if existing.returncode == 0:
        release = json.loads(existing.stdout)
        if not release["isDraft"]:
            verify_uploaded(release, assets=assets, notes=notes)
            print("同じ内容が公開済みです。変更しません")
            return
        subprocess.run([*base, "edit", tag, "--repo", repository, "--title", tag,
                        "--notes-file", str(notes)], check=True)
    else:
        subprocess.run([*base, "create", tag, "--repo", repository, "--verify-tag", "--draft",
                        "--title", tag, "--notes-file", str(notes)], check=True)
    # 完了前のdraftにだけ再アップロードし、全ファイルの一致後に公開する。
    subprocess.run([*base, "upload", tag, "--repo", repository, "--clobber", *map(str, assets)], check=True)
    verify_uploaded(json.loads(output(*view)), assets=assets, notes=notes)
    subprocess.run([*base, "edit", tag, "--repo", repository, "--draft=false", "--latest"], check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--repository", required=True)
    arguments = parser.parse_args()
    publish(arguments.directory.resolve(), tag=arguments.tag, repository=arguments.repository)


if __name__ == "__main__":
    main()
