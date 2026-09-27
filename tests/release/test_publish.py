import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/publish-release.py"
SPEC = importlib.util.spec_from_file_location("publish_release", SCRIPT)
publisher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publisher)


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.binary = self.directory / f"amitoki-plugin-postgres-{publisher.TARGET}"
        self.binary.write_bytes(b"verified executable")
        self.manifest = {"name": "postgres", "version": "0.2.0", "protocol_version": 1}
        self.package = {"manifest": self.manifest, "target": publisher.TARGET,
                        "binary": "amitoki-plugin-postgres", "sha256": publisher.digest(self.binary)}
        self.manifest_path = self.directory / f"plugin-{publisher.TARGET}.json"
        self.manifest_path.write_text(json.dumps(self.package))
        self.schema = self.directory / "source.sql"
        self.schema.write_bytes(b"CREATE SCHEMA example;\n")
        (self.directory / "schema.sql").write_bytes(self.schema.read_bytes())

    def verify(self):
        return publisher.verify_package(self.directory, release_version="0.2.0", schema=self.schema)

    def test_matching_package_produces_checksums_for_all_public_files(self):
        with patch.object(publisher, "output", return_value=json.dumps(self.manifest)), \
                patch.object(publisher.subprocess, "check_output", return_value=self.schema.read_bytes()):
            assets = self.verify()
        self.assertEqual(len(assets), 4)
        self.assertEqual((self.directory / "SHA256SUMS").read_text().splitlines(),
                         [f"{publisher.digest(path)}  {path.name}" for path in assets[:-1]])

    def test_corrupted_binary_is_rejected_before_execution(self):
        self.binary.write_bytes(b"corrupted executable")
        with patch.object(publisher, "output") as execute:
            with self.assertRaisesRegex(ValueError, "SHA256"):
                self.verify()
            execute.assert_not_called()

    def test_wrong_version_or_architecture_is_rejected(self):
        for field, value in (("target", "aarch64-unknown-linux-gnu"), ("version", "0.1.2")):
            with self.subTest(field=field):
                package = json.loads(json.dumps(self.package))
                if field == "version":
                    package["manifest"][field] = value
                else:
                    package[field] = value
                self.manifest_path.write_text(json.dumps(package))
                with self.assertRaises(ValueError):
                    self.verify()

    def test_stale_embedded_schema_is_rejected(self):
        with patch.object(publisher, "output", return_value=json.dumps(self.manifest)), \
                patch.object(publisher.subprocess, "check_output", return_value=b"old SQL"):
            with self.assertRaisesRegex(ValueError, "埋め込まれたSQL"):
                self.verify()

    def test_missing_uploaded_asset_or_changed_notes_prevent_publication(self):
        notes = self.directory / "notes.md"
        notes.write_text("release notes\n")
        release = {"body": notes.read_text(), "assets": [
            {"name": self.binary.name, "digest": f"sha256:{publisher.digest(self.binary)}"}]}
        publisher.verify_uploaded(release, assets=[self.binary], notes=notes)
        with self.assertRaises(ValueError):
            publisher.verify_uploaded(release, assets=[self.binary, self.schema], notes=notes)
        release["body"] = "different release notes"
        with self.assertRaises(ValueError):
            publisher.verify_uploaded(release, assets=[self.binary], notes=notes)

    def test_different_published_release_is_never_overwritten(self):
        notes = self.directory / "notes.md"
        notes.write_text("release notes\n")
        existing = publisher.subprocess.CompletedProcess([], 0, json.dumps(
            {"isDraft": False, "body": notes.read_text(), "assets": []}))
        with patch.object(publisher, "release_preflight", return_value=("0.2.0", notes)), \
                patch.object(publisher, "verify_package", return_value=[self.binary]), \
                patch.object(publisher.subprocess, "run", return_value=existing) as command:
            with self.assertRaises(ValueError):
                publisher.publish(self.directory, tag="v0.2.0", repository="amitoki/amitoki-plugin-postgres")
            self.assertEqual(command.call_count, 1)
            self.assertEqual(command.call_args.args[0][:3], ["gh", "release", "view"])


if __name__ == "__main__":
    unittest.main()
