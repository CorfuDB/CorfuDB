"""Regression coverage for compactor provider classpath construction."""
import importlib.util
import os
from pathlib import Path
import sys
import shlex
import tempfile
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location(
    "compactor_runner", Path(__file__).resolve().parents[1] / "compactor_runner.py")
runner = importlib.util.module_from_spec(spec)
# Command construction does not use network discovery or YAML parsing.
with patch.dict(sys.modules, {"netifaces": Mock(), "yaml": Mock()}):
    spec.loader.exec_module(runner)


class ProviderClasspathTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.jar = self.root / "tools with spaces.jar"
        self.jar.touch()
        self.provider_file = self.root / "BC_CLASSPATH"
        patcher = patch.object(runner, "BC_CLASSPATH_FILE", str(self.provider_file))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.builder = runner.CommandBuilder(runner.Config())
        self.config = {
            "GCParameters": {},
            "ConfigFiles": {
                "ClassPath": str(self.jar), "TempDir": str(self.root),
                "CompactorLogbackPath": "logback.xml", "HeapDumpPath": "heap.hprof",
            },
        }

    def arguments(self):
        command = self.builder.append_compactor_config(self.config)
        args = shlex.split(command)
        # Parsing alone cannot detect shell expansion of $(), semicolons or
        # wildcards. Also require the classpath to be rendered as a quoted token.
        classpath = args[args.index("-cp") + 1]
        self.assertTrue(command.endswith("-cp " + shlex.quote(classpath)))
        return args

    def assert_default_classpath(self):
        args = self.arguments()
        self.assertEqual(str(self.jar), args[args.index("-cp") + 1])
        self.assertNotIn("-Dorg.bouncycastle.native.dir=" + str(self.root), args)

    def test_missing_file_preserves_default(self):
        self.assert_default_classpath()

    def test_empty_file_preserves_default(self):
        self.provider_file.write_bytes(b"")
        self.assert_default_classpath()

    def test_invalid_utf8_file_preserves_default(self):
        self.provider_file.write_bytes(b"\xff")
        self.assert_default_classpath()

    def test_unreadable_file_preserves_default(self):
        with patch("builtins.open", side_effect=PermissionError):
            self.assert_default_classpath()

    def test_file_classpath_preserves_multiple_entries_and_wildcards(self):
        providers = [str(self.root / "provider jars" / "*"), str(self.root / "other.jar")]
        (self.root / "provider jars").mkdir()
        (self.root / "provider jars" / "bc.jar").touch()
        self.provider_file.write_text(os.pathsep.join(providers) + "\n")
        args = self.arguments()
        self.assertEqual(os.pathsep.join([str(self.jar)] + providers), args[args.index("-cp") + 1])
        self.assertIn("-Dorg.bouncycastle.native.dir=" + str(self.root), args)

    def test_yaml_string_and_list_override_file(self):
        self.provider_file.write_text("ignored/*")
        for value in ["one/*" + os.pathsep + "two/*", ["one/*", "two/*"]]:
            with self.subTest(value=value):
                self.config["ConfigFiles"]["ProviderClassPath"] = value
                args = self.arguments()
                self.assertEqual(os.pathsep.join([str(self.jar), "one/*", "two/*"]),
                                 args[args.index("-cp") + 1])

    def test_explicit_empty_override_disables_fallback(self):
        self.provider_file.write_text("ignored/*")
        for value in ["", []]:
            self.assertEqual([], self.builder.get_provider_class_path({"ProviderClassPath": value}))

    def test_controller_and_checkpointer_share_provider_configuration(self):
        self.provider_file.write_text("providers/*")
        self.builder._config.hostname = "localhost"
        self.builder._config.corfu_port = "9000"
        self.config.update({
            "MemoryOptions": {}, "CorfuPaths": {},
            "Security": {"Keystore": "key.jks", "KsPassword": "key.pass",
                         "Truststore": "trust.jks", "TruststorePassword": "trust.pass"},
        })
        for main_class in [runner.COMPACTOR_CONTROLS_CLASS_NAME, runner.COMPACTOR_CHECKPOINTER_CLASS_NAME]:
            with self.subTest(main_class=main_class):
                args = shlex.split(self.builder.get_corfu_compactor_cmd(self.config, main_class))
                self.assertIn(main_class, args)
                self.assertIn("--tlsEnabled=true", args)
                self.assertEqual(os.pathsep.join([str(self.jar), "providers/*"]),
                                 args[args.index("-cp") + 1])

    def test_classpath_shell_metacharacters_are_literal(self):
        entry = str(self.root / "$(touch SHOULD_NOT_EXIST);'provider'/*")
        self.config["ConfigFiles"]["ProviderClassPath"] = [entry]
        args = self.arguments()
        self.assertEqual(os.pathsep.join([str(self.jar), entry]), args[args.index("-cp") + 1])


if __name__ == "__main__":
    unittest.main()
