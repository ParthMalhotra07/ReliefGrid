#!/usr/bin/env python3
"""Regression checks for report coverage and source line references."""
import contextlib
import importlib.util
import io
from pathlib import Path
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('report_verify', Path(__file__).with_name('verify-report.py'))
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)


class ReportLinkTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for name in report.REQUIRED:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('# chapter\n')
        self.addendum = self.root / 'docs/report/validation-addendum.md'
        self.addendum.parent.mkdir(parents=True, exist_ok=True)
        self.addendum.write_text('# additional validation\n')
        self.patch = mock.patch.object(report, 'ROOT', self.root)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def test_validation_addendum_broken_link_rejected(self):
        self.addendum.write_text('[missing evidence](../evidence/not-retained.txt)\n')
        with self.assertRaisesRegex(SystemExit, 'validation-addendum.md: broken local link'):
            report.verify()

    def test_out_of_bounds_source_line_rejected(self):
        source = self.root / 'internal/fsm/fsm.go'
        source.parent.mkdir(parents=True)
        source.write_text('package fsm\n')
        (self.root / 'docs/report/architecture.md').write_text('[source](../../internal/fsm/fsm.go#L200)\n')
        with self.assertRaisesRegex(SystemExit, 'source line outside file'):
            report.verify()

    def test_valid_source_range_and_external_link(self):
        source = self.root / 'internal/fsm/fsm.go'
        source.parent.mkdir(parents=True)
        source.write_text('package fsm\n// second line\n')
        (self.root / 'docs/report/architecture.md').write_text(
            '[source](../../internal/fsm/fsm.go#L1-L2)\n[historical](https://example.test/tree/recorded)\n')
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            report.verify()
        self.assertIn('REPORT VERIFY PASS', output.getvalue())


if __name__ == '__main__':
    unittest.main()
