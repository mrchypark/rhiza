"""Check that the S3 fixture can retry bucket creation safely."""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from operator_fixture import resources


class BucketJobTest(unittest.TestCase):
    def test_bucket_creation_is_idempotent_and_rejects_bad_credentials(self):
        job = next(
            item for item in resources("test", "db", "operator")
            if item["kind"] == "Job" and item["metadata"]["name"] == "rhiza-create-bucket"
        )
        command = job["spec"]["template"]["spec"]["containers"][0]["args"][0]
        with tempfile.TemporaryDirectory() as directory:
            fake_aws = Path(directory, "aws")
            fake_aws.write_text(
                '#!/bin/sh\n'
                'test "$AWS_SECRET_ACCESS_KEY" = valid || exit 1\n'
                'case "$*" in\n'
                '  *head-bucket*) test -f "$BUCKET_MARKER" ;;\n'
                '  *"s3 mb"*) touch "$BUCKET_MARKER" ;;\n'
                'esac\n'
            )
            fake_aws.chmod(0o755)
            environment = dict(os.environ, PATH=f"{directory}:{os.environ['PATH']}",
                               BUCKET_MARKER=str(Path(directory, "created")),
                               AWS_SECRET_ACCESS_KEY="valid")
            for _ in range(2):
                self.assertEqual(subprocess.run(["/bin/sh", "-c", command], env=environment,
                                                capture_output=True, timeout=5).returncode, 0)
            environment["AWS_SECRET_ACCESS_KEY"] = "invalid"
            self.assertNotEqual(subprocess.run(["/bin/sh", "-c", command.replace("-lt 120", "-lt 1")],
                                              env=environment, capture_output=True, timeout=5).returncode, 0)


if __name__ == "__main__":
    unittest.main()
