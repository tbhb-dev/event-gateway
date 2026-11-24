# SPDX-License-Identifier: Apache-2.0
# Copyright Tony Burns

"""Check that CI can run on a merge group without a pull request payload."""

import re
import sys
from pathlib import Path

import yaml

PR_PAYLOAD = re.compile(
    r"\bgithub\s*(?:\.\s*event|\[\s*'event'\s*\])"
    r"\s*(?:\.\s*pull_request\b|\[\s*'pull_request'\s*\])",
    re.IGNORECASE,
)
PR_ONLY = re.compile(r"github\.event_name\s*==\s*'pull_request'", re.IGNORECASE)


def uses_pr_payload(value):
    if isinstance(value, dict):
        return any(uses_pr_payload(child) for child in value.values())
    if isinstance(value, list):
        return any(uses_pr_payload(child) for child in value)
    return isinstance(value, str) and PR_PAYLOAD.search(value) is not None


def pr_only(config):
    condition = config.get("if", "").strip()
    condition = condition.removeprefix("${{").removesuffix("}}").strip()
    return PR_ONLY.fullmatch(condition) is not None


def main():
    path = Path(".github/workflows/ci.yml")
    workflow = yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    errors = []
    if workflow.get("on", {}).get("merge_group") != {"types": ["checks_requested"]}:
        errors.append("CI must run when a merge group requests checks")

    for job_id, job in workflow["jobs"].items():
        if pr_only(job):
            continue
        if uses_pr_payload({key: value for key, value in job.items() if key != "steps"}):
            errors.append(f"Job {job_id} uses a pull request payload without a guard")
        for index, step in enumerate(job.get("steps", []), start=1):
            if not pr_only(step) and uses_pr_payload(step):
                errors.append(f"Job {job_id} step {index} uses a pull request payload without a guard")

    for error in errors:
        print(f"{path}: {error}", file=sys.stderr)
    return int(bool(errors))


if __name__ == "__main__":
    sys.exit(main())
