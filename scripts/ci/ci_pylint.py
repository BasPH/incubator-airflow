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

import os
import re
import subprocess
import sys
from io import StringIO

from collections import defaultdict
from pathlib import Path

from pylint import lint
from pylint.reporters.text import ColorizedTextReporter

# This script runs Pylint and filters out only the changed lines.
# TODO remove this script once the complete Airflow codebase is Pylint compatible.

# Get Python files to run Pylint against.
# Git command from https://github.com/sk-/git-lint/blob/master/gitlint/git.py
git_modified_files_cmd = "git status --porcelain --untracked-files=all --ignore-submodules=all".split()
status_lines = subprocess.check_output(git_modified_files_cmd).decode("utf-8").splitlines()
accepted_modes = ["M", "A", "AM", "MM", "??"]
accepted_modes_regex = "|".join([re.escape(mode) for mode in accepted_modes])
regex = r"\s?({0})\s*(?P<filename>.+)$".format(accepted_modes_regex)
py_files_to_check = set()
for line in status_lines:
    regex_match = re.match(regex, line)
    filename = regex_match.group("filename")
    if filename.endswith(".py"):
        py_files_to_check.add(filename)

# Get lines to run Pylint against.
# Note: check unstaged lines! So use this script in combination with ci_pylint.sh.
filename_lines = defaultdict(list)
for filename in py_files_to_check:
    git_blame_cmd = "git blame --porcelain {0}".format(filename).split()
    try:
        with open(os.devnull, "w") as devnull:
            blame_lines = subprocess.check_output(git_blame_cmd, stderr=devnull).decode("utf-8").splitlines()
    except Exception:
        # Raised if new file -> no history. Set linenumbers to None.
        filename_lines[filename] = None
        continue

    # We only check lines starting with 40 zeros. Therefore run this script only from ci_pylint.sh
    # which does a git soft reset to move the HEAD to the starting commit.
    check_lines = [l for l in blame_lines if l.startswith("0" * 40)]
    for check_line in check_lines:
        linenr = check_line.split(" ")[1]
        filename_lines[filename].append(linenr)

# Now run Pylint on all Python files and filter output.
exit_code = 0
for filename, changed_linenrs in filename_lines.items():
    reporter = ColorizedTextReporter()
    result = StringIO()
    reporter.set_output(result)

    args = [
        "--rcfile={0}/.pylintrc".format(Path(__file__).resolve().parents[2]),
        "--reports=n",
        "--score=n",
        filename,
    ]
    lint.Run(args, reporter=reporter, do_exit=False)

    output = result.getvalue()
    # Output is "" if no Pylint messages for this file
    if output != "":
        output = output.splitlines()
        output_header = output[0]
        output_lines = output[1:]
        output_filtered = []
        line_regex = r"{0}:(?P<linenr>[0-9]+):.*".format(filename)
        for output_line in output_lines:
            if output_line != "":
                regex_match = re.match(line_regex, output_line)
                linenr_matched = regex_match.group("linenr")
                if changed_linenrs is None or linenr_matched in changed_linenrs:
                    output_filtered.append(output_line)

        if output_filtered:
            exit_code = 1
            print(output_header)
            for filtered_line in output_filtered:
                print(filtered_line)
            print()

# Finally, exit with the set exit code.
sys.exit(exit_code)
