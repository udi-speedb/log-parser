# Copyright (C) 2023 Speedb Ltd. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.'''

from log_entry import LogEntry
from log_file import ParsedLog
from test.sample_log_info import SampleLogInfo


def read_file(file_path):
    with open(file_path, "r") as f:
        return f.readlines()


def create_parsed_log(file_path):
    log_lines = read_file(file_path)
    return ParsedLog(SampleLogInfo.FILE_PATH, log_lines)


def line_to_entry(line):
    assert LogEntry.is_entry_start(line)
    return LogEntry(0, line)


def lines_to_entries(lines):
    entries = []
    entry = None
    for i, line in enumerate(lines):
        if LogEntry.is_entry_start(line):
            if entry:
                entries.append(entry.all_lines_added())
            entry = LogEntry(i, line)
        else:
            assert entry
            entry.add_line(line)

    if entry:
        entries.append(entry.all_lines_added())

    return entries


def add_stats_entry_lines_to_counters_and_histograms_mngr(entry_lines, mngr):
    entry = LogEntry(0, entry_lines[0])
    for line in entry_lines[1:]:
        entry.add_line(line)
    mngr.add_entry(entry.all_lines_added())
