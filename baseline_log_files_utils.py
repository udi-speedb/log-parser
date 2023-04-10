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

import bisect
import pathlib
import re
from dataclasses import dataclass

import regexes
import utils
from database_options import DatabaseOptions
from log_file import ParsedLog


@dataclass
class Version:
    major: int
    minor: int
    patch: int

    def __init__(self, version_str):
        version_parts = re.findall(regexes.VERSION, version_str)
        assert len(version_parts) == 1 and len(version_parts[0]) == 3
        self.major = int(version_parts[0][0])
        self.minor = int(version_parts[0][1])
        self.patch = int(version_parts[0][2]) if version_parts[0][2] else None

    def get_patch_for_comparison(self):
        if self.patch is None:
            return -1
        return self.patch

    def __eq__(self, other):
        return self.major == other.major and \
               self.minor == other.minor and \
               self.get_patch_for_comparison() == \
               other.get_patch_for_comparison()

    def __lt__(self, other):
        if self.major != other.major:
            return self.major < other.major
        elif self.minor != other.minor:
            return self.minor < other.minor
        else:
            return self.get_patch_for_comparison() < \
                   other.get_patch_for_comparison()

    def __repr__(self):
        if self.patch is not None:
            patch = f".{self.patch}"
        else:
            patch = ""

        return f"{self.major}.{self.minor}{patch}"


@dataclass
class BaselineLogFileInfo:
    file_name: str
    version: Version

    def __lt__(self, other):
        return self.version < other.version


def find_all_baseline_log_files(baselines_logs_folder, product_name):
    if product_name == utils.ProductName.ROCKSDB:
        logs_regex = regexes.ROCKSDB_BASELINE_LOG_FILE
    elif product_name == utils.ProductName.SPEEDB:
        logs_regex = regexes.SPEEDB_BASELINE_LOG_FILE
    else:
        assert False

    logs_folder_path = pathlib.Path(baselines_logs_folder)

    files = []
    for file_name in logs_folder_path.iterdir():
        file_match = re.findall(logs_regex, file_name.name)
        if file_match:
            assert len(file_match) == 1
            files.append(BaselineLogFileInfo(
                file_name.name, Version(file_match[0])))

    files.sort()
    return files


def find_closest_version_idx(baseline_versions, version):
    if isinstance(version, str):
        version = Version(version)

    if baseline_versions[0] == version:
        return 0

    baseline_versions.sort()
    closest_version_idx = bisect.bisect_right(baseline_versions, version)

    if closest_version_idx:
        return closest_version_idx-1
    else:
        return None


def find_closest_baseline_log_file_name(baselines_logs_folder, product_name,
                                        version_str):
    baseline_files = find_all_baseline_log_files(baselines_logs_folder,
                                                 product_name)
    baseline_versions = [file.version for file in baseline_files]
    if not baseline_versions:
        return None

    closest_version_idx = find_closest_version_idx(baseline_versions,
                                                   version_str)
    if closest_version_idx is not None:
        return baseline_files[closest_version_idx].file_name,\
               baseline_files[closest_version_idx].version
    else:
        return None


def get_baseline_database_options(baselines_logs_folder,
                                  product_name,
                                  version_str):
    closest_baseline_info = \
        find_closest_baseline_log_file_name(baselines_logs_folder,
                                            product_name,
                                            version_str)
    if closest_baseline_info is None:
        return None

    closest_baseline_log_file_name, closest_version = closest_baseline_info
    baseline_log_path = utils.get_file_path(
        baselines_logs_folder, closest_baseline_log_file_name)

    with baseline_log_path.open() as baseline_log_file:
        log_lines = baseline_log_file.readlines()
        log_lines = [line for line in log_lines]

        main_parsing_context = utils.parsing_context
        parsed_log = ParsedLog(str(baseline_log_path), log_lines)
        utils.parsing_context = main_parsing_context

        baseline_options = parsed_log.get_database_options()
        return baseline_options, closest_version


def find_options_diff_relative_to_baseline(baselines_logs_folder,
                                           product_name,
                                           version,
                                           database_options):
    baseline_info = get_baseline_database_options(baselines_logs_folder,
                                                  product_name,
                                                  version)
    if baseline_info is None:
        return None

    baseline_database_options, closest_version = baseline_info
    diff = DatabaseOptions.get_options_diff(
        baseline_database_options.get_all_options(),
        database_options.get_all_options())
    return diff, closest_version
