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

import io
import logging

import display_utils
import log_file
import utils
from log_file import ParsedLog


def get_title(log_file_path, parsed_log):
    return f"Parsing of: {log_file_path}"


def print_title(f, log_file_path, parsed_log):
    title = get_title(log_file_path, parsed_log)
    print(f"{title}", file=f)
    print(len(title) * "=", file=f)


def print_cf_console_printout(f, parsed_log):
    assert isinstance(parsed_log, log_file.ParsedLog)

    cfs_info_for_display = \
        display_utils.prepare_general_cf_info_for_display(parsed_log)

    cfs_display_values = []
    for cf_name, cf_info in cfs_info_for_display.items():
        # cf_info = cfs_info_for_display[cf_name]
        row = [
            cf_name,
            cf_info["CF Size"],
            cf_info["Avg. Key Size"],
            cf_info["Avg. Value Size"],
            cf_info["Compaction Style"],
            cf_info["Compression"],
            cf_info["Filter-Policy"]
        ]
        cfs_display_values.append(row)

    table_header = ["Column Family",
                    "Size (*)",
                    "Avg. Key Size",
                    "Avg. Value Size",
                    "Compaction Style",
                    "Compression",
                    "Filter-Policy"]
    ascii_table = display_utils.generate_ascii_table(table_header,
                                                     cfs_display_values)
    print(ascii_table, file=f)


def print_general_info(f, parsed_log: ParsedLog):
    display_general_info_dict = \
        display_utils.prepare_db_wide_info_for_display(parsed_log)

    if isinstance(display_general_info_dict["Error Messages"], dict):
        error_lines = ""
        for error_time, error_msg in \
                display_general_info_dict["Error Messages"].items():
            error_lines += f"\n{error_time} {error_msg}"
        display_general_info_dict["Error Messages"] = error_lines

    if isinstance(display_general_info_dict["Fatal Messages"], dict):
        error_lines = ""
        for error_time, error_msg in \
                display_general_info_dict["Fatal Messages"].items():
            error_lines += f"\n{error_time} {error_msg}"
        display_general_info_dict["Fatal Messages"] = error_lines

    display_general_info_dict = \
        utils.replace_key_keep_order(
            display_general_info_dict, "DB Size", "DB Size (*)")
    db_size_time = display_general_info_dict["DB Size Time"]
    del display_general_info_dict["DB Size Time"]

    ingest_time = display_general_info_dict["Ingest Time"]

    suffix = "*"
    if db_size_time != ingest_time:
        suffix += "*"
    display_general_info_dict = \
        utils.replace_key_keep_order(
            display_general_info_dict, "Ingest", f"Ingest ({suffix})")
    del display_general_info_dict["Ingest Time"]

    width = 25
    for field_name, value in display_general_info_dict.items():
        print(f"{field_name.ljust(width)}: {value}", file=f)
    print_cf_console_printout(f, parsed_log)

    if db_size_time != ingest_time:
        print(f"(*) DB / CF-s Size is calculated at: "
              f"{db_size_time}", file=f)
        print(f"(**) Ingest Data are calculated at: {ingest_time}", file=f)
    else:
        print(f"(*) Data is calculated at: {db_size_time}", file=f)


def get_console_output(log_file_path, parsed_log, output_type):
    logging.debug(f"Preparing {output_type} Console Output")

    f = io.StringIO()

    if output_type == utils.ConsoleOutputType.SHORT:
        print_title(f, log_file_path, parsed_log)
        print_general_info(f, parsed_log)

    return f.getvalue()
