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

import defs_and_utils
import calc_utils
import baseline_log_files_utils
from database_options import DatabaseOptions, CfsOptionsDiff, SectionType
import database_options
from database_options import SanitizedValueType
from dataclasses import dataclass


def prepare_db_wide_user_opers_stats_for_display(db_wide_info):
    display_info = {}

    total_num_user_opers = db_wide_info["total_num_user_opers"]
    total_num_flushed_entries = db_wide_info['total_num_flushed_entries']

    if total_num_user_opers > 0:
        display_info['Writes'] = \
            f"{db_wide_info['percent_written']}" \
            f"({db_wide_info['num_written']}/{total_num_user_opers})"
        display_info['Reads'] = \
            f"{db_wide_info['percent_read']}" \
            f"({db_wide_info['num_read']}/{total_num_user_opers})"
        display_info['Seeks'] = \
            f"{db_wide_info['percent_seek']}" \
            f"({db_wide_info['num_seek']}/{total_num_user_opers})"
    else:
        display_info['Writes'] = "Not Available"
        display_info['Reads'] = "Not Available"
        display_info['Seeks'] = "Not Available"

    if total_num_flushed_entries > 0:
        display_info['Deletes'] = \
            f"{db_wide_info['total_percent_deletes']}" \
            f"({db_wide_info['total_num_deletes']}/" \
            f"{total_num_flushed_entries})"

    return display_info


@dataclass
class NotableEntityInfo:
    display_title: str
    display_text: str
    special_value_type: SanitizedValueType
    special_value_text: str


notable_entities = {
    "statistics": NotableEntityInfo("Statistics",
                                    "Available",
                                    SanitizedValueType.NO_VALUE,
                                    "No Statistics")

}


def get_db_wide_notable_entities_display_info(parsed_log):
    display_info = {}
    db_options = parsed_log.get_database_options()
    for option_name, info in notable_entities.items():
        option_value = db_options.get_db_wide_option(option_name)
        if option_value is None:
            logging.warning(f"Option {option_name} not found in "
                            f"{parsed_log.get_log_file_path()}")
            continue

        option_value_type = SanitizedValueType.get_type_from_str(option_value)
        if option_value_type == info.special_value_type:
            display_value = info.special_value_text
        else:
            if info.display_text is None:
                display_value = option_value
            else:
                display_value = f"{info.display_text} ({option_value})"
        display_info[info.display_title] = display_value

    return display_info


def prepare_db_wide_info_for_display(parsed_log):
    display_info = {}

    db_wide_info = calc_utils.get_db_wide_info(parsed_log)
    db_wide_notable_entities = \
        get_db_wide_notable_entities_display_info(parsed_log)
    cfs_options = calc_utils.get_applicable_cf_options(parsed_log)

    size_for_display = defs_and_utils.get_size_for_display

    display_info["Name"] = parsed_log.get_log_file_path()
    display_info["Creator"] = db_wide_info['creator']
    display_info["Version"] = f"{db_wide_info['version']} " \
                              f"[{db_wide_info['git_hash']}]"
    display_info["DB Size"] = size_for_display(db_wide_info['db_size_bytes'])
    display_info["Num Keys"] = f"{db_wide_info['num_written']:,}"
    display_info["Average Key Size"] = \
        size_for_display(db_wide_info['avg_key_size_bytes'])
    display_info["Average Value Size"] = \
        size_for_display(db_wide_info['avg_value_size_bytes'])
    display_info["Num Warnings"] = db_wide_info['num_warns']
    display_info["Num Errors"] = db_wide_info['num_errors']
    display_info["Num Stalls"] = db_wide_info['num_stalls']
    display_info["Compaction Style"] = \
        cfs_options['compaction_style']['common']
    display_info["Compression"] = cfs_options['compression']['common']
    display_info["Filter-Policy"] = cfs_options['filter_policy']['common']
    display_info.update(db_wide_notable_entities)
    display_info.update(prepare_db_wide_user_opers_stats_for_display(
        db_wide_info))
    display_info['Num CF-s'] = db_wide_info['num_cfs']

    return display_info


def prepare_general_cf_info_for_display(parsed_log):
    display_info = {}

    cf_names = parsed_log.get_cf_names_that_have_options()
    events_mngr = parsed_log.get_events_mngr()

    size_for_display = defs_and_utils.get_size_for_display

    for i, cf_name in enumerate(cf_names):
        table_creation_stats = \
            calc_utils.calc_cf_table_creation_stats(cf_name, events_mngr)
        cf_options = calc_utils.get_applicable_cf_options(parsed_log)
        cf_size_bytes = calc_utils.get_cf_size_bytes(parsed_log, cf_name)

        display_info[cf_name] = {}
        cf_display_info = display_info[cf_name]

        cf_display_info["CF Size"] = size_for_display(cf_size_bytes)
        cf_display_info["Avg Key Size"] = \
            size_for_display(table_creation_stats['avg_key_size'])
        cf_display_info["Avg Value Size"] = \
            size_for_display(table_creation_stats['avg_value_size'])

        if cf_options['compaction_style'][cf_name] is not None:
            cf_display_info["Compaction Style"] = \
                cf_options['compaction_style'][cf_name]
        else:
            cf_display_info["Compaction Style"] = "UNKNOWN"

        if cf_options['compression'][cf_name] is not None:
            cf_display_info["Compression"] = \
                cf_options['compression'][cf_name]
        elif calc_utils.is_cf_compression_by_level(parsed_log, cf_name):
            cf_display_info["Compression"] = "Per-Level"
        else:
            cf_display_info["Compression"] = "UNKNOWN"

        if cf_options['filter_policy'][cf_name] is not None:
            cf_display_info["Filter-Policy"] = \
                cf_options['filter_policy'][cf_name]
        else:
            cf_display_info["Filter-Policy"] = "UNKNOWN"

    return display_info


def prepare_warnings_for_display(parsed_log):
    warnings = parsed_log.get_warnings_mngr().get_all_warnings()
    display_warnings = {}
    for key in warnings.keys():
        if len(warnings[key]) == 0:
            continue

        display_warnings[key.value] = {}
        for warning_id in warnings[key]:
            warning_id_warnings = warnings[key][warning_id]
            num_warnings_of_id = len(warning_id_warnings)
            warning_id_key = f"{warning_id} ({num_warnings_of_id})"
            display_warnings[key.value][warning_id_key] = {}
            for warn_info in warning_id_warnings:
                temp = display_warnings[key.value][warning_id_key]
                temp[warn_info.warning_time] = \
                    f"[{warn_info.cf_name}] {warn_info.warning_msg}"
    return display_warnings if display_warnings else "No Warnings"


def get_all_options_for_display(parsed_log):
    options_per_cf = {}
    cf_names = parsed_log.get_cf_names()
    db_options = parsed_log.get_database_options()

    options_per_cf["DB-Wide"] = db_options.get_db_wide_options_for_display()
    for cf_name in cf_names:
        cf_options, cf_table_options = \
            db_options.get_cf_options_for_display(cf_name)
        options_per_cf[cf_name] = cf_options
        options_per_cf[cf_name]["Table-Options"] = cf_table_options

    return options_per_cf


def prepare_db_wide_diff_dict_for_display(baseline_version, db_wide_diff):
    del(db_wide_diff[CfsOptionsDiff.CF_NAMES_KEY])

    display_db_wide_diff = {"Baseline": str(baseline_version),
                            "DB-Wide": {},
                            "Misc": {}}

    for full_option_name in db_wide_diff:
        if DatabaseOptions.is_misc_option(full_option_name):
            display_db_wide_diff["Misc"][full_option_name] =\
                db_wide_diff[full_option_name]
        else:
            section_type = SectionType.extract_section_type(full_option_name)
            option_name =\
                database_options.extract_option_name(full_option_name)

            if section_type == SectionType.DB_WIDE:
                display_db_wide_diff["DB-Wide"][option_name] =\
                    db_wide_diff[full_option_name]
            elif section_type == SectionType.VERSION:
                pass
            else:
                assert False, "Unexpected section type"

    if not display_db_wide_diff["DB-Wide"]:
        del(display_db_wide_diff["DB-Wide"])
    if not display_db_wide_diff["Misc"]:
        del(display_db_wide_diff["Misc"])
    if list(display_db_wide_diff.keys()) == ["Baseline"]:
        display_db_wide_diff = {}

    return display_db_wide_diff


def prepare_cfs_diff_dict_for_display(baseline_opts, log_opts, cfs_names):
    display_cfs_diff = {}
    for log_cf_name in cfs_names:
        cf_options_diff = DatabaseOptions.get_cfs_options_diff(
            baseline_opts, "default", log_opts, log_cf_name).get_diff_dict()

        if cf_options_diff:
            assert cf_options_diff[CfsOptionsDiff.CF_NAMES_KEY]["New"] == \
                   log_cf_name
            del(cf_options_diff[CfsOptionsDiff.CF_NAMES_KEY])

            cf_diff_key = f"{log_cf_name}"
            display_cfs_diff[cf_diff_key] = {"CF": {},
                                             "Table": {}}

            for full_option_name in cf_options_diff:
                section_type =\
                    SectionType.extract_section_type(full_option_name)
                option_name =\
                    database_options.extract_option_name(full_option_name)

                if section_type == SectionType.CF:
                    key = "CF"
                elif section_type == SectionType.TABLE_OPTIONS:
                    key = "Table"
                else:
                    assert False, "Unexpected section type"

                display_cfs_diff[cf_diff_key][key][option_name] =\
                    cf_options_diff[full_option_name]

            if not display_cfs_diff[cf_diff_key]["CF"]:
                del(display_cfs_diff[cf_diff_key]["CF"])
            if not display_cfs_diff[cf_diff_key]["Table"]:
                del(display_cfs_diff[cf_diff_key]["Table"])
            if not display_cfs_diff[cf_diff_key]:
                del(display_cfs_diff[cf_diff_key])

    return display_cfs_diff


def get_options_baseline_diff_for_display(parsed_log):
    log_metadata = parsed_log.get_metadata()
    log_database_options = parsed_log.get_database_options()

    baseline_info = baseline_log_files_utils.get_baseline_database_options(
        defs_and_utils.BASELINE_LOGS_FOLDER,
        log_metadata.get_product_name(),
        log_metadata.get_version())

    if baseline_info is None:
        return "NO BASELINE FOUND"

    baseline_database_options, baseline_version = baseline_info
    baseline_opts = baseline_database_options.get_all_options()

    log_opts = log_database_options.get_all_options()

    db_wide_diff = \
        DatabaseOptions.get_db_wide_options_diff(baseline_opts, log_opts)
    db_wide_diff = db_wide_diff.get_diff_dict()
    display_diff = prepare_db_wide_diff_dict_for_display(baseline_version,
                                                         db_wide_diff)

    cfs_names = log_database_options.get_cfs_names()
    cfs_diff = prepare_cfs_diff_dict_for_display(baseline_opts, log_opts,
                                                 cfs_names)
    if cfs_diff:
        display_diff["CF-s"] = cfs_diff

    return display_diff


def prepare_flushes_histogram_for_display(parsed_log):
    flushes_for_display = {}

    cf_names = parsed_log.get_cf_names()
    events_mngr = parsed_log.get_events_mngr()
    for cf_name in cf_names:
        cf_flushes_histogram = calc_utils.calc_flushes_histogram(cf_name,
                                                                 events_mngr)
        if cf_flushes_histogram:
            flushes_for_display[cf_name] = cf_flushes_histogram

    return flushes_for_display


def prepare_db_wide_stalls_entries_for_display(parsed_log):
    stalls_display_entries = {}

    mngr = parsed_log.get_stats_mngr().get_db_wide_stats_mngr()
    stalls_entries = mngr.get_stalls_entries()

    for entry_time, entry_stats in stalls_entries.items():
        stalls_display_entries[entry_time] = \
            {"Interval-Duration": str(entry_stats["interval_duration"]),
             "Interval-Percent": entry_stats["interval_percent"],
             "Cumulative-Duration": str(entry_stats["cumulative_duration"]),
             "Cumulative-Percent": entry_stats["cumulative_percent"]}

    return stalls_display_entries if stalls_display_entries \
        else "No Stalls"


def prepare_cf_stalls_entries_for_display(parsed_log):
    mngr = parsed_log.get_stats_mngr().get_cf_no_file_stats_mngr()
    stall_counts = mngr.get_stall_counts()

    display_stall_counts = {}
    for cf_name in stall_counts.keys():
        if stall_counts[cf_name]:
            display_stall_counts[cf_name] = stall_counts[cf_name]

    return display_stall_counts if display_stall_counts \
        else "No Stalls"


def generate_ascii_table(columns_names, table):
    f = io.StringIO()

    if len(table) < 1:
        return

    max_columns_widths = []
    num_columns = len(columns_names)
    for i in range(num_columns):
        max_value_len = max([len(str(row[i])) for row in table])
        column_name_len = len(columns_names[i])
        max_columns_widths.append(2 + max([max_value_len, column_name_len]))

    header_line = ""
    for i, name in enumerate(columns_names):
        width = max_columns_widths[i]
        header_line += f'|{name.center(width)}'
    header_line += '|'

    print('-' * len(header_line), file=f)
    print(header_line, file=f)
    print('-' * len(header_line), file=f)

    for row in table:
        row_line = ""
        for i, value in enumerate(row):
            width = max_columns_widths[i]
            row_line += f'|{str(value).center(width)}'
        row_line += '|'
        print(row_line, file=f)

    print('-' * len(header_line), file=f)

    return f.getvalue()


if __name__ == '__main__':
    columns_names = ["first", "second", "third"]
    table = \
        [
            [100, 20, 400],
            ['b', 'g', 'r'],
            ["XXX", "TTTTTT", "S"]
        ]
    print(generate_ascii_table(columns_names, table))
