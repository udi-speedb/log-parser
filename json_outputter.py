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
import json
import logging

import cache_utils
import calc_utils
import display_utils
import utils


def get_db_size_json(db_wide_stats_mngr, compactions_stats_mngr):
    db_size_json = {}

    ingest_info = calc_utils.get_db_ingest_info(db_wide_stats_mngr)
    if ingest_info:
        ingest_json = \
            display_utils.prepare_db_ingest_info_for_display(ingest_info)
        db_size_json["Ingest"] = ingest_json
    else:
        db_size_json["Ingest"] = "No Ingest Info Available"

    growth_info = \
        calc_utils.calc_cfs_size_bytes_growth(compactions_stats_mngr)
    growth_json =\
        display_utils.prepare_cfs_size_bytes_growth_for_display(growth_info)
    db_size_json["CF-s Growth"] = growth_json

    return db_size_json


def get_flushes_json(parsed_log):
    flushes_json = {}

    flushes_stats =\
        display_utils.prepare_cf_flushes_stats_for_display(parsed_log)

    if flushes_stats:
        for cf_name in parsed_log.get_cfs_names():
            if cf_name in flushes_stats:
                flushes_json[cf_name] = flushes_stats[cf_name]
    else:
        flushes_json = "No Flushes"

    return flushes_json


def get_compactions_json(parsed_log):
    compactions_json = {}

    compactions_stats = \
        display_utils.prepare_cf_compactions_stats_for_display(parsed_log)
    if compactions_stats:
        compactions_json.update(
            display_utils.prepare_global_compactions_stats_for_display(
                parsed_log))
        compactions_json["CF-s"] = {}
        for cf_name in parsed_log.get_cfs_names():
            if cf_name in compactions_stats:
                compactions_json["CF-s"][cf_name] = compactions_stats[cf_name]
    else:
        compactions_json = "No Compactions"

    return compactions_json


def get_reads_json(parsed_log):
    stats_mngr = parsed_log.get_stats_mngr()
    counters_and_histograms_mngr = \
        parsed_log.get_counters_and_histograms_mngr()
    read_stats = \
        calc_utils.get_applicable_read_stats(counters_and_histograms_mngr,
                                             stats_mngr)
    if read_stats:
        return read_stats
    else:
        return "No Reads"


def get_seeks_json(parsed_log):
    counters_and_histograms_mngr = \
        parsed_log.get_counters_and_histograms_mngr()
    seek_stats = calc_utils.get_applicable_seek_stats(
        counters_and_histograms_mngr)
    if seek_stats:
        disp_dict = display_utils.prepare_seek_stats_for_display(seek_stats)
        return disp_dict
    else:
        return "No Seeks"


def get_warn_warnings_json(warnings_mngr):
    warnings_info = calc_utils.get_warn_warnings_info(warnings_mngr)
    if warnings_info:
        disp_dict = \
            display_utils.prepare_warn_warnings_for_display(warnings_info)
        return disp_dict
    else:
        return "No Warnings"


def get_error_warnings_json(warnings_mngr):
    return display_utils.prepare_error_or_fatal_warnings_for_display(
        warnings_mngr, prepare_error=True)


def get_fatal_warnings_json(warnings_mngr):
    return display_utils.prepare_error_or_fatal_warnings_for_display(
        warnings_mngr, prepare_error=False)


def get_warnings_json(parsed_log):
    warnings_mngr = parsed_log.get_warnings_mngr()

    warnings_json = dict()
    warnings_json["Warnings"] = get_warn_warnings_json(warnings_mngr)
    warnings_json["Errors"] = get_error_warnings_json(warnings_mngr)
    warnings_json["Fatals"] = get_fatal_warnings_json(warnings_mngr)

    return warnings_json


def get_block_cache_json(parsed_log):
    cache_stats = \
        cache_utils.calc_block_cache_stats(
            parsed_log.get_database_options(),
            parsed_log.get_counters_and_histograms_mngr(),
            parsed_log.get_files_monitor())
    if cache_stats:
        stats_mngr = parsed_log.get_stats_mngr()
        detailed_block_cache_stats = \
            stats_mngr.get_block_cache_stats_mngr().get_all_cache_entries()

        display_stats = \
            display_utils.prepare_block_cache_stats_for_display(
                cache_stats, detailed_block_cache_stats)
        return display_stats
    else:
        return "No Block Cache Stats"


def get_filter_json(parsed_log):
    filter_stats = \
        calc_utils.calc_filter_stats(
            parsed_log.get_files_monitor(),
            parsed_log.get_counters_and_histograms_mngr())

    if filter_stats:
        display_stats = \
            display_utils.prepare_filter_stats_for_display(filter_stats)
        return display_stats
    else:
        return "No Filter Stats Available"


def get_json(parsed_log):
    j = dict()

    stats_mngr = parsed_log.get_stats_mngr()
    db_wide_stats_mngr = stats_mngr.get_db_wide_stats_mngr()
    compactions_stats_mngr = stats_mngr.get_compactions_stats_mngr()

    j["General"] = display_utils.prepare_db_wide_info_for_display(parsed_log)
    j["General"]["CF-s"] = \
        display_utils.prepare_general_cf_info_for_display(parsed_log)

    j["Options"] = {
        "Diff":
            display_utils.get_options_baseline_diff_for_display(parsed_log),
        "All Options": display_utils.get_all_options_for_display(parsed_log)
    }

    j["DB-Size"] = get_db_size_json(db_wide_stats_mngr, compactions_stats_mngr)

    j["Flushes"] = get_flushes_json(parsed_log)
    j["Compactions"] = get_compactions_json(parsed_log)
    j["Reads"] = get_reads_json(parsed_log)
    j["Seeks"] = get_seeks_json(parsed_log)
    j["Warnings"] = get_warnings_json(parsed_log)
    j["Block-Cache-Stats"] = get_block_cache_json(parsed_log)
    j["Filter"] = get_filter_json(parsed_log)

    return j


def get_json_dump_str(json_content):
    f = io.StringIO()
    json.dump(json_content, f, indent=1)
    return f.getvalue()


def write_json(json_file_name, json_content, output_folder, report_to_console):
    json_path = utils.get_json_file_path(output_folder, json_file_name)
    with json_path.open(mode='w') as json_file:
        json.dump(json_content, json_file)
    logging.info(f"JSON Output is in {json_path}")
    if report_to_console:
        print(f"JSON Output is in {json_path.as_uri()}")
