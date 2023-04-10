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
from dataclasses import dataclass, asdict

import baseline_log_files_utils
import cache_utils
import calc_utils
import database_options
import db_files
import utils
from database_options import DatabaseOptions, CfsOptionsDiff, SectionType
from database_options import SanitizedValueType
from stats_mngr import CompactionStatsMngr
from warnings_mngr import WarningType, WarningElementInfo, WarningsMngr

num_for_display = utils.get_human_readable_number
num_bytes_for_display = utils.get_human_readable_num_bytes


def prepare_db_wide_user_opers_stats_for_display(db_wide_info):
    display_info = {}

    total_num_user_opers = db_wide_info["total_num_user_opers"]
    total_num_flushed_entries = db_wide_info['total_num_flushed_entries']

    if total_num_user_opers > 0:
        display_info['Writes'] = \
            f"{db_wide_info['percent_written']} " \
            f"({db_wide_info['num_written']}/{total_num_user_opers})"
        display_info['Reads'] = \
            f"{db_wide_info['percent_read']} " \
            f"({db_wide_info['num_read']}/{total_num_user_opers})"
        display_info['Seeks'] = \
            f"{db_wide_info['percent_seek']} " \
            f"({db_wide_info['num_seek']}/{total_num_user_opers})"
    else:
        display_info['Writes'] = "Not Available"
        display_info['Reads'] = "Not Available"
        display_info['Seeks'] = "Not Available"

    if total_num_flushed_entries > 0:
        display_info['Deletes'] = \
            f"{db_wide_info['total_percent_deletes']} " \
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


def prepare_error_or_fatal_warnings_for_display(warnings_mngr, prepare_error):
    assert isinstance(warnings_mngr, WarningsMngr)

    if prepare_error:
        all_type_warnings = \
            warnings_mngr.get_warnings_of_type(WarningType.ERROR)
        if not all_type_warnings:
            return "No Errors"
    else:
        all_type_warnings = \
            warnings_mngr.get_warnings_of_type(WarningType.FATAL)
        if not all_type_warnings:
            return "No Fatals"

    warnings_tuples = list()
    for cf_name, cf_info in all_type_warnings.items():
        for category, err_infos in cf_info.items():
            for info in err_infos:
                assert isinstance(info, WarningElementInfo)
                warnings_tuples.append((info.time, info.warning_msg))

    # First item in every tuple is time => will be sorted on time
    warnings_tuples.sort()

    return {time: msg for time, msg in warnings_tuples}


def prepare_db_wide_info_for_display(parsed_log):
    display_info = {}

    db_wide_info = calc_utils.get_db_wide_info(parsed_log)
    db_wide_notable_entities = \
        get_db_wide_notable_entities_display_info(parsed_log)
    display_info["Name"] = parsed_log.get_log_file_path()
    display_info["Creator"] = db_wide_info['creator']
    display_info["Version"] = f"{db_wide_info['version']} " \
                              f"[{db_wide_info['git_hash']}]"
    display_info["DB Size"] = \
        num_bytes_for_display(db_wide_info['db_size_bytes'])
    display_info["Num Keys Written"] = \
        num_for_display(db_wide_info['num_keys_written'])
    display_info["Avg. Written Key Size"] = \
        num_bytes_for_display(db_wide_info['avg_key_size_bytes'])
    display_info["Avg. Written Value Size"] = \
        num_bytes_for_display(db_wide_info['avg_value_size_bytes'])
    display_info["Num Warnings"] = db_wide_info['num_warnings']
    display_info["Num Errors"] = db_wide_info['num_errors']
    display_info["Num Fatals"] = db_wide_info['num_fatals']
    display_info.update(db_wide_notable_entities)
    display_info.update(prepare_db_wide_user_opers_stats_for_display(
        db_wide_info))
    display_info['Num CF-s'] = db_wide_info['num_cfs']

    return display_info


def prepare_general_cf_info_for_display(parsed_log):
    display_info = {}

    cf_names = parsed_log.get_cfs_names_that_have_options()
    events_mngr = parsed_log.get_events_mngr()
    compaction_stats_mngr = \
        parsed_log.get_stats_mngr().get_compactions_stats_mngr()

    for i, cf_name in enumerate(cf_names):
        table_creation_stats = \
            calc_utils.calc_cf_table_creation_stats(cf_name, events_mngr)
        cf_options = calc_utils.get_applicable_cf_options(parsed_log)
        cf_size_bytes = compaction_stats_mngr.get_cf_size_bytes_at_end(cf_name)

        display_info[cf_name] = {}
        cf_display_info = display_info[cf_name]

        cf_display_info["CF Size"] = num_bytes_for_display(cf_size_bytes)
        cf_display_info["Avg. Key Size"] = \
            num_bytes_for_display(table_creation_stats['avg_key_size'])
        cf_display_info["Avg. Value Size"] = \
            num_bytes_for_display(table_creation_stats['avg_value_size'])

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


def prepare_warn_warnings_for_display(warn_warnings_info):
    # The input is in the following format:
    # {<warning-type>: {<cf-name>: {<category>: <number of messages>}}

    disp = dict()

    for cf_name, cf_info in warn_warnings_info.items():
        if cf_name == utils.NO_CF:
            disp_cf_name = "DB"
        else:
            disp_cf_name = cf_name

        disp[disp_cf_name] = dict()
        for category, num_in_category in cf_info.items():
            disp_category = category.value
            disp[disp_cf_name][disp_category] = num_in_category

    if not disp:
        return None

    return disp


def get_all_options_for_display(parsed_log):
    all_options = {}
    cf_names = parsed_log.get_cfs_names()
    db_options = parsed_log.get_database_options()

    all_options["DB"] = db_options.get_db_wide_options_for_display()

    cfs_options = {}
    for cf_name in cf_names:
        cf_options = db_options.get_cf_options(cf_name)
        cfs_options[cf_name] = cf_options

    common_cfs_options, unique_cfs_options = \
        db_options.get_unified_cfs_options(cfs_options)

    if common_cfs_options:
        options, table_options = \
            DatabaseOptions.prepare_flat_full_names_cf_options_for_display(
                common_cfs_options, None)
        all_options["CF-s (Common)"] = {
            "CF": options,
            "Table": table_options
        }

    if unique_cfs_options:
        all_options["CF-s"] = {}
        for cf_name, cf_options in unique_cfs_options.items():
            if cf_options:
                all_options["CF-s"][cf_name] = \
                    DatabaseOptions.\
                    prepare_flat_full_names_cf_options_for_display(
                        cf_options, None)
        if not all_options["CF-s"]:
            del all_options["CF-s"]

    return all_options


def get_diff_tuple_for_display(raw_diff_tuple):
    return {utils.DIFF_BASELINE_NAME: raw_diff_tuple[0],
            utils.DIFF_LOG_NAME: raw_diff_tuple[1]}


def prepare_db_wide_diff_dict_for_display(product_name, baseline_version,
                                          db_wide_diff):
    display_db_wide_diff = {"Baseline":
                            f"{str(baseline_version)} ({product_name})"}

    if db_wide_diff is None:
        display_db_wide_diff["DB"] = "No Diff"
        return display_db_wide_diff

    del (db_wide_diff[CfsOptionsDiff.CF_NAMES_KEY])
    display_db_wide_diff["DB"] = {}

    for full_option_name in db_wide_diff:
        section_type = SectionType.extract_section_type(full_option_name)
        option_name = \
            database_options.extract_option_name(full_option_name)

        if section_type == SectionType.DB_WIDE:
            display_db_wide_diff["DB"][option_name] = \
                get_diff_tuple_for_display(db_wide_diff[full_option_name])
        elif section_type == SectionType.VERSION:
            pass
        else:
            assert False, "Unexpected section type"

    if not display_db_wide_diff["DB"]:
        del (display_db_wide_diff["DB"])
    if list(display_db_wide_diff.keys()) == ["Baseline"]:
        display_db_wide_diff = {}

    return display_db_wide_diff


def prepare_cfs_diff_dict_for_display(baseline_opts, log_opts, cfs_names):
    display_cfs_diff = {}

    cfs_diffs = []
    for log_cf_name in cfs_names:
        cf_diff = DatabaseOptions.get_cfs_options_diff(
            baseline_opts, utils.DEFAULT_CF_NAME, log_opts,
            log_cf_name).get_diff_dict()
        cfs_diffs.append(cf_diff)

        common_cfs_diffs, unique_cfs_diffs = \
            DatabaseOptions.get_unified_cfs_diffs(cfs_diffs)

        if common_cfs_diffs:
            options, table_options = \
                DatabaseOptions.prepare_flat_full_names_cf_options_for_display(
                    common_cfs_diffs, get_diff_tuple_for_display)
            display_cfs_diff["CF-s (Common)"] = {
                "CF": options,
                "Table": table_options
            }

        display_cfs_diff["CF-s"] = {}
        for cf_diff in unique_cfs_diffs:
            if len(list(cf_diff.keys())) > 1:
                cf_name = cf_diff[CfsOptionsDiff.CF_NAMES_KEY]["New"]
                del(cf_diff[CfsOptionsDiff.CF_NAMES_KEY])
                options, table_options = \
                    DatabaseOptions.\
                    prepare_flat_full_names_cf_options_for_display(
                        cf_diff, get_diff_tuple_for_display)

                display_cfs_diff["CF-s"][cf_name] = {
                    "CF": options,
                    "Table": table_options
                }
        if not display_cfs_diff["CF-s"]:
            del(display_cfs_diff["CF-s"])

    return display_cfs_diff


def get_options_baseline_diff_for_display(parsed_log):
    log_metadata = parsed_log.get_metadata()
    log_database_options = parsed_log.get_database_options()

    baseline_info = baseline_log_files_utils.get_baseline_database_options(
        utils.BASELINE_LOGS_FOLDER,
        log_metadata.get_product_name(),
        log_metadata.get_version())

    if baseline_info is None:
        return "NO BASELINE FOUND"

    baseline_database_options, baseline_version = baseline_info
    baseline_opts = baseline_database_options.get_all_options()

    log_opts = log_database_options.get_all_options()

    db_wide_diff = \
        DatabaseOptions.get_db_wide_options_diff(baseline_opts, log_opts)
    if db_wide_diff is not None:
        db_wide_diff = db_wide_diff.get_diff_dict()
    display_diff = prepare_db_wide_diff_dict_for_display(
        log_metadata.get_product_name(), baseline_version, db_wide_diff)

    cfs_names = log_database_options.get_cfs_names()
    cfs_diff = prepare_cfs_diff_dict_for_display(baseline_opts, log_opts,
                                                 cfs_names)
    if cfs_diff:
        display_diff["CF-s"] = cfs_diff

    return display_diff


def prepare_cf_flushes_stats_for_display(parsed_log):
    disp = {}

    def calc_sizes_histogram():
        sizes_histogram = {}
        bucket_min_size_mb = 0
        for i, num_in_bucket in \
                enumerate(reason_stats["sizes_histogram"]):
            if i < len(calc_utils.FLUSHED_SIZES_HISTOGRAM_BUCKETS_MB):
                bucket_max_size_mb = \
                    calc_utils.FLUSHED_SIZES_HISTOGRAM_BUCKETS_MB[i]
                bucket_title = f"{bucket_min_size_mb} - " \
                               f"{bucket_max_size_mb} [MB]"
            else:
                bucket_title = f"> {bucket_min_size_mb} [MB]"
            bucket_min_size_mb = bucket_max_size_mb

            sizes_histogram[bucket_title] = num_in_bucket
        return sizes_histogram

    def get_write_amp_level1():
        cf_compaction_stats =\
            compactions_stats_mngr.get_cf_level_entries(cf_name)
        if not cf_compaction_stats:
            return None
        last_dump_stats = cf_compaction_stats[-1]

        return CompactionStatsMngr.get_level_field_value(
            last_dump_stats, level=1,
            field=CompactionStatsMngr.LevelFields.WRITE_AMP)

    cf_names = parsed_log.get_cfs_names()
    events_mngr = parsed_log.get_events_mngr()
    stats_mngr = parsed_log.get_stats_mngr()
    compactions_stats_mngr = stats_mngr.get_compactions_stats_mngr()

    for cf_name in cf_names:
        cf_flushes_stats = calc_utils.calc_cf_flushes_stats(cf_name,
                                                            events_mngr)
        if cf_flushes_stats:
            write_amp_level1 = get_write_amp_level1()
            if not write_amp_level1:
                write_amp_level1 = "No Write Amp for Level1"
            for reason_stats in cf_flushes_stats.values():
                reason_stats["sizes_histogram"] = calc_sizes_histogram()

                reason_stats["min_total_data_size_bytes"] = \
                    num_bytes_for_display(
                        reason_stats["min_total_data_size_bytes"])
                reason_stats["max_total_data_size_bytes"] = \
                    num_bytes_for_display(
                        reason_stats["max_total_data_size_bytes"])
            cf_flushes_stats["Write-Amp"] = write_amp_level1
            disp[cf_name] = cf_flushes_stats

    return disp


def prepare_global_compactions_stats_for_display(parsed_log):
    disp = {}
    compactions_monitor = parsed_log.get_compactions_monitor()
    largest_compaction_size_bytes = \
        calc_utils.get_largest_compaction_size_bytes(compactions_monitor)
    disp["largest compaction size"] = \
        num_bytes_for_display(largest_compaction_size_bytes)
    return disp


def prepare_cf_compactions_stats_for_display(parsed_log):
    disp = {}

    cf_names = parsed_log.get_cfs_names()
    for cf_name in cf_names:
        cf_compactions_stats = \
            calc_utils.calc_cf_compactions_stats(cf_name, parsed_log)
        if cf_compactions_stats:
            disp[cf_name] = cf_compactions_stats

    return disp


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


def prepare_cfs_size_bytes_growth_for_display(growth):
    disp = {}
    if not growth:
        return "All CF-s Empty"

    for cf_name in growth:
        disp[cf_name] = {}

        if not growth[cf_name]:
            disp[cf_name] = "Empty Column-Family"
            continue

        for level, sizes_bytes in growth[cf_name].items():
            start_size_bytes = sizes_bytes[0]
            end_size_bytes = sizes_bytes[1]

            if start_size_bytes is None:
                start_size_bytes = 0
            start_size_str = num_bytes_for_display(start_size_bytes)

            if end_size_bytes is not None:
                if start_size_bytes == end_size_bytes:
                    if start_size_bytes > 0:
                        value_str = f"{start_size_str} (No Change)"
                    else:
                        value_str = "Empty Level"
                else:
                    end_size_str = num_bytes_for_display(end_size_bytes)
                    delta = end_size_bytes - start_size_bytes
                    abs_delta_str = num_bytes_for_display(abs(delta))
                    if delta >= 0:
                        delta_str = f"(+{abs_delta_str})"
                    else:
                        delta_str = f"(-{abs_delta_str})"
                    value_str = \
                        f"{start_size_str} -> {end_size_str}  {delta_str}"
            else:
                value_str = f"{start_size_str} -> 0"

            disp[cf_name][f"Level {level}"] = value_str

    return disp


def prepare_db_ingest_info_for_display(ingest_info):
    disp = {}
    if not ingest_info:
        return "No Ingest Info"

    disp["ingest"] = utils.get_human_readable_number(ingest_info.ingest)
    disp["ingest-rate"] = f"{ingest_info.ingest_rate_mbps} MBps"

    return disp


def prepare_seek_stats_for_display(seek_stats):
    assert isinstance(seek_stats, calc_utils.SeekStats)

    disp = dict()
    disp["Num Seeks"] = num_for_display(seek_stats.num_seeks)
    disp["Num Found Seeks"] = num_for_display(seek_stats.num_found_seeks)
    disp["Num Nexts"] = num_for_display(seek_stats.num_nexts)
    disp["Num Prevs"] = num_for_display(seek_stats.num_prevs)
    disp["Avg. Seek Range Size"] = f"{seek_stats.avg_seek_range_size:.1f}"
    disp["Avg. Seeks Rate Per Second"] = \
        f"{seek_stats.avg_seek_rate_per_second:.1f}"
    disp["Avg. Seek Latency"] = f"{seek_stats.avg_seek_latency_us:.1f} us"

    return disp


def prepare_cache_id_options_for_display(options):
    assert isinstance(options, cache_utils.CacheOptions)

    disp = dict()

    disp["Capacity"] = num_bytes_for_display(options.cache_capacity_bytes)
    disp["Num Shards"] = 2 ** options.num_shard_bits
    disp["Shard Size"] = num_bytes_for_display(options.shard_size_bytes)
    disp["CF-s"] = \
        {cf_name: asdict(cf_options) for cf_name, cf_options in
         options.cfs_specific_options.items()}

    return disp


def prepare_block_stats_of_cache_for_display(block_stats):
    assert isinstance(block_stats, db_files.BlockStats)

    disp = dict()

    disp["Avg. Size"] = num_for_display(int(block_stats.avg_size_bytes))
    disp["Max Size"] = num_for_display(block_stats.max_size_bytes)
    disp["Total Size (All Blocks)"] = \
        num_for_display(block_stats.max_size_bytes)

    return disp


def prepare_block_cache_info_for_display(cache_info):
    assert isinstance(cache_info, cache_utils.CacheIdInfo)

    disp = dict()
    disp.update(prepare_cache_id_options_for_display(cache_info.options))
    disp["Index Block"] = \
        prepare_block_stats_of_cache_for_display(cache_info.files_stats.index)
    disp["Filter Block"] = \
        prepare_block_stats_of_cache_for_display(cache_info.files_stats.filter)
    return disp


def prepare_block_counters_for_display(cache_counters):
    assert isinstance(cache_counters, cache_utils.CacheCounters)

    disp = asdict(cache_counters)
    disp = {key: num_for_display(value) for key, value in disp.items()}
    return disp


# TODO: The detailed display should be moved to its own csv
# TODO: The cache id in the detailed should not include the process id so it
#  matches the non-detailed display
def prepare_detailed_block_cache_stats_for_display(detailed_block_cache_stats):
    disp_stats = detailed_block_cache_stats.copy()
    for cache_stats in disp_stats.values():
        cache_stats['Capacity'] = num_for_display(cache_stats['Capacity'])
        cache_stats['Usage'] = num_for_display(cache_stats['Usage'])
        for cache_stats_key, entry in cache_stats.items():
            if utils.parse_time_str(cache_stats_key, expect_valid_str=False):
                entry['Usage'] = num_for_display(entry['Usage'])
                for entry_key, role_values in entry.items():
                    if entry_key == 'CF-s':
                        for cf_name in entry['CF-s']:
                            for role, cf_role_value in \
                                    entry['CF-s'][cf_name].items():
                                entry['CF-s'][cf_name][role] =\
                                    num_for_display(cf_role_value)
                    elif entry_key != 'Usage':
                        role_values['Size'] =\
                            num_for_display(role_values['Size'])
    return disp_stats


def prepare_block_cache_stats_for_display(cache_stats,
                                          detailed_block_cache_stats):
    assert isinstance(cache_stats, cache_utils.CacheStats)

    disp = dict()
    if cache_stats.per_cache_id_info:
        disp["Caches"] = {}
        for cache_id, cache_info in cache_stats.per_cache_id_info.items():
            disp["Caches"][cache_id] = \
                prepare_block_cache_info_for_display(cache_info)

    if cache_stats.global_cache_counters:
        disp["DB Counters"] = \
            prepare_block_counters_for_display(
                cache_stats.global_cache_counters)
    else:
        disp["DB Counters"] = "No Counters"

    detailed_disp_block_cache_stats = None
    if detailed_block_cache_stats:
        detailed_disp_block_cache_stats = \
            prepare_detailed_block_cache_stats_for_display(
                detailed_block_cache_stats)
    if detailed_disp_block_cache_stats:
        disp["Detailed"] = detailed_disp_block_cache_stats
    else:
        disp["Detailed"] = "No Detailed Block Cache Stats Available"

    return disp


def prepare_filter_stats_for_display(filter_stats):
    assert isinstance(filter_stats, calc_utils.FilterStats)

    disp = {}

    if filter_stats.files_filter_stats:
        disp["CF-s"] = {}
        for cf_name, cf_stats in filter_stats.files_filter_stats.items():
            assert isinstance(cf_stats, calc_utils.CfFilterFilesStats)
            if cf_stats.filter_policy:
                cf_disp_stats = {
                    "Filter-Policy": cf_stats.filter_policy,
                    "Avg. BPK": f"{cf_stats.avg_bpk:.1f}",
                    "Max Filter Size":
                        num_bytes_for_display(cf_stats.max_filter_size_bytes)
                }
            else:
                cf_disp_stats = "No Filter"

            disp["CF-s"][cf_name] = cf_disp_stats
    else:
        disp["CF-s"] = "No Filters used In SST-s"

    if filter_stats.filter_counters:
        disp_counters = {
            "False-Positive-Rate":
                f"1 in {filter_stats.filter_counters.one_in_n_fpr}",
            "False-Positives":
                num_for_display(filter_stats.filter_counters.false_positives),
            "Negatives":
                num_for_display(filter_stats.filter_counters.negatives),
            "True-Positives":
                num_for_display(filter_stats.filter_counters.true_positives)
        }
        disp["Counters"] = disp_counters
    else:
        disp["Counters"] = "No Filter Counters Available"

    return disp
