import re
import regexes
import defs_and_utils
import pathlib
import bisect
from log_file import ParsedLog
from database_options import DatabaseOptions, SANITIZED_NO_VALUE


def find_all_baseline_log_files(baselines_logs_folder, product_name):
    if product_name == defs_and_utils.ProductName.ROCKSDB:
        logs_regex = regexes.ROCKSDB_BASELINE_LOG_FILE_REGEX
    elif product_name == defs_and_utils.ProductName.SPEEDB:
        logs_regex = regexes.SPEEDB_BASELINE_LOG_FILE_REGEX
    else:
        assert False

    logs_folder_path = pathlib.Path(baselines_logs_folder)

    files = []
    for file_name in logs_folder_path.iterdir():
        file_match = re.findall(logs_regex, file_name.name)
        if file_match:
            assert len(file_match) == 1
            files.append(defs_and_utils.BaselineLogFileInfo(
                file_name.name, defs_and_utils.Version(file_match[0])))

    files.sort()
    return files


def find_closest_version_idx(baseline_versions, version):
    if isinstance(version, str):
        version = defs_and_utils.Version(version)

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


def sanitize_baseline_options(baseline_options):
    db_wide_options = ["statistics",
                       "env",
                       "info_log",
                       "write_buffer_manager"]
    cf_table_options = ["block_cache",
                        "flush_block_policy_factory",
                        "filter_policy"]

    for option_name in db_wide_options:
        baseline_options.set_db_wide_option(option_name, SANITIZED_NO_VALUE)
    for option_name in cf_table_options:
        baseline_options.set_cf_table_option(defs_and_utils.DEFAULT_CF_NAME,
                                             option_name,
                                             SANITIZED_NO_VALUE)


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
    baseline_log_path = defs_and_utils.get_file_path(
        baselines_logs_folder, closest_baseline_log_file_name)

    with baseline_log_path.open() as baseline_log_file:
        log_lines = baseline_log_file.readlines()
        log_lines = [line.strip() for line in log_lines]
        parsed_log = ParsedLog(str(baseline_log_path), log_lines)
        baseline_options = parsed_log.get_database_options()
        sanitize_baseline_options(baseline_options)
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
