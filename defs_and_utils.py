import datetime
from enum import Enum, auto
import re
import time
from calendar import timegm
from typing import Optional
from dataclasses import dataclass
import pathlib
import regexes


NO_COL_FAMILY = 'DB_WIDE'
LOGGER_NAME = "log-analyzer-logger"
BASELINE_LOGS_FOLDER = "baseline_logs"
ARTEFACTS_FOLDER = "artefacts"
DEFAULT_LOG_FILE_NAME = "log_parser.log"
DEFAULT_COUNTERS_FILE_NAME = "counters.csv"
DEFAULT_HISTOGRAMS_FILE_NAME = "histograms.csv"
DEFAULT_COMPACTION_STATS_FILE_NAME = "compaction.csv"
DEFAULT_CF_NAME = "default"


@dataclass
class ErrorContext:
    file_path: Optional[str] = None
    log_line_idx: Optional[int] = None
    log_line: Optional[str] = None

    def __str__(self):
        file_path = self.file_path if self.file_path is not None else "?"
        line_num = self.log_line_idx + 1 \
            if self.log_line_idx is not None else "?"

        result_str = f"[File:{file_path} (line#:{line_num})]"
        if self.log_line is not None:
            result_str += f"\n{self.log_line}"
        return result_str


def get_error_context_from_entry(entry, file_path=None):
    error_context = ErrorContext()
    if not error_context:
        error_context = ErrorContext()
    if file_path:
        error_context.file_path = file_path
    error_context.log_line = entry.get_msg()
    error_context.log_line_idx = entry.get_start_line_idx()
    return error_context


def format_err_msg(msg, error_context=None, entry=None, file_path=None):
    if entry:
        error_context = get_error_context_from_entry(entry, file_path)

    result_str = msg
    if error_context is not None:
        result_str += " - " + str(error_context)
    return result_str


class ParsingError(Exception):
    def __init__(self, msg, error_context=None):
        self.msg = msg
        self.context = error_context

    def __str__(self):
        result_str = self.msg
        if self.context is not None:
            result_str += str(self.context)
        return result_str

    def set_context(self, error_context):
        self.context = error_context


class ParsingAssertion(ParsingError):
    def __init__(self, msg, error_context=None):
        super().__init__(msg, error_context)


class LogFileNotFoundError(Exception):
    def __init__(self, file_path):
        self.msg = f"{file_path} Not Found"


class PointerResult(Enum):
    POINTER = auto()
    NULL_POINTER = auto()
    NOT_A_POINTER = auto()


class WarningType(str, Enum):
    WARN = "WARN"
    ERROR = "ERROR"
    FATAL = "FATAL"


def get_type(warning_type_str):
    return WarningType(warning_type_str)


class ConsoleOutputType(str, Enum):
    SHORT = "short"
    FULL = "full"


def try_parse_pointer(value_str):
    value_str = value_str.strip()
    if value_str == "(nil)":
        return PointerResult.NULL_POINTER
    else:
        match = re.findall(r'0x[\dA-Fa-f]+', value_str)
        return PointerResult.POINTER if len(match) == 1 else \
            PointerResult.NOT_A_POINTER


def get_gmt_timestamp(time_str):
    # example: '2018/07/25-11:25:45.782710' will be converted to the GMT
    # Unix timestamp 1532517945 (note: this method assumes that self.time
    # is in GMT)
    hr_time = time_str + 'GMT'
    return timegm(time.strptime(hr_time, "%Y/%m/%d-%H:%M:%S.%f%Z"))


def compare_times(time1_str, time2_str):
    time1_gmt = get_gmt_timestamp(time1_str)
    time2_gmt = get_gmt_timestamp(time2_str)
    diff = time1_gmt - time2_gmt
    if diff < 0:
        return -1
    elif diff > 0:
        return 1
    else:
        return 0


def parse_date_time(date_time_str):
    try:
        return datetime.datetime.strptime(date_time_str,
                                          '%Y/%m/%d-%H:%M:%S.%f')
    except ValueError:
        return None


def get_value_by_unit(size_str, size_units_str):
    size_units_str = size_units_str.strip()

    multiplier = 1
    if size_units_str == "KB" or size_units_str == "K":
        multiplier = 2 ** 10
    elif size_units_str == "MB" or size_units_str == "M":
        multiplier = 2 ** 20
    elif size_units_str == "GB" or size_units_str == "G":
        multiplier = 2 ** 30
    elif size_units_str == "TB" or size_units_str == "T":
        multiplier = 2 ** 40
    elif size_units_str != '':
        assert False, f"Unexpected size units ({size_units_str}"

    result = float(size_str) * multiplier
    return int(result)


def get_value_by_size_with_unit(size_with_unit_str):
    size, size_unit = size_with_unit_str.split()
    return get_value_by_unit(size, size_unit)


def get_size_for_display(size_in_bytes):
    if size_in_bytes < 2 ** 10:
        return str(size_in_bytes) + " B"
    elif size_in_bytes < 2 ** 20:
        size_units_str = "KB"
        divider = 2 ** 10
    elif size_in_bytes < 2 ** 30:
        size_units_str = "MB"
        divider = 2 ** 20
    elif size_in_bytes < 2 ** 40:
        size_units_str = "GB"
        divider = 2 ** 30
    else:
        size_units_str = "TB"
        divider = 2 ** 40

    return f"{float(size_in_bytes) / divider:.1f} {size_units_str}"


class ProductName(str, Enum):
    ROCKSDB = "RocksDB"
    SPEEDB = "Speedb"

    def __eq__(self, other):
        return self.lower() == other.lower()


@dataclass
class Version:
    major: int
    minor: int
    patch: int

    def __init__(self, version_str):
        version_parts = re.findall(regexes.VERSION_REGEX, version_str)
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


def get_line_num_from_entry(entry, rel_line_idx=None):
    line_idx = entry.get_start_line_idx()
    if rel_line_idx is not None:
        line_idx += rel_line_idx
    return line_idx + 1


def format_line_num_from_line_idx(line_idx):
    return f"[line# {line_idx + 1}]"


def format_line_num_from_entry(entry, rel_line_idx=None):
    line_idx = entry.get_start_line_idx()
    if rel_line_idx is not None:
        line_idx += rel_line_idx
    return format_line_num_from_line_idx(line_idx)


def format_lines_range_from_entries(start_entry, end_entry):
    result = "[lines#"
    result += f"{start_entry.get_start_line_idx()+1}"
    result += "-"
    result += f"{end_entry.get_end_line_idx()+1}"
    result += "]"
    return result


def format_lines_range_from_entries_idxs(log_entries, start_idx, end_idx):
    return format_lines_range_from_entries(log_entries[start_idx],
                                           log_entries[end_idx])


def get_file_path(file_folder_name, file_basename):
    return pathlib.Path(f"{file_folder_name}/{file_basename}")


def get_json_file_path(json_file_name):
    return get_file_path(ARTEFACTS_FOLDER, json_file_name)


def get_default_counters_csv_file_path():
    return get_file_path(ARTEFACTS_FOLDER, DEFAULT_COUNTERS_FILE_NAME)


def get_default_histograms_csv_file_path():
    return get_file_path(ARTEFACTS_FOLDER, DEFAULT_HISTOGRAMS_FILE_NAME)


def get_default_compaction_stats_csv_file_path():
    return get_file_path(ARTEFACTS_FOLDER, DEFAULT_COMPACTION_STATS_FILE_NAME)
