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

INT_NC = r'[\d]+'
INT = r"([\d]+)"
FLOAT = r'([-+]?(?:\d+(?:[.,]\d*)?|[.,]\d+)(?:[eE][-+]?\d+)?)'
UNIT = r'(KB|MB|GB|TB)'
EMPTY_LINE_REGEX = r'^\s*$'
CF_NAME = r'\[(\w*?)\]'
CF_ID = fr'\(ID\s+{INT}\)'

TIMESTAMP_REGEX = r'\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2}\.\d{6}'
FILE_PATH_REGEX = r"\/?.*?\.[\w:]+"

ORIG_TIME_REGEX = fr"\(Original Log Time ({TIMESTAMP_REGEX})\)"
CODE_POS_REGEX = fr"\[{FILE_PATH_REGEX}:\d+\]"

START_LINE_WITH_WARN_PARTS_REGEX = \
    fr"({TIMESTAMP_REGEX}) (\w+)\s*(?:{ORIG_TIME_REGEX})?\s*" \
    fr"\[(WARN|ERROR|FATAL)\]\s*({CODE_POS_REGEX})?(.*)"
START_LINE_PARTS_REGEX = \
    fr"({TIMESTAMP_REGEX}) (\w+)\s*" \
    fr"(?:{ORIG_TIME_REGEX})?\s*({CODE_POS_REGEX})?(.*)"

# <product name> version: <version number>
PRODUCT_AND_VERSION_REGEX = r"(\S+) version: ([0-9.]+)"

GIT_HASH_LINE_REGEX = r"Git sha \s*(\S+)"

JOB_REGEX = r"\[JOB [0-9]+\]"

OPTION_LINE_REGEX = r"\s*Options\.(\S+)\s*:\s*(.+)?"
DB_WIDE_WBM_PSEUDO_OPTION_LINE_REGEX = r"\s*wbm\.(\S+)\s*:\s*(.+)"

# example:
# --------------- Options for column family [default]:
# In case of a match the result will be [<column-family name>] (List)
CF_OPTIONS_START_REGEX = \
    r"--------------- Options for column family \[(.*)\]:.*"

TABLE_OPTIONS_START_LINE_REGEX =\
    r"^\s*table_factory options:\s*(\S+)\s*:(.*)"
TABLE_OPTIONS_CONTINUATION_LINE_REGEX = r"^\s*(\S+)\s*:(.*)"

EVENT_REGEX = r"\s*EVENT_LOG_v1"

PREAMBLE_EVENT_REGEX = r"\[(.*?)\] \[JOB ([0-9]+)\]\s*(.*)"

STALLS_WARN_MSG_PREFIX = "Stalling writes"
STOPS_WARN_MSG_PREFIX = "Stopping writes"

#
# STATISTICS RELATED
#
DUMP_STATS_REGEX = r'------- DUMPING STATS -------'
DB_STATS_REGEX = r'^\s*\*\* DB Stats \*\*\s*$'
COMPACTION_STATS_REGEX = r'^\s*\*\* Compaction Stats\s*\[(.*)\]\s*\*\*\s*$'

FILE_READ_LATENCY_STATS_REGEX =\
    r'^\s*\*\* File Read Latency Histogram By Level\s*\[(.*)\]\s*\*\*\s*$'

LEVEL_READ_LATENCY_STATS_REGEX =\
    r'** Level ([0-9]+) read latency histogram (micros):'

STATS_COUNTERS_AND_HISTOGRAMS_REGEX = r'^\s*STATISTICS:\s*$'

UPTIME_STATS_LINE_REGEX =\
    r'^\s*Uptime\(secs\):\s*([0-9\.]+)\s*total,'\
    r'\s*([0-9\.]+)\s*interval\s*$'

CACHE_ID = r"(\S+)"
BLOCK_CACHE_STATS_START_REGEX = \
    fr'Block cache {CACHE_ID} capacity: {FLOAT} {UNIT} '

BLOCK_CACHE_ENTRY_STATS_REGEX = \
    r"Block cache entry stats\(count,size,portion\): (.*)"

BLOCK_CACHE_CF_ENTRY_STATS_REGEX = fr"Block cache {CF_NAME} (.*)"

BLOCK_CACHE_ENTRY_ROLES_NAMES_REGEX = r"([A-Za-z]+)\("

BLOCK_CACHE_ENTRY_ROLES_STATS = r"[a-zA-Z]+\(([^\)]+?)\)"

STATS_COUNTER_REGEX = r"^\s*([\w\.]+) COUNT : (\d+)\s*$"

STATS_HISTOGRAM_REGEX = \
    fr'^\s*([\w\.]+) P50 : {FLOAT} P95 : {FLOAT} P99 : {FLOAT} '\
    fr'P100 : {FLOAT} COUNT : {INT} SUM : {INT}'

BLOB_STATS_LINE_REGEX = \
    fr'Blob file count: ([\d]+), total size: {FLOAT} GB, '\
    fr'garbage size: {FLOAT} GB, space amp: {FLOAT}'

SUPPORT_INFO_START_LINE_REGEX = r'\s*Compression algorithms supported:\s*$'

VERSION_REGEX = r'(\d+)\.(\d+)\.?(\d+)?'

ROCKSDB_BASELINE_LOG_FILE_REGEX = r"LOG-rocksdb-(\d+\.\d+\.?\d*)"
SPEEDB_BASELINE_LOG_FILE_REGEX = r"LOG-speedb-(\d+\.\d+\.?\d*)"

DB_WIDE_INTERVAL_STALL_REGEX = \
    fr"Interval stall: (\d+):(\d+):(\d+)\.(\d+) H:M:S, {FLOAT} percent"

DB_WIDE_CUMULATIVE_STALL_REGEX = \
    fr"Cumulative stall: (\d+):(\d+):(\d+)\.(\d+) H:M:S, {FLOAT} percent"

CF_STALLS_LINE_START = "Stalls(count):"
CF_STALLS_COUNT_AND_REASON_REGEX = r"\b(\d+) (.*?),"
CF_STALLS_INTERVAL_COUNT_REGEX = r".*interval (\d+) total count$"

DB_SESSION_ID_REGEX = r"DB Session ID:\s*([0-9A-Z]+)"

RECOVERED_CF_REGEX = fr"Column family {CF_NAME}\s*{CF_ID}"
CREATE_CF_REGEX = fr"Created column family {CF_NAME}\s*{CF_ID}"
DROP_CF_REGEX = fr"Dropped column family with id {INT}\s*"
