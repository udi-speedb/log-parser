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

import calc_utils
import test.test_utils as test_utils
from test.sample_log_info import SampleLogInfo


def test_get_cf_size_bytes():
    parsed_log = test_utils.create_parsed_log(SampleLogInfo.FILE_PATH)

    for cf_name in SampleLogInfo.CF_NAMES:
        actual_size_bytes = calc_utils.get_cf_size_bytes(parsed_log, cf_name)
        assert actual_size_bytes == SampleLogInfo.CF_SIZE_BYTES[cf_name]


def test_get_db_size_bytes():
    parsed_log = test_utils.create_parsed_log(SampleLogInfo.FILE_PATH)

    actual_size_bytes = calc_utils.get_db_size_bytes(parsed_log)
    assert actual_size_bytes == SampleLogInfo.DB_SIZE_BYTES


def test_calc_all_events_histogram():
    parsed_log = test_utils.create_parsed_log(SampleLogInfo.FILE_PATH)

    events_histogram = calc_utils.calc_all_events_histogram(
        SampleLogInfo.CF_NAMES, parsed_log.get_events_mngr())
    assert events_histogram == SampleLogInfo.EVENTS_HISTOGRAM
