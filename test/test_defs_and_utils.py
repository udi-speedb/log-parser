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

import defs_and_utils


def test_compare_times():
    time1 = "2022/11/24-15:50:09.512106"
    time2 = "2022/11/24-15:51:09.512106"

    assert defs_and_utils.compare_times(time1, time1) == 0
    assert defs_and_utils.compare_times(time1, time2) < 0
    assert defs_and_utils.compare_times(time2, time1) > 0


def test_get_table_options_topic_info():
    assert defs_and_utils.get_table_options_topic_info(
        "metadata_cache_options") == ("metadata_cache_options",
                                      "metadata_cache_")
    assert defs_and_utils.get_table_options_topic_info(
        "block_cache_options") == ("block_cache_options", "block_cache_")

    assert defs_and_utils.get_table_options_topic_info("block_cache") is None
