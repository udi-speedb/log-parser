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

import pytest

import database_options
import utils
from database_options import DatabaseOptions, FullNamesOptionsDict, \
    SectionType, OptionsDiff, CfsOptionsDiff, SANITIZED_NO_VALUE, \
    RAW_NULL_PTR, SANITIZED_NULL_PTR, sanitized_to_raw_ptr_value

default = 'default'
cf1 = 'cf1'
cf2 = 'cf2'
cf3 = 'cf3'

EMPTY_FULL_NAMES_OPTIONS_DICT = FullNamesOptionsDict()
DB_WIDE_CF_NAME = utils.NO_CF


baseline_options = {
    'DBOptions.stats_dump_freq_sec': {utils.NO_CF: '20'},
    'DBOptions.enable_pipelined_write': {utils.NO_CF: '0'},
    'CFOptions.write_buffer_size': {
        default: '1024000',
        cf1: '128000',
        cf2: '128000000'
    },
    'TableOptions.BlockBasedTable.index_type': {cf2: '1'},
    'TableOptions.BlockBasedTable.format_version': {cf1: '4', cf2: '5'},
    'TableOptions.BlockBasedTable.block_size': {cf1: '4096', cf2: '4096'},
    'DBOptions.use_fsync': {utils.NO_CF: 'true'},
    'DBOptions.max_log_file_size':
        {utils.NO_CF: '128000000'},
    'DBOptions.ptr_option1': {utils.NO_CF: '0x12345678'}
}
new_options = {
    'bloom_bits': {utils.NO_CF: '4'},
    'DBOptions.enable_pipelined_write': {utils.NO_CF: 'True'},
    'CFOptions.write_buffer_size': {
        default: '128000000',
        cf1: '128000',
        cf3: '128000000'
    },
    'TableOptions.BlockBasedTable.checksum': {cf1: 'true'},
    'TableOptions.BlockBasedTable.format_version': {cf1: '5', cf2: '5'},
    'TableOptions.BlockBasedTable.block_size': {cf1: '4096', cf2: '4096'},
    'DBOptions.use_fsync': {utils.NO_CF: 'true'},
    'DBOptions.max_log_file_size': {utils.NO_CF: '0'},
    'DBOptions.ptr_option1': {utils.NO_CF: '0x55555555'}
}

baseline = FullNamesOptionsDict(baseline_options)
new = FullNamesOptionsDict(new_options)


def test_sanitized_to_raw_ptr_value():
    assert sanitized_to_raw_ptr_value(SANITIZED_NULL_PTR) == RAW_NULL_PTR
    assert sanitized_to_raw_ptr_value("Pointer (0x1234)") == '0x1234'
    assert sanitized_to_raw_ptr_value("Pointer (0xa0FAA55)") == '0xa0FAA55'


def test_extract_section_type():
    assert SectionType.extract_section_type("Version.option-name1") == \
           "Version"
    assert SectionType.extract_section_type("DBOptions.option-name1") == \
           "DBOptions"
    assert SectionType.extract_section_type("CFOptions.option-name1") == \
           "CFOptions"
    assert SectionType.extract_section_type(
        "TableOptions.BlockBasedTable.option-name1") ==\
        "TableOptions.BlockBasedTable"

    with pytest.raises(utils.ParsingError):
        SectionType.extract_section_type("Dummy.option-name1")


def test_misc_option_name_utils():
    # parse_full_option_name
    assert database_options.parse_full_option_name("Version.option-name1") \
           == ("Version", "option-name1")
    with pytest.raises(utils.ParsingError):
        SectionType.extract_section_type("Dummy.option-name1")

    # get_full_option_name
    assert database_options.get_full_option_name("DBOptions", "option1") == \
           "DBOptions.option1"
    with pytest.raises(utils.ParsingError):
        database_options.get_full_option_name("Dummy", "option-name1")

    # get_db_wide_full_option_name
    assert database_options.get_db_wide_full_option_name("option1") == \
           "DBOptions.option1"

    # extract_option_name
    assert database_options.extract_option_name("DBOptions.option1") == \
           "option1"
    with pytest.raises(utils.ParsingError):
        database_options.extract_option_name("Dummy.option-name1")

    # extract_db_wide_option_name
    assert database_options.extract_db_wide_option_name("DBOptions.option1")\
           == "option1"
    with pytest.raises(utils.ParsingError):
        database_options.extract_db_wide_option_name("CFOptions.option1")

    # get_cf_full_option_name
    assert database_options.get_cf_full_option_name("option1") == \
           "CFOptions.option1"

    # extract_db_wide_option_name
    assert database_options.extract_cf_option_name("CFOptions.option1")\
           == "option1"
    with pytest.raises(utils.ParsingError):
        database_options.extract_cf_option_name("DBOptions.option1")

    # get_cf_table_full_option_name
    assert database_options.get_cf_table_full_option_name("option1") == \
           "TableOptions.BlockBasedTable.option1"

    # extract_db_wide_option_name
    assert database_options.extract_cf_table_option_name(
        "TableOptions.BlockBasedTable.option1") == "option1"
    with pytest.raises(utils.ParsingError):
        database_options.extract_cf_table_option_name("DBOptions.option1")


def test_misc_options_sanitization_utils():
    # check_and_sanitize_if_no_value
    assert database_options.check_and_sanitize_if_null_ptr(None) == \
           (False, None)
    assert database_options.check_and_sanitize_if_null_ptr("None") == \
           (True, SANITIZED_NULL_PTR)
    assert database_options.check_and_sanitize_if_null_ptr("NONE") == \
           (True, SANITIZED_NULL_PTR)
    assert database_options.check_and_sanitize_if_null_ptr("(nil)") == \
           (True, SANITIZED_NULL_PTR)
    assert database_options.check_and_sanitize_if_null_ptr("nil") == \
           (True, SANITIZED_NULL_PTR)
    assert database_options.check_and_sanitize_if_null_ptr("nullptr") == \
           (True, SANITIZED_NULL_PTR)
    assert database_options.check_and_sanitize_if_null_ptr("null") == \
           (True, SANITIZED_NULL_PTR)
    assert database_options.check_and_sanitize_if_null_ptr("NullPtr") == \
           (True, SANITIZED_NULL_PTR)
    assert database_options.check_and_sanitize_if_null_ptr("0x0") == \
           (True, SANITIZED_NULL_PTR)

    assert database_options.check_and_sanitize_if_null_ptr("") == \
           (False, "")
    assert database_options.check_and_sanitize_if_null_ptr("XXX") == \
           (False, "XXX")
    assert database_options.check_and_sanitize_if_null_ptr(0) == \
           (False, 0)
    assert database_options.check_and_sanitize_if_null_ptr(False) == \
           (False, False)
    assert database_options.check_and_sanitize_if_null_ptr("0x1234") == \
           (False, "0x1234")

    # is_bool_value
    assert database_options.check_and_sanitize_if_bool_value(
        True, include_int=False) == (True, "True")
    assert database_options.check_and_sanitize_if_bool_value(
        False, include_int=False) == (True, "False")
    assert database_options.check_and_sanitize_if_bool_value(
        "true", include_int=False) == (True, "True")
    assert database_options.check_and_sanitize_if_bool_value(
        "TRUE", include_int=False) == (True, "True")
    assert database_options.check_and_sanitize_if_bool_value(
        "false", include_int=False) == (True, "False")
    assert database_options.check_and_sanitize_if_bool_value(
        "FALSE", include_int=False) == (True, "False")
    assert database_options.check_and_sanitize_if_bool_value(
        0, include_int=False) == (False, 0)
    assert database_options.check_and_sanitize_if_bool_value(
        1, include_int=False) == (False, 1)
    assert database_options.check_and_sanitize_if_bool_value(
        0, include_int=True) == (True, "False")
    assert database_options.check_and_sanitize_if_bool_value(
        1, include_int=True) == (True, "True")

    assert database_options.check_and_sanitize_if_pointer_value(None) == \
           (False, None)
    assert database_options.check_and_sanitize_if_pointer_value("(nil)") ==\
           (False, "(nil)")
    assert database_options.check_and_sanitize_if_pointer_value(0) ==\
           (False, 0)
    assert database_options.check_and_sanitize_if_pointer_value("0x0") ==\
           (False, "0x0")
    assert database_options.check_and_sanitize_if_pointer_value("0x123") ==\
           (True, "Pointer (0x123)")

    # get_sanitized_value
    assert database_options.get_sanitized_value(None) == SANITIZED_NO_VALUE
    assert database_options.get_sanitized_value("None") == SANITIZED_NULL_PTR
    assert database_options.get_sanitized_value("None") == SANITIZED_NULL_PTR
    assert database_options.get_sanitized_value("0x0") == SANITIZED_NULL_PTR
    assert database_options.get_sanitized_value(True) == "True"
    assert database_options.get_sanitized_value(False) == "False"
    assert database_options.get_sanitized_value(False) == "False"
    assert database_options.get_sanitized_value("False") == "False"
    assert database_options.get_sanitized_value("100") == "100"
    assert database_options.get_sanitized_value("0x123") == "Pointer (0x123)"

    # get_sanitized_options_diff
    assert database_options.are_non_sanitized_values_different(0, 1)
    assert database_options.are_non_sanitized_values_different(False, 1)
    assert database_options.are_non_sanitized_values_different(True, 0)
    assert database_options.are_non_sanitized_values_different("True", 0)
    assert database_options.are_non_sanitized_values_different("True", "0")
    assert database_options.are_non_sanitized_values_different("False", 1)
    assert database_options.are_non_sanitized_values_different("False", "1")
    assert not database_options.are_non_sanitized_values_different(False, 0)
    assert not database_options.are_non_sanitized_values_different(True, 1)
    assert not database_options.are_non_sanitized_values_different("False", 0)
    assert not database_options.are_non_sanitized_values_different("False",
                                                                   "0")
    assert not database_options.are_non_sanitized_values_different("True", 1)
    assert not database_options.are_non_sanitized_values_different("True", "1")
    assert not database_options.are_non_sanitized_values_different(0, "False")
    assert not database_options.are_non_sanitized_values_different(1, "TRUE")

    assert not database_options.are_non_sanitized_values_different(None, None)
    assert database_options.are_non_sanitized_values_different(None, "nil")
    assert database_options.are_non_sanitized_values_different(None, "False")
    assert database_options.are_non_sanitized_values_different(False, None)

    assert not database_options.are_non_sanitized_values_different("0x0",
                                                                   "(nil)")
    assert not database_options.are_non_sanitized_values_different("0x1234",
                                                                   "0x5678")
    assert database_options.are_non_sanitized_values_different("0x0", "0x5678")

    # get_sanitized_options_diff
    assert database_options.get_sanitized_options_diff(
        "None", None, expect_diff=False) == \
        (True, SANITIZED_NULL_PTR, SANITIZED_NO_VALUE)

    assert database_options.get_sanitized_options_diff(
        "None", "false", expect_diff=True) == (SANITIZED_NULL_PTR, "False")

    assert database_options.get_sanitized_options_diff(
        "false", True, expect_diff=True) == ("False", "True")

    assert database_options.get_sanitized_options_diff(
        0, True, expect_diff=True) == ("False", "True")

    assert database_options.get_sanitized_options_diff(
        0, 1, expect_diff=True) == (0, 1)

    assert database_options.get_sanitized_options_diff(
        0, "1", expect_diff=True) == (0, "1")

    assert database_options.get_sanitized_options_diff(
        "0x1234", "0x5678", expect_diff=False) == \
        (False, "Pointer (0x1234)", "Pointer (0x5678)")


def test_full_name_options_dict():
    db_option1 = "DB-OPTION-1"
    db_value1 = "DB-VALUE-1"
    db_value1_1 = "DB-VALUE-1-1"

    cf1 = "CF-1"
    option1 = "OPTION-1"
    value1 = "VALUE-1"
    value1_1 = "VALUE-1-1"

    cf2 = "CF-2"
    option2 = "OPTION-2"
    value2 = "VALUE-2"
    value2_1 = "VALUE-2-1"

    fnd1 = FullNamesOptionsDict()
    fnd2 = FullNamesOptionsDict()
    assert fnd1 == fnd2
    assert fnd1 == fnd2.get_options_dict()

    assert fnd1.get_option_by_full_name(f"DBOptions.{db_option1}") is None
    fnd1.set_option(SectionType.DB_WIDE, DB_WIDE_CF_NAME, db_option1,
                    db_value1)
    assert fnd1 != fnd2

    assert fnd1.get_option_by_full_name(f"DBOptions.{db_option1}") == {
        DB_WIDE_CF_NAME: db_value1}
    assert fnd1.get_option_by_parts(SectionType.DB_WIDE, db_option1) == \
           {DB_WIDE_CF_NAME: db_value1}
    assert fnd1.get_option_by_parts(SectionType.DB_WIDE, db_option1,
                                    DB_WIDE_CF_NAME) == db_value1
    assert fnd1.get_option_by_parts(SectionType.CF, db_option1,
                                    DB_WIDE_CF_NAME) is None
    assert fnd1.get_option_by_parts(SectionType.DB_WIDE, db_option1, cf1) \
           is None
    assert fnd1.get_db_wide_option(db_option1) == db_value1
    assert fnd1.get_cf_option(db_option1) is None
    assert fnd1.get_cf_option(db_option1, cf1) is None
    assert fnd1.get_cf_table_option(db_option1) is None
    assert fnd1.get_cf_table_option(db_option1, cf1) is None
    fnd1.set_db_wide_option(db_option1, db_value1_1)
    assert fnd1.get_db_wide_option(db_option1) == db_value1_1

    fnd1.set_cf_option(cf1, option1, value1)
    assert fnd1.get_option_by_parts(SectionType.CF, option1) == {cf1: value1}
    assert fnd1.get_option_by_parts(SectionType.CF, option1, cf1) == value1
    assert fnd1.get_option_by_parts(SectionType.DB_WIDE, option1) is None
    assert fnd1.get_option_by_parts(SectionType.DB_WIDE, option1,
                                    DB_WIDE_CF_NAME) is None
    assert fnd1.get_cf_option(option1) == {cf1: value1}
    assert fnd1.get_cf_option(option1, cf1) == value1
    assert fnd1.get_cf_table_option(option1) is None
    assert fnd1.get_cf_table_option(option1, cf1) is None
    fnd1.set_cf_option(cf1, option1, value1_1)
    assert fnd1.get_cf_option(option1, cf1) == value1_1

    fnd1.set_cf_table_option(cf2, option2, value2)
    assert fnd1.get_option_by_parts(SectionType.TABLE_OPTIONS, option2) == {
        cf2: value2}
    assert fnd1.get_option_by_parts(SectionType.TABLE_OPTIONS, option2,
                                    cf2) == value2
    assert fnd1.get_option_by_parts(SectionType.DB_WIDE, option2) is None
    assert fnd1.get_option_by_parts(SectionType.DB_WIDE, option2,
                                    DB_WIDE_CF_NAME) is None
    assert fnd1.get_cf_table_option(option2) == {cf2: value2}
    assert fnd1.get_cf_table_option(option2, cf2) == value2
    assert fnd1.get_cf_option(option2) is None
    assert fnd1.get_cf_option(option2, cf2) is None
    fnd1.set_cf_table_option(cf2, option2, value2_1)
    assert fnd1.get_cf_table_option(option2, cf2) == value2_1

    expected_dict = {'DBOptions.DB-OPTION-1': {'DB_WIDE': 'DB-VALUE-1-1'},
                     'CFOptions.OPTION-1': {'CF-1': 'VALUE-1-1'},
                     'TableOptions.BlockBasedTable.OPTION-2':
                         {'CF-2': 'VALUE-2-1'}}
    assert fnd1.get_options_dict() == expected_dict

    fnd1.set_db_wide_option(option2, value1_1)
    fnd1.set_cf_option(cf2, option1, db_value1)
    fnd1.set_cf_table_option(cf1, option2, value2)
    expected_dict = {'DBOptions.DB-OPTION-1': {'DB_WIDE': 'DB-VALUE-1-1'},
                     'DBOptions.OPTION-2': {'DB_WIDE': 'VALUE-1-1'},
                     'CFOptions.OPTION-1': {'CF-1': 'VALUE-1-1',
                                            'CF-2': 'DB-VALUE-1'},
                     'TableOptions.BlockBasedTable.OPTION-2':
                         {'CF-2': 'VALUE-2-1', 'CF-1': 'VALUE-2'}}
    assert fnd1.get_options_dict() == expected_dict


def test_empty_db_options():
    db_options = DatabaseOptions()
    assert db_options.get_all_options() == EMPTY_FULL_NAMES_OPTIONS_DICT
    assert db_options.get_db_wide_options() == EMPTY_FULL_NAMES_OPTIONS_DICT
    assert not db_options.are_db_wide_options_set()
    assert db_options.get_cfs_names() == []
    assert db_options.get_db_wide_options_for_display() == {}
    assert db_options.get_db_wide_option("manual_wal_flush") is None

    with pytest.raises(AssertionError):
        db_options.set_db_wide_option("manual_wal_flush", "1",
                                      allow_new_option=False)
    db_options.set_db_wide_option("manual_wal_flush", "1",
                                  allow_new_option=True)
    assert db_options.get_db_wide_option("manual_wal_flush") == "1"
    assert db_options.get_db_wide_option("Dummy-Options") is None
    assert db_options.get_options({"DBOptions.manual_wal_flush"}).\
        get_options_dict() == {'DBOptions.manual_wal_flush': {"DB_WIDE": "1"}}
    assert db_options.get_options({"DBOptions.Dummy-Option"}) == \
           EMPTY_FULL_NAMES_OPTIONS_DICT

    assert db_options.get_cf_options(default) == EMPTY_FULL_NAMES_OPTIONS_DICT
    assert db_options.get_cf_options_for_display(default) == ({}, {})
    assert db_options.get_cf_option(default, "write_buffer_size") is None
    with pytest.raises(AssertionError):
        db_options.set_cf_option(default, "write_buffer_size", "100",
                                 allow_new_option=False)
    db_options.set_cf_option(default, "write_buffer_size", "100",
                             allow_new_option=True)
    assert db_options.get_cf_option(default, "write_buffer_size") == "100"
    assert db_options.get_cf_option(default, "Dummmy-Options") is None
    assert db_options.get_cf_option("Dummy-CF", "write_buffer_size") is None
    assert db_options.get_options({"CFOptions.write_buffer_size"}) == \
           {'CFOptions.write_buffer_size': {default: '100'}}
    assert db_options.get_options({"CFOptions.write_buffer_size"}, default) \
           == {'CFOptions.write_buffer_size': {default: '100'}}
    assert db_options.get_options({"CFOptions.write_buffer_size"}, "Dummy-CF")\
           == {}
    assert db_options.get_options({"CFOptions.Dummy-Options"}, default) == {}

    assert db_options.get_cf_table_option(default, "write_buffer_size") is \
           None

    assert db_options.get_options(["TableOptions.BlockBasedTable.block_align"],
                                  cf1) == {}

    with pytest.raises(AssertionError):
        db_options.set_cf_table_option(cf1, "index_type", "3",
                                       allow_new_option=False)
    db_options.set_cf_table_option(cf1, "index_type", "3",
                                   allow_new_option=True)
    assert db_options.get_cf_table_option(cf1, "index_type") == "3"
    assert db_options.get_cf_table_option(cf1, "Dummy-Options") is None
    assert db_options.get_cf_table_option(default, "index_type") is None

    assert db_options.get_options({"TableOptions.BlockBasedTable.index_type"})\
           == {'TableOptions.BlockBasedTable.index_type': {cf1: '3'}}
    assert db_options.get_options({"TableOptions.BlockBasedTable.index_type"},
                                  cf1) == \
           {'TableOptions.BlockBasedTable.index_type': {cf1: '3'}}
    assert db_options.get_options({
        "TableOptions.BlockBasedTable.index_type"}, "Dummy-CF") == {}
    assert db_options.get_options({
        "TableOptions.BlockBasedTable.Dummy-Option"}) == {}


def test_set_db_wide_options():
    input_dict = {"manual_wal_flush": "false",
                  "db_write_buffer_size": "0"}
    expected_options = {
        'DBOptions.manual_wal_flush': {utils.NO_CF: 'False'},
        'DBOptions.db_write_buffer_size': {utils.NO_CF: '0'},
    }

    db_options = DatabaseOptions()
    db_options.set_db_wide_options(input_dict)

    assert db_options.get_all_options() == expected_options
    assert db_options.get_db_wide_options() == expected_options
    assert db_options.get_db_wide_option("manual_wal_flush") == 'False'
    assert db_options.get_db_wide_option("manual_wal_flush-X") is None
    assert db_options.get_cfs_names() == []

    db_options.set_db_wide_option('manual_wal_flush', 'true',
                                  allow_new_option=False)
    assert db_options.get_db_wide_option("manual_wal_flush") == 'True'

    db_options.set_db_wide_option('manual_wal_flush', 'false',
                                  allow_new_option=False)
    assert db_options.get_db_wide_option("manual_wal_flush") == 'False'

    assert db_options.get_db_wide_option("DUMMY") is None
    with pytest.raises(AssertionError):
        db_options.set_db_wide_option('DUMMY', 'XXX', allow_new_option=False)

    db_options.set_db_wide_option('DUMMY', 'XXX', True)
    assert db_options.get_db_wide_option("DUMMY") == "XXX"

    expected_db_wide_display_options = {'manual_wal_flush': 'False',
                                        'db_write_buffer_size': '0',
                                        'DUMMY': 'XXX'}
    assert db_options.get_db_wide_options_for_display() == \
           expected_db_wide_display_options


def test_set_cf_options():
    default_input_dict = {"write_buffer_size": "1000"}
    default_table_input_dict = {"index_type": "1"}

    cf1_input_dict = {"write_buffer_size": "2000"}
    cf1_table_input_dict = {"index_type": "2"}

    cf2_input_dict = {"write_buffer_size": "3000"}
    cf2_table_input_dict = {"index_type": "3"}

    db_options = DatabaseOptions()

    expected_options = dict()
    expected_options['CFOptions.write_buffer_size'] = {default: '1000'}
    expected_options['TableOptions.BlockBasedTable.index_type'] = \
        {default: '1'}
    db_options.set_cf_options(default, default_input_dict,
                              default_table_input_dict)
    assert db_options.get_all_options() == expected_options
    assert db_options.get_cfs_names() == [default]

    expected_options['CFOptions.write_buffer_size'][cf1] = "2000"
    expected_options['TableOptions.BlockBasedTable.index_type'][cf1] = "2"
    db_options.set_cf_options(cf1, cf1_input_dict, cf1_table_input_dict)
    assert db_options.get_all_options().get_options_dict() == expected_options
    assert db_options.get_cfs_names() == [default, cf1]

    expected_options['CFOptions.write_buffer_size'][cf2] = "3000"
    expected_options['TableOptions.BlockBasedTable.index_type'][cf2] = "3"
    db_options.set_cf_options(cf2, cf2_input_dict, cf2_table_input_dict)
    assert db_options.get_all_options() == expected_options
    assert db_options.get_cfs_names() == [default, cf1, cf2]

    expected_dict_default = \
        {'CFOptions.write_buffer_size': {default: '1000'},
         'TableOptions.BlockBasedTable.index_type': {default: '1'}}

    expected_dict_cf1 = \
        {'CFOptions.write_buffer_size': {cf1: '2000'},
         'TableOptions.BlockBasedTable.index_type': {cf1: '2'}}

    expected_dict_cf2 = \
        {'CFOptions.write_buffer_size': {cf2: '3000'},
         'TableOptions.BlockBasedTable.index_type': {cf2: '3'}}

    assert db_options.get_db_wide_options().get_options_dict() == {}
    assert expected_dict_default == db_options.get_cf_options(default)
    assert expected_dict_cf1 == db_options.get_cf_options(cf1)
    assert expected_dict_cf2 == db_options.get_cf_options(cf2)
    assert db_options.get_cf_option(default, "write_buffer_size") == '1000'
    assert db_options.get_cf_table_option(default, "index_type", ) == '1'
    assert db_options.get_cf_option(cf1, "write_buffer_size") == '2000'
    assert db_options.get_cf_table_option(cf1, "index_type", ) == '2'
    db_options.set_cf_table_option(cf1, "index_type", '100')
    assert db_options.get_cf_table_option(cf1, "index_type", ) == '100'
    assert db_options.get_cf_option(cf2, "write_buffer_size") == '3000'
    assert db_options.get_cf_table_option(cf2, "index_type") == '3'
    db_options.set_cf_table_option(cf2, "index_type", 'XXXX')
    assert db_options.get_cf_table_option(cf2, "index_type") == 'XXXX'

    assert db_options.get_cf_option(default, "index_type") is None
    db_options.set_cf_option(default, "index_type", "200", True)
    assert db_options.get_cf_option(default, "index_type") == "200"
    assert db_options.get_cf_table_option(cf1, "write_buffer_size") is None

    expected_default_display_options = {'write_buffer_size': '1000',
                                        'index_type': '200'}
    expected_default_display_table_options = {'index_type': '1'}

    expected_cf1_display_options = {'write_buffer_size': '2000'}
    expected_cf1_display_table_options = {'index_type': '100'}

    expected_cf2_display_options = {'write_buffer_size': '3000'}
    expected_cf2_display_table_options = {'index_type': 'XXXX'}

    assert db_options.get_cf_options_for_display(default) == \
           (expected_default_display_options,
            expected_default_display_table_options)

    assert db_options.get_cf_options_for_display(cf1) == \
           (expected_cf1_display_options,
            expected_cf1_display_table_options)

    assert db_options.get_cf_options_for_display(cf2) == \
           (expected_cf2_display_options,
            expected_cf2_display_table_options)


def test_get_unified_options():
    get_unified = DatabaseOptions.get_unified_cfs_options

    assert get_unified({}) == ({}, {})

    cf1_options =\
        FullNamesOptionsDict({'CFOptions.write_buffer_size': {cf1: 1000}})
    cfs_options = {cf1: cf1_options}
    assert get_unified(cfs_options) == \
           ({"CFOptions.write_buffer_size": 1000},
            {cf1: {}})

    cf2_options =\
        FullNamesOptionsDict({'CFOptions.write_buffer_size': {cf2: 1000}})
    cfs_options = {cf1: cf1_options, cf2: cf2_options}
    assert get_unified(cfs_options) == \
           ({"CFOptions.write_buffer_size": 1000},
            {cf1: {}, cf2: {}})

    cf2_options.set_cf_option(cf2, "write_buffer_size", 2000)
    assert get_unified(cfs_options) == \
           ({},
            {cf1: {"CFOptions.write_buffer_size": 1000},
             cf2: {"CFOptions.write_buffer_size": 2000}})

    cf1_options.set_cf_option(cf1, "cf1_only_option", "1234")
    assert get_unified(cfs_options) == \
           ({},
            {cf1: {"CFOptions.write_buffer_size": 1000,
                   "CFOptions.cf1_only_option": "1234"},
             cf2: {"CFOptions.write_buffer_size": 2000}})

    cf2_options.set_cf_option(cf2, "cf2_only_option", "5678")
    assert get_unified(cfs_options) == \
           ({},
            {cf1: {"CFOptions.write_buffer_size": 1000,
                   "CFOptions.cf1_only_option": "1234"},
             cf2: {"CFOptions.write_buffer_size": 2000,
                   "CFOptions.cf2_only_option": "5678"}})

    cf2_options.set_cf_option(cf2, "write_buffer_size", 1000)
    assert get_unified(cfs_options) == \
           ({"CFOptions.write_buffer_size": 1000},
            {cf1: {"CFOptions.cf1_only_option": "1234"},
             cf2: {"CFOptions.cf2_only_option": "5678"}})


def test_options_diff_class():
    diff = OptionsDiff(baseline_options, new_options)
    assert diff.get_diff_dict() == {}

    diff.diff_in_base(cf2, 'CFOptions.write_buffer_size')
    diff.diff_in_new(cf3, 'CFOptions.write_buffer_size')
    diff.diff_between(default, 'CFOptions.write_buffer_size')

    assert diff.get_diff_dict() == {
        'CFOptions.write_buffer_size': {cf2: ('128000000', SANITIZED_NO_VALUE),
                                        cf3: (SANITIZED_NO_VALUE, '128000000'),
                                        default: ('1024000', '128000000')}
    }


def test_get_options_diff():
    expected_diff = {
        'DBOptions.stats_dump_freq_sec':
            {utils.NO_CF: ('20', SANITIZED_NO_VALUE)},
        'DBOptions.enable_pipelined_write':
            {utils.NO_CF: ('False', 'True')},
        'bloom_bits': {utils.NO_CF: (SANITIZED_NO_VALUE, '4')},
        'CFOptions.write_buffer_size': {
            default: ('1024000', '128000000'),
            cf2: ('128000000', SANITIZED_NO_VALUE),
            cf3: (SANITIZED_NO_VALUE, '128000000')},
        'TableOptions.BlockBasedTable.index_type':
            {'cf2': ('1', SANITIZED_NO_VALUE)},
        'TableOptions.BlockBasedTable.checksum':
            {cf1: (SANITIZED_NO_VALUE, 'True')},
        'TableOptions.BlockBasedTable.format_version': {'cf1': ('4', '5')},
        'DBOptions.max_log_file_size':
            {utils.NO_CF: ('128000000', '0')}
    }

    full_name_baseline_dict = FullNamesOptionsDict(baseline_options)
    full_name_new_dict = FullNamesOptionsDict(new_options)
    diff = DatabaseOptions.get_options_diff(full_name_baseline_dict,
                                            full_name_new_dict)
    assert diff.get_diff_dict() == expected_diff


def test_cfs_options_diff_class():
    diff = CfsOptionsDiff(baseline_options, default, new_options, cf1)
    assert diff.get_diff_dict() == {}

    diff.diff_between('CFOptions.write_buffer_size')

    assert diff.get_diff_dict() == {
        CfsOptionsDiff.CF_NAMES_KEY: {"Base": default,
                                      "New": cf1},
        'CFOptions.write_buffer_size': ('1024000', '128000')
    }


def test_get_db_wide_options_diff():
    assert DatabaseOptions.get_db_wide_options_diff(baseline, baseline) is None

    expected_diff = {'bloom_bits': (SANITIZED_NO_VALUE, '4'),
                     'DBOptions.enable_pipelined_write': ('False', 'True'),
                     'DBOptions.max_log_file_size': ('128000000', '0'),
                     'DBOptions.stats_dump_freq_sec':
                         ('20', SANITIZED_NO_VALUE),
                     CfsOptionsDiff.CF_NAMES_KEY:
                         {"Base": 'DB_WIDE',
                          "New": 'DB_WIDE'}}

    assert DatabaseOptions.get_db_wide_options_diff(baseline, new).\
        get_diff_dict() == expected_diff


def test_get_cfs_options_diff():
    assert DatabaseOptions.get_cfs_options_diff(
        baseline, utils.NO_CF,
        baseline, utils.NO_CF) is None
    assert DatabaseOptions.get_cfs_options_diff(baseline, default,
                                                baseline, default) is None

    assert DatabaseOptions.get_cfs_options_diff(baseline, cf1,
                                                baseline, cf1) is None
    assert DatabaseOptions.get_cfs_options_diff(baseline, cf2,
                                                baseline, cf2) is None
    assert DatabaseOptions.get_cfs_options_diff(baseline, cf3,
                                                baseline, cf3) is None

    assert DatabaseOptions.get_cfs_options_diff(
        new, utils.NO_CF,
        new, utils.NO_CF) is None

    assert DatabaseOptions.get_cfs_options_diff(new, default,
                                                new, default) is None
    assert DatabaseOptions.get_cfs_options_diff(new, cf1, new, cf1) is None
    assert DatabaseOptions.get_cfs_options_diff(new, cf2, new, cf2) is None
    assert DatabaseOptions.get_cfs_options_diff(new, cf3, new, cf3) is None

    expected_diff = {
        CfsOptionsDiff.CF_NAMES_KEY:
            {"Base": default,
             "New": default},
        'CFOptions.write_buffer_size': ('1024000', '128000000')
    }

    assert DatabaseOptions.get_cfs_options_diff(baseline, default,
                                                new, default) == \
           expected_diff

    expected_diff = {
        CfsOptionsDiff.CF_NAMES_KEY:
            {"Base": default,
             "New": cf1},
        'CFOptions.write_buffer_size': ('1024000', '128000'),
        'TableOptions.BlockBasedTable.checksum': (SANITIZED_NO_VALUE, 'True'),
        'TableOptions.BlockBasedTable.format_version':
            (SANITIZED_NO_VALUE, '5'),
        'TableOptions.BlockBasedTable.block_size': (SANITIZED_NO_VALUE, '4096')
    }
    assert DatabaseOptions.get_cfs_options_diff(baseline, default,
                                                new, cf1) == \
           expected_diff

    expected_diff = {
        CfsOptionsDiff.CF_NAMES_KEY:
            {"Base": cf1,
             "New": default},
        'CFOptions.write_buffer_size': ('128000', '1024000'),
        'TableOptions.BlockBasedTable.checksum': ('True', SANITIZED_NO_VALUE),
        'TableOptions.BlockBasedTable.format_version':
            ('5', SANITIZED_NO_VALUE),
        'TableOptions.BlockBasedTable.block_size': ('4096', SANITIZED_NO_VALUE)
    }
    assert DatabaseOptions.get_cfs_options_diff(new, cf1,
                                                baseline, default) == \
           expected_diff

    expected_diff = {
        CfsOptionsDiff.CF_NAMES_KEY:
            {"Base": cf2,
             "New": cf2},
        'CFOptions.write_buffer_size': ('128000000', SANITIZED_NO_VALUE),
        'TableOptions.BlockBasedTable.index_type': ('1', SANITIZED_NO_VALUE),

    }
    assert DatabaseOptions.get_cfs_options_diff(baseline, cf2,
                                                new, cf2) == \
           expected_diff

    expected_diff = {
        CfsOptionsDiff.CF_NAMES_KEY:
            {"Base": cf3,
             "New": cf3},
        'CFOptions.write_buffer_size': (SANITIZED_NO_VALUE, '128000000')}
    assert DatabaseOptions.get_cfs_options_diff(baseline, cf3,
                                                new, cf3) == \
           expected_diff


def test_get_unified_cfs_diffs():
    cfs_diffs = {}
    assert DatabaseOptions.get_unified_cfs_diffs(cfs_diffs) == ({}, {})

    cf1_vs_default_diff = {
        CfsOptionsDiff.CF_NAMES_KEY:
            {"Base": default,
             "New": cf1},
        'CFOptions.write_buffer_size': ('1024000', '128000'),
    }
    cfs_diffs = [cf1_vs_default_diff]
    assert DatabaseOptions.get_unified_cfs_diffs(cfs_diffs) == (
        {'CFOptions.write_buffer_size': ('1024000', '128000')},
        [{CfsOptionsDiff.CF_NAMES_KEY: {"Base": default, "New": cf1}}])

    cf2_vs_default_diff = {
        CfsOptionsDiff.CF_NAMES_KEY:
            {"Base": default,
             "New": cf2},
        'CFOptions.write_buffer_size': ('1024000', '128000'),
    }
    cfs_diffs = [cf1_vs_default_diff, cf2_vs_default_diff]
    assert DatabaseOptions.get_unified_cfs_diffs(cfs_diffs) == (
        {'CFOptions.write_buffer_size': ('1024000', '128000')},
        [{CfsOptionsDiff.CF_NAMES_KEY: {"Base": default, "New": cf1}},
         {CfsOptionsDiff.CF_NAMES_KEY: {"Base": default, "New": cf2}}])

    cf1_vs_default_diff['CFOptions.write_buffer_size'] = (1000, 2000)
    assert DatabaseOptions.get_unified_cfs_diffs(cfs_diffs) == (
        {},
        [{CfsOptionsDiff.CF_NAMES_KEY: {"Base": default, "New": cf1},
          'CFOptions.write_buffer_size': (1000, 2000)},
         {CfsOptionsDiff.CF_NAMES_KEY: {"Base": default, "New": cf2},
          'CFOptions.write_buffer_size': ('1024000', '128000')}])

    cf1_vs_default_diff['CFOptions.write_buffer_size'] = \
        cf2_vs_default_diff['CFOptions.write_buffer_size']
    cf2_vs_default_diff['TableOptions.BlockBasedTable.cf2_option'] = (200, 300)
    assert DatabaseOptions.get_unified_cfs_diffs(cfs_diffs) == (
        {'CFOptions.write_buffer_size': ('1024000', '128000')},
        [{CfsOptionsDiff.CF_NAMES_KEY: {"Base": default, "New": cf1}},
         {CfsOptionsDiff.CF_NAMES_KEY: {"Base": default, "New": cf2},
          'TableOptions.BlockBasedTable.cf2_option': (200, 300)}])

    cf1_vs_default_diff['TableOptions.BlockBasedTable.cf1_option'] = (True,
                                                                      False)
    assert DatabaseOptions.get_unified_cfs_diffs(cfs_diffs) == (
        {'CFOptions.write_buffer_size': ('1024000', '128000')},
        [{CfsOptionsDiff.CF_NAMES_KEY: {"Base": default, "New": cf1},
          'TableOptions.BlockBasedTable.cf1_option': (True, False)},
         {CfsOptionsDiff.CF_NAMES_KEY: {"Base": default, "New": cf2},
          'TableOptions.BlockBasedTable.cf2_option': (200, 300)}])
