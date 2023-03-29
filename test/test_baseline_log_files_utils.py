import baseline_log_files_utils
import defs_and_utils


v = defs_and_utils.Version
baseline_logs_folder_path = "../" + defs_and_utils.BASELINE_LOGS_FOLDER


def test_version():
    assert str(v("1.2.3")) == "1.2.3"
    assert str(v("1.2")) == "1.2"
    assert str(v("10.20.30")) == "10.20.30"
    assert str(v("10.20")) == "10.20"

    v1 = v("1.2.3")
    assert v1.major == 1
    assert v1.minor == 2
    assert v1.patch == 3

    v2 = v("4.5")
    assert v2.major == 4
    assert v2.minor == 5
    assert v2.patch is None

    assert v("1.2.3") < v("2.1.1")
    assert v("2.1.1") > v("1.2.3")
    assert v("2.2.3") < v("2.3.1")
    assert v("2.2.1") < v("2.2.2")
    assert not v("2.2") < v("2.2")
    assert v("2.2") < v("2.2.0")
    assert v("2.2") < v("2.3")
    assert v("2.2") < v("2.2.2")
    assert v("2.2.1") > v("2.2")

    assert v("2.2.3") == v("2.2.3")
    assert v("2.2") == v("2.2")
    assert v("2.2.3") != v("2.2")


def test_find_closest_version():
    # prepare an unsorted list on purpose
    baseline_versions = [v("1.2"),
                         v("1.2.0"),
                         v("1.2.1"),
                         v("2.1"),
                         v("2.1.0"),
                         v("3.0"),
                         v("3.0.0")]

    find = baseline_log_files_utils.find_closest_version_idx

    assert find(baseline_versions, v("1.1")) is None
    assert find(baseline_versions, v("1.1.9")) is None
    assert find(baseline_versions, v("1.2")) == 0  # v("1.2")
    assert find(baseline_versions, v("1.2.0")) == 1  # v("1.2.0")
    assert find(baseline_versions, v("1.2.2")) == 2  # v("1.2.1")
    assert find(baseline_versions, v("2.0.0")) == 2  # v("1.2.1")
    assert find(baseline_versions, v("2.1")) == 3  # v("2.1")
    assert find(baseline_versions, v("2.1.0")) == 4  # v("2.1.0")
    assert find(baseline_versions, v("2.1.1")) == 4  # v("2.1.0")
    assert find(baseline_versions, v("3.0")) == 5  # v("3.0")
    assert find(baseline_versions, v("3.0.0")) == 6  # v("3.0.0")
    assert find(baseline_versions, v("3.0.1")) == 6  # v("3.0.0")
    assert find(baseline_versions, v("3.1")) == 6  # v("3.0.0")
    assert find(baseline_versions, v("99.99")) == 6  # v("3.0.0")
    assert find(baseline_versions, v("99.99.99")) == 6  # v("3.0.0")


def test_find_closest_baseline_log_file():
    find = baseline_log_files_utils.find_closest_baseline_log_file_name

    closest_baseline_info = \
        find(baseline_logs_folder_path,
             defs_and_utils.ProductName.SPEEDB,
             v("1.0.0"))
    assert closest_baseline_info is None

    closest_baseline_info = \
        find(baseline_logs_folder_path,
             defs_and_utils.ProductName.SPEEDB,
             v("2.2.1"))
    assert closest_baseline_info is not None
    closest_file_name, closest_version = closest_baseline_info
    assert closest_file_name == "LOG-speedb-2.2.1"
    assert closest_version == defs_and_utils.Version("2.2.1")

    closest_file_name, closest_version = \
        find(baseline_logs_folder_path,
             defs_and_utils.ProductName.SPEEDB,
             v("2.2.2"))
    assert closest_file_name == "LOG-speedb-2.2.1"
    assert closest_version == defs_and_utils.Version("2.2.1")

    closest_file_name, closest_version = \
        find(baseline_logs_folder_path,
             defs_and_utils.ProductName.SPEEDB,
             v("32.0.1 "))
    assert closest_file_name == "LOG-speedb-2.3.0"
    assert closest_version == defs_and_utils.Version("2.3.0")

    # closest_file_path, closest_version = \
    #     find(options_files_path,
    #          defs_and_utils.ProductName.ROCKSDB,
    #          v("7.8.3"))
    # assert closest_file_path.name == "LOG-rocksdb-7.7.8"
    # assert closest_version == defs_and_utils.Version("7.7.8")
    #


def test_find_options_diff():
    database_options_2_1_0, _ =\
        baseline_log_files_utils.get_baseline_database_options(
            baseline_logs_folder_path,
            defs_and_utils.ProductName.SPEEDB,
            version_str="2.1.0")

    options_diff_2_1_0_vs_itself, closest_version =\
        baseline_log_files_utils.find_options_diff_relative_to_baseline(
            baseline_logs_folder_path,
            defs_and_utils.ProductName.SPEEDB,
            v("2.1.0"),
            database_options_2_1_0)
    assert options_diff_2_1_0_vs_itself is None
    assert closest_version == defs_and_utils.Version("2.1.0")

    expected_diff = {}
    updated_database_options_2_1_0 = database_options_2_1_0

    curr_max_open_files_value = \
        updated_database_options_2_1_0.get_db_wide_option('max_open_files')
    new_max_open_files_value = str(int(curr_max_open_files_value) + 100)
    updated_database_options_2_1_0.set_db_wide_option('max_open_files',
                                                      new_max_open_files_value)
    expected_diff['DBOptions.max_open_files'] = \
        {defs_and_utils.NO_COL_FAMILY: (curr_max_open_files_value,
                                        new_max_open_files_value)}

    updated_database_options_2_1_0.set_db_wide_option("NEW_DB_WIDE_OPTION1",
                                                      "NEW_DB_WIDE_VALUE1",
                                                      True)
    expected_diff['DBOptions.NEW_DB_WIDE_OPTION1'] = \
        {defs_and_utils.NO_COL_FAMILY: ("No Value", "NEW_DB_WIDE_VALUE1")}

    cf_name = "default"

    curr_ttl_value = updated_database_options_2_1_0.get_cf_option(cf_name,
                                                                  "ttl")
    new_ttl_value = str(int(curr_ttl_value) + 1000)
    updated_database_options_2_1_0.set_cf_option(cf_name, "ttl",
                                                 new_ttl_value)
    updated_database_options_2_1_0.set_cf_option('default', 'ttl',
                                                 new_ttl_value)
    expected_diff['CFOptions.ttl'] = {'default': (curr_ttl_value,
                                                  new_ttl_value)}

    updated_database_options_2_1_0.set_cf_option(cf_name, "NEW_CF_OPTION1",
                                                 "NEW_CF_VALUE1", "True")
    expected_diff['CFOptions.NEW_CF_OPTION1'] =\
        {'default': ("No Value", "NEW_CF_VALUE1")}

    curr_block_align_value = \
        updated_database_options_2_1_0.get_cf_table_option(cf_name,
                                                           "block_align")
    new_block_align_value = "dummy_block_align_value"
    updated_database_options_2_1_0.set_cf_table_option(cf_name,
                                                       "block_align",
                                                       new_block_align_value)
    expected_diff['TableOptions.BlockBasedTable.block_align'] = \
        {'default': (curr_block_align_value, new_block_align_value)}
    updated_database_options_2_1_0.set_cf_table_option(cf_name,
                                                       "NEW_CF_TABLE_OPTION1",
                                                       "NEW_CF_TABLE_VALUE1",
                                                       "True")
    expected_diff['TableOptions.BlockBasedTable.NEW_CF_TABLE_OPTION1'] = \
        {'default': ("No Value", "NEW_CF_TABLE_VALUE1")}

    options_diff_2_1_0_vs_itself, closest_version =\
        baseline_log_files_utils.find_options_diff_relative_to_baseline(
            baseline_logs_folder_path,
            defs_and_utils.ProductName.SPEEDB,
            v("2.1.0"),
            updated_database_options_2_1_0)
    assert options_diff_2_1_0_vs_itself.get_diff_dict() == expected_diff
    assert closest_version == defs_and_utils.Version("2.1.0")
