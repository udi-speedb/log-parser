import copy

import db_files
import events
from test.testing_utils import create_event

job_id1 = 1
job_id2 = 2

cf1 = "cf1"
cf2 = "cf2"
cf_names = [cf1, cf2]

time1_minus_10_sec = "2023/01/24-08:54:30.130553"
time1 = "2023/01/24-08:54:40.130553"
time1_plus_10_sec = "2023/01/24-08:54:50.130553"
time1_plus_11_sec = "2023/01/24-08:55:50.130553"

file_number1 = 1234
file_number2 = 5678
file_number3 = 9999
cmprsd_data_size_bytes = 62396458
num_data_blocks = 1000
total_keys_sizes_bytes = 2000
total_values_sizes_bytes = 3333
index_size = 3000
filter_size = 4000
num_entries = 5555
filter_policy = "bloomfilter"
num_filter_entries = 6666
compression_type = "NoCompression"

table_properties = {
    "data_size": cmprsd_data_size_bytes,
    "index_size": index_size,
    "index_partitions": 0,
    "top_level_index_size": 0,
    "index_key_is_user_key": 1,
    "index_value_is_delta_encoded": 1,
    "filter_size": filter_size,
    "raw_key_size": total_keys_sizes_bytes,
    "raw_average_key_size": 24,
    "raw_value_size": total_values_sizes_bytes,
    "raw_average_value_size": 1000,
    "num_data_blocks": num_data_blocks,
    "num_entries": num_entries,
    "num_filter_entries": num_filter_entries,
    "num_deletions": 0,
    "num_merge_operands": 0,
    "num_range_deletions": 0,
    "format_version": 0,
    "fixed_key_len": 0,
    "filter_policy": filter_policy,
    "column_family_name": "default",
    "column_family_id": 0,
    "comparator": "leveldb.BytewiseComparator",
    "merge_operator": "nullptr",
    "prefix_extractor_name": "nullptr",
    "property_collectors": "[]",
    "compression": compression_type,
    "oldest_key_time": 1672823099,
    "file_creation_time": 1672823099,
    "slow_compression_estimated_data_size": 0,
    "fast_compression_estimated_data_size": 0,
    "db_id": "c100448c-dc04-4c74-8ab2-65d72f3aa3a8",
    "db_session_id": "4GAWIG5RIF8PQWM3NOQG",
    "orig_file_number": 37155}


def test_create_delete_file():
    creation_event1 = create_event(job_id1, cf_names, time1,
                                   events.EventType.TABLE_FILE_CREATION, cf1,
                                   file_number=file_number1,
                                   table_properties=table_properties)

    monitor = db_files.DbFilesMonitor()
    assert monitor.get_all_live_files() == {}
    assert monitor.get_cf_live_files(cf1) == []

    expected_data_size_bytes = \
        total_keys_sizes_bytes + total_values_sizes_bytes

    info1 = \
        db_files.DbFileInfo(file_number=file_number1,
                            cf_name=cf1,
                            creation_time=time1,
                            deletion_time=None,
                            size_bytes=0,
                            compressed_size_bytes=0,
                            compressed_data_size_bytes=cmprsd_data_size_bytes,
                            data_size_bytes=expected_data_size_bytes,
                            index_size_bytes=index_size,
                            filter_size_bytes=filter_size,
                            filter_policy=filter_policy,
                            num_filter_entries=num_filter_entries,
                            compression_type=compression_type,
                            level=None)

    assert monitor.new_event(creation_event1)
    assert monitor.get_all_files() == {cf1: [info1]}
    assert monitor.get_all_cf_files(cf1) == [info1]
    assert monitor.get_all_cf_files(cf2) == []
    assert monitor.get_all_live_files() == {cf1: [info1]}
    assert monitor.get_cf_live_files(cf1) == [info1]
    assert monitor.get_cf_live_files(cf2) == []

    deletion_event = create_event(job_id1, cf_names, time1_plus_10_sec,
                                  events.EventType.TABLE_FILE_DELETION, cf1,
                                  file_number=file_number1)
    assert monitor.new_event(deletion_event)
    info1.deletion_time = time1_plus_10_sec
    assert monitor.get_all_files() == {cf1: [info1]}
    assert monitor.get_all_cf_files(cf1) == [info1]
    assert monitor.get_all_cf_files(cf2) == []
    assert monitor.get_all_live_files() == {}
    assert monitor.get_cf_live_files(cf1) == []
    assert monitor.get_cf_live_files(cf2) == []

    creation_event2 = create_event(job_id1, cf_names, time1_plus_10_sec,
                                   events.EventType.TABLE_FILE_CREATION, cf1,
                                   file_number=file_number2,
                                   table_properties=table_properties)
    info2 = copy.deepcopy(info1)
    info2.file_number = file_number2
    info2.creation_time = time1_plus_10_sec
    info2.deletion_time = None

    assert monitor.new_event(creation_event2)
    assert monitor.get_all_files() == {cf1: [info1, info2]}
    assert monitor.get_all_cf_files(cf1) == [info1, info2]
    assert monitor.get_all_cf_files(cf2) == []
    assert monitor.get_all_live_files() == {cf1: [info2]}
    assert monitor.get_cf_live_files(cf2) == []
    assert monitor.get_cf_live_files(cf1) == [info2]

    creation_event3 = create_event(job_id1, cf_names, time1_plus_10_sec,
                                   events.EventType.TABLE_FILE_CREATION, cf2,
                                   file_number=file_number3,
                                   table_properties=table_properties)
    info3 = copy.deepcopy(info1)
    info3.file_number = file_number3
    info3.cf_name = cf2
    info3.creation_time = time1_plus_10_sec
    info3.deletion_time = None
    assert monitor.new_event(creation_event3)
    assert monitor.get_all_files() == {cf1: [info1, info2], cf2: [info3]}
    assert monitor.get_all_cf_files(cf1) == [info1, info2]
    assert monitor.get_all_cf_files(cf2) == [info3]
    assert monitor.get_all_live_files() == {cf1: [info2], cf2: [info3]}
    assert monitor.get_cf_live_files(cf1) == [info2]
    assert monitor.get_cf_live_files(cf2) == [info3]
