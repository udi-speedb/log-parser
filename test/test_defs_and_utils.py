import defs_and_utils


def test_compare_times():
    time1 = "2022/11/24-15:50:09.512106"
    time2 = "2022/11/24-15:51:09.512106"

    assert defs_and_utils.compare_times(time1, time1) == 0
    assert defs_and_utils.compare_times(time1, time2) < 0
    assert defs_and_utils.compare_times(time2, time1) > 0
