import csv
import io
import logging
import defs_and_utils


def get_counters_csv(counter_and_histograms_mngr):
    f = io.StringIO()
    writer = csv.writer(f)

    mngr = counter_and_histograms_mngr
    # Get all counters for which at least one entry is not 0 (=> there is at
    # least one value that should be included for them in the CSV)
    all_applicable_entries = mngr.get_counters_entries_not_all_zeroes()

    if not all_applicable_entries:
        logging.info("No counters with non-zero values => NO CSV")
        return None

    counters_names = list(all_applicable_entries.keys())
    times = mngr.get_counters_times()

    # Support counter entries with missing entries for some time point
    # Maintain an index per counter that advances only when the counter has
    # a value per csv row (one row per time point)
    counters_idx = {name: 0 for name in counters_names}

    # csv header line (counter names)
    writer.writerow([""] + counters_names)

    # Write one line per time:
    for time_idx, time in enumerate(times):
        csv_line = list()
        csv_line.append(time)
        for counter_name in counters_names:
            counter_entries = all_applicable_entries[counter_name]

            value = 0
            if counters_idx[counter_name] < len(counter_entries):
                counter_entry_time =\
                    counter_entries[counters_idx[counter_name]]["time"]
                time_diff = defs_and_utils.compare_times(counter_entry_time,
                                                         time)
                assert time_diff >= 0
                if time_diff == 0:
                    value =\
                        counter_entries[counters_idx[counter_name]]["value"]
                    counters_idx[counter_name] += 1

            csv_line.append(value)

        writer.writerow(csv_line)

    return f.getvalue()


def get_histogram_csv(counter_and_histograms_mngr):
    f = io.StringIO()
    writer = csv.writer(f)

    mngr = counter_and_histograms_mngr
    # Get all histograms for which at least one entry is not 0 (=> there is at
    # least one value that should be included for them in the CSV)
    all_applicable_entries = mngr.get_histogram_entries_not_all_zeroes()

    if not all_applicable_entries:
        logging.info("No Histograms with non-zero values => NO CSV")
        return None

    counters_names = list(all_applicable_entries.keys())
    times = mngr.get_histogram_counters_times()

    # Support histogram entries with missing entries for some time point
    # Maintain an index per histogram that advances only when the histogram has
    # a value per csv row (one row per time point)
    histograms_idx = {name: 0 for name in counters_names}

    # csv header lines (counter names)
    header_line1 = [""]
    header_line2 = [""]
    counter_histogram_columns =\
        list(all_applicable_entries[counters_names[0]][0]["values"].keys())
    num_counter_columns = len(counter_histogram_columns)
    for counter_name in counters_names:
        name_columns = ["" for i in range(num_counter_columns)]
        name_columns[int(num_counter_columns/2)] = counter_name
        header_line1.extend(name_columns)

        header_line2.extend(counter_histogram_columns)

    writer.writerow(header_line1)
    writer.writerow(header_line2)

    # Write one line per time:
    zero_values = [0 for i in range(num_counter_columns)]
    for time_idx, time in enumerate(times):
        csv_line = list()
        csv_line.append(time)
        for counter_name in counters_names:
            histogram_entries = all_applicable_entries[counter_name]

            values = zero_values
            idx = histograms_idx[counter_name]
            if idx < len(histogram_entries):
                counter_entry_time = histogram_entries[idx]["time"]
                time_diff = defs_and_utils.compare_times(counter_entry_time,
                                                         time)
                assert time_diff >= 0
                if time_diff == 0:
                    values = list(histogram_entries[idx]["values"].values())
                    histograms_idx[counter_name] += 1

            csv_line.extend(values)

        writer.writerow(csv_line)

    return f.getvalue()


def get_compaction_stats_csv(compaction_stats_mngr):
    f = io.StringIO()
    writer = csv.writer(f)

    entries = compaction_stats_mngr.get_level_entries()

    if not entries:
        logging.info("No Compaction Stats => NO CSV")
        return None

    temp = list(list(entries.values())[0].values())[0]
    columns_names = list(list(temp.values())[0].keys())
    header_line = ["time", "Column Family", "Level"] + columns_names
    writer.writerow(header_line)

    for time, time_entry in entries.items():
        for cf_name, cf_entry in time_entry.items():
            for level, level_values in cf_entry.items():
                row = [time, cf_name, level]
                row += list(level_values.values())
                writer.writerow(row)

    return f.getvalue()
