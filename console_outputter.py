import io
import logging
import defs_and_utils
from log_file import ParsedLog
import display_utils


def get_title(log_file_path, parsed_log):
    metadata = parsed_log.get_metadata()
    log_time_span = metadata.get_log_time_span_seconds()
    title = f"{log_file_path} " \
            f"({log_time_span:.1f} Seconds) " \
            f"[{str(metadata.get_start_time())} - " \
            f"{str(metadata.get_end_time())}]"
    return title


def print_title(f, log_file_path, parsed_log):
    title = get_title(log_file_path, parsed_log)
    print(f"{title}", file=f)
    print(len(title) * "=", file=f)


def print_cf_console_printout(f, parsed_log):
    cfs_info_for_display = \
        display_utils.prepare_general_cf_info_for_display(parsed_log)
    cfs_names = parsed_log.get_cf_names_that_have_options()

    cfs_display_values = []
    for i, cf_name in enumerate(cfs_names):
        cf_info = cfs_info_for_display[cf_name]
        row = [
            cf_name,
            cf_info["CF Size"],
            cf_info["Avg Key Size"],
            cf_info["Avg Value Size"],
            cf_info["Compaction Style"],
            cf_info["Compression"],
            cf_info["Filter-Policy"]
        ]
        cfs_display_values.append(row)

    table_header = ["Column Family",
                    "Size",
                    "Avg Key Size",
                    "Avg Value Size",
                    "Compaction Style",
                    "Compression",
                    "Filter-Policy"]
    ascii_table = display_utils.generate_ascii_table(table_header,
                                                     cfs_display_values)
    print(ascii_table, file=f)


def print_general_info(f, parsed_log: ParsedLog):
    display_general_info_dict = \
        display_utils.prepare_db_wide_info_for_display(parsed_log)

    width = 25
    for field_name, value in display_general_info_dict.items():
        print(f"{field_name.ljust(width)}: {value}", file=f)
    print_cf_console_printout(f, parsed_log)


def get_console_output(log_file_path, parsed_log, output_type):
    logging.debug(f"Preparing {output_type} Console Output")

    f = io.StringIO()

    if output_type == defs_and_utils.ConsoleOutputType.SHORT:
        print_title(f, log_file_path, parsed_log)
        print_general_info(f, parsed_log)

    return f.getvalue()
