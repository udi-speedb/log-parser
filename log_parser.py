import os
import io
import argparse
import textwrap
import logging
import pathlib
import shutil
import logging.config
import defs_and_utils
import json_output
import console_outputter
from log_file import ParsedLog
import csv_outputter


def parse_log(log_file_path):
    if not os.path.isfile(log_file_path):
        raise defs_and_utils.LogFileNotFoundError(log_file_path)

    logging.debug(f"Parsing {log_file_path}")
    with open(log_file_path) as log_file:
        log_lines = log_file.readlines()
        log_lines = [line.strip() for line in log_lines]
        return ParsedLog(log_file_path, log_lines)


def setup_cmd_line_parser():
    epilog = textwrap.dedent('''\
    Notes:
    - The default it to print to the console in a short format
    - It is possible to specify both json and console outputs. Both will be 
       generated.
    ''') # noqa

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog)
    parser.add_argument("log",
                        metavar="log file path",
                        help="A path to a log file to parse")
    parser.add_argument("-c", "--console",
                        choices=["short", "full"],
                        help="Print to console a summary (short) or a "
                             "full output ")
    parser.add_argument("-j", "--json-file-name",
                        metavar="[json file name]",
                        help="Optional output json file name.")

    return parser


def validate_and_sanitize_cmd_line_args(cmdline_args):
    # The default is short console output
    if not cmdline_args.console and not cmdline_args.json_file_name:
        cmdline_args.console = defs_and_utils.ConsoleOutputType.SHORT


def report_error(e):
    logging.error(e.msg)


def handle_exception(e):
    logging.exception(f"\n{e}")
    exit(1)


def setup_logger():
    logging.config.fileConfig(fname='logger.conf',
                              disable_existing_loggers=True)


def prepare_artefacts_folder():
    artefacts_path = pathlib.Path(defs_and_utils.ARTEFACTS_FOLDER)
    if artefacts_path.exists():
        shutil.rmtree(defs_and_utils.ARTEFACTS_FOLDER)
    artefacts_path.mkdir()


def print_to_console_if_applicable():
    if cmdline_args.console:
        f.write(
            console_outputter.get_console_output(
                log_file_path,
                parsed_log,
                cmdline_args.console))
        print(f"{f.getvalue()}")


def generate_json_if_applicable():
    if cmdline_args.json_file_name:
        json_content = json_output.get_json(parsed_log)
        json_output.write_json(cmdline_args.json_file_name, json_content)


def generate_counters_csv(mngr):
    counters_csv = csv_outputter.get_counters_csv(mngr)

    if counters_csv:
        counters_csv_file_name = \
            defs_and_utils.get_default_counters_csv_file_path()
        with open(counters_csv_file_name, "w") as f:
            f.write(counters_csv)
        print(f"Counters CSV Is in {counters_csv_file_name}")
    else:
        print("No Counters to report")


def generate_histograms_csv(mngr):
    histograms_csv = csv_outputter.get_histogram_csv(mngr)
    if histograms_csv:
        histograms_csv_file_name = \
            defs_and_utils.get_default_histograms_csv_file_path()
        with open(histograms_csv_file_name, "w") as f:
            f.write(histograms_csv)
        print(f"Counters CSV Is in {histograms_csv_file_name}")
    else:
        print("No Counters Histograms to report")


def generate_compaction_csv(mngr):
    compaction_stats_csv = csv_outputter.get_compaction_stats_csv(mngr)
    if compaction_stats_csv:
        compactions_csv_file_name = \
            defs_and_utils.get_default_compaction_stats_csv_file_path()
        with open(compactions_csv_file_name, "w") as f:
            f.write(compaction_stats_csv)
        print(f"Compactions CSV Is in {compactions_csv_file_name}")
    else:
        print("No Compaction Stats to report")


def generate_csvs_if_applicable():
    stats_mngr = parsed_log.get_stats_mngr()

    counters_and_histograms_mngr = stats_mngr.get_counter_and_histograms_mngr()
    generate_counters_csv(counters_and_histograms_mngr)
    generate_histograms_csv(counters_and_histograms_mngr)

    compaction_stats_mngr = stats_mngr.get_compaction_stats_mngr()
    generate_compaction_csv(compaction_stats_mngr)


if __name__ == '__main__':
    prepare_artefacts_folder()
    setup_logger()
    parser = setup_cmd_line_parser()
    cmdline_args = parser.parse_args()
    validate_and_sanitize_cmd_line_args(cmdline_args)

    e = None
    try:
        f = io.StringIO()
        log_file_path = cmdline_args.log
        parsed_log = parse_log(log_file_path)

        print_to_console_if_applicable()
        generate_json_if_applicable()
        generate_csvs_if_applicable()
    except defs_and_utils.ParsingError as e:
        report_error(e)
    except defs_and_utils.LogFileNotFoundError as e:
        report_error(e)
    except ValueError as e:
        handle_exception(e)
    except AssertionError as e:
        handle_exception(e)
    except Exception as e:  # noqa
        handle_exception(e)
