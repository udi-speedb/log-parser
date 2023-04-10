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

import os
import sys
import io
import argparse
import textwrap
import logging
import pathlib
import shutil
import logging.config
import defs_and_utils
import json_outputter
import console_outputter
import csv_outputter
from log_file import ParsedLog


DEBUG_MODE = True


def parse_log(log_file_path):
    if not os.path.isfile(log_file_path):
        raise defs_and_utils.LogFileNotFoundError(log_file_path)

    logging.debug(f"Parsing {log_file_path}")
    with open(log_file_path) as log_file:
        log_lines = log_file.readlines()
        # log_lines = [line.strip() for line in log_lines]
        return ParsedLog(log_file_path, log_lines)


def setup_cmd_line_parser():
    epilog = textwrap.dedent('''\
    Notes:
    - The default it to print to the console in a short format.
    - It is possible to specify both json and console outputs. Both will be generated.
    ''') # noqa

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog)
    parser.add_argument("log_file_path",
                        metavar="log-file-path",
                        help="A path to a log file to parse "
                             "(default: %(default)s)")
    parser.add_argument("-c", "--console",
                        choices=["short", "long"],
                        help="Print to console a summary (short) or a "
                             "detailed (long) output (default: %(default)s)")
    parser.add_argument("-j", "--json-file-name",
                        metavar="[json file name]",
                        help="Optional output json file name"
                             " (default: %(default)s)")
    parser.add_argument("-o", "--output-folder",
                        default=defs_and_utils.DEFAULT_OUTPUT_FOLDER,
                        help='''
                        Optional folder where a sub-folder containing the parser'
                        output files will be written (default: "%(default)s")
                        ''')
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


def setup_logger(output_folder):
    my_log_file_path = defs_and_utils.get_log_file_path(output_folder)
    logging.basicConfig(filename=my_log_file_path,
                        format="%(asctime)s - %(levelname)s [%(filename)s "
                               "(%(funcName)s:%(lineno)d)] %(message)s)",
                        level=logging.DEBUG)
    return my_log_file_path


def prepare_output_folder(output_folder_parent):
    output_path_parent = pathlib.Path(output_folder_parent)
    largest_num = 0
    if output_path_parent.exists():
        for file in output_path_parent.iterdir():
            name = file.name
            if name.startswith(defs_and_utils.OUTPUT_SUB_FOLDER_PREFIX):
                name = name[len(defs_and_utils.OUTPUT_SUB_FOLDER_PREFIX):]
                if name.isnumeric() and len(name) == 4:
                    num = int(name)
                    largest_num = max(largest_num, num)

        if largest_num == 9999:
            largest_num = 1

    output_folder = f"{output_folder_parent}/log_parser_{largest_num + 1:04}"
    if not output_path_parent.exists():
        os.makedirs(output_folder_parent)
    shutil.rmtree(output_folder, ignore_errors=True)
    os.makedirs(output_folder)
    return output_folder


def print_to_console_if_applicable(cmdline_args, log_file_path,
                                   parsed_log, json_content):
    f = io.StringIO()
    console_content = None
    if cmdline_args.console == defs_and_utils.ConsoleOutputType.SHORT:
        f.write(
            console_outputter.get_console_output(
                log_file_path,
                parsed_log,
                cmdline_args.console))
        console_content = f.getvalue()
    elif cmdline_args.console == defs_and_utils.ConsoleOutputType.LONG:
        console_content = json_outputter.get_json_dump_str(json_content)

    if console_content is not None:
        print(console_content)


def generate_json_if_applicable(cmdline_args, parsed_log, output_folder):
    json_content = None
    if cmdline_args.json_file_name or \
            cmdline_args.console == defs_and_utils.ConsoleOutputType.LONG:
        json_content = json_outputter.get_json(parsed_log)
        if cmdline_args.json_file_name:
            json_outputter.write_json(cmdline_args.json_file_name,
                                      json_content, output_folder)

    return json_content


def generate_counters_csv(mngr, output_folder):
    counters_csv = csv_outputter.get_counters_csv(mngr)

    if counters_csv:
        counters_csv_file_name = \
            defs_and_utils.get_counters_csv_file_path(output_folder)
        with open(counters_csv_file_name, "w") as f:
            f.write(counters_csv)
        logging.info(f"Counters CSV Is in {counters_csv_file_name}")
    else:
        logging.info("No Counters to report")


def generate_human_readable_histograms_csv(mngr, output_folder):
    histograms_csv = \
        csv_outputter.get_human_readable_histogram_csv(mngr)
    if histograms_csv:
        histograms_csv_file_name = \
            defs_and_utils.\
            get_human_readable_histograms_csv_file_path(output_folder)
        with open(histograms_csv_file_name, "w") as f:
            f.write(histograms_csv)
        logging.info(f"Human Readable Counters Histograms CSV Is in"
                     f" {histograms_csv_file_name}")
        return True
    else:
        logging.info("No Counters Histograms to report")
        return True


def generate_tools_histograms_csv(mngr, output_folder):
    histograms_csv = \
        csv_outputter.get_tools_histogram_csv(mngr)
    if histograms_csv:
        histograms_csv_file_name = \
            defs_and_utils.get_tools_histograms_csv_file_path(output_folder)
        with open(histograms_csv_file_name, "w") as f:
            f.write(histograms_csv)
        logging.info(f"Tools Counters Histograms CSV Is in"
                     f" {histograms_csv_file_name}")
    else:
        logging.info("No Counters Histograms to report")


def generate_histograms_csv(mngr, output_folder):
    if not generate_human_readable_histograms_csv(mngr, output_folder):
        return
    generate_tools_histograms_csv(mngr, output_folder)


def generate_compaction_csv(mngr, output_folder):
    compaction_stats_csv = csv_outputter.get_compaction_stats_csv(mngr)
    if compaction_stats_csv:
        compactions_csv_file_name = \
            defs_and_utils.get_compaction_stats_csv_file_path(output_folder)
        with open(compactions_csv_file_name, "w") as f:
            f.write(compaction_stats_csv)
        logging.info(f"Compactions CSV Is in {compactions_csv_file_name}")
    else:
        logging.info("No Compaction Stats to report")


def generate_csvs_if_applicable(parsed_log, output_folder):
    stats_mngr = parsed_log.get_stats_mngr()

    counters_and_histograms_mngr = stats_mngr.get_counter_and_histograms_mngr()
    generate_counters_csv(counters_and_histograms_mngr, output_folder)
    generate_histograms_csv(counters_and_histograms_mngr, output_folder)

    compaction_stats_mngr = stats_mngr.get_compaction_stats_mngr()
    generate_compaction_csv(compaction_stats_mngr, output_folder)


def main():
    parser = setup_cmd_line_parser()
    cmdline_args = parser.parse_args()
    output_folder = cmdline_args.output_folder

    output_folder = prepare_output_folder(output_folder)
    my_log_file_path = setup_logger(output_folder)
    validate_and_sanitize_cmd_line_args(cmdline_args)

    try:
        log_file_path = cmdline_args.log_file_path
        parsed_log = parse_log(log_file_path)

        json_content = generate_json_if_applicable(cmdline_args, parsed_log,
                                                   output_folder)
        print_to_console_if_applicable(cmdline_args, log_file_path, parsed_log,
                                       json_content)
        generate_csvs_if_applicable(parsed_log, output_folder)
    except defs_and_utils.ParsingError as exception:
        report_error(exception)
    except defs_and_utils.LogFileNotFoundError as exception:
        report_error(exception)
        print(exception.msg, file=sys.stderr)
    except defs_and_utils.EmptyLogFile as exception:
        report_error(exception)
        print(exception.msg, file=sys.stderr)
    except defs_and_utils.InvalidLogFile as exception:
        report_error(exception)
        print(exception.msg, file=sys.stderr)
    except ValueError as exception:
        handle_exception(exception)
    except AssertionError as exception:
        if not DEBUG_MODE:
            handle_exception(exception)
        else:
            raise
    except Exception as exception:  # noqa
        if not DEBUG_MODE:
            print(f"An unrecoverable error occurred while parsing "
                  f"{log_file_path}."
                  f"\nMore details may be found in {my_log_file_path}",
                  file=sys.stderr)
            handle_exception(exception)
        else:
            raise


if __name__ == '__main__':
    main()
