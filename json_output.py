import json
import calc_utils
import display_utils
import defs_and_utils


def get_json(parsed_log):
    j = dict()

    cf_names = parsed_log.get_cf_names()
    events_mngr = parsed_log.get_events_mngr()
    stats_mngr = parsed_log.get_stats_mngr()

    j["General"] = display_utils.prepare_db_wide_info_for_display(parsed_log)
    j["General"]["CF-s"] = \
        display_utils.prepare_general_cf_info_for_display(parsed_log)

    j["Options"] = {
        "Diff":
            display_utils.get_options_baseline_diff_for_display(parsed_log),
        "All Options": display_utils.get_all_options_for_display(parsed_log)
    }

    j["Warnings"] = display_utils.prepare_warnings_for_display(parsed_log)

    events = calc_utils.calc_all_events_histogram(cf_names, events_mngr)
    if events:
        j["Events"] = events
    else:
        j["Events"] = "No Events"

    flushes = display_utils.prepare_flushes_histogram_for_display(parsed_log)
    if flushes:
        j["Flushes"] = flushes
    else:
        j["Flushes"] = "No Flushes"

    j["Stalls"] = \
        {"DB-Wide":
            display_utils.prepare_db_wide_stalls_entries_for_display(
                parsed_log),
         "CF-s":
             display_utils.prepare_cf_stalls_entries_for_display(parsed_log)}

    j["Compaction-Stats"] = \
        stats_mngr.get_compaction_stats_mngr().get_level_entries()

    block_cache_stats =\
        stats_mngr.get_block_cache_stats_mngr().get_all_cache_entries()
    if block_cache_stats:
        j["Block-Cache-Stats"] = block_cache_stats
    else:
        j["Block-Cache-Stats"] = "No Block Cache Stats"

    return j


def write_json(json_file_name, json_content):
    json_path = defs_and_utils.get_json_file_path(json_file_name)
    with json_path.open(mode='w') as json_file:
        json.dump(json_content, json_file)
    print(f"JSON Output is in {json_path}")
