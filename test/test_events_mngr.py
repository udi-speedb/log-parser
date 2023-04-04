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
import defs_and_utils
from log_entry import LogEntry
from events_mngr import Event, EventsMngr, EventType


def create_event_entry(time_str, event_type=None, cf_name=None, job_id=100,
                       make_illegal_json=False):
    event_line = time_str + " "
    event_line += '7f4a8b5bb700 EVENT_LOG_v1 {"time_micros": '

    time_micros_part = time_str.split(".")[1]
    event_line += str(defs_and_utils.get_gmt_timestamp(time_str)) + \
        time_micros_part

    if event_type is not None:
        event_line += f', "event": "{str(event_type.value)}"'
    if cf_name is not None:
        event_line += f', "cf_name": "{cf_name}"'
    if job_id is not None:
        event_line += f', "job": {job_id}'
    if make_illegal_json:
        event_line += ", "

    event_line += '}'

    event_entry = LogEntry(0, event_line, True)
    assert Event.is_an_event_entry(event_entry, [cf_name])
    return event_entry


def verify_expected_events(events_mngr, expected_events_dict):
    # Expecting a dictionary of:
    # {<cf_name>: [<events entries for this cf>]}
    cf_names = list(expected_events_dict.keys())

    # prepare the expected events per (cf, event type)
    expected_cf_events_per_type = dict()
    for name in cf_names:
        expected_cf_events_per_type[name] = {event_type: [] for event_type
                                             in EventType}

    for cf_name, cf_events_entries in expected_events_dict.items():
        expected_cf_events = [Event(event_entry, cf_names) for event_entry
                              in cf_events_entries]
        assert events_mngr.get_cf_events(cf_name) == expected_cf_events

        for event in expected_cf_events:
            expected_cf_events_per_type[cf_name][event.get_type()].append(
                event)

        for event_type in EventType:
            assert events_mngr.get_cf_events_by_type(cf_name, event_type) ==\
                   expected_cf_events_per_type[cf_name][event_type]


def test_event_type():
    assert EventType.get_type("flush_finished") == EventType.FLUSH_FINISHED
    assert str(EventType.get_type("flush_finished")) == "flush_finished"

    assert EventType.get_type("Dummy") == EventType.UNKNOWN
    assert str(EventType.get_type("Dummy")) == "UNKNOWN"


@pytest.mark.parametrize("cf_name", ["default", ""])
def test_event(cf_name):
    cf_names = [cf_name]
    event_entry = create_event_entry("2022/04/17-14:42:19.220573",
                                     EventType.FLUSH_FINISHED, cf_name,
                                     35)

    assert Event.is_an_event_entry(event_entry, cf_names)
    assert not Event.try_parse_event_preamble(event_entry, cf_names)

    event1 = Event(event_entry, cf_names)
    assert event1.get_type_str() == "flush_finished"
    assert event1.get_type() == EventType.FLUSH_FINISHED
    assert event1.get_job_id() == 35
    assert event1.get_cf_name() == cf_name
    assert not event1.is_db_wide_event()
    assert event1.is_cf_event()
    event1 = None

    # Unknown event (legal)
    event_entry = create_event_entry("2022/04/17-14:42:19.220573",
                                     EventType.UNKNOWN, cf_name, job_id=35)
    event2 = Event(event_entry, cf_names)
    assert event2.get_type_str() == "UNKNOWN"
    assert event2.get_type() == EventType.UNKNOWN
    assert event2.get_job_id() == 35
    assert event2.get_cf_name() == cf_name
    assert not event2.is_db_wide_event()
    assert event2.is_cf_event()


@pytest.mark.parametrize("cf_name", ["default", ""])
def test_event_preamble(cf_name):
    preamble_line = f"""2022/04/17-14:42:11.398681 7f4a8b5bb700 
    [/flush_job.cc:333] [{cf_name}] [JOB 8] 
    Flushing memtable with next log file: 5
    """ # noqa

    preamble_line = " ".join(preamble_line.splitlines())

    cf_names = [cf_name, "dummy_cf"]

    preamble_entry = LogEntry(0, preamble_line, True)
    event_entry = create_event_entry("2022/11/24-15:58:17.683316",
                                     EventType.FLUSH_STARTED,
                                     defs_and_utils.NO_COL_FAMILY,
                                     8)

    assert not Event.try_parse_event_preamble(event_entry, ["dummy_cf"])

    preamble_info = Event.try_parse_event_preamble(preamble_entry, cf_names)
    assert preamble_info
    assert preamble_info.job_id == 8
    assert preamble_info.type == EventType.FLUSH_STARTED
    assert preamble_info.cf_name == cf_name

    assert Event.is_an_event_entry(event_entry, cf_names)
    assert not Event.try_parse_event_preamble(event_entry, cf_names)

    event = Event(event_entry, cf_names)
    assert event.get_type_str() == "flush_started"
    assert event.get_type() == EventType.FLUSH_STARTED
    assert event.get_job_id() == 8
    assert event.get_cf_name() == defs_and_utils.NO_COL_FAMILY
    assert event.is_db_wide_event()
    assert not event.is_cf_event()

    assert event.try_adding_preamble_event(preamble_info.type,
                                           preamble_info.cf_name)
    assert event.get_cf_name() == cf_name
    assert not event.is_db_wide_event()
    assert event.is_cf_event()


def test_illegal_events():
    cf1 = "CF1"
    cf_names = [cf1]
    # Illegal event json
    event_entry = create_event_entry("2022/04/17-14:42:19.220573",
                                     EventType.FLUSH_FINISHED, cf1,
                                     35, make_illegal_json=True)
    assert Event.try_parse_event_preamble(event_entry, cf_names) is None

    event = Event(event_entry, cf_names)
    assert not event.does_have_details()
    assert not event.is_valid()
    assert event.get_time() is None
    assert event.get_type_str() == "Type Unavailable"
    assert event.get_type() is None
    assert event.get_job_id() is None
    assert event.get_cf_name() is None
    assert not event.is_db_wide_event()
    assert event.is_cf_event()

    # Missing Job id (sort of illegal)
    event_entry = create_event_entry("2022/04/17-14:42:19.220573",
                                     EventType.FLUSH_FINISHED, cf_name=cf1,
                                     job_id=None)
    assert Event.try_parse_event_preamble(event_entry, cf_names) is None
    assert Event.is_an_event_entry(event_entry, [cf1])
    event = Event(event_entry, cf_names)
    assert event.does_have_details()
    assert event.is_valid()
    assert event.get_job_id() is None
    assert event.get_type() == EventType.FLUSH_FINISHED

    # Missing Event Type (definitely illegal)
    event_entry = create_event_entry("2022/04/17-14:42:19.220573",
                                     event_type=None, cf_name=cf1,
                                     job_id=200)
    assert Event.try_parse_event_preamble(event_entry, cf_names) is None
    assert Event.is_an_event_entry(event_entry, [cf1])
    event = Event(event_entry, cf_names)
    assert event.does_have_details()
    assert not event.is_valid()
    assert event.get_job_id() == 200
    assert event.get_type() is None


def test_adding_events_to_events_mngr():
    cf1 = "cf1"
    cf2 = "cf2"
    cf_names = [cf1, cf2]
    events_mngr = EventsMngr(cf_names)

    assert not events_mngr.get_cf_events(cf1)
    assert not events_mngr.get_cf_events_by_type(cf2,
                                                 EventType.FLUSH_FINISHED)

    expected_events_entries = {cf1: [], cf2: []}

    event1_entry = create_event_entry("2022/04/17-14:42:19.220573",
                                      EventType.FLUSH_FINISHED, cf1)
    assert events_mngr.try_adding_entry(event1_entry) == (True, cf1)
    expected_events_entries[cf1] = [event1_entry]
    verify_expected_events(events_mngr, expected_events_entries)

    event2_entry = create_event_entry("2022/04/18-14:42:19.220573",
                                      EventType.FLUSH_STARTED, cf2)
    assert events_mngr.try_adding_entry(event2_entry) == (True, cf2)
    expected_events_entries[cf2] = [event2_entry]
    verify_expected_events(events_mngr, expected_events_entries)

    # Create another cf1 event, but set its time to EARLIER than event1
    event3_entry = create_event_entry("2022/03/17-14:42:19.220573",
                                      EventType.FLUSH_FINISHED, cf1)
    assert events_mngr.try_adding_entry(event3_entry) == (True, cf1)
    # Expecting event3 to be before event1
    expected_events_entries[cf1] = [event3_entry, event1_entry]
    verify_expected_events(events_mngr, expected_events_entries)

    # Create some more cf21 event, later in time
    event4_entry = create_event_entry("2022/05/17-14:42:19.220573",
                                      EventType.COMPACTION_STARTED, cf2)
    event5_entry = create_event_entry("2022/05/17-15:42:19.220573",
                                      EventType.COMPACTION_STARTED, cf2)
    event6_entry = create_event_entry("2022/05/17-16:42:19.220573",
                                      EventType.COMPACTION_FINISHED, cf2)
    assert events_mngr.try_adding_entry(event4_entry) == (True, cf2)
    assert events_mngr.try_adding_entry(event5_entry) == (True, cf2)
    assert events_mngr.try_adding_entry(event6_entry) == (True, cf2)
    expected_events_entries[cf2] = [event2_entry, event4_entry,
                                    event5_entry, event6_entry]
    verify_expected_events(events_mngr, expected_events_entries)


def test_try_adding_invalid_event_to_events_mngr():
    cf1 = "cf1"
    cf2 = "cf2"
    cf_names = [cf1, cf2]
    events_mngr = EventsMngr(cf_names)

    # Illegal event json
    invalid_event_entry = create_event_entry("2022/04/17-14:42:19.220573",
                                             EventType.FLUSH_FINISHED, cf1,
                                             35, make_illegal_json=True)

    assert events_mngr.try_adding_entry(invalid_event_entry) == (True, None)
    assert events_mngr.debug_get_all_events() == {}

    event1_entry = create_event_entry("2022/04/17-14:42:19.220573",
                                      EventType.FLUSH_FINISHED, cf1)
    assert events_mngr.try_adding_entry(event1_entry) == (True, cf1)
    assert len(events_mngr.debug_get_all_events()) == 1

    assert events_mngr.try_adding_entry(invalid_event_entry) == (True, None)
    assert len(events_mngr.debug_get_all_events()) == 1
