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

import re
import json
import logging
from enum import Enum
from dataclasses import dataclass

import defs_and_utils
import regexes
from log_entry import LogEntry


class EventType(str, Enum):
    FLUSH_STARTED = "flush_started"
    FLUSH_FINISHED = "flush_finished"
    COMPACTION_STARTED = "compaction_started"
    COMPACTION_FINISHED = "compaction_finished"
    TABLE_FILE_CREATION = 'table_file_creation'
    TABLE_FILE_DELETION = "table_file_deletion"
    TRIVIAL_MOVE = "trivial_move"
    RECOVERY_STARTED = "recovery_started"
    RECOVERY_FINISHED = "recovery_finished"
    INGEST_FINISHED = "ingest_finished"
    BLOB_FILE_CREATION = "blob_file_creation"
    BLOB_FILE_DELETION = "blob_file_deletion"
    UNKNOWN = "UNKNOWN"

    def __str__(self):
        return str(self.value)

    @staticmethod
    def get_type(event_type_str):
        try:
            return EventType(event_type_str)
        except ValueError:
            return EventType.UNKNOWN


class Event:
    @dataclass
    class EventPreambleInfo:
        cf_name: str
        type: str
        job_id: str

    @staticmethod
    def is_an_event_entry(log_entry, cf_names):
        assert isinstance(log_entry, LogEntry)
        return \
            re.findall(regexes.EVENT_REGEX, log_entry.get_msg()) != \
            []

    @staticmethod
    def try_parse_event_preamble(log_entry, cf_names):
        cf_preamble_match = re.findall(regexes.PREAMBLE_EVENT_REGEX,
                                       log_entry.get_msg())
        if not cf_preamble_match:
            return None
        cf_name = cf_preamble_match[0][0]
        if cf_name not in cf_names:
            return None

        job_id = int(cf_preamble_match[0][1])
        rest_of_msg = cf_preamble_match[0][2].strip()
        if rest_of_msg.startswith('Compacting '):
            event_type = EventType.COMPACTION_STARTED
        elif rest_of_msg.startswith('Flushing memtable with next log file'):
            event_type = EventType.FLUSH_STARTED
        else:
            return None

        return Event.EventPreambleInfo(cf_name, event_type, job_id)

    def __init__(self, log_entry, cf_names):
        assert Event.is_an_event_entry(log_entry, cf_names)

        entry_msg = log_entry.get_msg()
        event_json_str = entry_msg[entry_msg.find("{"):]

        self.event_details_dict = None
        try:
            self.event_details_dict = json.loads(event_json_str)
        except json.JSONDecodeError:
            logging.error(f"Error decoding event's json fields.\n{log_entry}")

        if self.does_have_details() and \
                "cf_name" not in self.event_details_dict:
            self.event_details_dict["cf_name"] = defs_and_utils.NO_COL_FAMILY

        self.event_type = None
        if self.does_have_details():
            self.event_type = EventType.get_type(self.get_type_str())

    def __str__(self):
        if not self.does_have_details():
            return "Event: No Details"

        # Accessing all fields via their methods and not logging errors
        # to avoid endless recursion
        return f"Event: type:{self.get_type(log_error=False)}, job-id:" \
               f"{self.get_job_id(log_error=False)}, " \
               f"cf:{self.get_cf_name(log_error=False)}"

    # By default, sort events based on their time
    def __lt__(self, other):
        return self.get_time() < other.get_time()

    def __eq__(self, other):
        return self.get_time() == other.get_time() and \
               self.get_type() == other.get_type() and\
               self.get_cf_name() == other.get_cf_name()

    def does_have_details(self):
        return self.event_details_dict is not None

    def is_valid(self):
        return self.does_have_details() and self.get_type() is not None

    def get_type_str(self, log_error=True):
        type = self.get_type(log_error)
        return str(type) if type is not None else "Type Unavailable"

    def get_type(self, log_error=True):
        return self.get_event_data_by_key("event", log_error)

    def get_job_id(self, log_error=True):
        return self.get_event_data_by_key("job", log_error)

    def get_time(self, log_error=True):
        return self.get_event_data_by_key("time_micros", log_error)

    def get_event_data_by_key(self, key, log_error):
        if not self.does_have_details():
            if log_error:
                logging.error(f"No event details, can't find key "
                              f"({key}). {self}")
            return None

        if key not in self.event_details_dict:
            if log_error:
                logging.error(f"Can't find key ({key}). {self}")
            return None

        return self.event_details_dict[key]

    def is_db_wide_event(self):
        return self.get_cf_name() == defs_and_utils.NO_COL_FAMILY

    def is_cf_event(self):
        return not self.is_db_wide_event()

    def get_cf_name(self, log_error=True):
        return self.get_event_data_by_key("cf_name", log_error)

    def try_adding_preamble_event(self, event_preamble_type, cf_name):
        if self.get_type() != event_preamble_type:
            return False

        # Add the cf_name as if it was part of the event
        self.event_details_dict["cf_name"] = cf_name
        return True


class EventsMngr:
    """
    The events manager contains all of the events.

    It stores them in a dictionary of the following format:
    <cf-name>: Dictionary of cf events
    (The db-wide events are stored under the "cf name" No_COL_NAME)

    Dictionary of cf events is itself a dictionary of the following format:
    <event-type>: List of Event-s, ordered by their time
    """
    def __init__(self, cf_names=[]):
        self.cf_names = cf_names
        self.preambles = dict()
        self.events = dict()

    def try_parsing_as_preamble(self, entry):
        preamble_info = \
            Event.try_parse_event_preamble(entry, self.cf_names)
        if not preamble_info:
            return False

        (cf_name, event_type, job_id) = (preamble_info.cf_name,
                                         preamble_info.type,
                                         preamble_info.job_id)

        # If a preamble was already encountered, it must be for the same
        # parameters
        if job_id in self.preambles:
            assert self.preambles[job_id] == (event_type, cf_name)
        self.preambles[job_id] = (event_type, cf_name)

        return True

    def try_adding_entry(self, entry):
        assert isinstance(entry, LogEntry)

        event_cf_name = None

        # A preamble event is an entry that will be pending for its
        # associated event entry to provide the event with its cf name
        if self.try_parsing_as_preamble(entry):
            return True, event_cf_name

        if not Event.is_an_event_entry(entry, self.cf_names):
            return False, event_cf_name

        event = Event(entry, self.cf_names)
        if not event.is_valid():
            # telling caller I have added it as an event since it's
            # supposedly and event, but badly formatted somehow
            logging.error(f"Discarding badly constructed event.\n{entry}")
            return True, event_cf_name

        # Combine associated event preamble, if any exists
        event_job_id = event.get_job_id(log_error=False)
        if event_job_id is not None and event_job_id in self.preambles:
            preamble_info = self.preambles[event_job_id]
            if event.try_adding_preamble_event(preamble_info[0],
                                               preamble_info[1]):
                del(self.preambles[event_job_id])

        event_cf_name = event.get_cf_name()
        event_type = event.get_type()

        if event_cf_name not in self.events:
            self.events[event_cf_name] = dict()
        if event_type not in self.events[event_cf_name]:
            self.events[event_cf_name][event_type] = []
        self.events[event_cf_name][event_type].append(event)

        if event_cf_name == defs_and_utils.NO_COL_FAMILY:
            event_cf_name = None

        return True, event_cf_name

    def add_cf_name(self, cf_name):
        if cf_name not in self.cf_names:
            self.cf_names.append(cf_name)

    def get_cf_events(self, cf_name):
        if cf_name not in self.events:
            return []

        all_cf_events = []
        for cf_events in self.events[cf_name].values():
            all_cf_events.extend(cf_events)

        # Return the events sorted by their time
        all_cf_events.sort()
        return all_cf_events

    def get_cf_events_by_type(self, cf_name, event_type):
        assert isinstance(event_type, EventType)

        if cf_name not in self.events:
            return []
        if event_type not in self.events[cf_name]:
            return []

        # The list may not be ordered due to the original time issue
        # or having event preambles matched to their events somehow
        # out of order. Sorting will insure correctness even if the list
        # is already sorted
        events = self.events[cf_name][event_type]
        events.sort()
        return events

    def debug_get_all_events(self):
        return self.events
