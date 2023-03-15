import re
from dataclasses import dataclass
from enum import Enum, auto
import defs_and_utils
import regexes
import logging


get_error_context = defs_and_utils.get_error_context_from_entry


@dataclass
class CfMetadata:
    name: str
    discovery_time: str
    has_options: bool
    auto_generated: bool
    id: int = None
    drop_time: str = None


class CfOper(Enum):
    CREATE = auto()
    RECOVER = auto()
    DROP = auto()


class CfsMetadata:
    def __init__(self, log_file_path):
        self.cfs_info = {}
        self.log_file_path = log_file_path

    def add_cf_found_during_cf_options_parsing(self, cf_name, cf_id,
                                               is_auto_generated, entry):
        self.validate_cf_doesnt_exist(cf_name, entry)

        has_options = True
        self.cfs_info[cf_name] = CfMetadata(cf_name,
                                            entry.get_time(),
                                            has_options,
                                            is_auto_generated,
                                            id=cf_id)

    def handle_cf_name_found_during_parsing(self, cf_name, entry):
        if cf_name in self.cfs_info:
            return False

        logging.info(f"Found cf name during parsing [{cf_name}]")
        has_options = False
        is_auto_generated = False
        self.cfs_info[cf_name] = CfMetadata(cf_name,
                                            entry.get_time(),
                                            has_options,
                                            is_auto_generated)
        return True

    def try_parse_as_cf_lifetime_entries(self, log_entries, entry_idx):
        # CF lifetime messages in log are currently always single liners
        entry = log_entries[entry_idx]
        next_entry_idx = entry_idx + 1

        # The cf lifetime entries are printed when the CF will have been
        # discovered
        if self.try_parse_as_drop_cf(entry):
            return True, next_entry_idx
        if self.try_parse_as_recover_cf(entry):
            return True, next_entry_idx
        if self.try_parse_as_create_cf(entry):
            return True, next_entry_idx

        return None, entry_idx

    def try_parse_as_drop_cf(self, entry):
        cf_id_match = re.findall(regexes.DROP_CF_REGEX, entry.get_msg())
        if not cf_id_match:
            return False

        assert len(cf_id_match) == 1

        dropped_cf_id = cf_id_match[0]
        # A dropped cf may have been discovered without knowing its id (
        # e.g., auto-generated one)
        cf_info = self.get_cf_info_by_id(dropped_cf_id)
        if not cf_info:
            logging.info(defs_and_utils.format_err_msg(
                f"CF with Id {dropped_cf_id} "
                f"dropped but no cf with matching id.", error_context=None,
                entry=entry, file_path=self.log_file_path))
            return True

        self.validate_cf_wasnt_dropped(cf_info, entry)
        cf_info.drop_time = entry.get_time()

        return True

    def try_parse_as_recover_cf(self, entry):
        cf_match = re.findall(regexes.RECOVERED_CF_REGEX, entry.get_msg())
        if not cf_match:
            return False

        assert len(cf_match) == 1 and len(cf_match[0]) == 2

        cf_name, cf_id = cf_match[0]
        self.validate_cf_exists(cf_name, entry)

        self.update_cf_internal(cf_name, cf_id, entry)
        return True

    def try_parse_as_create_cf(self, entry):
        cf_match = re.findall(regexes.CREATE_CF_REGEX, entry.get_msg())
        if not cf_match:
            return False

        assert len(cf_match) == 1 and len(cf_match[0]) == 2

        cf_name, cf_id = cf_match[0]
        self.update_cf_internal(cf_name, cf_id, entry)
        return True

    def update_cf_internal(self, cf_name, cf_id, entry):
        self.validate_cf_exists(cf_name, entry)
        self.validate_cf_id_unknown_or_same(cf_name, cf_id, entry)

        is_auto_generated = False
        self.cfs_info[cf_name] = \
            CfMetadata(cf_name, entry.get_time(), int(cf_id),
                       is_auto_generated)

    def set_cf_id(self, cf_name, cf_id, line, line_idx):
        self.validate_cf_exists(cf_name, line, line_idx)
        self.validate_cf_id_unknown_or_same(cf_name, cf_id, line, line_idx)

        self.cfs_info[cf_name].id = int(cf_id)

    def get_cf_id(self, cf_name):
        cf_id = None
        if cf_name in self.cfs_info:
            cf_id = self.cfs_info[cf_name].id
        return cf_id

    def get_cf_info_by_name(self, cf_name):
        cf_info = None
        if cf_name in self.cfs_info:
            cf_info = self.cfs_info[cf_name]
        return cf_info

    def get_cf_info_by_id(self, cf_id):
        cf_id = int(cf_id)
        for cf_info in self.cfs_info.values():
            if cf_info.id == cf_id:
                return cf_info

        return None

    def get_non_auto_generated_cf_names(self):
        return [cf_name for cf_name in self.cfs_info.keys() if not
                self.cfs_info[cf_name].auto_generated]

    def get_auto_generated_cf_names(self):
        return [cf_name for cf_name in self.cfs_info.keys() if
                self.cfs_info[cf_name].auto_generated]

    def get_num_cfs(self):
        return len(self.cfs_info) if self.cfs_info else 0

    def was_cf_dropped(self, cf_name):
        cf_info = self.get_cf_info_by_name(cf_name)
        if not cf_info:
            return False
        return cf_info.drop_time is not None

    def get_cf_drop_time(self, cf_name):
        drop_time = None
        cf_info = self.get_cf_info_by_name(cf_name)
        if cf_info:
            drop_time = cf_info.drop_time
        return drop_time

    def validate_cf_exists(self, cf_name, entry):
        if cf_name not in self.cfs_info:
            raise defs_and_utils.ParsingError(
                f"cf ({cf_name}) doesn't exist.",
                self.get_error_context(entry))

    def validate_cf_doesnt_exist(self, cf_name, entry):
        if cf_name in self.cfs_info:
            raise defs_and_utils.ParsingError(
                f"cf ({cf_name}) already exists.",
                self.get_error_context(entry))

    def validate_known_cf_id(self, cf_id, entry):
        if not self.get_cf_info_by_id(cf_id):
            raise defs_and_utils.ParsingError(
                f"No CF has that id ({cf_id}).", self.get_error_context(entry))

    def validate_cf_id_unknown_or_same(self, cf_name, cf_id, entry):
        curr_cf_id = self.cfs_info[cf_name].id
        if curr_cf_id and curr_cf_id != int(cf_id):
            raise defs_and_utils.ParsingError(
                f"id ({cf_id}) of cf ({cf_name}) already exists.",
                self.get_error_context(entry))

    def validate_cf_is_not_auto_generated(self, cf_name, entry):
        if self.cfs_info[cf_name].auto_generated:
            raise defs_and_utils.ParsingError(
                    f"CF ({cf_name}) is auto-generated.",
                    self.get_error_context(entry))

    def validate_cf_wasnt_dropped(self, cf_info, entry):
        if cf_info.drop_time:
            raise defs_and_utils.ParsingError(
                f"CF ({cf_info.name}) already dropped at "
                f"({cf_info.drop_time}).",
                self.get_error_context(entry))

    def get_error_context(self, entry):
        return defs_and_utils.get_error_context_from_entry(entry,
                                                           self.log_file_path)
