import logging
from dataclasses import dataclass

import events
import utils


@dataclass
class DbFileInfo:
    file_number: int
    cf_name: str
    creation_time: str
    deletion_time: str = None
    size_bytes: int = 0
    compressed_size_bytes: int = 0
    compressed_data_size_bytes: int = 0
    data_size_bytes: int = 0
    index_size_bytes: int = 0
    filter_size_bytes: int = 0
    filter_policy: str = None
    num_filter_entries: int = 0
    compression_type: str = None
    level: int = None

    def is_alive(self):
        return self.deletion_time is None

    def file_deleted(self, deletion_time):
        assert self.is_alive()
        self.deletion_time = deletion_time


@dataclass
class DbLiveFilesStats:
    total_live_indexes_sizes_bytes: int = 0
    total_live_filters_sizes_bytes: int = 0
    max_total_live_indexes_sizes_bytes: int = 0
    max_live_indexes_size_time: str = None
    max_total_live_filters_sizes_bytes: int = 0
    max_live_filters_size_time: str = None

    def file_created(self, file_info):
        assert isinstance(file_info, DbFileInfo)

        self.total_live_indexes_sizes_bytes += file_info.index_size_bytes
        if self.max_total_live_indexes_sizes_bytes <\
                self.total_live_indexes_sizes_bytes:
            self.max_total_live_indexes_sizes_bytes = \
                self.total_live_indexes_sizes_bytes
            self.max_live_indexes_size_time = file_info.creation_time

        self.total_live_filters_sizes_bytes += file_info.filter_size_bytes
        if self.max_total_live_filters_sizes_bytes <\
                self.total_live_filters_sizes_bytes:
            self.max_total_live_filters_sizes_bytes = \
                self.total_live_filters_sizes_bytes
            self.max_live_filters_size_time = file_info.creation_time

    def file_deleted(self, file_info):
        assert isinstance(file_info, DbFileInfo)
        assert self.total_live_indexes_sizes_bytes >= \
               file_info.index_size_bytes
        assert self.total_live_filters_sizes_bytes >= \
               file_info.filter_size_bytes
        self.total_live_indexes_sizes_bytes -= file_info.index_size_bytes
        self.total_live_filters_sizes_bytes -= file_info.filter_size_bytes


class DbFilesMonitor:
    def __init__(self):
        # Format: {<file_number>
        self.files = dict()
        self.live_files_stats = DbLiveFilesStats()
        self.cfs_names = list()

    def new_event(self, event):
        event_type = event.get_type()
        if event_type == events.EventType.TABLE_FILE_CREATION:
            return self.file_created(event)
        elif event_type == events.EventType.TABLE_FILE_DELETION:
            return self.file_deleted(event)

        return True

    def file_created(self, event):
        assert isinstance(event, events.TableFileCreationEvent)

        file_number = event.get_created_file_number()
        if file_number in self.files:
            logging.error(
                f"Created File (number {file_number}) already exists, "
                f"Ignoring Event.\n{event}")
            return False

        cf_name = event.get_cf_name()
        file_info = DbFileInfo(file_number,
                               cf_name=cf_name,
                               creation_time=event.get_log_time())
        file_info.compressed_size_bytes = \
            event.get_compressed_file_size_bytes()
        file_info.compressed_data_size_bytes = \
            event.get_compressed_data_size_bytes()
        file_info.data_size_bytes = event.get_data_size_bytes()
        file_info.index_size_bytes = event.get_index_size_bytes()
        file_info.filter_size_bytes = event.get_filter_size_bytes()
        file_info.compression_type = event.get_compression_type()

        if event.does_use_filter():
            file_info.filter_policy = event.get_filter_policy()
            file_info.num_filter_entries = event.get_num_filter_entries()

        self.files[file_number] = file_info
        self.live_files_stats.file_created(file_info)
        if cf_name not in self.cfs_names:
            self.cfs_names.append(cf_name)

        return True

    def file_deleted(self, event):
        assert isinstance(event, events.TableFileDeletionEvent)

        file_number = event.get_deleted_file_number()
        if file_number not in self.files:
            logging.info(f"File deleted event ((number {file_number}) "
                         f"without a previous file created event, Ignoring "
                         f"Event.\n{event}")
            return False

        file_info = self.files[file_number]
        if not file_info.is_alive():
            logging.error(
                f"Deleted File (number {file_number}) already "
                f"deleted, Ignoring Event.\n{event}")
            return False

        file_info.file_deleted(event.get_log_time())
        self.live_files_stats.file_deleted(file_info)

        return True

    def get_all_files(self, file_filter=lambda info: True):
        # Returns {<cf>: [<live files for this cf>
        files = {}
        for info in self.files.values():
            if file_filter(info):
                if info.cf_name not in files:
                    files[info.cf_name] = list()
                files[info.cf_name].append(info)
        return files

    def get_all_cf_files(self, cf_name):
        all_cf_files = \
            self.get_all_files(lambda info: info.cf_name == cf_name)
        if not all_cf_files:
            return []
        return all_cf_files[cf_name]

    def get_all_live_files(self):
        return self.get_all_files(lambda info: info.is_alive())

    def get_cf_live_files(self, cf_name):
        cf_live_files = \
            self.get_all_files(
                lambda info: info.is_alive() and info.cf_name == cf_name)
        if not cf_live_files:
            return []
        return cf_live_files[cf_name]

    def get_live_files_stats(self):
        return self.live_files_stats

    def get_cfs_names(self):
        return self.cfs_names


#
# Files Stats
#


@dataclass
class BlockStats:
    total_size_bytes: int = 0
    avg_size_bytes: int = 0
    max_size_bytes: int = 0


@dataclass
class CfFilterSpecificStats:
    filter_policy: str = None
    avg_bpk: float = 0.0


@dataclass
class CfsFilesStats:
    index: BlockStats = None
    filter: BlockStats = None
    cfs_filter_specific: dict() = None

    def __init__(self):
        self.index = BlockStats()
        self.filter = BlockStats()

        # {<cf_name>: FilterSpecificStats}
        self.cfs_filter_specific = dict()


def calc_cf_files_stats(cfs_names, files_monitor):
    assert isinstance(files_monitor, DbFilesMonitor)

    stats = CfsFilesStats()
    num_cf_files = 0
    for cf_name in cfs_names:
        cf_files = files_monitor.get_all_cf_files(cf_name)
        num_cf_files += len(cf_files) if cf_files else 0

        if num_cf_files == 0:
            logging.info(f"No files for cf {cf_name}")
            continue

        total_filters_sizes_bytes = 0
        total_num_filters_entries = 0
        filter_policy = None
        for i, file_info in enumerate(cf_files):
            stats.index.total_size_bytes += file_info.index_size_bytes
            stats.filter.total_size_bytes += file_info.filter_size_bytes
            stats.index.max_size_bytes = \
                max(stats.index.max_size_bytes, file_info.index_size_bytes)
            stats.filter.max_size_bytes = \
                max(stats.filter.max_size_bytes, file_info.filter_size_bytes)

            if i == 0:
                # Set the filter policy for the cf on the first. Changing
                # it later is either a bug, or an unsupported (by the
                # parser) dynamic change of the filter policy
                filter_policy = file_info.filter_policy
            else:
                if filter_policy != utils.INVALID_FILTER_POLICY \
                        and filter_policy != file_info.filter_policy:
                    logging.warning(
                        f"Filter policy changed for CF. Not supported"
                        f"currently. cf:{cf_name}\nfile_info:{file_info}")
                    filter_policy = utils.INVALID_FILTER_POLICY
                    continue
            total_filters_sizes_bytes += file_info.filter_size_bytes
            total_num_filters_entries += file_info.num_filter_entries

        avg_bpk = 0
        if filter_policy is not None and filter_policy != \
                utils.INVALID_FILTER_POLICY:
            if total_num_filters_entries > 0:
                avg_bpk =\
                    (8 * total_filters_sizes_bytes) / total_num_filters_entries
            else:
                logging.warning(f"No filter entries. cf:{cf_name}\n"
                                f"file info:{file_info}")
        stats.cfs_filter_specific[cf_name] =\
            CfFilterSpecificStats(filter_policy=filter_policy, avg_bpk=avg_bpk)

    if num_cf_files == 0:
        logging.info(f"No files for all cf-s ({[cfs_names]}")
        return None

    stats.index.avg_size_bytes = stats.index.total_size_bytes / num_cf_files
    stats.filter.avg_size_bytes = stats.filter.total_size_bytes / num_cf_files

    return stats
