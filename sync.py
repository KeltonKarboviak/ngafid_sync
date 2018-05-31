#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging.config
import os
from pathlib import Path
from queue import Queue
from threading import Thread
from typing import List, Union

import dropbox.exceptions
import dropbox.files
from dropbox import Dropbox

from config import SERVER_PREFIX, logging_config
from models.organization import Organization


logging.config.dictConfig(logging_config)
logger = logging.getLogger('sync')

PENDING_FOLDER = 'UploadPending'
COMPLETE_FOLDER = 'UploadComplete'
DBX_PATH_SEP = '/'

dbx_token_file = Path('dbx_org_tokens.json')


class DownloadWorker(Thread):

    def __init__(self, d_queue: Queue, m_queue: Queue):
        Thread.__init__(self)
        self.download_queue = d_queue
        self.move_queue = m_queue

    def run(self):
        while True:
            # Get a download link from the queue
            dbx_obj, server_path, move_from_path, move_to_path = \
                self.download_queue.get()

            try:
                logger.info(
                    'DownloadWorker: saving [%-50s] to [%-50s]',
                    move_from_path,
                    server_path
                )
                dbx_obj.files_download_to_file(server_path, move_from_path)

                # Put the paths for moving after a successful download
                self.move_queue.put((dbx_obj, move_from_path, move_to_path))
            except (dropbox.exceptions.ApiError, IOError) as e:
                logger.exception('Error in DownloadWorker: %s', e)
            finally:
                self.download_queue.task_done()


class MoveWorker(Thread):

    def __init__(self, queue: Queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            # Get a from_path and to_path for moving the file on Dropbox
            dbx_obj, from_path, to_path = self.queue.get()

            try:
                logger.info(
                    'MoveWorker: from [%-50s] to [%-50s]',
                    from_path,
                    to_path
                )
                dbx_obj.files_move(from_path, to_path, autorename=True)
            except dropbox.exceptions.ApiError as e:
                logger.exception('Error in MoveWorker: %s', e)
            finally:
                self.queue.task_done()


def get_pending_files(
    dbx_obj: Dropbox,
    org: Organization
) -> List[Union[dropbox.files.FileMetadata, dropbox.files.FolderMetadata]]:
    if org.is_und_org:
        # UND's Dropbox has 3 separate sub-locations: UND-GFK, UND-IWA, UND-CKN
        # these are a special case
        dbx_folder_path = DBX_PATH_SEP.join(['', org.name, PENDING_FOLDER])
    else:
        dbx_folder_path = DBX_PATH_SEP + PENDING_FOLDER

    entries = []
    try:
        result = dbx_obj.files_list_folder(dbx_folder_path, recursive=True)
        entries.extend(result.entries)

        # Dropbox API can only give so many results at a time, so we need
        # to continually call the API until there are no more results
        while result.has_more:
            result = dbx_obj.files_list_folder_continue(result.cursor)
            entries.extend(result.entries)
    except dropbox.exceptions.ApiError as e:
        logger.exception(
            '[%s] Error while calling Dropbox.files_list_folder: %s', org, e
        )

    return entries


def process_file_entry(
    entry: dropbox.files.FileMetadata,
    org: Organization,
    download_q: Queue,
    move_q: Queue,
    dbx_obj: dropbox.Dropbox
):
    dbx_path_list = entry.path_lower.strip(DBX_PATH_SEP).split(DBX_PATH_SEP)

    if org.is_und_org:
        # UND's dropbox has 3 separate sub-locations: UND-GFK,
        # UND-IWA, UND-CKN these are a special case
        _, status, aircraft, recorder, n_number, flight_file = dbx_path_list

        # Get the path for moving the file on Dropbox
        move_to_path = DBX_PATH_SEP.join([
            '',
            org.name,
            COMPLETE_FOLDER,
            aircraft,
            recorder,
            n_number,
            flight_file,
        ])
    else:
        status, aircraft, recorder, n_number, flight_file = dbx_path_list

        # Get the path for moving the file on Dropbox
        move_to_path = DBX_PATH_SEP.join([
            '',
            COMPLETE_FOLDER,
            aircraft,
            recorder,
            n_number,
            flight_file,
        ])

    aircraft, recorder, n_number = (
        aircraft.upper(), recorder.upper(), n_number.upper(),
    )

    # Get the path on the server for downloading
    server_path = Path(
        SERVER_PREFIX,
        org.name,
        aircraft,
        recorder,
        n_number,
        flight_file,
    )

    if not server_path.exists():
        # Put a download path into download_queue as a tuple since
        # it doesn't exist, DownloadWorker thread will download file
        # from Dropbox to server
        file_status = 'File does not exist'
        action = 'queueing'
        download_q.put(
            (dbx_obj, str(server_path), entry.path_lower, move_to_path)
        )
    else:
        # File already exists, so put in move_queue and have
        # MoveWorker thread move to COMPLETE_FOLDER
        file_status = 'File does exist'
        action = 'skipping'
        move_q.put((dbx_obj, entry.path_lower, move_to_path))

    logger.info(
        '%-25s [%-11s]: %s',
        file_status,
        action,
        server_path
    )


def process_folder_entry(
    entry: dropbox.files.FolderMetadata,
    org: Organization
):
    dbx_path_list = entry.path_lower.strip(DBX_PATH_SEP).split(DBX_PATH_SEP)

    if org.is_und_org:
        # UND org, so we need to skip /UND-*/UploadPending in Dropbox path
        offset = 2
    else:
        # Need to skip /UploadPending in Dropbox path
        offset = 1

    org_path = os.path.join(org.name, *dbx_path_list[offset:]).upper()
    full_path = os.path.join(SERVER_PREFIX, org_path)

    # This will try to recursively make the dirs in full_path. If the dirs
    # already exist, the exist_ok flag will simply ignore it.
    os.makedirs(full_path, exist_ok=True)


def main():
    # Create a queue to communicate with DownloadWorker threads
    download_queue = Queue()

    # Create a queue to communicate with MoveWorker threads
    move_queue = Queue()

    for x in range(1):  # Create thread(s) for downloading
        download_worker = DownloadWorker(download_queue, move_queue)
        download_worker.daemon = True
        download_worker.start()

    for x in range(4):  # Create thread(s) for moving
        move_worker = MoveWorker(move_queue)
        move_worker.daemon = True
        move_worker.start()

    # Read in all organizations' Dropbox tokens
    with dbx_token_file.open('r') as json_file:
        org_data = json.load(json_file)

    for org_name, token in org_data.items():
        org = Organization(org_name, token)
        dbx = Dropbox(org.token)

        entries = get_pending_files(dbx, org)
        file_count = 0

        for entry in entries:
            try:
                if isinstance(entry, dropbox.files.FileMetadata):
                    process_file_entry(
                        entry, org, download_queue, move_queue, dbx
                    )
                    file_count += 1
                elif isinstance(entry, dropbox.files.FolderMetadata):
                    process_folder_entry(entry, org)
            except Exception as e:
                logger.exception(
                    'Exception occurred while processing entry [%s]: %s',
                    entry,
                    e
                )

        logger.info('[%s] Num entries found: %d', org.name, file_count)

    # Causes main thread to wait for the queues to be empty
    download_queue.join()
    move_queue.join()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logger.exception('Script crashed while executing: %s', exc)
