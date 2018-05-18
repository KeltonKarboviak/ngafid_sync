#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import contextlib
import json
import logging.config
import os
import time
from queue import Queue
from threading import Thread

import dropbox

from config import SERVER_PREFIX, logging_config


logging.config.dictConfig(logging_config)
logger = logging.getLogger('sync')

PENDING_FOLDER = 'UploadPending'
COMPLETE_FOLDER = 'UploadComplete'



class DownloadWorker(Thread):
    def __init__(self, d_queue, m_queue):
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
                slack_log_error('Error in DownloadWorker', e)
            finally:
                self.download_queue.task_done()


class MoveWorker(Thread):
    def __init__(self, queue):
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


def get_pending_files(dbx_obj, org, org_is_UND):
    if org_is_UND:
        # UND's dropbox has 3 separate sub-locations: UND-GFK, UND-IWA, UND-CKN
        # these are a special case
        list_folder_path = '/'.join(["", org, PENDING_FOLDER])
    else:
        list_folder_path = '/' + PENDING_FOLDER

    entries = []
    try:
        result = dbx_obj.files_list_folder(list_folder_path, recursive=True)
        entries.extend(result.entries)

        # Dropbox API can only give so many results at a time, so we need
        # to continually call the API until there are no more results
        while result.has_more:
            result = dbx_obj.files_list_folder_continue(result.cursor)
            entries.extend(result.entries)
    except dropbox.exceptions.ApiError as e:
        logger.exception(
            '[%s] Error while calling Dropbox.files_list_folder: %s',
            org,
            e
        )

    return entries


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
    with open('dbx_org_tokens.json', 'r') as json_infile:
        org_data = json.load(json_infile)

    for org, token in org_data.items():
        dbx = dropbox.Dropbox(token)

        org_is_UND = org.startswith('UND')

        entries = get_pending_files(dbx, org, org_is_UND)
        file_count = 0

        for entry in entries:
            if isinstance(entry, dropbox.files.FileMetadata):
                file_count += 1

                if org_is_UND:
                    # UND's dropbox has 3 separate sub-locations: UND-GFK,
                    # UND-IWA, UND-CKN these are a special case
                    org, status, aircraft, n_number, flight_file = \
                        entry.path_lower.strip('/').split('/')
                    org = org.upper()

                    # Get the path for moving the file on Dropbox
                    move_to_path = '/'.join([
                        "",
                        org,
                        COMPLETE_FOLDER,
                        aircraft,
                        n_number,
                        flight_file
                    ])
                else:
                    status, aircraft, n_number, flight_file = \
                        entry.path_lower.strip('/').split('/')

                    # Get the path for moving the file on Dropbox
                    move_to_path = '/'.join([
                        "",
                        COMPLETE_FOLDER,
                        aircraft,
                        n_number,
                        flight_file
                    ])

                aircraft, n_number = aircraft.upper(), n_number.upper()

                # Get the path on the server for downloading
                server_path = os.path.join(
                    SERVER_PREFIX,
                    org,
                    aircraft,
                    n_number,
                    flight_file
                )

                if not os.path.exists(server_path):
                    # Put a download path into download_queue as a tuple since
                    # it doesn't exist, DownloadWorker thread will download file
                    # from Dropbox to server
                    logger.info(
                        '%-25s [%-11s]: %s',
                        'File does not exist',
                        'queueing',
                        server_path
                    )
                    download_queue.put(
                        (dbx, server_path, entry.path_lower, move_to_path)
                    )
                else:
                    # File already exists, so put in move_queue and have
                    # MoveWorker thread move to COMPLETE_FOLDER
                    logger.info(
                        '%-25s [%-11s]: %s',
                        'File does exist',
                        'skipping',
                        server_path
                    )
                    move_queue.put((dbx, entry.path_lower, move_to_path))
            elif isinstance(entry, dropbox.files.FolderMetadata):
                dbx_path_list = entry.path_lower.strip('/').split('/')

                # Check to see if entry is a directory just above flight CSV
                # files. If so, then we'll see if the path exists on the server
                if (org_is_UND and len(dbx_path_list) == 4) or (
                        not org_is_UND and len(dbx_path_list) == 3):
                    if org_is_UND:
                        org, status, aircraft, n_number = dbx_path_list
                        org = org.upper()
                    else:
                        status, aircraft, n_number = dbx_path_list

                    org_path = os.path.join(org, aircraft, n_number).upper()
                    full_path = os.path.join(SERVER_PREFIX, org_path)

                    if not os.path.exists(full_path):
                        # Make directory on server since it doesn't exist
                        logger.info(
                            '%-25s [%-11s]: %s',
                            'Folder does not exist',
                            'creating',
                            full_path
                        )
                        os.makedirs(full_path)
                    else:
                        # Directory already exists, so do nothing
                        logger.info(
                            '%-25s [%-11s]: %s',
                            'Folder does exist',
                            'skipping',
                            full_path
                        )
        # end for

        logger.info('[%s] Num entries found: %d', org, file_count)
    # end for

    # Causes main thread to wait for the queues to be empty
    download_queue.join()
    move_queue.join()


@contextlib.contextmanager
def stopwatch(msg):
    """Context manager to print how long a block of code ran."""
    t0 = time.time()
    try:
        yield
    finally:
        t1 = time.time()

    logger.info('Total elapsed time for %s: %.3f seconds', msg, t1 - t0)


if __name__ == "__main__":
    try:
        with stopwatch('Syncing files/folders'):
            logger.info('Script started')
            main()
    except Exception as exc:
        logger.exception('Script crashed while executing: %s', exc)
