#!/usr/bin/env python

import contextlib
import json
import logging
import os
import time
from Queue import Queue
from threading import Thread

import dropbox
from dotenv import load_dotenv
from slackweb import Slack


logging.basicConfig(
    filename='logs/' + time.strftime('%Y_%m_%d') + '.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logging.getLogger('dropbox').setLevel(logging.CRITICAL)
logging.getLogger('requests').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)

SERVER_PREFIX = ''
PENDING_FOLDER = 'UploadPending'
COMPLETE_FOLDER = 'UploadComplete'

slack = None


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
                logging.info(
                    'DownloadWorker: saving [%-50s] to [%-50s]',
                    move_from_path,
                    server_path
                )
                dbx_obj.files_download_to_file(server_path, move_from_path)

                # Put the paths for moving after a successful download
                self.move_queue.put((dbx_obj, move_from_path, move_to_path))
            except (dropbox.exceptions.ApiError, IOError) as e:
                logging.exception('Error in DownloadWorker: %s', e)
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
                logging.info(
                    'MoveWorker: from [%-50s] to [%-50s]',
                    from_path,
                    to_path
                )
                dbx_obj.files_move(from_path, to_path, autorename=True)
            except dropbox.exceptions.ApiError, e:
                logging.exception('Error in MoveWorker: %s', e)
                slack_log_error('Error in MoveWorker', e)
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
    except dropbox.exceptions.ApiError, e:
        logging.exception(
            '[%s] Error while calling Dropbox.files_list_folder: %s',
            org,
            e
        )
        slack_log_error('Error while calling Dropbox.files_list_folder', e)

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

        org_is_UND = org in ['UND-GFK', 'UND-IWA', 'UND-CKN']

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
                    logging.info(
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
                    logging.info(
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
                    else:
                        status, aircraft, n_number = dbx_path_list

                    org_path = os.path.join(org, aircraft, n_number).upper()
                    full_path = os.path.join(SERVER_PREFIX, org_path)

                    if not os.path.exists(full_path):
                        # Make directory on server since it doesn't exist
                        logging.info(
                            '%-25s [%-11s]: %s',
                            'Folder does not exist',
                            'creating',
                            full_path
                        )
                        os.makedirs(full_path)
                    else:
                        # Directory already exists, so do nothing
                        logging.info(
                            '%-25s [%-11s]: %s',
                            'Folder does exist',
                            'skipping',
                            full_path
                        )
        # end for

        logging.info('[%s] Num entries found: %d', org, file_count)
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

    logging.info('Total elapsed time for %s: %.3f seconds', msg, t1 - t0)
    slack_log_msg('Elapsed time for ' + msg, '%.3f seconds' % (t1 - t0))


def init_globals():
    """
    This function solely loads environment variables into the global variables.
    Should be called after load_dotenv()
    :return: void
    :rtype: void
    """
    global SERVER_PREFIX
    global slack

    SERVER_PREFIX = os.environ.get('SERVER_PREFIX')

    slack = Slack(os.environ.get('SLACK_WEBHOOK_URL'))


def slack_log_msg(title, description=None, author=__file__, color='#36a64f'):
    attachment = {
        "title":       str(title),
        'author_name': str(author),
        'color':       str(color)
    }

    if description is not None:
        attachment['text'] = str(description)

    slack.notify(attachments=[attachment])


def slack_log_error(title, exception=None, author=__file__):
    slack_log_msg(title, exception, author, '#ff0000')


if __name__ == "__main__":
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')

    try:
        if load_dotenv(dotenv_path):
            init_globals()

            with stopwatch('Syncing files/folders'):
                slack_log_msg('Script started')
                main()
        else:
            # Need to log an error stating the .env file could not be loaded
            slack_log_error('Could not load .env file')
    except Exception as e:
        logging.exception('Script crashed while executing: %s', e)
        slack_log_error('Script crashed while executing', e)
