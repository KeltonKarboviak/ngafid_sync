#!/usr/bin/env python

import contextlib
import dbx_config  # Contains Dropbox API Key & Secret
import dropbox
import json
import logging
import os
import time
from Queue import Queue
from threading import Thread


logging.basicConfig(filename=time.strftime('%Y_%m_%d') + '.log', level=logging.DEBUG, format='%(asctime)s - %(name)s - %(thread)d - %(levelname)s - %(message)s')
logging.getLogger('dropbox').setLevel(logging.CRITICAL)
logging.getLogger('requests').setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)

DEBUG = True

org_data = {}

SERVER_PREFIX = "./server"
PENDING_FOLDER = "UploadPending"
COMPLETE_FOLDER = "UploadComplete"


class DownloadWorker(Thread):
    def __init__(self, dbxObj, d_queue, m_queue):
        Thread.__init__(self)
        self.dbx = dbxObj
        self.download_queue = d_queue
        self.move_queue = m_queue
    # end def __init__()

    def run(self):
        while True:
            # Get a download link from the queue
            server_path, move_from_path, move_to_path = self.download_queue.get()

            try:
                logging.info("DownloadWorker: saving [%-50s] to [%-50s]", move_from_path, server_path)
                self.dbx.files_download_to_file(server_path, move_from_path)

                # Put the paths for moving after a successful download
                self.move_queue.put( (move_from_path, move_to_path) )
                # move_list.append( dropbox.files.RelocationPath(move_from_path, move_to_path) )
            except dropbox.exceptions.ApiError, e:
                logging.exception("Error while accessing Dropbox: %s", e)

            self.download_queue.task_done()
        # end while
    # end def run()
# end class DownloadWorker


class MoveWorker(Thread):
    def __init__(self, dbx_obj, queue):
        Thread.__init__(self)
        self.dbx = dbx_obj
        self.queue = queue
    # end def __init__()

    def run(self):
        while True:
            # Get a from_path and to_path for moving the file on Dropbox
            from_path, to_path = self.queue.get()

            try:
                logging.info("MoveWorker: from [%-50s] to [%-50s]", from_path, to_path)
                self.dbx.files_move(from_path, to_path)
            except dropbox.exceptions.ApiError, e:
                logging.exception("MoveWorker: %s", e)

            self.queue.task_done()
        # end while
    # end def run()
# end class MoveWorker


def get_new_files(dbx_obj, org):
    try:
        res = dbx_obj.files_list_folder(
            '/'.join(["", org, PENDING_FOLDER]) if DEBUG else '/' + PENDING_FOLDER,
            recursive=True
        )

        return res.entries
    except dropbox.exceptions.ApiError, e:
        logging.exception("[%s] Error while calling Dropbox.files/list_folder/continue: %s", org, e)
        return []  # If exception is thrown, then return an empty list
# end def get_new_files()


def main():
    # dbx = dropbox.Dropbox(ACCESS_TOKEN)

    download_queue = Queue()  # Create a queue to communicate with DownloadWorker threads
    move_queue = Queue()  # Create a queue to communicate with MoveWorker threads

    for x in range(1):  # Create 1 threads for downloading
        download_worker = DownloadWorker(dbx, download_queue, move_queue)
        download_worker.daemon = True
        download_worker.start()

    for x in range(4):  # Create 4 threads for moving
        move_worker = MoveWorker(dbx, move_queue)
        move_worker.daemon = True
        move_worker.start()

    # Read in all organizations' Dropbox Tokens
    with open('dbx_org_sync_data.json', 'r') as in_json_datafile:
        org_data = json.load(in_json_datafile)

    for org, token in org_data.items():
        dbx = dropbox.Dropbox(dbx_config.ACCESS_TOKEN if DEBUG else token)  # USE FOR PROD
        entries = get_new_files(dbx, org)
        file_count = 0

        for entry in entries:
            if isinstance(entry, dropbox.files.FileMetadata):
                file_count += 1

                org, status, aircraft, n_number, flight = entry.path_lower.strip('/').split('/')
                org, aircraft, n_number = org.upper(), aircraft.upper(), n_number.upper()
                path = os.path.join(SERVER_PREFIX, org, aircraft, n_number, flight)

                # Get the paths for moving the file on Dropbox
                move_to_path = '/'.join(["", org, COMPLETE_FOLDER, aircraft, n_number, flight])

                if not os.path.exists(path):
                    # Put a download path into download_queue as a tuple since
                    # it doesn't exist, DownloadWorker thread will download file
                    # from Dropbox to server
                    logging.info("%-25s [%-11s]: %s", "File does not exist", "downloading", path)
                    download_queue.put( (path, entry.path_lower, move_to_path) )
                else:
                    # File already exists, so put in move_queue and have
                    # MoveWorker thread to move to COMPLETE_FOLDER
                    logging.info("%-25s [%-11s]: %s", "File does exist", "skipping", path)
                    move_queue.put( (entry.path_lower, move_to_path) )
            elif isinstance(entry, dropbox.files.FolderMetadata):
                dbx_path_list = entry.path_lower.strip('/').split('/')

                if len(dbx_path_list) == 4:
                    org, status, aircraft, n_number = dbx_path_list
                    org_path = os.path.join(org, aircraft, n_number).upper()
                    full_path = os.path.join(SERVER_PREFIX, org_path)
                    if not os.path.exists(full_path):
                        # Make directory on server since it doesn't exist
                        logging.info("%-25s [%-11s]: %s", "Folder does not exist", "creating", full_path)
                        os.makedirs(full_path)
                    else:
                        # Directory already exists, so do nothing
                        logging.info("%-25s [%-11s]: %s", "Folder does exist", "skipping", full_path)
            # end if/elif
        # end for

        print "[%s] Num entries found: %d" % (org, file_count)
    # end for

    # Causes main thread to wait for the queues to be empty
    download_queue.join()
    move_queue.join()

    # print move_list
    # logging.info("Num to move: %d", len(move_list))
    #
    # job_id_list = []
    #
    # for i in xrange(0, len(move_list), 30):
    #     job_id = dbx.files_move_batch(move_list[i:i + 30]).get_async_job_id()
    #     logging.info("[Move Batch ID]: %s", job_id)
    #     job_id_list.append(job_id)
    #
    # for j_id in job_id_list:
    #     print "Job ID:", j_id
    #     job_status = dbx.files_move_batch_check(j_id)
    #
    #     count = 0
    #     while not (job_status.is_complete() or job_status.is_failed()):
    #         time.sleep(2)
    #         job_status = dbx.files_move_batch_check(job_id)
    #         print "\tChecking again:", count
    #         count += 1
    #
    #     if job_status.is_complete():
    #         print "\tMove Batch Completed!"
    #     elif job_status.is_failed():
    #         print "\tMove Batch Failed:", job_status.get_failed()
# end def main()


@contextlib.contextmanager
def stopwatch(msg):
    """Context manager to print how long a block of code took."""
    t0 = time.time()
    try:
        yield
    finally:
        t1 = time.time()
    print "Total elapsed time for %s: %.3f seconds" % (msg, t1 - t0)
# end def stopwatch()


if __name__ == "__main__":
    with stopwatch('Syncing files/folders'):
        main()
