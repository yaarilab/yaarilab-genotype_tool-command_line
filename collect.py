import os
os.environ['GEVENT_SUPPORT'] = 'True'
import json
import pandas as pd
import argparse
import grequests
import requests
import sys
import logging
from urllib.parse import urlparse
import numpy as np
import requests
import threading
from queue import Queue
import time
import os
#import pipi 
import gzip
import signal
import pycurl
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

default_repository_df = pd.DataFrame(
    [
        'https://covid19-1.ireceptor.org',
        'https://covid19-2.ireceptor.org',
        'https://covid19-3.ireceptor.org',
        'https://covid19-4.ireceptor.org',
        'https://ipa1.ireceptor.org',
        'https://ipa2.ireceptor.org',
        'https://ipa3.ireceptor.org',
        'https://ipa4.ireceptor.org',
        'https://ipa5.ireceptor.org',
        'https://ipa6.ireceptor.org',
        'https://vdjserver.org',
        'https://scireptor.dkfz.de',
        'https://airr-seq.vdjbase.org',
        'https://roche-airr.ireceptor.org',
        'https://t1d-1.ireceptor.org',
        'https://agschwab.uni-muenster.de'
    ], columns=['URL']
)

max_concurrent_downloads = 3
download_lock = threading.Lock()
downloader_table_size = 10
downloader_table = []
max_concurrent_downloads = 3

#logging.getLogger("urlib3").addFilter(lambda record: "Unverified HTTPS request is being made" not in record.getMessage())
logger = logging.getLogger("genotype")
logger.setLevel(logging.INFO)


def collect_repertoires(repository_df, study_id):

    logger.info(f"querying repertoires for study_id: {study_id}, in {len(repository_df)} repositories.")
    responses = []
    for url in repository_df.URL:
        try:
            response = requests.post(
                url + "/airr/v1/repertoire",
                json={
                    "filters":
                        {
                            "op": "=",
                            "content":
                                {
                                    "field": "study.study_id",
                                    "value": str(study_id)
                                }
                        },
                    "format": "tsv"
                },
                verify=False
            )
            responses.append(response)
        except Exception as e:
            logger.warning(f'failed sending request to: {url}, error: {str(e)}')
            continue
    results = {"Repertoire": []}
    # iterate the results
    for i, response in enumerate(responses):

        url = repository_df.URL.iloc[i]
        if not response:
            logger.warning(f'failed getting response from: {url}')
            print(f'failed getting response from: {url}')
            continue
        # try to parse the response as json
        try:
            response = json.loads(response.content)
        except Exception as e:
            logger.warning(f'failed parsing response to json: {url}, error: {str(e)}')
            continue

        repertoires = response.get('Repertoire', [])
        if len(repertoires):
            logger.info(f'found {len(repertoires)} repertoires in repository: {url}')
            for repertoire in repertoires:
                repertoire['repository'] = urlparse(url).netloc
            results['Repertoire'].extend(repertoires)
        else:
            print("no repertoires was found in ",url)

    logger.info(json.dumps(results, indent=2))
    return results


def count_rearrangements(repertoires):

    repertoires_df = pd.DataFrame(
        [[repertoire['repertoire_id'], repertoire['repository']] for repertoire in repertoires['Repertoire']],
        columns=['repertoire_id', 'repository']
    )

    rs = (
        grequests.post(
            url + "/airr/v1/rearrangement",
            json={
                "filters": {
                    "op": "in",
                    "content":
                        {
                            "field": "repertoire_id",
                            "value": [f"{repertoire_id}" for repertoire_id in repertoire_ids]
                        }
                },
                "facets": "repertoire_id"
            }, verify=False
        ) for url, repertoire_ids in repertoires_df.groupby('repository').apply(
            lambda x: (f"https://{x.repository.iloc[0]}", x.repertoire_id.to_list())
        )
    )
    repertoires_df.set_index('repertoire_id', inplace=True)
    responses = grequests.map(rs)
    for i, response in enumerate(responses):
        url = repertoires_df.groupby('repository').apply(lambda x: x.repository.iloc[0]).iloc[i]
        if not response:
            logger.warning(f'failed getting response from: {url}')
            continue
        try:
            response = json.loads(response.content)
        except Exception as e:
            logger.warning(f'failed parsing response to json: {url}, error: {str(e)}')
            continue
        for item in response['Facet']:
            repertoires_df.loc[item['repertoire_id'], 'rearrangements'] = item['count']

    for repertoire in repertoires['Repertoire']:
        repertoire['rearrangements'] = repertoires_df.loc[repertoire['repertoire_id'], 'rearrangements']

    return repertoires


def collect_repertoires_and_count_rearrangements(default_repository_df, study_id):

    results = collect_repertoires(default_repository_df, study_id)
    if len(results["Repertoire"]) == 0:
        return results
    
    results = count_rearrangements(results)
    # summaries the results
    subject_ids = []
    repositories = []
    rearrangements = 0
    repertoires = 0
    for repertoire in results['Repertoire']:
        subject_ids.append(repertoire['subject']['subject_id'])
        repositories.append(repertoire['repository'])
        rearrangements += repertoire.get('rearrangements', 0)
        repertoires += 1
    results['repertoires'] = repertoires
    results['subjects'] = len(np.unique(subject_ids))
    results['repositories'] = len(np.unique(repositories))
    results['rearrangements'] = rearrangements

    return results

def download_study(study_id, repertoires, output_dir):
        
        output_dir = os.path.join(output_dir, study_id)
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)

        downloader = BatchDownloader(repertoires, output_dir, max_concurrent_downloads)
        with download_lock:
            if len(downloader_table) >= downloader_table_size:
                if downloader_table[0].is_alive():
                    return {
                        "error": "too many concurrent downloads jobs"
                    }
                downloader_table.pop(0)
            downloader_table.append(downloader)
        downloader.start()
        response = {
            "downloader_id": len(downloader_table) - 1,
            "downloads": downloader.status()
        }
        return response


class RepDownloader(threading.Thread):

    def __init__(self, download, callback):
        threading.Thread.__init__(self)
        self.download = download
        self.callback = callback
        self.start_time = time.time()
        self._stop_event = threading.Event()
        self.req = None
        self.cancel = False

    def progress(self, total, downloaded, __, ___):
        if self._stop_event.is_set():
            return -1
        download = self.download.copy()
        download['total'] = total
        download['downloaded'] = downloaded
        download['duration_sec'] = time.time() - self.start_time
        self.download = download

    def header_callback(self, header_line):
        if header_line.startswith(b'HTTP'):
            http_code = int(header_line.split()[1])
            if http_code != 200:
                self.download['status'] = 'failed'
                self.download['error'] = f'HTTP error: {http_code}'
        return len(header_line)

    def cancel_download(self):
        self._stop_event.set()

    def run(self):
        self.download['status'] = 'downloading'
        request_json = {
            "filters":  {
                "op": "=",
                "content": {
                    "field": "repertoire_id",
                    "value": str(self.download['repertoire_id'])
                }
            },
            "format": "tsv"
        }
        try:
            with gzip.open(self.download['filename'], 'wb') as f_out:
                req = pycurl.Curl()
                req.setopt(req.URL, self.download['url'])
                req.setopt(req.POST, 1)
                req.setopt(req.POSTFIELDS, json.dumps(request_json))
                req.setopt(pycurl.HTTPHEADER, ['Content-Type: application/json'])
                req.setopt(req.ACCEPT_ENCODING, 'gzip')
                req.setopt(req.HEADERFUNCTION, lambda header_line: self.header_callback(header_line))
                req.setopt(req.WRITEFUNCTION, f_out.write)
                req.setopt(req.NOPROGRESS, False)
                req.setopt(
                    req.PROGRESSFUNCTION, lambda total, downloaded, upload_t, upload_d: self.progress(total, downloaded, upload_t, upload_d)
                )
                self.req = req
                self.req.perform()
                self.req.close()
                self.callback(self.download)

        except Exception as e:
            if isinstance(e, pycurl.error) and e.args[0] == pycurl.E_ABORTED_BY_CALLBACK:
                self.download['status'] = "canceled"
            else:
                self.download['status'] = 'failed'
            self.download['error'] = str(e)
            self.callback(self.download)

    def status(self):
        return self.download


class BatchDownloader(threading.Thread):
    def __init__(self, repertoires, output_dir, max_concurrent_downloads=3): 
        threading.Thread.__init__(self)
        self.metadata = {
            'Repertoire': repertoires
        }
        self.max_concurrent_downloads = max_concurrent_downloads
        self.output_dir = output_dir
        self.download_queue = Queue()
        self.in_progress_downloads = []
        self.completed_downloads = []
        self.lock = threading.Lock()
        self.downloaders = pd.Series()
        self._stop_event = threading.Event()

    def download_callback(self, download):
        if download.get('error') is not None:
            logger.warning(
                f"Failed to download repertoire {download['repertoire_id']} from url {download['url']}:"
                f" {download['error']}"
            )
        else:
            logger.info(f"Successfully downloaded {download['repertoire_id']} from url {download['url']}")
        with self.lock:
            for download_itr in self.in_progress_downloads:
                if download_itr['repertoire_id'] == download['repertoire_id']:
                    self.in_progress_downloads.remove(download_itr)
                    break
            if download.get('error') is None:
                download['status'] = 'completed'
            self.completed_downloads.append(download)
            self.downloaders.drop(download['repertoire_id'], inplace=True)

    def run(self):
        download_tasks = pd.DataFrame(
            list(map(
                lambda x: [
                    x['repertoire_id'],
                    f"https://{x['repository']}/airr/v1/rearrangement",
                    x['subject']['subject_id'],
                    os.path.join(self.output_dir, f"{x['repertoire_id']}.tsv.gz"),
                    x['rearrangements'],
                    'pending',
                    0,
                    0,
                    0
                ], self.metadata['Repertoire']
            )),
            columns=['repertoire_id', 'url', 'subject_id', 'filename', 'rearrangements', 'status', 'downloaded', 'total', 'duration_sec']
        )
        download_tasks = download_tasks.apply(lambda x: x.to_dict(), axis=1).to_list()
        for download_task in download_tasks:
            self.download_queue.put(download_task)

        while not self.download_queue.empty() or (len(self.in_progress_downloads) > 0):
            while not self.download_queue.empty() and (len(self.in_progress_downloads) < self.max_concurrent_downloads):
                with self.lock:
                    if not self._stop_event.is_set():
                        download = self.download_queue.get()
                        self.in_progress_downloads.append(download)
                        downloader = RepDownloader(download, lambda x: BatchDownloader.download_callback(self, x))
                        downloader.start()
                        self.downloaders[download['repertoire_id']] = downloader
            time.sleep(1)

        with self.lock:
            downloaders = self.downloaders.copy()
        # wait for the downloaders to finish
        for downloader in downloaders:
            downloader.join()

        for download in self.completed_downloads:
            for repertoire in self.metadata['Repertoire']:
                if repertoire['repertoire_id'] == download['repertoire_id']:
                    repertoire['filename'] = download['filename']
        with open(os.path.join(self.output_dir, 'metadata.json'), 'w') as f_out:
            json.dump(self.metadata, f_out, indent=2)

    def cancel_download(self):
        with self.lock:
            self._stop_event.set()
            for downloader in self.downloaders:
                downloader.cancel_download()

    def status(self):
        with self.lock:
            return {
                "completed": sum([download['status'] == "completed" for download in self.completed_downloads]),
                "failed": sum([download['status'] != "completed" for download in self.completed_downloads]),
                "downloaded": sum([download['downloaded'] for download in self.completed_downloads + self.in_progress_downloads]),
                "in_progress": len(self.in_progress_downloads),
                "pending": self.download_queue.qsize(),
                "rearrangements": sum([download['rearrangements'] for download in self.completed_downloads]),
                "downloads": self.completed_downloads.copy() + self.in_progress_downloads.copy() + list(self.download_queue.queue.copy()),
                "download_dir": self.output_dir
            }










