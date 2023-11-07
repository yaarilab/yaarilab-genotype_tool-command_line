import unittest
from collect import collect_repertoires, count_rearrangements, default_repository_df, RepDownloader, BatchDownloader
import os
import pandas as pd
import time
import json
import tempfile



class TestMethods(unittest.TestCase):
    def test_collect_repertoires(self):
        study_id = 'PRJNA628125'
        results = collect_repertoires(default_repository_df, study_id)
        assert len(results['Repertoire']
                   ) > 0, f"no results for study_id: {study_id}"
        for repertoire in results['Repertoire']:
            assert repertoire.get(
                'repository') is not None, f"missing 'repository' attribute from results"

    def test_count_rearrangements(self):

        repertoires = {
            "Repertoire": [
                {
                    'repertoire_id': '5ed685a099011334ac05e848',
                    'repository': 'covid19-1.ireceptor.org'
                }
            ]
        }
        results = count_rearrangements(repertoires)
        assert results["Repertoire"][0].get(
            'rearrangements') is not None, f"missing 'rearrangements' attribute from results"
        assert results["Repertoire"][0].get(
            'rearrangements', -1) == 279041, f"missing 'rearrangements' attribute from results"

    def test_RepDownloader(self):

        with tempfile.TemporaryDirectory() as output_dir:
            def download_callback(x):
                if x['status'] == "downloading":
                    x['status'] = 'completed'

            downloader = RepDownloader(
                {
                    "repertoire_id": "517",
                    "url": "https://ipa1.ireceptor.org/airr/v1/rearrangement",
                    "subject_id": "Patient 4 CLL/SLL",
                    "filename": os.path.join(output_dir, "517.tsv.gz"),
                    "rearrangements": 19510,
                    "status": "pending"
                },
                download_callback
            )
            downloader.start()
            time.sleep(1)
            downloader.cancel_download()
            downloader.join()
            download = downloader.status()
            assert download['status'] == "canceled"

            downloader = RepDownloader(
                {
                    "repertoire_id": "2",
                    "url": "https://ipa1.ireceptor.org/airr/v1/rearrangement",
                    "subject_id": "BS-HS-21_TCRB",
                    "filename": os.path.join(output_dir, "2.tsv.gz"),
                    "rearrangements": 4182,
                    "status": "pending"
                },
                download_callback
            )
            downloader.start()
            time.sleep(1)
            download = downloader.status()
            assert download['status'] in ['downloading', "completed"]
            downloader.join()
            download = downloader.status()
            assert download['status'] == 'completed'
            assert download['downloaded'] > 0
            df = pd.read_csv(download['filename'], sep='\t')
            assert len(df) == download['rearrangements']

    def test_BatchDownloader(self):

        with tempfile.TemporaryDirectory() as output_dir:
            output_dir = output_dir
            downloader = BatchDownloader(
                repertoires=[
                    {
                        "repertoire_id": "517",
                        "repository": "ipa1.ireceptor.org",
                        "subject": {
                            "subject_id": "Patient 4 CLL/SLL"
                        },
                        "rearrangements": 19510
                    },
                ],
                output_dir=r"C:\Users\yaniv\Desktop\yaarilab-genotype_tool-b11f598b9e93\yaarilab-genotype_tool-b11f598b9e93\downloads",
                max_concurrent_downloads=1
            )
            downloader.start()
            time.sleep(1)
            downloader.cancel_download()
            downloader.join()
            assert len(downloader.downloaders) == 0
            status = downloader.status()
            assert status['completed'] == 0
            for download in status['downloads']:
                assert download['status'] == 'canceled'

            downloader = BatchDownloader(
                repertoires=[
                    {
                        "repertoire_id": "2",
                        "repository": "ipa1.ireceptor.org",
                        "subject": {
                            "subject_id": "32"
                        },
                        "rearrangements": 4182
                    },
                    {
                        "repertoire_id": "12",
                        "repository": "ipa1.ireceptor.org",
                        "subject": {
                            "subject_id": "44"
                        },
                        "rearrangements": 4381
                    }
                ],
                output_dir=output_dir,
                max_concurrent_downloads=1
            )
            downloader.start()
            downloader.join()
            status = downloader.status()
            assert len(status['downloads']) == 2
            downloaded = 0
            rearrangements = 0
            assert status['completed'] == 2
            for download in status['downloads']:
                assert download['status'] == 'completed'
                df = pd.read_csv(download['filename'], sep='\t')
                assert len(df) == download['rearrangements']
                downloaded += download['downloaded']
                rearrangements += download['rearrangements']
                os.remove(download['filename'])
            assert downloaded == status['downloaded']
            assert rearrangements == status['rearrangements']
            with open(os.path.join(output_dir, 'metadata.json'), 'r') as f_metadata:
                metadata = json.load(f_metadata)
                assert len(metadata['Repertoire']) == 2


if __name__ == '__main__':
    unittest.main()
