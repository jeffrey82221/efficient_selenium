"""
TODO:
- [X] Remove table if exist
- [X] Refactor: combine initialize & nav_segment_generator -> Nav Batch Generator
- [X] Refactor: seperate isin, company extraction
    -> 1. [X] build pipe (load isin & company here)
    -> 2. [X] run pipe (delete table at exception)
- [ ] Make sure chromedriver is deleted when KeyboradException occur
- [X] Allow partial download if the file already exist
    - [X] Step 1: nav_gen needs to be path through a date existence checker (stop once the date already exists)
    - [X] Step 2: to turn-on the tmp mode to allow __get_table_path & __get_folder_path to located at nav_tmp/
    - [X] Step 3: build the same pipe
    - [X] Step 4: run the pipe
    - [X] Step 5: allow passing of `filter` to the connection of nav_batch_generator (via callback)
    - [X] Step 6: Outside this function, allow combination of tmp and original
- [ ] Allow swaping of .h5 from nav_tmp to nav for full download 
"""
from src.exceptions import IterationFailError, PipeBuildFailError, ExtractorBuildFailError
from src.page_extractor.fund_info_extractor import FundInfoExtractor, FundDocumentPage
from src.utils import batch_generator
import pandas as pd
from functools import partial
import tqdm
import ray
from ray.util import ActorPool
import pprint
import os
import traceback
import gc
import time
import random
import shutil
from datetime import datetime
from src.path import TablePathExtractor
VERBOSE = False
TQDM_VERBOSE = False


@ray.remote
class _FundNavExtractActor:
    def __init__(self, pending_time=0, nav_view_cls=None):
        self.nav_view = nav_view_cls()
        self._pending_time = pending_time
        self._first_try = True

    def request(self, input_tuple):
        try:
            if VERBOSE:
                print(f'[request {input_tuple[0]}] Start')
            if self._first_try:
                time.sleep(int(self._pending_time * random.random()))
                self._first_try = False
            gc.collect()
            fund_name, url = input_tuple
            self._path_extractor = TablePathExtractor(fund_name)
            if VERBOSE:
                print(f'[request {input_tuple[0]}] _path_extractor built')
            if os.path.exists(self._path_extractor.tmp.table_path):
                self._delete_h5()
            if os.path.exists(self._path_extractor.table_path):
                download_mode = 'partial'
            else:
                download_mode = 'full'
            if VERBOSE:
                print(f'[request {input_tuple[0]}] {download_mode} pipe built')
            end_pipe = self._build_pipe(fund_name, url, download_mode=download_mode)
            agg_result = list(end_pipe)
            if VERBOSE:
                print(
                    f'[request {input_tuple[0]}] {download_mode} download {len(agg_result)} navs downloaded')
            if download_mode == 'full':
                self._move_h5()
                if VERBOSE:
                    print(f'[request {input_tuple[0]}] move h5 from nav_tmp to nav')
            if download_mode == 'partial' and len(agg_result) > 0:
                self._update_h5()
                if VERBOSE:
                    print(f'[request {input_tuple[0]}] h5 updated with {len(agg_result)} navs')
            return f"NAV DOWNLOAD OF {fund_name}:{self._path_extractor.table_path} COMPLETE"
        except (PipeBuildFailError, ExtractorBuildFailError) as e:
            result = f"NAV DOWNLOAD FAILED with {str(e)}:\n{fund_name}\nURL:\n{url}"
            print(result)
            return result
        except (IterationFailError, KeyboardInterrupt):
            # KeyboardInterupt goes here
            result = f"NAV DOWNLOAD FAILED with IterationFailError:\n{fund_name}\nURL:\n{url}"
            print(result)
            print(traceback.format_exc())
            self._delete_h5()
            return result
        except BaseException as e:
            print(traceback.format_exc())
            self.quit()
            raise e

    def quit(self):
        self.nav_view.quit()

    def _build_pipe(self, fund, url, download_mode='full'):
        assert download_mode == 'full' or download_mode == 'partial'
        try:
            # If the table does not exist, do full download
            if download_mode == 'partial':
                nav_gen, _ = self._build_nav_batch_generator(
                    url, nav_filter=self._nav_filter)
                batch_count = self._estimate_batch_count()
            else:
                # full download
                nav_gen, batch_count = self._build_nav_batch_generator(
                    url)
            h5_pipe = map(
                self._save_to_h5,
                nav_gen
            )
            if TQDM_VERBOSE:
                end_pipe = tqdm.tqdm(
                    h5_pipe,
                    desc=f'{fund}:{self._path_extractor.isin}',
                    total=batch_count)
            else:
                end_pipe = h5_pipe
            return end_pipe
        except BaseException:
            print(traceback.format_exc())
            raise PipeBuildFailError()


    def _build_nav_batch_generator(
            self, url, batch_size=10, nav_filter=lambda x: x):
        self.nav_view.initialize(url)
        estimated_batch_count = int(self.nav_view.max_page_count * (10. / batch_size))
        nav_gen = self.nav_view.nav_generator()
        nav_gen =  nav_filter(nav_gen)
        return batch_generator(nav_gen, batch_size=batch_size), estimated_batch_count

    # @common
    def build_nav_generator(self, url):
        self.nav_view.initialize(url)
        estimated_count = self.nav_view.max_page_count * 10
        return self.nav_view.nav_generator(), estimated_count

    def _nav_filter(self, nav_gen):
        for date, nav in nav_gen:
            if VERBOSE:
                print(f'[_nav_filter] date: {date}')
            if len(pd.read_hdf(self._path_extractor.table_path,
                   'nav', where=f'date=="{date}"')) >= 1:
                if VERBOSE:
                    print(f'[_nav_filter] filter break')
                break
            else:
                yield date, nav

    def _estimate_batch_count(self):
        last_date_str = pd.read_hdf(
            self._path_extractor.table_path,
            'nav',
            where='index=0').iloc[0].date
        last_date = datetime.strptime(last_date_str, "%Y/%m/%d")
        return int((datetime.now() - last_date).days / 10 + 1)

    def _save_to_h5(self, nav_segment):
        try:
            path_extractor = self._path_extractor.tmp
            if VERBOSE:
                pprint.pprint(nav_segment)
            table = pd.DataFrame(nav_segment)
            table.columns = ['date', 'nav']
            if VERBOSE:
                pprint.pprint(table)
            directory = path_extractor.folder_path
            if not os.path.exists(directory):
                os.makedirs(directory)
            table.to_hdf(path_extractor.table_path,
                         'nav', append=True, format='table', 
                         data_columns=table.columns)
            if VERBOSE:
                print(f'save to: {path_extractor.table_path}')
            return str(table['date'].values[-1])
        except GeneratorExit as e:
            raise e
        except BaseException:
            print(traceback.format_exc())
            raise IterationFailError()

    def _update_h5(self):
        try:
            tmp_path_extractor = self._path_extractor.tmp
            # Step 1: concate old navs to tmp h5 file
            original_table = pd.read_hdf(
                self._path_extractor.table_path, 'nav')
            original_table.to_hdf(tmp_path_extractor.table_path,
                                  'nav', append=True, format='table', data_columns=original_table.columns)
        except BaseException:
            print(traceback.format_exc())
            raise IterationFailError()
        # Step 2: replace the old h5 file with the new h5 file
        shutil.copyfile(
            tmp_path_extractor.table_path,
            self._path_extractor.table_path)
        # Step 3: remove the tmp h5 file in nav_tmp
        os.remove(tmp_path_extractor.table_path)

    def _move_h5(self):
        tmp_path_extractor = self._path_extractor.tmp
        directory = self._path_extractor.folder_path
        if not os.path.exists(directory):
            os.makedirs(directory)
        # Step 1: copy nav_tmp h5 file to nav
        shutil.copyfile(
            tmp_path_extractor.table_path,
            self._path_extractor.table_path)
        # Step 2: remove the tmp h5 file in nav_tmp
        os.remove(tmp_path_extractor.table_path)

    def _delete_h5(self):
        path_extractor = self._path_extractor.tmp
        if os.path.exists(path_extractor.table_path):
            if VERBOSE:
                print(f'delete {path_extractor.table_path}')
            os.remove(path_extractor.table_path)


class ParallelFundNavDownloader:
    """
    TODO:
    - [ ] Filter the fund_link_generator to ignore re-download at the same day.
    - [-] Feature: Do not use SeleniumBase if the re-download time is later than 5 days ago. 
        - [X] Step 1: Build an HttpBase version FundNavExtractor, which only takes nav of the first page. 
            - [X] Refactor: split NavView into SeleniumNavView & HttpNavView
        - [X] Step 2: Allow filtering of fund_links by whether or not 
            the latest date is later than 5 days ago. 
                If true, send the items into the HttpBase Actor, else 
                send it to the SeleniumBase Actor.
        - [-] Step 3: Run HttpNavView-based and SeleniumNavView-based download
    """
    def __init__(self, parallel_cnt, nav_view_cls):
        self._actors = [_FundNavExtractActor.remote(pending_time=parallel_cnt * 20, nav_view_cls=nav_view_cls)
                        for i in range(parallel_cnt)]
        self._pool = ActorPool(self._actors)

    def map(self, fund_link_generator):
        try:
            return self._pool.map(lambda actor, input_tuple: actor.request.remote(input_tuple), fund_link_generator)
        except BaseException:
            print('start quiting actors')
            self.quite()

    def quit(self):
        # only do quit on the SeleniumBase Actors
        for actor in self._actors:
            try:
                actor.quit.remote()
                print(f'quit actor: {actor}')
            except BaseException:
                print(str(actor), 'selenium driver already quit')


@ray.remote
class _NewFundIdentifier:
    def request(self, input_tuple):
        fund_name, url = input_tuple
        is_new = self.is_new_link(fund_name)
        return fund, url, is_new
    def is_new_link(self, fund_name):
        """
        Args: fund_name (str)
        Returns: (bool) Whether or not the page of url has been crawled less than 5 days ago
        """
        path_extractor = TablePathExtractor(fund_name)
        if os.path.exists(path_extractor.table_path):
            last_date_str = pd.read_hdf(
                path_extractor.table_path,
                'nav',
                where='index=0').iloc[0].date
            last_date = datetime.strptime(last_date_str, "%Y/%m/%d")
            if (datetime.now() - last_date).days < 10:
                return True
            else:
                return False
        else:
            return False

class ParallelUpdatedFundIdentifier:
    def __init__(self, parallel_cnt):
        self._actors = [_NewFundIdentifier.remote()
                        for i in range(parallel_cnt)]
        self._pool = ActorPool(self._actors)
    def map(self, fund_link_generator):
        return self._pool.map(
            lambda actor, input_tuple: actor.request.remote(input_tuple), fund_link_generator)
            