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
"""
from src.page_extractor.fund_nav_extractor import NavView, NavExtractor, IterationFailError
from src.page_extractor.fund_info_extractor import FundInfoExtractor, FundDocumentPage
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
import copy
import shutil
from datetime import datetime

VERBOSE = False
TQDM_VERBOSE = True


class PipeBuildFailError(BaseException):
    pass


class ExtractorBuildFailError(BaseException):
    pass


class _TablePathExtractor(object):
    def __init__(self, url):
        try:
            info_extractor = FundInfoExtractor(
                FundDocumentPage().get_html(url))
        except BaseException:
            print(traceback.format_exc())
            raise ExtractorBuildFailError()
        self._isin = info_extractor.isin
        self._company = info_extractor.company
        self.dir_name = 'nav'

    @property
    def isin(self):
        return self._isin

    @property
    def table_path(self):
        return f'{self.folder_path}/{self._isin}.h5'

    @property
    def folder_path(self):
        return f'data/{self.dir_name}/{self._company}'

    @property
    def tmp(self):
        new_path_extractor = copy.deepcopy(self)
        new_path_extractor.dir_name = 'nav_tmp'
        return new_path_extractor


@ray.remote
class _FundNavExtractActor:
    def __init__(self, pending_time=0):
        self._nav_selenium = NavView()
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
            fund, url = input_tuple
            self._path_extractor = _TablePathExtractor(url)
            if VERBOSE:
                print(f'[request {input_tuple[0]}] _path_extractor built')
            if os.path.exists(self._path_extractor.table_path):
                download_mode = 'partial'
            else:
                download_mode = 'full'
            if VERBOSE:
                print(f'[request {input_tuple[0]}] {download_mode} pipe built')
            end_pipe = self._build_pipe(fund, url, download_mode=download_mode)
            agg_result = list(end_pipe)
            if VERBOSE:
                print(
                    f'[request {input_tuple[0]}] {download_mode} download finish')
            if download_mode == 'partial' and len(agg_result) > 0:
                if VERBOSE: 
                    print(f'[request {input_tuple[0]}] len(agg_result): {len(agg_result)}')
                self._update_h5()
                if VERBOSE:
                    print(f'[request {input_tuple[0]}] h5 updated')
            return f"NAV DOWNLOAD OF {fund}:{self._path_extractor.table_path} COMPLETE"
        except (PipeBuildFailError, ExtractorBuildFailError) as e:
            result = f"NAV DOWNLOAD FAILED with {str(e)}:\n{fund}\nURL:\n{url}"
            print(result)
            return result
        except (IterationFailError, KeyboardInterrupt):
            # KeyboardInterupt goes here
            result = f"NAV DOWNLOAD FAILED with IterationFailError:\n{fund}\nURL:\n{url}"
            print(result)
            print(traceback.format_exc())
            self._delete_h5(download_mode=download_mode)
            self._nav_selenium.driver.refresh()
            return result
        except BaseException as e:
            print(traceback.format_exc())
            self.quit()
            raise e

    def quit(self):
        self._nav_selenium.quit()

    def _build_pipe(self, fund, url, download_mode='full'):
        assert download_mode == 'full' or download_mode == 'partial'
        try:
            # If the table does not exist, do full download
            if download_mode == 'partial':
                nav_gen, _ = self._nav_selenium.build_nav_batch_generator(
                    url, nav_filter=self._nav_filter)
                batch_count = self._estimate_batch_count()
            else:
                # full download
                nav_gen, batch_count = self._nav_selenium.build_nav_batch_generator(
                    url)
            h5_pipe = map(
                partial(self._save_to_h5, download_mode=download_mode),
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

    def _nav_filter(self, nav_gen):
        for date, nav in nav_gen:
            if len(pd.read_hdf(self._path_extractor.table_path,
                   'nav', where=f'date=="{date}"')) >= 1:
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

    def _save_to_h5(self, nav_segment, download_mode='full'):
        try:
            assert download_mode == 'full' or download_mode == 'partial'
            if download_mode == 'partial':
                path_extractor = self._path_extractor.tmp
            else:
                path_extractor = self._path_extractor
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
                         'nav', append=True, format='table', data_columns=table.columns)
            return str(table['date'].values[-1])
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

    def _delete_h5(self, download_mode='full'):
        assert download_mode == 'full' or download_mode == 'partial'
        if download_mode == 'partial':
            path_extractor = self._path_extractor.tmp
        else:
            path_extractor = self._path_extractor
        if os.path.exists(path_extractor.table_path):
            if VERBOSE:
                print(f'delete {path_extractor.table_path}')
            os.remove(path_extractor.table_path)


class ParallelFundNavDownloader:
    def __init__(self, parallel_cnt):
        self._actors = [_FundNavExtractActor.remote(pending_time=parallel_cnt * 20)
                        for i in range(parallel_cnt)]
        self._pool = ActorPool(self._actors)

    def map(self, fund_link_generator):
        try:
            return self._pool.map(lambda actor, input_tuple: actor.request.remote(input_tuple),
                                  fund_link_generator)
        except BaseException:
            print('start quiting actors')
            self.quite()

    def quit(self):
        for actor in self._actors:
            try:
                actor.quit.remote()
                print(f'quit actor: {actor}')
            except BaseException:
                print(str(actor), 'selenium driver already quit')
