"""
TODO: 
- [X] Remove table if exist
- [X] Refactor: combine initialize & nav_segment_generator -> Nav Batch Generator
- [X] Refactor: seperate isin, company extraction 
    -> 1. [X] build pipe (load isin & company here)
    -> 2. [X] run pipe (delete table at exception)
- [ ] Make sure chromedriver is deleted when KeyboradException occur
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

VERBOSE = False
TQDM_VERBOSE = True

class PipeBuildFailError(BaseException):
    pass

@ray.remote
class _FundNavExtractActor:
    def __init__(self, pending_time=0):
        self._nav_selenium = NavView()
        self._pending_time = pending_time
        self._first_try = True
    
    def request(self, input_tuple):
        try:
            if self._first_try:
                time.sleep(int(self._pending_time * random.random()))
                self._first_try = False
            gc.collect()
            fund, url = input_tuple
            end_pipe = self._build_pipe(fund, url)
            _ = list(end_pipe)
            return f"NAV DOWNLOAD OF {fund}:{self.__get_table_path(company, isin)} COMPLETE"
        except PipeBuildFailError as e:
            result = f"NAV DOWNLOAD FAILED with PipeBuildFailError:\n{fund}\nURL:\n{url}"
            print(result)
            return result
        except IterationFailError:
            result = f"NAV DOWNLOAD FAILED with IterationFailError:\n{fund}\nURL:\n{url}"
            print(result)
            print(traceback.format_exc())
            self._delete_h5(company, isin)
            self._nav_selenium.driver.refresh()
            return result
        except BaseException as e:
            print(traceback.format_exc())
            self.quit()
            raise e

    def quit(self):
        self._nav_selenium.quit()

    def _build_pipe(self, fund, url):
        try:
            info_extractor = FundInfoExtractor(FundDocumentPage().get_html(url))
            isin = info_extractor.isin
            company = info_extractor.company
            self._delete_h5(company, isin)
            nav_gen, batch_count = self._nav_selenium.build_nav_batch_generator(url)
            h5_pipe = map(
                partial(self._save_to_h5, isin=isin, company=company),
                nav_gen
            )
            if TQDM_VERBOSE:
                end_pipe = tqdm.tqdm(
                        h5_pipe,
                        desc=fund,
                        total=batch_count)
            else:
                end_pipe = h5_pipe
            return end_pipe
        except:
            print(traceback.format_exc())
            raise PipeBuildFailError()

    def _save_to_h5(self, nav_segment, isin='default', company='default'):
        table = pd.DataFrame(nav_segment)
        if VERBOSE:
            print(f'isin: {isin}; company: {company}')
            pprint.pprint(table)
        table.columns = ['date', 'nav']
        directory = self.__get_table_folder(company)
        if not os.path.exists(directory):
            os.makedirs(directory)
        table.to_hdf(self.__get_table_path(company, isin), 'nav', append=True)
        return str(table['date'].values[-1])
    
    def __get_table_path(self, company, isin):
        return f'{self.__get_table_folder(company)}/{isin}.h5'

    def __get_table_folder(self, company):
        return f'data/nav/{company}'
    
    def _delete_h5(self, company, isin):
        if os.path.exists(self.__get_table_path(company, isin)):
            if VERBOSE:
                print(f'delete {self.__get_table_path(company, isin)}')
            os.remove(self.__get_table_path(company, isin))

class ParallelFundNavDownloader:
    def __init__(self, parallel_cnt):
        self._actors = [_FundNavExtractActor.remote(pending_time=parallel_cnt * 20)
                    for i in range(parallel_cnt)]
        self._pool = ActorPool(self._actors)

    def map(self, fund_link_generator):
        try:
            return self._pool.map(lambda actor, input_tuple: actor.request.remote(input_tuple),
                                fund_link_generator)
        except:
            self.quite()

    def quit(self):
        for actor in self._actors:
            try:
                actor.quit.remote()
            except:
                print(str(actor), 'selenium driver already quit')
