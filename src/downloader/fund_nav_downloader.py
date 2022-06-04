"""
TODO: 
- [ ] Remove table if exist
"""
from src.page_extractor.fund_nav_extractor import NavView, NavExtractor
from src.page_extractor.fund_info_extractor import FundInfoExtractor, FundDocumentPage
import pandas as pd
from functools import partial
import tqdm
import ray
from ray.util import ActorPool
import pprint
import os
import traceback
VERBOSE = False
TQDM_VERBOSE = False
@ray.remote
class _FundNavExtractActor:
    def __init__(self):
        self._nav_selenium = NavView()

    def quit(self):
        self._nav_selenium.quit()

    def delete_table(self, company, isin):
        if os.path.exists(self.__get_table_path(company, isin)):
            if VERBOSE:
                print(f'delete {self.__get_table_path(company, isin)}')
            os.remove(self.__get_table_path(company, isin))

    def request(self, input_tuple):
        try:
            fund, url = input_tuple
            self._nav_selenium.initialize(url)
            info_extractor = FundInfoExtractor(FundDocumentPage().get_html(url))
            isin = info_extractor.isin
            company = info_extractor.company
            self.delete_table(company, isin)
            nav_segment_gen = self._nav_segment_generator()
            if TQDM_VERBOSE:
                for item in tqdm.tqdm(
                    map(
                        partial(self._save_to_h5, isin=isin, company=company),
                        nav_segment_gen
                    ),
                    desc=fund,
                    total=self._nav_selenium.max_page_count):
                    if VERBOSE:
                        print(f"Finish Saving Data of {item} of {isin} of {company}")
                    else:
                        continue
            else:
                _ = list(map(
                        partial(self._save_to_h5, isin=isin, company=company),
                        nav_segment_gen
                    ))
            return f"NAV DOWNLOAD OF {fund}:{self.__get_table_path(company, isin)} COMPLETE"
        except BaseException as e:
            result = f"NAV DOWNLOAD FAILED:\n{fund}\nURL:\n{url}"
            print(result)
            traceback.print_exc()
            self.quite()
            print('Selenium driver stopped')
            self.delete_table(company, isin)
            print('Stored Table deleted')
            self._nav_selenium = NavView()
            print('Selenium driver restart')
            return result

    def _nav_segment_generator(self):
        for i in range(self._nav_selenium.max_page_count):
            nav_segment = NavExtractor(self._nav_selenium.get_html()).extract_info()
            yield nav_segment
            try:
                assert self._nav_selenium.has_next_page()
                self._nav_selenium.goto_next_page()
            except AssertionError:
                break

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
    

class ParallelFundNavDownloader:
    def __init__(self, parallel_cnt):
        self._actors = [_FundNavExtractActor.remote()
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
