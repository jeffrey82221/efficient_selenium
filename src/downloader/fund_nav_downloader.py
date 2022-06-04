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
VERBOSE = False
TQDM_VERBOSE = True
@ray.remote
class _FundNavExtractActor:
    def __init__(self):
        self._nav_selenium = NavView()

    def quit(self):
        self._nav_selenium.quit()

    def request(self, input_tuple):
        fund, url = input_tuple
        self._nav_selenium.initialize(url)
        info_extractor = FundInfoExtractor(FundDocumentPage().get_html(url))
        isin = info_extractor.isin
        company = info_extractor.company
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
        return f"NAV DOWNLOAD OF {fund}:{isin} COMPLETE"

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
        table.to_hdf(f'data/nav/{company}.h5', isin, append=True)
        return str(table['date'].values[-1])

class ParallelFundNavDownloader:
    def __init__(self, parallel_cnt):
        self._actors = [_FundNavExtractActor.remote()
                    for i in range(parallel_cnt)]
        self._pool = ActorPool(self._actors)

    def map(self, fund_link_generator):
        return self._pool.map(lambda actor, input_tuple: actor.request.remote(input_tuple),
                              fund_link_generator)
    def quit(self):
        for actor in self._actors:
            actor.quit.remote()
