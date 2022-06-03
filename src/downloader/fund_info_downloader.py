from src.page_extractor.fund_info_extractor import FundDocumentPage, FundInfoExtractor
class SerialFundInfoDownloader:
    @staticmethod
    def map(fund_link_generator):
        for fund, url in fund_link_generator:
            doc = FundInfoExtractor(FundDocumentPage().get_html(url))
            yield fund, doc.extract_info()

import ray
from ray.util import ActorPool

@ray.remote
class _FundInfoExtractActor:
    def request(self, input_tuple):
        fund, url = input_tuple
        doc = FundInfoExtractor(FundDocumentPage().get_html(url))
        return fund, doc.extract_info()

class ParallelFundInfoDownloader:
    def __init__(self, parallel_cnt):
        self._pool = ActorPool([_FundInfoExtractActor.remote() for i in range(parallel_cnt)])
    
    def map(self, fund_link_generator):
        return self._pool.map(lambda actor, input_tuple: actor.request.remote(input_tuple), 
            fund_link_generator)

    
