from src.processors.main_page import FundCountExtractor, MainPageDriver
from src.processors.fund_link_pages import LinkPageDriver, LinkExtractor

class SerialFundLinkDownloader:
    def __init__(self):
        main_page_driver = MainPageDriver()
        self.fund_count = FundCountExtractor(main_page_driver.get_html_by_url()).extract_info()
        del main_page_driver

    # Serial Method
    def fund_link_generator(self):
        page_index = 1
        link_page_driver = LinkPageDriver()
        html = link_page_driver.get_html(page_index)
        link_extractor = LinkExtractor(html)
        generator = link_extractor.extract_info()
        for i in range(self.fund_count):    
            try:
                yield next(generator)
            except StopIteration:
                del html, generator, link_extractor
                page_index += 1
                html = link_page_driver.get_html(page_index)
                link_extractor = LinkExtractor(html)
                generator = link_extractor.extract_info()

# Multi-Process Methods:
import ray
import gc
@ray.remote
class FundLinkProducer:
    def __init__(self):
        self.__link_page_driver = LinkPageDriver()
        self.__generator = None
    def switch_generator(self, page_index):
        html = self.__link_page_driver.get_html(page_index)
        self.__generator = LinkExtractor(html).extract_info()
        del html
        gc.collect()
    def next_item(self):
        if self.__generator is None:
            return None
        else:
            try:
                return next(self.__generator)
            except StopIteration:
                return None

class ParallelFundLinkDownloader:
    def __init__(self, parallel_cnt=1):
        main_page_driver = MainPageDriver()
        self.fund_count = FundCountExtractor(main_page_driver.get_html_by_url()).extract_info()
        del main_page_driver
        self.parallel_cnt = parallel_cnt
        self.producer_pool = [FundLinkProducer.remote()] * parallel_cnt
    def fund_link_generator(self):
        page_index = 1
        producer_index = 0
        for producer_index in range(self.fund_count):
            producer = self.producer_pool[producer_index % self.parallel_cnt]
            fund_link_item = ray.get(producer.next_item.remote())
            if fund_link_item is None:
                producer.switch_generator.remote(page_index)
                page_index += 1
                fund_link_item = ray.get(producer.next_item.remote())
            yield fund_link_item
