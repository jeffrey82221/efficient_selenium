from src.page_extractor.fund_nav_extractor import NavView, NavExtractor
from src.page_extractor.fund_info_extractor import FundInfoExtractor, FundDocumentPage
import pprint
import pandas as pd
from functools import partial
import tqdm
class FundNavExtractActor:
    def request(self, input_tuple):
        fund, url = input_tuple
        nav_selenium = NavView()
        nav_selenium.initialize(url)
        info_extractor = FundInfoExtractor(FundDocumentPage().get_html(url))
        print(info_extractor.extract_info())
        isin = info_extractor.isin
        company = info_extractor.company
        nav_segment_gen = self.nav_segment_generator(nav_selenium)
        _ = list(tqdm.tqdm(
            map(
                partial(self.save_to_h5, isin=isin, company=company),
                nav_segment_gen
            ), 
            total=nav_selenium.max_page_count))
        nav_selenium.quit()

    def nav_segment_generator(self, nav_selenium):
        for i in range(nav_selenium.max_page_count):
            nav_segment = NavExtractor(nav_selenium.get_html()).extract_info()
            yield nav_segment
            try:
                assert nav_selenium.has_next_page()
                nav_selenium.goto_next_page()
            except AssertionError:
                break

    def save_to_h5(self, nav_segment, isin='default', company='default'):
        table = pd.DataFrame(nav_segment)
        table.columns = ['date', 'nav']
        table.to_hdf(f'data/nav/{company}.h5', isin, append=True)
        return table

FundNavExtractActor().request(('中國信託 ESG 碳商機多重資產基金-臺幣B', "https://fund.cnyes.com/detail/%E4%B8%AD%E5%9C%8B%E4%BF%A1%E8%A8%97%20ESG%20%E7%A2%B3%E5%95%86%E6%A9%9F%E5%A4%9A%E9%87%8D%E8%B3%87%E7%94%A2%E5%9F%BA%E9%87%91-%E8%87%BA%E5%B9%A3B/A26105/"))