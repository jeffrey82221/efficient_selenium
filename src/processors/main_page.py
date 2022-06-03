from src.base.driver_base import SeleniumBase
from src.base.html_base import HtmlBase

class MainPageDriver(SeleniumBase):
    @property
    def url(self):
        return 'https://fund.cnyes.com/search/'

class FundCountExtractor(HtmlBase):
    def extract_info(self):
        soup = self.soup
        fund_count = list(filter(
                lambda x: len(
                    x.find_all('strong')
                ) == 1 and '根據您的搜尋條件，共有' in x.get_text(
            ), soup.find_all('div')))[0].find_all('strong')[0].get_text()
        return int(fund_count)