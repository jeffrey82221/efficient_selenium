from src.base.http_base import HttpBase
from src.base.html_base import HtmlBase


class FundDocumentPage(HttpBase):
    def get_url(self, url):
        return url.replace('/report/', '/document/')


class FundInfoExtractor(HtmlBase):
    def extract_info(self):
        """
        Get All Information
        """
        soup = self.soup
        tables = soup.find_all('table')
        base_info = tables[0]
        fee_info = tables[1]
        fund_info = {
            **self.__extract_fund_info(base_info),
            **self.__extract_fund_info(fee_info)
        }
        return fund_info

    @property
    def isin(self):
        return self.extract_info()['ISIN']

    @property
    def company(self):
        return self.extract_info()['基金管理公司']

    def __extract_fund_info(self, table_soup):
        cols = table_soup.find_all('tr')
        result = []
        for col in cols:
            ths = col.find_all('th')
            tds = col.find_all('td')
            for th, td in zip(ths, tds):
                result.append((
                    th.get_text(),
                    td.get_text()
                ))
        return dict(result)
