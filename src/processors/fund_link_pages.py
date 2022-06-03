from src.base.http_base import HttpBase
from src.base.html_base import HtmlBase
class LinkPageDriver(HttpBase):
    def get_url(self, page_number):
        assert page_number >= 1
        assert type(page_number) == int
        return f'https://fund.cnyes.com/search/?page={page_number}'

class LinkExtractor(HtmlBase):
    def extract_info(self):
        soup = self.soup
        fund_table = soup.find_all('table')[-1]
        name_n_links_generator = filter(
            lambda x: x is not None,
            map(self.get_name_n_link, fund_table.find_all('tr')[2:])
        )
        return name_n_links_generator

    def get_name_n_link(self, tr):
        try:
            a = tr.find_all('a')[0]
            return a.get_text(), 'https://fund.cnyes.com' + a['href']
        except (IndexError, KeyError):
            return None