import abc
from bs4 import BeautifulSoup
class HtmlBase(object):
    """
    Object using BeautifulSoup to extract
    information from an HTML page.
    """
    def __init__(self, html):
        self.soup = BeautifulSoup(html, 'html.parser')
        
    @abc.abstractmethod
    def extract_info(self, *args, **kargs):
        """
        Example:

        fund_count = list(filter(
            lambda x: len(
                x.find_all('strong')
            ) == 1 and '根據您的搜尋條件，共有' in x.get_text(
            ), soup.find_all('div'))
        )[0].find_all('strong')[0].get_text()
        fund_count = int(fund_count)
        return fund_count
        """
        pass