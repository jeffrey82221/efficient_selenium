from src.base.driver_base import SeleniumBase
from src.base.html_base import HtmlBase

import time

class NavView(SeleniumBase):
    @property
    def url(self):
        return "https://fund.cnyes.com/detail/%E5%AF%8C%E8%98%AD%E5%85%8B%E6%9E%97%E5%9D%A6%E4%BC%AF%E9%A0%93%E5%85%A8%E7%90%83%E6%8A%95%E8%B3%87%E7%B3%BB%E5%88%97-%E5%85%A8%E7%90%83%E5%B9%B3%E8%A1%A1%E5%9F%BA%E9%87%91%E7%BE%8E%E5%85%83A%20(acc)%E8%82%A1/B15%2C101/report/"


class NavExtractor(HtmlBase):
    def extract_info(self):
        soup = self.soup
        tables = soup.find_all('table')
        assert len(tables) == 2
        nav_table = tables[1]
        rows = nav_table.find_all('tr')[1:]
        date_n_navs = list(map(self.extract_date_n_nav_from_row, rows))
        return date_n_navs

    def extract_date_n_nav_from_row(self, row):
        cells = row.find_all('td')
        return cells[0].get_text(), float(cells[1].get_text())
    
nav_selenium = NavView()
html = nav_selenium.get_html_by_url()
nav_segment = NavExtractor(html).extract_info()
print(nav_segment)
nav_selenium.click_text('下一頁')
time.sleep(2)
html = nav_selenium.get_html_by_url()
nav_segment = NavExtractor(html).extract_info()
print(nav_segment)
nav_selenium.quit()