from src.base.driver_base import SeleniumBase
from src.base.html_base import HtmlBase
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import gc
import pprint

class _LastPageExtractor(HtmlBase):
    def extract_info(self):
        soup = self.soup
        a_elements = soup.find_all('a')
        page_buttoms = list(filter(
            lambda a: a.has_attr('data-value'), a_elements))
        page_count = int(page_buttoms[0].parent.parent.findChildren()[-1].text)
        return page_count

class NavView(SeleniumBase):
    def __init__(self, verbose=False):
        super().__init__()
        self.__verbose = verbose
    def initialize(self, url):
        self.url = url
        self.load_url(url)
        self.current_page_index = 1
        buttom_1 = WebDriverWait(self.driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//a[@data-value='1']"))
        )
        self.current_page_buttom_class_name = buttom_1.get_attribute("class")
        self.max_page_count = _LastPageExtractor(self.get_html()).extract_info()
        gc.collect()
            
    def show_current_states(self):
        pprint.pprint({
            'url': self.url,
            'max_page_count': self.max_page_count,
            'current_page_buttom_class_name': self.current_page_buttom_class_name,
            'current_page': self.current_page_index
        })

    def has_next_page(self):
        return self.current_page_index < self.max_page_count

    def goto_next_page(self):
        assert self.current_page_index < self.max_page_count
        self.make_action()
        self.current_page_index += 1
        self.wait_state_change(self.current_page_index)

    def make_action(self):
        buttom_name = '下一頁'
        buttom = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.LINK_TEXT, buttom_name))
        )
        assert buttom.is_enabled()
        assert buttom.text == buttom_name
        java_script = f"""
        for (const a of document.querySelectorAll('a')) {{
            if (a.textContent.includes('{buttom_name}')) {{
                a.click()
            }}
        }}
        """
        self.driver.execute_script(java_script)
    
    def wait_state_change(self, page_index):
        xpath_selector = f"//a[@class='{self.current_page_buttom_class_name}']"
        WebDriverWait(self.driver, 20).until(
            EC.text_to_be_present_in_element((
                By.XPATH, 
                xpath_selector
            ), str(page_index))
        )


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
nav_selenium.initialize("https://fund.cnyes.com/detail/%E4%B8%AD%E5%9C%8B%E4%BF%A1%E8%A8%97%20ESG%20%E7%A2%B3%E5%95%86%E6%A9%9F%E5%A4%9A%E9%87%8D%E8%B3%87%E7%94%A2%E5%9F%BA%E9%87%91-%E8%87%BA%E5%B9%A3B/A26105/report/")
nav_segment = NavExtractor(nav_selenium.get_html()).extract_info()
pprint.pprint(nav_segment)
while nav_selenium.has_next_page():
    nav_selenium.goto_next_page()
    nav_selenium.show_current_states()
    nav_segment = NavExtractor(nav_selenium.get_html()).extract_info()
    pprint.pprint(nav_segment)

nav_selenium.quit()