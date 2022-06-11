from src.base.driver_base import SeleniumBase
from src.base.http_base import HttpBase
from src.base.html_base import HtmlBase
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import gc
import pprint
import traceback


class _LastPageExtractor(HtmlBase):
    def extract_info(self):
        soup = self.soup
        a_elements = soup.find_all('a')
        page_buttoms = list(filter(
            lambda a: a.has_attr('data-value'), a_elements))
        page_count = int(page_buttoms[0].parent.parent.findChildren()[-1].text)
        return page_count




# TODO: Refactor # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# 1. [X] extract @common in NavView(SeleniumBase) to fund_nav_downloader 
# 2. [X] In HttpBase version of NavView, build its own version of method labeled with @http
#    - [X] allow get_url of the child of HttpBase to have load_url, 
#            where self._url is set and used in get_url
#    - [X] The HttpBase version Class must have its own _initialize, 
#            where self.current_page_index / self.current_page_buttom_class_name can be ignored. 
#    - [ ] TODO: Problem: HttpNavView produce html that has only one table! 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
class HttpNavView(HttpBase):
    # @http
    def nav_generator(self):
        try:
            nav_segment = NavExtractor(self.get_html()).extract_info()
            for date, nav in nav_segment:
                yield date, nav
        except GeneratorExit as e:
            raise e
        except BaseException as e:
            print(f'[_nav_generator] error happended for url:{self.get_url()}')
            print(traceback.format_exc())
            raise e

    def get_url(self, *args, **kargs):
        return self._url

    # @http
    def initialize(self, url):
        self._load_url(url)
        self.max_page_count = 1
    
    def _load_url(self, url):
        self._url = url
    
class SeleniumNavView(SeleniumBase):
    # @selenium
    def nav_generator(self):
        try:
            for i in range(self.max_page_count):
                nav_segment = NavExtractor(self.get_html()).extract_info()
                for date, nav in nav_segment:
                    yield date, nav
                if self._has_next_page():
                    self._goto_next_page()
                else:
                    break
        except GeneratorExit as e:
            raise e
        except BaseException as e:
            print(f'[_nav_generator] error happended for url:{self.url}')
            print(traceback.format_exc())
            raise e

    # @selenium 
    def initialize(self, url):
        self.url = url
        self.load_url(url)
        self.current_page_index = 1
        buttom_1 = WebDriverWait(self.driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//a[@data-value='1']"))
        )
        self.current_page_buttom_class_name = buttom_1.get_attribute("class")
        self.max_page_count = _LastPageExtractor(
            self.get_html()).extract_info()
        gc.collect()


    # @selenium
    def _has_next_page(self):
        return self.current_page_index < self.max_page_count

    # @selenium
    def _goto_next_page(self):
        assert self.current_page_index < self.max_page_count
        self._make_action()
        self.current_page_index += 1
        self._wait_state_change(self.current_page_index)

    # @selenium
    def _make_action(self):
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

    # @selenium
    def _wait_state_change(self, page_index):
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
        if len(tables) != 2:
            print('[NavExtractor: extract_info] len(tables):', len(tables), '!=2')
            raise AssertionError()
        nav_table = tables[1]
        rows = nav_table.find_all('tr')[1:]
        date_n_navs = list(map(self.extract_date_n_nav_from_row, rows))
        return date_n_navs
        

    def extract_date_n_nav_from_row(self, row):
        cells = row.find_all('td')
        return cells[0].get_text(), float(cells[1].get_text())
