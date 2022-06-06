from src.base.driver_base import SeleniumBase
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


class IterationFailError(BaseException):
    pass


class NavView(SeleniumBase):
    def __init__(self, verbose=False):
        super().__init__()
        self.__verbose = verbose

    def build_nav_batch_generator(
            self, url, batch_size=10, nav_filter=lambda x: x):
        self._initialize(url)
        estimated_batch_count = int(self.max_page_count * (10. / batch_size))
        return self._nav_batch_generator(nav_filter(
            self._nav_generator()), batch_size=batch_size), estimated_batch_count

    def build_nav_generator(self, url):
        self._initialize(url)
        estimated_count = self.max_page_count * 10
        return self._nav_generator(), estimated_count

    def _nav_batch_generator(self, nav_generator, batch_size=10):
        """
        Args:
            - url: the url of the nav page
            - batch_size: number of nav per page
        """
        try:
            result = []
            for date, nav in nav_generator:
                result.append((date, nav))
                if len(result) == batch_size:
                    yield result
                    result = []
                    gc.collect()
        except KeyboardInterrupt as e:
            raise e
        except BaseException:
            print(traceback.format_exc())
            raise IterationFailError()

    def _nav_generator(self):
        try:
            for i in range(self.max_page_count):
                nav_segment = NavExtractor(self.get_html()).extract_info()
                for date, nav in nav_segment:
                    yield date, nav
                if self._has_next_page():
                    self._goto_next_page()
                else:
                    break
        except BaseException as e:
            print(f'[_nav_generator] error happended for url:{self.url}')
            raise e

    def _initialize(self, url):
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

    def show_current_states(self):
        pprint.pprint({
            'url': self.url,
            'max_page_count': self.max_page_count,
            'current_page_buttom_class_name': self.current_page_buttom_class_name,
            'current_page': self.current_page_index
        })

    def _has_next_page(self):
        return self.current_page_index < self.max_page_count

    def _goto_next_page(self):
        assert self.current_page_index < self.max_page_count
        self._make_action()
        self.current_page_index += 1
        self._wait_state_change(self.current_page_index)

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
