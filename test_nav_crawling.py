from src.base.driver_base import SeleniumBase
from src.base.html_base import HtmlBase
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

class NavView(SeleniumBase):
    def initialize(self, url):
        self.load_url(url)
        self.current_page = 1
        buttom_1 = WebDriverWait(self.driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//a[@data-value='1']"))
        )
        self.current_page_buttom_class_name = buttom_1.get_attribute("class")

    def goto_next_page(self):
        self.make_action()
        self.current_page += 1
        self.wait_state_change(self.current_page)

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
nav_selenium.initialize("https://fund.cnyes.com/detail/%E5%AF%8C%E8%98%AD%E5%85%8B%E6%9E%97%E5%9D%A6%E4%BC%AF%E9%A0%93%E5%85%A8%E7%90%83%E6%8A%95%E8%B3%87%E7%B3%BB%E5%88%97-%E5%85%A8%E7%90%83%E5%B9%B3%E8%A1%A1%E5%9F%BA%E9%87%91%E7%BE%8E%E5%85%83A%20(acc)%E8%82%A1/B15%2C101/report/")

html = nav_selenium.get_html()
nav_segment = NavExtractor(html).extract_info()
print(nav_segment)
nav_selenium.goto_next_page()
html_new = nav_selenium.get_html()
nav_segment = NavExtractor(html_new).extract_info()
print(nav_segment)
nav_selenium.goto_next_page()
html_new = nav_selenium.get_html()
nav_segment = NavExtractor(html_new).extract_info()
print(nav_segment)
nav_selenium.goto_next_page()
html_new = nav_selenium.get_html()
nav_segment = NavExtractor(html_new).extract_info()
print(nav_segment)
nav_selenium.quit()