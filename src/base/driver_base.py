import abc
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException,
    ElementNotInteractableException
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains

class SeleniumBase(object):
    """
    Object using selenium to obtain
    a HTML page given a url dependent on
    some arguments.
    """

    def __init__(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_prefs = {}
        chrome_prefs["profile.default_content_settings"] = {"images": 2}
        chrome_prefs["profile.managed_default_content_settings"] = {"images": 2}
        chrome_options.experimental_options["prefs"] = chrome_prefs
        try:
            self.driver = webdriver.Chrome(
                'chromedriver', options=chrome_options)
        except BaseException:
            self.driver = webdriver.Chrome(
                ChromeDriverManager().install(),
                options=chrome_options)

    @property
    @abc.abstractmethod
    def url(self):
        return ""

    def get_html_by_url(self):
        self.driver.get(
            self.url
        )
        html = self.driver.page_source
        return html

    def get_html(self, *args, **kargs):
        self.driver.get(
            self.get_url(*args, **kargs)
        )
        html = self.driver.page_source
        return html

    @abc.abstractmethod
    def get_url(self, *args, **kargs):
        pass

    def quit(self):
        self.driver.quit()

    def click_text(self, text):
        try:
            button = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.LINK_TEXT, text))
            )
            assert button.is_enabled()
            assert button.text == text
            action = ActionChains(self.driver)
            action.move_to_element(button).click().perform()
            action.move_to_element(button).click().perform()
            action.move_to_element(button).click().perform()
            button = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.LINK_TEXT, '上一頁'))
            )
            print(button.text)
            success = True
        except (NoSuchElementException, ElementNotInteractableException):
            success = False
        except BaseException as e:
            raise e
        return success

