
import abc
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import gc
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
        try:
            self.driver = webdriver.Chrome('chromedriver', options=chrome_options)
        except:
            self.driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
    
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

    @abc.abstractmethod
    def get_url(self, *args, **kargs):
        pass

    def get_html(self, *args, **kargs):
        self.driver.get(
            self.get_url(*args, **kargs)
        )
        html = self.driver.page_source
        return html   
    
    def __del__(self):
        self.driver.quit()
        del self.driver
        gc.collect()