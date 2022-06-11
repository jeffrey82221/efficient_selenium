import abc
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager


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
        chrome_prefs["profile.managed_default_content_settings"] = {
            "images": 2}
        chrome_options.experimental_options["prefs"] = chrome_prefs
        try:
            # On Mac M1:
            self.driver = webdriver.Chrome(
                'chromedriver', options=chrome_options)
        except BaseException:
            self.driver = webdriver.Chrome(
                ChromeDriverManager().install(),
                options=chrome_options)
        tz_params = {'timezoneId': 'Asia/Shanghai'}
        self.driver.execute_cdp_cmd('Emulation.setTimezoneOverride', tz_params)
        print('TimeZone Set to Asia/Shanghai')

    def load_url(self, url):
        self.driver.get(url)

    def get_html(self):
        return self.driver.page_source

    def quit(self):
        self.driver.quit()

    @abc.abstractmethod
    def make_action(self, *arg, **kargs):
        """
        Example:
        button = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.LINK_TEXT, text))
            ).click()
        """
        pass
