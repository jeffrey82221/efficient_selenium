import sys
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import sys
from time import sleep
from getpass import getpass
import os
import datetime
from bs4 import BeautifulSoup
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome('chromedriver', options=chrome_options)
def get_total_fund_count():
    driver.get(
        'https://fund.cnyes.com/search/'
    )
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    fund_count = list(filter(
        lambda x: len(
            x.find_all('strong')
        ) == 1 and '根據您的搜尋條件，共有' in x.get_text(
        ), soup.find_all('div'))
    )[0].find_all('strong')[0].get_text()
    fund_count = int(fund_count)
    return fund_count
print(get_total_fund_count())

def extract_fund_name_n_link(html, verbose=True):
    soup = BeautifulSoup(html, 'html.parser')
    fund_table = soup.find_all('table')[-1]
    def get_name_n_link(tr):
        try:
            a = tr.find_all('a')[0]
            return a.get_text(), 'https://fund.cnyes.com' + a['href']
        except (IndexError, KeyError):
            return None
    gen = filter(
        lambda x: x is not None,
        map(get_name_n_link, fund_table.find_all('tr')[2:])
    )
    name_n_links = list(gen)
    return name_n_links

def get_fund_name_n_link_page_html_core(page_number, verbose=True):
    assert page_number >= 1
    assert type(page_number) == int
    if verbose:
        print('GET FUND LINKS OF PAGE', page_number)

    driver.get(
        f'https://fund.cnyes.com/search/?page={page_number}'
    )
    html = driver.page_source
    return html

html = get_fund_name_n_link_page_html_core(1)
ans = extract_fund_name_n_link(html)
print(ans)