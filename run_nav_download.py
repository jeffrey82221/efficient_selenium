import tqdm
from src.downloader.fund_link_downloader import ParallelFundLinkDownloader
from src.downloader.fund_nav_downloader import ParallelFundNavDownloader
from src.page_extractor.fund_nav_extractor import SeleniumNavView
import pickle
import gc
from src.path import FUND_LINK_PATH

try:
    fund_links = pickle.load(open(FUND_LINK_PATH, 'rb'))
except BaseException:
    link_downloader = ParallelFundLinkDownloader(20)
    fund_links = list(
        tqdm.tqdm(
            link_downloader.fund_link_generator(),
            desc='Extract Fund Links from Web',
            total=link_downloader.fund_count))
    with open(FUND_LINK_PATH, 'wb') as f:
        pickle.dump(fund_links, f)
print(f'fund_links Loaded: {len(fund_links)}')
full_nav_downloader = ParallelFundNavDownloader(10, SeleniumNavView)
_ = list(tqdm.tqdm(
        full_nav_downloader.map(fund_links),
        desc='Download Fund Navs',
        total=len(fund_links)))
full_nav_downloader.quit()
print('Finish Download Fund Navs')