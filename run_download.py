import tqdm
from src.downloader.fund_link_downloader import ParallelFundLinkDownloader
from src.downloader.fund_nav_downloader import ParallelFundNavDownloader
from src.page_extractor.fund_nav_extractor import SeleniumNavView
import pickle
import gc
fund_links_path = 'data/fund_links.pickle'
try:
    fund_links = pickle.load(open(fund_links_path, 'rb'))
except BaseException:
    link_downloader = ParallelFundLinkDownloader(20)
    fund_links = list(
        tqdm.tqdm(
            link_downloader.fund_link_generator(),
            desc='Extract Fund Links from Web',
            total=link_downloader.fund_count))
    with open('data/fund_links.pickle', 'wb') as f:
        pickle.dump(fund_links, f)
print(f'fund_links Loaded: {len(fund_links)}')
full_nav_downloader = ParallelFundNavDownloader(10, SeleniumNavView)
_ = list(tqdm.tqdm(
        full_nav_downloader.map(fund_links),
        desc='Download Fund Navs',
        total=len(fund_links)))
full_nav_downloader.quit()
print('Finish Download Fund Navs')