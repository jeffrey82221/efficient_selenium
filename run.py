import tqdm
from src.downloader.fund_link_downloader import ParallelFundLinkDownloader
from src.downloader.fund_info_downloader import ParallelFundInfoDownloader
from src.downloader.fund_nav_downloader import ParallelFundNavDownloader
import pickle
link_downloader = ParallelFundLinkDownloader(20)
fund_links_path = 'data/fund_links.pickle'
try:
    fund_links = pickle.load(open(fund_links_path, 'rb'))
except BaseException:
    fund_links = list(
        tqdm.tqdm(
            link_downloader.fund_link_generator(),
            total=link_downloader.fund_count))
    with open('data/fund_links.pickle', 'wb') as f:
        pickle.dump(fund_links, f)
print('fund_links Loaded')

fund_nav_downloader = ParallelFundNavDownloader(100)
for fund_name in tqdm.tqdm(
        fund_nav_downloader.map(fund_links), total=link_downloader.fund_count):
    print(fund_name)
fund_nav_downloader.quit()
"""
fund_info_downloader = ParallelFundInfoDownloader(30)
for fund_name, info in tqdm.tqdm(
        fund_info_downloader.map(fund_links), total=link_downloader.fund_count):
    new = fund_name
print('fund_info Loaded')
"""