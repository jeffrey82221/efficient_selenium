import tqdm
from src.downloader.fund_links_downloader import ParallelFundLinkDownloader
from src.downloader.fund_info_downloader import ParallelFundInfoDownloader
import pickle
downloader = ParallelFundLinkDownloader(20)
fund_links_path = 'data/fund_links.pickle'
try:
    fund_links = pickle.load(open(fund_links_path, 'rb'))
except:
    fund_links = list(tqdm.tqdm(downloader.fund_link_generator(), total=downloader.fund_count))
    with open('data/fund_links.pickle', 'wb') as f:
        pickle.dump(fund_links, f)
print('fund_links Loaded')
fund_info_downloader = ParallelFundInfoDownloader(5)
for fund_name, info in tqdm.tqdm(
    fund_info_downloader.map(fund_links), total=downloader.fund_count):
    new = fund_name
print('fund_info Loaded')