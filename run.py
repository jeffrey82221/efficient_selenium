import tqdm
from src.downloader.fund_link_downloader import ParallelFundLinkDownloader
from src.downloader.fund_info_downloader import ParallelFundInfoDownloader
from src.downloader.fund_nav_downloader import ParallelFundNavDownloader, ParallelUpdatedFundIdentifier
from src.page_extractor.fund_nav_extractor import SeleniumNavView, HttpNavView
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
update_fund_identifier = ParallelUpdatedFundIdentifier(20)
fund_links_w_status = list(tqdm.tqdm(update_fund_identifier.map(fund_links), desc='Identify Fund Nav Status', total=len(fund_links)))
print('Finish Identifying Download Status of Funds')
updated_fund_links = list(map(lambda x: (x[0], x[1]), filter(lambda x: x[2], fund_links_w_status)))
outdated_fund_links = list(map(lambda x: (x[0], x[1]), filter(lambda x: not x[2], fund_links_w_status)))
del update_fund_identifier, fund_links_w_status
gc.collect()
print(f'updated fund count: {len(updated_fund_links)}')
print(f'outdated fund count: {len(outdated_fund_links)}')
"""
_ = list(tqdm.tqdm(
        ParallelFundNavDownloader(20, HttpNavView).map(updated_fund_links),
        desc='Update Fund Navs',
        total=len(updated_fund_links)))
del updated_fund_links
gc.collect()
print('Finish Update Fund Navs')
"""
full_nav_downloader = ParallelFundNavDownloader(10, SeleniumNavView)
_ = list(tqdm.tqdm(
        full_nav_downloader.map(outdated_fund_links),
        desc='Download Fund Navs Fully',
        total=len(outdated_fund_links)))
full_nav_downloader.quit()
del outdated_fund_links
gc.collect()
print('Finish Download Fund Navs Fully')