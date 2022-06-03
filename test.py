from src.processors.fund_links_downloader import ParallelFundLinkDownloader

downloader = ParallelFundLinkDownloader(8)
import tqdm
for fund_link in tqdm.tqdm(downloader.fund_link_generator(), total=downloader.fund_count):
    new = fund_link
