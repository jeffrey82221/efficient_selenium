from src.processors.fund_info_extractor import FundDocumentPage, FundInfoExtractor
from src.downloader.fund_links_downloader import ParallelFundLinkDownloader

downloader = ParallelFundLinkDownloader(100)
import tqdm
for fund_name, url in tqdm.tqdm(downloader.fund_link_generator(), total=downloader.fund_count):
    doc = FundInfoExtractor(FundDocumentPage().get_html(url))
    print(doc.isin)
    print(doc.company)
