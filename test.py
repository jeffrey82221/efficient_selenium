from src.processors.main_page import FundCountExtractor, MainPageDriver
from src.processors.fund_link_pages import LinkPageDriver, LinkExtractor

main_page_driver = MainPageDriver()
fund_count = FundCountExtractor(main_page_driver.get_html_by_url()).extract_info()
print(fund_count)
del main_page_driver

# Serial Method
def fund_link_generator():
    page_index = 1
    link_page_driver = LinkPageDriver()
    html = link_page_driver.get_html(page_index)
    link_extractor = LinkExtractor(html)
    generator = link_extractor.extract_info()
    for i in range(fund_count):    
        try:
            yield next(generator)
        except StopIteration:
            del html, generator, link_extractor
            page_index += 1
            html = link_page_driver.get_html(page_index)
            link_extractor = LinkExtractor(html)
            generator = link_extractor.extract_info()
result = []
for name_link_tuple in fund_link_generator():
    print(name_link_tuple)
    