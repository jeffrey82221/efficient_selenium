from src.page_extractor.fund_nav_extractor import NavView, NavExtractor
import pprint

nav_selenium = NavView()
nav_selenium.initialize(
    "https://fund.cnyes.com/detail/%E4%B8%AD%E5%9C%8B%E4%BF%A1%E8%A8%97%20ESG%20%E7%A2%B3%E5%95%86%E6%A9%9F%E5%A4%9A%E9%87%8D%E8%B3%87%E7%94%A2%E5%9F%BA%E9%87%91-%E8%87%BA%E5%B9%A3B/A26105/report/")
nav_segment = NavExtractor(nav_selenium.get_html()).extract_info()
pprint.pprint(nav_segment)
while nav_selenium.has_next_page():
    nav_selenium.goto_next_page()
    nav_selenium.show_current_states()
    nav_segment = NavExtractor(nav_selenium.get_html()).extract_info()
    pprint.pprint(nav_segment)

nav_selenium.quit()
