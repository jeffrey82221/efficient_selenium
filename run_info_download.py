"""
Save Fund Infos: 
columns = ['基金名稱', '基金名稱 (英文)', '基金管理公司', '基金經理人', '基金規模', '基金註冊地', 
        '投資地區', '計價幣別', '基金組別', '晨星組別', 'ISIN', '基準指數', '成立日期', '風險評等', 
        '晨星評等', '投資策略', '最低認購金額', '申購手續費', '管理費', '最高管理費', '遞延費', '最高遞延費', '分銷費', '保管費']
"""
import tqdm
from src.downloader.fund_link_downloader import ParallelFundLinkDownloader
from src.downloader.fund_info_downloader import ParallelFundInfoDownloader
from src.utils import batch_generator
import pickle
import pandas as pd
import gc
import os
import traceback
import warnings
from tables import NaturalNameWarning
from src.path import H5_PATH, FUND_LINK_PATH

warnings.filterwarnings('ignore', category=NaturalNameWarning)
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

BATCH_SIZE = 1000

def save_to_h5(info_batch):
    table = pd.DataFrame.from_records(info_batch)
    table.to_hdf(H5_PATH,
                 'raw', append=True, format='table', 
                 data_columns=table.columns,
                 min_itemsize={
                     '基金名稱': 150, 
                     '基金名稱 (英文)': 100, 
                     '晨星組別': 50, 
                     '基金註冊地': 10,
                     '投資地區': 40,
                     '基準指數': 100,
                     '基金經理人': 50,
                     '基金組別': 30,
                     '申購手續費': 6
                    }
                 )

if not os.path.exists(H5_PATH):
    try:
        info_downloader = ParallelFundInfoDownloader(20)
        info_gen = map(lambda x: x[1], info_downloader.map(fund_links))
        info_batch_gen = batch_generator(info_gen, batch_size=BATCH_SIZE)
        _ = list(tqdm.tqdm(map(save_to_h5, info_batch_gen), total=int(len(fund_links)/BATCH_SIZE)))
        print('Finish Download Fund Info Fully')
    except:
        print(traceback.format_exc())
        os.remove(H5_PATH)
        print(f'{H5_PATH} Removed')

table = pd.read_hdf(H5_PATH, 'raw', where=f'基金名稱=="貝萊德世界科技基金 A2"', 
    columns = ['ISIN', '基金管理公司'])
print(table['ISIN'].values[0])
print(table['基金管理公司'].values[0])