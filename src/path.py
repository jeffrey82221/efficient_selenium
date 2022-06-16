import pandas as pd
import os
import copy
from pathlib import Path
import traceback
# import ray
import functools
from src.exceptions import ExtractorBuildFailError
import shutil
H5_PATH = 'data/fund_info.h5'
FUND_LINK_PATH = 'data/fund_links.pickle'

class TablePathExtractor(object):
    def __init__(self, fund_name, id):
        self._company, self._isin = TablePathExtractor.__get_data(fund_name, id)
        self.dir_name = 'nav'
    
    @staticmethod
    def register(id):
        shutil.copyfile(
            H5_PATH,
            TablePathExtractor._get_h5_path(id))

    @staticmethod
    def unregister(id):
        os.remove(TablePathExtractor._get_h5_path(id))
        
    @staticmethod
    def _get_h5_path(id):
        return f'{H5_PATH.split(".h5")[0]}_{id}.h5'

    @staticmethod
    @functools.lru_cache(maxsize=None)
    def __get_data(fund_name, id):
        try:
            table = pd.read_hdf(
                TablePathExtractor._get_h5_path(id), 'raw', where=f'基金名稱=="{fund_name}"', mode='r',
                columns = ['ISIN', '基金管理公司'])
            assert len(table) == 1
            isin = table['ISIN'].values[0]
            company = table['基金管理公司'].values[0]
        except BaseException:
            print(traceback.format_exc())
            raise ExtractorBuildFailError()
        return company, isin

    @property
    def isin(self):
        return self._isin

    @property
    def table_path(self):
        return f'{self.folder_path}/{self._isin}.h5'

    @property
    def folder_path(self):
        current_path = Path(__file__).parent.parent
        return os.path.join(current_path, f'data/{self.dir_name}/{self._company}')

    @property
    def tmp(self):
        new_path_extractor = copy.deepcopy(self)
        new_path_extractor.dir_name = 'nav_tmp'
        return new_path_extractor
    