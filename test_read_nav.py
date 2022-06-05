import pandas as pd
current_date = '2022/06/02'
print(pd.read_hdf('data/nav/景順證券投資信託股份有限公司/LU0048816135.h5', 'nav', where='date=="2022/06/02"'))
