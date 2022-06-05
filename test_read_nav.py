import pandas as pd
import os
table = pd.read_hdf('data/nav/施羅德投資管理（盧森堡）有限公司/LU0091253459.h5', 'nav')
print(table)

# Step 1: concate old navs to tmp h5 file
# original_table = pd.read_hdf('data/nav/施羅德投資管理（盧森堡）有限公司/LU0091253459.h5', 'nav')
# original_table.to_hdf('data/nav_tmp/施羅德投資管理（盧森堡）有限公司/LU0091253459.h5', 'nav', append=True, format='table', data_columns=original_table.columns)

# print(pd.read_hdf('data/nav_tmp/施羅德投資管理（盧森堡）有限公司/LU0091253459.h5', 'nav'))

# Step 2: replace the old h5 file with the new h5 file 
# import shutil
# shutil.copyfile('data/nav_tmp/施羅德投資管理（盧森堡）有限公司/LU0091253459.h5', 'data/nav/施羅德投資管理（盧森堡）有限公司/LU0091253459.h5')
# print(pd.read_hdf('data/nav/施羅德投資管理（盧森堡）有限公司/LU0091253459.h5', 'nav'))

# Step 3: remove the tmp h5 file in nav_tmp
#　import os
# os.remove('data/nav_tmp/施羅德投資管理（盧森堡）有限公司/LU0091253459.h5')
