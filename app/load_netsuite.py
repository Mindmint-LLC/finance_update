#%%

import pandas as pd
import os
from dbharbor.bigquery import SQL
import dbharbor
import _walk_gdrive as wg
import dlogging
logger = dlogging.NewLogger(__file__, use_cd=True)

logger.info('Beginning file loop')
con = SQL(os.getenv('DBT_KEYFILE'))


#%%

df = wg.main()
df = dbharbor.clean(df)

if df[df['Entry_ID'] == 'The results of this report are too large. Please narrow your results.'].shape[0] > 0:
    raise Exception('Missing Data')

logger.info('Starting data cleanup')
def amt_cleanup(x):
    x = x.replace('(', '-')
    x = x.replace('$', '')
    x = x.replace(')', '')
    x = x.replace(',', '')
    x = float(x)
    return x

df['Amount'] = df['Amount'].apply(amt_cleanup)
df['Effective_Date'] = pd.to_datetime(df['Effective_Date'])
df['Posted_Date'] = pd.to_datetime(df['Posted_Date'])


#%%

logger.info('Sending to Bigquery')
con.to_sql(df, 'fpa.stg_netsuite', index=False, if_exists='replace')
# df.to_sql('stg_netsuite', index=True, con=con.con, if_exists='replace')

logger.info('Done loading files')

#%%