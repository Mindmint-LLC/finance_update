#%%

import os
from dbharbor.bigquery import SQL
from dgsheet import to_gsheet
import dlogging
logger = dlogging.NewLogger(__file__, use_cd=True)

con = SQL(os.getenv('DBT_KEYFILE'))
logger.info('Pulling fct_gl_summary')

df = con.read('''
select Effective_Date,
    Company,
    Account,
    Account_Name,
    Category,
    Category_Name,
    SubCategory1,
    SubCategory2,
    Dept,
    Dept_Name,
    Amount,
    yr,
    mnth,
    Account_Type,
    Cash_Type,
    AmountCF,
    PostedEOM,
    Category_CashIS
from fpa.fct_gl__summary''')


url = "https://docs.google.com/spreadsheets/d/1_knhmDRaQWrcFfhbywHv-O0rCQ-VhBdb18wNRQdSvZI/edit?gid=2013706554#gid=2013706554"
filepath_cred='./volume/bigquery-bbg-platform.json'


logger.info('Sending to gsheet')
to_gsheet(df, url, filepath_cred=filepath_cred, clear_sheet=True)

logger.info('gsheet loaded')


#%%