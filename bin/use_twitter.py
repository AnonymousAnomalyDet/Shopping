#!/usr/bin/env python

import pandas as pd
from rpy2.robjects import r, pandas2ri
import sys
pandas2ri.activate()
from rpy2.robjects.packages import importr
import df_utils



# df=df_utils.read_csv('out_total_combined.csv')
# df['hour']=df.date_key.map(lambda x: x.hour)

# hour=8
# #for hour in range(8,12):
# df_ts=df[(df.Region=='NAM') & (df.SVC_NM=='ATSEIMIP') & (df.hour==hour)][['date_key','CPU_TPS']]
# df_ts.rename_axis({'date_key':'timestamp', 'CPU_TPS':'count'},axis=1,inplace=True)


# tw_brk=importr('BreakoutDetection')
# df_ts_R=pandas2ri.py2ri(df_ts)
# result=tw_brk.breakout(df_ts_R, method='multi')

# for v in result.rx2('loc'):
#     print(v,df_ts[v:v+1], df_ts_R[v])
#     #print(v, df_ts[v:v+1], df_ts_R[v:v+1])
#     continue
# #    continue






def get_breakpoints(df, ts='ds', y='y'):
    df_R=pandas2ri.py2ri(df.rename_axis({ts:'timestamp', y:'count'},axis=1))
    
    tw_brk=importr('BreakoutDetection')
    result=tw_brk.breakout(df_R, method='multi')

    breakouts=[]
    for v in result.rx2('loc'):
        breakouts.append(df[ts][v])
    return breakouts

