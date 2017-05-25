#!/usr/bin/env python

import PlottingUtils
plt=PlottingUtils.Plt(True)
import pandas as pd
import fbprophet
import datetime
import df_utils
import numpy as np

from use_twitter import get_breakpoints

max_date=pd.Timestamp('2017-05-01')
log_transform=True

def predict(df,periods=None,name='none', maxy=None, clear=False, ylabel='CPU-TPS', ignore_after_infit=None,freq='d'):
    l_bad_hours=['ATSEIMIP_APAC_current_CPU_TPS_8', 'ATSEIMIP_APAC_current_CPU_TPS_21','ATSEIMIP_EMEA_current_CPU_TPS_7',
                 'HPSLFIS1_EMEA_current_CPU_TPS_17','HPSLFIS1_EMEA_current_CPU_TPS_18',]
    bad_hours=set(l_bad_hours)

    global log_transform
    
    yearly_seasonality=True
    if name in bad_hours:yearly_seasonality=False
    print('fbprophet predict:', name, 'yearly_seasonality:', yearly_seasonality)
    now=pd.Timestamp(datetime.datetime.now())
    fit_cut_off=now
    if ignore_after_infit is not None:
        fit_cut_off=pd.Timestamp(ignore_after_infit)
        pass
    
    if periods is None:
        periods=(pd.Timestamp('2018-01-31')-fit_cut_off).days
        pass
    
    df_fit=df[df.date_key<pd.Timestamp(fit_cut_off)][['ds','y']].copy()
    df_fit.index=[i for i in range(len(df_fit))]
    changepoints=get_breakpoints(df_fit)
    print(changepoints)
    m=fbprophet.forecaster.Prophet(growth='linear', holidays=pd.DataFrame({ "ds": [pd.Timestamp('2016-11-28')], "holiday": ['cyber monday']}),
                                   yearly_seasonality=yearly_seasonality,)#changepoints=changepoints,)
    plt.clf()
    plt.plot(df_fit.ds,df_fit.y)
    plt.savefig('plots/%s_pre.png' %name)
    m.fit(df_fit)
    future=m.make_future_dataframe(periods=periods,freq='d')
    forecast=m.predict(future)

    m.plot(forecast)
    plt.xlabel('Date')
    plt.ylabel(ylabel)
    for cp in changepoints:
        plt.axvline(cp,color='r',linewidth=0.3)
        continue
    plt.savefig('plots/%s.png' %name)
    plt.clf()
    # forecast.loc[forecast.ds<=df_fit.ds.max(),'CPU_TPS']=df[df.date_key<=df_fit.ds.max()].CPU_TPS.tolist()
    # if fit_cut_off != now :
    #     forecast.loc[(forecast.ds>=df_fit.ds.max()) & (forecast.ds<=df.date_key.max()), 'CPU_TPS']=df[(df.ds>df_fit.ds.max()) & (df.hour==df.PeakHour)].CPU_TPS.tolist()
    #     pass
    # forecast.loc[forecast.ds>df.date_key.max(),'CPU_TPS']=forecast[forecast.ds>df.date_key.max()].yhat
    #return (forecast[['ds','yhat']], now, fit_cut_off, df.SVC_NM.unique().tolist()[0])
    #return forecast[['ds','yhat']]
    if log_transform:
        forecast['yhat']=np.exp(forecast['yhat'])
        name+='_log'
        pass
    df_utils.to_csv(forecast, 'fbprophet_results/%s.csv' %name)
    return
    
    

df=df_utils.read_csv('out_total_combined_smooth.csv')
if 'date2' not in df.columns: df['date2']=df.date_key.map(lambda x: x.date())
df['hour']=df.date_key.map(lambda x: x.hour)
import datetime

df=df[df.date_key<max_date]

if log_transform:        
    df['CPU_TPS']=np.log(df['CPU_TPS'])
    pass


from multiprocessing import Pool, Process
from multiprocessing.managers import SyncManager
m=SyncManager()
m.start()

pool=Pool(processes=8)


for r in ['LAC', 'ALL', 'EMEA', 'APAC', 'NAM']:
#for r in ['EMEA', 'APAC', 'NAM']:   
    for s in df.SVC_NM.unique():
    #for s in ['ISELL']:
        for hour in range(0,24):
            # df2=df_utils.make_future(max_date, pd.Timestamp('2018-01-31'), col_name='date2')
            # df2['SVC_NM']=df2.date2.map(lambda x: s)
            # df2['Region']=df2.date2.map(lambda x: r)
            # df2['date_key']=df2.date2.map(lambda x: x+datetime.timedelta(0,hour*3600))
            # df2['hour']=df2.date_key.map(lambda x: x.hour)
            # df2['CPU_TPS']=0
            # df2=df2.append(df[(df.Region==r) &(df.SVC_NM==s)&(df.hour==hour)].
            for y in ['CPU_TPS',]:#'CPU_TPS_smooth_all_hourly']:
                #if s != 'ALL' : continue
                for name, cut_off in { "current": max_date,
                                       # "1_week": max_date-datetime.timedelta(7), "2_week": max_date-datetime.timedelta(14),
                                       # "1_month": max_date-datetime.timedelta(30), "2_month": max_date-datetime.timedelta(63), "Current_year": pd.Timestamp("2017-01-01")
            } .items():
                    df['ds']=df['date_key']
                    df['y']=df[y]
                    #predict(df,periods=None,name='none', maxy=None, clear=False, ylabel='CPU-TPS', ignore_after_infit=None,freq='d'):                    
                    pool.apply_async(predict, args=(df[(df.Region==r)&(df.SVC_NM==s)&(df.hour==hour)], None,'%s_%s_%s_%s_%i_%s' %(s,r,name,y,hour,str(cut_off.date())), None, False, 'CPU_TPS', cut_off))
                    # print('********************************************************************************')
                    # print (df[['hour', 'Region', 'SVC_NM', 'date_key']].tail())
                    #result=predict(df[(df.SVC_NM==s) & (df.hour==hour) & (df.Region==r)], name='%s_%s_%s_%s_%i' %(s,r,name,y,hour), ignore_after_infit=cut_off)
                    # print('================================================================================')
                    # print (df[['hour', 'Region', 'SVC_NM', 'date_key']].tail())
                    # break
                    # print(len(df[(df.SVC_NM==s) & (df.hour==hour) & (df.Region==r)]), len(result))
                    # print(result.head(), result.tail(), df_slim.head(), df_slim.tail(),df2.tail())
                    # print(len(df[(df.SVC_NM==s) & (df.hour==hour) & (df.Region==r) & (df.date_key>=cut_off)]), len(result), len(result[result.ds>=cut_off]))
                    # print(df[(df.SVC_NM==s) & (df.hour==hour) & (df.Region==r) & (df.date_key>=cut_off)].date_key.head())
                    # print(df[(df.SVC_NM==s) & (df.hour==hour) & (df.Region==r) & (df.date_key>=cut_off)].date_key.tail())
                    # print(result[result.ds>=cut_off].head())
                    # print(result[result.ds>=cut_off].tail())
                    #df.loc[(df.SVC_NM==s) & (df.hour==hour) & (df.Region==r) & (df.date_key>=cut_off), y]=result[result.ds>=cut_off].yhat.tolist()                    
                    continue
                continue
            continue
        continue
    continue

pool.close()
pool.join()
m.shutdown()

#df.dropna(axis=1).sort_values(['date_key','SVC_NM','Region']).to_csv('out_forecast.csv')


