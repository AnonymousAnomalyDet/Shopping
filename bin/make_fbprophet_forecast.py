#!/usr/bin/env python


import df_utils
import pandas as pd

cut_off_date='2017-05-01'
cut_off=pd.Timestamp(cut_off_date)
ForecastMonth='April'#one month behind cut_off

df=df_utils.read_csv('out_total_combined_smooth.csv')
df=df[['date_key','SVC_NM','Region','CPU_TPS']]
df=df[df.date_key<cut_off]

import glob
for f in glob.glob('fbprophet_results/*'+cut_off_date+'.csv'):
    dfi=df_utils.read_csv(f,)[['ds','yhat']]
    dfi.ds=dfi.ds.map(lambda x: pd.Timestamp(x))
    dfi.rename_axis({'ds':'date_key','yhat':'CPU_TPS'},axis=1,inplace=True)
    dfi=dfi[dfi.date_key>=cut_off]
    f=f[f.rfind('/')+1:]
    svc=f[0:f.find('_')]
    region=f[len(svc)+1:f.find('_',len(svc)+1)]
    #print(region, svc)
    dfi['SVC_NM']=dfi.date_key.map(lambda x: svc)
    dfi['Region']=dfi.date_key.map(lambda x: region)
    df=df.append(dfi)
    #print(len(df))
    continue

df=df.sort_values(['date_key','SVC_NM','Region'])

df_utils.to_csv(df, 'fb_prediction.csv')

########################################
##Do peak analysis and plots
########################################

df=df_utils.read_csv('fb_prediction.csv')

df['hour']=df.date_key.map(lambda x: x.hour)

df2=df[df.Region=='ALL'].copy()
df2.Region='ALL2'
df2.CPU_TPS=-1
df=df.append(df2,ignore_index=True)
df=df.sort_values(['date_key','SVC_NM','Region'])

df['CPU_TPS_Region_Sum']=-1
df.loc[(df.Region!='ALL2')&(df.Region!='ALL'),'CPU_TPS_Region_Sum']=df[(df.Region!='ALL')&(df.Region!='ALL2')].groupby(['date_key','SVC_NM']).CPU_TPS.transform('sum')
df.loc[(df.Region!='POP'),'CPU_TPS_Region_Sum']=df.groupby(['date_key','SVC_NM']).CPU_TPS_Region_Sum.transform('max')
df.loc[(df.Region=='ALL2'),'CPU_TPS']=df[df.Region=='ALL2'].CPU_TPS_Region_Sum
df.drop(['CPU_TPS_Region_Sum'],axis=1,inplace=True)

df['date']=df.date_key.map(lambda x: pd.Timestamp(x.date()))
df['YearMonth']=df.date_key.map(lambda x: int('%i%02i' %(x.year,x.month)))

#print(df.date.head(),df.date_key.head())
df.loc[df['date']>pd.Timestamp('2007-01-01'),'peakCPU']=df.groupby(['date','SVC_NM','Region']).CPU_TPS.transform('max')
df['peakHour']=-1
df.loc[df.peakCPU==df.CPU_TPS, 'peakHour']=df[df.peakCPU==df.CPU_TPS].groupby(['date','SVC_NM','Region']).hour.transform('max')
df.loc[df.date>pd.Timestamp('2007-01-01'),'peakHour']=df.groupby(['date','SVC_NM','Region']).peakHour.transform('max')
df.loc[df.date>pd.Timestamp('2007-01-01'),'peakMonthlyCPU']=df.groupby(['YearMonth','SVC_NM','Region']).CPU_TPS.transform('max')

#print(df)

from PlottingUtils import Plotter

for svc in df.SVC_NM.unique():
    peakPlot=Plotter('plots/summaryPeakHour_'+svc+"_"+ForecastMonth+'.png')
    peakMonthPlot=Plotter('plots/summaryMontlyPeakHour_'+svc+"_"+ForecastMonth+'.png')
    for region in ['LAC','APAC','EMEA','NAM','ALL']:
        dfi=df[(df.SVC_NM==svc)&(df.Region==region)]
        peakPlot.plot(dfi[dfi.date_key<cut_off].date_key,dfi[dfi.date_key<cut_off].peakCPU,label=region)
        peakPlot.plot(dfi[dfi.date_key>cut_off].date_key,dfi[dfi.date_key>cut_off].peakCPU,color=region,linestyle='--')
        peakMonthPlot.plot(dfi[dfi.date_key<cut_off].date_key,dfi[dfi.date_key<cut_off].peakMonthlyCPU,label=region)
        peakMonthPlot.plot(dfi[dfi.date_key>cut_off].date_key,dfi[dfi.date_key>cut_off].peakMonthlyCPU,color=region,linestyle='--')
        if region=='ALL':
            dfi=df[(df.SVC_NM==svc)&(df.Region=='ALL2')]
            #plt.plot(dfi.date_key,dfi.peakCPU,label='ALL2')
            peakPlot.plot(dfi[dfi.date_key<cut_off].date_key,dfi[dfi.date_key<cut_off].peakCPU,label='ALL2')
            peakPlot.plot(dfi[dfi.date_key>cut_off].date_key,dfi[dfi.date_key>cut_off].peakCPU,color='ALL2',linestyle='--')
            peakMonthPlot.plot(dfi[dfi.date_key<cut_off].date_key,dfi[dfi.date_key<cut_off].peakMonthlyCPU,label='ALL2')
            peakMonthPlot.plot(dfi[dfi.date_key>cut_off].date_key,dfi[dfi.date_key>cut_off].peakMonthlyCPU,color='ALL2',linestyle='--')
            pass
        continue
    peakPlot.xLabelDateFormat(DateFormatter='%Y%m')
    peakPlot.ax.grid(True)
    peakPlot.savefig()
    peakMonthPlot.xLabelDateFormat(DateFormatter='%Y%m')
    peakMonthPlot.ax.grid(True)
    peakMonthPlot.savefig()
    continue

df_utils.to_csv(df, ForecastMonth+'_fb_prediction2.csv')
df=df[df.peakCPU==df.CPU_TPS]
df.loc[df.date_key>=cut_off,'Forecast']=df[df.date_key>=cut_off].CPU_TPS.tolist()
df.loc[df.date_key>=cut_off,'CPU_TPS']=0
df.drop(['date_key','YearMonth','hour'],inplace=True, axis=1)

df_utils.to_csv(df, ForecastMonth+'Forecast_v2_raw.csv')


df_fabio=None
for region in df.Region.unique():
    if region=='ALL': continue
    if region=='unk': continue
    region_name=region
    if region=='ALL2': region_name='Global'
    for svc in df.SVC_NM.unique():
        #print (region, svc)
        dfi=df[(df.Region==region)&(df.SVC_NM==svc)&(df.peakCPU==df.CPU_TPS)]
        if df_fabio is None:
            df_fabio=dfi.copy()
            cols=df.columns.tolist()
            cols.remove('date')
            df_fabio.drop(cols, axis=1,inplace=True)
            pass
        #print(len(df_fabio), len(dfi))
        df_fabio.loc[df_fabio.date<cut_off,'%s %s Actuals' %(region_name, svc)] = dfi[dfi.date<cut_off].peakCPU.tolist()
        df_fabio.loc[df_fabio.date>=cut_off,'%s %s Forecats' %(region_name, svc)] = dfi[dfi.date>=cut_off].peakCPU.tolist()
        df_fabio.loc[df_fabio.date>pd.Timestamp('2007-01-01'), '%s %s Peak Month' %(region_name, svc)] =dfi.peakMonthlyCPU.tolist()
        continue
    continue

df_utils.to_csv(df_fabio, ForecastMonth+'Forecast_v2.csv')

