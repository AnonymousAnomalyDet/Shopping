#!/usr/bin/env python

import df_utils
import pandas as pd

dfs=[
    [df_utils.read_csv('fb_prediction2.csv'),pd.Timestamp('2017-05-01')],
    [df_utils.read_csv('March_fb_prediction2.csv'),pd.Timestamp('2017-04-01')],
    [df_utils.read_csv('February_fb_prediction2.csv'),pd.Timestamp('2017-03-01')],
    [df_utils.read_csv('February_fb_prediction2.csv'),pd.Timestamp('2017-03-01')],
]


for df in dfs:
    df[0]['Day']=df[0].date_key.map(lambda x: x.day)
    continue

from PlottingUtils import Plotter

for svc in dfs[0][0].SVC_NM.unique():
    peakPlot=Plotter('plots/summaryPeakHour_'+svc+"_comp"+'.png')
    peakMonthPlot=Plotter('plots/summaryMontlyPeakHour_'+svc+"_comp"+'.png')
    for region in ['LAC','APAC','EMEA','NAM','ALL2']:
        for counter,df_and_cut,ls in zip(range(3), dfs,['--','-.',':']):
            df=df_and_cut[0]
            cut=df_and_cut[1]
            dfi=df[(df.SVC_NM==svc)&(df.Region==region)]
            if counter==0:
                peakPlot.plot(dfi[dfi.date_key<cut].date_key,dfi[dfi.date_key<cut].peakCPU,label=region)
                peakMonthPlot.plot(dfi[(dfi.date_key<cut)&(dfi.Day==28)].date_key,dfi[(dfi.date_key<cut)&(dfi.Day==28)].peakMonthlyCPU,label=region)
                pass            
            peakPlot.plot(dfi[dfi.date_key>=cut].date_key,dfi[dfi.date_key>=cut].peakCPU,color=region,linestyle=ls)
            peakMonthPlot.plot(dfi[(dfi.date_key>=cut)&(dfi.Day==1)].date_key,dfi[(dfi.date_key>=cut)&(dfi.Day==1)].peakMonthlyCPU,color=region,linestyle=ls)                
            # if region=='ALL':
            #     dfi=df[(df.SVC_NM==svc)&(df.Region=='ALL2')]
            #     if counter==0:
            #         peakPlot.plot(dfi[dfi.date_key<cut].date_key,dfi[dfi.date_key<cut].peakCPU,label='ALL2')
            #         peakMonthPlot.plot(dfi[dfi.date_key>cut].date_key,dfi[dfi.date_key>cut].peakMonthlyCPU,label='ALL2')
            #         pass
            #     peakPlot.plot(dfi[dfi.date_key>=cut].date_key,dfi[dfi.date_key>=cut].peakCPU,color='ALL2',linestyle=ls)
            #     peakMonthPlot.plot(dfi[dfi.date_key>=cut].date_key,dfi[dfi.date_key>=cut].peakMonthlyCPU,color='ALL2',linestyle=ls)
            #     pass
            continue
        continue
    peakPlot.xLabelDateFormat(DateFormatter='%Y%m')
    peakPlot.ax.grid(True)
    peakPlot.savefig()
    peakMonthPlot.xLabelDateFormat(DateFormatter='%Y%m')
    peakMonthPlot.ax.grid(True)
    peakMonthPlot.add_legend({'April Forecast':'--','March Forecast':'-.', 'Feb Forecast':':'})    
    peakMonthPlot.savefig()
    continue

