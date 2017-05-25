#!/usr/bin/env python

import df_utils, my_smoothing
import PlottingUtils
plt=PlottingUtils.Plt(True)
#import pylab as plt
import pandas as pd
import sys

infile=None
df=None
if len(sys.argv)==2:
    infile=sys.argv[1]
    df=df_utils.read_csv(infile)
    pass
elif len(sys.argv)>2:
    for arg in sys.argv[1:]:
        df=df_utils.read_csv(arg, append_to=df)
        continue
    pass
else: sys.exit(0)

df
df=df.sort_values(['date_key','SVC_NM','Region'])

df=df_utils.make_columns_with_simple_lambda(df, {'month': 'month',
                                                 'weekday': pd.Timestamp.weekday,
                                                 #'weekday': weekday,
                                                 'year': 'year',
                                                 'week': 'week',
                                                 'hour': 'hour',
                                                 'dayofmonth' : 'day' },
                                            'date_key')

svcs=df.SVC_NM.unique().tolist()
regions=df.Region.unique().tolist()

df['DayAsInt']=df.date_key.map(lambda x: int('%i%02i%02i' %(x.year,x.month,x.day)))
df['Hour']=df.GEN_HOUR
df.loc[df.SVC_NM!="", "DailyPeak"]=df.groupby(['DayAsInt','SVC_NM','Region']).CPU_TPS.transform('max')
df.DailyPeakHour=-1
df.loc[df.CPU_TPS==df.DailyPeak, "DailyPeakHour"]=df[df.CPU_TPS==df.DailyPeak].groupby(['DayAsInt','SVC_NM','Region']).Hour.transform('max')
df.loc[df.CPU_TPS>=-111, "DailyPeakHour"]=df.groupby(['DayAsInt','SVC_NM','Region']).DailyPeakHour.transform('max')
df.loc[df.CPU_TPS>=-111, 'CPUMean']=df.groupby(['DayAsInt', "SVC_NM", "Region"]).CPU_TPS.transform('mean')

selections={
    "all": "hour>-1",
    "peak": "DailyPeakHour==hour",
    #"Weekday": 'weekday>=5',
    #"PeakWeekday": 'PeakHour==hour and weekday!="Sun" and weekday!="Sat"'
}

g_window_size=35
g_order=3
for svc in svcs:
    for region in regions:
        df_svc=df[(df.SVC_NM==svc) & (df.Region==region)]
        for name,sel in selections.items():
            for hour in range(-1,24,1):
                window_size=g_window_size
                order=g_order
                sel_hour=sel
                if hour>=0 and sel.count('PeakHour'): continue
                elif hour>=0: sel_hour=sel_hour+' and hour==%i' %hour

                df2=df_utils.select(df_svc, sel_hour)

                if not sel.count('PeakHour') and hour<0: window_size*=24
                if window_size%2==0: window_size+=1
                if window_size<order+2: continue

                y=df2.CPU_TPS
                ybump=df2.CPU_TPS*df2.DailyPeak/df2.CPUMean*1.1
                try:
                    y2=my_smoothing.savitzky_golay(y, window_size, order)
                    y2bump=my_smoothing.savitzky_golay(ybump, window_size, order)
                except:
                    print( 'smoothing failed for ', name, window_size, order, len(y))
                    continue
                
                if len(y) != len(y2):
                    print('inconsistent size: ', name, hour, window_size, order, len(y), len(df2), len(y2))
                    continue
                
                if hour==-1 and not sel.count('PeakHour'): y2=y2bump
                elif sel.count('PeakHour'): y2=y2*1.15
                
                smooth_name='CPU_TPS_smooth_'+name
                if hour>=0: smooth_name+='_hourly'
                eval_str=('SVC_NM=="%s" and Region=="%s" and ' %(svc,region)) + sel_hour
                #print(len(df[df.eval(eval_str)]), len(y2))
                if smooth_name not in df.columns.unique().tolist(): df[smooth_name]=0            
                df.loc[ df.eval(eval_str), smooth_name ]=y2                    
                continue
            continue
        continue
    continue


if infile is not None: df.to_csv(infile.replace('.csv','_smooth.csv'),index=False)
else : df_utils.to_csv(df, 'out_smooth.csv')


for svc in svcs:
    for region in regions:
        df_SVC=df[(df.SVC_NM==svc) & (df.Region==region)]
        plt.clf()
        plt.plot(df_SVC.date_key, df_SVC.CPU_TPS, label='Actuals')
        plt.plot(df_SVC.date_key, df_SVC.CPU_TPS_smooth_all_hourly*1.2, label='Smooth hourly', linestyle='--')
        plt.plot(df_SVC.date_key, df_SVC.CPU_TPS_smooth_all, label='Smooth total')
        plt.plot(df_SVC[df_SVC.DailyPeakHour==df_SVC.hour].date_key, df_SVC[df_SVC.DailyPeakHour==df_SVC.hour].CPU_TPS, label='Actual Peak')
        plt.plot(df_SVC[df_SVC.DailyPeakHour==df_SVC.hour].date_key, df_SVC[df_SVC.DailyPeakHour==df_SVC.hour].CPU_TPS_smooth_peak, label='Smooth Peak')
        plt.legend()
        plt.savefig('smooth_%s_%s.png' %(svc, region))
 
