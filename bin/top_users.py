#!/usr/bin/env python

import df_utils
import pandas as pd
import os, sys
import PlottingUtils

df=None

dirname=''
level='Region'

def skimmer(df, Nlevels=200, level=level):
    df.loc[df.eval('SVC_NM!="ABC"'), 'Overall_Peak_Hour']=df.groupby(['SVC_NM',level]).CPU_TPS.transform('max')
    df_trimmed=df[(df.SVC_NM=='ATSEIMIP')&(df.Overall_Peak_Hour==df.CPU_TPS)].sort_values(['Overall_Peak_Hour'], ascending=False)
    expression=''
    for l in df_trimmed[level].head(Nlevels).tolist():
        expression+='%s=="%s" or ' %(level, l)
        continue
    expression=expression[0:expression.rfind('or')]
    df_slim=df[df.eval(expression)]
    df_slim=df_slim.sort_values(['date_key','SVC_NM',level])
    return df_slim

def getLevel(arg):
    level_i=arg.replace('Level_','').replace('.csv','').replace('_fixed','').replace('_slim','')
    level_i=level_i[level_i.rfind('/')+1:]
    return level_i

if len(sys.argv)<2: sys.exit(1)
if len(sys.argv)==2:
    dirname=sys.argv[1]
    dirname=dirname[0:dirname.rfind('/')]
    if dirname.count('/'): dirname=dirname[dirname.rfind('/')+1:]
#df=df_utils.read_csv(sys.argv[1:], selection=(skimmer, 20, getLevel(sys.argv[1])))
df=df_utils.read_csv(sys.argv[1:])
df.loc[df.SVC_NM!="ABC","total_hourly_CPU_TPS"]=df.groupby(['SVC_NM','date_key']).CPU_TPS.transform('sum')
df["fraction_CPU_usage"]=df.CPU_TPS/df.total_hourly_CPU_TPS
df=skimmer(df,200,getLevel(sys.argv[1]))
df['hour']=df.date_key.map(lambda x:x.hour)
df['day']=df.date_key.map(lambda x:x.day)
df.loc[df.SVC_NM!='ABC', 'CPU_hourly_mean']=df.groupby(['hour','SVC_NM',level]).CPU_TPS.transform('mean')
df.loc[df.SVC_NM!='ABC', 'total_CPU_hourly_mean']=df.groupby(['hour','SVC_NM']).CPU_TPS.transform('mean')

df_utils.to_csv(df, 'top_200.csv')

print(df[level].unique(), len(df[level].unique()))

for svc in df.SVC_NM.unique():
    #print(svc)
    #print(df[(df.SVC_NM==svc)&(df.CPU_TPS==df.Overall_Peak_Hour)].sort_values(['Overall_Peak_Hour'],ascending=False)[['date_key',level,'CPU_TPS','bookings']].head(10))
    n=100
    if level=='Region': n=5
    do_legend=n<=10
    top_n=df[(df.SVC_NM==svc)&(df.CPU_TPS==df.Overall_Peak_Hour)].sort_values(['Overall_Peak_Hour'],ascending=False)[level].head(n).tolist()
    if level=='Region':top_n=['EMEA','NAM','APAC','LAC','unk']
    p=PlottingUtils.Plotter('top_users_%s' %svc)
    phour=PlottingUtils.StackPlotter('top_users_hour_%s' %svc)
    print(top_n)
    for sc_code in top_n:
        var='fraction_CPU_usage'
        p.ax.hist(df[(df.SVC_NM==svc)&(df[level]==sc_code)].sort_values([var],ascending=False)[var].head(100).tolist(),label=sc_code,stacked=False,histtype='step')
        d_hourly=df_utils.pairwise_dict(df, 'SVC_NM=="%s" and %s=="%s" and day==15' %(svc,level,sc_code), cols=['hour','CPU_hourly_mean'])
        average_hourly_cpu=[]
        for i in range(24):
            if i in d_hourly: average_hourly_cpu.append(d_hourly[i])
            else: average_hourly_cpu.append(0)
        phour.plot(range(24), average_hourly_cpu,label=sc_code)
        #if len(d_hourly)!=24: print(sc_code, '\n', d_hourly, '\n', average_hourly_cpu)
        continue
    p.savefig(legend=do_legend)
    average_hourly_cpu=df[(df.SVC_NM==svc)&(df[level]=='K7P4')&(df.day==15)].sort_values(['hour']).total_CPU_hourly_mean.tolist()
    if len(average_hourly_cpu)==24: phour.ax.plot(range(24), average_hourly_cpu,label='total',color='black')
    if svc=='ATSEIMIP' : phour.ax.set_ylim(0,1e14)
    phour.ax.set_title(dirname)
    phour.savefig(legend=do_legend)
        

    
