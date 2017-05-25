#!/usr/bin/env python

import PlottingUtils
plt=PlottingUtils.Plt(True)

import os, sys
import df_utils

df=df_utils.read_csv(sys.argv[1:])

print (len(df))

df['day']=df.date_key.map(lambda x: x.day)
df['hour']=df.date_key.map(lambda x: x.hour)
df.bookings=df.bookings.map(lambda x: int(x) if (x!='unk' and x==x) else 0)
df['Month']=df.date_key.map(lambda x: x.month)

df.loc[df.CPU_TPS==df.CPU_TPS, 'MeanMontlyCPU']=df.groupby(['Month','Region','SVC_NM']).CPU_TPS.transform('mean')

var='bookings'
for region in df.Region.unique():
    dfm=df[(df.hour==0) & (df.day==1) & (df.Region==region) & (df.SVC_NM=="ATSEIMIP") & (df[var]==df[var])]
    plt.plot(dfm.date_key, dfm.CPU_TPS/dfm[var], label=region)
    continue

plt.legend()
plt.savefig('bookings_by_region.png')
