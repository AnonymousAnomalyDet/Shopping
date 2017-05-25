#!/usr/bin/env python

import PlottingUtils 
plt=PlottingUtils.Plt(True)
import df_utils

Rimp='2017-05-15_ATSEIMIP_APAC_.csv'
Pyimp='out_ATSEIMIP_APAC_nn.csv'

for region in ['APAC', 'NAM', "EMEA", "LAC"]:
    print(region)
    dfR=df_utils.read_csv( Rimp.replace('APAC', region) )
    dfPy=df_utils.read_csv( Pyimp.replace('APAC', region) )

    fig,ax=plt.subplots()
    ax.plot(dfR.Date, dfR.peakCPU, label='R')
    ax.plot(dfPy.date_key, dfPy.CPU_TPS_current_Forecast, label="py")
    ax.legend()

    #    print(dfR.columns, dfPy.columns)


    fig.savefig('comp_'+region+'.png')
    plt.close(fig)
