#!/usr/bin/env python

import df_utils
import h2o
import pandas as pd

h2o.init(nthreads=8)

def weekday(d):
    d=pd.Timestamp.weekday(d)-5
    if d<1: d+=7
    return d

df_original=df_utils.read_csv('Level_region_all.csv')

if 'DailyPeakHour' not in df_original.columns.tolist():
    df_original['DateAsInt']=df_original.date_key.map(lambda x: int('%i%02i%02i' %(x.year, x.month, x.day)))
    df_original.loc[df_original.SVC_NM!='ppp', 'DailyPeak']=df_original.groupby(['DateAsInt','SVC_NM','Region']).CPU_TPS.transform('max')
    df_original['DailyPeakHour']=-1
    df_original.loc[df_original.CPU_TPS==df_original.DailyPeak,'DailyPeakHour']=df_original[df_original.CPU_TPS==df_original.DailyPeak].groupby(['DateAsInt','SVC_NM','Region']).GEN_HOUR.transform('max')
    df_original.loc[df_original.CPU_TPS>-111,'DailyPeakHour']=df_original.groupby(['DateAsInt','SVC_NM','Region']).DailyPeakHour.transform('max')
    pass

#for svc in df_original.SVC_NM.unique():
for svc in ['ISELL']:
    #for region in df_original.Region.unique():
    for region in ['LAC', 'NAM', 'unk']:
        df=df_original[(df_original.SVC_NM==svc) & (df_original.Region==region) & (df_original.GEN_HOUR==df_original.DailyPeakHour)]
        min_date=df.date_key.min()
        #max_training_date=pd.Timestamp(df.date_key.max(),freq='d')
        max_training_date=pd.Timestamp('2017-05-01',freq='d')
    
        df2=df_utils.make_future(begin=max_training_date+1,freq='d',end='2018-01-31', col_name='date_key')
        df2['CPU_TPS']=0
    
        df=df.append(df2,ignore_index=True)

        df['nn']=df.date_key.map(lambda x: (x-min_date).days * 24 + x.hour)
        df=df_utils.make_columns_with_simple_lambda(df, {'month': 'month',
                                                         #'weekday': pd.Timestamp.weekday,
                                                         'weekday': weekday,
                                                         'year': 'year',
                                                         'week': 'week',
                                                         'hour': 'hour',
                                                         'dayofmonth' : 'day' },
                                                    'date_key')

        print (df.head())

        #factor_columns= ['month', 'weekday', 'year', 'dayofmonth', 'week', 'hour']
        #factor_columns= ['month', 'weekday', 'year', 'dayofmonth', 'week',]
        factor_columns= ['month', 'weekday', 'dayofmonth', 'week',]
        numeric_columns= ['nn']

        import datetime
        date_cut_offs={"current": max_training_date,
                       "1_week": max_training_date-datetime.timedelta(7),
                       # "2_week": max_training_date-datetime.timedelta(14),
                       "1_month": max_training_date-datetime.timedelta(30),
                       # "Current_year": pd.Timestamp("2017-01-01"),
        } 

        for time_name, time_delta in date_cut_offs.items():
            nn=len(df[df.date_key<=time_delta])#before appending
    
            data=h2o.H2OFrame(df)
            for col in factor_columns : data[col]=data[col].asfactor()
        
            #print(data.head(), data.tail())
        
            N=len(data)
        
            train=data[0:nn,:]
            test=data[nn:N,:]
        
            print(nn, N, len(train), len(test), len(data))
            
            #for ycol in ['CPU_TPS', 'CPU_TPS_smooth_all', 'CPU_TPS_smooth_all_hourly']:
            for ycol in [
                #'CPU_TPS_smooth_peak', 
                'CPU_TPS']:            
                modelCPU=h2o.estimators.deeplearning.H2ODeepLearningEstimator(epochs=10000,nfolds = 5)
                modelCPU.train(x=factor_columns+numeric_columns,y=ycol,training_frame = train)
                print(modelCPU.varimp())
                predictions=modelCPU.predict(test)
                past_predictions=modelCPU.predict(train)
                test['Peak']=predictions
            
                df.loc[df.date_key<=time_delta,ycol+"_"+time_name+'_Forecast']=past_predictions.as_data_frame(use_pandas=True).predict.tolist()
                df.loc[df.date_key>time_delta,ycol+"_"+time_name+'_Forecast']=predictions.as_data_frame(use_pandas=True).predict.tolist()        
        
                continue
            continue
        df.to_csv('out_%s_%s_nn.csv' %(svc,region),index=False)
        continue
    continue
