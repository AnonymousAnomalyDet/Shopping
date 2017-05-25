#!/usr/bin/env python

import df_utils
import pandas as pd
import glob
import sys, os

#df_user=pd.read_csv('../4-30-17-hourly-user_id2.export.csv')#, parse_dates=['DATETOHOUR'])
df_user=None
dirname=None
if len(sys.argv)>1:
    files=[]
    for arg in sys.argv[1:]:
        if os.path.isfile(arg): files.append(arg)
        elif os.path.isdir(arg) : 
            files+=glob.glob(arg+'/*export.csv')
            if dirname is None: dirname=arg
            else : dirname=[dirname]+[arg]
        continue
    for f in files:
        if df_user is None:df_user=pd.read_csv(f)
        else: df_user=df_user.append(pd.read_csv(f))
        continue            
    pass
else:
    df_user=pd.read_csv('../02-16-pcc-export.csv')
    pass

pcc_map=pd.read_csv('~/workarea/PCC Mapping May 4 2017.csv')
df_scans=df_utils.read_csv('~/workarea/Downloads/2016_Scans_Jan_June.csv', encoding='latin1')
df_scans=df_scans.append(df_utils.read_csv('~/workarea/Downloads/2016_Scans_July_Dec.csv'))
df_scans=df_scans.append(df_utils.read_csv('~/workarea/Downloads/2017_Scans_Jan_March.csv'))

df_bookings=df_utils.read_csv('~/workarea/Downloads/2016_Bookings_Jan_Jun.csv')
df_bookings=df_bookings.append(df_utils.read_csv('~/workarea/Downloads/2016_Bookings_July_Dec.csv'))
df_bookings=df_bookings.append(df_utils.read_csv('~/workarea/Downloads/2017_Bookings_Jan_Mar.csv'))

print('data loaded')
df_user.dropna(axis=0,inplace=True)
import datetime
df_user['hour']=df_user.GEN_HOUR.map(lambda x: datetime.timedelta(0, 3600*x))
d={ 'JAN':1, 'FEB':2, "MAR":3, "APR":4, 'MAY':5, 'JUN':6, 'JUL':7, 'AUG':8, 'SEP':9, 'OCT':10, 'NOV':11, 'DEC':12}
df_user.DATETOHOUR=df_user.DATETOHOUR.map( lambda x: x.replace(x[3:6], str(d[x[3:6]])))
df_user.DATETOHOUR=pd.to_datetime(df_user.DATETOHOUR, format='%d-%m-%y')     
df_user['date_key']=df_user.DATETOHOUR+df_user.hour
df_user['YearMonth']=df_user.date_key.map(lambda x: int(str('%i%02i' %(x.year,x.month))))

def getgroup(pcc, pcc_dict, pcc_unk, label, ym=None):
    if pcc in pcc_unk or pcc not in pcc_dict: return 'unk'
    pcc_info=pcc_dict[pcc]
        
    if label in pcc_info:
        #print('label', label, ym)
        if ym is None: return pcc_info[label]
        elif ym in pcc_info[label]: return pcc_info[label][ym]
    else: return 0
    pass

def groupby(df, pcc_dict, pcc_unk, level='PNL', sums=['CPU_TPS', 'NSOLREQ', 'NSOLPRD', 'CNT', 'ELAPSED'], keep=[], level_dict=None):
    print('grouping by:', level)
    df=df.copy()
    df[level]=df.PCC.map( lambda x: getgroup(x,pcc_dict,pcc_unk,level) )
    df['Channel']=df.PCC.map( lambda x: getgroup(x,pcc_dict,pcc_unk,'Channel'))
    df.Channel=df.Channel.map( lambda x: int(x) if x!='unk' else 0)
    for k_level in keep:
        df[k_level]=df.PCC.map( lambda x: getgroup(x,pcc_dict,pcc_unk,k_level) )
        continue

    for s in sums:
        df.loc[df.eval('SVC_NM!="POOP"'), level+'_'+s]=df.groupby(['date_key', 'SVC_NM', level])[s].transform('sum')
        continue
    df=df.drop_duplicates(subset=['date_key', 'SVC_NM', level]).copy()
    rename={}
    for s in sums:
        rename[level+'_'+s]=s
        continue
    df=df.drop(sums, axis=1)
    df.rename(index=str, columns=rename, inplace=True)
    
    df['bookings']=0
    df['basic']=0
    df['fare']=0
    df['search']=0
    df['bookings_ota_mix']=0
    df['basic_ota_mix']=0
    df['fare_ota_mix']=0
    df['search_ota_mix']=0
    for ym in df.YearMonth.unique():
        df.loc[df.YearMonth==ym,'bookings']=df[df.YearMonth==ym][level].map(lambda x: getgroup(x, level_dict, pcc_unk, 'bookings',ym))
        df.loc[df.YearMonth==ym,'basic']=df[df.YearMonth==ym][level].map(lambda x: getgroup(x, level_dict, pcc_unk, 'basic',ym))
        df.loc[df.YearMonth==ym,'fare']=df[df.YearMonth==ym][level].map(lambda x: getgroup(x, level_dict, pcc_unk, 'fare',ym))
        df.loc[df.YearMonth==ym,'search']=df[df.YearMonth==ym][level].map(lambda x: getgroup(x, level_dict, pcc_unk, 'search',ym))
        df.loc[df.YearMonth==ym,'bookings_ota_mix']=df[df.YearMonth==ym][level].map(lambda x: getgroup(x, level_dict, pcc_unk, 'bookings_ota_mix_value',ym))
        df.loc[df.YearMonth==ym,'basic_ota_mix']=df[df.YearMonth==ym][level].map(lambda x: getgroup(x, level_dict, pcc_unk, 'basic_ota_mix_value',ym))
        df.loc[df.YearMonth==ym,'fare_ota_mix']=df[df.YearMonth==ym][level].map(lambda x: getgroup(x, level_dict, pcc_unk, 'fare_ota_mix_value',ym))
        df.loc[df.YearMonth==ym,'search_ota_mix']=df[df.YearMonth==ym][level].map(lambda x: getgroup(x, level_dict, pcc_unk, 'search_ota_mix_value',ym))
        continue

    prepend=''
    if dirname is not None and type(dirname)==str: prepend=dirname+'/'
    df_utils.to_csv(df.drop(['PCC','DATETOHOUR'],axis=1).sort_values(['date_key','SVC_NM',level]), prepend+'Level_%s.csv' %level)
    pass

def add_ota_mix(pcc, t=['bookings'], sc=None, pnl=None, region=None, country=None,ym=0):
    for l1,l2 in zip([pcc['SC_Code'], pcc['PNL'], pcc['Region'], pcc['Country']],[sc,pnl,region,country]):
        if l1 not in l2: l2[l1]={}
        for booking_type in t:
            if booking_type+'_ota_mix' not in l2[l1]:
                l2[l1][booking_type+'_ota_mix']={}
                l2[l1][booking_type+'_ota_mix_value']={}
                pass
            if booking_type not in l2[l1]: l2[l1][booking_type]={}
            if ym not in l2[l1][booking_type+'_ota_mix']:
                l2[l1][booking_type+'_ota_mix'][ym]=[0,0]
                l2[l1][booking_type+'_ota_mix_value'][ym]=0
                pass
            if ym not in l2[l1][booking_type]: l2[l1][booking_type][ym]=0                
            #for fucks sake
            #dict by sc_code by booking type by month by [sum(bookings*channel), N bookings]
            #ota_mix per sc_code per month is then sum(bookings*channel)/N
            l2[l1][booking_type+'_ota_mix'][ym][0]+=pcc[booking_type][ym]*pcc['Channel']
            l2[l1][booking_type+'_ota_mix'][ym][1]+=pcc[booking_type][ym]
            if l2[l1][booking_type+'_ota_mix'][ym][1] > 0 :l2[l1][booking_type+'_ota_mix_value'][ym]=l2[l1][booking_type+'_ota_mix'][ym][0]/l2[l1][booking_type+'_ota_mix'][ym][1]
            l2[l1][booking_type][ym]+=pcc[booking_type][ym]
            continue
        continue
    pass


def make_pcc_dict(pcc_map, pcc_users, df_bookings, df_scans):
    import df_utils
    df_utils.robust_column_names(pcc_map)
    pcc_dict={}
    sc_code_dict={}
    pnl_dict={}
    region_dict={}
    country_dict={}
    for r in pcc_map.itertuples():
        if r.PCC not in pcc_users: continue
        pcc_dict[r.PCC]={'SC_Code': r.SC_Code,
                         'PNL': r.PNL,
                         'Region': r.Region,
                         'Country': r.Country,}        
        continue

    import locale
    locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' )
    for r in df_bookings.itertuples():
        if r.PCC not in pcc_users: continue
        pcc=None
        if r.PCC in pcc_dict: pcc=pcc_dict[r.PCC]
        else : pcc={ 'SC_Code' : r.SC_Code,
                     'PNL' : r.PNL,
                     'Region' : r.Region,
                     'Country' : r.Country, }
        if 'bookings' not in pcc: pcc['bookings']={}
        if r.Channel=='OTA': pcc['Channel']=1
        else : pcc['Channel']=0
        ym=int('%i%02i' %(pd.Timestamp(r.Date).year,pd.Timestamp(r.Date).month))
        pcc['bookings'][ym]=locale.atoi(r.Bookings)
        add_ota_mix(pcc, ['bookings'], sc=sc_code_dict, pnl=pnl_dict, region=region_dict, country=country_dict,ym=ym)
        continue

    for r in df_scans.itertuples():
        if r.PCC not in pcc_users: continue
        pcc=None
        if r.PCC in pcc_dict: pcc=pcc_dict[r.PCC]
        else : pcc={ 'SC_Code' : r.SC_Code,
                     'PNL' : r.PNL,
                     'Region' : r.Region,
                     'Country' : r.Country, }
        if 'scans' not in pcc: pcc['scans']={}
        if r.Channel=='OTA': pcc['Channel']=1
        else : pcc['Channel']=0
        ym=int('%i%02i' %(pd.Timestamp(r.Date).year,pd.Timestamp(r.Date).month))
        for t in ['basic','fare','search']:
            if t not in pcc: pcc[t]={}
            continue        
        pcc['basic'][ym]=locale.atoi(r.Basic)
        pcc['fare'][ym]=locale.atoi(r.Fare)
        pcc['search'][ym]=locale.atoi(r.Search)
        add_ota_mix(pcc, ['basic','fare','search'], sc=sc_code_dict, pnl=pnl_dict, region=region_dict, country=country_dict,ym=ym)
        continue

    return pcc_dict,sc_code_dict,pnl_dict,region_dict,country_dict

pcc_users=set(df_user.PCC.unique().tolist())
pcc_all=set(pcc_map.PCC.unique().tolist())
pcc_bookings=set(df_bookings.PCC.unique().tolist())
pcc_scans=set(df_scans.PCC.unique().tolist())

pcc_intersection=pcc_all.intersection(pcc_users)
pcc_unk=pcc_users.difference(pcc_intersection)

#print( 'bookings, scan: ', len(pcc_bookings), len(pcc_scans), len(pcc_bookings.intersection(pcc_scans)), len(pcc_bookings.intersection(pcc_all)))

# for pcc in pcc_unk: print(pcc)
# import sys
# sys.exit(0)


df_user.loc[df_user.eval('SVC_NM!="ISELL"'), 'CPU_TPS']=df_user[df_user.eval('SVC_NM!="ISELL"')].groupby(['date_key','SVC_NM','PCC']).CPU.transform('sum')
df_user.loc[df_user.eval('SVC_NM=="ISELL"'), 'CPU_TPS']=df_user[df_user.eval('SVC_NM=="ISELL"')].groupby(['date_key','SVC_NM','PCC']).CNT.transform('sum')/3553.8

df_user.loc[df_user.eval('SVC_NM!="POOP"'), 'UNK_PCC']=df_user.PCC.map(lambda x: 1 if x in pcc_unk else 0)

pcc_dict,sc_code_dict,pnl_dict,region_dict,country_dict=make_pcc_dict(pcc_map, pcc_users, df_bookings, df_scans)
#print(region_dict)
#print(sc_code_dict['K7P4'])
#print (pcc_dict)
print ('pcc dict created')
groupby(df_user, pcc_dict, pcc_unk, level='PNL',level_dict=pnl_dict)
groupby(df_user, pcc_dict, pcc_unk, level='SC_Code',level_dict=sc_code_dict)
groupby(df_user, pcc_dict, pcc_unk, level='Region',level_dict=region_dict)
groupby(df_user, pcc_dict, pcc_unk, level='Country',level_dict=country_dict)
