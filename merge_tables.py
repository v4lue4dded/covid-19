import pandas as pd
import numpy as np
import sys as sys
import os as os
import datetime as dt

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 50)

data_path = os.getcwd() + '\\COVID-19\\csse_covid_19_data\\'
print(data_path)

df_lu = pd.read_csv(data_path+ 'UID_ISO_FIPS_LookUp_Table.csv')
df_co = pd.read_csv(data_path+ 'csse_covid_19_time_series//time_series_covid19_confirmed_global.csv')
df_re = pd.read_csv(data_path+ 'csse_covid_19_time_series//time_series_covid19_recovered_global.csv')
df_de = pd.read_csv(data_path+ 'csse_covid_19_time_series//time_series_covid19_deaths_global.csv')

df_europe  = pd.read_csv(os.getcwd() + '//european_countries.tsv', sep='\t')

df_owid = pd.read_csv(os.getcwd() + '\\covid-19-data\\public\\data\\owid-covid-data.csv')

df_lu.columns
df_co.columns
df_re.columns
df_de.columns

country_regions_to_group = set([
     "Canada"         #because of Problems within John Hopkins Dataset
    ,"China"          #because of Problems merging with owid
    ,"Australia"      #because of Problems merging with owid   
    ,"United Kingdom" #because of Problems merging with owid   
    ])

join_columns = ['country_region', 'province_state', 'date']

df_co['Country/Region'].isin(country_regions_to_group)
id_vars = ['Province/State', 'Country/Region', 'Lat', 'Long']

def clean_df(df, id_vars):
    value_vars = set(df.columns) - set(id_vars)
    return pd.melt(
        df
        , id_vars=id_vars
        , value_vars=value_vars
        , var_name='date'
        , value_name='number'
    ).assign(
        date              = lambda x: pd.to_datetime(x.date,format = '%m/%d/%y')
        , province_state  = lambda x: np.where(x['Country/Region'].isin(country_regions_to_group), '', x['Province/State'].fillna(''))
        , country_region  = lambda x: x['Country/Region']
    )[join_columns + ['number']].groupby(join_columns).sum().reset_index()

df_co_clean = clean_df(df_co, id_vars).assign(confirmed = lambda x: x.number).drop(columns = 'number')
df_re_clean = clean_df(df_re, id_vars).assign(recovered = lambda x: x.number).drop(columns = 'number')
df_de_clean = clean_df(df_de, id_vars).assign(deaths    = lambda x: x.number).drop(columns = 'number')

assert df_co_clean[join_columns].equals(df_re_clean[join_columns]),  "df_co_clean[join_columns] != df_re_clean[join_columns]"
assert df_re_clean[join_columns].equals(df_de_clean[join_columns]),  "df_re_clean[join_columns] != df_de_clean[join_columns]"


df_country_map_names = pd.DataFrame(
{ 'country_region'    : ['US'                      ,'Congo (Brazzaville)', 'Congo (Kinshasa)','Burma'  ,"Cote d'Ivoire", 'South Sudan', 'Central African Republic','Korea, South']
, 'country_region_map': ['United States of America','Congo'              , 'Dem. Rep. Congo' ,'Myanmar',"Côte d'Ivoire", 'S. Sudan'   , 'Central African Rep.'    ,'South Korea' ]  
})

df_lu_rename = df_lu.assign(
      province_state  = lambda x: x['Province_State'].fillna('')
    , country_region  = lambda x: x['Country_Region']
    , lu_id           = lambda x: range(x.shape[0])
).drop(columns = ['Province_State', 'Country_Region']).merge(
    df_country_map_names, how = 'outer', on = ['country_region']
).merge(
    df_europe, how = 'outer', on = ['iso2']
).assign(
      country_region_map = lambda x: x.country_region_map.fillna(x.country_region)
    , country_group = lambda x: x.country_group.fillna('')
)

unique_crps = df_co_clean[['country_region', 'province_state']].drop_duplicates().reset_index()

# check that all country_regions are in lookup table
assert df_lu_rename.merge(unique_crps, how = "inner").shape[0] == unique_crps.shape[0]
df_lu_clean = df_lu_rename.merge(unique_crps, how = "inner")

# only two rows that don't have iso3
assert df_lu_clean[df_lu_clean.iso3.isna()].shape[0] == 2

df_te = df_owid.merge(
      df_lu_clean[df_lu_clean.iso3.notna()]
    , how = 'outer'
    , left_on='iso_code'
    , right_on='iso3'
    , validate="m:1"    
).assign(
   ones = 1
,  iso_code        = lambda x: x.iso_code.fillna("missing")
,  iso3            = lambda x: x.iso3.fillna("missing")
,  location        = lambda x: x.location.fillna("missing")
,  country_region  = lambda x: x.country_region.fillna("missing")
)

# #TODO: Fix this to make it empty
# assert df_te.groupby([
#   "iso_code"      
# , "iso3"          
# , "location"      
# , "country_region"]).sum().reset_index().query(
# "iso_code == 'missing' or iso3 == 'missing' or location == 'missing' or country_region == 'missing'"
# ).shape[0] == 0

df_te_clean = df_te.assign(
      date   = lambda x: pd.to_datetime(x.date,format = '%Y-%m-%d')
    , tested = lambda x: x.total_tests
)[join_columns + ['tested']].groupby(join_columns).sum().reset_index()

df_data_clean = df_co_clean.merge(
    df_re_clean, how = 'inner', validate = "1:1", on = join_columns).merge(
    df_de_clean, how = 'inner', validate = "1:1", on = join_columns).merge(
    df_te_clean, how = 'inner', validate = "1:1", on = join_columns).merge(
    df_lu_clean, how = 'inner', validate = "m:1", on = ['country_region', 'province_state']).assign(
          confirmed = lambda x: x['confirmed'].fillna(0)
        , recovered = lambda x: x['recovered'].fillna(0)
        , deaths    = lambda x: x['deaths'   ].fillna(0)
        , tested    = lambda x: x['tested'   ].fillna(0)
        , active    = lambda x: (x.confirmed - x.deaths - x.recovered).fillna(0)
    )[
        ["lu_id", 'country_region', 'province_state', 'date', 'confirmed', 'recovered', 'deaths', 'tested', 'active']
    ].query("date.notnull()", engine = "python").assign(
        lu_id = lambda x: x.lu_id.fillna(-1)
    ,   lag_1_confirmed = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['confirmed'].shift(1).fillna(0)
    ,   lag_1_recovered = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['recovered'].shift(1).fillna(0)
    ,   lag_1_deaths    = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['deaths'   ].shift(1).fillna(0)
    ,   lag_1_tested    = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['tested'   ].shift(1).fillna(0)
    ,   lag_1_active    = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['active'   ].shift(1).fillna(0)
    ,   lag_7_confirmed = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['confirmed'].shift(7).fillna(0)
    ,   lag_7_recovered = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['recovered'].shift(7).fillna(0)
    ,   lag_7_deaths    = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['deaths'   ].shift(7).fillna(0)
    ,   lag_7_tested    = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['tested'   ].shift(7).fillna(0)
    ,   lag_7_active    = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['active'   ].shift(7).fillna(0)
    ).query('''confirmed > 0 or recovered > 0 or deaths > 0 or tested > 0 or active> 0'''
    )

max_date = max(df_data_clean.date)

df_lu_clean.to_csv("df_lu_clean.tsv", index = False, sep = '\t', encoding='utf-8-sig')
df_data_clean.to_csv("df_data_clean.tsv", index = False, sep = '\t' ,encoding='utf-8-sig')
df_data_clean.query("date == @max_date").to_csv("df_data_clean_max_date.tsv", index = False, sep = '\t' ,encoding='utf-8-sig')

def power_bi_type_cast(df):
    type_string = '= Table.TransformColumnTypes(#"Promoted Headers",\n{   \n'
    first = True
    
    
    max_len_c_name = len(max(df.columns, key=len))
    
    for i_c in df.dtypes.iteritems():
        c_name = i_c[0]
        c_type = i_c[1]        
        
        if first:
            type_string += ' ' 
            first = False
        else:
            type_string += ','
            
        type_string += '{"'+c_name+'" '       
        type_string +=' '*(max_len_c_name-len(c_name)) # Ensures that all types start at the same point making it easiert to read
        type_string +=', '
        
        # python type to Power_BI type
        if c_type in ['object','bool']:
            type_string += 'type text'
        elif c_type in ['int64', 'int32']:
            type_string += 'Int64.Type'
        elif c_type in ['float64']:
            type_string += 'type number'
        elif c_type in ['<M8[ns]']:
            type_string += 'type date'
        else:
            type_string += 'ERROR'
        type_string += '}\n'        
    type_string += '})\n'
    return type_string


print(power_bi_type_cast(df_lu_clean))
print(power_bi_type_cast(df_data_clean))

df_te_clean_estimated = df_te_clean.assign(
       tested_or_nan                        = lambda x: x.tested.replace(0, np.nan)
     , tested_or_nan_log                    = lambda x: np.log(x.tested.replace(0, np.nan))
     , tested_interpolated_geo              = lambda x: np.exp(x.groupby(['country_region','province_state']).apply(lambda group: group.interpolate(method='index', limit_direction='both', limit_area='inside'))["tested_or_nan_log"])
     , change_tested_interpolated_geo       = lambda x: x[['country_region','province_state','tested_interpolated_geo']].groupby(['country_region','province_state']).pct_change()['tested_interpolated_geo']
)

import matplotlib.pyplot as plt
from scipy import interpolate

df_collect_temps = pd.DataFrame()

for lu in set(df_data_clean.lu_id):
    df_temp = df_data_clean.loc[df_data_clean.lu_id == lu]
    df_temp = df_temp.assign(
         tested_or_nan = lambda x: x.tested.replace(0, np.nan)
       , counter       = lambda x: range(len(x))
    )
    contains_tested = df_temp.tested_or_nan.notna().astype(int)
    if sum(contains_tested) >= 2 :
        x = df_temp[df_temp.tested_or_nan.notna()].counter
        y = df_temp[df_temp.tested_or_nan.notna()].tested_or_nan
        f = interpolate.interp1d(x, y, fill_value='extrapolate')
        df_temp = df_temp.assign(
            tested_estimated = lambda x: f(x.counter)
        )
        df_collect_temps = df_collect_temps.append(df_temp)

df_data_clean = df_data_clean.merge(df_collect_temps[['lu_id', 'date', 'tested_estimated']], how = 'left', on = ['lu_id', 'date'])

ger_df = df_data_clean.query("country_region == 'Germany'").reset_index()






# ger_df['tested_estimated'] -ger_df['tested_estimated'].shift(1)

# df_te_clean_estimated.to_csv("df_te_clean_interpolated.tsv", index = False, sep = '\t', encoding='utf-8-sig')

# daily growth active prev day            = COALESCE(DIVIDE(SUM('data_at'[active]   ), SUM('data_at'[lag_1_active]   )),1)       - 1
# daily growth confirmed prev day         = COALESCE(DIVIDE(SUM('data_at'[confirmed]), SUM('data_at'[lag_1_confirmed])),1)       - 1
# daily growth deaths prev day            = COALESCE(DIVIDE(SUM('data_at'[deaths]   ), SUM('data_at'[lag_1_deaths]   )),1)       - 1
# daily growth recovered prev day         = COALESCE(DIVIDE(SUM('data_at'[recovered]), SUM('data_at'[lag_1_recovered])),1)       - 1
# daily growth active prev week           = COALESCE(DIVIDE(SUM('data_at'[active]   ), SUM('data_at'[lag_7_active]   )),1)^(1/7) - 1      
# daily growth confirmed prev week        = COALESCE(DIVIDE(SUM('data_at'[confirmed]), SUM('data_at'[lag_7_confirmed])),1)^(1/7) - 1      
# daily growth deaths prev week           = COALESCE(DIVIDE(SUM('data_at'[deaths]   ), SUM('data_at'[lag_7_deaths]   )),1)^(1/7) - 1      
# daily growth recovered prev week        = COALESCE(DIVIDE(SUM('data_at'[recovered]), SUM('data_at'[lag_7_recovered])),1)^(1/7) - 1      
# hist daily growth active prev day       = COALESCE(DIVIDE(SUM('data_ot'[active]   ), SUM('data_ot'[lag_1_active]   )),1)       - 1
# hist daily growth confirmed prev day    = COALESCE(DIVIDE(SUM('data_ot'[confirmed]), SUM('data_ot'[lag_1_confirmed])),1)       - 1
# hist daily growth deaths prev day       = COALESCE(DIVIDE(SUM('data_ot'[deaths]   ), SUM('data_ot'[lag_1_deaths]   )),1)       - 1
# hist daily growth recovered prev day    = COALESCE(DIVIDE(SUM('data_ot'[recovered]), SUM('data_ot'[lag_1_recovered])),1)       - 1
# hist daily growth active prev week      = COALESCE(DIVIDE(SUM('data_ot'[active]   ), SUM('data_ot'[lag_7_active]   )),1)^(1/7) - 1
# hist daily growth confirmed prev week   = COALESCE(DIVIDE(SUM('data_ot'[confirmed]), SUM('data_ot'[lag_7_confirmed])),1)^(1/7) - 1
# hist daily growth deaths prev week      = COALESCE(DIVIDE(SUM('data_ot'[deaths]   ), SUM('data_ot'[lag_7_deaths]   )),1)^(1/7) - 1
# hist daily growth recovered prev week   = COALESCE(DIVIDE(SUM('data_ot'[recovered]), SUM('data_ot'[lag_7_recovered])),1)^(1/7) - 1


# positive test rate total           = DIVIDE(SUM('data_at'[confirmed])                                , SUM('data_at'[tested])                               )                                
# positive test rate prev day        = DIVIDE(SUM('data_at'[confirmed])-SUM('data_at'[lag_1_confirmed]), SUM('data_at'[tested]) - SUM('data_at'[lag_1_tested]))
# positive test rate prev week       = DIVIDE(SUM('data_at'[confirmed])-SUM('data_at'[lag_7_confirmed]), SUM('data_at'[tested]) - SUM('data_at'[lag_7_tested]))
# hist positive test rate            = DIVIDE(SUM('data_ot'[confirmed])                                , SUM('data_ot'[tested])                               )                                
# hist positive test rate prev day   = DIVIDE(SUM('data_ot'[confirmed])-SUM('data_ot'[lag_1_confirmed]), SUM('data_ot'[tested]) - SUM('data_ot'[lag_1_tested]))
# hist positive test rate prev week  = DIVIDE(SUM('data_ot'[confirmed])-SUM('data_ot'[lag_7_confirmed]), SUM('data_ot'[tested]) - SUM('data_ot'[lag_7_tested]))
