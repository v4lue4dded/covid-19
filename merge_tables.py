import pandas as pd
import numpy as np
import sys as sys
import os as os
import datetime as dt

data_path = os.getcwd() + '\\COVID-19\\csse_covid_19_data\\'
print(data_path)

df_lu  = pd.read_csv(data_path+ 'UID_ISO_FIPS_LookUp_Table.csv')
df_co  = pd.read_csv(data_path+ 'csse_covid_19_time_series//time_series_covid19_confirmed_global.csv')
df_re  = pd.read_csv(data_path+ 'csse_covid_19_time_series//time_series_covid19_recovered_global.csv')
df_de  = pd.read_csv(data_path+ 'csse_covid_19_time_series//time_series_covid19_deaths_global.csv')

df_lu.columns
df_co.columns
df_re.columns
df_de.columns


df_country_map_names = pd.DataFrame(
{ 'country_region'    : ['US'                      ,'Congo (Brazzaville)', 'Congo (Kinshasa)','Burma'  ,"Cote d'Ivoire", 'South Sudan', 'Central African Republic','Korea, South']
, 'country_region_map': ['United States of America','Congo'              , 'Dem. Rep. Congo' ,'Myanmar',"Côte d'Ivoire", 'S. Sudan'   , 'Central African Rep.'    ,'South Korea' ]  
})

df_lu_clean = df_lu.assign(
      province_state  = lambda x: x['Province_State'].fillna('')
    , country_region  = lambda x: x['Country_Region']
    , lu_id           = lambda x: range(x.shape[0])
).drop(columns = ['Province_State', 'Country_Region']).merge(
    df_country_map_names, how = 'outer', on = ['country_region']
).assign(
    country_region_map = lambda x: x.country_region_map.fillna(x.country_region)
)

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
        , province_state  = lambda x: x['Province/State'].fillna('')
        , country_region  = lambda x: x['Country/Region']
    )[['country_region', 'province_state', 'date', 'number']]

df_co_clean = clean_df(df_co, id_vars).assign(confirmed = lambda x: x.number).drop(columns = 'number')
df_re_clean = clean_df(df_re, id_vars).assign(recovered = lambda x: x.number).drop(columns = 'number')
df_de_clean = clean_df(df_de, id_vars).assign(deaths    = lambda x: x.number).drop(columns = 'number')



df_data_clean = df_lu_clean.merge(
    df_co_clean, how = 'outer', on = ['country_region', 'province_state']).merge(
    df_re_clean, how = 'outer', on = ['country_region', 'province_state', 'date']).merge(
    df_de_clean, how = 'outer', on = ['country_region', 'province_state', 'date']).assign(
          confirmed = lambda x: x['confirmed'].fillna(0)
        , recovered = lambda x: x['recovered'].fillna(0)
        , deaths    = lambda x: x['deaths'   ].fillna(0)
        , active    = lambda x: (x.confirmed - x.deaths - x.recovered).fillna(0)
    )[
        ["lu_id", 'country_region', 'province_state', 'date', 'confirmed', 'active', 'deaths', 'recovered']
    ].query("date.notnull()", engine = "python").assign(
        lu_id = lambda x: x.lu_id.fillna(-1)
    ,   lag_1_confirmed = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['confirmed'].shift(1).fillna(0)
    ,   lag_1_recovered = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['recovered'].shift(1).fillna(0)
    ,   lag_1_deaths    = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['deaths'   ].shift(1).fillna(0)
    ,   lag_1_active    = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['active'   ].shift(1).fillna(0)
    ,   lag_7_confirmed = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['confirmed'].shift(7).fillna(0)
    ,   lag_7_recovered = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['recovered'].shift(7).fillna(0)
    ,   lag_7_deaths    = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['deaths'   ].shift(7).fillna(0)
    ,   lag_7_active    = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['active'   ].shift(7).fillna(0)
    ).query('''confirmed > 0 or recovered > 0 or deaths > 0 or active> 0'''
    )

df_lu_clean.to_csv("df_lu_clean.tsv", index = False, sep = '\t', encoding='utf-8-sig')
df_data_clean.to_csv("df_data_clean.tsv", index = False, sep = '\t' ,encoding='utf-8-sig')

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


# daily growth confirmed prev day  = (SUM('data_at'[confirmed]) / SUM('data_at'[lag_1_confirmed])) -1
# daily growth recovered prev day  = (SUM('data_at'[recovered]) / SUM('data_at'[lag_1_recovered])) -1
# daily growth deaths prev day     = (SUM('data_at'[deaths]   ) / SUM('data_at'[lag_1_deaths]   )) -1
# daily growth active prev day     = (SUM('data_at'[active]   ) / SUM('data_at'[lag_1_active]   )) -1
# daily growth confirmed prev week = DIVIDE(SUM('data_at'[confirmed]   ), SUM('data_at'[lag_7_confirmed]   ))^(1/7) -1
# daily growth recovered prev week = DIVIDE(SUM('data_at'[recovered]   ), SUM('data_at'[lag_7_recovered]   ))^(1/7) -1
# daily growth deaths prev week = DIVIDE(SUM('data_at'[deaths]   ), SUM('data_at'[lag_7_deaths]   ))^(1/7) -1
# daily growth active prev week = DIVIDE(SUM('data_at'[active]   ), SUM('data_at'[lag_7_active]   ))^(1/7) -1