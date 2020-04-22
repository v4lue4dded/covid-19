df_co  = pd.read_csv(data_path+ 'csse_covid_19_time_series//time_series_covid19_confirmed_global.csv')
df_re  = pd.read_csv(data_path+ 'csse_covid_19_time_series//time_series_covid19_recovered_global.csv')
df_de  = pd.read_csv(data_path+ 'csse_covid_19_time_series//time_series_covid19_deaths_global.csv')



df_lu.columns
df_co.columns
df_re.columns
df_de.columns

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
    
max(df_lu_clean[["iso3", "country_region", "province_state"]].drop_duplicates().iso3.value_counts())
max(df_lu_clean[["iso3", "country_region", "province_state"]].drop_duplicates().country_region.value_counts()) 

df_lu_clean.shape
df_co_clean.groupby(["country_region", "province_state"]).ngroups
df_co_clean.groupby(["date"]).ngroups
df_lu_clean.groupby(["iso3"]).ngroups


     , tested_or_nan_log                    = lambda x: np.log(x.tested.replace(0, np.nan))
     , tested_interpolated_geo              = lambda x: np.exp(x.groupby(['country_region','province_state']).apply(lambda group: group.interpolate(method='index', limit_direction='both', limit_area='inside'))["tested_or_nan_log"])
     , change_tested_interpolated_geo       = lambda x: x[['country_region','province_state','tested_interpolated_geo']].groupby(['country_region','province_state']).pct_change()['tested_interpolated_geo']


        df_data_clean.loc[df_data_clean.lu_id == lu].tested_estimated = df_data_clean.loc[df_data_clean.lu_id == lu].merge(df_temp, on=["lu_id","date"]).tested_estimated_y
