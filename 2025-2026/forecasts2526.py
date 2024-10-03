import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from datetime import date


################################################################################
##################################Variables#####################################
################################################################################
#if true, remove 2020 data
covid_bool = True
#Make the NEL 1+ day forecasts use august data as startpoint rather than sept
#(THIS MAY NEED TO CHANGE ONCE NOVEMBER, AS OCTOBER MAY BE OK TO USE).
NEL_plus1_bool = True
#Define forecast period (april25-march26)
pred_period = pd.DataFrame(pd.date_range('2025-04-01', '2026-03-01', freq='MS'),
                           columns=['monthstartdate'])
pred_period['Month'] = pred_period['monthstartdate'].dt.month
pred_period['Data'] = 'Forecast Period'
#Get first date of current month to remove this from the data
curr_month = datetime.today().replace(day=1)


################################################################################
############################Read in and tidy data###############################
################################################################################

###############################Data tidy function
def tidy_data(df, covid_bool, groupby_cols):
    #Convert dates and add a month column
    df['monthstartdate'] = pd.to_datetime(df['monthstartdate'],
                                          format='%Y-%m-%d')
    df['Month'] = df['monthstartdate'].dt.month
    #Filter data to not include incomplete current month
    df = df.loc[df['monthstartdate'] < curr_month
                .replace(hour=0, minute=0, second=0, microsecond=0)].copy()
    #Rename columns to Attendances if required
    if 'AmbulanceArrivals' in df.columns:
        df = df.rename(columns={'AmbulanceArrivals':'Attendances'})
        df['Ambulance'] = 'Ambulance'
    if 'Admissions' in df.columns:
        df = df.rename(columns={'Admissions':'Attendances'})
    #Remove 2020 covid data if bool is true
    if covid_bool:
        df = df.loc[df['monthstartdate'].dt.year != 2020].copy()
    #groupby and sum data to ensure one row per location/month
    df = (df.groupby(groupby_cols + ['monthstartdate', 'Month'], as_index=False)
          ['Attendances'].sum())
    return df
###############################Create sql engine
cl3_engine = create_engine('mssql+pyodbc://@cl3-data/DataWarehouse?'\
                           'trusted_connection=yes&driver=ODBC+Driver+17'\
                           '+for+SQL+Server')
###############################UEC attendances by type and site - ED Derriford
UEC_ED_query = """---ED  Derriford
select dte.monthstartdate, EDDepartmentType, count(*) as Attendances, Campus = 'Derriford'
from DataWarehouse.ed.vw_EDAttendance nerve
left join DataWarehouse.Reference.[Date] dte on dte.Date = nerve.ArrivalDate
group by dte.monthstartdate, EDDepartmentType

union all

select dte.monthstartdate, EDDepartmentType = '01', count(*) as Attendances, Campus = 'Derriford'
FROM [sdmartdatalive2].[PiMSMarts].[dbo].[accident_emergency] emerg
left join DataWarehouse.Reference.[Date] dte on dte.[Date] = cast(emerg.arrvl_dttm as date)
where [arrvl_year_month] between '201101' and '201711' 
  and campus = 'DH' ----only ED patients
  group by dte.monthstartdate
order by MonthStartDate
"""
UEC_ED_df = pd.read_sql(UEC_ED_query, cl3_engine)
UEC_ED_df = tidy_data(UEC_ED_df, covid_bool, ['EDDepartmentType', 'Campus'])
#Remove the period of time where onsite GP was killed off (Oct 2020 - June 2023)
#to remove low attendance data.
UEC_ED_df = UEC_ED_df.loc[~((UEC_ED_df['EDDepartmentType'] == '03')
                          & (UEC_ED_df['monthstartdate']
                             .isin(pd.date_range('2020-10-01', '2023-06-01',
                                                 freq='MS'))))].copy()
###############################UEC attendances by type and site - Peripherals
UEC_PERIPH_qery = """---Type 3 Peripheral Sites
--Nervecentre data
select dte.monthstartdate, EDDepartmentType, count(*) as Attendances, Campus = 'UTC Cumberland'
from DataWarehouse.ed.vw_UTCAttendance nerve
left join DataWarehouse.Reference.[Date] dte on dte.Date = nerve.ArrivalDate
group by dte.monthstartdate, EDDepartmentType, Campus

union all 
select dte.monthstartdate, EDDepartmentType, count(*) as Attendances
,case when Campus = 'RK935' then 'Kingsbridge MIU' 
		when Campus = 'RK922' then 'Tavistock MIU' end as Campus
from DataWarehouse.ed.vw_MIUAttendance nerve
left join DataWarehouse.Reference.[Date] dte on dte.Date = nerve.ArrivalDate
group by dte.monthstartdate, EDDepartmentType, Campus

union all
---systmOne data
select dte.monthstartdate, EDDepartmentType = '03', count(*) as Attendances
,case when SiteCode = 'RK935' then 'Kingsbridge MIU' 
		when SiteCode = 'RK922' then 'Tavistock MIU' 
		when SiteCode = 'RK906' then 'UTC Cumberland'
		 end as Campus
from [sdmartdatalive2].[PiMSMarts].[dbo].[MIUAttendances] miu
left join DataWarehouse.Reference.[Date] dte on dte.Date = miu.ArrivalDate
where ArrivalDate >= '01-Jan-2011' --and '201711'
group by dte.monthstartdate,  case when SiteCode = 'RK935' then 'Kingsbridge MIU' 
		when SiteCode = 'RK922' then 'Tavistock MIU' 
		when SiteCode = 'RK906' then 'UTC Cumberland'
		 end
order by dte.MonthStartDate
"""
UEC_PERIPH_df = pd.read_sql(UEC_PERIPH_qery, cl3_engine)
UEC_PERIPH_df = tidy_data(UEC_PERIPH_df, covid_bool, ['EDDepartmentType', 'Campus'])
#Remove the period of time where Tavistock was closed (Aug 2019 - Feb 2020)
#to remove low attendance data.
UEC_PERIPH_df = (UEC_PERIPH_df.loc[~(
                               (UEC_PERIPH_df['Campus'] == 'Tavistock MIU')
                             & (UEC_PERIPH_df['monthstartdate']
                                .isin(pd.date_range('2019-08-01', '2020-02-01',
                                                    freq='MS'))))].copy())
###############################Ambulance arrivals to ED
AMB_query = """---ED  Derriford
select dte.monthstartdate, count(*) as AmbulanceArrivals
from DataWarehouse.ed.vw_EDAttendance nerve
left join DataWarehouse.Reference.[Date] dte on dte.Date = nerve.ArrivalDate
where AmbulanceArrivalDateTime is not NULL
and AmbulanceArrivalDateTime > '01-JUN-2022 23:59:59'
group by dte.monthstartdate

union all

select dte.monthstartdate,  count(*) as AmbulanceArrivals
FROM [sdmartdatalive2].[PiMSMarts].[dbo].[accident_emergency] emerg
left join DataWarehouse.Reference.[Date] dte on dte.[Date] = cast(emerg.arrvl_dttm as date)
where [arrvl_year_month] between '201101' and '202206' ---only attends since 2009
  and campus = 'DH' ----only ED patients
  and arrvl_mode = '1' -----best way to get ambulances from EDIS?
  group by dte.monthstartdate
order by MonthStartDate"""
AMB_df = pd.read_sql(AMB_query, cl3_engine)
AMB_df = tidy_data(AMB_df, covid_bool, ['Ambulance'])
###############################NEL Admissions split by speciality and 0 and 1+ day stays
NEL_query = """select dte.monthstartdate,
ipdc.pfmgt_spec_desc, count(*) as Admissions, case when los = '0' then '0 Day LoS' else '1+ Day LoS' end as LoSGroup
from [SDMartDataLive2].[InfoDB].[dbo].[vw_ipdc_fces_pfmgt] ipdc 
left join [SDMartDataLive2].[InfoDB].[dbo].[vw_cset_specialties] spec on spec.local_spec = ipdc.local_spec
left join DataWarehouse.Reference.[Date] dte on dte.[Date] = cast(ipdc.admit_dttm as date)
where ffce_yn = 'Y' --only one row per admission
and admit_dttm > '31-MAR-2013 23:59:59'
and admet_flag <> 'EL' ---exclude elective admissions
and (patcl = '1' or disch_dttm is NULL)---only inpatients (including current inpatients)
and fce_start_site = 'rk950' --Derriford site
and Spec.nat_spec not in ('199','223','290','291','331','344','345','346','360','424','499','501','560'
        ,'650','651','652','653','654','655','656','657','658','659','660','661','662'
        ,'700','710','711','712','713','715','720','721','722','723','724','725','726'
        ,'727','840','920') --exclude non acute specialties 
and (AAUSpell not in ('L','O') or AAUSpell is null) 
group by dte.monthstartdate, ipdc.pfmgt_spec_desc, case when los = '0' then '0 Day LoS' else '1+ Day LoS' end
order by dte.monthstartdate, ipdc.pfmgt_spec_desc, case when los = '0' then '0 Day LoS' else '1+ Day LoS' end
"""
NEL_df = pd.read_sql(NEL_query, cl3_engine)
#If this is true, use august data to start forecast from (MAY NEED TO CHANGE
#IF SEPT DATA INCREASES/OCT DATA BETTER)
if NEL_plus1_bool:
    NEL_df = NEL_df.loc[~((NEL_df['monthstartdate'] == date(2024, 9, 1))
                         & (NEL_df['LoSGroup'] == '1+ Day LoS'))]
NEL_df = tidy_data(NEL_df, covid_bool, ['pfmgt_spec_desc', 'LoSGroup'])

###############################Dispose of engine
cl3_engine.dispose()


################################################################################
#########################Function to create forecasts###########################
################################################################################

###############################Function to calculate forecasts
def demand_forecasting(df, groupby_cols, NEL):
    ###############################Get a dataframe of the unique groups
    groups = df[groupby_cols].drop_duplicates()
    groups['key'] = 0 
    ###############################Get current position rows
    #(first date of prev month so using whole month)
    curr_pos = (df.loc[df['monthstartdate']
                       == (curr_month - pd.DateOffset(months=1))
                       .floor(freq = 'd')].copy())
    ###############################Get average monthly % change trend
    trend_df = (df.groupby(groupby_cols + ['Month'], as_index=False)
                    ['Attendances'].mean())
    trend_df['Percent Change'] = (trend_df['Attendances']
                                    .div(trend_df
                                         .groupby(groupby_cols)['Attendances']
                                    .transform(lambda x: np.roll(x, 1))))
    ###############################If not NEL_df, then use original method
    if (not NEL) or (not NEL_plus1_bool):
        #Add data column and rename attendances column in current position data
        curr_pos['Data'] = 'Current Position'
        curr_pos = curr_pos.rename(columns={'Attendances':'Trend'})
        #Get period between current position and forecasted period
        interim_period = pd.DataFrame(pd.to_datetime(
                                      pd.date_range(curr_month,
                                      pred_period['monthstartdate'].min(),
                                      freq='MS').strftime('%Y-%m-%d')),
                                    columns=['monthstartdate'])
        interim_period['Month'] = interim_period['monthstartdate'].dt.month
        interim_period['Data'] = 'Interim Period'
        #Add the interim and predicted periods into one dataframe, create 0 key
        #column to cross join  to replicate for each group in the data. Concat
        #currrent position onto this df.
        trend_period = pd.concat([interim_period, pred_period])
        trend_period['key'] = 0
        merge_col = 'key'
    ###############################For NEL_df, +1 days uses august as start
    #(THIS MAY NEED TO CHANGE IF SEPT DATA IMPROVES OR ONCE NOVEMBER, AS OCTOBER
    #MAY BE OK TO USE).
    else:
        #Add august data for 1+ Day LoS to curr_pos if this is NEL_plus1_bool
        #is true
        aug_plus1 = df.loc[((df['monthstartdate'] == '2024-08-01')
                          & (df['LoSGroup'] == '1+ Day LoS'))]
        curr_pos = pd.concat([curr_pos, aug_plus1])
        curr_pos['Data'] = 'Current Position'
        curr_pos = curr_pos.rename(columns={'Attendances':'Trend'})
        #As 1+ Day LoS is currently forecasting from august, we need different
        #interim periods for this data.
        interim_dates_lst = []
        LoS_lst = []
        pred_period_lst = []
        for LoS_group in ['0 Day LoS', '1+ Day LoS']:
            #Get the data for each LoS
            LoS_df = df.loc[df['LoSGroup'] == LoS_group].copy()
            #Get the interim start date as final date in the data +1 month
            interim_start = (LoS_df['monthstartdate'].max()
                             + pd.DateOffset(months=1))
            #Get list of each monthly date between the start of the interim
            #period and the start of the predicted period (not including the
            #max date in data and the min date in pred period).
            interim_dates = pd.to_datetime(
                            pd.date_range(interim_start,
                                          pred_period['monthstartdate'].min(),
                                          freq='MS', inclusive='left')
                                          .strftime('%Y-%m-%d'))
            interim_dates_lst += list(interim_dates)
            LoS_lst += [LoS_group] * len(interim_dates)
            #create pred df for each LoS group as well, so this is a populated
            #column to simplify future code
            pred = pred_period.copy()
            pred['LoSGroup'] = LoS_group
            pred_period_lst.append(pred)
        #Create dataframe of the interim period dates and LoSGroup
        interim_period = pd.DataFrame({'monthstartdate':interim_dates_lst,
                                       'LoSGroup':LoS_lst,
                                       'Data':['Interim Period']
                                              * len(LoS_lst)})
        interim_period['Month'] = interim_period['monthstartdate'].dt.month
        #concat the interim and pred periods
        new_pred_period = pd.concat(pred_period_lst)
        trend_period = pd.concat([interim_period, new_pred_period])
        merge_col = 'LoSGroup'
    ###############################Merge trend periods onto trend data
    #Merge the trend period on the groups to get a dataframe with the trend
    #for each group ready to apply.
    trend_period = (groups.merge(trend_period, on=merge_col, how='outer')
                    .drop('key', axis=1))
    trend_period = pd.concat([curr_pos, trend_period])
    #Merge the trend period onto the trend_df to add in the trend data and
    #sort values.
    trend_period = (trend_period.merge(
                    trend_df[groupby_cols
                             + ['Month', 'Attendances', 'Percent Change']],
                    on=groupby_cols + ['Month'])
                    .sort_values(by=groupby_cols + ['monthstartdate', 'Month']))
    ###############################Loop over each group and apply the trend.
    #Set empty list to store the forecast, loop over each group in the data and
    #apply the trend to it's starting point.
    group_col = []
    dates_col = []
    forec_col = []
    #Due to +1days having a different interim period, sometime this will need to
    #be -1.
    total_trend_length = len(trend_period['monthstartdate'].drop_duplicates())
    for index, group in groups[groupby_cols].iterrows():
        #for each group, merge to that group to get it's trend data
        group_df = pd.DataFrame({col:group[col] for col in groupby_cols},
                                index=[0])
        trend_data = trend_period.merge(group_df, on=list(groupby_cols),
                                        how='inner')
        #If no data at this point, skip.
        if len(trend_data) > 0:
            #Apply the trend to that groups data
            forecast = [trend_data.loc[0, 'Trend']]
            trend = trend_data['Percent Change'].iloc[1:].values.tolist()
            #As we are using august data for the +1 days, the length will be
            #shorter when 0 Day LoS, so add the below line to counter that
            #HOPEFULLY CAN GET RID IF CURRENT POSITION IMPROVES.
            if NEL:
                trend_length = (total_trend_length
                                if not (NEL_plus1_bool
                                and group_df.loc[0, 'LoSGroup'] == '0 Day LoS')
                                else total_trend_length - 1)
            else:
                trend_length = total_trend_length
            #If there is current position data and enough trend, use this
            if (not np.isnan(forecast[0])) and (len(trend) == trend_length-1):
                for i, change in enumerate(trend):
                    forecast.append(forecast[i] * change)
            #else forecast is historical average
            else:
                forecast = trend_data['Attendances'].values.tolist()
                print(f'Average being used for {group.values.tolist()}')
            #add the forecasts to the empty list
            group_col += (trend_data[groupby_cols].apply(tuple, axis=1)
                          .values.tolist())
            dates_col += trend_data['monthstartdate'].values.tolist()
            forec_col += forecast
        #If no trend data, skip
        else:
            print(f'Skipped group {group.values.tolist()} due to no data')
    ###############################Create dataframe of the forecasts
    #Create dataframe of all the groups and forecasts
    trend_period = pd.DataFrame({'Group':group_col,
                                 'monthstartdate':pd.to_datetime(dates_col),
                                 'Forecast':forec_col})
    #Reduce dataframe to just the forecasted period.
    forecast_df = trend_period.merge(pred_period['monthstartdate'],
                                     on='monthstartdate', how='right')
    ###############################Apply the yearly uplifts
    #calculate yearly uplift and apply. NEL done at top level.
    merge_col = 'Group'
    if NEL:
        #use different groupby and merge columns so uplift is applied at a
        #LoSGroup level
        groupby_cols = ['LoSGroup']
        merge_col = 'LoSGroup'
        forecast_df[['spec', 'LoSGroup']] = forecast_df['Group'].values.tolist()
    #Get the uplift between years
    scalar_df = (df.groupby(groupby_cols + [df['monthstartdate'].dt.year])
                    ['Attendances'].agg(['sum', 'count'])).reset_index()
    #scale to be a full year if less than 12 entries
    scalar_df['sum'] = (scalar_df['sum']/scalar_df['count'])*12
    #Calculate the percent change between each years and get the average
    #change between years for each group.
    scalar_df['scalar'] = scalar_df.groupby(groupby_cols)['sum'].pct_change()+1
    scalar_df = scalar_df.groupby(groupby_cols, as_index=False)['scalar'].mean()
    if not NEL:
        #Add Group column to merge on if not NEL data
        scalar_df['Group'] = (scalar_df[groupby_cols].apply(tuple, axis=1)
                              .values.tolist())
    #Merge forecast onto scalar dataframe and apply uplift to the forecast
    forecast_df = forecast_df.merge(scalar_df, on=merge_col)
    forecast_df['Forecast'] = (forecast_df['Forecast']
                               * forecast_df['scalar']).astype(int)
    #Select only required columns
    forecast_df = forecast_df[['Group', 'monthstartdate', 'Forecast']].copy()
    return forecast_df, scalar_df

###############################Run forecast on each dataframe
UEC_ED_forecast, UEC_ED_scalar = demand_forecasting(UEC_ED_df,
                                        ['EDDepartmentType', 'Campus'], False)
UEC_PERIPH_forecast, UEC_PERIPH_scalar = demand_forecasting(UEC_PERIPH_df,
                                        ['EDDepartmentType', 'Campus'], False)
AMB_forecast, AMB_scalar = demand_forecasting(AMB_df, ['Ambulance'], False)
NEL_forecast, NEL_scalar = demand_forecasting(NEL_df,
                                        ['pfmgt_spec_desc', 'LoSGroup'], True)
scalars = pd.concat([UEC_ED_scalar, UEC_PERIPH_scalar, AMB_scalar, NEL_scalar])


################################################################################
##############################combine forecasts#################################
################################################################################

###############################combine forecasts into one and output.
#add a sort column to keep the forecasts in the correct order
UEC_ED_forecast['sort'] = 1
UEC_PERIPH_forecast['sort'] = 2
AMB_forecast['sort'] = 3
NEL_forecast['sort'] = 4
###############################Concat tables together and pivot
full_forecast = pd.concat([UEC_ED_forecast, UEC_PERIPH_forecast,
                           AMB_forecast, NEL_forecast]).round()
full_forecast['monthstartdate'] = (full_forecast['monthstartdate']
                                   .dt.strftime('%Y-%m-%d'))                 
full_forecast = (full_forecast.pivot(index=['sort', 'Group'],
                                     columns='monthstartdate',
                                     values='Forecast').reset_index()
                                     .drop('sort', axis=1))


################################################################################
##############################create excel output###############################
################################################################################

###############################Pivot table function
def create_data_forecast_table(df, forecast, groupby_cols):
    #Add year and group columns to the historical dataframe
    df['year'] = df['monthstartdate'].dt.year
    df['Group'] = df[groupby_cols].apply(tuple, axis=1).values.tolist()
    #Add month and year columns to the forecast dataframe
    forecast['Month'] = forecast['monthstartdate'].dt.month
    forecast['year'] = forecast['monthstartdate'].dt.year
    #Pivot and concatenate both into one table of historical and forecast data.
    table_df = (pd.concat([df
                           .pivot(index=['Group', 'year'], columns='Month',
                                  values='Attendances'),
                           forecast
                           .pivot(index=['Group', 'year'],
                                  columns='Month', values='Forecast')])
               .reset_index().sort_values(by=['Group', 'year']))
    return table_df
###############################Create past and forecast data pivot tables
UEC_ED_table = create_data_forecast_table(UEC_ED_df, UEC_ED_forecast,
                                          ['EDDepartmentType', 'Campus'])
UEC_PERIPH_table = create_data_forecast_table(UEC_PERIPH_df,
                                              UEC_PERIPH_forecast,
                                              ['EDDepartmentType', 'Campus'])
AMB_table = create_data_forecast_table(AMB_df, AMB_forecast, ['Ambulance'])
#Group up NEL data to 0 and 1+ LoS
NEL_df_sum = (NEL_df.groupby(['monthstartdate', 'LoSGroup', 'Month'],
                             as_index=False)['Attendances'].sum())
NEL_df_sum['Group'] = NEL_df_sum['LoSGroup']
NEL_forecast[['specialty', 'LoSGroup']] = NEL_forecast['Group'].values.tolist()
NEL_forecast_sum = (NEL_forecast.groupby(['monthstartdate', 'LoSGroup'],
                                         as_index=False)['Forecast'].sum())
NEL_forecast_sum['Group'] = (NEL_forecast_sum[['LoSGroup']]
                             .apply(tuple, axis=1).values.tolist())
NEL_table = create_data_forecast_table(NEL_df_sum, NEL_forecast_sum,
                                       ['LoSGroup'])
###############################Combine into one and create an excel output
combined = pd.concat([UEC_ED_table, UEC_PERIPH_table,
                      AMB_table, NEL_table]).round()
writer = pd.ExcelWriter(
         f"Demand Forecast Compare {datetime.today().strftime('%Y-%m-%d')}.xlsx",
         engine='xlsxwriter')
workbook = writer.book
#create one sheet for each group
for group in combined['Group'].values.tolist():
    #Write dataframe to excel file
    group_df = combined.loc[combined['Group'] == group].copy()
    sheet_name = ' '.join(group)
    group_df.to_excel(writer, sheet_name=sheet_name, index=False,
                      engine='xlsxwriter')
    worksheet = writer.sheets[sheet_name]
    max_rows = len(group_df)
    #Add line chart
    #Get a list of HTML greyscale values to use for the n past years in the
    #chart add 2 yellow at the end for the forecast
    HTML_greyscale = (['#%02x%02x%02x' % (n, n, n) for n in
                      [round(i) for i in np.linspace(250, 0, max_rows-2)]]
                      + ['#ffcc00', '#ffcc00'])
    # Create a chart object.
    chart = workbook.add_chart({"type": "line"})
    years = group_df['year'].drop_duplicates().values.tolist()
    #loop over each year and plot the data
    for i, year in enumerate(years):
        row = i+1
        width = 1.5 if i < max_rows-2 else 3 #forecast to be thicker lines
        chart.add_series({'name':str(year),
                          'values':[sheet_name, row, 2, row, 13],
                          'line':{'color' : HTML_greyscale[i],
                                  'width' : width}})
    chart.set_x_axis({'name':'Month'})
    chart.set_y_axis({'name':'Attendances',
                      'major_gridlines':{'visible':False}})
    chart.set_size({'width':950, 'height':800})
    worksheet.insert_chart(0, 14, chart)
#Add full forecast and scalars sheet
full_forecast.to_excel(writer, sheet_name='Full Forecast', index=False,
                       engine='xlsxwriter')
scalars.to_excel(writer, sheet_name='Scalars', index=False,
                       engine='xlsxwriter')
#save excel
writer.close()
