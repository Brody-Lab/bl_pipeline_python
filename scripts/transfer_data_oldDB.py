
from scripts.conf_file_finding import try_find_conf_file
try_find_conf_file()

import pandas as pd
import datajoint as dj
import datetime
import sys
import numpy as np
import json
import sys

if len(sys.argv) > 1:
    num_days_before = int(sys.argv[1])
else:
    num_days_before = 3

dj.blob.bypass_serialization = True

#Convert time column in pandas to string (for DatajointInsertion)
def convert_time_2_str(df_column):
    
    df_column = df_column - pd.to_timedelta(df_column.dt.days, unit='d')
    df_column = df_column.astype(str)
    df_column = df_column.str.slice(start=-8)
    df_column[df_column == 'NaT']  = None
    df_column[df_column == '0 days']  = '00:00'

    return df_column

#Convert date column in pandas to string (for DatajointInsertion)
def convert_dates_2_str(df_column):
    
    df_column = df_column.astype(str)
    df_column.loc[df_column == "0000-00-00"] = None
    df_column.loc[df_column == "None"] = None
    df_column.loc[df_column == "NaT"] = None
    
    return df_column

#Convert datetimes column in pandas to string (for DatajointInsertion)
def convert_datetimes_2_str(df_column):
    
    df_column = df_column.astype(str)
    df_column.loc[df_column == "0000-00-00 00:00:00"] = "1000-01-01 00:00:00"
    df_column.loc[df_column == "None"] = None
    df_column.loc[df_column == "NaT"] = None
    
    return df_column
    
#Convert time column in pandas to string (for DatajointInsertion)
def fix_zero_fill(df_column):
    
    df_column = df_column.astype(str)
    df_column[df_column == '001']  = '1'
    df_column[df_column == '000']  = '0'

    df_column = df_column.astype(int)

    return df_column

# Transfer data from 5 days in the past
date_ref = datetime.date.today()
date_ref = date_ref - datetime. timedelta(num_days_before)
date_ref = date_ref.strftime("%Y-%m-%d")

# Special parameters, date columns and big tables with no date field
nodate_tables = ['sess_list', 'parsed_events', 'sessions', 'channels', '_behavior_event']

noneed_copy = ['schedule', 'carlosexperiment', 'infusions', 'events', 'raw_tracking', 'tracking', 'gcs_old', 'sched_rescue', 'gcs', 'video_log', 'spktimes', 'rigtrials', '~log']

dict_dates_big_tables = {
    'technotes': 'datestr',
    'tech_schedule': 'date',
    'surgery': 'date',
    'rigwater':  'dateval',
    'sess_started': 'sessiondate',
    'mass': 'date',
    'water': 'date',
    'schedule': 'date',
    'sessions': 'sessiondate',
    'session_agg_date': 'sessiondate',
    'calibration_info_tbl': 'dateval'
}
time2_str_dict = {
    'rigtrials': ['lastupdate'],
    'technotes': ['timestr'],
    'mass': ['timeval'],
    'water': ['starttime', 'stoptime'],
    'sessions': ['starttime', 'endtime'],
    'session_agg_date': ['starttime', 'endtime'],
    'turn_down_log': ['turn_time'],
    'sess_started': ['starttime']
}
date2_str_dict = {
    'rats': ['dateSac', 'deliverydate'],
    'rat_history': ['dateSac'],   
    'eibs': ['made_on'],
    'experiment': ['end_date'],
    'turn_down_log': ['turn_date'],
    'rigvideo': ['dateval'],
}
datetime2_str_dict = {
    'rig_maintenance': ['broke_date', 'fix_date'],
    'calibration_info_tbl': ['dateval']
}
fix_zero_fill_dict = {
    'rigflush': ['wasflushed']
}

#Connect to datajoint DB
conn1 = dj.conn(reset=True)
bdatatest = dj.create_virtual_module('bdatatest', 'bdatatest')
ratinfotest = dj.create_virtual_module('ratinfotest','ratinfotest')

# Connect to BrodyLab DB
db_params_file = open('brodylab_db_conf.json','r')
db_params = db_params_file.read()
db_params = json.loads(db_params)

con = dj.conn(host=db_params['host'],user=db_params['user'],password=db_params['password'], reset=True)
#con=client.connect(host=db_params['host'],user=db_params['user'],password=db_params['password'], ssl=True)

#with con.cursor() as cur:
sql = """SELECT TABLE_SCHEMA, TABLE_NAME, UPDATE_TIME, 
    ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024) AS `Size_MB`
    FROM   information_schema.tables
    where TABLE_SCHEMA = 'bdata' or TABLE_SCHEMA = 'ratinfo'"""
tables_db = pd.DataFrame(con.query(sql, as_dict=True))
# tables_db = pd.DataFrame(data_fetched, columns=['TABLE_SCHEMA', 'TABLE_NAME', 'UPDATE_TIME','Size_MB'])

### Only copy data from tables that have been updated this year
tables_db_copy = tables_db.loc[tables_db['UPDATE_TIME'] > datetime.datetime(2021,1,1), :]

#Select tables to copy
#### 1. smalltables (copy all)
#### 2. Tables with date field (copy from selected date)
#### 3. Tables with no date (get sessid to copy from) 
tables_db_copy = tables_db_copy.sort_values(by='Size_MB')
tables_db_copy = tables_db_copy.reset_index(drop=True)
# sessidlist and parsed events
tables_nodate_copy = tables_db_copy.loc[tables_db_copy['TABLE_NAME'].isin(nodate_tables), :]
tables_nodate_copy = tables_nodate_copy.reset_index(drop=True)
#rest of tables to copy
tables_db_copy2 = tables_db_copy.loc[~tables_db_copy['TABLE_NAME'].isin(nodate_tables), :]
tables_db_copy2 = tables_db_copy2.loc[~tables_db_copy2['TABLE_NAME'].isin(noneed_copy), :]
tables_db_copy2 = tables_db_copy2.reset_index(drop=True)

## Fetch from brodylabvm and insert on datajoint01 (group 1 and 2)
for i in range(tables_db_copy2.shape[0]):

    this_table = tables_db_copy2.loc[i, 'TABLE_NAME']

    #if this_table in ['rigtrials', 'technician_notes_tbl', 'sessview', 'biasview', 'rigflush', 'gcs_old', 'sched_rescue', 'gcs']:
    #    continue
    #if this_table in ['gcs_old', 'sched_rescue', 'gcs']:
    #    continue

    sql = "SELECT * from " + tables_db_copy2.loc[i, 'TABLE_SCHEMA'] + "." + tables_db_copy2.loc[i, 'TABLE_NAME'] +  " "

    print(sql)

    #Insert where > date from big tables
    if tables_db_copy2.loc[i, 'TABLE_NAME'] in (list(dict_dates_big_tables.keys())):   
        sql += ' WHERE '  + dict_dates_big_tables[tables_db_copy2.loc[i, 'TABLE_NAME']] + ' > "' + date_ref + '"' 
    
    #Fetch data
    con = dj.conn(host=db_params['host'],user=db_params['user'],password=db_params['password'], reset=True)
    dj.blob.bypass_serialization = True
    data_insert = pd.DataFrame(con.query(sql, as_dict=True))
    con.close()
    #with con.cursor() as cur:
    #    cur.execute(sql)
    #    data_insert = pd.DataFrame(cur.fetchall(), columns=columns_table)

    if data_insert.shape[0] > 0:

        conn1 = dj.conn(reset=True)
        
        #Get min session for tables that dont have date
        if tables_db_copy2.loc[i, 'TABLE_NAME'] == "sess_started":
            min_session = data_insert['sessid'].min()
            max_session = data_insert['sessid'].max()

        #Convert time columns
        if tables_db_copy2.loc[i, 'TABLE_NAME'] in (list(time2_str_dict.keys())):
            list_times = time2_str_dict[tables_db_copy2.loc[i, 'TABLE_NAME']]
            for j in list_times:
                data_insert[j] = convert_time_2_str(data_insert[j])    
        
        #Convert date columns
        if tables_db_copy2.loc[i, 'TABLE_NAME'] in (list(date2_str_dict.keys())):
            list_dates = date2_str_dict[tables_db_copy2.loc[i, 'TABLE_NAME']]
            for j in list_dates:
                data_insert[j] = convert_dates_2_str(data_insert[j])
                
        #Convert datetime columns
        if tables_db_copy2.loc[i, 'TABLE_NAME'] in (list(datetime2_str_dict.keys())):
            list_dates = datetime2_str_dict[tables_db_copy2.loc[i, 'TABLE_NAME']]
            for j in list_dates:
                data_insert[j] = convert_datetimes_2_str(data_insert[j])

        #Fix zero fill
        if tables_db_copy2.loc[i, 'TABLE_NAME'] in (list(fix_zero_fill_dict.keys())):
            list_zerofill = fix_zero_fill_dict[tables_db_copy2.loc[i, 'TABLE_NAME']]
            for j in list_zerofill:
                data_insert[j] = fix_zero_fill(data_insert[j])
        
        #Get datajoint table handle
        schema_class= getattr(sys.modules[__name__], tables_db_copy2.loc[i, 'TABLE_SCHEMA']+"test")
        table_class = getattr(schema_class, dj.utils.to_camel_case(tables_db_copy2.loc[i, 'TABLE_NAME']))
        table_instance = table_class()

        print(data_insert)

        dj.blob.bypass_serialization = True
        table_instance.insert(data_insert, skip_duplicates=True)
        conn1.close()

## Fetch from brodylabvm and insert on datajoint01 (group 3)
for i in range(tables_nodate_copy.shape[0]):

     sql = "SELECT * from " + tables_nodate_copy.loc[i, 'TABLE_SCHEMA'] + "." + tables_nodate_copy.loc[i, 'TABLE_NAME'] +  " "

     #Get datajoint table handle
     schema_class= getattr(sys.modules[__name__], tables_nodate_copy.loc[i, 'TABLE_SCHEMA']+"test")
     table_class = getattr(schema_class, dj.utils.to_camel_case(tables_nodate_copy.loc[i, 'TABLE_NAME']))
     table_instance = table_class()

     #Insert on groups of 1000 sessions (if not data to big)
     sess_array = np.arange(min_session,max_session+50,50)
     for j in range(sess_array.shape[0]-1):
        sql2 = sql +' WHERE sessid >= ' + str(sess_array[j]) + " AND sessid < " + str(sess_array[j+1])
        print(sql2)

        con = dj.conn(host=db_params['host'],user=db_params['user'],password=db_params['password'], reset=True)
        dj.blob.bypass_serialization = True
        data_insert = pd.DataFrame(con.query(sql2, as_dict=True))
        con.close()

        if data_insert.shape[0] > 0:

            conn1 = dj.conn(reset=True)
        
            #Convert time columns
            if tables_nodate_copy.loc[i, 'TABLE_NAME'] in (list(time2_str_dict.keys())):
                list_times = time2_str_dict[tables_nodate_copy.loc[i, 'TABLE_NAME']]
                for j in list_times:
                    data_insert[j] = convert_time_2_str(data_insert[j])
    
            print(data_insert)
            dj.blob.bypass_serialization = True
            table_instance.insert(data_insert, skip_duplicates=True)
            conn1.close()
    
