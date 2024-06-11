import traceback
import datetime
import numpy as np
import datajoint as dj

from bl_pipeline.datajoint01_pipeline.shadow import lab as lab_shadow
from bl_pipeline.datajoint01_pipeline.shadow import subject as subject_shadow
from bl_pipeline.datajoint01_pipeline.shadow import action as action_shadow
from bl_pipeline.datajoint01_pipeline.shadow import acquisition as acquisition_shadow
from bl_pipeline.datajoint01_pipeline.shadow import behavior as behavior_shadow

from bl_pipeline.datajoint01_pipeline import lab, subject, action, acquisition, behavior

import sys

if len(sys.argv) > 1:
    num_days_before = int(sys.argv[1])
else:
    num_days_before = 3


dj.config["enable_python_native_blobs"] = True

dict_dates_big_tables = {
    'Rigfood':  'rigfood_datetime',
    'RigMaintenance': 'broke_date',
    'CalibrationInfoTbl': 'calibration_datetime',
    'Technotes': 'technote_datetime',
    'TechSchedule': 'techschedule_date',
    'Surgery': 'surgery_date',
    'Rigwater':  'earnedwater_datetime',
    'SessStarted': 'session_date',
    'Mass': 'weighing_datetime',
    'Water': 'administration_date',
    'Schedule': 'schedule_date',
    'Sessions': 'session_date',
    #'Rats': 'deliverydate',
    'RatHistory': 'logtime',   
}

dict_dates_big_tables_no_date = {
    'BehaviorEvent':  'sessid',
    'SessionProtocolData': 'sessid',
    'ParsedEvents': 'sessid',
}

dict_tables_primary_id = {
    'Technotes': ['technote_datetime', 'technote_id'],
    'TechSchedule': ['techschedule_date','scheduleid'],
    'Surgery': ['surgery_date', 'surgery_old_id'],
    'Rigwater': ['earnedwater_datetime', 'rigwater_id'],
    'SessStarted': ['session_date', 'sessid'],
    'Mass': ['weighing_datetime', 'mass_id'],
    'Water': ['administration_date', 'water_id'],
    'Schedule': ['schedule_date', 'schedule_id'],
    'Sessions': ['session_date', 'sessid'],
    'RatHistory': ['logtime', 'rathistory_old_id']
}

# Transfer data from 5 days in the past
date_ref = datetime.date.today()
date_ref = date_ref - datetime.timedelta(num_days_before)
date_ref = date_ref.strftime("%Y-%m-%d")

# Copy data from shadow table (src_schema) to new table (target_schema)
def copy_table(target_schema, src_schema, table_name, **kwargs):
    target_table = getattr(target_schema, table_name)
    src_table = getattr(src_schema, table_name)

    if table_name in list(dict_dates_big_tables.keys()):
        query =  dict_dates_big_tables[table_name] + ">='" + date_ref + "'"
        src_table = src_table & query
        target_table = target_table & query
    
    q_insert = src_table - target_table.proj()
    
    try:
        target_table.insert(q_insert, **kwargs)
    except Exception:
        for t in (q_insert).fetch(as_dict=True):
            try:
                target_table.insert1(t, **kwargs)
            except Exception:
                print("Error when inserting {}".format(t))
                traceback.print_exc()

# Copy data from shadow table (src_schema) to new table (target_schema) with no date reference
def copy_table_no_date(target_schema, src_schema, table_name, id_ref1, id_ref2, **kwargs):
    target_table = getattr(target_schema, table_name)
    src_table = getattr(src_schema, table_name)

    if table_name in list(dict_dates_big_tables_no_date.keys()):
        query =  dict_dates_big_tables_no_date[table_name] + ">='" + str(id_ref1) + "' and " + dict_dates_big_tables_no_date[table_name] + "<" + str(id_ref2) + "'"
        print(query) 
        src_table = src_table & query
        target_table = target_table & query
    
    q_insert = src_table - target_table.proj()
    
    try:
        target_table.insert(q_insert, **kwargs)
    except Exception:
        for t in (q_insert).fetch(as_dict=True):
            try:
                target_table.insert1(t, **kwargs)
            except Exception:
                print("Error when inserting {}".format(t))
                traceback.print_exc()
    

def copy_sessions_with_blob():

    bdatatest = dj.create_virtual_module('bdatatatest', 'bdatatest')

    sessions_t = bdatatest.Sessions * bdatatest.ParsedEvents & 'session_date > "2022-08-01"'


    q_insert = sessions_t - acquisition.Sessions2.proj()


MODULES = [
    dict(
            module=(lab, lab_shadow),
            tables=[
                'Contacts',
                'Riginfo',
                'Rigflush',
                'Rigfood',
                'RigMaintenance',
                'TrainingRoom'
            ]
        ),
    dict(
            module=(subject, subject_shadow),
            tables=[
                'Rats',
                # 'Rats.Contact',
                #'RatHistory',
                # 'RatHistory.Contact'
            ]
        ),
    dict(
            module=(action, action_shadow),
            tables=[
               # 'CalibrationInfoTbl',
                'Mass',
                'Rigwater',
                'Schedule',
                'Surgery',
                'TechSchedule',
                'Technotes',
                'Water'
            ]
        ),
    dict(
            module=(acquisition, acquisition_shadow),
            tables=[
                'SessStarted',
                'Sessions'
            ]
        )
]

MODULES_NO_DATE = [
    dict(
            module=(acquisition, acquisition_shadow),
            tables=[
                'SessionProtocolData',
                'ParsedEvents'
            ]
        ),

    #dict(
    #        module=(behavior, behavior_shadow),
    #        tables=[
    #            'BehaviorEvent'
    #        ]
    #    )
]

# Copy data from source tables to shadow tables
def ingest_shadow():

    kwargs = dict(display_progress=True, suppress_errors=False)
    for m in MODULES:
        for table_name in m['tables']:
            table_shadow = getattr(m['module'][1], table_name)
            print(f'Populating shadow table {table_name}')
            if table_name in list(dict_tables_primary_id.keys()):
                query_date =  [dict_tables_primary_id[table_name][0] + ">='" + date_ref + "'"]
                print('query_date', query_date)
                id_date = (table_shadow & query_date).fetch(dict_tables_primary_id[table_name][1], limit=1)
                print('id_date', id_date)
                print(query_date)
                if len(id_date) > 0:
                    id_date = id_date[0]
                    query =  [dict_tables_primary_id[table_name][1] + ">=" + str(id_date)]
                    print('query', query)
                    table_shadow.populate(query, **kwargs)
            else:
                table_shadow.populate(**kwargs)

# Copy data from shadow table to new table
def ingest_real():

    for m in MODULES:
        for table_name in m['tables']:
            print(f'Copying to real table {table_name}')
            copy_table(m['module'][0], m['module'][1], table_name)

def get_sessid_date():

    query_date = "session_date >='"  + date_ref +"'"
    min_sessid = (acquisition_shadow.SessStarted & (query_date)).fetch("sessid", order_by="sessid", limit=1)
    max_sessid = (acquisition_shadow.SessStarted & (query_date)).fetch("sessid", order_by="sessid DESC", limit=1)

    return min_sessid, max_sessid

# Copy data from source tables to shadow tables
def ingest_shadow_no_date(min_sessid, max_sessid):

    kwargs = dict(display_progress=True, suppress_errors=False)
    for m in MODULES_NO_DATE:
        for table_name in m['tables']:
            table_shadow = getattr(m['module'][1], table_name)
            print(f'Populating shadow table no_date {table_name}')
           
            sess_array = np.arange(min_sessid,max_sessid+1000,1000)

            for j in range(sess_array.shape[0]-1):
                sql2 = 'sessid >= ' + str(sess_array[j]) + " AND sessid < " + str(sess_array[j+1])
                print(sql2)
                table_shadow.populate(sql2, **kwargs)

# Copy data from shadow table to new table
def ingest_real_no_date(min_sessid, max_sessid):

    for m in MODULES_NO_DATE:
        for table_name in m['tables']:
            print(f'Copying to real table no date {table_name}')

            sess_array = np.arange(min_sessid,max_sessid+1000,1000)
            for j in range(sess_array.shape[0]-1):
                copy_table_no_date(m['module'][0], m['module'][1], table_name, sess_array[j], sess_array[j+1])

def main():



    #ingest_shadow()
    #ingest_real()

    min_sessid, max_sessid = get_sessid_date()
    #ingest_shadow_no_date(min_sessid, max_sessid)
    ingest_real_no_date(min_sessid, max_sessid)


    # Copy data from shadow table to new table
    # subject.Rats.Contact.insert(subject_shadow.Rats.Contact)
    # subject.RatHistory.Contact.insert(subject_shadow.RatHistory.Contact)

if __name__ == '__main__':
    main()
