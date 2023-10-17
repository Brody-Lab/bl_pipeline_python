
import os
import pathlib
import datetime
import datajoint as dj
import pandas as pd
from scripts.conf_file_finding import try_find_conf_file

def get_all_session_aggregate(subjects = None):

    bdata = dj.create_virtual_module('bdata', 'bdata')
    ratinfo = dj.create_virtual_module('ratinfo', 'ratinfo')

    df_agg_data = get_previous_agg_data(subjects=subjects,bdata=bdata)
    df_sessions = aggregate_todays_sessions(subjects=subjects, bdata=bdata)
    df_mass = aggregate_todays_mass(subjects=subjects, ratinfo=ratinfo)
    df_water = aggregate_todays_water(subjects=subjects, ratinfo=ratinfo)
    df_rigwater = aggregate_todays_rigwater(subjects=subjects, ratinfo=ratinfo)

    df_sessions = pd.merge(df_sessions, df_mass, how='left', on='ratname')
    df_sessions = df_sessions.drop(columns='date')
    df_sessions = pd.merge(df_sessions, df_water, how='left', left_on='ratname', right_on='rat')
    df_sessions = df_sessions.drop(columns='date')
    df_sessions['num_water'] = df_sessions['num_water'].fillna(0)
    df_sessions = pd.merge(df_sessions, df_rigwater, how='left', on='ratname')
    df_sessions = df_sessions.drop(columns='dateval')
    df_sessions = df_sessions.drop(columns='rat')
    df_sessions['num_rigwater'] = df_sessions['num_rigwater'].fillna(0)

    df_sessions = pd.concat([df_agg_data, df_sessions], axis=0, ignore_index=True)

    return df_sessions

def get_previous_agg_data(subjects=None, bdata=None):

    if bdata is None:
        bdata = dj.create_virtual_module('bdata', 'bdata')

    if subjects is not None:
        session_query = [{'ratname': x} for x in subjects]
    else:
        query_date = datetime.datetime.today() - datetime.timedelta(days=30)
        query_date = query_date.strftime('%Y-%m-%d')
        session_query = "sessiondate > '" + query_date + "'"

    columns_query = ['ratname', 'sessiondate', 'num_sessions', 'n_done_trials', 'hostname', 'starttime', 'endtime',
                    'total_correct', 'percent_violations', 'right_correct', 'left_correct',
                    'mass', 'tech', 
                    'percent_target', 'volume', 'num_water', 
                    'totalvol', 'num_rigwater']

    df_agg_data = pd.DataFrame((bdata.SessionAggDate & session_query).fetch(*columns_query,as_dict=True))

    if df_agg_data.shape[0] > 0:
        df_agg_data['sessiondate'] = pd.to_datetime(df_agg_data['sessiondate'], format='%Y-%m-%d')
        df_agg_data['starttime'] = df_agg_data['starttime'].apply(lambda x: (datetime.datetime.min + x.to_pytimedelta()).time())
        df_agg_data['endtime'] = df_agg_data['endtime'].apply(lambda x: (datetime.datetime.min + x.to_pytimedelta()).time())
    else:
        df_agg_data = pd.DataFrame(columns_query)

    return df_agg_data


def aggregate_todays_sessions(subjects = None, bdata=None):

    #try_find_conf_file()
    if bdata is None:
        bdata = dj.create_virtual_module('bdata', 'bdata')

    todays_date = datetime.datetime.today().strftime('%Y-%m-%d')
    todays_session_query = {'sessiondate': todays_date}

    if subjects is not None:
        todays_session_query = [{'ratname': x, 'sessiondate': todays_date} for x in subjects] 

    columns_query = ['ratname', 'sessiondate', 'n_done_trials',
                    'starttime', 'endtime', 'hostname', 'total_correct', 'percent_violations', 'right_correct', 'left_correct']

    todays_sessions = pd.DataFrame((bdata.Sessions & todays_session_query).fetch(*columns_query, as_dict=True))
    if todays_sessions.shape[0] > 0:
        todays_sessions['sessiondate'] = pd.to_datetime(todays_sessions['sessiondate'], format='%Y-%m-%d')
        todays_sessions['starttime'] = todays_sessions['starttime'].apply(lambda x: (datetime.datetime.min + x.to_pytimedelta()).time())
        todays_sessions['endtime'] = todays_sessions['endtime'].apply(lambda x: (datetime.datetime.min + x.to_pytimedelta()).time())
        todays_sessions['total_correct_n'] = todays_sessions['total_correct'] * todays_sessions['n_done_trials']
        todays_sessions['percent_violations_n'] = todays_sessions['percent_violations'] * todays_sessions['n_done_trials']
        todays_sessions['right_correct_n'] = todays_sessions['right_correct'] * todays_sessions['n_done_trials']
        todays_sessions['left_correct_n'] = todays_sessions['left_correct'] * todays_sessions['n_done_trials']


        todays_sessions_agg1 = todays_sessions.groupby('ratname').agg({
            'sessiondate': [('sessiondate', 'min')],
            'n_done_trials': [('num_sessions', 'count'), ('n_done_trials', 'sum')],
            'hostname': [('hostname', 'min')],
            'starttime': [('starttime', 'min')],
            'endtime': [('endtime', 'max')],
            'total_correct_n': [('total_correct', 'sum')],
            'percent_violations_n': [('percent_violations', 'sum')],
            'right_correct_n': [('right_correct', 'sum')],
            'left_correct_n': [('left_correct', 'sum')],
        })

        todays_sessions_agg1.columns = todays_sessions_agg1.columns.droplevel()

        todays_sessions_agg1['total_correct'] = todays_sessions_agg1['total_correct'] / todays_sessions_agg1['n_done_trials'] 
        todays_sessions_agg1['percent_violations'] = todays_sessions_agg1['percent_violations'] / todays_sessions_agg1['n_done_trials'] 
        todays_sessions_agg1['right_correct'] = todays_sessions_agg1['right_correct'] / todays_sessions_agg1['n_done_trials'] 
        todays_sessions_agg1['left_correct'] = todays_sessions_agg1['left_correct'] / todays_sessions_agg1['n_done_trials'] 

        todays_sessions_agg1 = todays_sessions_agg1.reset_index()
    else:
        todays_sessions_agg1 = pd.DataFrame(columns=columns_query)

    return todays_sessions_agg1

def aggregate_todays_mass(subjects = None, ratinfo=None):

    if ratinfo is None:
        ratinfo = dj.create_virtual_module('ratinfo', 'ratinfo')

    todays_date = datetime.datetime.today().strftime('%Y-%m-%d')
    todays_session_query = {'date': todays_date}

    columns_query = ['ratname', 'date', 'mass', 'tech']

    if subjects is not None:
        todays_session_query = [{'ratname': x, 'date': todays_date} for x in subjects] 

    todays_mass = pd.DataFrame((ratinfo.Mass & todays_session_query).fetch(*columns_query, as_dict=True))

    if todays_mass.shape[0] > 0:
        todays_mass['date'] = pd.to_datetime(todays_date, format='%Y-%m-%d')
    else:
        todays_mass = pd.DataFrame(columns=columns_query)

    return todays_mass
    
def aggregate_todays_water(subjects = None, ratinfo=None):

    if ratinfo is None:
        ratinfo = dj.create_virtual_module('ratinfo', 'ratinfo')

    todays_date = datetime.datetime.today().strftime('%Y-%m-%d')
    todays_session_query = {'date': todays_date}

    columns_query = ['rat', 'date', 'percent_target', 'volume']

    if subjects is not None:
        todays_session_query = [{'rat': x, 'date': todays_date} for x in subjects] 

    todays_water = pd.DataFrame((ratinfo.Water & todays_session_query).fetch(*columns_query, as_dict=True))

    if todays_water.shape[0] > 0:

        todays_water['date'] = pd.to_datetime(todays_water['date'], format='%Y-%m-%d')
        todays_water_agg1 = todays_water.groupby('rat').agg({
            'date': [('date', 'min')],
            'percent_target': [('percent_target', 'max')],
            'volume': [('volume', 'max'), ('num_water', 'count')]
        })

        todays_water_agg1.columns = todays_water_agg1.columns.droplevel()
        todays_water_agg1 = todays_water_agg1.reset_index()
    else:
        todays_water_agg1 = pd.DataFrame(columns=columns_query)
    
    return todays_water_agg1


def aggregate_todays_rigwater(subjects = None, ratinfo=None):

    if ratinfo is None:
        ratinfo = dj.create_virtual_module('ratinfo', 'ratinfo')

    todays_date = datetime.datetime.today().strftime('%Y-%m-%d')
    todays_session_query = {'dateval': todays_date}

    columns_query = ['ratname', 'dateval', 'totalvol']

    if subjects is not None:
        todays_session_query = [{'ratname': x, 'dateval': todays_date} for x in subjects] 

    todays_rigwater = pd.DataFrame((ratinfo.Rigwater & todays_session_query).fetch(*columns_query, as_dict=True))

    if todays_rigwater.shape[0] > 0:

        todays_rigwater['dateval'] = pd.to_datetime(todays_rigwater['dateval'], format='%Y-%m-%d')
        todays_rigwater_agg1 = todays_rigwater.groupby('ratname').agg({
            'dateval': [('dateval', 'min')],
            'totalvol': [('totalvol', 'max'), ('num_rigwater', 'count')]
        })

        todays_rigwater_agg1.columns = todays_rigwater_agg1.columns.droplevel()
        todays_rigwater_agg1 = todays_rigwater_agg1.reset_index()
    else:
        todays_rigwater_agg1 = pd.DataFrame(columns=columns_query)
    
    return todays_rigwater_agg1