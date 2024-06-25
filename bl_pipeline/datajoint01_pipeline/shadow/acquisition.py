import datajoint as dj
import utility.blob_transformation as bt


bdata   = dj.create_virtual_module('bdata', 'bdatatest')
ratinfo = dj.create_virtual_module('ratinfo', 'ratinfotest')

# create new schema
schema = dj.schema('bl_shadow_acquisition')


@schema
class SessStarted(dj.Computed):
     definition = """
     ->bdata.SessStarted                             # Must `allow null` for attribute `crashed`
     -----
     session_started_rat:               VARCHAR(8)   # Rats name inherited from rats table
     session_started_rigid:             INT(3)       # Rig id number inherited from riginfo table
     session_date='1000-01-01':         DATE         # Date the session started on in yyyy-mm-dd format
     session_starttime='00:00:00':      TIME         # Time session started at
     was_ended=0:                       TINYINT(1)   # 0 if the session has not ended yet, 1 if it has
     crashed:                           TINYINT(3)   # 0 if did not crash, 1 if runrats caught a crash, 2 for all other crashes
     """

     def make(self,key):
          data = (bdata.SessStarted & key).fetch1()
          
          if data['ratname'] != None:
               entry = dict(
                    sessid                   = data['sessid'],
                    session_started_rat      = data['ratname'],
                    session_started_rigid    = data['rigid'],
                    session_date             = data['sessiondate'],
                    session_starttime        = data['starttime'],
                    was_ended                = data['was_ended'],
                    crashed                  = data['crashed']
               )

               if data['rigid'] == 0 and data['hostname'] != None:
                    if 'Rig' in data['hostname']:
                         entry.update(session_started_rigid=int(data['hostname'].strip('Rig')))
                    else:
                         entry.update(session_started_rigid=int(float(data['hostname'])))
                     
               self.insert1(entry)
          else:
               print(data['ratname'])


@schema
class Sessions(dj.Computed):
     definition = """
     ->bdata.Sessions
     -----
     session_rat:                       VARCHAR(8)      # Rat name inherited from rats table
     session_userid:                    VARCHAR(32)     # Rat owner inherited from contacts table
     session_rigid:                     INT(3)          # Rig id number inherited from riginfo table
     session_date='1000-01-01':         DATE            # Date session started on
     session_starttime='00:00:00':      TIME            # Time session started
     session_endtime='00:00:00':        TIME            # Time session ended
     protocol='':                       VARCHAR(30)     # Protocol name
     peh=null:                          MEDIUMBLOB      # Parsed events history state, poke, and wave times on each trial
     n_done_trials=0:                   INT(11)         # Number of trials completed
     session_comments=null:             VARCHAR(1000)   # General comments
     settings_file='':                  VARCHAR(200)    # File containing settings saved at end of session
     settings_path='':                  VARCHAR(200)    # Path to settings file
     data_file='':                      VARCHAR(200)    # File containing data saved at end of session
     data_path='':                      VARCHAR(200)    # Path to data file
     video_file='':                     VARCHAR(200)    # File containing video saved at end of session
     video_path='':                     VARCHAR(200)    # Path to video file
     total_correct=null:                FLOAT(2,2)      # Percent trials correct
     right_correct=null:                FLOAT(2,2)      # Percent right trials correct
     left_correct=null:                 FLOAT(2,2)      # Percent left trials correct
     percent_violations=null:           FLOAT(2,2)      # Percent violation trials
     protocol_data=null:                MEDIUMBLOB      # Data structure containing selection of data from data and settings files
     left_pokes=null:                   INT(10)         # Number of left pokes performed
     center_pokes=null:                 INT(10)         # Number of center pokes performed
     right_pokes=null:                  INT(10)         # Number of right pokes performed
     ip_addr=null:                      VARCHAR(15)     # IP address of rig session ran on
     foodpuck=0:                        TINYINT(1)      # 1 if food was in the rig during the sessions, 0 if not
     """

     def make(self,key):
          fields = bdata.Sessions.heading.names
          fields.remove('protocol_data')
          data = (bdata.Sessions & key).fetch(*fields, as_dict=True)[0]

          try:
               email = (ratinfo.Contacts & 'experimenter="'+ data['experimenter'] + '"').fetch1('email')

               entry = dict(
                    sessid                   = data['sessid'],
                    session_rat              = data['ratname'],
                    session_userid           = email.split('@')[0],
                    session_rigid            = int(data['hostname'].strip('Rig')),
                    session_date             = data['sessiondate'],
                    session_starttime        = data['starttime'],
                    session_endtime          = data['endtime'],
                    protocol                 = data['protocol'],
                    # peh                    = data['peh'], # Unclear where this data is stored
                    n_done_trials            = data['n_done_trials'],
                    session_comments         = data['comments'],
                    settings_file            = data['settings_file'],
                    settings_path            = data['settings_path'],
                    data_file                = data['data_file'],
                    data_path                = data['data_path'],
                    video_file               = data['video_file'],
                    video_path               = data['video_path'],
                    total_correct            = data['total_correct'],
                    right_correct            = data['right_correct'],
                    left_correct             = data['left_correct'],
                    percent_violations       = data['percent_violations'],
                    # protocol_data          = data['protocol_data'], # Error when fetching this field
                    left_pokes               = data['left_pokes'],
                    center_pokes             = data['center_pokes'],
                    right_pokes              = data['right_pokes'],
                    ip_addr                  = data['IP_addr'],
                    foodpuck                 = data['foodpuck']
               )
               self.insert1(entry)
          except:
               print('No valid contact: ', data['experimenter'])

@schema
class AcquisitionSessions(dj.Manual):
     definition = """
     ->Sessions
     -----
     session_rat:                       VARCHAR(8)      # ratname inherited from rats table
     session_userid:                    VARCHAR(32)     # rat owner inherited from contacts table
     session_rigid:                     INT(3)          # rig id number inherited from riginfo table
     acquisition_type:                  VARCHAR(32)     # ephys or imaging
     acquisition_raw_abs_path=null:     VARCHAR(200)    # absoulte path of raw files 
     acquisition_raw_rel_path=null:     VARCHAR(200)    # relative path (from ephys or imaging root dir)
     acquisition_post_abs_path=null:    VARCHAR(200)    # absoulte path of post processing file (clustered/segmented)
     acquisition_post_rel_path=null:    VARCHAR(200)    # relative path (from ephys or imaging  clustering/segmentation root dir)
     """

@schema
class ParsedEvents(dj.Computed):
     definition = """
     ->bdata.Sessions
     -----
     peh:                               mediumblob      # peh data from ParsedEvents transformed by trial events
     """
     def make(self,key):
          dj.blob.use_32bit_dims = True
          parsed_events = (bdata.ParsedEvents & key).fetch(as_dict=True)
          peh = bt.transform_blob(parsed_events[0]['peh'])
          df_trial_peh = bt.blob_peh_to_df(peh, append_original_columnname=True)
          key['peh'] = df_trial_peh.to_dict('records')
          self.insert1(key)

@schema
class SessionProtocolData(dj.Computed):
     definition = """
     ->bdata.Sessions
     -----
     protocol_data:                     mediumblob      # protocol_data from Sessions transformed per trial events
     """
     def make(self,key):
          dj.blob.use_32bit_dims = True
          try:
               protocol_data = (bdata.Sessions & key).fetch('protocol_data', as_dict=True)
               key['protocol_data'] = bt.transform_blob(protocol_data[0]['protocol_data'])
               self.insert1(key)
          except Exception:
                print("Error when inserting {}".format(key))