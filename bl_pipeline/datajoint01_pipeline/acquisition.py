import datajoint as dj
import utility.blob_transformation as bt
import pandas as pd
from bl_pipeline.datajoint01_pipeline  import lab, subject


# create new schema
schema = dj.schema('bl_new_acquisition')

@schema
class SessStarted(dj.Manual):
     definition = """
     sessid:                            INT(11)        # Unique number assigned to each training session
     -----
     ->subject.Rats.proj(session_started_rat='ratname')# rats name inherited from rats table
     ->lab.Riginfo.proj(session_started_rigid='rigid') # rig id number inherited from riginfo table
     session_date='1000-01-01':         DATE           # date the session started on in yyyy-mm-dd format
     session_starttime='00:00:00':      TIME           # time session started at
     was_ended=0:                       TINYINT(1)     # 0 if the session has not ended yet, 1 if it has
     crashed:                           TINYINT(3)     # 0 if did not crash, 1 if runrats caught a crash, 2 for all other crashes
     """


@schema
class Sessions(dj.Manual):
     definition = """
     ->SessStarted
     -----
     ->subject.Rats.proj(session_rat='ratname')         # rats name inherited from rats table
     ->lab.Contacts.proj(session_userid='user_id')      # rat owner inherited from contacts table
     ->lab.Riginfo.proj(session_rigid='rigid')          # rig id number inherited from riginfo table
     session_date='1000-01-01':         DATE            # date session started on
     session_starttime='00:00:00':      TIME            # time session started
     session_endtime='00:00:00':        TIME            # time session ended
     protocol='':                       VARCHAR(30)     # protocol name
     peh=null:                          MEDIUMBLOB      # parsed events history state, poke, and wave times on each trial
     n_done_trials=0:                   INT(11)         # number of trials completed
     session_comments=null:             VARCHAR(1000)   # general comments
     settings_file='':                  VARCHAR(200)    # file containing settings saved at end of session
     settings_path='':                  VARCHAR(200)    # path to settings file
     data_file='':                      VARCHAR(200)    # file containing data saved at end of session
     data_path='':                      VARCHAR(200)    # path to data file
     video_file='':                     VARCHAR(200)    # file containing video saved at end of session
     video_path='':                     VARCHAR(200)    # path to video file
     total_correct=null:                FLOAT(2,2)      # percent trials correct
     right_correct=null:                FLOAT(2,2)      # percent right trials correct
     left_correct=null:                 FLOAT(2,2)      # percent left trials correct
     percent_violations=null:           FLOAT(2,2)      # percent violation trials
     protocol_data=null:                MEDIUMBLOB      # data structure containing selection of data from data and settings files
     left_pokes=null:                   INT(10)         # number of left pokes performed
     center_pokes=null:                 INT(10)         # number of center pokes performed
     right_pokes=null:                  INT(10)         # number of right pokes performed
     ip_addr=null:                      VARCHAR(15)     # IP address of rig session ran on
     foodpuck=0:                        TINYINT(1)      # 1 if food was in the rig during the sessions, 0 if not
     """

@schema
class DataFrameTest(dj.Manual):
     definition = """
     ->SessStarted
     -----
     dataframe_peh=null:                          MEDIUMBLOB      # parsed events history state, poke, and wave times on each trial
     """


@schema
class Sessions2(dj.Manual):
     definition = """
     ->SessStarted
     -----
     ->subject.Rats.proj(session_rat='ratname')         # rats name inherited from rats table
     ->lab.Contacts.proj(session_userid='user_id')      # rat owner inherited from contacts table
     ->lab.Riginfo.proj(session_rigid='rigid')          # rig id number inherited from riginfo table
     session_date='1000-01-01':         DATE            # date session started on
     session_starttime='00:00:00':      TIME            # time session started
     session_endtime='00:00:00':        TIME            # time session ended
     protocol='':                       VARCHAR(30)     # protocol name
     peh=null:                          blob@sessions_blobs     # parsed events history state, poke, and wave times on each trial
     n_done_trials=0:                   INT(11)         # number of trials completed
     session_comments=null:             VARCHAR(1000)   # general comments
     settings_file='':                  VARCHAR(200)    # file containing settings saved at end of session
     settings_path='':                  VARCHAR(200)    # path to settings file
     data_file='':                      VARCHAR(200)    # file containing data saved at end of session
     data_path='':                      VARCHAR(200)    # path to data file
     video_file='':                     VARCHAR(200)    # file containing video saved at end of session
     video_path='':                     VARCHAR(200)    # path to video file
     total_correct=null:                FLOAT(2,2)      # percent trials correct
     right_correct=null:                FLOAT(2,2)      # percent right trials correct
     left_correct=null:                 FLOAT(2,2)      # percent left trials correct
     percent_violations=null:           FLOAT(2,2)      # percent violation trials
     protocol_data=null:                blob@sessions_blobs      # data structure containing selection of data from data and settings files
     left_pokes=null:                   INT(10)         # number of left pokes performed
     center_pokes=null:                 INT(10)         # number of center pokes performed
     right_pokes=null:                  INT(10)         # number of right pokes performed
     ip_addr=null:                      VARCHAR(15)     # IP address of rig session ran on
     foodpuck=0:                        TINYINT(1)      # 1 if food was in the rig during the sessions, 0 if not
     """

@schema
class PreAcquisitionSessions(dj.Manual):
     definition = """
     ->Sessions
     directory_num                      INT(3) 
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
class AcquisitionSessions(dj.Manual):
     definition = """
     ->Sessions
     -----
     session_rat:                       VARCHAR(8)      # ratname inherited from rats table
     session_userid:                    VARCHAR(32)     # rat owner inherited from contacts table
     session_rigid:                     INT(3)          # rig id number inherited from riginfo table
     acquisition_type:                  VARCHAR(32)     # ephys or imaging
     acquisition_raw_rel_path=null:    VARCHAR(200)     # absoulte path of raw files 
     acquisition_post_rel_path=null:    VARCHAR(200)    # relative path (from ephys or imaging  clustering/segmentation root dir)
     """

@schema
class Acquisitions(dj.Manual):
     definition = """
     acquisition_id:                   INT(11) AUTO_INCREMENT    # Unique number assigned to each acquisition on lab (ephys, imaging, video)          
     -----
     -> [nullable] SessStarted.proj(acquisition_sessid='sessid') # sessid inherited from SessStarted
     -> subject.Rats.proj(acquisition_rat='ratname')             # rat inherited from rats table
     -> lab.Contacts                                             # rat owner inherited from contacts table
     acquisition_type:                  VARCHAR(32)              # ephys or imaging
     acquisition_raw_rel_path=null:    VARCHAR(200)              # absoulte path of raw files 
     """    

@schema
class Sortings(dj.Manual):
     definition = """
     sorting_id:                       INT(11) AUTO_INCREMENT    # Unique number assigned to each sorting done in the lab (ephys, imaging, video)          
     -----
     ->Acquisitions                                              # acquisition id (id to raw path of acquisition)
     acquisition_post_rel_path=null:    VARCHAR(200)             # relative path (from ephys or imaging  clustering/segmentation root dir)
     """        

#Status pipeline dictionary
status_pipeline_dict = {
    'ERROR':             {'Value': -1,
                         'Label': 'Error in process',
                         'Task_Field': None},
    'NEW_SESSION':       {'Value': 0,
                         'Label': 'New session',
                         'Task_Field': None},
    'RAW_FILE_REQUEST':  {'Value': 1,
                          'Label': 'Raw file transfer requested',
                          'Task_Field': 'task_copy_id_pre_path'},
    'RAW_FILE_CLUSTER':  {'Value': 2,
                         'Label': 'Raw file transferred to cluster',
                         'Task_Field': None},
    'JOB_QUEUE':         {'Value': 3,
                         'Label': 'Processing job in queue',
                         'Task_Field': 'slurm_id_sorting'},
    'JOB_FINISHED':      {'Value': 4,
                         'Label': 'Processing job finished',
                         'Task_Field': None},
    'PROC_FILE_REQUEST': {'Value': 5,
                         'Label': 'Processed file transfer requested',
                         'Task_Field': 'task_copy_id_pos_path'},
    'PROC_FILE_HOME':    {'Value': 6,
                         'Label': 'Processed file transferred to PNI',
                         'Task_Field': None},
    'CANONICAL_PIPELINE': {'Value': 7,
                         'Label': 'Processed with Canonical pipeline',
                         'Task_Field': None},
}

def get_content_list_from_status_dict():
    contents = list()
    for i in status_pipeline_dict.keys():    
        contents.append([status_pipeline_dict[i]['Value'], status_pipeline_dict[i]['Label']])
    return contents


@schema
class StatusDefinition(dj.Lookup):
     definition = """
     status_pipeline:                    TINYINT(1)      # status in the ephys/imaging pipeline
     ---
     status_definition:                  VARCHAR(256)    # Status definition 
     """
     contents = get_content_list_from_status_dict()


@schema
class AcquisitionSessionsTestAutoPipeline(dj.Manual):
     definition = """
     ->Sessions
     -----
     ->StatusDefinition                                # current status in the ephys pipeline
     session_rat:                       VARCHAR(8)      # ratname inherited from rats table
     session_userid:                    VARCHAR(32)     # rat owner inherited from contacts table
     session_rigid:                     INT(3)          # rig id number inherited from riginfo table
     acquisition_type:                  VARCHAR(32)     # ephys or imaging
     acquisition_raw_rel_path=null:     VARCHAR(200)    # relative path of raw files 
     acquisition_post_rel_path=null:    VARCHAR(200)    # relative path for sorted files
     task_copy_id_pre_path=null:        UUID            # id for globus transfer task raw file cup->tiger  
     task_copy_id_pos_path=null:        UUID            # id for globus transfer task sorted file tiger->cup 
     slurm_id_sorting=null:             VARCHAR(16)     # id for slurm process in tiger
     """    

@schema
class AcquisitionSessionsStatus(dj.Manual):
     definition = """
     ->AcquisitionSessions
     -----
     -> StatusDefinition.proj(status_pipeline_old='status_pipeline') # old status in the ephys pipeline
     -> StatusDefinition.proj(status_pipeline_new='status_pipeline') # current status in the ephys pipeline
     status_timestamp:                  DATETIME        # timestamp when status change ocurred
     error_message=null:                VARCHAR(4096)   # Error message if status now is failed
     error_exception=null:              BLOB            # Error exception if status is failed
     """

@schema
class ParsedEvents(dj.Manual):
     definition = """
     ->Sessions
     -----
     peh:                               mediumblob      # ratname inherited from rats table
     """

     @classmethod
     def load_behavior_event_from_parsed_events(cls,query):

          parsed_events = (cls & query).fetch(as_dict=True)

          num_sessions = 0
          for parsed_event_session in parsed_events:
               df_parsed_event = pd.DataFrame(parsed_event_session['peh'])
               df_event = bt.peh_trial_df_to_event_df(df_parsed_event, parsed_event_session['sessid'])
               if num_sessions == 0:
                    df_behavior_event = df_event.copy()
               else:
                    df_behavior_event = pd.concat([df_behavior_event, df_event])
               num_sessions = num_sessions + 1

          return df_behavior_event
          
@schema
class SessionProtocolData(dj.Manual):
     definition = """
     ->Sessions
     -----
     protocol_data:                     mediumblob      # protocol_data from Sessions transformed per trial events
     """

@schema
class BehaviorEvent(dj.Computed):
     definition = """
     ->ParsedEvents
     id_event:                          INT(11)        # Unique number for event              
     -----
     trial:                             INT(10)        # trial number in session
     event_type:                        VARCHAR(16)    # type of event in session (e.g. pokes, states)
     event_name:                        VARCHAR(32)    # sub category of event type (e.g. C, L, R, state0)
     entry_num:                         INT(10)        # occurence number of event inside trial
     in_time=null:                      DOUBLE         # start time of event
     out_time=null:                     DOUBLE         # end time of event
     """
     def make(self,key):

          peh = (ParsedEvents & key).fetch('peh', as_dict=True)
          df_event = bt.peh_trial_df_to_event_df(pd.DataFrame(peh[0]['peh']), key['sessid'])
          dict_event = df_event.to_dict(orient='records')
          self.insert(dict_event)