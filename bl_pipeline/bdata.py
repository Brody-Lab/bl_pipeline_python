import datajoint as dj
import utility.blob_transformation as bt

schema = dj.schema('bdata')

bdata = dj.create_virtual_module('bdata', 'bdata')


@schema
class Sessions(dj.Manual):
     definition = """
     sessid:                  INT(11)                   # Unique number for session                
     -----
     ratname:  varchar(30)
     hostname:  varchar(30)
     experimenter:  varchar(30)
     starttime:  time
     endtime:  time
     sessiondate:  date
     n_done_trials:  int(11)
     comments:  varchar(1000)
     settings_file:  varchar(200)
     settings_path:  varchar(200)
     data_file:  varchar(200)
     data_path:  varchar(200)
     video_file:  varchar(200)
     video_path:  varchar(200)
     protocol:  varchar(30)
     total_correct:  float(2,2)
     right_correct:  float(2,2)
     left_correct:  float(2,2)
     percent_violations:  float(2,2)
     brokenbits:  int(11)
     protocol_data:  mediumblob
     left_pokes:  int(10)
     center_pokes:  int(10)
     right_pokes:  int(10)
     technotes:  varchar(200)
     IP_addr:  varchar(15)
     crashed:  tinyint(1)
     foodpuck:  tinyint(1)
     """

@schema
class BehaviorEvent(dj.Imported):
     definition = """
     ->bdata.Sessions
     id_event:               INT(11)                    # Unique number for event                      
     -----
     trial:                  INT(10)                    # trial number in session
     event_type:             VARCHAR(16)                # type of event in session (e.g. pokes, states)
     event_name:             VARCHAR(32)                # sub category of event type (e.g. C, L, R, state0)
     entry_num:              INT(10)                    # occurence number of event inside trial
     in_time=null:           DOUBLE                     # start time of event
     out_time=null:          DOUBLE                     # end time of event
     """

     @property
     def key_source(self):
          return super().key_source & (bdata.Sessions & 'sessid > 904000')

     def make(self,key):

          print(key)

          parsed_events = (bdata.ParsedEvents & key).fetch(as_dict=True)
          peh = bt.transform_blob(parsed_events[0]['peh'])
          df_trial_peh = bt.blob_peh_to_df(peh, append_original_columnname=True)
          df_event = bt.peh_trial_df_to_event_df(df_trial_peh, key['sessid'])

          dict_event = df_event.to_dict('records')

          self.insert(dict_event)



#sessid:                  INT(11)                    # Unique number for session   