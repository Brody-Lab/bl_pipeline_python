import datajoint as dj


bdata   = dj.create_virtual_module('bdata', 'bdatatest')

'''
# create new schema
schema = dj.schema('bl_shadow_behavior')

@schema
class BehaviorEvent(dj.Computed):
     definition = """
     ->bdata.Sessions                                  # Unique number assigned to each training session
     id_event:                          INT(11)        # Unique number for event              
     -----
     trial:                             INT(10)        # trial number in session
     event_type:                        VARCHAR(16)    # type of event in session (e.g. pokes, states)
     event_name:                        VARCHAR(32)    # sub category of event type (e.g. C, L, R, state0)
     entry_num:                         INT(10)        # occurence number of event inside trial
     in_time:                           DOUBLE         # start time of event
     out_time:                          DOUBLE         # end time of event
     """
'''