import datajoint as dj
import datetime


bdata   = dj.create_virtual_module('bdata', 'bdatatest')
ratinfo = dj.create_virtual_module('ratinfo', 'ratinfotest')

# create shadow schema
schema = dj.schema('bl_shadow_action')


@schema
class CalibrationInfoTbl(dj.Computed):
     definition = """
     -> bdata.CalibrationInfoTbl.proj(calibration_info_tbl_id='calibrationid')            
     -----
     rigid:                              INT(3)
     calibration_datetime:               DATETIME
     open_valve_time:                    DOUBLE
     calibration_person:                 VARCHAR(32)
     valve:                              VARCHAR(15)
     dispense:                           DOUBLE                  # amount dispensed
     isvalid:                            TINYINT(1)
     target:                             VARCHAR(10)
     """
     
     def make(self,key):
          print(key)
          key_shadow = dict(calibrationid=key['calibration_info_tbl_id'])
          data = (bdata.CalibrationInfoTbl & key_shadow).fetch1()
          
          try:
               int(data['rig_id'])

               entry = dict(
                    calibration_info_tbl_id  = data['calibrationid'],
                    rigid                    = data['rig_id'],
                    calibration_datetime     = data['dateval'],
                    open_valve_time          = data['timeval'],
                    calibration_person       = data['initials'],
                    valve                    = data['valve'],
                    dispense                 = data['dispense'],
                    isvalid                  = data['isvalid'],
                    target                   = data['target']
               )
               self.insert1(entry)       
          except Exception as e: 
               print(e)   

@schema
class Mass(dj.Computed):
     definition = """
     -> ratinfo.Mass.proj(mass_id='weighing')
     -----
     ratname:                      VARCHAR(8)
     weigh_person:                 VARCHAR(32)                   # renamed tech (initials) with weigh_person
     weighing_datetime:            DATETIME
     mass=null:                    SMALLINT(5)
     """
     
     def make(self,key):
          key_shadow = dict(weighing=key['mass_id'])
          data = (ratinfo.Mass & key_shadow).fetch1()

          if data['date'] != '0000-00-00':
               try:
                    data_email = (ratinfo.Contacts & {'initials': data['tech']}).fetch('email')
                    if len(data_email) == 1:
                         data_email = data_email[0]
                         data_email = data_email.split('@')[0]

                         weighing_datetime = datetime.datetime.combine(data['date'],(datetime.datetime.min + data['timeval']).time())

                         entry = dict(
                              mass_id                  = data['weighing'],
                              ratname                  = data['ratname'],
                              weigh_person             = data_email,
                              weighing_datetime        = weighing_datetime,
                              mass                     = data['mass']
                         )

                         self.insert1(entry)
               except Exception as e: 
                    print(e)

@schema
class Rigwater(dj.Computed):
     definition = """
     -> ratinfo.Rigwater.proj(rigwater_id='id')
     -----
     ratname:                         VARCHAR(8)
     earnedwater_datetime:            DATETIME                     
     totalvol:                        DECIMAL(10,5) 
     trialvol:                        DECIMAL(10,5) 
     complete:                        TINYINT(3) 
     n_rewarded_trials:               INT(5) 
     target_percent=null:             DECIMAL(5,2) 
     """
     
     def make(self,key):
          key_shadow = dict(id=key['rigwater_id'])
          data = (ratinfo.Rigwater & key_shadow).fetch1()

          try:
               str_date = data['dateval'].strftime("%Y-%m-%d")
               if data['dateval'] != '0000-00-00':
                    entry = dict(
                         rigwater_id              = data['id'],
                         ratname                  = data['ratname'],
                         earnedwater_datetime     = data['dateval'],
                         totalvol                 = data['totalvol'],
                         trialvol                 = data['trialvol'],
                         complete                 = data['complete'],
                         n_rewarded_trials        = data['n_rewarded_trials'],
                         target_percent           = data['target_percent']
                    )

                    self.insert1(entry)
          except Exception as e: 
               print(e)

@schema
class Schedule(dj.Computed):
     definition = """
     -> ratinfo.Schedule.proj(schedule_id='schedentryid')      
     -----
     ratname=null:                    VARCHAR(30)
     experimenter=null:               VARCHAR(30)
     schedule_date:                   DATE
     schedule_timeslot:               TINYINT
     rig:                             INT(11)
     instructions=null:               VARCHAR(250)       # 'Special instructions for the technicians'
     schedule_comments=null:          VARCHAR(250)       # Comments seen by experimenters, i.e. slot reserved for..., this is a special rig 6-poke, opto, phys...
     """
     
     def make(self,key):
          key_shadow = dict(schedentryid=key['schedule_id'])
          data = (ratinfo.Schedule & key_shadow).fetch1()

          if data['date'] != '0000-00-00':
               entry = dict(
                    schedule_id            = data['schedentryid'],
                    ratname                = data['ratname'],
                    experimenter           = data['experimenter'],
                    schedule_date          = data['date'],
                    schedule_timeslot      = data['timeslot'],
                    rig                    = data['rig'],
                    instructions           = data['instructions'],
                    schedule_comments      = data['comments']
               )

               self.insert1(entry)
         

@schema
class Surgery(dj.Computed):
     definition = """
     -> ratinfo.Surgery.proj(surgery_old_id='id')
     -----
     ratname:                            VARCHAR(8)
     surgery_date:                       DATE
     surgery_starttime:                  TIME
     surgery_stoptime=null:              TIME
     surgeon:                            VARCHAR(32)
     ratgrams=null:                      INT(11) 
     surgery_type=null:                  VARCHAR(100)         # Electrode, cannula, lesion
     eib_num=null:                       VARCHAR(20) 
     coordinates=null:                   VARCHAR(40) 
     brainregions:                       VARCHAR(40) 
     ketamine=null:                      FLOAT                # how many CC's of ketamine used
     buprenex=null:                      FLOAT                # how many CC's of buprenex used
     surgery_notes=null:                 VARCHAR(600) 
     bregma=null:                        TINYBLOB             # AP, ML, DV  tinyblob for storage as vector (should we store it as three separate fields? ) 
     ia_zero=null:                       TINYBLOB             # AP, ML, DV  tinyblob for storage as vector (should we store it as three separate fields? )
     angle=0:                            FLOAT
     tilt_axis=null:                     ENUM('AP', 'ML')
     """
     
     def make(self,key):
          key_shadow = dict(id=key['surgery_old_id'])
          data = (ratinfo.Surgery & key_shadow).fetch1()

          if data['date'] != '0000-00-00':
               entry = dict(
                    ratname            = data['ratname'],
                    surgery_date       = data['date'],
                    surgery_starttime  = data['starttime'],
                    surgery_stoptime   = data['endtime'],
                    surgeon            = data['surgeon'],
                    surgery_old_id     = data['id'],
                    ratgrams           = data['ratgrams'],
                    surgery_type       = data['type'],
                    eib_num            = data['eib_num'],
                    coordinates        = data['coordinates'],
                    brainregions       = data['brainregions'],
                    ketamine           = data['ketamine'],
                    buprenex           = data['buprenex'],
                    surgery_notes      = data['notes'],
                    bregma             = data['Bregma'],
                    ia_zero            = data['IA0'],
                    angle              = data['angle'],
                    tilt_axis          = data['tilt_axis']
               )

               self.insert1(entry)


@schema
class TechSchedule(dj.Computed):
     definition = """
     -> ratinfo.TechSchedule
     -----
     techschedule_date:               DATE
     day:                             VARCHAR(45)
     overnight:                       VARCHAR(200)
     morning:                         VARCHAR(200)
     evening:                         VARCHAR(200)
     """
     
     def make(self,key):
          data = (ratinfo.TechSchedule & key).fetch1()
          
          if data['date'] != '0000-00-00':
               entry = dict(
                    scheduleid          = data['scheduleid'],
                    techschedule_date   = data['date'],
                    day                 = data['day'],
                    overnight           = data['overnight'],
                    morning             = data['morning'],
                    evening             = data['evening']
               )

               self.insert1(entry)

@schema
class Technotes(dj.Computed):
     definition = """
     -> ratinfo.Technotes.proj(technote_id='technoteid')
     -----
     technote_datetime:                      DATETIME
     technician=null:                        VARCHAR(32) 
     technote_rat=null:                      VARCHAR(8)       # definition only in python
     technote_rig=null:                      INT(3)           # definition only in python
     technote_experimenter=null:             VARCHAR(32)      # definition only in python, individual in the lab to receive the technote
     timeslot=null:                          TINYINT(1)
     technotes_note:                         VARCHAR(800)
     flag=null:                              INT(3)
     """
     
     def make(self,key):
          key_shadow = dict(technoteid=key['technote_id'])
          data = (ratinfo.Technotes & key_shadow).fetch1()
          
          technote_datetime = datetime.datetime.combine(data['datestr'],(datetime.datetime.min + data['timestr']).time())

          entry = dict(
               technote_id              = data['technoteid'],
               technote_datetime        = technote_datetime,
               technician               = data['techinitials'],
               technote_rat             = data['ratname'],
               technote_rig             = data['rigid'],
               technote_experimenter    = data['experimenter'],
               timeslot                 = data['timeslot'],
               technotes_note           = data['note'],
               flag                     = data['flag']
          )

          self.insert1(entry)
  

@schema
class Water(dj.Computed):
     definition = """
     -> ratinfo.Water.proj(water_id='watering')
     -----
     ratname:                          VARCHAR(8)
     administration_date:              DATE
     administration_starttime:         TIME
     administration_stoptime=null:     TIME 
     administration_person:            VARCHAR(32)
     volume:                           DECIMAL(6,3) 
     percent_bodymass:                 DECIMAL(6,3) 
     percent_target:                   DECIMAL(6,3)      
     """
               
     def make(self,key):
          key_shadow = dict(watering=key['water_id'])
          data = (ratinfo.Water & key_shadow).fetch1()

          try:
               str_date = data['date'].strftime("%Y-%m-%d")
               #datetime.datetime.strptime(data['date'], "%Y-%m-%d")
               
               entry = dict(
                    water_id                 = data['watering'],
                    ratname                  = data['rat'],
                    administration_date      = data['date'],
                    administration_starttime = data['starttime'],
                    administration_stoptime  = data['stoptime'],
                    administration_person    = data['tech'],
                    volume                   = data['volume'],
                    percent_bodymass         = data['percent_bodymass'],
                    percent_target           = data['percent_target']
               )

               self.insert1(entry)
          
          except Exception as e: 
               print(e)

 