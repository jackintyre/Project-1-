import requests
import json
from config import key
import matplotlib.pyplot as plt
import pandas as pd
import datetime
from datetime import date
from datetime import datetime
from scipy.stats import linregress
import numpy as np
from scipy.stats import linregress
from collections import Counter 
from datetime import datetime, timedelta

        #get current local date
starttime = date.today()
        #convert local date to datetime
start_datetime = datetime(starttime.year, starttime.month, starttime.day)
        #change date to 1 week later for end datetime
end_datetime = start_datetime + timedelta(7)

        #convert start_datetime to epoch date
startstamp = start_datetime.timestamp()
        #convert end_datetime to epoch date
endstamp= end_datetime.timestamp()

#url ='https://ufp5x5qk2i.execute-api.us-east-1.amazonaws.com/prod/eventmanager/events?starttime=1617768000.001&endtime=1618372800'

url ='https://ufp5x5qk2i.execute-api.us-east-1.amazonaws.com/prod/eventmanager/events?starttime='+str(startstamp)+'&endtime='+str(endstamp)

        #headers pulls api key from config.py, assignes it to x-api-key
headers = {"x-api-key": key}

        #assemble request, pulls x-api-key as header to access api
req = requests.get(url, headers=headers)

req

        #Create and clean dataframe from API call line 41 to 111
data = req.json()
data.keys()

newDict={}
print(data['count'])
newCount=0
#filter out the passthrough records
Counts=data['count']
#first look for passthrough = True
for index in range(1,Counts):
    if data['events'][index]['is_passthrough']==False:
        newDict=data
        newCount= newCount+1
    
#print(newDict)    
#print(f'filtered data counts {newCount}')

ID=[]
Type=[]
Scheduled=[]
Status=[]
CType=[]
Pass=[]
Start=[]
End=[]
Event_Title=[]
School_Name=[]
School_Code=[]
Game=[]
PubPoint=[]


for index in range(1,newCount):
    try:
        ID.append(newDict['events'][index]['prismid'])
        Type.append(newDict['events'][index]['eventtype'])
        Scheduled.append(newDict['events'][index]['eventstate'])
        Status.append(newDict['events'][index]['eventstatus'])
        #CType.append(newDict['events'][index]['contenttype'])
        Pass.append(newDict['events'][index]['is_passthrough'])
        Start.append(newDict['events'][index]['starttime'])
        End.append(newDict['events'][index]['endtime'])
        Event_Title.append(newDict['events'][index]['eventtitle'])
        School_Name.append(newDict['events'][index]['school_name'])
        School_Code.append(newDict['events'][index]['school'])
        Game.append(newDict['events'][index]['sport_name'])
        #PubPoint.append(newDict['events'][index]['ingest']['primary']['pub_point'])
    except ValueError:
        continue
    except KeyError:
        print(index)
        continue
             
        
  
    
event_df=pd.DataFrame(ID)
event_df['Event type']=Type
event_df['Scheduled']=Scheduled
event_df['Is Live']=Status
#event_df['Content Type']=CType
event_df['PassThru']=Pass
event_df['Start Time']=Start
event_df['End Time']=End
event_df['Event']=Event_Title
event_df['School Name']=School_Name
event_df['School Code']=School_Code
event_df['Sport']=Game
#event_df['Access Point']=PubPoint
event_df.rename(columns={0:'ID'},inplace=True)
event_df.set_index('ID',inplace=True)

        #Weekly and daily breakdown lines 114 to 221
import datetime
count=[]
x=0
        #read in start and end epoch times convert to integer and append to new list
Start3=list(map(int, Start))   
End3=list(map(int, End))
        #convert epoch times to datetime for readability
y=len(Start)
for x in range(len(Start)):
    Start3[x]=datetime.datetime.fromtimestamp(Start3[x])
    End3[x]=datetime.datetime.fromtimestamp(End3[x])
            #counter for Index ID in dataframe
    count.append(x)
        #create new dataframe for event start and end datetimes
active_runtime_df= pd.DataFrame(count)  
Start2=list(map(int, Start))   
End2=list(map(int, End))
active_runtime_df['epochStart']= pd.DataFrame(Start2)
active_runtime_df['epochEnd']= pd.DataFrame(End2)
    #active_runtime_df['Start']= datetime.datetime.fromtimestamp(Start)  
active_runtime_df['Start']= pd.DataFrame(Start3)
    #active_runtime_df['End']= datetime.datetime.fromtimestamp(End)   
active_runtime_df['End']= pd.DataFrame(End3)

        #sort datafrane by earliest Start
active_runtime_df = active_runtime_df.sort_values(by="Start")
#active_runtime_df

import pandas as pd
from io import StringIO
        #split start and end datetimes to Date and Time columns
active_runtime_df['Start Date'], active_runtime_df['Start Time'] = active_runtime_df['Start'].dt.date, active_runtime_df['Start'].dt.time
active_runtime_df['End Date'], active_runtime_df['End Time'] = active_runtime_df['End'].dt.date, active_runtime_df['End'].dt.time

#print(active_runtime_df)

  #Weekly break down of how many events per day
dates=[]
dates=active_runtime_df['Start Date'].unique()
daycount= active_runtime_df.pivot_table(index = ['Start Date'], aggfunc ='size')
#daycount
#rough count of events per day, not including events that run into next day

 #create bar graph of weekly breakdown
x_axis =dates
plt.figure(figsize=(10,4))
plt.title('Weekly Event Schedule')
plt.ylabel('Event Count')
plt.xlabel('Dates "YYYY-MM-DD"')
plt.bar(x_axis, daycount, color='g', alpha=.5, align="center")

import datetime

        #pull in user specific date to get daily breakdown by hour of day
date_entry= input('Enter a date in YYYY-MM-DD format ')
        #convert user date into datetime
year, month, day = map(int, date_entry.split('-'))
date1 = datetime.date(year, month, day)
#print(date1)
        #Convert datetime to epoch timestamp
from datetime import datetime
datetime1 = datetime(date1.year, date1.month, date1.day)
epoch_datetime = datetime1.timestamp()

import datetime
count=[]
timepoints=[]
i=0

x=int(epoch_datetime)
q=int(epoch_datetime+86400)
#print(x)
j=0
            #compare each starttime in epoch to current epoch hour of given day
            #add to count if between starttime and endtime for each row that applies
            #append that count to hourly list and interate to next hour of the day
for i in range(24):
    timepoints.append(i+1)
    b=0
    for index, row in active_runtime_df.iterrows():
        date2=row['Start Date']
        if date2==date1:
               
            starttime1=row['epochStart']
            endtime1=row['epochEnd']
            begin=int(starttime1)
            ending=int(endtime1)
            if x>=row['epochStart'] and x<=row['epochEnd']:
                b+=1
                #print(b)
    count.append(b)
    x=x+3600
    #print(x)
#print(count[20])

import matplotlib.pyplot as plt
import datetime
import matplotlib.dates as mdates

        #create line graph of daily breakdown by hour 
        #starting from 12am to 1am on selected day and ending at 12am the following day
        #Does not include events that started the day before
plt.figure(figsize=(10,4))
plt.xticks(timepoints)
plt.title('Hourly Event Schedule '+str(date1))
plt.ylabel('Event Count')
plt.xlabel('Hour of Day')
plt.plot(timepoints, count, color='blue', marker='o')

plt.show()

               