#!/usr/bin/env python
# coding: utf-8

# # AC52048: ETL Project
# 

# In[1]:


#Extracting outputlong and outputshort from log files using given python code. 

import os

#If we not us delete files it will cause errors and keep on re reading old files. 
def DeleteFiles():
    OutputFileShort=open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/Outputshort.txt', 'w')
    OutputFileLong=open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/Outputlong.txt', 'w')

    
def CleanHash(filename):
    
    #Helps in checking wether it is correct log file 
    
    type=filename[-3:len(filename)]
    if (type=="log"):
    
        OutputFileShort=open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/Outputshort.txt', 'a')
        OutputFileLong=open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/Outputlong.txt', 'a')

        InFile = open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/'+filename,'r')
        print(filename)
    
        Lines= InFile.readlines()
        for line in Lines:
            if (line[0]!="#"):
                Split=line.split(" ")
                
 #if the number of column is 14 it is outputshort  

                if (len(Split)==14):
                   
                   OutputFileShort.write(line)
                else:
#if the number of column is 18 it is outputlong

                   if (len(Split)==18):
                       OutputFileLong.write(line)
                   else:
                       print ("Fault "+str(len(Split)))
                
                

arr=os.listdir("C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/")
DeleteFiles()
for f in arr:
    CleanHash(f)


# In[2]:


#Extracting Outfact from outputshort and outputlong using given python code.
    

def BuildFactShort():
    InFile = open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/Outputshort.txt','r')
    OutFact1=open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/OutFact1.txt', 'a')

    Lines= InFile.readlines()
    for line in Lines:
        Split=line.split(" ")
        Browser=Split[9].replace(","",")

#splitting date, time, IP, Browser, Response time        
        Out=Split[0]+","+Split[1]+","+Browser+","+Split[8]+","+Split[13]

        OutFact1.write(Out)
def BuildFactLong():
    InFile = open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/Outputlong.txt','r')
    OutFact1=open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/OutFact1.txt', 'a')

    Lines= InFile.readlines()
    for line in Lines:
        Split=line.split(" ")
        Browser=Split[9].replace(",","")
        Out=Split[0]+","+Split[1]+","+Browser+","+Split[8]+","+Split[16]
        OutFact1.write(Out)

with open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/OutFact1.txt', 'w') as file:
    file.write("Date,Time,Browser,IP,ResponseTime\n")
BuildFactShort()
BuildFactLong()


# In[3]:


#Extracting DimIP and outfact1 from log files using given python code.

InFile = open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/OutFact1.txt', 'r')
OutputFile=open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/DimIP.txt', 'w')

Lines= InFile.readlines()
for line in Lines:
    
    Split=line.split(",")
    
#Extracting IP column from OutFact1
    Out=Split[3]+"\n"
    OutputFile.write(Out)


# In[4]:


import pandas as pd 


# In[5]:


df = pd.read_csv('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/DimIP.txt')


# In[6]:


df.head()


# In[7]:


df.to_csv('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/DimIPUniq.txt',index=False)


# In[8]:


#Extracting location information from unique IP addresses 
import requests
import json

InFile=open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/DimIPUniq.txt', 'r')
OutFile=open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/DimIPLoc.txt', 'w')


Lines= InFile.readlines()
for line in Lines:
    line=line.replace("\n","")
    # URL to send the request to
    request_url = 'https://geolocation-db.com/jsonp/' + line
    print (request_url)
    # Send request and decode the result
    response = requests.get(request_url)
    result = response.content.decode()
    
# Clean the returned string so it just contains the dictionary data for the IP address
    result = result.split("(")[1].strip(")")
    
# Convert this data into a dictionary
    result  = json.loads(result)
    out=line+","+str(result["country_code"])+","+str(result["country_name"])+","+str(result["city"])+","+str(result["latitude"])+","+str(result["longitude"])+"\n"
    print(out)
    with open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/DimIPLoc.txt', 'a',encoding='utf-8') as file:
       file.write(out)
    


# In[9]:


#Getting date column from outfact1

InFile = open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/OutFact1.txt', 'r')
OutputFile=open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/DimDate.txt', 'w')

Lines= InFile.readlines()
for line in Lines:
    Split=line.split(",")
    Out=Split[0]+"\n"
    OutputFile.write(Out)


# In[10]:


#Reads the data 

date=pd.read_csv('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/DimDate.txt')


# In[11]:


date.head()


# In[12]:


#Removes duplicate entries from date file 
date=date.drop_duplicates(keep='last')


# In[13]:


date.head()


# In[14]:


date.to_csv('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/DimDateUniq.txt',index=False)


# In[15]:


#Extracting date information using python date method. 

from datetime import datetime

InFile = open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/DimDateUniq.txt', 'r')
OutputFile=open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/DimDateTable.txt', 'w')
Days=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
with open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/DimDateTable.txt', 'w') as file:
    file.write("Date,Year,Month,Day,DayofWeek\n")
Lines= InFile.readlines()[1:]
for line in Lines:
    line=line.replace("\n","")
    date=datetime.strptime(line,"%Y-%m-%d").date()
    
    
    weekday=Days[date.weekday()]
    out=str(date)+","+str(date.year)+","+str(date.month)+","+str(date.day)+","+weekday+"\n"
    print(out)
    with open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/DimDateTable.txt', 'a') as file:
       file.write(out)


# In[14]:


#Extracting Errors (status and substatus) from log files
InFile = open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/AllLogs.txt', 'r')
OutputFile=open('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/Errors.txt', 'w')

Lines= InFile.readlines()
for line in Lines:
    Split=line.split(",")
    Out=Split[13]+" "+Split[14]+"\n"
    OutputFile.write(Out)


# In[15]:


ip=pd.read_csv('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/Errors.txt', sep=' ')


# In[16]:


ip.head() 


# In[20]:


#Extracting user sessions from DimIPLoc files. 

import pandas as pd

# Path to the log file containing IP locations details
log_file_path = 'C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/DimIPLoc.txt'

# Create an empty dictionary to store the country counts
country_counts = {}

# Open the log file
with open(log_file_path, 'r', encoding='utf-8') as log_file:
    # Loop through each line in the log file
    for line in log_file:
        # Extract the country code from the log line
        country_code = line.split()[-1]
        # Increment the count for the country
        if country_code in country_counts:
            country_counts[country_code] += 1
        else:
            country_counts[country_code] = 1

# Create a DataFrame from the country counts dictionary
country_df = pd.DataFrame(list(country_counts.items()), columns=['Country', 'Sessions'])

# Sort the DataFrame by the Sessions column in descending order
country_df = country_df.sort_values(by=['Sessions'], ascending=False)

# Write the DataFrame to a text file
country_df.to_csv('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/most_active_countries.txt', sep='\t', index=False)

# Print the top 10 most active countries
print(country_df.head(10))


# In[6]:


#Extracting file type and file size from log files

import os
import re
from collections import defaultdict

# Define the folder where the log files are stored
folder_path = 'C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/'

# Define the output file where the results will be saved
output_file = 'C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/accessf.txt'

# Define the regular expression to extract the file type and size
regex = r'\.(aspx|css|txt|htm|jpeg)\s+(\d+)'

# Create a dictionary to store the total size of each file type
file_sizes = defaultdict(int)

# Loop through all the log files in the folder
for file_name in os.listdir(folder_path):
    if file_name.endswith('.txt'):
        # Read the log file
        with open(os.path.join(folder_path, file_name), 'r',encoding='utf-8') as f:
            log_data = f.read()

        # Extract the file types and sizes using regular expressions
        matches = re.findall(regex, log_data, re.IGNORECASE)

        # Add the file sizes to the dictionary
        for match in matches:
            file_type, file_size = match
            file_sizes[file_type.lower()] += int(file_size)

# Sort the file types by size and limit to the top 10
sorted_files = sorted(file_sizes.items(), key=lambda x: x[1], reverse=True)[:10]

# Write the results to the output file
with open(output_file, 'w') as f:
    f.write('File Type  Size (KB)\n')
    for file_type, file_size in sorted_files:
        f.write(f'{file_type}  {file_size}\n')


# In[7]:


import pandas as pd 


# In[8]:


f=pd.read_csv('C:/Users/mehul/OneDrive/Desktop/ETL/W3SVC1/accessf.txt')


# In[9]:


f.head(5)


# In[ ]:




