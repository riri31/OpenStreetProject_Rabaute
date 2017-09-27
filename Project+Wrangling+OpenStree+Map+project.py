
# coding: utf-8

# # Project Wrangling:  Open Stree Map project

# Objectives:
# - This project plans to get a dataset from Open Street, to clean it and to import into a Data Base (SQLite for me)
# - Then the data will be analysed and observations reported into this document
# - Observations that could lead to dataset corrections will be reinjected into data import step
# - A last anaylysis will be done on the cleaned data

# Material:
# 
#     Map Area: Colomiers, Occitanie, France. This the place where I live.
#     (This map has been extracted from Metro extracts.The file size is around 58 MB.)
# 
# The tools used for the analysis are:
# - Python as coding language
# - Jupyter for code editing + analysis report
# - SQLite for Data Base
# 

# ## 1) First observations on data set

# In a first step, I'm checking structure of the dataset with a sample function (given in Udacity instructions)

# In[1]:

#!/usr/bin/env python


import xml.etree.ElementTree as ET  # Use cElementTree or lxml if too slow

OSM_FILE = "Colomiers.osm"  # Replace this with your osm file
SAMPLE_FILE = "sample.osm"

k = 5 # Parameter: take every k-th top level element

def get_element(osm_file, tags=( 'node','way', 'relation')):
    """Yield element if it is the right type of tag

    Reference:
    http://stackoverflow.com/questions/3095434/inserting-newlines-in-xml-file-generated-via-xml-etree-elementtree-in-python
    """
    context = iter(ET.iterparse(osm_file, events=('start', 'end')))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


with open(SAMPLE_FILE, 'wb') as output:
    output.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    output.write('<osm>\n  ')

    # Write every kth top level element
    for i, element in enumerate(get_element(OSM_FILE)):
        if i % k == 0:
            output.write(ET.tostring(element, encoding='utf-8'))

    output.write('</osm>')


# The first observations enabled me to understand structure of XML file and to get a smaller extract of OSM data than original dataset.
# 
# I tested all my code against this sample data each time I could to minimize computing time.

# ## 2) Creation of CSV files

# Before cleaning the data. I decided to create csv file related to Data schema (provided into project instructions).

# In[2]:

import csv

############## Function that creates csv files with first level tags ##############
##### This function get the given first level tags and generates csv files #####

def create_table(osm_file,csv_file,fieldnames,tag,k):
    with open(csv_file, 'wb') as csvfile:
                
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
    
        for i, element in enumerate(get_element(OSM_FILE,tag)):
            if i % k == 0:
                entry={}

                for key in element.attrib.keys():

                    entry[key]=element.attrib[key].encode('utf-8')
                writer.writerow(entry)    

############## Function that creates csv files with second level tags ##############
##### This function get the given second level tags and generates csv files
##### This function is enriched as well with a mapping function k<=>key, v <=> value
##### Moreover, this function get the 'type' pattern from the code and injected into the csv file in the 'type' collumn'

def create_table_tags(osm_file,csv_file,fieldnames,tag,k):
    with open(csv_file, 'wb') as csvfile:
                
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
    
        for i, element in enumerate(get_element(OSM_FILE,tag)):
            if i % k == 0:

                for elem in element.iter("tag"):
                    entry={}
                    for key in elem.attrib.keys():
                        # mapping k<=> key
                        if key=='k':
                            o='key'
                        # mapping v<=> value
                        elif key=='v':
                            o='value'
                        else:
                            o=key
                        entry[o]=elem.attrib[key].encode('utf-8')
                        if key=="k" :
                            if ":" in elem.attrib[key]:
                                entry['type']=elem.attrib[key].split(":")[0].encode('utf-8')    
                                entry[o]=elem.attrib[key].split(":")[1].encode('utf-8')   
                    entry['id']=element.attrib['id'].encode('utf-8')                  
                    writer.writerow(entry)  

############## Function that creates csv files with second level tags correspondinf ways nodes ##############
##### This function get the given second level tags labbeled nodes and generates csv files #####

def create_ways_node(osm_file,csv_file,fieldnames,tag,k):
    with open(csv_file, 'wb') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
    
        for i, element in enumerate(get_element(OSM_FILE,tag)):
            if i % k == 0:
                identifier=element.attrib["id"]
                for elem in element.iter("nd"):
                    entry={}
                    entry["id"]=identifier.encode('utf-8')
                    entry["node_id"]=elem.attrib["ref"].encode('utf-8')
                    writer.writerow(entry) 


############## CSV files creation ##############
def tables_creation(k=1,osm_file='Colomiers.osm',csv_file_nodes='nodes.csv',csv_file_nodes_tags='nodes_tags.csv',csv_file_ways='ways.csv',csv_file_ways_tags='ways_tags.csv',csv_file_ways_nodes='ways_nodes.csv'):


                    
    ################ Create nodes table ################
    print 'Creating node table...'

    fieldnames = ['id', 'lat','lon','user','uid','version','changeset','timestamp']
    tag='node'

    create_table(osm_file,csv_file_nodes,fieldnames,tag,k)
    print 'Creating node table done'

    ################ Create nodes tag table ################
    print 'Creating node tag table...'
    fieldnames = ['id', 'key','value','type']
    tag='node'

    create_table_tags(osm_file,csv_file_nodes_tags,fieldnames,tag,k)
    print 'Creating node tag table done'

    ################ Create ways table ################

    print 'Creating ways table...'
    fieldnames = ['id', 'user','uid','version','changeset','timestamp']
    tag='way'

    create_table(osm_file,csv_file_ways_tags,fieldnames,tag,k)
    print 'Creating ways table done'

    ################ Create ways tags table ################
    print 'Creating ways tag table...'
    fieldnames = ['id', 'key','value','type']
    tag='ways'

    create_table_tags(osm_file,csv_file_ways_tags,fieldnames,tag,k)
    print 'Creating ways tag table done'

    ################ Create ways node table ################
    print 'Creating ways node...'
    fieldnames = ['id', 'node_id']
    tag='ways'

    create_ways_node(osm_file,csv_file_ways_nodes,fieldnames,tag,k)
    print 'Creating ways nodes done'

           
    
############## Main ##############

tables_creation()


# ## 3)  Import into Database

# After creating csv files, I import them into SQLlite Data Base thanks to pandas python library.

# In[3]:

import sqlite3


def sql_creation():
    ################# Connect the Data Base 'OSM.db' ################
    conn = sqlite3.connect('OSM.db')

    c = conn.cursor()

    ################# Clean the pre-existing tables ################
    c.execute('''DROP TABLE nodes;''')
    c.execute('''DROP TABLE nodes_tags;''')
    c.execute('''DROP TABLE ways;''')
    c.execute('''DROP TABLE ways_tags;''')
    c.execute('''DROP TABLE ways_nodes;''')

    ################# Create tables ################


    ### Create table nodes ###
    c.execute('''CREATE TABLE nodes (
        id INTEGER PRIMARY KEY NOT NULL,
        lat REAL,
        lon REAL,
        user TEXT,
        uid INTEGER,
        version INTEGER,
        changeset INTEGER,
        timestamp DATE
    );''')

    ### Create table tags nodes ###
    c.execute('''CREATE TABLE nodes_tags (
        id INTEGER,
        key TEXT,
        value TEXT,
        type TEXT,
        FOREIGN KEY (id) REFERENCES nodes(id)
    );''')

    ### Create table ways ###
    c.execute('''CREATE TABLE ways (
        id INTEGER PRIMARY KEY NOT NULL,
        user TEXT,
        uid INTEGER,
        version TEXT,
        changeset INTEGER,
        timestamp DATE
    );''')

    ### Create table tags ways ###
    c.execute('''CREATE TABLE ways_tags (
        id INTEGER NOT NULL,
        key TEXT NOT NULL,
        value TEXT NOT NULL,
        type TEXT,
        FOREIGN KEY (id) REFERENCES ways(id)
    );''')

    ### Create table ways nodes ###
    c.execute('''CREATE TABLE ways_nodes (
        id INTEGER NOT NULL,
        node_id INTEGER NOT NULL,
        FOREIGN KEY (id) REFERENCES ways(id),
        FOREIGN KEY (node_id) REFERENCES nodes(id)
    );''')


    conn.close()

    
    
    
############## Main ##############

sql_creation()


# In opposition of Project instructions, I decided to not add position parameter into Ways nodes table because I don't get the benefit to add this parameter in this table.
# 
# Indeed, precise position parameters are already in nodes table (lat, long) and nodes are already referenced into ways nodes table. It is in opposition of normalization rules that are to not have redondant information.

# In[4]:

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def import_sql(csv_file_nodes='nodes.csv',csv_file_nodes_tags='nodes_tags.csv',csv_file_ways='ways.csv',csv_file_ways_tags='ways_tags.csv',csv_file_ways_nodes='ways_nodes.csv'):
    ################# Connect the Data Base 'OSM.db' ################
    conn = sqlite3.connect('OSM.db')
    conn.text_factory = str
    c = conn.cursor()

    ### Import table nodes ###
    print "Importing",csv_file_nodes,"..."
    df = pd.read_csv(csv_file_nodes)
    df.to_sql('nodes', conn, if_exists='append', index=False)
    print "Import done"

    ### Import table nodes tags ###
    print "Importing",csv_file_nodes_tags,"..."
    df = pd.read_csv(csv_file_nodes_tags)
    df.to_sql('nodes_tags', conn, if_exists='append', index=False)
    print "Import done"

    ### Import table ways ###
    print "Importing",csv_file_ways,"..."
    df = pd.read_csv(csv_file_ways)
    df.to_sql('ways', conn, if_exists='append', index=False)
    print "Import done"

    ### Import table ways tags ###
    print "Importing",csv_file_ways_tags,"..."
    df = pd.read_csv(csv_file_ways_tags)
    df.to_sql('ways_tags', conn, if_exists='append', index=False)
    print "Import done"

    ### Import table ways nodes ###
    print "Importing",csv_file_ways_nodes,"..."
    df = pd.read_csv(csv_file_ways_nodes)
    df.to_sql('ways_nodes', conn, if_exists='append', index=False)
    print "Import done"


    conn.close()

############## Main ##############

import_sql()


# In[5]:

## 4)  First analysis from Database


# In[6]:

def display(result):
    j=1
    for i in result:
        #print i[0],":",i[1]
        print '({}){}:\t{}'.format(j,i[0],i[1])
        j+=1
        
def display_total(result):
    for i in result:
        print '\n\tTotal:\t{}'.format(i[0])
    

def top_sql():
    conn = sqlite3.connect('OSM.db')
    conn.text_factory = str
    c = conn.cursor()


    ### Show top 15 users ###
    result=c.execute('''SELECT user,count(user) from (select user from nodes UNION ALL select user from ways) group by user order by count(user) desc limit 15
    ;''')
    print "\nTop 10 users:"
    display(result)

    result=c.execute('''SELECT count(user) from (select user from nodes UNION ALL select user from ways)
    ;''')
    display_total(result)

    ### Show top 10 postcode ###
    result=c.execute('''SELECT value,count(value) from (select * from nodes_tags UNION ALL select * from ways_tags) where key='postcode' group by value order by count(value) desc limit 10
    ;''')
    print "\nTop 10 postcode:"
    display(result)
    
    result=c.execute('''SELECT count(value) from (select * from nodes_tags UNION ALL select * from ways_tags) where key='postcode' 
    ;''')
    display_total(result)

    ### Show top 10 city ###
    result=c.execute('''SELECT value,count(value) from (select * from nodes_tags UNION ALL select * from ways_tags) where key='city' group by value order by count(value) desc limit 10
    ;''')
    print "\nTop 10 city:"
    display(result)
    
    result=c.execute('''SELECT count(value) from (select * from nodes_tags UNION ALL select * from ways_tags) where key='city'
    ;''')
    display_total(result)
    
    
    ### Show top 10 street type ###
    result=c.execute('''SELECT type,count(type) from (SELECT SUBSTR(value,0,INSTR( value , " " )) as type from(select * from nodes_tags UNION ALL select * from ways_tags) where key='street' limit 100) group by type;''')
    print "\nTop 10 street type:"
    display(result)

    result=c.execute('''SELECT count(type) from (SELECT SUBSTR(value,0,INSTR( value , " " )) as type from(select * from nodes_tags UNION ALL select * from ways_tags) where key='street' limit 100) ;''')
    display_total(result)
        
    ### Show top 10 source ###
    result=c.execute('''SELECT value,count(value) from (select * from nodes_tags UNION ALL select * from ways_tags) where key='source' group by value order by count(value) desc limit 10
    ;''')
    print "\nTop 10 source:"
    display(result)

    result=c.execute('''SELECT count(value) from (select * from nodes_tags UNION ALL select * from ways_tags) where key='source'
    ;''')
    display_total(result)



    conn.close()

############## Main ##############

top_sql()


# Analysis:
# - On top users, except 'Chouloute' extra number of inputs, nothing to report
# - On top postcodes, it is interesting to observe that not only Colomiers postal code is represented (31770), Tournefeuille (31170) and Toulouse (31300&31000) are numberous as well. That could be explained by the fact Tournefeuille and Colomiers are neighbours of Colomiers. Same for Pibrac (31820) but with less quantity. 31776 corresponds to Colomiers with a different format. I will analyse afterwards.
# - On top city, same observation as for postal code, Colomiers represents half of references. On interesting thinh is Colomiers with Upper and Lower cases that make two entries into Data Base. I propose to clean it afterwards.
# - On top street types, we can see there are only 6 entries. There is one issue related to sensitive case (Place and place). I will fix it in next section
# - On top sources, Cadastre (ie. the register of the real estate or real property's metes-and-bounds of France) is the top listed that is normal because it is its primary function. Nothing else interesting to notice.
# 
# Synthesis:
# - No big issues on top parameters analysis, the dataset seems to be well built

# ## 4)  Cleaning of Dataset

# In this chapter, I propose to clean the data in the csv creation step. The advantage is I have just to adapt my code. Moreover, as the OSM dataset is pretty clean I will have only a few correction to do.
# 
# I create a new function named 'csv_data_cleaning' that will get a csv file produced by python code, apply some modifications and output a new csv file I will import into a new SQLite DataBase (in fact, I drop all the existing tables in the existing DB and I import again with modified csv files).
# 
# a) City and street type modification
# 
# On first analysis, we noticed that both city entries were different because our code was sensitive to upper/lower cases. Same for street types.
# 
# I propose to use string.capitalize() function to have the first letter of city in capitize letter and the rest lowercased.

# In[7]:

def name_cleaning(city_name):
    return city_name.capitalize() 

def csv_data_cleaning(csv_file_source,csv_file_dest):
    print 'Clean data from {} to {}...'.format(csv_file_source,csv_file_dest) 
    with open(csv_file_dest,'wb') as csvfile_dest:
        with open(csv_file_source,'rb') as csvfile_source:
            reader = csv.DictReader(csvfile_source)
            writer = csv.DictWriter(csvfile_dest, reader.fieldnames)
            writer.writeheader()
            for row_source in reader:
                row_destination=row_source
                for cell in row_destination.keys():
                    if cell=='key':
                        # Cleaning of city names
                        if row_destination['key']=='city':
                            row_destination['value']=name_cleaning(row_destination['value'])
                        # Cleaning of street type
                        elif row_destination['key']=='street': 
                            row_destination['value']=name_cleaning(row_destination['value'])
                            street_type=row_destination['value'].split(" ")[0]
                            
                writer.writerow(row_destination)
    print 'Clean data... done'


############## Main ##############

csv_data_cleaning('nodes_tags.csv','nodes_tags_cleaned.csv')
csv_data_cleaning('ways_tags.csv','ways_tags_cleaned.csv')


# In[8]:

sql_creation()
import_sql(csv_file_nodes='nodes.csv',csv_file_nodes_tags='nodes_tags_cleaned.csv',csv_file_ways='ways.csv',csv_file_ways_tags='ways_tags_cleaned.csv',csv_file_ways_nodes='ways_nodes.csv')
top_sql()


# Analysis:
# - The few corrections have been done and give the goog results
# - Nothing more to report on users, postcode and city
# - On street types, it is interresting that the majority of street type are "Allée" (ie. Alley) whereas it should be "Rue" (ie. Street) as most of street are "Rue". That could be explained by the fact that users focus on main street and not on small street ("Rue")
# - On top source, we can see we have update date in most of cases, I propose to make a last study on this item in order to check if this dataset is uptodate
# 
# 
# Synthesis:
# - This OSM dataset seems far more clean as examples during udacity course
# 

# ## 5)  Last analysis

# On this last chapter, I propose to check date related to source tag in the dataset. It will enable to give a status about how uptodate the dataset is.
# For this, I propose to extract the date from the value of 'source' tag in nodes_tags and ways_tags tables.
# 
# I will use the following regular expression to match the year: "^(19|20)\d{2}$" may be additional filter will be needed.

# In[9]:

import re
from collections import Counter

def display_year(result):
    keylist =result.keys()
    keylist.sort(reverse=True)
    for i in keylist:
        print i,result[i]

x=[]

### Connection to Data Base ###
conn = sqlite3.connect('OSM.db')
conn.text_factory = str
c = conn.cursor()

### Extract year from source tag value ###
result=c.execute('''SELECT value from (select * from nodes_tags UNION ALL select * from ways_tags) where key='source';''')
j=0
for i in result:  
    m = re.search('(19|20)\d{2}$',i[0])
    if m:
        #print m.group(0)
        x.append(m.group(0))

#print 'Update year:{}'.format(dict(Counter(x)))
#print 'Total source tag with year:{}'.format(len(x))
display_year(dict(Counter(x)))
print '\nTotal source tag with year:{}'.format(len(x))
conn.close()


# Analysis:
# - It appears that data are quite old since the main part of data are from 2010 (ie. 7 years old)

# ## 6)  Conclusions

# Thanks to analysis, we have checked quality from an extract of Open Street dataset and analyse some items.
# 
# - The extract from Metro extract is too large in comparison of study topic (Colomiers town), neighbouroud cities have been incorporated too much
# - The quality of the data is quite good as only a few of modifications have been needed
# - The common mistake of case sencitiveness could be preventer by a systematic cleaning routine that could enforce rules for every entries
# - The data is quite old since most of information are 7 years old
# 

# In[ ]:



