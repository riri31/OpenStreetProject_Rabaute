
# coding: utf-8

# # Project Wrangling:  Open Stree Map project

# <p><span style="text-decoration: underline;"><strong>Objectives:</strong></span></p>
# <ul>
# <li>This project plans to get a dataset from Open Street, to clean it and to import into a Data Base (SQLite for me)</li>
# <li>Then the data will be analysed and observations reported into this document</li>
# <li>Observations that could lead to dataset corrections will be reinjected into data import step</li>
# <li>A last anaylysis will be done on the cleaned data</li>
# </ul>

# <p><span style="text-decoration: underline;"><strong>Material</strong></span>:</p>
# <p><span style="color: #0000ff;">&nbsp;&nbsp;&nbsp; Map Area: <em>Colomiers, Occitanie, France. This the place where I live.</em><br /><em>&nbsp;&nbsp;&nbsp; (This map has been extracted from Metro extracts.The file size is around 58 MB.)</em></span></p>
# <p>The tools used for the analysis are:</p>
# <ul>
# <li>&nbsp;Python as coding language</li>
# <li>Jupyter for code editing + analysis report</li>
# <li>SQLite for Data Base</li>
# </ul>
# 

# ## 1) First observations on data set

# In a first step, I'm checking structure of the dataset with a sample function (given in Udacity instructions).
# 
# The output is an XML file that I'm checking with a simple tool like Notepad++.

# In[1]:

#!/usr/bin/env python


import xml.etree.ElementTree as ET  # Use cElementTree or lxml if too slow

OSM_FILE = "Colomiers.osm"  # Replace this with your osm file
SAMPLE_FILE = "sample.osm"

k = 2 # Parameter: take every k-th top level element

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


# <p><span style="text-decoration: underline;"><strong>Example of Node:</strong></span>:</p>
# <p>&nbsp;&lt;node changeset="30514765" id="339791273" lat="43.6149147" lon="1.3294562" timestamp="2015-04-26T21:04:24Z" uid="1626" user="FredB" version="6"&gt;<br />&nbsp;&nbsp;&lt;tag k="name" v="Pharmacie du Prat" /&gt;<br />&nbsp;&nbsp;&lt;tag k="source" v="Celtipharm - 10/2014" /&gt;<br />&nbsp;&nbsp;&lt;tag k="amenity" v="pharmacy" /&gt;<br />&nbsp;&nbsp;&lt;tag k="dispensing" v="yes" /&gt;<br />&nbsp;&nbsp;&lt;tag k="wheelchair" v="yes" /&gt;<br />&nbsp;&nbsp;&lt;tag k="ref:FR:FINESS" v="310008230" /&gt;</p>

# <p><span style="text-decoration: underline;"><strong>Example of Way point:</strong></span>:</p>
# 
# <p>&nbsp;&lt;way changeset="20389389" id="35773868" timestamp="2014-02-05T11:41:35Z" uid="149399" user="awikatchikaen" version="5"&gt;<br />&nbsp;&nbsp;&lt;nd ref="418245304" /&gt;<br />&nbsp;&nbsp;&lt;nd ref="418245306" /&gt;<br />&nbsp;&nbsp;&lt;nd ref="418245307" /&gt;<br />&nbsp;&nbsp;&lt;nd ref="418245309" /&gt;<br />&nbsp;&nbsp;&lt;nd ref="418245311" /&gt;<br />&nbsp;&nbsp;&lt;nd ref="2280539648" /&gt;<br />&nbsp;&nbsp;&lt;nd ref="2655807950" /&gt;<br />&nbsp;&nbsp;&lt;tag k="highway" v="path" /&gt;<br />&nbsp;&lt;/way&gt;</p>

# The first observations enabled me to understand structure of XML file and to get a smaller extract of OSM data than original dataset.
# 
# I tested all my code against this sample data each time I could to minimize computing time.

# # 2 ) Problems observed in the sample

# <p>Thanks to the sample, the following issues could be quickly raised:</p>
# <ul>
# <li>tag properties are <strong><span style="color: #0000ff;">k</span></strong> and <strong><span style="color: #0000ff;">v</span></strong> whereas <strong><span style="color: #0000ff;">key</span> </strong>and <strong><span style="color: #0000ff;">value</span> </strong>are requested in SQL schema</li>
# <li>Type of tags are under the format:
# <ul>
# <li>&lt;tag k="<strong><span style="color: #0000ff;">addr</span></strong>:city" v="Tournefeuille" /&gt;</li>
# <li>The type is <span style="color: #0000ff;"><strong>addr</strong> </span>(ie. address)</li>
# <li>That means processing is required to extract the from tag key</li>
# </ul>
# </li>
# <li>Problem of case sensitiveness for City: <span style="color: #0000ff;">'<strong>COLOMIERS</strong>'</span> and <span style="color: #0000ff;">'<strong>Colomiers</strong>'</span></li>
# <li>Same for street types: <span style="color: #0000ff;">'<strong>PLACE</strong>'</span> and <span style="color: #0000ff;">'<strong>Place</strong>'</span></li>
# <li>Date of source data are present in two fields:
# <ul>
# <li>&lt;tag k="source_date" v="<span style="color: #0000ff;"><strong>04/04/2012</strong></span>" /&gt;</li>
# <li>&lt;tag k="source" v="cadastre-dgi-fr source : Direction G&eacute;n&eacute;rale des Imp&ocirc;ts - Cadastre. Mise &agrave; jour : <span style="color: #0000ff;"><strong>2009</strong></span>" /&gt;</li>
# </ul>
# </li>
# </ul>

# ## 3) Creation of CSV files

# <p>Before cleaning the data. I decided to create csv file related to Data schema (provided into project instructions).</p>
# <p>The only modifcation linked to issues observed in the sample are:</p>
# <ul>
# <li>the type of tag with <strong><span style="color: #0000ff;"><em>addr</em> </span></strong>substring
# <ul>
# <li><em><strong><span style="color: #0000ff;">addr</span></strong></em>:city will be split into 2 substrings with the first one as tag <strong><span style="color: #0000ff;"><em>type</em> </span></strong>and the second as <strong><span style="color: #0000ff;"><em>key </em></span></strong><span style="color: #0000ff;"><em><span style="color: #000000;">(even if a : is present into the second sub string)</span></em></span></li>
# </ul>
# </li>
# <li>tag properties <strong><span style="color: #0000ff;">k</span></strong> and <strong><span style="color: #0000ff;">v</span></strong> will be mapped with <strong><span style="color: #0000ff;">key</span> </strong>and <strong><span style="color: #0000ff;">value</span> </strong>as requested in SQL schema</li>
# </ul> 

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
                            if ":" in elem.attrib[key]: #extract of tag type from k field
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


# <p>As an output, we have several csv files corresponding to SQL schema provided in project instructions:&nbsp;</p>
# <ul>
# <li><em>nodes.csv</em></li>
# <li><em>nodes_tags.csv</em></li>
# <li><em>ways.csv</em></li>
# <li><em>ways_tags.csv</em></li>
# <li><em>ways_nodes.csv</em></li>
# </ul>

# ## 4)  Import into Database

# <p>After creating csv files, I import them into SQLlite Data Base thanks to pandas python library.</p>
# <p>I do in two steps:</p>
# <ol>
# <li>creation of&nbsp;SQL schemas</li>
# <li>Import of csv files into DB thanks to pandas dataframe</li>
# </ol>

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


# # 5)  First analysis from Database

# Once data imported into SQL database, we can analyse the data.
# 
# For that, I develop a Python function that will display Top 10 for users, city, postcode and street types. That will enable me to see if there is some obvious issues with data.

# In[5]:

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


# <p><span style="text-decoration: underline;"><strong>Analysis:</strong></span></p>
# <ul>
# <li style="padding-left: 30px;">On top users, except 'Chouloute' extra number of inputs, nothing to report</li>
# <li style="padding-left: 30px;">On top postcodes, it is interesting to observe that not only Colomiers postal code is represented (31770), Tournefeuille (31170) and Toulouse (31300&amp;31000) are numberous as well. That could be explained by the fact Tournefeuille and Colomiers are neighbours of Colomiers. Same for Pibrac (31820) but with less quantity. 31776 corresponds to Colomiers with a different format. I will analyse afterwards.</li>
# <li style="padding-left: 30px;">On top city, same observation as for postal code, Colomiers represents half of references. As I noticed in the first analysis from the sample, Colomiers with Upper and Lower cases that make two entries into Data Base. I propose to clean it afterwards.</li>
# <li style="padding-left: 30px;">On top street types, we can see there are only 6 entries. There is one issue related to sensitive case (Place and place). I will fix it in next section</li>
# <li style="padding-left: 30px;">On top sources, Cadastre (ie. the register of the real estate or real property's metes-and-bounds of France) is the top listed that is normal because it is its primary function. Nothing else interesting to notice</li>
# </ul>
# <p><span style="text-decoration: underline;"><strong>Synthesis:</strong></span></p>
# <ul>
# <li>No big issues on top parameters analysis, the dataset seems to be well built</li>
# </ul>

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

# In[6]:

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


# In[7]:

sql_creation()
import_sql(csv_file_nodes='nodes.csv',csv_file_nodes_tags='nodes_tags_cleaned.csv',csv_file_ways='ways.csv',csv_file_ways_tags='ways_tags_cleaned.csv',csv_file_ways_nodes='ways_nodes.csv')
top_sql()


# <p><span style="text-decoration: underline;"><strong>Analysis:</strong></span></p>
# <ul>
# <li>The few corrections have been done and give the good results</li>
# <li>Nothing more to report on users, postcode and city</li>
# <li>On street types, it is interresting that the majority of street type are "All&eacute;e" (ie. Alley) whereas it should be "Rue" (ie. Street) as most of street are "Rue". That could be explained by the fact that users focus on main street and not on small street ("Rue")</li>
# <li>On top source, we can see we have update date in most of cases, I propose to make a last study on this item in order to check if this dataset is uptodate</li>
# </ul>
# <p><br /><span style="text-decoration: underline;"><strong>Synthesis:</strong></span></p>
# <ul>
# <li>This OSM dataset seems far cleaner as examples during udacity course</li>
# </ul>

# ## 5)  Last analysis

# <p>On this last chapter, I propose to check <strong>date </strong>related to <strong>source </strong>tag in the dataset. It will enable to give a status about how uptodate the dataset is.</p>
# <p>For this, I propose to extract the date from:</p>
# <ul>
# <li>the value of '<strong>source_date'</strong> tag in nodes_tags and ways_tags tables.
# <ul>
# <li>&lt;tag k="<strong>source_date"</strong> v="<strong>04/04/2012</strong>" /&gt;</li>
# <li>To notice that date could be under different formats:
# <ul>
# <li>18/03/2013</li>
# <li>2017-09-04</li>
# <li>10/2016</li>
# <li>But as the year is only interesting for my topic, I choose to use a regular expression to match the year: <em>"^(19|20)\d{2}\$</em>"&nbsp;</li>
# </ul>
# </li>
# </ul>
# </li>
# </ul>
# <ul>
# <li>the string of '<strong>source</strong>' tag in nodes_tags and ways_tags tables.
# <ul>
# <li>&lt;tag k="<strong>source"</strong> v="cadastre-dgi-fr source : Direction G&eacute;n&eacute;rale des Imp&ocirc;ts - Cadastre. Mise &agrave; jour : <strong>2009</strong>" /&gt;</li>
# <li>I use the&nbsp;same&nbsp;regular expression to match the year: <em>"^(19|20)\d{2}$"</em></li>
# </ul>
# </li>
# </ul>
# <ul>
# </ul>

# In[8]:

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
        x.append(m.group(0))

## Extract year from source_date tag value ###
result=c.execute('''SELECT value from (select * from nodes_tags UNION ALL select * from ways_tags) where key='source_date';''')
j=0
for i in result:  
    m = re.search('(19|20)\d{2}$',i[0])
    if m:
        x.append(m.group(0))

#Display of the year
display_year(dict(Counter(x)))
print '\nTotal source tag with year:{}'.format(len(x))
conn.close()


# <p><span style="text-decoration: underline;"><strong>Analysis:</strong></span></p>
# <ul>
# <li>It appears that data are quite old since the main part of data are from 2010 (ie. 7 years old)</li>
# </ul>

# ## 6)  Conclusions

# Thanks to analysis, we have checked quality from an extract of Open Street dataset and analyse some items.
# 
# - The extract from Metro extract is too large in comparison of study topic (Colomiers town), neighbouroud cities have been incorporated too much
# - The quality of the data is quite good as only a few of modifications have been needed
# - The common mistake of case sencitiveness could be preventer by a systematic cleaning routine that could enforce rules for every entries
# - The data is quite old since most of information are 7 years old
# 
