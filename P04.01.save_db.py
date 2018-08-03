#!/usr/bin/python
# Author : Michel Benites Nascimento
# Date   : 04/12/2018
# Descr. : Inserting records on Cassandra column families 

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

import uuid
import random
import datetime
import sys

# Function to return a random datetime. Parameter: dt_now -> current time
def randomtime(dt_now):
    # Generating the random datetime and formating.
    ret = dt_now + datetime.timedelta(minutes=random.randrange(180))
    ret = ret.strftime('%Y-%m-%d %H:%M:%S.%f')
    return ret

# Funtion to return a random url. Parameter: i_url -> limit value to generate the final number of url
def randomurl(i_url):
    # Generating the random url.
    ret = 'http://example.com/?url=' + str(random.randint(0,i_url)) 
    return ret

# Function to return random country.
def randomcountry():
    # List with some sample countries.
    l_country = ('USA', 'BR', 'USA', 'CA', 'MX', 'FR', 'USA')
    return (l_country[random.randint(0, 6)])

# Set the username and password and connecting on Cassandra database.
auth_provider = PlainTextAuthProvider(username='cassandra', password='mLLQEe5Qe3it')
cluster = Cluster(auth_provider=auth_provider)
session = cluster.connect('hw10')
session.set_keyspace('hw10')

# Define the SQL insert.
sql_insert   =  " INSERT INTO hw10_p2 " + \
                "( url,          " + \
                "  ua_country,   " + \
                "  time_hour,    " + \
                "  insert_time,  " + \
                "  user_id,      " + \
                "  TTFB )        " + \
                " VALUES         " + \
                "(%s, %s, %s, %s, %s, %s) "
   
# Loop to generate 200 events and insert into Cassandra database.
for x in range(200):

    # Store the random values into variables.
    _id          = uuid.uuid1()
    s_timestamp  = str(randomtime(datetime.datetime.now()))[:-3]
    s_timehour   = s_timestamp[:13] + ':00:00.000'
    s_url        = randomurl(3)
    s_country    = randomcountry()
    i_ttfb       = random.randint(100, 300)

    # Create a command with QUORUM consistency level.
    s_query = SimpleStatement(sql_insert, consistency_level=ConsistencyLevel.QUORUM)

    # Running asynchronous insert command.
    future = session.execute_async(s_query, (s_url, s_country, s_timehour, \
                                   s_timestamp, _id, i_ttfb))

# Verify error and wait to finish the inserts
try:
    rows = future.result()
except ReadTimeout:
    log.exception("Query timed out:")

# Define variables to Select
sql_url     = sys.argv[1] 
sql_country = sys.argv[2] 
sql_time    = sys.argv[3]

# Create sql command for Select
sql_select = "select count(*) as total from hw10_p2 where url = %s "
sql_select = sql_select + " and ua_country = %s " 
sql_select = sql_select + " and time_hour  = %s "  

# Create a select command with QUORUM consistency level.
s_query = SimpleStatement(sql_select, consistency_level=ConsistencyLevel.QUORUM)

# Running asynchronous Select command.
future = session.execute_async(s_query, (sql_url, sql_country, sql_time))

# Verify error and wait to finish the selects
try:
    rows = future.result()
    # Start printing results.
    print ''
    print 'Q1 - Result - Count'
    for x in rows:
        print x[0], ' event(s) for ', sql_url, ',',  sql_country, ',', sql_time

except ReadTimeout:
    log.exception("Query timed out:")

# Create sql for Select
sql_select = "select avg(TTFB) as total from hw10_p2 where url = %s "
sql_select = sql_select + " and ua_country = %s " 
sql_select = sql_select + " and time_hour  = %s "  

# Create a select command with QUORUM consistency level.
s_query = SimpleStatement(sql_select, consistency_level=ConsistencyLevel.QUORUM)

# Running asynchronous Select command.
future = session.execute_async(s_query, (sql_url, sql_country, sql_time))

# Verify error and wait to finish the selects
try:
    # Start printing results.
    rows = future.result()
    print ''
    print 'Q2 - Result - Average'
    for x in rows:
        print x[0], ' => average of TTFB for ', sql_url, ',',  sql_country, ',', sql_time

except ReadTimeout:
    log.exception("Query timed out:")

# Shutdown the session.
session.shutdown()    
