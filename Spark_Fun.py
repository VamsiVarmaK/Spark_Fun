
# coding: utf-8
#Exploring a dataset in Spark (coz my weekend plan has just changed)

# In[1]:


import os
import sys
import re
import datetime
from pyspark.sql import functions as F


# In[2]:


from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
sc = SparkContext()
sqlContext = SQLContext(sc)


# In[3]:


data = sqlContext.read.text('apache.access.log')


# In[4]:


print(data.count())


# In[5]:


print(data.printSchema())


# In[6]:


data.show(n=7,truncate=False)


# In[7]:


split_df = data.select(
  # \s = whitespace char, \d = digit char [0-9], \w = word char
  # 'host' field: ([^\s]+\s) means take group who DOESN'T begin with whitespace char, and regex stop when it encounters \s
  F.regexp_extract('value', r'^([^\s]+\s)', 1).alias('host'),
  # 'timestamp' field: capture group whose enclosed by bar bracket [] - parenthesis doesn't cover the bar-brack cuz you just want the timestamp.
  #                    it goes like: "2-dig/3-alpha/4-dig/2dig:2dig:2dig: -3dig"
  F.regexp_extract('value', r'^.*\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]', 1).alias('timestamp'),
  # 'path' field: ^.*" = take any char until you hit the double-quote char.  \w+\s = http request method.
  #               Finally, ([^\s]+)\s+HTTP = keep extracing all non-whitespace char until you bump into \s followed up HTTP
  F.regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*"', 1).alias('path'),
  # 'status' field: http://www.w3schools.com/tags/ref_httpmessages.asp
  F.regexp_extract('value', r'^.*"\s+([^\s]+)', 1).cast('integer').alias('status'),
  # 'content_size' field: the ending series of digits
  F.regexp_extract('value', r'^.*\s+(\d+)$', 1).cast('integer').alias('content_size'))
split_df.show(n=5,truncate=False)


# In[8]:


cleaned_df = split_df.na.fill({'content_size': 0})


# In[9]:


month_map = {
  'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
  'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12
}

def parse_clf_time(s):
    """ Convert Common Log time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format [dd/mmm/yyyy:hh:mm:ss (+/-)zzzz]
    Returns:
        a string suitable for passing to CAST('timestamp')
    """
    # NOTE: We're ignoring time zone here. In a production application, you'd want to handle that.
    return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
      int(s[7:11]),
      month_map[s[3:6]],
      int(s[0:2]),
      int(s[12:14]),
      int(s[15:17]),
      int(s[18:20])
    )

u_parse_time = F.udf(parse_clf_time)


# In[10]:


col_to_append = (u_parse_time(cleaned_df['timestamp'])
                 .cast('timestamp') # convert column type. https://wtak23.github.io/pyspark/generated/generated/sql.Column.cast.html
                 .alias('time')     # rename
                )
print(col_to_append)


# In[11]:


# now append column to our parsed, cleaned dataframe
logs_df = cleaned_df.select('*', col_to_append)
logs_df.show(n=5,truncate=False)


# In[12]:


logs_df = logs_df.drop('timestamp')
logs_df.show(n=5,truncate=False)


# In[13]:


total_log_entries = logs_df.count()
print(total_log_entries)


# In[14]:


logs_df.printSchema()


# In[15]:


logs_df.show()


# -> Let's see the content size!

# In[16]:


logs_df.describe("content_size").show()


# -> How about counting and sorting?

# In[17]:


log_status = logs_df.groupBy('status').count().sort('status')
log_status.show()



# -> A plot for fun!

# In[18]:


import matplotlib.pyplot as plt
import pandas as pd 
get_ipython().magic('matplotlib inline')
logs_pd = log_status.toPandas()
logs_pd.plot(x="status", kind='Bar', title='Log_status')


# -> Let's see some actual details now. All the hosts that has accessed the server more than 10 times? 

# In[19]:


log_host = logs_df.groupby('host').count()
log_host_15 = log_host.where(log_host['count']>10)
log_host_15.show(n=15)


# -> Top 10 path based on their count 

# In[20]:


log_path = logs_df.groupby('path').count().sort('count', ascending=False)
log_path.show(n=10)



# Difficulty: Intermediate

# -> Some paths which do not return code 200? 

# In[21]:


path_200 = logs_df.filter(logs_df.status!=200)
path_200.groupby('path').count().sort('count', ascending=False).show(n=10)


# ->  Unique hosts are there in the entire log?

# In[22]:


U_host = logs_df.select('host').distinct()
U_host.count()



# Difficulty: Hard (So, I've been told!)

# -> Determining the number of unique hosts in the entire log on a day-by-day basis. 


# In[31]:

#getdays = logs_df.select(logs_df['host'], F.dayofmonth(logs_df['time']).alias('day'))
#getdays.toPandas()

#getdays.select(int(getdays['day'])).count()

#getdays.groupBy(getdays['day']).count().show()

#m=getdays.groupBy('host')

#x=getdays.groupBy('day').count()

from pyspark.sql.functions import dayofmonth
dayshost = logs_df.select(logs_df['host'],dayofmonth(logs_df['time']).alias('day'))
daysdistinct = dayshost.distinct()
daysgroup = daysdistinct.groupBy('day').count()
final_days = daysgroup.sort('day')
final_days.show()


# Visualising the Number of Unique Daily Hosts above. 

# In[33]:


final_days_pd = final_days.toPandas()
final_days_pd.plot(x="day", kind='Bar', title='Host Count')


# In[ ]:




