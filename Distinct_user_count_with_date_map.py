# Databricks notebook source
# DBTITLE 1,CREATING SparkSession and pandas instance

# Notebook Published - https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/931328135249323/524104771162516/5764242340617371/latest.html 
import pandas as pd
from pyspark.sql import SparkSession
sc = SparkSession.builder.appName("distinct user count").getOrCreate()


# COMMAND ----------

# DBTITLE 1,creating data
list1=[]
for i in range(1,21):
  dates= '01/'+str(i)+'/2019'
  list1.append(dates)
  print(dates)

dateList=['a','b','c','d','a','b','d','e','g','t','a','b','d','c','t','j','k','a','m','l']

#to get length of the list
#len(dateList)

# COMMAND ----------

# DBTITLE 1,Creating pandas df
# creating dictionary for making datafrane
data = {"user": dateList,
         "date":list1}

#making dataframe df
df=pd.DataFrame(data)
df['date']= pd.to_datetime(df['date'])


# COMMAND ----------

# DBTITLE 1,Spark dataframe [dfs]
from pyspark.sql import *
dfs=sc.createDataFrame(df)
dfs.show()

# COMMAND ----------

# DBTITLE 1,Creating mapping with collect but not good.
'''
def func(n):
  for i in n :
    print(i)
    #print("hey")
    for j in n:
      print(i ,"*", j)
     # print (i,"&",j)

result = map(func, dfs.select(dfs['date']).collect())
print(list(result))
'''

# COMMAND ----------

# DBTITLE 1,Creating pivot dataframe [dfpivot] for each user for each date
#GROUPING ALL USER FOR EACH ROW AND PIVOTTING ALL DATE ROW IN EACH COLUMN

dfpivot = dfs.groupBy("user").pivot("date").agg({"user":"count"}).na.fill(0)
display(dfpivot)


# COMMAND ----------

# DBTITLE 1,Getting all column name except column[user] in `columname`
#COLLECTING LIST OF ALL COLUMN NAME
columname = dfpivot.columns

# REMOVING FIRST `USER ` COLUMN FROM LIST
columname.pop(0)
columname

# COMMAND ----------

'''

#ds = dftest
#datepivot = dfpivot

#le = length1

#de = df0

'''

# COMMAND ----------

from functools import reduce
from pyspark.sql.functions import *

length1 = len(dfpivot.columns)
#saving same dataframe to other
dftest=dfpivot

#CACHNING THE DATAFRAME IN MEMORY FOR FAST EXECUTION
dftest.cache()

testeddfs = 0
values=0
countvalue = []
fromdatelist=[]
todatelist=[]

#function
'''
def column_add(a,b):
  return a+b
'''

#running first loop
for first in range(1,length1):
   
  # running second loop from 1/1/19 onwards to all value and so on...  
  for second in range(first, length1):
    
  
    a=str(dftest[first])
    #splitting the data into particular column name
    a_split=a.split("'")[1]
    fromdatelist.append(a_split)  
  
    
    b = str(dftest[second])
    b_split=b.split("'")[1]
    todatelist.append(b_split)
  
    # applying reduce operation on each column [REDUCE TAKE TWO ARGUMENT A & B]
    df0 = dftest.withColumn('total val', reduce(lambda x,y: x+y, (dftest[second1] for second1 in range(first, second+1)) ) )
    
    '''
    #PRINTING THE FIRST AND SECOND SEQUENCE VALUE
    #print("column", first, second)
    
    #SHOWING ALL THE COLUMN OF TOTAL VAL COLUMN
    #df0.select("total val").show()
    '''
    
    testeddfs = df0.filter(df0["total val"] > 0)
  
    values = testeddfs.select("total val").count()
    countvalue.append(values)


# COMMAND ----------

# DBTITLE 1,Creating pandas dataframe from list 
lastdata = {"from": fromdatelist,
           "to": todatelist,
           "count": countvalue}

dfpd = pd.DataFrame(lastdata)

# CHANGING SCHEMA FROM STRING TO TIMESTAMP FOR EVERY DATE
dfpd["from"] = pd.to_datetime(dfpd["from"])
dfpd["to"] = pd.to_datetime(dfpd["to"])


display(dfpd)

# COMMAND ----------

# DBTITLE 1,Spark dataframe with the final value.
#CREATING SPARK DATAFRAME FROM THE PANDAS DATAFRAME
dflast = sc.createDataFrame(dfpd)
display(dflast)
