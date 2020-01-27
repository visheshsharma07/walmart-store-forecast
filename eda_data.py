# spark-submit --conf spark.dynamicAllocation.enabled=true --conf spark.sql.broadcastTimeout=-1 --conf spark.sql.autoBroadcastJoinThreshold=-1 eda_data.py
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark
from pyspark.sql.functions import broadcast
from pyspark import HiveContext
from pyspark.ml.stat import Summarizer
import pandas as pd

conf = pyspark.SparkConf().setAppName('MasterFrame').setMaster('local[*]')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)
sqlContext = HiveContext(sc)
sql = sqlContext.sql

# Stores CSV Dataframe
stores_df = sqlContext.read.csv("./stores.csv",header=True,inferSchema=True)

for i in stores_df.columns:
    stores_df = stores_df.withColumnRenamed(i,i+"_Stores")

test_df = sqlContext.read.csv("./test.csv",header=True,inferSchema=True)

# Train CSV Dataframe

train_df = sqlContext.read.csv("./train.csv",header=True,inferSchema=True)

for i in train_df.columns:
    train_df = train_df.withColumnRenamed(i,i+"_Train")

# Features Dataframe

features_df = sqlContext.read.csv("./features.csv",header=True,inferSchema=True)

for i in features_df.columns:
    features_df = features_df.withColumnRenamed(i,i+"_Features")

stores_df.printSchema()
train_df.printSchema()
features_df.printSchema()

# 1st Join

cond_1 = stores_df['Store_Stores'] == train_df['Store_Train']

tmp_ = train_df.join(stores_df, cond_1 ,"inner") \
                .drop('Store_Stores')

# 2nd Join

cond_2 = tmp_['Store_Train'] == features_df['Store_Features']
cond_3 = tmp_['Date_Train'] == features_df['Date_Features']

final_ = tmp_.join(features_df, (cond_2) & (cond_3),"inner") \
                .drop('Date_Train','Store_Train')

# for i in final_.columns:
#     final_.filter('{} is null'.format(i)).show(3,False)
# final_.printSchema()

pd_final = final_.toPandas()

pd_final.head() 

sc.stop()