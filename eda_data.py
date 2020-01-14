from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark

conf = pyspark.SparkConf().setAppName('appName').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)
#comment check

from pyspark import HiveContext

sqlContext = HiveContext(sc)

sql = sqlContext.sql

stores_df = sqlContext.read.csv("./stores.csv",header=True,inferSchema=True)

stores_df.show(10,False)

sc.stop()