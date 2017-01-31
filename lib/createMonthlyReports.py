import sys
from operator import add

from pyspark import SparkConf, SparkContext, HiveContext

conf = (SparkConf()
         .setMaster("local")
         .setAppName("Monthly Report Creator"))

sc = SparkContext(conf = conf)

sqlContext = HiveContext(sc)


# Queries can be expressed in HiveQL.
results = sqlContext.sql("select xchange,symbol,sum(volume) as total_volume from tradingdb.eod_stock_data where xchange='NYSE' and month(trade_date) = 01 group by xchange,symbol order by total_volume desc limit 10").collect()

print (results)
