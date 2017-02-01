import sys
from operator import add

from pyspark import SparkConf, SparkContext, HiveContext

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("   usage: createMonthlyReports.py <arg1>  <arg2>")
        print("   where... ")
        print("          arg1 can be NYSE or NASDAQ")
        print("          arg2 can be in YYYY-MM format")
        sys.exit(2)
    if sys.argv[1] not in ("NYSE","NASDAQ") :
        print("   NYSE and NASDAQ only is currently supported")
        sys.exit(2)
    arg2 = sys.argv[2].split('-')

    if int(arg2[0]) <=2016 and int(arg2[1]) < 12:
        print("We have data only from Dec 2016")
        sys.exit(2)

conf = (SparkConf()
         .setMaster("local")
         .setAppName("Monthly Report Creator"))

sc = SparkContext(conf = conf)

sqlContext = HiveContext(sc)

Top10HighVolumeRpt = "/data/processed/monthly_reports/"+sys.argv[1]+"/"+sys.argv[2]+"/Top10HighVolumeRpt"
Top10LowVolumeRpt = "/data/processed/monthly_reports/"+sys.argv[1]+"/"+sys.argv[2]+"/Top10LowVolumeRpt"


top10HighVolume = sqlContext.sql("select xchange,'"+sys.argv[2]+"',symbol,sum(volume) as total_volume from tradingdb.eod_stock_data where xchange='"+sys.argv[1]+"' and date_format(trade_date,'YYYY-MM') = '"+sys.argv[2]+"'  group by xchange,symbol order by total_volume desc limit 10")

top10HighVolume.save(Top10HighVolumeRpt,"com.databricks.spark.csv")

top10LowVolume = sqlContext.sql("select xchange,'"+sys.argv[2]+"',symbol,sum(volume),'"+sys.argv[2]+"' as total_volume from tradingdb.eod_stock_data where xchange='"+sys.argv[1]+"' and date_format(trade_date,'YYYY-MM') = '"+sys.argv[2]+"'  group by xchange,symbol order by total_volume limit 10")

top10LowVolume.save(Top10LowVolumeRpt,"com.databricks.spark.csv")

