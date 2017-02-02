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

Top10HighPrcChgDayRpt = "/data/processed/monthly_reports/"+sys.argv[1]+"/"+sys.argv[2]+"/Top10HighPrcChgDayRpt"
Top10HighPrcChgMonRpt = "/data/processed/monthly_reports/"+sys.argv[1]+"/"+sys.argv[2]+"/Top10HighPrcChgMonRpt"
Top10LowPrcChgMonRpt  = "/data/processed/monthly_reports/"+sys.argv[1]+"/"+sys.argv[2]+"/Top10LowPrcChgMonRpt"


#top10HighVolume = sqlContext.sql("select xchange,'"+sys.argv[2]+"',symbol,sum(volume) as total_volume from tradingdb.eod_stock_data where xchange='"+sys.argv[1]+"' and date_format(trade_date,'YYYY-MM') = '"+sys.argv[2]+"'  group by xchange,symbol order by total_volume desc limit 10")

#top10HighVolume.save(Top10HighVolumeRpt,"com.databricks.spark.csv")

#top10LowVolume = sqlContext.sql("select xchange,'"+sys.argv[2]+"',symbol,sum(volume) as total_volume from tradingdb.eod_stock_data where xchange='"+sys.argv[1]+"' and date_format(trade_date,'YYYY-MM') = '"+sys.argv[2]+"'  group by xchange,symbol order by total_volume limit 10")

#top10LowVolume.save(Top10LowVolumeRpt,"com.databricks.spark.csv")

#top10HighPrcChgDay = sqlContext.sql("select xchange,'"+sys.argv[2]+"',symbol,trade_date,open,close,cast(100*(close-open)/open as decimal(5,2)) as prcchgpct from tradingdb.eod_stock_data where xchange='"+sys.argv[1]+"' and date_format(trade_date,'YYYY-MM') = '"+sys.argv[2]+"'  order by prcchgpct desc limit 10")

#top10HighPrcChgDay.save(Top10HighPrcChgDayRpt,"com.databricks.spark.csv")

firstDayofMon = sqlContext.sql("select a.xchange,a.symbol,a.trade_date,a.open,a.high,a.low,a.close,a.volume from tradingdb.eod_stock_data a inner join (select symbol,min(trade_date) as max_trade_date from tradingdb.eod_stock_data where xchange='"+sys.argv[1]+"' and date_format(trade_date,'YYYY-MM') = '"+sys.argv[2]+"' group by symbol) b on a.symbol=b.symbol and a.trade_date = b.max_trade_date and date_format(trade_date,'YYYY-MM') = '"+sys.argv[2]+"'")
firstDayofMon.registerTempTable("firstDayofMon")


lastDayofMon = sqlContext.sql("select a.xchange,a.symbol,a.trade_date,a.open,a.high,a.low,a.close,a.volume from tradingdb.eod_stock_data a inner join (select symbol,max(trade_date) as max_trade_date from tradingdb.eod_stock_data where xchange='"+sys.argv[1]+"' and date_format(trade_date,'YYYY-MM') = '"+sys.argv[2]+"'   group by symbol) b on a.symbol=b.symbol and a.trade_date = b.max_trade_date and date_format(trade_date,'YYYY-MM') = '"+sys.argv[2]+"' ")
lastDayofMon.registerTempTable("lastDayofMon")


top10HighPrcChgMonRpt = sqlContext.sql("select a.xchange,'"+sys.argv[2]+"',a.symbol,100*(a.close-b.close)/a.close as changePct from firstDayofMon a, lastDayofMon b where a.symbol=b.symbol order by changePct desc limit 10")

top10HighPrcChgMonRpt.write.save(Top10HighPrcChgMonRpt,"com.databricks.spark.csv")

top10LowPrcChgMonRpt = sqlContext.sql("select a.xchange,'"+sys.argv[2]+"',a.symbol,100*(a.close-b.close)/a.close as changePct from firstDayofMon a, lastDayofMon b where a.symbol=b.symbol order by changePct limit 10")

top10LowPrcChgMonRpt.write.save(Top10LowPrcChgMonRpt,"com.databricks.spark.csv")
