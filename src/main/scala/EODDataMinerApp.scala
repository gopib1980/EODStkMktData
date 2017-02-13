package org.gopib.sparkApps.EODMiner

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object EODDataMinerApp {

  def main(args: Array[String]) {

    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Stock Market Data mining App"))

    // determine input file name
    val eodData = args(0) +"/xchange="+ args(1)

    // determine output file name
    val outStatsData = "/data/stock_statistics/xchange="+args(1)+"/trade_date=" + args(2)

    // create a new StockDataMinerJob object
    val job   = new StockDataMinerJob(sc,eodData,args(1),args(2))

    // run the job
    val outRDD = job.run()

    // Write the RDD to the hive managed external table
    outRDD.saveAsTextFile(outStatsData)

    // stop the Spark context
    sc.stop()

  }

}
