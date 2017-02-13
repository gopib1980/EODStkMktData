package org.gopib.sparkApps.EODMiner

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat
import java.util.Calendar


class StockDataMinerJob(val sc: SparkContext, val eodData: String, val xchange: String, val tdate: String) {

    var tradeDates: Array[String] = null

    private def calcChange(a: Iterable[Float]) = {
	val it = a.iterator
	val startClosePrice = it.next()
	if (it.hasNext)
        {	val endClosePrice  = it.next()
	        ((endClosePrice-startClosePrice)*100/startClosePrice)
        }
	else
	 	0
    }
    private def calcDate(a: String) = {
        val today = Calendar.getInstance
        val regPattern = Array(0,-1,-9,-29,-49,-199,-365,-370)

	if (!tdate.equals(null) && !tdate.equals(" ")) 
	{
             val dteFields = tdate.split("-")
             today.set(dteFields(0).toInt,dteFields(1).toInt-1,dteFields(2).toInt)
 	}
        val tempDay = a match {
             case "TRADEDTE"  => { today.add(Calendar.DATE,regPattern(0));  today.getTime }
	     case "PREVTRDT"  => { today.add(Calendar.DATE,regPattern(1));  today.getTime }
	     case "10DAYSBK"  => { today.add(Calendar.DATE,regPattern(2));  today.getTime }
	     case "30DAYSBK"  => { today.add(Calendar.DATE,regPattern(3));  today.getTime }
	     case "50DAYSBK"  => { today.add(Calendar.DATE,regPattern(4));  today.getTime }
	     case "2HDAYSBK"  => { today.add(Calendar.DATE,regPattern(5));  today.getTime }
	     case "01YEARBK"  => { today.add(Calendar.DATE,regPattern(6));  today.getTime }
	     case "1Y4DAYBK"  => { today.add(Calendar.DATE,regPattern(7));  today.getTime }
        }
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
        dateFormat.format(tempDay)
    }

    private def findAvailableDate(newDate: String) : String  = {
     	for (x <- tradeDates) {
       	if (x == newDate)
          	return newDate
       	if (x < newDate)
          	return x
    	}
    	return newDate  
    }

    def run() = {
    // calculate all the required dates for stock price and volume statistics
    val tradeDate   = calcDate("TRADEDTE")
    var oneYr4DBack = calcDate("1Y4DAYBK")

    // create a RDD with the past 52 week + 4 day's  backdata and cache it
    val eoddataRDD = sc.textFile(eodData).map(_.split(",")).filter(_(1)>=oneYr4DBack.toString).filter(_(6).toInt>0).cache()

    var tradeDates = eoddataRDD.map(x=>(x(1),1)).distinct().sortByKey(false).map(x=>x._1).collect()

    val prevTrDte   = findAvailableDate(calcDate("PREVTRDT"))
    val tenDaysBack = findAvailableDate(calcDate("10DAYSBK"))
    val amonthBack  = findAvailableDate(calcDate("30DAYSBK"))
    val fiftyDaysBk = findAvailableDate(calcDate("50DAYSBK"))
    var twoHnDaysBk = findAvailableDate(calcDate("2HDAYSBK"))
    var oneYearBack = findAvailableDate(calcDate("01YEARBK"))

    
    System.out.println("Trade Date is       " + tradeDate.toString)
    System.out.println("1Yr 4 day  is       " + oneYr4DBack.toString)
    System.out.println("Prev Tr Dt is       " + prevTrDte.toString)
    System.out.println("10 days bk is       " + tenDaysBack.toString)
    System.out.println("50 days bk is       " + fiftyDaysBk.toString)
    System.out.println("200days bk is       " + twoHnDaysBk.toString)
    System.out.println("1 Year bck is       " + oneYearBack.toString)
 

    val prevDayClosePrcRDD = eoddataRDD.filter(_(1)==prevTrDte.toString).map(x=>(x(0),x(5)))


    var volChangedates = List(oneYearBack.toString,tradeDate.toString)

    // create an RDD for the slice of data for lastday, last 30 days and 31 days
    val slicedTrdayRDD  = eoddataRDD.filter(x=>(x(1)==tradeDate.toString))
    val sliced52WksRDD  = eoddataRDD.filter(x=>(x(1)<=tradeDate.toString)).filter(x=>(x(1)>=oneYearBack.toString))
    val sliced30DayRDD  = eoddataRDD.filter(x=>(x(1)<=tradeDate.toString)).filter(x=>(x(1)>=amonthBack.toString))
    val sliced10DayRDD  = eoddataRDD.filter(x=>(x(1)<=tradeDate.toString)).filter(x=>(x(1)>=tenDaysBack.toString))
    val edge52WeeksRDD  = eoddataRDD.filter(x=>(x(1)<=tradeDate.toString)).filter{case (x) => volChangedates.contains(x(1))} 
    val sliced2HDayRDD  = eoddataRDD.filter(x=>(x(1)<=tradeDate.toString)).filter(x=>(x(1)>=twoHnDaysBk.toString))
    val sliced50DayRDD  = eoddataRDD.filter(x=>(x(1)<=tradeDate.toString)).filter(x=>(x(1)>=fiftyDaysBk.toString))

    
    // create a KV pair with todays price range high-low 
    val todaysPriceRDD = slicedTrdayRDD.map(x=>(x(0),f"${x(2)},${x(5)},'${x(4)}-${x(3)}',${x(6)}")).join(prevDayClosePrcRDD).map(x=>(x._1,f"${x._2._2},${x._2._1}"))

    // create a KV pair with 30 day price range high-low 
    val monthlyHighRDD = sliced52WksRDD.map(x=>(x(0),x(3).toFloat)).reduceByKey(math.max(_, _))
    val monthlyLowRDD  = sliced52WksRDD.map(x=>(x(0),x(4).toFloat)).reduceByKey(math.min(_, _))
    val monthPriceRDD  = monthlyHighRDD.join(monthlyLowRDD).map(x=>((x._1),f"'${x._2._2}-${x._2._1}'"))
    val prcStatsRDD    = todaysPriceRDD.join(monthPriceRDD).map(x=>((x._1),f"${x._2._1},${x._2._2}"))
   
    // create a KV pair with average and total volume for 30 days and 10 days
    val avg30DayVolRDD   = sliced30DayRDD.map(x=>(x(0),x(6).toInt)).aggregateByKey((0,0)) ((a,b) => (a._1+b,a._2+1),(a,b) => (a._1+b._1,a._2+b._2)).mapValues(a => (a._1,1.0 *a._1/a._2))
    val avg10DayVolRDD   = sliced10DayRDD.map(x=>(x(0),x(6).toInt)).aggregateByKey((0,0)) ((a,b) => (a._1+b,a._2+1),(a,b) => (a._1+b._1,a._2+b._2)).mapValues(a => (a._1,1.0 *a._1/a._2))        
    val vol30DayChgRDD   = edge52WeeksRDD.map(x=>(x(0),x(5).toFloat)).groupByKey().map(x=> (x._1,calcChange(x._2)))
    val volStatsRDD      = avg30DayVolRDD.join(avg10DayVolRDD).join(vol30DayChgRDD).map(x=>(x._1,f"${x._2._1._1._2}%010.0f,${x._2._1._2._2}%010.0f,'${x._2._2}%+5.2f'"))
    val avg200DayClRDD   = sliced2HDayRDD.map(x=>(x(0),x(5).toFloat)).aggregateByKey((0.0,0)) ((a,b) => (a._1+b,a._2+1),(a,b) => (a._1+b._1,a._2+b._2)).mapValues(a => (1.0 *a._1/a._2))
    val avg50DayClRDD    = sliced50DayRDD.map(x=>(x(0),x(5).toFloat)).aggregateByKey((0.0,0)) ((a,b) => (a._1+b,a._2+1),(a,b) => (a._1+b._1,a._2+b._2)).mapValues(a => (1.0 *a._1/a._2))
    val movingAvgPrc     = avg200DayClRDD.join(avg50DayClRDD).map(x=>(x._1,f"${x._2._1}%09.2f,${x._2._2}%09.2f,'${xchange}','${tradeDate.toString}'"))

    // merger all the statistics into a single RDD
    val outFormattedRDD  = prcStatsRDD.join(volStatsRDD).map(x=>(x._1,f"${x._2._1},${x._2._2}"))

    outFormattedRDD.join(movingAvgPrc).map(x=>f"${x._1},${x._2._1},${x._2._2}")
 
   }
}
