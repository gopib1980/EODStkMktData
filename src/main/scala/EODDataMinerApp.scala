import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat
import java.util.Calendar

object EODDataMinerApp {
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Data mining App"))

    // determine input file name
    val eodData = args(0) +"/xchange="+ args(1)

    val tradeDates = sc.textFile(eodData).map(_.split(",")).map(x=>(x(1),1)).distinct().sortByKey(false).collect()


    // calculate all the required dates for stock price and volume statistics
    val end30Day   = calcDate(args(2),"TRADEDTE")
    val start10Day = calcDate(args(2),"10DAYSBK")
    val start30Day = calcDate(args(2),"30DAYSBK")
    var start31Day = calcDate(args(2),"31DAYSBK")

    start31Day = findAvailableDate(start31Day,tradeDates)

    var volChangedates = List(start31Day.toString,end30Day.toString)

    // determine output file name
    val outStatsData = "/data/processed/stock_statistics/xchange="+args(1)+"/trade_date=" + end30Day.toString

    // create an RDD for the slice of data for lastday, last 30 days and 31 days
    val slicedYedayRDD  = sc.textFile(eodData).map(_.split(",")).filter(x=>(x(1)==end30Day.toString))
    val sliced30DayRDD  = sc.textFile(eodData).map(_.split(",")).filter(x=>(x(1)<=end30Day.toString)).filter(x=>(x(1)>=start30Day.toString))
    val sliced10DayRDD  = sc.textFile(eodData).map(_.split(",")).filter(x=>(x(1)<=end30Day.toString)).filter(x=>(x(1)>=start10Day.toString))
    val sliced31DayRDD  = sc.textFile(eodData).map(_.split(",")).filter{case (x) => volChangedates.contains(x(1))} 

    // create a KV pair with todays price range high-low 
    val todaysPriceRDD = slicedYedayRDD.map(x=>(x(0),f"'${x(3)}-${x(4)}',${x(6)}"))

    // create a KV pair with 30 day price range high-low 
    val monthlyHighRDD = sliced30DayRDD.map(x=>(x(0),x(3).toFloat)).reduceByKey(math.max(_, _))
    val monthlyLowRDD  = sliced30DayRDD.map(x=>(x(0),x(4).toFloat)).reduceByKey(math.min(_, _))
    val monthPriceRDD  = monthlyHighRDD.join(monthlyLowRDD).map(x=>((x._1),f"'${x._2._1}-${x._2._2}'"))
    val prcStatsRDD    = todaysPriceRDD.join(monthPriceRDD).map(x=>((x._1),f"${x._2._1},${x._2._2}"))
   
    // create a KV pair with average and total volume for 30 days and 10 days
    val avg30DayVolRDD   = sliced30DayRDD.map(x=>(x(0),x(6).toInt)).aggregateByKey((0,0)) ((a,b) => (a._1+b,a._2+1),(a,b) => (a._1+b._1,a._2+b._2)).mapValues(a => (a._1,1.0 *a._1/a._2))
    val avg10DayVolRDD   = sliced10DayRDD.map(x=>(x(0),x(6).toInt)).aggregateByKey((0,0)) ((a,b) => (a._1+b,a._2+1),(a,b) => (a._1+b._1,a._2+b._2)).mapValues(a => (a._1,1.0 *a._1/a._2))        
    val vol30DayChgRDD   = sliced31DayRDD.map(x=>(x(0),x(5).toFloat)).groupByKey().map(x=> (x._1,calcChange(x._2)))
    val volStatsRDD      = avg30DayVolRDD.join(avg10DayVolRDD).join(vol30DayChgRDD).map(x=>(x._1,f"${x._2._1._1._1},${x._2._1._1._2}%010.0f,${x._2._1._2._2}%010.0f,'${x._2._2}%+5.2f'"))

    // merger all the statistics into a single RDD
    val finalStatsRDD    = prcStatsRDD.join(volStatsRDD).map(x=>f"'${x._1}',${x._2._1},${x._2._2},'${args(1)}','${end30Day.toString}'")

    // Write the RDD to the hive managed external table
    finalStatsRDD.saveAsTextFile(outStatsData)

  }
  def calcChange(a: Iterable[Float]) = {
	val it = a.iterator
	val startClosePrice = it.next()
	if (it.hasNext)
        {	val endClosePrice  = it.next()
	        ((endClosePrice-startClosePrice)*100/startClosePrice)
        }
	else
	 	0
  }
  def calcDate(newDate: String, a: String) = {
        val today = Calendar.getInstance
        val regPattern = Array(0,-9,-29,-30)

	if (!newDate.equals(null) && !newDate.equals(" ")) 
	{
             val dteFields = newDate.split("-")
             today.set(dteFields(0).toInt,dteFields(1).toInt-1,dteFields(2).toInt)
 	}
        val tempDay = a match {
             case "TRADEDTE"  => { today.add(Calendar.DATE,regPattern(0));  today.getTime }
	     case "10DAYSBK"  => { today.add(Calendar.DATE,regPattern(1));  today.getTime }
	     case "30DAYSBK"  => { today.add(Calendar.DATE,regPattern(2));  today.getTime }
	     case "31DAYSBK"  => { today.add(Calendar.DATE,regPattern(3));  today.getTime }
        }
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
        dateFormat.format(tempDay)
  }

  def findAvailableDate(newDate: String, tDates: Array[(String,Int)]) : String  = {
     for (x <- tDates) {
       if (x._1 == newDate)
          return newDate
       if (x._1 < newDate)
          return x._1
    }
    return newDate  
  }
}
