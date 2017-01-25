import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat
import java.util.Calendar

object firstApp {
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Trading App"))

    val today = Calendar.getInstance
    today.add(Calendar.DATE,-1)
    val yesterday = today.getTime
    today.add(Calendar.DATE,-29)
    val monthOld = today.getTime
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val endDate = dateFormat.format(yesterday)
    System.out.println(endDate.toString) //2017-01-21
    val startDate = dateFormat.format(monthOld)
    System.out.println(startDate.toString) //2017-01-21
    // read in text file and split each document into words
    //val enddt = sc.textFile(args(0)).map(_.split(",")(2)).sortBy(false).top(1)

    //val tokenized = sc.textFile(args(0)).map(_.split(",")).map(x=>(x(0),x(6).toInt)).reduceByKey(_+_)
    //val totalRecords = sc.textFile(args(0)).map(_.split(","))    
    val aMonthRecords = sc.textFile(args(0)).map(_.split(",")).filter(x=>(x(1)<=endDate.toString)).filter(x=>(x(1)>=startDate.toString))
    val totalVolumeRDD = aMonthRecords.map(x=>(x(0),x(6).toInt)).reduceByKey((a,b)=> a+b)
    val monthlyHighRDD = aMonthRecords.map(x=>(x(0),x(3).toFloat)).reduceByKey(math.max(_, _))
    val monthlyLowRDD = aMonthRecords.map(x=>(x(0),x(4).toFloat)).reduceByKey(math.min(_, _))
    val maxDailyDiffRDD = aMonthRecords.map(x=>(x(0),x(5).toFloat-x(2).toFloat)).reduceByKey(math.max(_, _))
    //val calcedRDD  = totalVolumeRDD.join(monthlyHighRDD).join(monthlyLowRDD).sortByKey().map(x=>(x._1,x._2._1._1,x._2._1._2,x._2._2))


    //System.out.println("Total Records are " + totalRecords + " a Month records " + aMonthRecords);

    //System.out.println(calcedRDD.collect().mkString(", "))
    System.out.println(maxDailyDiffRDD.sortByKey().collect().mkString(", "))

  }

}
