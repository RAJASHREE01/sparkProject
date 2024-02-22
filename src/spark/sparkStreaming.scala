package spark

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object sparkStreaming {
  
  def setUpLogging() = {
    import org.apache.log4j.{Level,Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
   }
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    setUpLogging()
    //batch interval 1
    val ssc = new StreamingContext(conf, Seconds(1))
    
    // terminal 9999 we will enter real time messages
    val lines = ssc.socketTextStream("localhost",9999)
    
    //flatmap to split the words and pair for key value to calculate count
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_+_)
    
    //print elements of each RDD
    wordCounts.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
} 