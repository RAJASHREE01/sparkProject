package spark

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._

object jsonReader {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var conf = new SparkConf().setAppName("sample")
    conf.setMaster("local")
    
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    def main(args: Array[String]):Unit = {
      val df = sqlContext.read.json("D:/Projects/data/dummy.json")
      df.printSchema()
      df.registerTempTable("JSONdata")
      
      val data=sqlContext.sql("select * from JSONdata")
      data.show()
      sc.stop()
    }
}