package spark
import org.apache.spark._ 
import org.apache.spark.sql._ 
import org.apache.log4j._ 
import org.apache.spark.sql.types.{StructType, StructField, StringType} 
import org.apache.spark.sql.Row

object textFileWriter {
  Logger.getLogger("org").setLevel(Level.ERROR)
     
    var conf = new SparkConf().setAppName("sample")
    conf.setMaster("local")
    
    val sc = new SparkContext(conf)
    
    val sqlContext = new SQLContext(sc)
  
    def main(args: Array[String]): Unit = {
      val geo = sc.textFile("D:/Projects/Data/Geo_information.txt")
      val geoHeader = "Key Country Origin Comment Date"
      
      val schema = StructType(geoHeader.split(" ")
          .map(fieldName => StructField(fieldName, StringType, true)))
          
      val rowRDD = geo.map(_.split(" ")).map(x => Row(x(0),x(1),x(2),x(3),x(4)))
      
      //creating dataframe
      val geoDF = sqlContext.createDataFrame(rowRDD, schema)
      
      geoDF.repartition(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header","true").save("targetfile.csv")
      
      sc.stop()
  }
  
}