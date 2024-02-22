package spark
import org.apache.spark._ 
import org.apache.spark.sql._ 
import org.apache.log4j._ 
import org.apache.spark.sql.types.{StructType, StructField, StringType} 
import org.apache.spark.sql.Row

object textReader {
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
         
          //make sure the files is space demilited between each columns and Row is for no of columns
      val rowRDD = geo.map(_.split(" ")).map(x => Row(x(0),x(1),x(2),x(3),x(4)))
      
      val geodf = sqlContext.createDataFrame(rowRDD, schema)
      geodf.registerTempTable("geo")
      
      val allrecords = sqlContext.sql("select * from geo")
      
      allrecords.show(5,false)
      allrecords.write.saveAsTable("geoInfo")
      allrecords.printSchema()
      
      val transformedData = sqlContext.sql("""select cast(date as string) as date from geo where type like '%application%'""")
        
        transformedData.printSchema()
        transformedData.show()
        sc.stop()
      
    }
}