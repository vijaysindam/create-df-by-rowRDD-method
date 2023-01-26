# create-df-by-rowRDD-method



package pack

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object obj {
  
 
  
  def main(args:Array[String]) : Unit = {
    
    
   val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
   val sc = new SparkContext(conf)
   sc.setLogLevel("ERROR")
   val spark = SparkSession.builder().getOrCreate()
   import spark.implicits._
   
   val data = sc.textFile("file:///c:/data/datatxns.txt",1)
   
   val ddata = data.map(x=>x.split(","))
    
   val rowrdd = ddata.map(x=>Row(x(0),x(1),x(2),x(3)))
   
  val tschema = StructType(Array(
    StructField("id",StringType,true),
    StructField("date",StringType,true),
    StructField("catagory",StringType,true),
    StructField("product", StringType, true)
  ))
   
  val df = spark.createDataFrame(rowrdd,tschema)
 
  df.show()
  
  df.createOrReplaceTempView("checktable")
   
  val ddf= spark.sql("select * from checktable where catagory = 'Exercise'")
   
  ddf.show()
    
  }
  
  
}
