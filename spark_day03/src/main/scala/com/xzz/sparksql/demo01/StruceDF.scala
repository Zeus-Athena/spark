package com.xzz.sparksql.demo01

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object StructDF {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().master("local[3]").appName("structType").getOrCreate()
    val sparkContext: SparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel("WARN")
    val fileRDD: RDD[String] = sparkContext.textFile("file:///H:\\23期大数据\\就业班\\7spark\\spark的四天资料\\3、spark第三天\\资料\\person.txt")
    val map: RDD[Array[String]] = fileRDD.map(x=>x.split(" "))
    val rowRDD: RDD[Row] = map.map(x=>Row(x(0).toInt,x(1),x(2).toInt))
    /**
      * 传递两个参数,一个是RDD[ROW]
      * 另一个参数是structType
      * rowRDD:RDD[row]  schema:StructType
      */
    val structType: StructType = new StructType().add("id",IntegerType,true).add("name",StringType,true).add("age",IntegerType,true)

    val dataFrame: DataFrame = sparkSession.createDataFrame(rowRDD,structType)
    dataFrame.registerTempTable("person")
    dataFrame.createOrReplaceTempView("person2")
    sparkSession.sql("select * from person2").show()
    sparkContext.stop()
    sparkSession.close()




















  }

}
