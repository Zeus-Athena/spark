package com.xzz.sparksql.demo04

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 读取person.txt文件里面的数据,转换成df从df当中查询数据写入到mysql中
  */
case class Person(id:Int,name:String,age:Int)
object SparkWriteMysql {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("sparkWriteMysql")
//      .master("local[2]")
      .getOrCreate()
    val sparkContext: SparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel("WARN")
//    val fileRDD: RDD[String] = sparkContext.textFile("file:///H:\\23期大数据\\就业班\\7spark\\spark第三天视频资料\\person.txt")
    val fileRDD: RDD[String] = sparkContext.textFile(args(0))
    //配合样例类
    val map: RDD[Array[String]] = fileRDD.map(x=>x.split(" "))
    val personRDD: RDD[Person] = map.map(x=>Person(x(0).toInt,x(1),x(2).toInt))
    import sparkSession.implicits._
    val personDF: DataFrame = personRDD.toDF()
    //推荐使用这种方式来创建临时表
    personDF.createOrReplaceTempView("person")
    val result: DataFrame = sparkSession.sql("select * from person")

    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","root")
    val url = "jdbc:mysql://192.168.24.253:3306/spark"
    result.write.mode(SaveMode.Append).jdbc(url,"person",properties)
    sparkContext.stop()
    sparkSession.close()


  }
}
