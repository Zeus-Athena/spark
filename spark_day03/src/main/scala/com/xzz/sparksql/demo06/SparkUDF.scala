package com.xzz.sparksql.demo06

import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.api.java.{UDF1, UDF2}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
case class LowToUpper(low: String)
object SparkUDF {
  //通过spark自定义函数,将小写转大写
  def main(args: Array[String]): Unit = {
    //获取sparksession,读取文件 ,整成一张表
    val sparkSession: SparkSession = SparkSession.builder().master("local[2]").appName("sparkUDF").getOrCreate()
    val context: SparkContext = sparkSession.sparkContext
    context.setLogLevel("WARN")
    //读取文件,配合样例类,转换成为df
    val fileRDD: RDD[String] = context.textFile("file:///H:\\23期大数据\\就业班\\7spark\\spark的四天资料\\3、spark第三天\\资料\\udf.txt")
    val lowRDD: RDD[LowToUpper] = fileRDD.map(x=>LowToUpper(x))
    import sparkSession.implicits._
    val frame: DataFrame = lowRDD.toDF()
    //注册成为一张表
    frame.createOrReplaceTempView("lowTable")
    //注册我们的自定义函数
    sparkSession.udf.register("lowCaseUpper",new UDF1[String,String]{
      override def call(t1: String): String = {
        t1.toUpperCase()
      }
    },StringType)

    /**
      * 使用自定义函数
      */
    sparkSession.sql("select low,lowCaseUpper(low) as upper from lowTable").show()
    context.stop()
    sparkSession.close()
  }

}
