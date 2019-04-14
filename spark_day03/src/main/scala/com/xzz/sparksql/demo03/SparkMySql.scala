package com.xzz.sparksql.demo03

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkMySql {


  def main(args: Array[String]): Unit = {
    //获取sparksession
    val sparkSession: SparkSession = SparkSession.builder().master("local[2]").appName("sparkMySql").getOrCreate()
    val url: String = "jdbc:mysql://localhost:3306/spark"
    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","root")
    val jdbc: DataFrame = sparkSession.read.jdbc(url,"iplocation",prop)
    jdbc.show()
    sparkSession.close()

  }


}
