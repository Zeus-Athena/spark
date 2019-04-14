package com.xzz.spark.demo01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkUVCount {
  def main(args: Array[String]): Unit = {
    //统计网站的uv
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkUVCount")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")
    //读取文件
    val file: RDD[String] = sparkContext.textFile("file:///H:\\23期大数据\\就业班\\7spark\\spark的四天资料\\2、spark第二天\\资料\\运营商日志")
//    println(file.collect()(0).toBuffer)
    //获取所有的session,这里没有session使用ip代替
//    val value: RDD[Array[String]] = file.map(x=>x.split(" "))
//    println(value.collect()(0).toBuffer)
    val ipRDD: RDD[String] = file.map(x=>x.split(" ")(0))
    //去重求和
    val count: Long = ipRDD.distinct().count()
    println(count)
    sparkContext.stop()


  }

}
