package com.xzz.spark.demo01

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SparkCount {
  def main(args: Array[String]): Unit = {
    //读取数据 统计pv
    val sparkCount: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkCount")
    val sparkContext = new SparkContext(sparkCount)
    sparkContext.setLogLevel("WARN")
    //读取文件,获取一个rdd,rdd里面装的就是文件内容
    val file: RDD[String] = sparkContext.textFile("file:///H:\\23期大数据\\就业班\\7spark\\spark的四天资料\\2、spark第二天\\资料\\运营商日志")
    //将rdd中的数据 缓存起来
//    file.cache()
//    file.persist(StorageLevel.MEMORY_AND_DISK_2)

    //统计多少行
    val count: Long = file.count()
    println(count)
    sparkContext.stop()

  }

}
