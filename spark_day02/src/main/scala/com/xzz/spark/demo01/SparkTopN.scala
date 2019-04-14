package com.xzz.spark.demo01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object SparkTopN {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkTopN")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")
    val fileRDD: RDD[String] = sparkContext.textFile("file:///H:\\23期大数据\\就业班\\7spark\\spark的四天资料\\2、spark第二天\\资料\\运营商日志")
    //切割过滤长度大于10的数组
    val map: RDD[Array[String]] = fileRDD.map(x => x.split(" "))
    //过滤长度大于10的数组
    val filter: RDD[Array[String]] = map.filter(x => x.length > 10)
    //获取来访url,并且给每个url记作1
    val urlAndOne: RDD[(String, Int)] = filter.map(x => (x(10), 1))
    //对url累加

    val countKey: RDD[(String, Int)] = urlAndOne.reduceByKey((x, y) => x + y)

    //按照出现的次数进行排序
    val sortBy: RDD[(String, Int)] = countKey.sortBy(x => x._2, false)
    //获取第一个
    val first: (String, Int) = sortBy.first()
    println(first._1)
    println(first._2)
    val take: Array[(String, Int)] = sortBy.take(5)
    val toBuffer: mutable.Buffer[(String, Int)] = take.toBuffer
    println(toBuffer)
    sparkContext.stop()


  }
}
