package com.xzz.scala.count

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object SparkWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkWordCount")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val file: RDD[String] = sc.textFile("file:///H:\\wordcount.txt")
    val words: RDD[String] = file.flatMap(x=>x.split(","))
    val wordAndOne: RDD[(String, Int)] = words.map(x=>(x,1))
    val wordResult: RDD[(String, Int)] = wordAndOne.reduceByKey((x,y) => x+y)
    val buffer: mutable.Buffer[(String, Int)] = wordResult.collect().toBuffer
    println(buffer)


  }
}
