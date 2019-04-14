package com.xzz.scala.count

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object SparkScala {

  def main(args: Array[String]): Unit = {
    //使用sparkContext的主构造器来创建sparkContext对象
    //创建sparkConf
    val sparkConf = new SparkConf().setAppName("sparkScala")
    //获取sc
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val file: RDD[String] = sc.textFile(args(0))
    //对文件内容进行切分压平
    val words: RDD[String] = file.flatMap(x => x.split(","))
    //将每个单词记作1
    val wordAndOne: RDD[(String, Int)] = words.map(x => (x, 1))
    //将相同的单词进行累加
    val wordAndResult: RDD[(String, Int)] = wordAndOne.reduceByKey((x, y) => x + y)
    //按照出现的次数排序
    val by: RDD[(String, Int)] = wordAndResult.sortBy(x => x._2, false)
    //返回tuple数组
    val collect: Array[(String, Int)] = by.collect()
    val toBuffer: mutable.Buffer[(String, Int)] = collect.toBuffer
    println(toBuffer)
    //把结果保存到文件
    by.saveAsTextFile(args(1))
    //关闭sc
    sc.stop()
  }
}
