package com.xzz.spark.demo02

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CountIP {

  def ipToLong(eachIP: String): Long = {
    var finalResult:Long=0
    val split: Array[String] = eachIP.split("\\.")
    for (eachNum <- split) {
      finalResult = eachNum.toLong | finalResult<<8L

    }
    finalResult
  }


  def binarySearch(longIP: Long, value: Array[(String, String, String, String)]): Int = {
    var start:Int = 0
    var end:Int = value.length-1
    while (start <= end){
      //获取中间下标
      var middle = (start+end)/2
      //找到需要的下标
      if(longIP >=value(middle)._1.toLong && longIP <= value(middle)._2.toLong){
        return middle
      }
      if(longIP < value(middle)._1.toLong){
        end = middle -1
      }
      if (longIP>value(middle)._2.toLong){
        start = middle+1
      }

    }
    //为了避免程序报错直接返回0
    0
  }

  val data2Mysql= (result: Iterator[((String, String), Int)]) => {
    //获取连接
    val connection: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark","root","root")
    val statement: PreparedStatement = connection.prepareStatement("insert into iplocation (longitude,latitude,total_count) values(?,?,?)")
    //获取result中的每一个元素
    result.foreach(line=>{
      //将数据保存到mysql
      statement.setString(1,line._1._1)
      statement.setString(2,line._1._2)
      statement.setInt(3,line._2)
      statement.execute()
    })
    statement.close()
    connection.close()
  }

  def main(args: Array[String]): Unit = {

    /**
      * 如何处理，获取所有的字典IP值，ip起始值，ip结束值，经度，维度
      * 获取所有的用户ip，将用户的ip转换成为long类型
      * 将用户ip与字典ip进行二分查找，确定用户ip对应哪一个经纬度，将经纬度出现一次，记做1
      * 将所有相同的经纬度出现的次数进行累加
      * 得到的结果值(（112.35,26.78）,50)
      * 将最终的结果值，保存到mysql里面去
      * */
    val sparkContext = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("ipCount"))
    //读取文件获取ip起始值 结束值 经纬度
    val fileRDD: RDD[String] = sparkContext.textFile("file:///H:\\23期大数据\\就业班\\7spark\\spark的四天资料\\2、spark第二天" +
      "\\资料\\服务器访问日志根据ip地址查找区域\\用户ip上网记录以及ip字典\\ip.txt")
    val arrayRDD: RDD[Array[String]] = fileRDD.map(x=>x.split("\\|"))
    //获取ip的起始值 结束值 经度,维度
    val tupleRDD: RDD[(String, String, String, String)] = arrayRDD.map(arr=>(arr(2),arr(3),arr(arr.length-2),arr(arr.length-1)))
    //spark调优,尽量避免大的广播量
    val collect: Array[(String, String, String, String)] = tupleRDD.collect()
    //设置广播变量
    val broadcast: Broadcast[Array[(String, String, String, String)]] = sparkContext.broadcast(collect)
    //获取所有的用户文件
    ////(16777472|16778239 119.306239|26.075302 )  (16779264|16781311  113.280637|23.125178)
    val allUserFile: RDD[String] = sparkContext.textFile("file:///H:\\23期大数据\\就业班\\7spark\\s" +
      "park的四天资料\\2、spark第二天\\资料\\服务器访问日志根据ip地址查找区域\\用户ip上网记录以及ip字典\\20090121000132.394251.http.format")
    //获取所有的用户ip
    val userIP: RDD[String] = allUserFile.map(x=>x.split("\\|")(1))
    //userIP.mapPartitions 一次性获取所有的分区的数据,取出来的数据是可迭代的
    val countIpTimes: RDD[((String, String), Int)] = userIP.mapPartitions(x => {
      //获取广播变量值
      val value: Array[(String, String, String, String)] = broadcast.value
      //通过x调用mao方法,将每一个分区的里面的数据挨个取出来,叫eachIP
      x.map(eachIP => {
        //定义一个方法,将ip转为long类型
        val longIP: Long = ipToLong(eachIP)
        //定义一个方法将longIP以及广播变量长入进去,返回下一个标志
        val indexReturn: Int = binarySearch(longIP, value)
        //每一个经纬度出现一次记作1 将经纬度组织成为一个元组,相同的元组就是相同的key
        ((value(indexReturn)._3, value(indexReturn)._4), 1)
      })

    })
    //将经纬度出现的次数累加,然后添加到mysql中
    val key: RDD[((String, String), Int)] = countIpTimes.reduceByKey(_+_)

    for (item <- key.collect()) {
      println(item._1._1)
      println(item._1._2)
      println(item._2)
    }

    //将数据保存到MySQL中
    key.foreachPartition(data2Mysql)
  }





















}
