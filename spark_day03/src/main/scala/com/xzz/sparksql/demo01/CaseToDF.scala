package com.xzz.sparksql.demo01

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

//样例类
case class Person(id:Int,name:String,age:Int)
object CaseToDF {
  def main(args: Array[String]): Unit = {
    //获取sparkSession
    val sparkSession: SparkSession = SparkSession.builder().appName("sparkSQL").master("local[3]").getOrCreate()
    //获取sparkContext读取文件
    val sparkContext: SparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel("WARN")
    val fileRDD: RDD[String] = sparkContext.textFile("file:///H:\\23期大数据\\就业班\\7spark\\spark的四天资料\\3、spark第三天\\资料\\person.txt")
    val map: RDD[Array[String]] = fileRDD.map(x=>x.split(" "))
    val personRDD: RDD[Person] = map.map(x=>Person(x(0).toInt,x(1),x(2).toInt))
    //导入隐式转换的包
    import sparkSession.implicits._
    //通过toDF方法,将我们的rdd转化成df
    val personDF: DataFrame = personRDD.toDF()
    //查看schema信息
    personDF.printSchema()

    println("==================DSL语法风格开始==============")
    personDF.select("name").show()
    personDF.select($"name").show()
    personDF.select(personDF.col("name")).show()
    personDF.select(col = "name").show()
    //显示name以及age字段,并且将age+1
    personDF.select($"name",$"age",$"age"+1).show()
    //按照年龄进行分组,统计每组中有多少人
    personDF.groupBy($"age").count().show()
    //过滤年龄大于28岁的人


    personDF.filter($"age">28).show()
    println("==================DSL语法风格结束==============")
    println("==================SQL语法风格开始==============")
    personDF.registerTempTable("person")
    personDF.createOrReplaceTempView("person2")
    sparkSession.sql("select * from person left join person2 on person.id=person2.id").show()
    sparkSession.sql("select count(1) from person2").show()
    println("**********")
    sparkSession.sql("select age,count(age) from person group by age ").show()
    println("**********")
    println("==================SQL语法风格结束==============")
    //关闭资源
    sparkContext.stop()
    sparkSession.close()
  }

}
