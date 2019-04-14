package com.xzz.sparksql.demo05

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}


object SparkScore {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().master("local[2]").appName("maxScore").getOrCreate()
    val sparkContext: SparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel("WARN")
    val json: DataFrame = sparkSession.read.json("file:///H:\\23期大数据\\就业班\\7spark\\spark的四天资料\\3、spark第三天\\资料\\score.txt")
    json.createOrReplaceTempView("score")

    /**
      * "常用分析函数：（最常用的应该是1.2.3 的排序）\n" +
      * "   1、row_number() over(partition by ... order by ...)\n" +
      * "   2、rank() over(partition by ... order by ...)\n" +
      * "   3、dense_rank() over(partition by ... order by ...)\n" +
      * "   4、count() over(partition by ... order by ...)\n" +
      * "   5、max() over(partition by ... order by ...)\n" +
      * "   6、min() over(partition by ... order by ...)\n" +
      * "   7、sum() over(partition by ... order by ...)\n" +
      * "   8、avg() over(partition by ... order by ...)\n" +
      * "   9、first_value() over(partition by ... order by ...)\n" +
      * "   10、last_value() over(partition by ... order by ...)\n" +
      * "   11、lag() over(partition by ... order by ...)\n" +
      * "   12、lead() over(partition by ... order by ...)\n" +
      * select clazz,count(1) from  score group by clazz
      * select name,clazz,score ,count()over (partition by clazz order by score )
      */
//  sparkSession.sql("select * from (select name,clazz,score,rank() over (partition by clazz order by score desc) rankNum from score) temp where temp.rankNum <=2").show()
  sparkSession.sql("select * from (select name,clazz,score,rank() over (partition by clazz order by score desc) rankNum from score) temp where temp.rankNum <=2").show()
    sparkContext.stop()
    sparkSession.close()
  }
}
