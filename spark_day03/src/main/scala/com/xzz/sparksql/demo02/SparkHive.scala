package com.xzz.sparksql.demo02

import org.apache.spark.sql.SparkSession

object SparkHive {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("sparkHive")
      .master("local[2]")
      .enableHiveSupport()//开启对hive sql的支持
      .getOrCreate()
    //创建表
    sparkSession.sql("create table student(id int,name string ,age int) row format delimited fields terminated by ','")
    //加载数据
    sparkSession.sql("load data local inpath './datas/student.csv' overwrite  into table student")
    sparkSession.sql("select * from student").show()
    sparkSession.close()
  }

}
