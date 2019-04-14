package com.xzz.java.count;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SparkJava {
    public static void main(String[] args) {
        //创建sparkContext对象
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkJava");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        //读取文件
        JavaRDD<String> javaRDD = javaSparkContext.textFile("file:///H:\\wordcount.txt");
        //调用flatMap  需要参数FlatMapFunction 两个泛型一个输入,一个输出
        JavaRDD<String> words = javaRDD.flatMap(new FlatMapFunction<String, String>() {
            //inputLine是输入数据
            @Override
            public Iterator<String> call(String inputLine) throws Exception {
                String[] split = inputLine.split(",");
                //将数组转换成迭代器
                return Arrays.asList(split).iterator();


            }
        });
        //将每个单词转换成元组
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String inputWord) throws Exception {

                return new Tuple2<String, Integer>(inputWord, 1);
            }
        });
        //将相同的key的value进行累加
        JavaPairRDD<String, Integer> reduceByKey = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //key,value反转
        JavaPairRDD<Integer, String> sortBy = reduceByKey.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<Integer, String>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }
        });
        //排序按照key
        JavaPairRDD<Integer, String> sorted = sortBy.sortByKey(false);
        //遍历
        List<Tuple2<Integer, String>> collect = sorted.collect();
        for (Tuple2<Integer, String> integerStringTuple2 : collect) {
            System.out.println(integerStringTuple2._2+","+integerStringTuple2._1);
        }

    }
}
