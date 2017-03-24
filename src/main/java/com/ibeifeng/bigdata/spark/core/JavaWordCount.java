package com.ibeifeng.bigdata.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * 使用Java语言进行SparkCore程序开发
 *  -1, 回顾一下Spark Application三部曲
 *      Step 1: intput
 *          RDD <- SparkContext <- SparkConf
 *          sc.textFile()
 *      Step 2: process
 *          RDD#xx(transformation)
 *      Step 3:output
 *          RDD#xx(action)
 *  -2, 对比SCALA语言发言， 类比Java语言
 *      对于Java语言编程Spark开发的话，一定一定要开源码
 *          SCALA 高阶函数 ->  JAVA中的匿名内部类(call方法）  -  JDK 1.8之前
 *
 *   注意：
 *          JAVa语言和SCALA语言可以相互调用
 */
public class JavaWordCount {

    public static void main(String[] args) {

        // 创建SparkConf，读取配置信息
        SparkConf conf = new SparkConf()
            .setAppName(JavaWordCount.class.getSimpleName())
            .setMaster("local[2]") ;

        // 创建JavaSparkContext，构建Spark Application入口
        JavaSparkContext sc = new JavaSparkContext(conf) ;

/** ========================================================================== */
        // Step 1: intpu data -> RDD
        JavaRDD<String> rdd = sc.textFile("/test/wc.input") ;

        // Step 2: process data
        // TODO ：
        //      rdd.flatMap(line => line.split(" "))
        JavaRDD<String> wordRdd = rdd.flatMap(
                new FlatMapFunction<String, String>(){
                    // call method
                    public Iterable<String> call(String line) throws Exception {
                        // List本身就是Iterable,将数组转换为List
                        return Arrays.asList(line.split(" "));
                    }
                }
        );

        // TODO :
        //      rdd.map(word => (word, 1))
        JavaPairRDD<String, Integer> tupleRdd = wordRdd.mapToPair(
          new PairFunction<String, String, Integer>(){
              // call method
              public Tuple2<String, Integer> call(String word) throws Exception {
                  return new Tuple2<String, Integer>(word, 1);
              }
          }
        );

        // TODO :
        //      rdd.reduceByKey((a, b) => (a + b))
        JavaPairRDD<String, Integer> wordCountRdd = tupleRdd.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    // call method
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        // Step 3: output data
        // RDD#Collect
        List<Tuple2<String, Integer>> outputList = wordCountRdd.collect() ;
        // iterator
        for(Tuple2<String, Integer> output: outputList){
            System.out.println(output._1() + " : " + output._2());
        }

        // 将结果保存到HDFS文件中
        wordCountRdd.saveAsTextFile("/user/beifeng/spark/java/wc-output4");

        /**
         * 针对Count进行降序排序，使用top方法
         */
        // TODO :
        //     rdd.top(5)(OrderingUtils.SecondValueOrdering)
        List<Tuple2<String, Integer>> topList = wordCountRdd.top(
                3, // TopKey程序汇总Key
                new SecondValueComparator() // 自定义比较器
                /**
                new Comparator<Tuple2<String, Integer>>() {
                    // Compares ite two arguments for order
                    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                        // 调用Integer的compareTo方法进行比较
                        return o1._2().compareTo(o2._2());
                    }
                 上述会出现以下错误异常：
                    Exception in thread "main" org.apache.spark.SparkException: Task not serializable
                */
        );
        // iterator
        for(Tuple2<String, Integer> output: topList){
            System.out.println(output._1() + " : " + output._2());
        }


/** ========================================================================== */
        // JavaSparkContext Stop
        sc.stop();

    }


    /**
     * 自定义比较器, 比较两个Tuple2中的第二元素的大小，记住一定要实现Serializable
     */
    static class SecondValueComparator
            implements Comparator<Tuple2<String, Integer>>, Serializable{
        // Compares ite two arguments for order
        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
            // 调用Integer的compareTo方法进行比较
            return o1._2().compareTo(o2._2());
        }
    }
}
