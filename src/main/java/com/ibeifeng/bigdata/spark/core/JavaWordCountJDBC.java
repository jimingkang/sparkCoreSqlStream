package com.ibeifeng.bigdata.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Iterator;
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
public class JavaWordCountJDBC {

    public static void main(String[] args) {

        // 创建SparkConf，读取配置信息
        SparkConf conf = new SparkConf()
            .setAppName(JavaWordCountJDBC.class.getSimpleName())
            .setMaster("local[2]") ;

        // 创建JavaSparkContext，构建Spark Application入口
        JavaSparkContext sc = new JavaSparkContext(conf) ;

/** ========================================================================== */
        // Step 1: intpu data -> RDD
        JavaRDD<String> rdd = sc.textFile("/test/wc.txt") ;

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

        /**
         * 将结果输出到MySQL数据库中
         */
        wordCountRdd.coalesce(2).foreachPartition(
                new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    // call method
                    public void call(Iterator<Tuple2<String, Integer>> iter) throws Exception {
                        // Step 1: Connection
                        Class.forName("com.mysql.jdbc.Driver");
                        String url = "jdbc:mysql://hadoop-senior01.ibeifeng.com:3306/test";
                        String username = "root";
                        String password = "123456";
                        Connection conn = null;
                        try {
                            // 1. 创建Connection 连接
                            conn = DriverManager.getConnection(url, username, password);

                            conn.setAutoCommit(false);//关闭事务

                            // 2. 构建PrepareStatement
                            String sql = "insert into tb_wordcount values (?, ?)";
                            PreparedStatement pstmt = conn.prepareStatement(sql);

                            // 3. 结果数据输出
                            while (iter.hasNext()) {
                                Tuple2<String, Integer> tuple = iter.next();
                                pstmt.setString(1, tuple._1());
                                pstmt.setInt(2, tuple._2());

                                // pstmt.executeUpdate();   // 每一条更新
                            }

                            pstmt.executeBatch();// 执行批量更新
                            conn.commit();// 语句执行完毕，提交本事务
                        } finally {
                            // 4. 关闭连接
                            conn.close();
                        }
                    }
                }
        );

/** ========================================================================== */
                    // JavaSparkContext Stop
                    sc.stop();

                }

    }
