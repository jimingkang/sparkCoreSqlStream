package com.ibeifeng.bigdata.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 使用Java语言编程实现SparkStreaming的WordCount程序，从NetWork读取数据
 *
 * 作业：
 *      使用Java语言实现从kafka topic中读取数据，并且实时状态更新统计
 *          --1,kafka direct
 *          --2,updateStateByKey
 *          --3,optional: SparkStreaming HA
 *
 */
public class JavaSocketWordCount {

    public static void main(String[] args) {

        // 创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName(JavaSocketWordCount.class.getSimpleName())
                .setMaster(args[0]);  // 应用运行在哪里，通过main方法参数指定

        // 创建JavaStreamingContext
        JavaStreamingContext jssc = new JavaStreamingContext(
          conf, Durations.seconds(5) // 指定batch interval ，批处理时间间隔
        ) ;

        /**
         * 从Socket读取数据
         *      Create a DStream that will connect to hostname:port
         */
        JavaReceiverInputDStream<String> linesDStream = jssc.socketTextStream(
                "hadoop-senior01.ibeifeng.com", 9999
        );

        /**
         * Split each line into words
         */
        JavaDStream<String> wordsDStream = linesDStream.flatMap(
                new FlatMapFunction<String, String>() {
                    // call method
                    public Iterable<String> call(String line) throws Exception {
                        // 按照空格隔开，转换为List集合
                        return Arrays.asList(line.split(" "));
                    }
                }
        );

        /**
         * Count each Word in each Batch
         */
        JavaPairDStream<String, Integer> pairsDStream = wordsDStream.mapToPair(
                new PairFunction<String, String, Integer>(){
                    // call method
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }
                }
        );
        JavaPairDStream<String, Integer> wordCountDStream = pairsDStream.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    // call method
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        /**
         * Print the first num elements of each RDD generated in this DStream.
         */
        wordCountDStream.print();

        // start
        jssc.start();
        jssc.awaitTermination();

        // StreamingContext Stop
        jssc.stop();
    }

}
