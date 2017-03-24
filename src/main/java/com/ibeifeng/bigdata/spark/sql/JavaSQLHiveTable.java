package com.ibeifeng.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 *  基于JAVA实现SparkSQL数据分析，主要是如何通过Java API创建DataFrame，尤其RDD转换为DataFrame
 */
public class JavaSQLHiveTable {


    public static void main(String[] args) {
        // 创建SparkConf，读取配置信息
        SparkConf conf = new SparkConf()
                .setAppName(JavaSQLHiveTable.class.getSimpleName())
                .setMaster("local[2]") ;

        // 创建JavaSparkContext，构建Spark Application入口
        JavaSparkContext sc = new JavaSparkContext(conf) ;

        // 创建SQLContext, 构建SparkSQL程序入口，读取数据，创建DataFrame
        /**
         * SparkSQL如果要想直接读取Hive表中的数据的话，与Hive进行集成,构建HiveContext
         *
         * 对于本地测试的话，我们需要启动Hive的Remote MetaStore服务
         *
         */
        SQLContext sqlContext = new HiveContext(sc) ;

        // DSL
        DataFrame pageViewDF = sqlContext
                .read()
                .table("page_views_txt") ;

        System.out.println("Count = " + pageViewDF.count());
        System.out.println(pageViewDF.first());

        try {
            Thread.sleep(1000000);
        }catch (Exception e){

        }

/** ========================================================================== */
        // JavaSparkContext Stop
        sc.stop();

        /**
         * Exception: java.lang.OutOfMemoryError thrown from the UncaughtExceptionHandler in thread "main"
         *
         * 异常解决：
         *      -XX:PermSize=128m -XX:MaxPermSize=256M
         */
    }
}
