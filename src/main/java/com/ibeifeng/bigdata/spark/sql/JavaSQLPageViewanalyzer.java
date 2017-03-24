package com.ibeifeng.bigdata.spark.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 *  基于JAVA实现SparkSQL数据分析，主要是如何通过Java API创建DataFrame，尤其RDD转换为DataFrame
 */
public class JavaSQLPageViewanalyzer {


    public static void main(String[] args) {
        // 创建SparkConf，读取配置信息
        SparkConf conf = new SparkConf()
                .setAppName(JavaSQLPageViewanalyzer.class.getSimpleName())
                .setMaster("local[2]") ;

        // 创建JavaSparkContext，构建Spark Application入口
        JavaSparkContext sc = new JavaSparkContext(conf) ;

        // 创建SQLContext, 构建SparkSQL程序入口，读取数据，创建DataFrame
        SQLContext sqlContext = new SQLContext(sc) ;

        /**
         * 自定义函数
         */
        // UDF
        sqlContext.udf().register(
                "toUpper",
                new UDF1<String, String>() {
                    public String call(String word) throws Exception {
                        return word.toUpperCase();
                    }
                },
                DataTypes.StringType
        );

        // UDAF
        sqlContext.udf().register(
                "sal_avg",
                new AvgSalUDAFJava()
                // TODO 依据SCALA语言编写的UADF实现JAVA语言的编程
        );

/** ========================================================================== */


        // 从HDFS读取数据 -> RDD
        // /user/hive/warehouse/db_hive.db/page_views
        JavaRDD<String> pageViewRdd = sc.textFile("/user/hive/warehouse/page_views") ;

        // RDD -> DataFrame
        /**
         * 采用自定义schema方式将RDD转换为DataFrame
         *      -1, RDD[Row]
         *      -2, schema
         *      -3, sqlContext.createDataFrame(rowRdd, schema)
         *
         *   track_time              string
             url                     string
             session_id              string
             referer                 string
             ip                      string
             end_user_id             string
             city_id                 string
         */
        JavaRDD<Row> rowRdd = pageViewRdd.map(
                new Function<String, Row>() {
                    // call method
                    public Row call(String line) throws Exception {
                        //　按照　制表符　进行字符串　　分割
                        String[] array = line.split("\t");
                        // Row ,RowFactory.create()
                        return RowFactory.create(
                                array[0], array[1], array[2], array[3], array[4], array[5], array[6]
                        );
                    }
                }
        );

        /**
         * definition schema
         */
        List<StructField> fields = new ArrayList<StructField>() ;
        // add StructField
        fields.add(DataTypes.createStructField("track_time", DataTypes.StringType, true)) ;
        fields.add(DataTypes.createStructField("url", DataTypes.StringType, true)) ;
        fields.add(DataTypes.createStructField("session_id", DataTypes.StringType, true)) ;
        fields.add(DataTypes.createStructField("referer", DataTypes.StringType, true)) ;
        fields.add(DataTypes.createStructField("ip", DataTypes.StringType, true)) ;
        fields.add(DataTypes.createStructField("end_user_id", DataTypes.StringType, true)) ;
        fields.add(DataTypes.createStructField("city_id", DataTypes.StringType, true)) ;

        // 创建StructType
        StructType schema = DataTypes.createStructType(fields) ;

        /**
         * 创建DataFrame
         */
        DataFrame pageViewDF = sqlContext.createDataFrame(rowRdd, schema) ;

        // print schema
        /**
         * root
             |-- track_time: string (nullable = true)
             |-- url: string (nullable = true)
             |-- session_id: string (nullable = true)
             |-- referer: string (nullable = true)
             |-- ip: string (nullable = true)
             |-- end_user_id: string (nullable = true)
             |-- city_id: string (nullable = true)
         */
        pageViewDF.printSchema();
        // print first
 //       System.out.println("Count " + pageViewDF.count());
 //       System.out.println(pageViewDF.first());


        // 注册DataFrame为临时表，以便使用SQL进行数据分析
        pageViewDF.registerTempTable("tmp_page_view");

        // 编写SQL语句，实现按照Session_ID进行分组统计，降序排序
        String sqlStr =
                "SELECT " +
                        "session_id , COUNT(*) cnt " +
                "FROM " +
                        "tmp_page_view " +
                "GROUP BY " +
                        "session_id " +
                "ORDER BY " +
                        "cnt DESC " +
                 "LIMIT 10" ;
        // 执行SQL语句
        DataFrame sessionIdCountDF = sqlContext.sql(sqlStr) ;

       List<String> resultList =  sessionIdCountDF
                .javaRDD()
                .map(
                  new Function<Row, String>(){
                      // call method
                      public String call(Row row) throws Exception {
                          return row.getString(0) + " -> " + row.getLong(1);
                      }
                  }
                ).collect();

        // iterator list
        for (String output: resultList){
            System.out.println(output);
        }

/** ========================================================================== */
        // JavaSparkContext Stop
        sc.stop();
    }
}
