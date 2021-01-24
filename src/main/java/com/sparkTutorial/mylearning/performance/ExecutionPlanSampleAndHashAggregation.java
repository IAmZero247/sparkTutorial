package com.sparkTutorial.mylearning.performance;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

import java.util.Scanner;

/*
 Hash Aggregation is possible only if the data for the value is MUTABLE
  note - Key can be immutable

 Hash Aggregation uses hashmap kind of implementaion but uses native
 (raw) memory. Means not in JVM.

 MUTABLE -> NULLTYPE
            BOOLEAN TYPE
            BYTE TYPE
            SHORT TYPE
            INTEGER TYPE
            LONG TYPE
            FLOAT TYPE
            DOUBLE TYPE
            DATE TYPE
            TIMESTAMP TYPE

 sun.misc.Unsafe

 SORT AGGREGATE is auto choice by spark if key is IMMUTABLE
 */
public class ExecutionPlanSampleAndHashAggregation {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("performance003").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        spark.conf().set("spark.sql.shuffle.partitions", 10);
        Dataset<Row> dataset = spark.read().option("header" ,"true").csv("in/narrow_and_wide_transformation/biglog.txt");

        //dataset.createOrReplaceTempView("logging_table");
        //**spark-sql
        //Dataset<Row> resultSql = spark.sql("select level, date_format(datetime, 'MMMM') AS month, count(1) as total from logging_table group by level, month order by first(cast(date_format(datetime, 'M') as int)), level");
        //**spark-dataframe

        Dataset<Row> resultDf = dataset.select(col("level"),
				                 date_format(col("datetime"), "MMMM").alias("month"),
				                 date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType) );
        resultDf = resultDf.groupBy("level","month","monthnum").count().as("total").orderBy("monthnum");
        resultDf = resultDf.drop("monthnum");


        resultDf.show(100);
        resultDf.explain();
        spark.close();
    }
}
/** SQL - Execution Plan
 * *(3) Project [level#10, month#14, total#15L]
 * +- *(3) Sort [aggOrder#45 ASC NULLS FIRST, level#10 ASC NULLS FIRST], true, 0
 *    +- Exchange rangepartitioning(aggOrder#45 ASC NULLS FIRST, level#10 ASC NULLS FIRST, 10)
 *       +- *(2) HashAggregate(keys=[level#10, date_format(cast(datetime#11 as timestamp), MMMM, Some(Asia/Calcutta))#47], functions=[count(1), first(cast(date_format(cast(datetime#11 as timestamp), M, Some(Asia/Calcutta)) as int), false)])
 *          +- Exchange hashpartitioning(level#10, date_format(cast(datetime#11 as timestamp), MMMM, Some(Asia/Calcutta))#47, 10)
 *             +- *(1) HashAggregate(keys=[level#10, date_format(cast(datetime#11 as timestamp), MMMM, Some(Asia/Calcutta)) AS date_format(cast(datetime#11 as timestamp), MMMM, Some(Asia/Calcutta))#47], functions=[partial_count(1), partial_first(cast(date_format(cast(datetime#11 as timestamp), M, Some(Asia/Calcutta)) as int), false)])
 *                +- *(1) FileScan csv [level#10,datetime#11] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/C:/sparklearning/sparkTutorial/in/narrow_and_wide_transformation/biglog.txt], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<level:string,datetime:string>
 */

/**
 * == Physical Plan ==
 * *(3) Project [level#10, month#14, count#24L]
 * +- *(3) Sort [monthnum#16 ASC NULLS FIRST], true, 0
 *    +- Exchange rangepartitioning(monthnum#16 ASC NULLS FIRST, 10)
 *       +- *(2) HashAggregate(keys=[level#10, month#14, monthnum#16], functions=[count(1)])
 *          +- Exchange hashpartitioning(level#10, month#14, monthnum#16, 10)
 *             +- *(1) HashAggregate(keys=[level#10, month#14, monthnum#16], functions=[partial_count(1)])
 *                +- *(1) Project [level#10, date_format(cast(datetime#11 as timestamp), MMMM, Some(Asia/Calcutta)) AS month#14, cast(date_format(cast(datetime#11 as timestamp), M, Some(Asia/Calcutta)) as int) AS monthnum#16]
 *                   +- *(1) FileScan csv [level#10,datetime#11] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/C:/sparklearning/sparkTutorial/in/narrow_and_wide_transformation/biglog.txt], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<level:string,datetime:string>
 */
