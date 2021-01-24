package com.sparkTutorial.mylearning.dataset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSql015Ordering {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("sparksql015").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        Dataset<Row> dataset = spark.read().option("header" ,"true").csv("in/narrow_and_wide_transformation/biglog.txt");

        dataset.createOrReplaceTempView("logging_table");
        /*
         *Dataset<Row> result = spark.sql("select level, date_format(datetime, 'MMMM') AS month, first(cast(date_format(datetime, 'M') as int)) AS monthInNum, count(1) as total from logging_table group by level, month order by monthInNum, level");
         *result=result.drop("monthInNum");
         */
        Dataset<Row> result = spark.sql("select level, date_format(datetime, 'MMMM') AS month, count(1) as total from logging_table group by level, month order by first(cast(date_format(datetime, 'M') as int)), level");

        result.show(100);
        spark.close();

    }
}
