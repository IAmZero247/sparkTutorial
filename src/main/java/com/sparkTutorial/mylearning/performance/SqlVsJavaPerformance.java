package com.sparkTutorial.mylearning.performance;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import java.util.Scanner;

public class SqlVsJavaPerformance {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("performance001").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        spark.conf().set("spark.sql.shuffle.partitions", 10);
        Dataset<Row> dataset = spark.read().option("header" ,"true").csv("in/narrow_and_wide_transformation/biglog.txt");

        dataset.createOrReplaceTempView("logging_table");
        //**spark-sql
        Dataset<Row> resultSql = spark.sql("select level, date_format(datetime, 'MMMM') AS month, count(1) as total from logging_table group by level, month order by first(cast(date_format(datetime, 'M') as int)), level");
        //**spark-dataframe
        /*
        Dataset<Row> resultDf = dataset.select(col("level"),
				                 date_format(col("datetime"), "MMMM").alias("month"),
				                 date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType) );
		resultDf = resultDf.groupBy("level","month","monthnum").count().as("total").orderBy("monthnum");
		resultDf = resultDf.drop("monthnum");
        */


        resultSql.show(100);
        Scanner scan = new Scanner(System.in);
        scan.nextLine();
        spark.close();
    }
}
