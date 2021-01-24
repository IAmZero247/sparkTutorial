package com.sparkTutorial.mylearning.performance;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

public class ShufflePartitionSetting {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("performance002").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        spark.conf().set("spark.sql.shuffle.partitions", 10);

        //for below task maximum of 6*12 partitions are needed
        // loglevels * months
        Dataset<Row> dataset = spark.read().option("header" ,"true").csv("in/narrow_and_wide_transformation/biglog.txt");

        dataset.createOrReplaceTempView("logging_table");
        //**spark-sql
        Dataset<Row> result = spark.sql("select level, date_format(datetime, 'MMMM') AS month, count(1) as total from logging_table group by level, month order by first(cast(date_format(datetime, 'M') as int)), level");
        //**spark-dataframe
        result.show(100);
        //Scanner scan = new Scanner(System.in);
        //scan.nextLine();
        spark.close();
    }
}
