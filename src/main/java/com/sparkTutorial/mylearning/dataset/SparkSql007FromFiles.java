package com.sparkTutorial.mylearning.dataset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSql007FromFiles {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        //SparkConf conf = new SparkConf().setAppName("sparksql001").setMaster("local[*]");
        //JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().appName("sparksql007").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        Dataset<Row> ds = spark.read().option("header" ,"true").csv("in/exams/students.csv");

        ds.show(10);
        Long dsSize = ds.count();
        System.out.println("dsSize --> " +dsSize);
        spark.close();

    }
}
