package com.sparkTutorial.mylearning.dataset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSql008First {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        //SparkConf conf = new SparkConf().setAppName("sparksql001").setMaster("local[*]");
        //JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().appName("sparksql008").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        Dataset<Row> ds = spark.read().option("header" ,"true").csv("in/exams/students.csv");
        Row firstRow = ds.first();


        String  subject = firstRow.getAs("subject").toString();
        int year = Integer.parseInt(firstRow.getAs("year"));

        System.out.println(subject);
        System.out.println(year);
        spark.close();
    }

}
