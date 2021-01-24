package com.sparkTutorial.mylearning.dataset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSql010FullSqlApproachUsingTempView {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("sparksql011").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        Dataset<Row> ds = spark.read().option("header" ,"true").csv("in/exams/students.csv");
        ds.createOrReplaceTempView("student");
        Dataset<Row> dsMath1 = spark.sql("select * from student where subject = 'Math' and grade = 'A+' and year > 2007");

        Dataset<Row> dsMath2 = spark.sql("select avg(score) as avg from student where subject = 'Math' and grade = 'A+' and year > 2007");

        Dataset<Row> dsMath3 = spark.sql("select distinct(year) from student order by year desc");

        dsMath3.show();
        spark.close();
    }
}
