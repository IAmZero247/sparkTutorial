package com.sparkTutorial.mylearning.dataset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class SparkSql009Filtering {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("sparksql009").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        Dataset<Row> ds = spark.read().option("header" ,"true").csv("in/exams/students.csv");
        /*Approach 1
         *Dataset<Row> dsMath  = ds.filter(" subject = 'Math' and grade = 'A+' and year > 2007 ");
         */

        /*Approach 2 - Lambda
         *Dataset<Row> dsMath = ds.filter( i -> i.getAs("subject").equals("Math") && i.getAs("grade").equals("A+") && Integer.parseInt(i.getAs("year")) >=2007 );
         */

        //using columns - Prefered
        Dataset<Row> dsMath = ds.filter( col("subject").equalTo("Math")
        .and(col("grade").equalTo("A+"))
                .and(col("year").geq(2007))
        );
        dsMath.show(10);

        spark.close();

    }
}
