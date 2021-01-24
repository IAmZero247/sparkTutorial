package com.sparkTutorial.mylearning.dataframe;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class SparkDataFrame005AddingColumnUsingLit {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("spardf005").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        Dataset<Row> dataframe = spark.read().option("header" ,"true").csv("in/exams/students.csv");
        dataframe.printSchema();
        //dataframe = dataframe.withColumn("EvaluatePass", lit("Pass"));
        dataframe = dataframe.withColumn("EvaluatePass", lit(col("grade").equalTo("A+")));
        dataframe.show(100);
        spark.close();


    }
}
