package com.sparkTutorial.mylearning.dataframe;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class SparkDataFrame002AggregationSamples {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("sparkdf002").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        //Dataset<Row> dataframe = spark.read().option("header" ,"true").csv("in/exams/students.csv");

        Dataset<Row> dataframe = spark.read().option("header" ,true)
                //.option("inferSchema",true ) // REDUCE performance
                .csv("in/exams/students.csv");

        dataframe.printSchema();

        //find max score on each subject based out on year
        //student_id|exam_center_id|   subject|year|quarter|score|grade

        dataframe = dataframe//select(col("subject"), col("year"), col("score"))
                .groupBy(col("subject"), col("year"))
                .agg(max(col("score").cast(DataTypes.IntegerType)).alias("max"),
                     min(col("score").cast(DataTypes.IntegerType)).alias("min"),
                     avg(col("score").cast(DataTypes.IntegerType)).alias("avg"))
                .orderBy(col("subject"),col("year").cast(DataTypes.IntegerType));
        dataframe.show(100);


        spark.close();

    }
}
