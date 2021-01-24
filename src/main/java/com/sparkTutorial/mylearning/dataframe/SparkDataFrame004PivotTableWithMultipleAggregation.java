package com.sparkTutorial.mylearning.dataframe;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class SparkDataFrame004PivotTableWithMultipleAggregation {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("sparkdf004").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        Dataset<Row> dataframe = spark.read().option("header" ,"true").csv("in/exams/students.csv");
        dataframe.printSchema();


        //student_id|exam_center_id|subject|year|quarter|score|grade

        //pivot on subject - score



        /*dataframe = dataframe//select(col("subject"), col("year"), col("score"))
                .groupBy(col("subject"), col("year"))
                .agg(max(col("score").cast(DataTypes.IntegerType)).alias("max"),
                        min(col("score").cast(DataTypes.IntegerType)).alias("min"),
                        avg(col("score").cast(DataTypes.IntegerType)).alias("avg"))
                .orderBy(col("subject"),col("year").cast(DataTypes.IntegerType));*/

        Dataset<Row> pivot = dataframe.groupBy(col("subject")).pivot("year")
                .agg(max(col("score").cast(DataTypes.IntegerType)).alias("max"),
                        min(col("score").cast(DataTypes.IntegerType)).alias("min"),
                        round(avg(col("score").cast(DataTypes.IntegerType)),2).alias("avg"));
        pivot.show(100);


        spark.close();

    }
}
