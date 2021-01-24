package com.sparkTutorial.mylearning.dataframe;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class SparkDataFrame001 {
    //DataSet<Row> = DataFrame
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("sparkdf001").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        Dataset<Row> dataframe = spark.read().option("header" ,"true").csv("in/narrow_and_wide_transformation/biglog.txt");
        /*
        dataset.createOrReplaceTempView("logging_table");
        Dataset<Row> result = spark.sql("select level, date_format(datetime, 'MMMM') AS month, count(1) as total from logging_table group by level, month order by first(cast(date_format(datetime, 'M') as int)), level");
        */


        //dataframe = dataframe.selectExpr("level","date_format(datetime , 'MMMM') as month");

        //below alternative for sql
        dataframe = dataframe.select(col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("monthNum").cast(DataTypes.IntegerType)
        );
        dataframe = dataframe.groupBy(col("level"),col("month") , col("monthNum") ).count();
        dataframe = dataframe.orderBy(col("monthNum"), col("level"));
        dataframe = dataframe.drop(col("monthNum"));
        dataframe.printSchema();
        dataframe.show(100);
        spark.close();

    }
}
